"""
This scraper can pull content from the most recently published Daily File PDF 
from either chamber of the CA State Assembly and parse floor session updates
for legislation tracking.

Input: None
Output: set of BillInfo objects

Dataclass BillInfo defines the attributres 
"""
import re
from pathlib import Path
from typing import List, Tuple, Dict, Optional
from datetime import date, timedelta, datetime
import requests
import fitz  # PyMuPDF
from dataclasses import dataclass, asdict
import json

# -----------------------------
# Data Classes
# -----------------------------
@dataclass
class CommitteeHearing:
    date: str
    time: str
    committee: str
    subcommittee: str
    chamber: str
    location: str
    room: str
    additional_info: str = ""

    def as_tuple(self):
        return (
            self.date,
            self.time,
            self.committee,
            self.subcommittee,
            self.chamber,
            self.location,
            self.room,
            self.additional_info,
        )


@dataclass
class BillEvent:
    bill_number: str
    event_date: str
    event_info: str = ""
    agenda_number: str = ""
    chamber: str = ""
    # hook for future linking
    hearing: Optional[CommitteeHearing] = None

    def as_tuple(self):
        """
        Return a tuple of the core fields suitable for DB insertion.
        The optional hearing is not included here but can be added later.
        """
        return (
            self.chamber,
            self.event_date,
            self.event_info,
            self.bill_number,
            self.agenda_number,
            self.hearing
            )

# -----------------------------
# Utilities
# -----------------------------
def normalize_whitespace_preserve_types(text: str) -> str:
    return re.sub(r"(\s)\1+", r"\1", text)


def daterange_backwards(start: date, days: int = 14):
    for i in range(days):
        yield start - timedelta(days=i)


def normalize_date_string(date_str: str) -> str:
    try:
        dt = datetime.strptime(date_str, "%A, %B %d, %Y")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        return date_str


# -----------------------------
# URL generation
# -----------------------------
class DailyFileURLBuilder:
    URL_TEMPLATES = {
        "Assembly": (
            "https://www.assembly.ca.gov/sites/assembly.ca.gov/files/"
            "dailyfile/archive/ASM_STD_{session}_R_{datestr}.PDF"
        ),
        "Senate": (
            "https://www.senate.ca.gov/dailyfile/publications/download/"
            "SEN_STD_{session}_R_{datestr}.PDF"
        ),
    }

    def __init__(self, chamber: str, session: str):
        self.chamber = chamber
        self.session = session

    def build_url(self, d: date) -> str:
        return self.URL_TEMPLATES[self.chamber].format(
            session=self.session,
            datestr=d.strftime("%Y%m%d"),
        )

    def find_latest_available(self, start: date, lookback_days: int = 14) -> str:
        for d in daterange_backwards(start, lookback_days):
            url = self.build_url(d)
            try:
                r = requests.head(url, timeout=10)
                if r.status_code == 200:
                    return url
            except requests.RequestException:
                continue
        raise RuntimeError("No valid Daily File PDF found in lookback window")


# -----------------------------
# Core Scraper
# -----------------------------
class DailyFilePDFScraper:
    def __init__(self, chamber: str, pdf_url: str):
        self.chamber = chamber
        self.pdf_url = pdf_url

    # -----------------------------
    # PDF Download / Sections
    # -----------------------------
    def download_pdf(self, output_path: Optional[str] = None) -> str:
        if output_path is None:
            filename = self.pdf_url.split("/")[-1].split("?")[0]
            output_path = str(Path.cwd() / filename)

        path = Path(output_path)
        if path.exists():
            return str(path)

        resp = requests.get(self.pdf_url, timeout=30)
        resp.raise_for_status()
        path.write_bytes(resp.content)
        return str(path)

    def extract_sections(self, pdf_path: str) -> List[Dict]:
        doc = fitz.open(pdf_path)
        toc = doc.get_toc()  # [level, title, start_page]

        sections = []
        for i, (_, title, start_page) in enumerate(toc):
            end_page = toc[i + 1][2] - 1 if i + 1 < len(toc) else doc.page_count
            sections.append({"title": title.strip(), "start_page": start_page, "end_page": end_page, "text": ""})

        for sec in sections:
            txt = ""
            for page_num in range(sec["start_page"] - 1, sec["end_page"]):
                txt += doc[page_num].get_text("text") + "\n"
            sec["text"] = normalize_whitespace_preserve_types(txt)

        return sections

    @staticmethod
    def extract_cover_date(doc: fitz.Document) -> str:
        first_page_text = doc[0].get_text("text")
        date_re = re.compile(r"([A-Z]+,\s+[A-Z]+\s+\d{1,2},\s+\d{4})")
        match = date_re.search(first_page_text)
        if match:
            return normalize_date_string(match.group(1))
        return ""

    # -----------------------------
    # Parsing Bills
    # -----------------------------
    def parse_bills_section(self, section_text: str, daily_file_date: str) -> List[BillEvent]:
        results = []
        lines = [line.strip() for line in section_text.splitlines() if line.strip()]
        i = 0
        while i < len(lines):
            line = lines[i]
            if re.match(r"^(S[AB]|AC[AR]|HR|AB|SB)\s*\d+", line):
                block = [line]
                j = i + 1
                while j < len(lines) and not re.match(r"^(S[AB]|AC[AR]|HR|AB|SB)\s*\d+", lines[j]):
                    block.append(lines[j])
                    j += 1
                bill_number = block[0]
                agenda_number = block[1] if len(block) > 1 else ""
                event_info = block[-2] if len(block) > 2 else ""
                if "Reading" in event_info:
                    results.append(
                        BillEvent(
                            bill_number, 
                            daily_file_date, 
                            event_info, 
                            agenda_number,
                            self.chamber
                            )
                        )
                i = j
            else:
                i += 1
        return results

    # -----------------------------
    # Scrape Entry
    # -----------------------------
    def scrape(self) -> Tuple[List[BillEvent], List[CommitteeHearing]]:
        pdf_path = self.download_pdf()
        doc = fitz.open(pdf_path)
        daily_file_date = self.extract_cover_date(doc)
        sections = self.extract_sections(pdf_path)

        bills: List[BillEvent] = []
        hearings: List[CommitteeHearing] = []

        for sec in sections:
            title_upper = sec["title"].upper()
            # Bills referenced in the Daily File
            if "BILLS ON THE" in title_upper:
                bills.extend(self.parse_bills_section(sec["text"], daily_file_date))
            elif "COMMITTEE HEARINGS" in title_upper or "SCHEDULE OF" in title_upper:
                print(sec["title"])
                print("-"*10)
                print(sec["text"])
        return bills, hearings

# -----------------------------
# Chamber-specific entry points
# -----------------------------
class DailyFileAssemblyScraper(DailyFilePDFScraper):
    def __init__(self, session: str, run_date: Optional[date] = None):
        run_date = run_date or date.today()
        builder = DailyFileURLBuilder("Assembly", session)
        url = builder.find_latest_available(run_date)
        super().__init__("Assembly", url)


class DailyFileSenateScraper(DailyFilePDFScraper):
    def __init__(self, session: str, run_date: Optional[date] = None):
        run_date = run_date or date.today()
        builder = DailyFileURLBuilder("Senate", session)
        url = builder.find_latest_available(run_date)
        super().__init__("Senate", url)


# -----------------------------
# Example usage
# -----------------------------
if __name__ == "__main__":
    test = DailyFileAssemblyScraper(session="2025")
    bills, hearings = test.scrape()
    print("\n---- First 10 Bills ----")
    for b in bills[:10]:
        print(json.dumps(asdict(b), sort_keys=True, indent=4))
