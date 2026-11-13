"""Unit tests for the Senate calendar scraper (no browser or network).

A small fake stands in for Playwright, shaped like the live senate.ca.gov/calendar markup:
page > day block > committee-hearings section > hearing item > name / time-location.
"""

import logging
from collections import namedtuple

import pytest

import sources.schedule_sen_fetch as sen

# Field order of the tuples returned by utils.scraping.normalize_scraper_results
Hearing = namedtuple(
    "Hearing",
    "chamber_id name date time_verbatim time_normalized is_allday location room notes",
)
ScheduledBill = namedtuple(
    "ScheduledBill",
    "chamber_id date hearing_name time_verbatim location room bill file_order footnote note_symbol",
)

SENATE = 2


class FakeElement:
    """Stand-in for a Playwright Page/Locator/Browser/handler with just what the scraper calls."""

    def __init__(self, text="", children=None, items=None, html=""):
        self.text = text
        self.html = html
        self.children = children or {}  # selector -> FakeElement
        self.items = items or []  # what count()/nth() iterate over
        self.closed = self.stopped = False
        self.waited_for = []

    def locator(self, selector):
        return self.children.get(selector, FakeElement())

    def get_by_role(self, *args, **kwargs):
        return self

    @property
    def first(self):
        return self

    def count(self):
        return len(self.items)

    def nth(self, i):
        return self.items[i]

    def inner_text(self):
        return self.text

    def inner_html(self):
        return self.html

    def wait_for(self, **kwargs):
        pass

    def wait_for_selector(self, selector, **kwargs):
        self.waited_for.append(selector)

    def click(self, **kwargs):
        pass

    def close(self):
        self.closed = True

    def stop(self):
        self.stopped = True


def hearing_item(name, time_location):
    return FakeElement(
        children={
            "div.hearing-name": FakeElement(name),
            "div.attribute.page-events__time-location": FakeElement(time_location),
        }
    )


def day_block(date_text, *hearings):
    hearing_list = FakeElement(items=list(hearings))
    section = FakeElement(
        children={"div.page-events__item.page-events__item--committee-hearing": hearing_list}
    )
    return FakeElement(
        children={
            "h2.page-events__date": FakeElement(date_text),
            "div.dailyfile-section.committee-hearings": section,
        }
    )


def calendar_page(*days):
    return FakeElement(children={sen.DAY_WRAPPER_SELECTOR: FakeElement(items=list(days))})


@pytest.fixture(autouse=True)
def read_text_from_fake_elements(monkeypatch):
    monkeypatch.setattr(
        sen.utils,
        "get_hearing_detail",
        lambda element, selector, transform="strip": element.locator(selector).text,
    )


@pytest.fixture
def serve_page(monkeypatch):
    """Serve a fake calendar page instead of launching Chromium.

    Every hearing's agenda lists one bill, SB 12. Returns the fake browser, page,
    and handler so tests can check they were closed.
    """
    opened = {}

    def install(page):
        opened.update(browser=FakeElement(), page=page, handler=FakeElement())
        monkeypatch.setattr(
            sen.utils, "make_page", lambda url: (opened["browser"], page, opened["handler"])
        )
        return opened

    sb_12 = {"type": "SB", "number": "12", "file_order": 1, "footnote": None, "note_symbol": None}
    monkeypatch.setattr(
        sen,
        "extract_hearing_notes_bills",
        lambda current_hearing, verbose, page: {"notes": "", "bills": [sb_12]},
    )
    return install


def all_closed(opened):
    return opened["page"].closed and opened["browser"].closed and opened["handler"].stopped


# --- Full scrape -------------------------------------------------------------


def test_scrapes_hearings_and_bills_across_days(serve_page):
    opened = serve_page(
        calendar_page(
            day_block("Oct 21, 2026", hearing_item("Health", "Time: 9 a.m. - 1021 O Street, Room 1200")),
            day_block("Oct 28, 2026", hearing_item("Rules", "Time: 3:30 p.m. - 1021 O Street, Room 2200")),
        )
    )

    hearings, bills = sen.scrape_committee_hearing()

    assert hearings == {
        Hearing(SENATE, "Health", "2026-10-21", "9 a.m.", "09:00:00", False, "1021 O Street", "Room 1200", ""),
        Hearing(SENATE, "Rules", "2026-10-28", "3:30 p.m.", "15:30:00", False, "1021 O Street", "Room 2200", ""),
    }
    scheduled = {(b.date, b.hearing_name, b.bill) for b in map(ScheduledBill._make, bills)}
    assert scheduled == {("2026-10-21", "Health", "SB 12"), ("2026-10-28", "Rules", "SB 12")}
    assert all_closed(opened)


def test_calendar_with_no_events_returns_empty_results(serve_page):
    # Zero day blocks: the live page shows "No events are scheduled." instead
    page = calendar_page()
    opened = serve_page(page)

    assert sen.scrape_committee_hearing() == (set(), set())
    # Waits for either outcome, rather than timing out waiting for the no-results message
    assert page.waited_for == [f"{sen.DAY_WRAPPER_SELECTOR}, {sen.NO_RESULTS_SELECTOR}"]
    assert all_closed(opened)


def test_day_without_hearings_returns_empty_results(serve_page):
    # e.g. Thanksgiving: a day block that lists holidays but no committee hearings
    serve_page(calendar_page(day_block("Nov 26, 2026")))

    assert sen.scrape_committee_hearing() == (set(), set())


def test_hearing_with_unparseable_details_is_skipped(serve_page, caplog):
    serve_page(
        calendar_page(
            day_block(
                "Oct 21, 2026",
                hearing_item("Rules", "Time: TBA"),
                hearing_item("Health", "Time: 9 a.m. - 1021 O Street, Room 1200"),
            )
        )
    )

    with caplog.at_level(logging.WARNING, logger=sen.logger.name):
        hearings, _ = sen.scrape_committee_hearing()

    assert [Hearing._make(h).name for h in hearings] == ["Health"]
    assert "No time or location details could be extracted for Rules" in caplog.text


# --- Failures return None, not an empty result --------------------------------


def test_page_failure_returns_none_and_closes_browser(serve_page):
    page = calendar_page()

    def time_out(selector, **kwargs):
        raise TimeoutError("Timeout 30000ms exceeded")

    page.wait_for_selector = time_out
    opened = serve_page(page)

    assert sen.scrape_committee_hearing() is None
    assert all_closed(opened)


def test_browser_launch_failure_returns_none(monkeypatch):
    def fail_to_launch(url):
        raise Exception("All user agents failed")

    monkeypatch.setattr(sen.utils, "make_page", fail_to_launch)

    assert sen.scrape_committee_hearing() is None


# --- Time and location parsing -----------------------------------------------


@pytest.mark.parametrize(
    "raw, time_verbatim, time_normalized, is_allday, location, room",
    [
        (
            "Time: 9 a.m. - 1021 O Street, Room 1200",
            "9 a.m.", "09:00:00", False, "1021 O Street", "Room 1200",
        ),
        (
            # Extra middle segment seen on the live calendar for closed sessions
            "Time: 3:30 p.m. - (Closed Session) - 1021 O Street, Room 2200",
            "3:30 p.m.", "15:30:00", False, "1021 O Street", "Room 2200",
        ),
        (
            "Time: Upon adjournment of Session - State Capitol, Room 113",
            "Upon adjournment of Session", None, True, "State Capitol", "Room 113",
        ),
        (
            # Off-site hearings have no room to split out
            "Time: 10 a.m. - Compton Courthouse 200 West Compton Boulevard",
            "10 a.m.", "10:00:00", False, "Compton Courthouse 200 West Compton Boulevard", "",
        ),
    ],
)
def test_parse_hearing_details(raw, time_verbatim, time_normalized, is_allday, location, room):
    details = sen.parse_hearing_details(hearing_item("Any Committee", raw))

    assert details == {
        "time_verbatim": time_verbatim,
        "time_normalized": time_normalized,
        "is_allday": is_allday,
        "location": location,
        "room": room,
    }


def test_parse_hearing_details_rejects_missing_location():
    with pytest.raises(ValueError):
        sen.parse_hearing_details(hearing_item("Any Committee", "Time: TBA"))


# --- Agenda parsing ------------------------------------------------------------


def test_informational_hearing_agenda_has_notes_and_no_bills():
    # Trimmed from a live informational-hearing agenda, which lists no measures
    agenda_html = (
        '<div class="agenda"><div class="agenda_hearingTime">9 a.m.</div>'
        '<span class="CommitteeTopic"><span class="HearingTopic">INFORMATIONAL HEARING</span>'
        '<span class="HearingSubject">Tribal-State Gaming Compacts</span></span></div>'
    )
    page = FakeElement(children={"div.agenda-container": FakeElement(html=agenda_html)})

    result = sen.extract_hearing_notes_bills(FakeElement(), False, page)

    assert result == {"notes": "informational hearing", "bills": []}
