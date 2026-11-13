"""Unit tests for the Senate calendar scraper (no browser or network)."""

import logging

import pytest

import sources.schedule_sen_fetch as sen

DAY = "div.page-events--day-wrapper"
SECTION = "div.dailyfile-section.committee-hearings"
ITEM = "div.page-events__item.page-events__item--committee-hearing"
NAME = "div.hearing-name"
TIME_LOC = "div.attribute.page-events__time-location"


class Fake:
    """Minimal stand-in for a Playwright Page/Locator/Browser/handler."""

    def __init__(self, text="", children=None, items=None, html=""):
        self.text = text
        self.html = html
        self.children = children or {}
        self.items = items or []
        self.closed = self.stopped = False
        self.waited_for = []

    def locator(self, selector):
        return self.children.get(selector, Fake())

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


def hearing(name, time_location):
    return Fake(children={NAME: Fake(name), TIME_LOC: Fake(time_location)})


def day(date_text, *hearings):
    return Fake(
        children={
            "h2.page-events__date": Fake(date_text),
            SECTION: Fake(children={ITEM: Fake(items=list(hearings))}),
        }
    )


def calendar(*days):
    return Fake(children={DAY: Fake(items=list(days))})


@pytest.fixture
def browser(monkeypatch):
    """Serve a fake calendar page instead of launching Chromium."""
    opened = {}

    def install(page):
        opened.update(browser=Fake(), page=page, handler=Fake())
        monkeypatch.setattr(
            sen.utils,
            "make_page",
            lambda url: (opened["browser"], opened["page"], opened["handler"]),
        )
        return opened

    # Read text straight from the fake elements
    monkeypatch.setattr(
        sen.utils,
        "get_hearing_detail",
        lambda element, selector, transform="strip": element.locator(selector).text,
    )
    monkeypatch.setattr(
        sen,
        "extract_hearing_notes_bills",
        lambda current_hearing, verbose, page: {
            "notes": "",
            "bills": [
                {"type": "SB", "number": "12", "file_order": 1, "footnote": None, "note_symbol": None}
            ],
        },
    )
    return install


def assert_closed(opened):
    assert opened["page"].closed and opened["browser"].closed and opened["handler"].stopped


def test_scrapes_hearings_across_days(browser):
    opened = browser(
        calendar(
            day("Oct 21, 2026", hearing("Health", "Time: 9 a.m. - 1021 O Street, Room 1200")),
            day("Oct 28, 2026", hearing("Rules", "Time: 3:30 p.m. - 1021 O Street, Room 2200")),
        )
    )

    hearings, bills = sen.scrape_committee_hearing()

    assert hearings == {
        (2, "Health", "2026-10-21", "9 a.m.", "09:00:00", False, "1021 O Street", "Room 1200", ""),
        (2, "Rules", "2026-10-28", "3:30 p.m.", "15:30:00", False, "1021 O Street", "Room 2200", ""),
    }
    assert {(b[1], b[2], b[6]) for b in bills} == {
        ("2026-10-21", "Health", "SB 12"),
        ("2026-10-28", "Rules", "SB 12"),
    }
    assert_closed(opened)


def test_calendar_with_no_events_returns_empty_results(browser):
    page = calendar()  # zero day blocks: the page shows "No events are scheduled."
    opened = browser(page)

    assert sen.scrape_committee_hearing() == (set(), set())
    # Waits for either outcome instead of timing out on the no-results message
    assert page.waited_for == [f"{DAY}, {sen.NO_RESULTS_SELECTOR}"]
    assert_closed(opened)


def test_day_without_hearings_returns_empty_results(browser):
    # e.g. Thanksgiving: a day block with no committee hearings
    browser(calendar(day("Nov 26, 2026")))

    assert sen.scrape_committee_hearing() == (set(), set())


def test_page_failure_returns_none_and_closes_browser(browser):
    page = calendar()

    def timeout(selector, **kwargs):
        raise TimeoutError("Timeout 30000ms exceeded")

    page.wait_for_selector = timeout
    opened = browser(page)

    assert sen.scrape_committee_hearing() is None
    assert_closed(opened)


def test_browser_launch_failure_returns_none(monkeypatch):
    def fail(url):
        raise Exception("All user agents failed")

    monkeypatch.setattr(sen.utils, "make_page", fail)

    assert sen.scrape_committee_hearing() is None


def test_hearing_with_unparseable_details_is_skipped(browser, caplog):
    browser(
        calendar(
            day(
                "Oct 21, 2026",
                hearing("Rules", "Time: TBA"),
                hearing("Health", "Time: 9 a.m. - 1021 O Street, Room 1200"),
            )
        )
    )

    with caplog.at_level(logging.WARNING, logger=sen.logger.name):
        hearings, _ = sen.scrape_committee_hearing()

    assert [h[1] for h in hearings] == ["Health"]
    assert any("Rules" in r.getMessage() for r in caplog.records if r.levelno == logging.WARNING)


@pytest.mark.parametrize(
    "raw, expected",
    [
        (
            "Time: 9 a.m. - 1021 O Street, Room 1200",
            ("9 a.m.", "09:00:00", False, "1021 O Street", "Room 1200"),
        ),
        (
            # Extra segment seen on the live calendar for closed sessions
            "Time: 3:30 p.m. - (Closed Session) - 1021 O Street, Room 2200",
            ("3:30 p.m.", "15:30:00", False, "1021 O Street", "Room 2200"),
        ),
        (
            "Time: Upon adjournment of Session - State Capitol, Room 113",
            ("Upon adjournment of Session", None, True, "State Capitol", "Room 113"),
        ),
        (
            "Time: 10 a.m. - Compton Courthouse 200 West Compton Boulevard",
            ("10 a.m.", "10:00:00", False, "Compton Courthouse 200 West Compton Boulevard", ""),
        ),
    ],
)
def test_parse_hearing_details(browser, raw, expected):
    details = sen.parse_hearing_details(hearing("x", raw))

    keys = ("time_verbatim", "time_normalized", "is_allday", "location", "room")
    assert tuple(details[k] for k in keys) == expected


def test_parse_hearing_details_rejects_missing_location(browser):
    with pytest.raises(ValueError):
        sen.parse_hearing_details(hearing("x", "Time: TBA"))


def test_agenda_notes_from_informational_hearing(monkeypatch):
    # Trimmed from a live informational-hearing agenda (no measures)
    modal = (
        '<div class="agenda"><div class="agenda_hearingTime">9 a.m.</div>'
        '<span class="CommitteeTopic"><span class="HearingTopic">INFORMATIONAL HEARING</span>'
        '<span class="HearingSubject">Tribal-State Gaming Compacts</span></span></div>'
    )
    page = Fake(children={"div.agenda-container": Fake(html=modal)})

    result = sen.extract_hearing_notes_bills(Fake(), False, page)

    assert result == {"notes": "informational hearing", "bills": []}
