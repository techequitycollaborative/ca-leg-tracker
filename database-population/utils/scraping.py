"""
General functions for Daily File scraper programs
"""

from bs4 import BeautifulSoup as bs
from playwright.sync_api import sync_playwright, Locator
from dateutil import parser
import datetime
import urllib.request
import random
from time import sleep
import re
import logging

logger = logging.getLogger(__name__)
USERAGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36",
]

detail_fns = {
    "strip": lambda x: x.strip(),
    "title": lambda x: x.strip().title(),
    "lower": lambda x: x.strip().lower(),
}


def get_start_end_query(source_url):
    start_date = datetime.date.today()
    end_date = start_date + datetime.timedelta(days=30)
    query_url = (
        source_url
        + "?startDate="
        + start_date.strftime("%Y-%m-%d")
        + "&endDate="
        + end_date.strftime("%Y-%m-%d")
        + "&committeeHearings=1"
    )
    return start_date, end_date, query_url


ALLDAY_PATTERNS = re.compile(r"prior|upon|adjournment|call of the chair", re.IGNORECASE)


def normalize_hearing_time(time_str):
    """
    Returns (time_normalized, is_allday) tuple.
    time_normalized is a time string in HH:MM:SS format, or None if all-day.
    """
    if not time_str or ALLDAY_PATTERNS.search(time_str):
        return None, True

    normalized = time_str.strip().lower()
    normalized = re.sub(
        r"\bto\b.*$", "", normalized
    ).strip()  # drop end time if present
    normalized = re.sub(r"a\.m\.", "AM", normalized, flags=re.IGNORECASE)
    normalized = re.sub(r"p\.m\.", "PM", normalized, flags=re.IGNORECASE)

    for fmt in ("%I:%M %p", "%I %p"):
        try:
            return (
                datetime.datetime.strptime(normalized, fmt).strftime("%H:%M:%S"),
                False,
            )
        except ValueError:
            continue

    # If parsing fails, preserve as all-day and log
    logger.warning(f"WARNING: could not parse time string '{time_str}', treating as all-day")
    return None, True


def text_to_date_string(s):
    """
    Input: string
    Output: datetime object

    Converts string to datetime object for easy conversion to other string formats.
    """
    try:
        dt = parser.parse(s)
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        logger.warning(f"WARNING: could not parse date string {s}")
        return None


def get_hearing_detail(hearing: Locator, selector: str, transform="strip"):
    result = hearing.locator(selector).inner_text().replace("\n", " ")
    if transform:
        return detail_fns[transform](result)
    else:
        return result


def prettify_structure(content):
    """
    Input: GET request response
    Output: None (prints parsed HTML to console)

    Function for early scraper development for quick and dirty HTML hierarchical visualization
    """
    soup = bs(content, "html.parser")

    # Prettify the HTML
    pretty_html = soup.prettify()
    print(pretty_html)
    print("***" * 20)
    return


def normalize_bill_number(text):
    """
    Input: bill number string from agenda, presumably X.B. No. 123
    Output: "XB 123"
    """
    return text.replace("No.", "").replace(".", "").strip()


def collect_measure_info(event_date, event_description, sel, chamber_id):
    """
    Input: date string, event description, list of selected measure HTML elements, chamber ID
    Output: set of tuples (CHAMBER_ID, EVENT_DATE, EVENT_TEXT, BILL_NUM)
    """
    results = set()
    for i, measure in enumerate(sel):
        results.add(
            (
                chamber_id,
                event_date,
                event_description,
                normalize_bill_number(measure.text),
                i + 1,  # generate an agenda item rank based on webpage
            )
        )
    return results


def add_measure_details(event_time, event_location, event_room, measures):
    results = set()

    for m in measures:  # unpack existing attributes and add these new ones
        results.add((*m, event_time, event_location, event_room))
    return results


def page_click(clickable, force=False):
    """
    Input: Playwright page object, pointer to clickable object
    Output: None (clicks the object)
    """
    try:
        clickable.wait_for(state="visible", timeout=5000)
        clickable.click(force=force)
    except Exception as e:
        logger.error(f"{str(e)}")
        return e


def make_page(url, max_retries=3, timeout=30000, headless=True):
    """
    Args:
        url: target webpage
        max_retries: number of times to retry connection with the same agent
        timeout: max buffer time before retrying connection
    Returns:
        Tuple of (browser, page) if successful
    Raises:
        Exception: If all user agents fail
    """
    # Shuffle possible agents
    shuffled_agents = USERAGENTS.copy()
    random.shuffle(shuffled_agents)

    # Rotate through agents until successful connection
    for user_agent in shuffled_agents:
        for attempt in range(max_retries):
            browser = None
            try:
                # Initialize Playwright handler
                # with sync_playwright() as p:
                handler = sync_playwright().start()
                browser = handler.chromium.launch(
                    headless=headless,
                    args=[
                        "--no-sandbox",
                        "--disable-setuid-sandbox",
                        "--disable-dev-shm-usage",  # /dev/shm is often too small in Docker
                        "--disable-gpu",
                    ]
                )
                # User agent, viewport, locale to avoid detection
                context = browser.new_context(
                    user_agent=user_agent,
                    viewport={"width": 1280, "height": 720},
                    locale="en-US",
                )
                page = context.new_page()
                assert page.evaluate("navigator.userAgent") == user_agent
                page.set_default_timeout(timeout)
                page.set_default_navigation_timeout(60000)  # for page.goto()

                # Randomized delay
                delay = random.uniform(0.5, 3)
                logger.info(f"Attempt {attempt+1}: Delay {delay:.2f}s ({user_agent[:30]}...)")
                sleep(delay)

                # Attempt navigation
                response = page.goto(
                    url, wait_until="domcontentloaded", referer="https://www.google.com"
                )
                # Log error if not successful
                if not response.ok:
                    raise Exception(f"HTTP {response.status}")

                # Log success and return
                logger.info(f"Success with user agent: {user_agent[:50]}...")
                return browser, page, handler

            except TimeoutError:
                logger.warning(
                    f"Attempt {attempt + 1}/{max_retries} failed with UA: {user_agent[:50]}..."
                )
                if browser:
                    browser.close()
                if attempt == max_retries - 1:
                    logger.warning(f"Max retries reached for this UA, trying next...")
                continue
            except Exception as e:
                logger.warning(
                    f"Attempt {attempt} failed ({user_agent[:30]}...): {str(e)[:100]}"
                )
                if browser:
                    browser.close()
                if handler:
                    handler.stop()
                if attempt == max_retries:
                    logger.warning(f"Exhausted retries for agent: {user_agent[:30]}...")

    raise Exception("All user agents failed")


def make_static_soup(page, tag_pattern, make_request=True):
    """
    Input: URL string OR request response, HTML tag pattern, optional request flag
    Output: array of selected tags matching the given HTML tag pattern from the generated soup object

    Flexible, generic soup selection function to abstract most common function in custom scraping
    """
    url = page
    if make_request:
        url = urllib.request.urlopen(page).read()
    soup = bs(url, "html.parser")
    return soup.select(tag_pattern)
