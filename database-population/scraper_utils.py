"""
General functions for Daily File scraper programs
"""

from bs4 import BeautifulSoup as bs
from dateutil import parser
import urllib.request

ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36 Edg/116.0.1938.81"


def text_to_date_string(s):
    """
    Input: string
    Output: datetime object

    Converts string to datetime object for easy conversion to other string formats.
    """
    try:
        dt = parser.parse(s)
        return dt.strftime("%Y-%m-%d")
    except ValueError:  # TODO: define more helpful behavior
        pass


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


def view_agenda(page, link):
    """
    Input: Playwright page object, link pointer
    Output: None (clicks the link)
    """
    try:
        link.wait_for(state="attached")
        link.click()
        page.wait_for_timeout(1000)  # Wait for content to load
    except Exception as e:  # TODO: define helpful exception behavior
        return e
    return


def make_page(p):
    browser = p.chromium.launch()
    context = browser.new_context(user_agent=ua)
    page = context.new_page()
    assert page.evaluate("navigator.userAgent") == ua
    return browser, page


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
