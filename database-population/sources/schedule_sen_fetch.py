"""
This scraper pulls content from the CA State Senate's Calendar page and parses relevant events for legislation tracking.

Input: URL for Senate Calendar page (already set as default input)
Output: set of tuples (EVENT_DATE, EVENT_TEXT, BILL_NUMBER) where EVENT_TEXT is a floor action or commitee
hearing description.

First, the page is manipulated using Playwright as a quick/dirty solution to rendering relevant event agendas
by simulating user page navigation.

Once the HTML content is manipulated into the desired state, the contents are parsed by BeautifulSoup so we can extract
elements marked as Daily File section items. For the committee hearings, we use the committee hearing agenda title as an
event description, extract the specified date from the preceding h5 element, and a list of bills within this hearing
subsection.

For each individual hearing, the event date, event text/description, and the list of bills are passed into a utils
function, which returns a set of tuples in the shape (DATE, EVENT_TEXT, BILL_NUMBER).
"""

from bs4 import BeautifulSoup as bs
import utils.scraping as utils
import logging

logger = logging.getLogger(__name__)

# TODO: refactor with Python classes for readability
def scrape_committee_hearing(
    source_url="https://www.senate.ca.gov/calendar", verbose=False
):
    # Generate start and end dates for a query on the Senate calendar
    start_date, end_date, query_url = utils.get_start_end_query(source_url)
    if verbose:
        logger.info("Querying for Senate events from {} to {}".format(start_date, end_date))
        logger.info(query_url)

    # Calendar v2.0
    hearing_cache = {}  # key: (date, name) -> {'index': int, 'bills': set, ...}

    # Try connecting to page
    try:
        browser, page, handler = utils.make_page(query_url)
        # iterate over date wrapper blocks
        page.wait_for_selector("div.page-events--day-wrapper")
        if verbose:
            logger.info("Found events by date")
        wrappers = page.locator("div.page-events--day-wrapper")
        wrapper_count = wrappers.count()

        logger.info("Preparing to scrape Senate Daily File")
        for i in range(wrapper_count):
            # Extract current date
            current_wrapper = wrappers.nth(i)
            current_date = utils.text_to_date_string(
                current_wrapper.locator("h2.page-events__date").first.inner_text()
            )
            if verbose:
                logger.info("Extracting {}".format(current_date))
            # Detect empty content
            empty_wrapper = page.locator("div.no-results-message")

            if empty_wrapper.count() > 0:
                logger.info(f"No events scheduled for {current_date}")
            else:
                if verbose:
                    logger.info("Looking for events")

                # Examine committee hearing content
                committee_hearing_section = current_wrapper.locator(
                    "div.dailyfile-section.committee-hearings"
                )
                hearing_elements = committee_hearing_section.locator(
                    "div.page-events__item.page-events__item--committee-hearing"
                )
                if verbose:
                    logger.info("Found {} hearings".format(hearing_elements.count()))
                # Iterate over individual hearings
                for j in range(hearing_elements.count()):
                    # Extract current hearing details
                    current_hearing = hearing_elements.nth(j)
                    current_name = utils.get_hearing_detail(
                        current_hearing, "div.hearing-name", "title"
                    )
                    # Extract details like time, location, room
                    try:
                        current_details = utils.get_hearing_detail(
                            current_hearing,
                            "div.attribute.page-events__time-location",
                            False,
                        )
                        current_time_verbatim, current_loc = current_details.split(
                            " - "
                        )
                        current_time_verbatim = current_time_verbatim.replace(
                            "Time: ", ""
                        )
                        current_time, is_allday = utils.normalize_hearing_time(
                            current_time_verbatim
                        )
                        if current_loc.count(",") == 1 and "Room" in current_loc:
                            current_location, current_room = current_loc.split(", ")
                        else:
                            current_location = current_loc
                            current_room = ""
                    except:
                        logger.warning(
                            f"No time or location details could be extracted for {current_name} on {current_date}"
                        )
                        logger.info(current_details)
                        continue

                    hearing_key = (
                        current_date,
                        current_name,
                        current_time_verbatim,
                        current_location,
                        current_room
                    )

                    # Extract every bill on the agenda
                    current_agenda = current_hearing.get_by_role(
                        "link", name="View Agenda"
                    )
                    utils.page_click(current_agenda)

                    # Extract HTML and parse as BeautifulSoup object
                    # Wait for the modal to be visible
                    page.wait_for_selector(
                        "div.agenda-container", state="visible", timeout=5000
                    )

                    # Get the HTML content of the modal
                    modal_html = page.locator("div.agenda-container").inner_html()

                    # Parse with BeautifulSoup
                    soup = bs(modal_html, "html.parser")

                                        # Extract hearing notes if available
                    # use HearingTopic for general notes with "; " separator
                    current_note = ""
                    topics = soup.select("span.HearingTopic")
                    logger.debug(topics)
                    current_note = "; ".join(
                        [
                            t.text.lower().strip()
                            for t in topics
                            if "_" not in t.text
                        ]
                    )
                    logger.debug(f"Note extracted: {current_note}")

                    # extract FootNote span if it exists
                    has_footnotes = soup.select_one("span.MeasureFootNotes")
                    symbol_to_footnote = None
                    if has_footnotes:
                        symbol_to_footnote = utils.extract_footnote_symbol(has_footnotes)
                        logger.info(
                            f"Footnote to symbol map:\n{symbol_to_footnote}"
                        )
                    # Extract all HTML elements with the measure identifier
                    measure_selector = soup.select("span.Measure")
                    if verbose:
                        logger.info("Found {} measures".format(len(measure_selector)))

                    current_bills = utils.collect_measure_order_footnotes(
                        measure_selector,
                        footnote_map=symbol_to_footnote
                    )

                    if has_footnotes:
                        logger.debug(current_bills)
                    if hearing_key not in hearing_cache:
                        hearing_cache[hearing_key] = {
                            'chamber_id': utils.transform_chamber_id(2, current_name),
                            'name': current_name,
                            'date': current_date,
                            'time_verbatim': current_time_verbatim,
                            'time_normalized': current_time,
                            'is_allday': is_allday,
                            'location': current_location,
                            'room': current_room,
                            'notes': current_note,
                            'bills': current_bills,
                            'index': j
                        }
                    elif j > hearing_cache[hearing_key]['index']:
                        hearing_cache[hearing_key] = {
                            'chamber_id': utils.transform_chamber_id(2, current_name),
                            'name': current_name,
                            'date': current_date,
                            'time_verbatim': current_time_verbatim,
                            'time_normalized': current_time,
                            'is_allday': is_allday,
                            'location': current_location,
                            'room': current_room,
                            'notes': current_note,
                            'bills': current_bills,
                            'index': j
                        }
                        if verbose:
                            logger.info(f"Replaced duplicate hearing: {hearing_key}")

                    # Close agenda pop-up
                    close_button = page.get_by_role("button", name="Close").first
                    close_button.click()

        browser.close()
        logger.info("Closed Senate browser")

    except Exception as e:
        logger.error(f"[SEN] Daily File scrape failed: {e}")
        return None
    finally:
        if page:
            page.close()
        if browser:
            browser.close()
        if handler:
            handler.stop()

    # Build final results from cache
    hearings_normalized = set()
    bills_natural_key = set()

    for cached in hearing_cache.values():
        hearings_normalized.add(
            (
                cached['chamber_id'],
                cached['name'],
                cached['date'],
                cached['time_verbatim'],
                cached['time_normalized'],
                cached['is_allday'],
                cached['location'],
                cached['room'],
                cached['notes']
            )
        )

        for bill in cached['bills']:
            bill_name = f"{bill["type"]} {bill["number"]}"

            # Manually reorder attributes to minimize refactor
            bills_natural_key.add(
                (
                    cached['chamber_id'],
                    cached['date'],
                    cached['name'],
                    cached['time_verbatim'],
                    cached['location'],
                    cached['room'],
                    bill_name,
                    bill['file_order'],
                    bill['footnote'],
                    bill['note_symbol']
                )
            )
    # Concatenate the results into a set
    return hearings_normalized, bills_natural_key

def main():
    logging.basicConfig(level=logging.DEBUG)
    hearings, bills = scrape_committee_hearing(verbose=True)

    print("Detected hearings:")
    for row in sorted(hearings, key=lambda x: x[2]):
        print(row)

    print("Detected bills scheduled for hearing:")
    for row in sorted(bills, key=lambda x: (x[2], x[4])):
        print(row)


if __name__ == "__main__":
    main()
