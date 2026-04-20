"""
This scraper pulls content from the CA State Assembly's Daily File page and parses relevant events for legislation
tracking.

Input: URL for Assembly Daily File page (already set as default input)
Output: set of tuples (EVENT_DATE, EVENT_TEXT, BILL_NUMBER) where EVENT_TEXT is a floor action or commitee
hearing description.

First, the page is manipulated using Playwright as a quick/dirty solution to render the agenda modal for each committee
hearing by simulating user page navigation. This simulated-navigation step is unnecessary to wrangle committee hearing
data. It is (typically) pre-loaded in the hidden components of the HTML and would unnecessarily complicate Playwright
handling.

Once the HTML content is manipulated into the desired state, the contents are parsed by BeautifulSoup so we can extract
elements marked as Daily File section items. For the committee hearings, we use the committee hearing agenda title as an
event description, extract the specified date from the preceding h5 element, and a list of bills within this hearing
subsection.

For both the floor action and committee hearing sections, we pass the event date, event text/description, and the list
of bills into a utils function, which returns a set of tuples in the shape (DATE, EVENT_TEXT, BILL_NUMBER).
"""

from bs4 import BeautifulSoup as bs
import utils.scraping as utils
import logging

logger = logging.getLogger(__name__)

# TODO: refactor with Python classes for readability
def scrape_committee_hearing(
    source_url="https://www.assembly.ca.gov/schedules-publications/daily-file",
    verbose=False,
):

    # Initialize
    hearing_cache = {}  # key: (date, name) -> {'index': int, 'bills': set, ...}

    # Try connecting to page
    try:
        browser, page, handler = utils.make_page(source_url)

        # Close welcome message if detected
        page.wait_for_selector(
            "div.ui-dialog.was-welcome-message-modal.ui-widget.ui-widget-content.ui-front"
        )
        if verbose:
            "Closing welcome message modal"
        close_button = page.get_by_role("button", name="Close").first
        close_button.click()

        # Navigate to committee hearings tab
        page.wait_for_selector("div.details-wrapper-committee-hearing")
        if verbose:
            logger.info("Found committee hearings tab")

        # Get pointers to each table row corresponding to a hearing
        hearing_rows = page.locator("tr.committee-hearing-details")
        hearing_count = hearing_rows.count()
        if verbose:
            logger.info(f"Found {hearing_count} hearings")

        # for each hearing
        for i in range(hearing_count):
            current_hearing = hearing_rows.nth(i)

            # get date, name, time, location
            details = {}
            for detail in ["date", "time", "name", "location"]:
                details[detail] = utils.get_hearing_detail(
                    current_hearing, f"td.committee_hearing-{detail}"
                )

            # normalize details
            details["date"] = utils.text_to_date_string(details["date"])
            details["time"] = details["time"].replace("am", " a.m.")
            details["time"] = details["time"].replace("pm", " p.m.")
            details["time_verbatim"] = details["time"]
            details["time_normalized"], details["is_allday"] = (
                utils.normalize_hearing_time(details["time_verbatim"])
            )
            # Only parse the 'normal' locations with typical address + room number
            if details["location"].count(",") == 1 and "Room" in details["location"]:
                details["location"], details["room"] = details["location"].split(", ")
            else:
                details["room"] = ""

            # Update hearing cache
            hearing_key = (
                details["date"],
                details["name"],
                details["time_verbatim"],
                details["location"],
                details["room"]
            )
            # click three-dot menu
            hearing_menu = current_hearing.locator("button").first
            utils.page_click(hearing_menu)

            # Check if View Agenda is on the menu, else skip it
            row_menu = current_hearing.locator("div.was-dropdown-menu.dd-show")
            row_menu_contents = row_menu.inner_html()
            hearing_notes = ""  # default value
            hearing_bills = [] # default value

            if "View Agenda" not in row_menu_contents:
                if verbose:
                    logger.info(f"No agenda found for {details["name"]}")
                # Close the dropdown menu by clicking the button again
                hearing_menu.click()
                page.wait_for_timeout(500)
            else:  # Check for bills on the hearing agenda
                logger.info("Clicking current hearing agenda")
                agenda_link = current_hearing.locator(
                    'a[href*="/api/dailyfile/agenda"]'
                )
                utils.page_click(agenda_link, force=True)

                # Wait for the modal to be visible
                page.wait_for_selector(
                    "div.agenda-container", state="visible", timeout=5000
                )

                # Get the HTML content of the modal
                modal_html = page.locator("div.agenda-container").inner_html()

                # Parse with BeautifulSoup
                soup = bs(modal_html, "html.parser")

                # with open(f"ASM_agenda-{i}.html", "w", encoding="utf-8") as f:
                #     f.write(soup.prettify())
                # Extract hearing topic if available
                hearing_topics = ""
                topics = soup.select("span.HearingTopic")
                hearing_notes = "".join(
                    [
                        t.get_text().lower().strip()
                        for t in topics
                    ]
                )

                # Extract measures
                measure_selector = soup.select("span.measureLink")
                if verbose:
                    logger.info("Found {} measures".format(len(measure_selector)))

                hearing_bills = [
                    utils.normalize_bill_number(m.text)
                    for m in measure_selector
                ]

                # Close agenda modal
                page.keyboard.press("Escape")
                page.wait_for_timeout(500)

                # Close the dropdown menu by clicking the button again
                hearing_menu.click()
                page.wait_for_timeout(500)
            
            if hearing_key not in hearing_cache:
                hearing_cache[hearing_key] = {
                    'chamber_id': utils.transform_chamber_id(1, details['name']),
                    'name': details["name"],
                    'date': details["date"],
                    'time_verbatim': details["time_verbatim"],
                    'time_normalized': details["time_normalized"],
                    'is_allday': details["is_allday"],
                    'location': details["location"],
                    'room': details["room"],
                    'notes': hearing_notes,
                    'bills': hearing_bills,
                    'index': i
                }
            elif i > hearing_cache[hearing_key]['index']:
                hearing_cache[hearing_key] = {
                    'chamber_id': utils.transform_chamber_id(1, details['name']),
                    'name': details["name"],
                    'date': details["date"],
                    'time_verbatim': details["time_verbatim"],
                    'time_normalized': details["time_normalized"],
                    'is_allday': details["is_allday"],
                    'location': details["location"],
                    'room': details["room"],
                    'notes': hearing_notes,
                    'bills': hearing_bills,
                    'index': i
                }
                if verbose:
                    logger.info(f"Replaced duplicate hearing: {hearing_key}")

    except Exception as e:
        logger.error(f"[ASM] Daily File scrape failed: {e}")
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

        for file_order, bill in enumerate(cached['bills']):
            # Manually reorder attributes to minimize refactor
            bills_natural_key.add(
                (
                    cached['chamber_id'],
                    cached['date'],
                    cached['name'],
                    bill,
                    file_order,
                    cached['time_verbatim'],
                    cached['location'],
                    cached['room']
                )
            )
    # Concatenate the results into a set
    return hearings_normalized, bills_natural_key


def main():
    hearings, bills = scrape_committee_hearing(verbose=True)

    print("Detected hearings:")
    for row in sorted(hearings, key=lambda x: x[2]):
        print(row)

    print("Detected bills scheduled for hearing:")
    for row in sorted(bills, key=lambda x: (x[3])):
        print(row)


if __name__ == "__main__":
    main()
