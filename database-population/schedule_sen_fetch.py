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
import scraper_utils

# TODO: refactor with Python classes for readability
def scrape_committee_hearing(source_url="https://www.senate.ca.gov/calendar", verbose=False):
    # Generate start and end dates for a query on the Senate calendar
    start_date, end_date, query_url = scraper_utils.get_start_end_query(source_url)
    if verbose:
        print("Querying for Senate events from {} to {}".format(start_date, end_date))
        print(query_url)

    floor_session_results = set()
    committee_hearing_results = set()
    committee_hearing_changes = set()
    
    # Calendar v2.0
    hearings_normalized = set()

     # Try connecting to page
    try:
        browser, page, handler = scraper_utils.make_page(query_url)
        # iterate over date wrapper blocks
        page.wait_for_selector("div.page-events--day-wrapper")
        if verbose:
            print("Found events by date")
        wrappers = page.locator("div.page-events--day-wrapper")
        wrapper_count = wrappers.count()

        print("Preparing to scrape Senate Daily File...")
        for i in range(wrapper_count):
            # Extract current date
            current_wrapper = wrappers.nth(i)
            current_date = scraper_utils.text_to_date_string(
                current_wrapper.locator("h2.page-events__date").first.inner_text()
            )
            if verbose:
                print("Extracting {}".format(current_date))
            # Detect empty content
            empty_wrapper = page.locator("div.no-results-message")

            if empty_wrapper.count() > 0:
                print(f"No events scheduled for {current_date}")
            else:
                if verbose:
                    print("Looking for events...")

                # Examine committee hearing content
                committee_hearing_section = current_wrapper.locator(
                    "div.dailyfile-section.committee-hearings"
                )
                hearing_elements = committee_hearing_section.locator(
                    "div.page-events__item.page-events__item--committee-hearing"
                )
                if verbose:
                    print("Found {} hearings...".format(hearing_elements.count()))
                # Iterate over individual hearings
                for j in range(hearing_elements.count()):
                    # Extract current hearing details
                    current_hearing = hearing_elements.nth(j)
                    current_name = (
                        current_hearing.locator("div.hearing-name").inner_text().title()
                    )
                    # Extract details like time, location, room
                    try:
                        current_details = current_hearing.locator(
                            "div.attribute.page-events__time-location"
                        ).inner_text()
                        current_time, current_loc = current_details.split(" - ")
                        current_time = current_time.replace("Time: ", "")
                        current_location, current_room = current_loc.split(", ")
                    except:
                        print(f"No time or location details could be extracted for {current_name} on {current_date}...")
                        print(current_details)
                        continue

                    # Extract hearing notes if available
                    current_note = (
                        current_hearing.locator("div.attribute.note")
                        .inner_text()
                        .lower()
                    )

                    if len(current_note) and "change" not in current_note:
                        temp = (
                            2,
                            current_date,
                            current_name,
                            current_time,
                            current_location,
                            current_room,
                        )
                        if "canceled" in current_note:
                            committee_hearing_changes.add((temp + ("canceled",)))
                        elif "postponed" in current_note:
                            committee_hearing_changes.add((temp + ("postponed",)))
                        else:
                            print("Unparseable note: {}".format(current_note))
                            print(
                                "Hearing details: {0}, {1}".format(
                                    current_date, current_name
                                )
                            )
                    # add to hearings_normalized — one row per unique hearing
                    hearings_normalized.add((
                        2,  # chamber_id
                        current_date,
                        current_name,
                        current_time,
                        current_location,
                        current_room
                    ))

                    # Extract every bill on the agenda
                    current_agenda = current_hearing.get_by_role(
                        "link", name="View Agenda"
                    )
                    scraper_utils.page_click(current_agenda)

                    # Extract HTML and parse as BeautifulSoup object
                    # Wait for the modal to be visible
                    page.wait_for_selector("div.agenda-container", state="visible", timeout=5000)

                    # Get the HTML content of the modal
                    modal_html = page.locator("div.agenda-container").inner_html()

                    # Parse with BeautifulSoup
                    soup = bs(modal_html, "html.parser")
                    
                    # with open(f"SEN_agenda-{i}.html", "w", encoding="utf-8") as f:
                    #     f.write(soup.prettify())
                    # Extract all HTML elements with the measure identifier
                    measure_selector = soup.select("span.measureLink")
                    if verbose:
                        print("Found {} measures...".format(len(measure_selector)))

                    # Generate a tuple with hearing date, name, chamber_id=2 for every measure element
                    current_events = scraper_utils.collect_measure_info(
                        current_date, current_name, measure_selector, 2
                    )

                    # Expand the tuples with all details
                    current_events_detailed = scraper_utils.add_measure_details(
                        current_time, current_location, current_room, current_events
                    )

                    # Update results with set intersection operation on a set of collected bills/measures
                    committee_hearing_results = (
                        committee_hearing_results | current_events_detailed
                    )

                    # Close agenda pop-up
                    close_button = page.get_by_role("button", name="Close").first
                    close_button.click()

        browser.close()
        print("Closed Senate browser")
        
        # Concatenate the results into a set
        final_results = floor_session_results | committee_hearing_results
        return hearings_normalized, final_results, committee_hearing_changes

    except Exception as e:
        print(f"[SEN] Daily File scrape failed: {e}")
        return None
    finally:
        if page:
            page.close()
        if browser:
            browser.close()
        if handler:
            handler.stop()
    

def main():
    # final, changes = scrape_committee_hearing()
    final, changes = scrape_committee_hearing(verbose=True)
    
    print("Detected changes:")
    for row in changes:
        print(row)

    print("Detected bills scheduled for hearing:")
    for row in final:
        print(row)

if __name__ == "__main__":
    main()
