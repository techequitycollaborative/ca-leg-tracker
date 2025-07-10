"""
This scraper pulls content from the CA State Senate's Daily File page and parses relevant events for legislation tracking.

Input: URL for Senate Calendar page (already set as default input)
Output: set of tuples (EVENT_DATE, EVENT_TEXT, BILL_NUMBER) where EVENT_TEXT is a floor action or commitee
hearing description.

First, the page is manipulated using Playwright as a quick/dirty solution to rendering relevant event agendas
by simulating user page navigation.

Once the HTML content is manipulated into the desired state, the contents are parsed by BeautifulSoup so we can extract
elements marked as Daily File section items. For the floor session, we extract the element header which should contain the
current date to enumerate against all floor actions and associated bills/measures. To do so, we need to iterate over all
action subsections (ex: 'First Reading'), which act as event descriptions, and extract a list of bills in that subsection.

For the committee hearings, we use the committee hearing agenda title as an event description, extract the specified date
from the preceding h5 element, and a list of bills within this hearing subsection.

For both the floor action and committee hearing sections, we pass the event date, event text/description, and the list of
bills into a utils function, which returns a set of tuples in the shape (DATE, EVENT_TEXT, BILL_NUMBER).
"""

from bs4 import BeautifulSoup as bs
import scraper_utils


def scrape_dailyfile(source_url="https://www.senate.ca.gov/calendar", verbose=False):
    # Generate start and end dates for a query on the Senate calendar
    start_date, end_date, query_url = scraper_utils.get_start_end_query(source_url)
    if verbose:
        print("Querying for Senate events from {} to {}".format(start_date, end_date))
        print(query_url)

    floor_session_results = set()
    committee_hearing_results = set()
    committee_hearing_changes = set()

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

                if i == 0:  # Assuming Senate DF is only updated day-of
                    # Examine floor session content
                    floor_section = current_wrapper.locator(
                        "div.dailyfile-section.floor-meetings"
                    ).first
                    scheduled = not floor_section.get_by_text(
                        "No floor session scheduled."
                    ).is_visible(timeout=1000)

                    if scheduled:
                        floor_agenda = floor_section.get_by_role(
                            "link", name="View Agenda"
                        ).first  # Assuming we only need first link
                        scraper_utils.view_agenda(page, floor_agenda)

                        agenda_found = not page.get_by_text(
                            "No Agendas were found."
                        ).is_visible(timeout=5000)

                        if agenda_found:
                            print("Floor agenda for {} found".format(current_date))
                            # Extract HTML and parse as BeautifulSoup object
                            content = page.content()
                            soup = bs(content, "html.parser")
                            floor_content = soup.select_one("div.agenda-container")
                            # Parse agenda into actions
                            floor_actions = floor_content.select("h3")
                            for a in floor_actions:

                                # Extract measures AKA bills to be covered in the floor session and union with existing results
                                measures = a.find_next_sibling(
                                    "div", class_="agenda-item"
                                ).select("span.measureLink")
                                # Update results with set intersection operation on a set of collected bills/measures
                                current_events = scraper_utils.collect_measure_info(
                                    current_date, a.text.title(), measures, 2
                                )

                                current_events_detailed = (
                                    scraper_utils.add_measure_details(
                                        "", "", "", current_events
                                    )
                                )
                                floor_session_results = (
                                    floor_session_results | current_events_detailed
                                )
                        # Close agenda pop-up
                        close_button = page.get_by_role("button", name="Close").first
                        close_button.click()

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
                        print("No time or location details could be extracted...")
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

                    # Extract every bill on the agenda
                    current_agenda = current_hearing.get_by_role(
                        "link", name="View Agenda"
                    )
                    scraper_utils.view_agenda(page, current_agenda)

                    # Extract HTML and parse as BeautifulSoup object
                    content = page.content()
                    soup = bs(content, "html.parser")
                    # Extract measures
                    measure_selector = soup.select("span.measureLink")
                    if verbose:
                        print("Found {} measures...".format(len(measure_selector)))

                    current_events = scraper_utils.collect_measure_info(
                        current_date, current_name, measure_selector, 2
                    )

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
        return final_results, committee_hearing_changes

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
    scrape_dailyfile()
    # scrape_dailyfile(verbose=True)


if __name__ == "__main__":
    main()
