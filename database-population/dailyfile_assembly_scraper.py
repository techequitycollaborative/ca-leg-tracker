"""
This scraper pulls content from the CA State Assembly's Daily File page and parses relevant events for legislation tracking.

Input: URL for Assembly Daily File page (already set as default input)
Output: set of tuples (EVENT_DATE, EVENT_TEXT, BILL_NUMBER) where EVENT_TEXT is a floor action or commitee
hearing description.

First, the page is manipulated using Playwright as a quick/dirty solution to rendering the relatively large floor session agenda
by simulating user page navigation. This simulated-navigation step is unnecessary to wrangle committee hearing data. It is
(typically) pre-loaded in the hidden components of the HTML and would unnecessarily complicate Playwright handling.

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


def scrape_dailyfile(
    url="https://www.assembly.ca.gov/schedules-publications/assembly-daily-file",
    verbose=False,
):
    # Initialize sets to store tuples of shape (DATE, EVENT_TEXT, BILL_NUMBER)
    floor_session_results = set()
    committee_hearing_results = set()
    committee_hearing_changes = set()

    # Try connecting to page
    try:
        browser, page, handler = scraper_utils.make_page(url)
        print("Preparing to scrape Assembly Daily File...")
        # Find floor session "View Agenda" button and click it to fetch floor session data
        floor_session_agenda = page.get_by_role("link", name="View Agenda").first
        scraper_utils.view_agenda(page, floor_session_agenda)

        # Extract HTML and parse as BeautifulSoup object
        content = page.content()
        soup = bs(content, "html.parser")

        # Select Daily File section elements to loop over them
        for idx, section in enumerate(soup.select("div.dailyfile-section-item")):
            if idx == 0:  # Special case for the floor session
                floor_date = scraper_utils.text_to_date_string(
                    section.select_one("div.header").text
                )

                if verbose:
                    print("Extracting floor session for {}.".format(floor_date))

                if "No agendas are found for this event." in section.text:
                    if verbose:
                        print("No floor session scheduled, moving on...")
                    continue  # TODO: define better behavior for undetermined agendas that can be more easily parsed downstream
                else:
                    # Find the full agenda
                    agenda = section.select_one(
                        "div.attribute.agenda-container:not(.hide)"
                    )

                    # Parse agenda into actions
                    floor_actions = agenda.select("h5")
                    for a in floor_actions:

                        # Extract measures AKA bills to be covered in the floor session and union with existing results
                        measures = a.find_next_sibling(
                            "div", class_="agenda-item"
                        ).select("span.measureLink")

                        # Update results with set intersection operation on a set of collected bills/measures
                        current_events = scraper_utils.collect_measure_info(
                            floor_date, a.text.title(), measures, 1
                        )

                        current_events_detailed = scraper_utils.add_measure_details(
                            "", "", "", current_events
                        )
                        floor_session_results = (
                            floor_session_results | current_events_detailed
                        )

            else:  # Committee hearing data is preloaded, no simulated clicks/fetching needed
                # Extract event text/description
                hearing_description = section.select_one("div.header").text.title()

                hearing_time_location = (
                    section.select_one("div.body")
                    .select_one(".attribute.time-location")
                    .text
                )

                try:
                    hearing_time, hearing_details = hearing_time_location.split(" - ")

                    hearing_location, hearing_room = hearing_details.split(", ")
                except:
                    print(
                        "Could not extract details for {}: {}".format(
                            hearing_description, hearing_time_location
                        )
                    )
                    # TODO: define behavior for hearings not held at the Capitol
                    continue  # Ignore this whole event if not all details can be found

                # Extract event date from the most recent h5 element
                hearing_date = scraper_utils.text_to_date_string(
                    section.find_previous("h5").text
                )
                if verbose:
                    print("Extracting hearing info for {}".format(hearing_date))

                # Extract hearing notes if available
                hearing_note = (
                    section.select_one("div.body")
                    .select_one(".attribute.note")
                    .text.lower()
                )
                if len(hearing_note) and "change" not in hearing_note:
                    temp = (
                        1,
                        hearing_date,
                        hearing_description,
                        hearing_time,
                        hearing_location,
                        hearing_room,
                    )
                    if "canceled" in hearing_note:
                        committee_hearing_changes.add((temp + ("canceled",)))
                    elif "postponed" in hearing_note:
                        committee_hearing_changes.add((temp + ("postponed",)))
                    else:
                        print("Unparseable note: {}".format(hearing_note))
                        print(
                            "Hearing details: {0}, {1}".format(
                                hearing_date, hearing_description
                            )
                        )

                # Select agenda content, which is either None or a Soup object
                agenda = section.select_one(
                    "div.footer div.attribute.agenda-container.hide"
                )

                # If agenda has been determined, extract specific measures
                if agenda:

                    # Select all measures by their link element
                    measures = agenda.select("span.measureLink")

                    # Extract core event info
                    current_events = scraper_utils.collect_measure_info(
                        hearing_date, hearing_description, measures, 1
                    )

                    # Extract event details
                    current_events_detailed = scraper_utils.add_measure_details(
                        hearing_time, hearing_location, hearing_room, current_events
                    )

                    # Update results with set intersection operation on a set of collected bills/measures
                    committee_hearing_results = (
                        committee_hearing_results | current_events_detailed
                    )
        # Close Playwright handler
        browser.close()
        print("Assembly browser closed")

        # Return the intersection of both sets as the total set of all evenets
        final_results = floor_session_results | committee_hearing_results

        return final_results, committee_hearing_changes

    except Exception as e:
        print(f"[ASM] Daily File scrape failed: {e}")
        return None
    finally:
        if page:
            page.close()
        if browser:
            browser.close()
        if handler:
            handler.stop()

def main():
    scrape_dailyfile(verbose=True)


if __name__ == "__main__":
    main()
