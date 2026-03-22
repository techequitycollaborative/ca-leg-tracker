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
import scraper_utils

# TODO: refactor with Python classes for readability
def scrape_committee_hearing(
        source_url="https://www.assembly.ca.gov/schedules-publications/daily-file", 
        verbose=False
        ):
    
    # Initialize 
    floor_session_results = set()
    committee_hearing_results = set()
    committee_hearing_changes = set()

    # Calendar v2.0
    hearings_normalized = set()

     # Try connecting to page
    try:
        browser, page, handler = scraper_utils.make_page(source_url)

        # Close welcome message if detected
        page.wait_for_selector("div.ui-dialog.was-welcome-message-modal.ui-widget.ui-widget-content.ui-front")
        if verbose:
            "Closing welcome message modal..."
        close_button = page.get_by_role("button", name="Close").first
        close_button.click()

        # Navigate to committee hearings tab
        page.wait_for_selector("div.details-wrapper-committee-hearing")
        if verbose:
            print("Found committee hearings tab")

        # Get pointers to each table row corresponding to a hearing
        hearing_rows = page.locator("tr.committee-hearing-details")
        hearing_count = hearing_rows.count()
        if verbose:
            print(f"Found {hearing_count} hearings")

        # for each hearing
        for i in range(hearing_count):
            # Refetch
            # hearing_rows = page.locator("tr.committee-hearing-details")
            current_hearing = hearing_rows.nth(i)

            # get date, name, time, location
            hearing_date = current_hearing.locator("td.committee_hearing-date").inner_text()

            # normalize time string
            hearing_time = current_hearing.locator("td.committee_hearing-time").inner_text()
            hearing_time = hearing_time.replace("am", " a.m.").replace("pm", " p.m.")
            hearing_name = current_hearing.locator("td.committee_hearing-name").inner_text()
            hearing_loc = current_hearing.locator("td.committee_hearing-location").inner_text()
            if "," in hearing_loc:
                hearing_location, hearing_room = hearing_loc.split(", ")
            else:
                hearing_location = hearing_loc
                hearing_room = ""

            # click three-dot menu
            hearing_menu = current_hearing.locator("button").first
            scraper_utils.page_click(hearing_menu)

            # Check if View Agenda is on the menu, else skip it
            row_menu = current_hearing.locator("div.was-dropdown-menu.dd-show")
            row_menu_contents = row_menu.inner_html()
            
            if "View Agenda" not in row_menu_contents:
                if verbose:
                    print(f"No agenda found for {hearing_name}, moving on...")
                # Close the dropdown menu by clicking the button again
                hearing_menu.click()
                page.wait_for_timeout(500)
                continue
            else:
                print("Clicking current hearing agenda...")
                agenda_link = current_hearing.locator('a[href*="/api/dailyfile/agenda"]')
                scraper_utils.page_click(agenda_link, force=True)
    
                # Wait for the modal to be visible
                page.wait_for_selector("div.agenda-container", state="visible", timeout=5000)

                # Get the HTML content of the modal
                modal_html = page.locator("div.agenda-container").inner_html()

                # Parse with BeautifulSoup
                soup = bs(modal_html, "html.parser")
                
                # with open(f"ASM_agenda-{i}.html", "w", encoding="utf-8") as f:
                #     f.write(soup.prettify())
                # Extract hearing topic if available
                hearing_topic = soup.select("span.HearingTopic")[0].get_text()
                # add to hearings_normalized — one row per unique hearing
                hearings_normalized.add((
                    1,  # chamber_id
                    hearing_date,
                    hearing_name,
                    hearing_time,
                    hearing_location,
                    hearing_room,
                    hearing_topic
                ))

                # Extract measures
                measure_selector = soup.select("span.measureLink")
                if verbose:
                    print("Found {} measures...".format(len(measure_selector)))

                hearing_bills = scraper_utils.collect_measure_info(
                    hearing_date, hearing_name, measure_selector, 1
                )

                current_events_detailed = scraper_utils.add_measure_details(
                    hearing_time, hearing_location, hearing_room, hearing_bills
                )

                # Update results with set intersection operation on a set of collected bills/measures
                committee_hearing_results = (
                    committee_hearing_results | current_events_detailed
                )

                # Close agenda modal
                page.keyboard.press("Escape")
                page.wait_for_timeout(500)

                # Close the dropdown menu by clicking the button again
                hearing_menu.click()
                page.wait_for_timeout(500)

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
    # Concatenate the results into a set
        final_results = floor_session_results | committee_hearing_results
        return hearings_normalized, final_results, committee_hearing_changes

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
