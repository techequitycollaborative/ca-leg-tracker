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

# TODO: refactor with Python classes for readability
def scrape_committee_hearing(
        source_url="https://www.assembly.ca.gov/schedules-publications/daily-file", 
        verbose=False
        ):
    
    # Initialize 
    hearings_normalized = set()
    bills_natural_key = set()

     # Try connecting to page
    try:
        browser, page, handler = utils.make_page(source_url)

        # Close welcome message if detected
        page.wait_for_selector("div.ui-dialog.was-welcome-message-modal.ui-widget.ui-widget-content.ui-front")
        if verbose:
            "Closing welcome message modal"
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
            details = {}
            for detail in ["date", "time", "name", "location"]:
                details[detail] = utils.get_hearing_detail(
                    current_hearing,
                    f"td.committee_hearing-{detail}"
                )
            
            # normalize details
            details["date"] = utils.text_to_date_string(details["date"])
            details["time"] = details["time"].replace("am", " a.m.")
            details["time"] = details["time"].replace("pm", " p.m.")
            details["time_verbatim"] = details["time"]
            details["time_normalized"], details["is_allday"] = utils.normalize_hearing_time(details["time_verbatim"])
            if "," in details["location"]:
                details["location"], details["room"] = details["location"].split(", ")
            else:
                details["room"] = ""
           
            # click three-dot menu
            hearing_menu = current_hearing.locator("button").first
            utils.page_click(hearing_menu)

            # Check if View Agenda is on the menu, else skip it
            row_menu = current_hearing.locator("div.was-dropdown-menu.dd-show")
            row_menu_contents = row_menu.inner_html()
            
            if "View Agenda" not in row_menu_contents:
                if verbose:
                    print(f"No agenda found for {details["name"]}, moving on")
                # Close the dropdown menu by clicking the button again
                hearing_menu.click()
                page.wait_for_timeout(500)
                continue
            else:
                print("Clicking current hearing agenda")
                agenda_link = current_hearing.locator('a[href*="/api/dailyfile/agenda"]')
                utils.page_click(agenda_link, force=True)
    
                # Wait for the modal to be visible
                page.wait_for_selector("div.agenda-container", state="visible", timeout=5000)

                # Get the HTML content of the modal
                modal_html = page.locator("div.agenda-container").inner_html()

                # Parse with BeautifulSoup
                soup = bs(modal_html, "html.parser")
                
                # with open(f"ASM_agenda-{i}.html", "w", encoding="utf-8") as f:
                #     f.write(soup.prettify())
                # Extract hearing topic if available
                topics = soup.select("span.HearingTopic")
                hearing_notes = topics[0].get_text() if topics else ""
                
                # add to hearings_normalized — one row per unique hearing
                hearings_normalized.add((
                    1,  # chamber_id
                    details["name"],
                    details["date"],
                    details["time_verbatim"],
                    details["time_normalized"],
                    details["is_allday"],
                    details["location"],
                    details["room"],
                    hearing_notes
                ))

                # Extract measures
                measure_selector = soup.select("span.measureLink")
                if verbose:
                    print("Found {} measures".format(len(measure_selector)))

                hearing_bills = utils.collect_measure_info(
                    details["date"], details["name"], measure_selector, 1
                )

                current_events_detailed = utils.add_measure_details(
                    details["time_verbatim"], details["location"], details["room"], hearing_bills
                )

                # Update results with set intersection operation on a set of collected bills/measures
                bills_natural_key = (
                    bills_natural_key | current_events_detailed
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
        return hearings_normalized, bills_natural_key

def main():
    hearings, bills = scrape_committee_hearing(verbose=True)

    print("Detected hearings:")
    for row in hearings:
        print(row)

    print("Detected bills scheduled for hearing:")
    for row in bills:
        print(row)

if __name__ == "__main__":
    main()
