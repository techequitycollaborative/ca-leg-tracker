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
import datetime
from playwright.sync_api import sync_playwright
import scraper_utils

def scrape_dailyfile(
        source_url="https://www.senate.ca.gov/calendar"
):
    start_date = datetime.date.today()
    end_date = start_date + datetime.timedelta(days=30)
    query_url = source_url + "?startDate=" + start_date.strftime("%Y-%m-%d") + "&endDate=" + end_date.strftime("%Y-%m-%d") + "&floorMeetings=1&committeeHearings=1"
    
    floor_session_results = set()
    committee_hearing_results = set()

    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        page.goto(query_url)

        # iterate over date wrapper blocks
        page.wait_for_selector("div.page-events--day-wrapper")
        wrappers = page.locator("div.page-events--day-wrapper")
        wrapper_count = wrappers.count()

        for i in range(wrapper_count):
            # Extract current date
            current_wrapper = wrappers.nth(i)
            current_date = scraper_utils.text_to_date_string(current_wrapper.locator("h2.page-events__date").first.inner_text())

            # Detect empty content
            empty_wrapper = page.locator("div.no-results-message")

            if empty_wrapper.count() > 0:
                print(f"No events scheduled for {current_date}")
            else:
                #TODO: Examine floor session content
                            
                # Examine committee hearing content
                committee_hearing_section = current_wrapper.locator("div.dailyfile-section.committee-hearings")
                hearing_elements = committee_hearing_section.locator("div.page-events__item.page-events__item--committee-hearing")

                # Iterate over individual hearings
                for j in range(hearing_elements.count()):
                    current_hearing = hearing_elements.nth(j)
                    current_name = current_hearing.locator("div.hearing-name").inner_text().title()
                    current_agenda = current_hearing.get_by_role("link", name="View Agenda")
                    scraper_utils.view_agenda(page, current_agenda)

                    # Extract agenda content
                    page.wait_for_selector("div.ui-dialog.ui-corner-all.ui-widget.ui-widget-content.ui-front.event-agenda-modal")
                    content = page.locator("div.ui-dialog.ui-corner-all.ui-widget.ui-widget-content.ui-front.event-agenda-modal").inner_html()
                    soup = bs(content, 'html.parser')

                    # Extract measures
                    committee_hearing_results = committee_hearing_results | scraper_utils.collect_measures(current_date, current_name, soup.select("span.measureLink"))

                    # Close agenda pop-up
                    close_button = page.get_by_role("button", name="Close").first
                    close_button.click()

        browser.close()
    return floor_session_results | committee_hearing_results

def main():
    scrape_dailyfile()

if __name__ == "__main__":
    main()