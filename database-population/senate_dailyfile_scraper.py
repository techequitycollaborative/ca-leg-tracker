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
            print(current_date)

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
                    print(current_name)
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