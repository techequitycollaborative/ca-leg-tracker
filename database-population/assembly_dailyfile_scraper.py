from bs4 import BeautifulSoup as bs
from dateutil import parser
from datetime import date
from playwright.sync_api import sync_playwright
import scraper_utils


def normalize_bill_number(text):
    return text.replace("No.", "").replace(".", "").strip()


def collect_measures(event_date, event_description, sel):
    results = set()
    for measure in sel:
        results.add((event_date, event_description, normalize_bill_number(measure.text)))
    return results


def view_agenda(page, link):
    try:
        link.wait_for(state='attached')
        link.click()
        page.wait_for_timeout(1000) # Wait for content to load
    except Exception as e:
        return e
    return

def scrape_dailyfile(
        url="https://www.assembly.ca.gov/schedules-publications/assembly-daily-file"
):
    floor_session_results = set()
    committee_hearing_results = set()
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        page.goto(url)
        floor_session_agenda = page.get_by_role("link", name="View Agenda").first
        view_agenda(page, floor_session_agenda)

        # Extract HTML
        content = page.content()
        soup = bs(content, 'html.parser')
        for idx, section in enumerate(soup.select('div.dailyfile-section-item')):
            if idx == 0: # Special case for the floor session
                floor_date = scraper_utils.text_to_date_string(section.select_one('div.header').text)
                if "No agendas are found for this event." in section.text:
                    continue #TODO: define better behavior for undetermined agendas that can be more easily parsed downstream
                else:
                    # Find the full agenda
                    agenda = section.select_one('div.attribute.agenda-container:not(.hide)')
                    # Parse agenda into actions
                    floor_actions = agenda.select('h5')
                    for a in floor_actions:
                        # Extract measures AKA bills to be covered in the floor session and union with existing results
                        measures = a.find_next_sibling("div", class_="agenda-item").select("span.measureLink")
                        floor_session_results = floor_session_results | collect_measures(floor_date, a.text.title(), measures)
            else:
                hearing_description = section.select_one('div.header').text.title()
                # Extract event date
                hearing_date = scraper_utils.text_to_date_string(section.find_previous('h5').text)
                agenda = section.select_one('div.footer div.attribute.agenda-container.hide')
                # If agenda has been determined, extract specific measures
                if agenda:
                    measures = agenda.select('span.measureLink')
                    committee_hearing_results = committee_hearing_results | collect_measures(hearing_date, hearing_description, measures)

        browser.close()
    return floor_session_results | committee_hearing_results

def main():
    scrape_dailyfile()

if __name__ == "__main__":
    main()
    