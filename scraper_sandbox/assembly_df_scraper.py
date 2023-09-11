from bs4 import BeautifulSoup as bs
from dateutil import parser
from datetime import date
import urllib.request
from time import sleep
from playwright.sync_api import sync_playwright


def make_soup(url, tag_pattern):
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        page.goto(url, wait_until="networkidle")
        agenda_link = page.get_by_role("link", name="View Agenda").first
        agenda_link.wait_for(state='attached')
        agenda_link.click()
        # visible_agenda = page.locator("div[class='agenda-item']").last
        # visible_agenda.wait_for(state='attached')
        sleep(1)
        soup = bs(page.content(), "html.parser")
        page.context.close()
        browser.close()
    return soup.select(tag_pattern)


def text_to_date_string(s):
    try:
        dt = parser.parse(s)
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        pass


def select_first_child(soup, pat):
    return soup.select(pat)[0]


def scrape_floor_session(
    source_url="https://www.assembly.ca.gov/schedules-publications/assembly-daily-file",
    pat="div[class='dailyfile-section'] > div[class='dailyfile-section-item']"
):
    event = make_soup(source_url, pat)[0]
    curr_date = text_to_date_string(select_first_child(event, "div[class='header']").text)
    pdf_link = select_first_child(event, "div[class='body'] > div > p > span > a").attrs['href']
    print(curr_date)
    print(pdf_link)
    full_agenda = select_first_child(event, "div[class='footer'] > div[class='attribute agenda-container']")
    agenda_sections = full_agenda.select("h5")
    for section in agenda_sections:
        section_title = section.text
        print(section_title)
        measures = section.next_sibling.select("span[class='Measure'] > a")
        for measure in measures:
            print(measure.text)


def scrape_cmte_hearing(
        source_url='https://www.assembly.ca.gov/schedules-publications/assembly-daily-file',
        pat="div[class='wrapper--border'] > div[class='dailyfile-section-item']"
):
    # committee hearing header >> committee name
    soup = make_soup(source_url, pat)
    i = 0
    for event in soup:
        print(i)
        print(event.text)
        i += 1
    return


def main():
    # scrape_floor_session()
    scrape_cmte_hearing()

if __name__ == "__main__":
    main()
