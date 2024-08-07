from bs4 import BeautifulSoup as bs
# from dateutil import parser
# from datetime import date
from playwright.sync_api import sync_playwright
import json
from time import sleep

url = "https://www.assembly.ca.gov/schedules-publications/assembly-daily-file"


def handle_response(response):
    if "/agendatext?" in response.url:
        print(response)
        print(json.dumps(response.json()))

def explore(url):
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        page.on("response", handle_response)
        page.goto(url, wait_until="networkidle")
        agenda_link = page.get_by_role("link", name="View Agenda").first
        agenda_link.wait_for(state='attached')
        agenda_link.click()
        sleep(1) # TODO: stabilize this wait time
        soup = bs(page.content(), "html.parser")
        page.context.close()
        browser.close()
        print(soup.select("div[class='dailyfile-results dailyfile-search-results']")[0].prettify())
        # for e in soup.select("div[class='agenda']"):
        #     print("#"*60)
        #     print(e.prettify())
        # print(soup.select("div[class='agenda']"))

def clear_text(tag):
    for element in tag.find_all(recursive=False):
        if element.string:
            element.string.replace_with('')
        clear_text(element)
    return


def prettify(content):
    soup = bs(content, 'html.parser')
    clear_text(soup)

    # Prettify the HTML
    pretty_html = soup.prettify()
    print(pretty_html)
    print('***'*20)
    return


def scrape_agendas(url):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)  # Change to headless=True if you don't want the browser to open
        page = browser.new_page()
        page.goto(url)
        agenda_links = page.query_selector_all('a:has-text("View Agenda")')
        for index, link in enumerate(agenda_links):
            try:
                link.click()
                page.wait_for_timeout(1000)  # Adjust the timeout as needed to wait for the content to load
                
                # Scrape the content after each click
                agenda = page.query_selector('div.attribute.agenda-container:not(.hide)')
                # Get the parent element
                parent_element = agenda.evaluate_handle('el => el.parentElement')
                
                # Get the next sibling of the parent element (the "sister" element)
                sister_element = parent_element.evaluate_handle('el => el.nextElementSibling')
                
                # Within the sister element, find the desired child element
                committee_element = sister_element.evaluate_handle('div.attribute.committees')
                
                if committee_element:
                    committee_content = committee_element.evaluate('el => el.innerHTML')
                    prettify(committee_element)












                # committee_lst = content.evaluate('(el) => el.closest("div.body div.attribute.committees")')
                # if committee_lst:
                #     prettify(committee_lst)
                # if content:
                #     inner_content = content.inner_html()
                #     prettify(inner_content)
                
                # Click the "Hide Agenda" link to close the agenda before proceeding
                hide_link = page.query_selector('a:has-text("Hide Agenda")')
                if hide_link:
                    hide_link.click()
                    page.wait_for_timeout(500)  # Adjust the timeout as needed to ensure the agenda is hidden
                
            except Exception as e:
                print(f"Error processing link {index + 1}: {e}")
    
        # Assuming that the content is loaded in a div with class 'dailyfile-section'
        # daily_file = page.query_selector('div.dailyfile-section')
        # prettify(daily_file.inner_html())
        # content = page.query_selector_all('div.dailyfile-section-item')
        # for c in content:
        #     print("#"*60)
        #     # curr_date = c.query_selector('h5').inner_text() if c.query_selector('h3') else 'No title'
        #     prettify(c.inner_html())
            
        
        browser.close()
        return

def scrape_dailyfile_section_items(url):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)  # Change to headless=True if you don't want the browser to open
        page = browser.new_page()
        page.goto(url)
        items = page.locator("div.dailyfile-section-item").all()
        
    return

# Example usage
# scrape_agendas(url)
scrape_dailyfile_section_items(url)   
