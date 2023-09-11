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

with sync_playwright() as p:
    browser = p.chromium.launch()
    page = browser.new_page()
    # page.on("response", handle_response)
    page.goto(url, wait_until="networkidle")
    agenda_link = page.get_by_role("link", name="View Agenda").first
    agenda_link.wait_for(state='attached')
    agenda_link.click()
    sleep(1) # TODO: stabilize this wait time
    soup = bs(page.content(), "html.parser")
    page.context.close()
    browser.close()
    print(soup.select("div[class='agenda']"))
