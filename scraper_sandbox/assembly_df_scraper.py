from bs4 import BeautifulSoup as bs
from dateutil import parser
from time import sleep
from playwright.sync_api import sync_playwright


def make_soup(
        tag_pattern,
        singleton=True,
        url="https://www.assembly.ca.gov/schedules-publications/assembly-daily-file"
):
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        page.goto(url, wait_until="networkidle")
        agenda_link = page.get_by_role("link", name="View Agenda").first
        agenda_link.wait_for(state='attached')
        agenda_link.click()
        sleep(1)
        soup = bs(page.content(), "html.parser")
        page.context.close()
        browser.close()
    if singleton:
        return soup.select_one(tag_pattern)
    else:
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
        pat="div[class='dailyfile-section'] > div[class='dailyfile-section-item']"
):
    result = dict()
    content = make_soup(pat)
    curr_date = text_to_date_string(content.find("div", class_='header').text)
    pdf_link = select_first_child(content, "div[class='body'] > div > p > span > a").attrs['href']
    result[curr_date] = dict()
    result[curr_date]["pdf_link"] = pdf_link
    full_agenda = select_first_child(content, "div[class='footer'] > div[class='attribute agenda-container']")
    agenda_sections = full_agenda.select("h5")
    for section in agenda_sections:
        section_title = section.text
        print(section_title)
        result[curr_date][section_title] = list()
        measures = section.next_sibling.select("span[class='Measure']")
        for measure in measures:
            measure_name = measure.select("a")[0].text
            vote_threshold = measure.select("span[class='VoteRequired']")[0]
            threshold_num = None
            if len(vote_threshold.text):
                threshold_num = int(vote_threshold.text.split(" ")[-1])  # vote required
            result[curr_date][section_title].append((measure_name, threshold_num))
    return result


def scrape_cmte_hearing(
        pat="div[class='wrapper--border'] > div[class='dailyfile-section-item']"
):
    result = dict()
    soup = make_soup(pat, False)
    for cmte_hearing in soup:
        cmte_name = cmte_hearing.find("div", class_="header").get_text()
        result[cmte_name] = dict()
        curr = result[cmte_name]
        hearing_canceled = cmte_hearing.find("div", class_="body").find("div", class_="attribute note").text
        if hearing_canceled == "HEARING  CANCELED":
            curr["status"] = "canceled"
        else:
            curr["status"] = "planned"
        hearing_agenda = cmte_hearing.find("div", class_="agenda")  # can be empty, ex Rules Committee
        if bool(hearing_agenda):
            curr["time"] = cmte_hearing.find("div", class_='agenda_hearingTime').get_text()
            curr["location"] = cmte_hearing.find("div", class_='agenda_hearingLocation').get_text()
            curr["items"] = list()
            hearing_bills = hearing_agenda.find_all("span", class_="Measure")
            for bill in hearing_bills:
                bill_name = bill.find("span", class_="measureLink").get_text()
                curr["items"].append(bill_name)
    return result


def main(func):
    import pprint
    pp = pprint.PrettyPrinter(depth=4)
    test = None
    if func == "floor":
        test = scrape_floor_session()
    elif func == "cmte":
        test = scrape_cmte_hearing()
    pp.pprint(test)


if __name__ == "__main__":
    main("cmte")
