from bs4 import BeautifulSoup as bs
from dateutil import parser
import urllib.request

def text_to_date_string(s):
    try:
        dt = parser.parse(s)
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        pass

def prettify_structure(content):
    soup = bs(content, 'html.parser')

    # Prettify the HTML
    pretty_html = soup.prettify()
    print(pretty_html)
    print('***'*20)
    return

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

def make_static_soup(page, tag_pattern, make_request=True):  # HELPER FUNCTION
    url = page
    if make_request:
        url = urllib.request.urlopen(page).read()
    soup = bs(url, "html.parser")
    return soup.select(tag_pattern)


