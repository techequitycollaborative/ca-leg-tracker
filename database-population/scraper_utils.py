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


def make_static_soup(page, tag_pattern, make_request=True):  # HELPER FUNCTION
    url = page
    if make_request:
        url = urllib.request.urlopen(page).read()
    soup = bs(url, "html.parser")
    return soup.select(tag_pattern)


