from bs4 import BeautifulSoup as bs
import urllib.request


def make_static_soup(page, tag_pattern, make_request=True):  # HELPER FUNCTION
    url = page
    if make_request:
        url = urllib.request.urlopen(page).read()
    soup = bs(url, "html.parser")
    return soup.select(tag_pattern)


