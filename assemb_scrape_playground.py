from bs4 import BeautifulSoup as bs
from bs4 import element
from dateutil import parser
import datetime
import time
import urllib.request

### CONSTANTS
assemb_df = "https://www.assembly.ca.gov/dailyfile"
df_url = urllib.request.urlopen(assemb_df).read()
soup = bs(df_url, "html.parser")
house = "California State Assembly"
# get all the DF content
df_content = soup.select("form[action='/dailyfile']")[1]

### FUNCTIONS
def text_to_date(s: str) -> datetime.datetime: #TODO: update per https://stackoverflow.com/questions/33093293/python-dateutil-date-conversion
    return parser.parse(s)

def before_today(s: element.Tag) -> bool:
    """
    I: HTML tag, assuming only contains date-like text
    O: Boolean

    Returns True if HTML tag's date-string came before today
    """
    # returns True if date string came before today
    day = text_to_date(s.text)
    return day < datetime.datetime.now()

def get_session(dt) -> int:
    return dt.year
	
def adjourned_until(reg_floor_session_header: element.Tag) -> datetime.datetime:
    adjourn_p = reg_floor_session_header.next_sibling.next_sibling.next_element
    date_string = ""
    prev_word = ""
    for word in adjourn_p.split(" "):
        if word == "at":
            break
        elif prev_word == "until":
            date_string += word + " "
        else:
            prev_word = word
    return text_to_date(date_string)

def get_next_floor_sess() -> datetime.datetime:
    reg_floor_session_header = df_content.select("h3")[0]
    next_floor_date_h4 = text_to_date(reg_floor_session_header.next_sibling.next_element)
    next_floor_date_p = adjourned_until(reg_floor_session_header)
    return next_floor_date_p
### PLAYGROUND
## get current/upcoming floor session date
# reg_floor_session_tags = df_content.select("h3")
# next_upcoming_day = parser.parse(reg_floor_session_tags[0].next_sibling.next_element)

## get prev?/planned committee hearing dates
# prev_planned = [t for t in reg_floor_session_tags[1].next_siblings if t.name == "h5"]
# for s in prev_planned:
#     print(get_session(s))
# agendas = df_content.select("div[class='agenda'] > div > span[class='Items'] > span[class='Item']")
if __name__ == "__main__":
    print(get_next_floor_sess())
