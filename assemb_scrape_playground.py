from bs4 import BeautifulSoup as bs
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
def before_today(s):
    """
    I: HTML tag, assuming only contains date-like text
    O: Boolean

    Returns True if HTML tag's date-string came before today
    """
    # returns True if date string came before today
    day = parser.parse(s.text)
    return day < datetime.datetime.now()

def get_session(s):
    dt = parser.parse(s.text)
    return dt.year

### PLAYGROUND
## get current/upcoming floor session date
# reg_floor_session_tags = df_content.select("h3")
# next_upcoming_day = parser.parse(reg_floor_session_tags[0].next_sibling.next_element)

## get prev?/planned committee hearing dates
# prev_planned = [t for t in reg_floor_session_tags[1].next_siblings if t.name == "h5"]
# for s in prev_planned:
#     print(get_session(s))
agendas = df_content.select("div[class='agenda'] > div > span[class='Items'] > span[class='Item']")
