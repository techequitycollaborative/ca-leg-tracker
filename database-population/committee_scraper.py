import scraper_utils
# scrape standing committees: (name, chamber_id, url)
# committee table: transform each committee name-link tuple for table insertion
# for each committee link, scrape for "XYZ Members" title >> IF exists, pull names ELSE + "/membersstaff" to URL to pull
assembly_cmte_home = "https://www.assembly.ca.gov/committees"
senate_cmte_home = "https://www.senate.ca.gov/committees"


def make_soup(chamber):
    soup = None
    pat = None
    chamber_url = None
    if chamber == "assembly":
        pat = "div[id='committees_wrapper']"
        chamber_url = assembly_cmte_home
    elif chamber == "senate":
        pat = "ul[class='page-committees__committee-menu']"
        chamber_url = senate_cmte_home
    return scraper_utils.make_static_soup(chamber_url, pat)


def extract_member_role(t):
    s = t.text
    member_role = s.split("(")
    cmte_member_data = {
        "link": t['href'],
        "name": member_role[0].strip()
    }
    if len(member_role) == 1:
        cmte_member_data["assignment_type"] = "Member"
    elif len(member_role) == 2:
        cmte_member_data["assignment_type"] = member_role[1].replace(")", "").strip()
    return cmte_member_data


def get_assembly_cmte_urls():
    source = make_soup("assembly")[0].select("li")
    results = list()
    for i in source:
        cmte_name = i.text.strip()
        cmte_url = i.find("a")["href"]
        cmte_data = {
            "name": cmte_name,
            "webpage_link": cmte_url,
            "chamber_id": 1
        }
        results.append(cmte_data)
    return results


def get_assembly_cmte_members(cmte_url):
    url = cmte_url + "/members"
    pat = "div[id='block-wdscommitteemembershipblock'] > table"
    source = scraper_utils.make_static_soup(url, pat)[0].find_all("tr")[1:]
    results = list()
    for i in source:
        member_role = i.select_one("td > a").text
        cmte_member_data = extract_member_role(member_role)
        results.append(cmte_member_data)
    return results


def get_senate_cmte_urls():
    source = make_soup("senate")[0].find_all("span", class_="field-content")
    results = list()
    for i in source:
        cmte_name = i.text.strip()
        cmte_url = i.find("a")["href"]
        cmte_data = {
            "name": cmte_name,
            "webpage_link": cmte_url,
            "chamber_id": 2
        }
        results.append(cmte_data)
    return results


def get_senate_cmte_members(cmte_url):
    field_pat = "div[class='field-item even']"
    source = scraper_utils.make_static_soup(cmte_url, field_pat)[0]
    results = list()
    header_tags = source.find_all("h2")
    for h in header_tags:
        if h.string == "Members:":
            print(h.next_element.next_element.next_element)
            # the line of code above provides evidence that Senate cmte members are not formatted predictably
            # therefore, it's better to manually insert them into DB
    return results


def main():
    for u in get_senate_cmte_urls():
        print(u)
        print(get_senate_cmte_members(u['webpage_link']))


if __name__ == "__main__":
    main()
