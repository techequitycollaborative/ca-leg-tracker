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
        pat = "div[id='block-view-committees-standing'] > div[class='content'] > div > div[class='view-content']"
        chamber_url = senate_cmte_home
    return scraper_utils.make_static_soup(chamber_url, pat)


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
        member_role = i.select_one("td > a").text.split("(")
        cmte_member_data = None
        if len(member_role) == 1:
            cmte_member_data = {
                "name": member_role[0].strip(),
                "assignment_type": "Member"
            }
        elif len(member_role) == 2:
            cmte_member_data = {
                "name": member_role[0].strip(),
                "assignment_type": member_role[1].replace(")", "").strip()
            }
        results.append(cmte_member_data)
    return results


def main():
    for _ in get_assembly_cmte_urls():
        print(_)


if __name__ == "__main__":
    main()
