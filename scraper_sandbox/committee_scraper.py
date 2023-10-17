from scraper_utils import make_static_soup

# scrape all committees by type as shown on committee homepage >> dict(cmte_type, [(cmte_name, cmte_link1),...]}
# committee table: transform each committee name-link tuple for table insertion
# for each committee link, scrape for "XYZ Members" title >> IF exists, pull names ELSE + "/membersstaff" to URL to pull
assembly_cmte_home = "https://www.assembly.ca.gov/committees"
senate_cmte_home = "https://www.senate.ca.gov/committees"