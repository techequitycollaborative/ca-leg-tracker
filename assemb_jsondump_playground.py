import html_to_json
import json
import urllib.request

def get_html():
    assemb_df = "https://www.assembly.ca.gov/dailyfile"
    df_html = urllib.request.urlopen(assemb_df).read()
    return df_html

def pretty_print():
    output_json = html_to_json.convert(get_html())
    print(output_json)
    # fname = 'test.json'
    # with open(fname, mode='w+') as f:
    #     # f.write(json.dumps(output_json, indent=4))

if __name__ == "__main__":
    pretty_print()
