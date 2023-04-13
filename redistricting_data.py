import json # for saving dictionaries
from bs4 import BeautifulSoup
from tqdm import tqdm
import numpy as np
import pandas as pd
import requests
import os

def make_folder(folder_name):
    folder_name = "./" + folder_name
    if not os.path.exists(folder_name):
        os.mkdir(folder_name)

def get_variables():
    urls = ["https://api.census.gov/data/2020/dec/pl/variables.html"]

    # scrape the webpages for all available variables
    src_2020 = requests.get(urls[0]).content
    print("Websites fetched!")

    soup_2020 = BeautifulSoup(src_2020, 'lxml')
    print("soupified...")

    # grab all of the variables from the table and turn them into dataframes
    table_2020 = soup_2020.find_all('table')

    df_2020 = pd.read_html(str(table_2020))[0]
    print("DataFrames created...")

    # create variable lookup dictionary to be used later
    lookup_2020 = {df_2020.loc[i, 'Name']: df_2020.loc[i, 'Label'] for i in tqdm(range(df_2020.shape[0]))}

    # save the dictionaries
    with open('./variable_summary.json', 'w') as f:
         json.dump(lookup_2020, f)

    # define variable lists:
    # use list comprehension
    vars_20 = [var for var in df_2020.loc[:, 'Name'] if var not in ("for", "in", "ucgid")]

    HOST = "https://api.census.gov/data"
    year = "2020"
    dataset = "dec/pl"
    base_url = "/".join([HOST, year, dataset])
    print("Generating data for ", base_url)

    return vars_20, base_url

def generate_data(vars_20, base_url, predicates):
    N = len(vars_20)
    slices = list(np.arange(0, N, 50))
    slices.append(N-1)

    df_state = pd.DataFrame()
    for i in tqdm(range(1, len(slices))):
        get_vars = vars_20[slices[i-1]:slices[i]]
        predicates["get"] = ",".join(get_vars)

        r = requests.get(base_url, params=predicates)
        if(r.status_code!= 200):
            print("Error retrieving data: {}".format(r.status_code))

        names = r.json()[0]
        data = r.json()[1:]
        df = pd.DataFrame(data=data, columns=names)
        for col in df.columns:
            if col not in df_state.columns:
                df_state = pd.concat([df_state, df[col]], axis=1)
    return df_state

def broad_hierarchy(vars_20, base_url, p):
    tqdm.write(f"Working on {value}")

    predicates = {
        "key": "97d54f0b2bec96ce45749d21afa620949a6cd273",
        "for": p[1],
    }

    print(predicates["key"], predicates["for"])
    df_state = generate_data(vars_20, base_url, predicates)

    make_folder(p[0])
    # csv_dir = p[0] + "/{}_census.csv"
    # df_state.to_csv(csv_dir.format(p[0]))
    json_dir = p[0] + "/{}_census.json"
    df_state.to_json(json_dir.format(p[0]))


def specific_hierarchy(vars_20, base_url, p, states):
    for key, value in states.items():
        tqdm.write(f"Working on {value}")

        predicates = {
            "key": "97d54f0b2bec96ce45749d21afa620949a6cd273",
            "for": p[1],
            "in": p[2].format(key) if "state:{}" in p[2] else p[2],
        }

        print(predicates["key"], predicates["for"], predicates["in"])
        df_state = generate_data(vars_20, base_url, predicates)

        make_folder(p[0])
        # csv_dir = p[0] + "/{}_census.csv"
        # df_state.to_csv(csv_dir.format(value))
        json_dir = p[0] + "/{}_census.json"
        df_state.to_json(json_dir.format(value))

# generate dictionary with state codes
# http://mcdc.missouri.edu/applications/geocodes/?state=00
states = {
    "01": "Alabama",
    "02": "Alaska ",
    "04": "Arizona",
    "05": "Arkansas",
    "06": "California",
    "08": "Colorado",
    "09": "Connecticut",
    "10": "Delaware",
    "11": "District of Columbia",
    "12": "Florida",
    "13": "Georgia",
    "15": "Hawaii",
    "16": "Idaho",
    "17": "Illinois",
    "18": "Indiana",
    "19": "Iowa",
    "20": "Kansas",
    "21": "Kentucky",
    "22": "Louisiana",
    "23": "Maine",
    "24": "Maryland",
    "25": "Massachusetts",
    "26": "Michigan",
    "27": "Minnesota",
    "28": "Mississippi",
    "29": "Missouri",
    "30": "Montana",
    "31": "Nebraska",
    "32": "Nevada",
    "33": "New Hampshire",
    "34": "New Jersey",
    "35": "New Mexico",
    "36": "New York",
    "37": "North Carolina",
    "38": "North Dakota",
    "39": "Ohio",
    "40": "Oklahoma",
    "41": "Oregon",
    "42": "Pennsylvania",
    "72": "Puerto Rico",
    "44": "Rhode Island",
    "45": "South Carolina",
    "46": "South Dakota",
    "47": "Tennessee",
    "48": "Texas",
    "49": "Utah",
    "50": "Vermont",
    "51": "Virginia",
    "53": "Washington",
    "54": "West Virginia",
    "55": "Wisconsin",
    "56": "Wyoming"
}

# setup_folders()
vars_20, base_url = get_variables()

# each key has a tuple: (name, for, in)
predicates = {
    "010": ("us", "us:*"),
    "020": ("region", "region:*"),
    "030": ("division", "division:*"), 
    "040": ("state", "state:*"), 
    "050": ("state-county", "county:*", "state:{}"),
    "060": ("state-county-county_subdivision", "county subdivision:*", "state:{}"),
    "100": ("state-county-tract-block", "block:*", "state:{};county:*"),
    "140": ("state-county-tract", "tract:*", "state:"),
    "150": ("state-county-tract-block_group", "block group:*", "state:{};county:*;tract:*"), 
    "155": ("state-place-county", "county:*", "state:{};place:*"),
    "160": ("state-place", "place:*", "state:{}"),
    "170": ("state-consolidated_city", "consolidated city:*", "state:{}"),
    "500": ("state-congressional_district", "congressional district:*", "state:{}"),
    "610": ("state-state_legislative_district", "state legislative district:*", "state:{}"),
    "620": ("state-state_legislative_district-county", "state legislative district:*", "state:{}"),
    "700": ("state-county-voting_district", "voting district:*", "state:{};county:*"),
    "709": ("state-county-voting_district-tract", "tract:*", "state:{};county:*;voting district:*"),
    "950": ("state-school_district_elementary", "school district (elementary):*", "state:{}"),
    "960": ("state-school_district_secondary", "school district (secondary):*", "state:{}"),
    "970": ("state-school_district_unified", "school district (unified):*", "state:{}"),
}


for key, value in predicates.items():
    if "state-" in value[0]:
        specific_hierarchy(vars_20, base_url, value, states)
    else:
         broad_hierarchy(vars_20, base_url, value)