import pandas as pd
import requests
from bs4 import BeautifulSoup
import numpy as np
from tqdm import tqdm
import json # for saving dictionaries

dataPath = "./"

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
lookup_2020 = {}
for i in tqdm(range(df_2020.shape[0])):
    lookup_2020[df_2020.loc[i, 'Name']] = df_2020.loc[i, 'Label']

# save the dictionaries
with open('./variable_summary.json', 'w') as f:
     json.dump(lookup_2020, f)

# define variable lists:
# use list comprehension

vars_20 = [var if(var != "for" and var != "in" and var != "ucgid") else "" for var in df_2020.loc[:, 'Name']]

# filter out the "" from the for and in names
vars_20 = list(filter(None, vars_20))

# generate data for 2020
# ------------------------------------------------------------------------------------------------------------
HOST = "https://api.census.gov/data"
year = "2020"
dataset = "dec/pl"
base_url = "/".join([HOST, year, dataset])


print("Generating data for ", base_url)

# create request dictionary
predicates = {}
predicates["key"] = "97d54f0b2bec96ce45749d21afa620949a6cd273"
predicates["for"] = "tract"
predicates["in"] = "state:48;county:*"

# generate dictionary with state codes
# http://mcdc.missouri.edu/applications/geocodes/?state=00
states = {"01": "Alabama",
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


# ------------------------------------------------------------------------------------------------------------
# loop over states to generate the data
for key, value in states.items():
    tqdm.write("Working on {}".format(value))
    predicates = {}
    predicates["key"] = "97d54f0b2bec96ce45749d21afa620949a6cd273"
    predicates["for"] = "tract"
    predicates["in"] = "state:{};county:*".format(key)

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
        # print(df.columns)
        # exit()
        #df_state = pd.concat([df_state, df], join="outer", axis=1)
        for col in df.columns:
            if col not in df_state.columns:
                # df_state[col] = df[col]
                df_state = pd.concat([df_state, df[col]], axis=1)

    df_state.to_csv("./{}_census.csv".format(value))


