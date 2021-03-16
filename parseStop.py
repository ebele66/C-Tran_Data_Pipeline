#!/usr/bin/env python3
#
# Author: Ebele Esimai

# Purpose:
# Utility. Parse stop events from HTML file into a list of json objects in a file.


import json
import pandas as pd
from bs4 import BeautifulSoup
def parse_table(table, trip_id):
    lst = trip_id.get_text().split()
    t_id = lst[4]
    column_names = []
    rows = table.find_all('tr')
    for th in rows[0].find_all('th'):
        column_names.append(th.get_text())

    columns = rows[1].find_all('td')
    r = {'trip_id': t_id}
    for i in range(len(column_names)):
        r[column_names[i]] = columns[i].get_text()

    return r
# response = requests.get("http://rbi.ddns.net/getStopEvents")
file = open("2021-03-04_getStopEvents.html", "r")
soup = BeautifulSoup(file, 'lxml')
tables = soup.find_all('table')
trip_ids = soup.find_all('h3')
stop_data = []
for i in range(len(trip_ids)):
    stop_data.append(parse_table(tables[i], trip_ids[i]))
df = pd.DataFrame(stop_data)
outf = open("data/stop2021-03-04.json", "w")
json.dump(stop_data,outf)
