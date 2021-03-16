#!/usr/bin/env python3

#
# Author: Ebele Esimai

# Purpose:
# Updates tables with stop_events messages from file.

import time
import psycopg2
import argparse
import re
import json
import pandas as pd

DBname = "project"
DBuser = "crumb"
DBpwd = "bread"

# Datafile = "filedoesnotexist"  # name of the data file to be loaded
CreateDB = False  # indicates whether the DB table should be (re)-created

# connect to the database
def dbconnect():
    connection = psycopg2.connect(
        host="localhost",
        database=DBname,
        user=DBuser,
        password=DBpwd,
    )
    connection.autocommit = True
    return connection

def initialize():

    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--datafile", required=True)
    args = parser.parse_args()

    global Datafile
    Datafile = args.datafile
 

# read the input data file into a list of row strings
# skip the header row
def readdata(fname):
    print(f"readdata: reading from File: {fname}")
    with open(fname, mode="r") as fil:
        rowlist = json.load(fil)
    return rowlist

def row2vals(row):
    # handle the null vals
    txtDF = pd.Series(row)
    dir_id = txtDF["direction"]
    if dir_id == "0" :
        dir = "Out"
    elif dir_id == "1":
        dir = "Back"
    else:
        dir = ""
    d = txtDF["service_key"]
    if d == "W":
        service_type = "Weekday"
    elif d == "S":
        service_type = "Saturday"
    else:
        service_type = "Sunday"
    stopDF = [int(txtDF["trip_id"]), int(txtDF["vehicle_number"]), txtDF["route_number"], dir, service_type]
    return stopDF

def getSQLcmnds(rows):
    cmdlist = []
    trip_ids = []
    for row in rows:
        stopDF = row2vals(row)
        if stopDF[0] not in trip_ids:
            trip_ids.append(stopDF[0])
            if stopDF[3] == '':
               cmd = f"UPDATE Trip SET route_id = '{stopDF[2]}', direction ='Out'  WHERE trip_id = '{stopDF[0]}' AND vehicle_id = '{stopDF[1]}' AND service_key = '{stopDF[4]}'; "
            else:
                cmd = f"UPDATE Trip SET route_id = '{stopDF[2]}', direction = '{stopDF[3]}' WHERE trip_id = '{stopDF[0]}' AND vehicle_id = '{stopDF[1]}' AND service_key = '{stopDF[4]}';"
            cmdlist.append(cmd)
    return cmdlist


def load(conn, icmdlist):

    with conn.cursor() as cursor:
        print(f"Loading {len(icmdlist)} rows")
        start = time.perf_counter()
    
        for cmd in icmdlist:
            # print (cmd)
            cursor.execute(cmd)

        elapsed = time.perf_counter() - start
        print(f'Finished Loading. Elapsed Time: {elapsed:0.4} seconds')


def main():
    initialize()
    conn = dbconnect()
    rlis = readdata(Datafile)
    cmdlist = getSQLcmnds(rlis)

    load(conn, cmdlist)


if __name__ == "__main__":
    main()




