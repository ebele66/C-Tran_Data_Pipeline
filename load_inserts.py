#!/usr/bin/env python3
#
# Author: Ebele Esimai

# Purpose:
# Create tables and insert messages from file.

import json
from datetime import date, datetime, timedelta
import argparse
import time
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_batch
import csv


BreadCrumbRows = []
TripRows = set()

parser = argparse.ArgumentParser()
parser.add_argument("-d", "--datafile", required=True)
args = parser.parse_args()
global Datafile
Datafile = args.datafile

# connect to the database
def dbconnect(host, db, user, pw):
    connection = psycopg2.connect(
        host=host,
        database=db,
        user=user,
        password=pw)
    connection.autocommit = True
    return connection

def createTables(conn):

    with conn.cursor() as cursor:
        cursor.execute(f"""
        create type if not exists service_type as enum ('Weekday', 'Saturday', 'Sunday');
        create type if not exists tripdir_type as enum ('Out', 'Back');

        CREATE TABLE IF NOT EXISTS Trip (
            trip_id integer NOT NULL,
            route_id integer,
            vehicle_id integer NOT NULL,
            service_key service_type,
            direction tripdir_type,
            PRIMARY KEY (trip_id)
        );
        CREATE TABLE IF NOT EXISTS BreadCrumb (
            tstamp timestamp NOT NULL,
            latitude float,
            longitude float,
            direction integer,
            speed float,
            trip_id integer NOT NULL,
            FOREIGN KEY (trip_id) REFERENCES Trip
        );
      """)

def transform(row_dict):
    # Repackage values as needed for target table meaning.

    # Set nulls to python none type
    for key in row_dict:
        if not row_dict[key]:
            row_dict[key] = None

    # Generate a timestamp from the date and seconds offset.
    date = datetime.strptime(row_dict['OPD_DATE'],'%d-%b-%y')
    row_dict['TIMESTAMP'] = date + timedelta(seconds = int(row_dict['ACT_TIME']))

    # Overwrite the date in correct format, to prevent need to convert it again.
    row_dict['OPD_DATE'] = date

    # Determine if it is a weekday/weekend schedule from the date
    days_of_week =["Weekday", "Weekday", "Weekday", "Weekday", "Weekday", "Saturday", "Sunday"]
    row_dict['SERVICE_KEY'] = days_of_week[datetime.weekday(date)]

    # Standin to determine the general out/in direction of the trip.
    # TODO set this to populate correctly once this data is available.
    row_dict['TRIP_DIRECTION'] = 'Out'

    # Standin to determine the route id of the trip.
    # TODO set this to populate correctly once this data is available.
    row_dict['ROUTE_ID'] = 0

    return row_dict


def validate_row(row_dict):
    # Do some simple value checking, skip insertion for invalid values.

    # Existence Assertion
    # Each message has a trip ID and timestamp.
    if not row_dict['EVENT_NO_TRIP']:
        return False

    if not row_dict['TIMESTAMP']:
        return False

    # Limit Assertions
    # Breadcrumb Direction = ''  or between 0 and 359.
    if row_dict['DIRECTION']:
        int_direction = int(row_dict['DIRECTION'])
        if int_direction > 359 or int_direction < 0:
            return False

    # Speed = '' or between 0 and 200.
    if row_dict['VELOCITY']:
        int_speed = int(row_dict['VELOCITY'])
        if int_speed > 200 or int_speed < 0:
            return False

    # Intra-record Assertion
    # Generated timestamp less than 48 hours after date.
    if (row_dict['TIMESTAMP'] - row_dict['OPD_DATE']) > timedelta(days = 2):
        return False

    return True

def store(row_dict):
    # Prep the data for insertion in batch.

    global BreadCrumbRows
    global TripRows

    BreadCrumbRows.append(
        (
            row_dict['TIMESTAMP'],
            row_dict['GPS_LATITUDE'],
            row_dict['GPS_LONGITUDE'],
            row_dict['DIRECTION'],
            row_dict['VELOCITY'],
            row_dict['EVENT_NO_TRIP']
        )
    )

    TripRows.add(
        (
            row_dict['EVENT_NO_TRIP'],
            row_dict['ROUTE_ID'],
            row_dict['VEHICLE_ID'],
            row_dict['SERVICE_KEY'],
            row_dict['TRIP_DIRECTION']
        )
    )
    return

def insert(conn):
    global BreadCrumbRows
    global TripRows

    # Convert the set of unique trips to a list for execute_batch
    UniqueTrips = []
    for element in TripRows:
        UniqueTrips.append(element)

    with conn.cursor() as cursor:
        bread_cmd = sql.SQL("INSERT INTO BreadCrumb VALUES (%s,%s,%s,%s,%s,%s) on conflict do nothing;")
        trip_cmd = sql.SQL("INSERT INTO Trip VALUES (%s,%s,%s,%s,%s) on conflict do nothing;")
        execute_batch(cursor,trip_cmd,TripRows)
        execute_batch(cursor,bread_cmd,BreadCrumbRows)

    # Clear the inserted values
    BreadCrumbRows = []
    TripRows = set()


if __name__ == '__main__':

   # args = initialize()
    topic = "datapipe"
    host = "localhost"
    db = "project"
    user = "crumb"
    pw = "bread"

    conn = dbconnect(host,db,user,pw)
    with open(Datafile, mode="r") as fil:
        rowlist = json.load(fil)
        for row in rowlist:
            row_dict = transform(row)
            # Check if the row is valid
            if validate_row(row_dict):
                store(row_dict)
            if len(BreadCrumbRows) >= 10000:
                insert(conn)
        if len(BreadCrumbRows) >0:
            insert(conn)
