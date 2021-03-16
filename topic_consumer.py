#!/usr/bin/env python3
#
# Author: Genevieve LaLonde
# Modified by Ebele Esimai

# Purpose:
# Consume messages from kafka, validate, and insert to postgres database.

# With thanks to Apache Confluent Kafka Client Examples
# URL: https://github.com/confluentinc/examples
#
# Copyright 2020 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# =============================================================================
#
# Consume messages from Confluent Cloud
# Using Confluent Python Client for Apache Kafka
#
# =============================================================================

from confluent_kafka import Consumer, KafkaError
import json
import ccloud_lib
from datetime import date, datetime, timedelta
import argparse
import pandas as pd
import time
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_batch
import re
import csv
import os

BreadCrumbRows = []
TripRows = set()

def initialize():

    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--config-file", 
        dest="config_file", 
        help="The path to the Confluent Cloud configuration file")
    parser.add_argument("-t", "--topic-name",
        dest="topic",
        help="topic name",
        required=True)
    parser.add_argument("-c", "--create-tables", 
        dest="create_tables",
        default='False',
        help="Creates the tables if they don't exist yet, without overwriting any existing table.",
        action="store_true")
    args = parser.parse_args()
    return args

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
    print(f"Created tables")

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
    with conn.cursor() as cursor:
        bread_cmd = sql.SQL("INSERT INTO BreadCrumb VALUES (%s,%s,%s,%s,%s,%s);")
        trip_cmd = sql.SQL("INSERT INTO Trip VALUES (%s,%s,%s,%s,%s) on conflict do nothing;")
        execute_batch(cursor,trip_cmd,TripRows)
        execute_batch(cursor,bread_cmd,BreadCrumbRows)

    # Clear the inserted values
    BreadCrumbRows = []
    TripRows = set()

def consume(conf,topic,conn):
        # Create Consumer instance
    # 'auto.offset.reset=earliest' to start reading from the beginning of the
    #   topic if no committed offsets exist
    consumer = Consumer({
        'bootstrap.servers': conf['bootstrap.servers'],
        'sasl.mechanisms': conf['sasl.mechanisms'],
        'security.protocol': conf['security.protocol'],
        'sasl.username': conf['sasl.username'],
        'sasl.password': conf['sasl.password'],
        'group.id': 'python_example_group_1',
        'auto.offset.reset': 'earliest',
    })
    # Subscribe to topic
    consumer.subscribe([topic])

    # Consume by inserting to postgres.
    consumed_messages = 0
    fail_count = 0
    skipped_rows = 0
    inserted_bread_rows = 0
    inserted_trip_rows = 0
    try:
        while True:
            msg = consumer.poll(1.0)
            # Check for message
            if msg is None:
                if fail_count >= 5:
                    # Insert records from the final batch.
                    inserted_bread_rows += len(BreadCrumbRows)
                    inserted_trip_rows += len(TripRows)
                    insert(conn)
                    f = open("/home/esimai/data/msg.log", 'a')
                    f.write("{} inserted bread_rows and {} inserted trip_rows\n".format(inserted_bread_rows, inserted_trip_rows))
                    f.close()
                    print("No new messages in 5 seconds, closing.")
                    break
                # No message available within timeout.
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                print("Waiting for message or event/error in poll()")
                fail_count += 1
                continue
            elif msg.error():
                print('error: {}'.format(msg.error()))
            else:
                # Reset if new messages were received.
                fail_count = 0 

                # Parse Kafka message
                record_key = msg.key()
                record_value = msg.value()
                # Each row of breadcrumb data is one record value.
                row_dict = json.loads(record_value)
                row_dict = transform(row_dict)
                # Check if the row is valid
                if validate_row(row_dict):
                    store(row_dict)
                else:
                    skipped_rows += 1

                # Insert in batches of up to 10k rows.
                if len(BreadCrumbRows) >= 10000:
                    inserted_bread_rows += len(BreadCrumbRows)
                    inserted_trip_rows += len(TripRows)
                    insert(conn)

                consumed_messages += 1

    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()
        print(f"Consumed {consumed_messages} messages.")
        print(f"Inserted {inserted_bread_rows} rows to `{db}.BreadCrumb`.")
        print(f"Inserted {inserted_trip_rows} unique rows to `{db}.Trip`.")
        print(f"Skipped {skipped_rows} messages due to data validation.")

if __name__ == '__main__':

    args = initialize()
    topic = args.topic
    host = "localhost"
    db = "project"
    user = "crumb"
    pw = "bread"
    create_tables = args.create_tables
    config_file = args.config_file

    conf = ccloud_lib.read_ccloud_config(config_file)
    conn = dbconnect(host,db,user,pw)


    #if create_tables:
      #createTables(conn)
    
    consume(conf,topic,conn)
