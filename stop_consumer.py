#!/usr/bin/env python3
#
# Author: Ebele Esimai

# Purpose:
# Consume stop_event messages from Kafka topic and update tables in the database.


from confluent_kafka import Consumer
import json
import pandas as pd
import ccloud_lib
from datetime import datetime
import time
import psycopg2
import argparse
import re

DBname = "project"
DBuser = "crumb"
DBpwd = "bread"

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

def load(conn, icmdlist):
    with conn.cursor() as cursor:
        print(f"Loading {len(icmdlist)} rows")
        start = time.perf_counter()

        for cmd in icmdlist:
            # print (cmd)
            cursor.execute(cmd)

        elapsed = time.perf_counter() - start
        print(f'Finished Loading. Elapsed Time: {elapsed:0.4} seconds')

# transform input data and prepare values for tables
def row2vals(row):
    # handle the null vals
    txtDF = pd.Series(row)
    dir_id = txtDF["direction"]
    if dir_id == "0":
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

# convert list of data rows into list of SQL 'UPDATE ...' commands
def getSQLcmnds(rows):
    cmdlist = []
    trip_ids = []
    for row in rows:
        stopDF = row2vals(row)
        if stopDF[0] not in trip_ids:
            trip_ids.append(stopDF[0])
            if stopDF[3] == '':
               cmd = f"UPDATE Trip SET route_id = '{stopDF[2]}', direction = 'Out' WHERE trip_id = '{stopDF[0]}' AND vehicle_id = '{stopDF[1]}' AND service_key = '{stopDF[4]}'; "
            else:
                cmd = f"UPDATE Trip SET route_id = '{stopDF[2]}', direction = '{stopDF[3]}' WHERE trip_id = '{stopDF[0]}' AND vehicle_id = '{stopDF[1]}' AND service_key = '{stopDF[4]}';"
            cmdlist.append(cmd)
    return cmdlist

if __name__ == '__main__':

    # Read arguments and configurations and initialize
    args = ccloud_lib.parse_args()
    config_file = args.config_file
    topic = args.topic
    conf = ccloud_lib.read_ccloud_config(config_file)
    CreateDB = False

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
    conn = dbconnect()

    # Process messages
    total_count = 0
    fail_count = 0
    read_data = []
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                # No message available within timeout.
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                print("Waiting for message or event/error in poll()")
                if len(read_data) > 0:
                  cmdlist = getSQLcmnds(read_data)
                  read_data = []
                  f = open("/home/esimai/data/msg.log", 'a')
                  f.write("{} records consumed\n".format(total_count))
                  f.close()
                  #print(total_count, "records consumed")
                  load(conn, cmdlist)
                if fail_count >= 5:
                    break
                fail_count += 1
                continue
            elif msg.error():
                print('error: {}'.format(msg.error()))
            else:
                fail_count = 0
                # Check for Kafka message
                data_key = msg.key()
                data_value = msg.value()
                if(data_key is not None and data_value is not None):
                    data = json.loads(data_value)
                    read_data.append(data)

                #increase breadcrumb count
                total_count += 1
         
                # print("Consumed record with key {} and value {}, \
                #       and updated total count to {}"
                #       .format(data_key, data_value, total_count))

    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()
