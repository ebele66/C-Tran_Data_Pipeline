#!/usr/bin/env python3

#
# Author: Ebele Esimai

# Purpose:
# Produce messages to a Kafka topic from the stop_events web server.

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
# Produce messages to Confluent Cloud
# Using Confluent Python Client for Apache Kafka
#
# =============================================================================

from confluent_kafka import Producer, KafkaError
import json
import ccloud_lib
import requests
from bs4 import BeautifulSoup
from urllib.request import urlopen
import ssl


if __name__ == '__main__':

    # Read arguments and configurations and initialize
    args = ccloud_lib.parse_args()
    config_file = args.config_file
    topic = args.topic
    conf = ccloud_lib.read_ccloud_config(config_file)

    # Create Producer instance
    producer = Producer({
        'bootstrap.servers': conf['bootstrap.servers'],
        'sasl.mechanisms': conf['sasl.mechanisms'],
        'security.protocol': conf['security.protocol'],
        'sasl.username': conf['sasl.username'],
        'sasl.password': conf['sasl.password'],
    })

    # Create topic if needed
    ccloud_lib.create_topic(conf, topic)

    delivered_records = 0

    # Optional per-message on_delivery handler (triggered by poll() or flush())
    # when a message has been successfully delivered or
    # permanently failed delivery (after retries).
    def acked(err, msg):
        global delivered_records
        """Delivery report handler called on
        successful or failed delivery of message
        """
        if err is not None:
            print("Failed to deliver message: {}".format(err))
        else:
            delivered_records += 1
            print("Produced record to topic {} partition [{}] @ offset {}"
                  .format(msg.topic(), msg.partition(), msg.offset()))
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

    producer.flush()
    # import data from web server
    url = "http://rbi.ddns.net/getStopEvents"
    soup = BeautifulSoup(urlopen(url), 'lxml')
    tables = soup.find_all('table')
    trip_ids = soup.find_all('h3')
    i = 0
    record_key = "stop_event"

    for i in range(len(trip_ids)):
        crumb = parse_table(tables[i], trip_ids[i])
        record_value = json.dumps(crumb)
        #print("Producing record: {}\t{}".format(record_key, record_value))
        producer.produce(topic, key=record_key, value=record_value, on_delivery=acked)
        # p.poll() serves delivery reports (on_delivery)
        # from previous produce() calls.
        if i % 2000 == 0 : 
            producer.flush()
        else:
            producer.poll(0)
        i += 1

    producer.flush()
f = open("/home/esimai/data/msg.log", 'a')
f.write("{} stop messages were produced to topic {}!\n".format(i, topic))
f.close()
print(i, "produced records")
print("done!")

