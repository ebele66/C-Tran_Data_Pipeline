#!/usr/bin/python3

#
# Modified by Ebele Esimai

# Purpose:
# Visulaization drvier. 

import csv, json
import psycopg2
from geojson import Feature, FeatureCollection, Point

DBname = "project"
DBuser = "crumb"
DBpwd = "bread"

def dbconnect():
    connection = psycopg2.connect(
        host="localhost",
        database=DBname,
        user=DBuser,
        password=DBpwd,
    )
    connection.autocommit = True
    return connection

features = []
conn = dbconnect()
cmd = f"select latitude ||' '||longitude, avg(speed) from BreadCrumb b join trip t on b.trip_id = t.trip_id where t.vehicle_id = 4008 and t.route_id =65 and t.direction='Out' and date_part('month',b.tstamp) = 10 and date_part('day',b.tstamp) = 18 and date_part('hour',b.tstamp) between 9 and 11 group by 1;"
with conn.cursor() as cursor:
    cursor.execute(cmd)
    result = cursor.fetchall()
#    print(result)
    for row in result:
                
        # Uncomment these lines
        lati, longi = row[0].split()
        #longi = row[2]
        speed = row[1]

        # skip the rows where speed is missing
        if speed is None or speed == "":
            continue
     	
        try:
            latitude, longitude = map(float, (lati,longi))
            features.append(
                Feature(
                    geometry = Point((longitude,latitude)),
                    properties = {
                        'speed': (int(speed))
                    }
                )
            )
        except ValueError:
            continue

collection = FeatureCollection(features)
with open("data.geojson", "w") as f:
    f.write('%s' % collection)
