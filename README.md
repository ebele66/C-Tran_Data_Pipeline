# C-Tran_Data_Pipeline
This project explores building and maintaining data pipelines with c-tran transit data for analysis and visualization of transit patterns and hotspots.

See Project 4.pdf for full report.

### Listing of files and their use

#### Data Pipeline
1. producer.py ---   produces messages from the breadcrumb server to a Kafka topic
2. file_consumer.py --- consumes messages from a Kafka topic and loads it into a file
3. topic_consumer.py --- consumes messages from a Kafka topic and loads it into database tables
4. stop_producer.py --- produces messages from the stop events server to a Kafka topic
5. stop_consumer.py --- consumes messages from a Kafka topic and updates the database tables

#### Utility
1. load_inserts.py --- create database tables and insert records from file
2. update_inserts.py --- update data in the tables from file
3. tsvscript.py --- visualization driver
