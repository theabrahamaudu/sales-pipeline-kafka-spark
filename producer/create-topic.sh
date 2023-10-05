#!/bin/bash

# run >>> chmod +x create-topic.sh <<< before running this script
# this script will start a kafka cluster and create a topic called orderstopicdemo
sudo systemctl start kafka
kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic orderstopicdemo
