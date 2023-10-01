#!/bin/bash

# run >>> chmod +x create-topic.sh <<< before running this script
sudo systemctl start kafka
kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic orderstopicdemo
