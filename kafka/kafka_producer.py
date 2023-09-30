from kafka import KafkaProducer
from datetime import datetime
import time
from json import dumps
import random

KAFKA_TOPIC_NAME_CONS = "testmsg"
KAFKA_BOOTSTRAP_SERVERS_CONS = '34.70.106.130:9092'