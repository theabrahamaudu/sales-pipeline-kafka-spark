from kafka import KafkaProducer
from datetime import datetime
import time
from json import dumps
import random
import pandas as pd


KAFKA_TOPIC_NAME_CONS = "orderstopicdemo"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'

class producer:
    def __init__(self, topic: str, server: str, local_file_path: str):

        # Load local file to pandas dataframe
        self.orders_df = pd.read_csv(local_file_path)

        # Set topic and server
        self.topic = topic
        self.server = server

        # Set producer
        self.producer = KafkaProducer(bootstrap_servers=server,
                                      value_serializer=lambda x: dumps(x).encode('utf-8'))
        
    def produce(self):
        """
        Produces messages by printing application start message, printing one row of orders dataframe, 
        converting dataframe to dict, producing messages with 2 seconds delay in between by printing 
        the message to be sent to kafka and sending message.
        """

        # Print application start message
        print("Producer Application started at: " + str(datetime.now()))

        # Print one row of orders dataframe
        print(self.orders_df.head(1))
        
        # Convert dataframe to dict
        self.orders_dict = self.orders_df.to_dict('records')

        # Produce messages with 2 seconds delay in between
        for order in self.orders_dict:
            # Print message to be sent to kafka
            print("Message to be sent: " + str(order))

            # Send message
            self.producer.send(self.topic, value=order)
            time.sleep(2)

if __name__ == '__main__':
    # Create producer object
    producer_obj = producer(topic=KAFKA_TOPIC_NAME_CONS,
                            server=KAFKA_BOOTSTRAP_SERVERS_CONS,
                            local_file_path="./data/orders.csv")
    producer_obj.produce()