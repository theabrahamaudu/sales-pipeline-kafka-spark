"""
This module defines a Kafka producer class for sending messages to a Kafka topic.

The producer class includes methods for:
1. Initializing the Kafka producer.
2. Producing messages to the specified Kafka topic.

Usage:
    To use this module, create an instance of the producer class and call its `produce` method to start
    sending messages to the Kafka topic.

Example:
    producer_obj = producer(
        config_path='./config',
        local_file_path="./data/orders.csv"
    )
    producer_obj.produce()
"""

from kafka import KafkaProducer
from datetime import datetime
import time
from json import dumps
import pandas as pd
import yaml


class producer:
    """
    This class represents a Kafka producer that sends messages to a Kafka topic.
    
    Attributes:
        topic (str): The name of the Kafka topic to send messages to.
        server (str): The bootstrap server address for the Kafka cluster.
        local_file_path (str): The file path of the local file to be loaded into a Pandas DataFrame.
    
    Methods:
        __init__(self, topic: str, server: str, local_file_path: str): Initializes the Kafka producer.
        produce(self): Produces messages to the Kafka topic.
    """
    def __init__(self, config_path: str, local_file_path: str):
        """
        Initializes an instance of the class.

        Parameters:
            topic (str): The topic to publish the messages to.
            server (str): The Kafka server to connect to.
            local_file_path (str): The path to the local file to load into a pandas dataframe.

        Returns:
            None
        """

        # Load local file to pandas dataframe
        self.orders_df = pd.read_csv(local_file_path)

        # Load config
        with open(config_path+'/config.yaml', 'r') as f:
            config = yaml.safe_load(f.read())
        self.config = config

        # Initialize kafka variables
        self.topic = self.config['kafka']['topic']
        self.server = self.config['kafka']['bootstrap_server']

        # Set producer
        self.producer = KafkaProducer(bootstrap_servers=self.server,
                                      value_serializer=lambda x: dumps(x).encode('utf-8'))
        
    def produce(self):
        """
        Produces messages by printing application start message, printing one row of orders dataframe, 
        converting dataframe to dict, producing messages with 1 second delay in between by printing 
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
            time.sleep(1)

if __name__ == '__main__':
    # Create producer object
    producer_obj = producer(
        config_path='./config',
        local_file_path="./data/orders.csv"
    )
    producer_obj.produce()