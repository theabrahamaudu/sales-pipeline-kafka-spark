from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time
import yaml
import pandas as pd
from dotenv import load_dotenv
import os

with open('config/config.yaml', 'r') as f:
    config = yaml.safe_load(f.read())

class streamHandler:
    def __init__(self, config_path: str, customer_data_path: str):

        # Load config
        with open(config_path+'/config.yaml', 'r') as f:
            config = yaml.safe_load(f.read())
        self.config = config

        # Load customer data
        self.customer_data = pd.read_csv(customer_data_path)

        # Initialize kafka variables
        self.topic = self.config['kafka']['topic']
        self.server = self.config['kafka']['bootstrap_server']

        # Initialize mysql variables
        load_dotenv(dotenv_path=config_path+'/.env')
        self.mysql_host = self.config['mysql']['host']
        self.mysql_port = self.config['mysql']['port']
        self.mysql_driver = self.config['mysql']['driver']
        self.mysql_database = self.config['mysql']['database']
        self.mysql_table = self.config['mysql']['table']
        self.mysql_username = os.getenv('MYSQL_USERNAME')
        self.mysql_password = os.getenv('MYSQL_PASSWORD')
        self.mysql_jdbc_url = f"jdbc:mysql://{self.mysql_host}:{self.mysql_port}/{self.mysql_database}"

        # Initialize cassandra variables
        self.cassandra_host = self.config['cassandra']['host']
        self.cassandra_port = self.config['cassandra']['port']
        self.cassandra_keyspace = self.config['cassandra']['keyspace']
        self.cassandra_table = self.config['cassandra']['table']


    def startSparkSession(self):
        spark = SparkSession \
            .builder \
            .appName("Pyspark Structured Streaming w/ Kafka-Cassandra-MySQL") \
            .master("local[*]") \
            .config("spark.some.config.option", "some-value") \
            .getOrCreate()
        return spark


    def saveToCassandra(self, current_df, epoch_id):
        # Print current epoch_id
        print(f"Current epoch_id:\n {epoch_id}")

        # Save current dataframe to cassandra
        current_df.write\
            .format("org.apache.spark.sql.cassandra")\
            .options(table=self.cassandra_table, keyspace=self.cassandra_keyspace)\
            .mode("append")\
            .save()


