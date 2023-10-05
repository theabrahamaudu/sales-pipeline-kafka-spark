from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time
import yaml
import pandas as pd
from dotenv import load_dotenv
import os


class streamHandler:
    def __init__(self, config_path: str, customer_data_path: str):

        # Load config
        with open(config_path+'/config.yaml', 'r') as f:
            config = yaml.safe_load(f.read())
        self.config = config

        # Define schema for orders dataframe
        self.orders_schema = StructType() \
            .add("order_id", StringType()) \
            .add("created_at", StringType()) \
            .add("discount", StringType()) \
            .add("product_id", StringType()) \
            .add("quantity", StringType()) \
            .add("subtotal", StringType()) \
            .add("tax", StringType()) \
            .add("total", StringType()) \
            .add("customer_id", StringType())

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

        # Start Spark session
        JARS_PATH = "file:///home/abraham-pc/Documents/personal_projects/pyspark/lib/jsr166e-1.1.0.jar," \
            "file:///home/abraham-pc/Documents/personal_projects/pyspark/lib/spark-cassandra-connector-2.4.0-s_2.11.jar," \
            "file:///home/abraham-pc/Documents/personal_projects/pyspark/lib/mysql-connector-java-8.0.30.jar," \
            "file:///home/abraham-pc/Documents/personal_projects/pyspark/lib/spark-sql-kafka-0-10_2.12-3.3.0.jar," \
            "file:///home/abraham-pc/Documents/personal_projects/pyspark/lib/kafka-clients-3.5.1.jar," \
            "file:///home/abraham-pc/Documents/personal_projects/pyspark/lib/spark-streaming-kafka-0-10-assembly_2.12-3.3.3.jar" 

        self.spark = SparkSession \
            .builder \
            .appName("Pyspark Structured Streaming w/ Kafka-Cassandra-MySQL") \
            .master("local[*]") \
            .config("spark.jars", JARS_PATH) \
            .config("spark.executor.extraClassPath", JARS_PATH) \
            .config("spark.executor.extraLibrary", JARS_PATH) \
            .config("spark.driver.extraClassPath", JARS_PATH) \
            .config("spark.cassandra.connection.host", self.cassandra_host) \
            .config("spark.cassandra.connection.port", self.cassandra_port) \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel('ERROR')

        # Load customer data
        self.customer_data = self.spark.read.csv(customer_data_path,
                                                 header=True,
                                                 inferSchema=True)


    def readFromKafka(self, offset_start='latest'):
        # Read from kafka
        orders_df = self.spark.readStream\
            .format("kafka")\
            .option("kafka.bootstrap.servers", self.server)\
            .option("subscribe", self.topic)\
            .option("startingOffsets", offset_start)\
            .load()
        
        # Print schema of dataframe
        print("Orders dataframe schema: ")
        orders_df.printSchema()

        return orders_df
    

    def transformOrdersData(self, orders_df):
        # Transform orders dataframe to include timestamp
        orders_df1 = orders_df\
            .selectExpr("CAST(value AS STRING)", "timestamp")
        
        orders_df2 = orders_df1\
            .select(from_json(col("value"), self.orders_schema).alias("orders"), "timestamp")\
        
        orders_df3 = orders_df2\
            .select("orders.*", "timestamp")
        
        return orders_df3
    

    def aggregateData(self, orders_df3):
        # print schema of customer dataframe
        print("Customer dataframe schema: ")
        self.customer_data.printSchema()

        # Join customer dataframe to orders dataframe by customer_id
        orders_df4 = orders_df3\
            .join(self.customer_data, orders_df3.customer_id == self.customer_data.ID, 'inner')
        
        # find total_sum_amount by grouping source, state
        orders_df5 = orders_df4.groupBy("source", "state")\
            .agg({"total": "sum"}).select("source", "state", col("sum(total)").alias("total_sum_amount"))
        
        # print aggregated dataframe schema
        print("Aggregated dataframe schema: ")
        orders_df5.printSchema()

        # write data to console for debugging
        orders_df5.writeStream\
            .trigger(processingTime='15 seconds')\
            .outputMode("update")\
            .option("truncate", "false")\
            .format("console")\
            .start()

        return orders_df5
    

    def writeAggToMySQL(self, orders_df5):
        orders_df5.writeStream\
            .trigger(processingTime='15 seconds')\
            .outputMode("update")\
            .foreachBatch(self.saveToMySQL)\
            .start()


    def saveToMySQL(self, current_df, epoch_id):
        # initialize db credentials
        db_credentials = {
            "user": self.mysql_username,
            "password": self.mysql_password,
            "driver": self.mysql_driver
        }

        # Print current epoch_id
        print(f"Current epoch_id:\n {epoch_id}")

        # get current time
        processed_at = time.strftime("%Y-%m-%d %H:%M:%S")

        # Create final dataframe
        current_df_final = current_df\
            .withColumn("processed_at", lit(processed_at))\
            .withColumn("batch_id", lit(epoch_id))
        
        # Write dataframe to mysql
        current_df_final.write\
            .jdbc(url=self.mysql_jdbc_url,
                  table=self.mysql_table,
                  mode="append",
                  properties=db_credentials)


    def writeOrdersToCassandra(self, orders_df3):
        # Write dataframe to cassandra
        orders_df3.writeStream\
            .trigger(processingTime='15 seconds')\
            .outputMode("update")\
            .foreachBatch(self.saveToCassandra)\
            .start()


    def saveToCassandra(self, current_df, epoch_id):
        # Print current epoch_id
        print(f"Current epoch_id:\n {epoch_id}")

        # Save current dataframe to cassandra
        current_df.write\
            .format("org.apache.spark.sql.cassandra")\
            .options(table=self.cassandra_table, keyspace=self.cassandra_keyspace)\
            .mode("append")\
            .save()



