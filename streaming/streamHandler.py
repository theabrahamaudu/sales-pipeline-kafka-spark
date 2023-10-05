"""
This module defines a streamHandler class for handling streaming operations in the sales pipeline.

The streamHandler class includes methods for:
1. Reading data from Kafka.
2. Transforming the orders DataFrame to include a timestamp.
3. Writing the transformed orders DataFrame to Cassandra.
4. Aggregating data from the orders DataFrame.
5. Writing the aggregated data to MySQL.

Usage:
    To use this module, create an instance of the streamHandler class and call its methods to execute the
    different steps of the sales pipeline streaming process.

Example:
    stream = streamHandler(config_path='./config', customer_data_path='/user/sales_pipeline/customers.csv')
    orders_df = stream.readFromKafka()
    orders_df3 = stream.transformOrdersData(orders_df)
    cassandra_query = stream.writeOrdersToCassandra(orders_df3)
    orders_df5, console_output = stream.aggregateData(orders_df3)
    mysql_query = stream.writeAggToMySQL(orders_df5)
    cassandra_query.awaitTermination()
    console_output.awaitTermination()
    mysql_query.awaitTermination()
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.streaming import StreamingQuery
import time
import yaml
import pandas as pd
from dotenv import load_dotenv
import os


class streamHandler:
    """
    This class handles streaming operations for the sales pipeline.

    Attributes:
        config_path (str): The path to the configuration file.
        customer_data_path (str): The path to the customer data file.

    Methods:
        __init__(self, config_path: str, customer_data_path: str):
            Initializes the StreamHandler object.

        readFromKafka(self):
            Reads data from Kafka into a DataFrame.

        transformOrdersData(self, orders_df):
            Transforms the orders DataFrame to include a timestamp.

        writeOrdersToCassandra(self, orders_df):
            Writes the transformed orders DataFrame to Cassandra.

        writeAggToMySQL(self, orders_df):
            Writes the aggregated data from the orders DataFrame to MySQL.

        saveToCassandra(self, current_df, epoch_id):
            Helper method for `writeOrdersToCassandra` to save the given DataFrame to Cassandra.

        saveToMySQL(self, current_df, epoch_id):
            Helper method for `writeAggToMySQL` to save the given DataFrame to MySQL.

        aggregateData(self, orders_df):
            Aggregates data from the orders DataFrame.
    """
    def __init__(self, config_path: str, customer_data_path: str):
        """
        Initializes the class with the given configuration on the code base 
        and customer data path on HDFS.
        Initializes the Spark session.
        
        Parameters:
            config_path (str): The path to the configuration file.
            customer_data_path (str): The path to the customer data file.
        
        """

        # Load config
        with open(config_path+'/config.yaml', 'r') as f:
            config = yaml.safe_load(f.read())
        self.config = config

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
        jars_dir = self.config['jars']['dir']
        JARS_PATH = "file://"+jars_dir+"/jsr166e-1.1.0.jar," \
            "file://"+jars_dir+"/spark-cassandra-connector-assembly_2.12-3.3.0.jar," \
            "file://"+jars_dir+"/mysql-connector-java-8.0.30.jar," \
            "file://"+jars_dir+"/spark-sql-kafka-0-10_2.12-3.3.0.jar," \
            "file://"+jars_dir+"/kafka-clients-3.5.1.jar," \
            "file://"+jars_dir+"/spark-streaming-kafka-0-10-assembly_2.12-3.3.3.jar," \
            "file://"+jars_dir+"/commons-pool2-2.11.1.jar" 

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


    def readFromKafka(self, offset_start='latest') -> DataFrame:
        """
        Reads data from Kafka using the specified offset start.

        Args:
            offset_start (str, optional): The offset to start reading from. Defaults to 'latest'.

        Returns:
            DataFrame: The dataframe containing the read data from Kafka.
        """
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
    

    def transformOrdersData(self, orders_df: DataFrame) -> DataFrame:
        """
        Transforms the orders DataFrame to include a timestamp.

        Parameters:
            orders_df (DataFrame): The input orders DataFrame.

        Returns:
            DataFrame: The transformed orders DataFrame with the timestamp included.
        """

        # Transform orders dataframe to include timestamp
        orders_df1 = orders_df\
            .selectExpr("CAST(value AS STRING)", "timestamp")
        
        orders_df2 = orders_df1\
            .select(from_json(col("value"), self.orders_schema).alias("orders"), "timestamp")
        
        orders_df3 = orders_df2\
            .select("orders.*", "timestamp")
        
        # Print schema of dataframe
        print("Transformed Orders dataframe schema: ")
        orders_df3.printSchema()
        
        return orders_df3
    

    def aggregateData(self, orders_df3: DataFrame) -> (DataFrame, StreamingQuery):
        """
        Aggregates data from the given orders DataFrame.

        Parameters:
            orders_df3 (DataFrame): The orders DataFrame to be aggregated.

        Returns:
            Tuple[DataFrame, StreamingQuery]: A tuple containing the aggregated DataFrame and the console output
            of the aggregation process.
        """
        # print schema of customer dataframe
        print("Customer dataframe schema: ")
        self.customer_data.printSchema()

        # Join customer dataframe to orders dataframe by customer_id
        orders_df4 = orders_df3\
            .join(self.customer_data, orders_df3.customer_id == self.customer_data.customer_id, 'inner')
        
        # find total_sum_amount by grouping source, state
        orders_df5 = orders_df4.groupBy("source", "state")\
            .agg({"total": "sum"}).select("source", "state", col("sum(total)").alias("total_sum_amount"))
        
        # print aggregated dataframe schema
        print("Aggregated dataframe schema: ")
        orders_df5.printSchema()

        # write data to console for debugging
        console_output = orders_df5.writeStream\
            .trigger(processingTime='15 seconds')\
            .outputMode("update")\
            .option("truncate", "false")\
            .format("console")\
            .start()
        
        return orders_df5, console_output
    

    def writeAggToMySQL(self, orders_df5: DataFrame) -> StreamingQuery:
        """
        Writes the aggregated data from the given DataFrame to MySQL.

        Args:
            orders_df5 (DataFrame): The DataFrame containing the aggregated data.

        Returns:
            StreamingQuery: The MySQL query used to write the data to the database.
        """
        mysql_query = orders_df5.writeStream\
            .trigger(processingTime='15 seconds')\
            .outputMode("update")\
            .foreachBatch(self.saveToMySQL)\
            .start()
        return mysql_query


    def saveToMySQL(self, current_df: DataFrame, epoch_id: int):
        """
        Helper method for `writeAggToMySQL` to save the given DataFrame to MySQL.
        Includes new columns for `batch_id` and `processed_at` in the data to be saved.

        Args:
            current_df (DataFrame): The DataFrame to be saved.
            epoch_id (int): The ID of the current epoch.

        Returns:
            None
        """
        # initialize db credentials
        db_credentials = {
            "user": self.mysql_username,
            "password": self.mysql_password,
            "driver": self.mysql_driver
        }

        # Print current epoch_id
        print(f"MySQL Current epoch_id:\n {epoch_id}")

        # get current time
        processed_at = time.strftime("%Y-%m-%d %H:%M:%S")

        # Create final dataframe including `processed_at` and `batch_id`
        current_df_final = current_df\
            .withColumn("processed_at", lit(processed_at))\
            .withColumn("batch_id", lit(epoch_id))
        
        # Write dataframe to mysql
        current_df_final.write\
            .jdbc(url=self.mysql_jdbc_url,
                  table=self.mysql_table,
                  mode="append",
                  properties=db_credentials)


    def writeOrdersToCassandra(self, orders_df3: DataFrame) -> StreamingQuery:
        """
        Writes the given DataFrame to Cassandra.

        Parameters:
            orders_df3 (DataFrame): The DataFrame to be written to Cassandra.

        Returns:
            StreamingQuery: The streaming query object representing the execution of the writeStream operation.
        """
        # Write dataframe to cassandra
        cassandra_query = orders_df3.writeStream\
            .trigger(processingTime='15 seconds')\
            .outputMode("update")\
            .foreachBatch(self.saveToCassandra)\
            .start()
        return cassandra_query


    def saveToCassandra(self, current_df: DataFrame, epoch_id: int):
        """
        Helper method for `writeOrdersToCassandra` to save the current DataFrame to Cassandra.

        Parameters:
            current_df (DataFrame): The current DataFrame to be saved.
            epoch_id (int): The current epoch ID.

        Returns:
            None
        """
        # Print current epoch_id
        print(f"Cassandra Current epoch_id:\n {epoch_id}")

        # Save current dataframe to cassandra
        current_df.write\
            .format("org.apache.spark.sql.cassandra")\
            .options(table=self.cassandra_table, keyspace=self.cassandra_keyspace)\
            .mode("append")\
            .save()



