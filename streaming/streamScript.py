"""
This module initializes and orchestrates the sales pipeline streaming process.

It uses the `streamHandler` class to perform the following steps:
1. Initialize the stream handler with the specified configuration and customer data paths.
2. Read data from Kafka.
3. Transform the orders DataFrame to include a timestamp.
4. Write the transformed orders DataFrame to Cassandra.
5. Aggregate the orders DataFrame with the customer DataFrame.
6. Write the aggregated orders DataFrame to MySQL.

Usage:
    To run this module, execute it as a standalone script.

Example:
    $ python3 streamScript.py
"""

from streamHandler import streamHandler

if __name__ == "__main__":
    # Initialize stream handler
    stream = streamHandler(config_path='./config', customer_data_path='/user/sales_pipeline/customers.csv')

    # Read from kafka
    orders_df = stream.readFromKafka()

    # Transform orders dataframe
    orders_df3 = stream.transformOrdersData(orders_df)

    # Write transformed orders dataframe to cassandra
    cassandra_query = stream.writeOrdersToCassandra(orders_df3)

    # Aggregate orders dataframe with customer dataframe
    orders_df5, console_output = stream.aggregateData(orders_df3)

    # Write aggregated orders dataframe to mysql
    mysql_query = stream.writeAggToMySQL(orders_df5)

    # Await terminations
    cassandra_query.awaitTermination()
    console_output.awaitTermination()
    mysql_query.awaitTermination()


