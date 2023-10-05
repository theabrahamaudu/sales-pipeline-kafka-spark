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


