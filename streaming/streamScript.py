from streamHandler import streamHandler

if __name__ == "__main__":
    # Initialize stream handler
    stream = streamHandler(config_path='./config', customer_data_path='./data/customers.csv')

    # Read from kafka
    orders_df = stream.readFromKafka()

    # Transform orders dataframe
    orders_df3 = stream.transformOrdersData(orders_df)

    # Write transformed orders dataframe to cassandra
    stream.writeOrdersToCassandra(orders_df3)

    # Aggregate orders dataframe with customer dataframe
    orders_df5 = stream.aggregateData(orders_df3)

    # Write aggregated orders dataframe to mysql
    stream.writeAggToMySQL(orders_df5)



