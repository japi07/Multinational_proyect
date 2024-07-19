import pandas as pd
from database_utils import DatabaseConnector

# Initialize the DatabaseConnector
db_connector = DatabaseConnector()

# Extract the data from the orders_table
orders_df = db_connector.read_rds_table('orders_table', 'new_db_creds.yaml')

# Check if the DataFrame is loaded
if orders_df is not None:
    # Print the DataFrame info
    print(orders_df.info())
else:
    print("Failed to load data from orders_table.")

