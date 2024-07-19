import pandas as pd
import tabula
import requests
import boto3
from io import StringIO
from database_utils import DatabaseConnector
from data_cleaning import DataCleaning

class DataExtractor:
    def __init__(self):
        self.db_connector = DatabaseConnector()

    def extract_from_db(self, table_name):
        engine = self.db_connector.init_db_engine()
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql_query(query, engine)
        engine.dispose()  # Close the engine connection
        return df

    def retrieve_pdf_data(self, pdf_url):
        tables = tabula.read_pdf(pdf_url, pages="all")
        pdf_data = pd.concat(tables, ignore_index=True)
        return pdf_data

    def list_number_of_stores(self, endpoint, headers):
        response = requests.get(endpoint, headers=headers)
        if response.status_code == 200:
            data = response.json()
            return data['number_stores']
        else:
            response.raise_for_status()

    def retrieve_stores_data(self, store_endpoint, headers, number_of_stores):
        stores_data = []
        for store_number in range(1, number_of_stores + 1):
            response = requests.get(store_endpoint.format(store_number=store_number), headers=headers)
            if response.status_code == 200:
                store_data = response.json()
                stores_data.append(store_data)
            else:
                print(f"Failed to retrieve data for store number {store_number}: {response.status_code}")
        stores_df = pd.DataFrame(stores_data)
        return stores_df

    def extract_from_s3(self, address):
        bucket_name = address.split('/')[2]
        key = '/'.join(address.split('/')[3:])
        s3_client = boto3.client('s3')
        try:
            csv_obj = s3_client.get_object(Bucket=bucket_name, Key=key)
            body = csv_obj['Body'].read().decode('utf-8')
            data = StringIO(body)
            df = pd.read_csv(data)
            return df
        except Exception as e:
            print(f"Failed to download file from S3: {e}")
            return pd.DataFrame()

    def extract_json_from_s3(self, json_url):
        response = requests.get(json_url)
        if response.status_code == 200:
            data = response.json()
            df = pd.DataFrame(data)
            return df
        else:
            print(f"Failed to retrieve JSON data from {json_url}: {response.status_code}")
            return pd.DataFrame()

if __name__ == "__main__":
    data_extractor = DataExtractor()
    data_cleaning = DataCleaning()
    db_connector = data_extractor.db_connector

    # List all tables in the database
    tables = db_connector.list_db_tables()
    print("Tables in the database:", tables)

    headers = {
        'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'
    }

    # Extract and upload store data
    number_of_stores_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
    number_of_stores = data_extractor.list_number_of_stores(number_of_stores_endpoint, headers)
    print(f"Number of stores: {number_of_stores}")

    store_details_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}"
    stores_data_df = data_extractor.retrieve_stores_data(store_details_endpoint, headers, number_of_stores)
    print("Extracted stores data:")
    print(stores_data_df.head())

    cleaned_stores_data = data_cleaning.clean_store_data(stores_data_df)
    print("Cleaned stores data:")
    print(cleaned_stores_data.head())

    try:
        db_connector.upload_to_db(cleaned_stores_data, 'public.dim_store_details')
        print("Cleaned stores data uploaded to dim_store_details table in sales_data database.")
    except Exception as e:
        print(f"An error occurred while uploading the DataFrame to the database: {e}")

    # Extract and upload card data from PDF
    pdf_url = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
    pdf_data = data_extractor.retrieve_pdf_data(pdf_url)
    print("Extracted PDF data:")
    print(pdf_data.head())

    cleaned_pdf_data = data_cleaning.clean_card_data(pdf_data)
    print("Cleaned PDF data:")
    print(cleaned_pdf_data.head())

    try:
        db_connector.upload_to_db(cleaned_pdf_data, 'public.dim_card_details')
        print("Cleaned PDF data uploaded to dim_card_details table in sales_data database.")
    except Exception as e:
        print(f"An error occurred while uploading the DataFrame to the database: {e}")

    # Extract and upload product data from S3
    s3_address = "s3://data-handling-public/products.csv"
    products_data_df = data_extractor.extract_from_s3(s3_address)
    if not products_data_df.empty:
        print("Extracted products data from S3:")
        pd.set_option('display.max_columns', None)
        print(products_data_df.head())
        print("Columns in the products data:", products_data_df.columns.tolist())

        try:
            cleaned_products_data = data_cleaning.clean_products_data(products_data_df)
            print("Cleaned products data:")
            print(cleaned_products_data.head())

            db_connector.upload_to_db(cleaned_products_data, 'public.dim_products')
            print("Extracted products data uploaded to dim_products table in sales_data database.")
        except Exception as e:
            print(f"An error occurred while uploading the products DataFrame to the database: {e}")
    else:
        print("Failed to extract products data from S3.")

    # Extract and upload orders data
    orders_table_name = 'orders_table'
    orders_df = data_extractor.extract_from_db(orders_table_name)
    print("Extracted orders data:")
    print(orders_df.head())

    cleaned_orders_data = data_cleaning.clean_orders_data(orders_df)
    if cleaned_orders_data is not None:
        print("Cleaned orders data:")
        print(cleaned_orders_data.head())

        try:
            db_connector.upload_to_db(cleaned_orders_data, 'public.orders_table')
            print(f"Cleaned orders data uploaded to {orders_table_name} table in sales_data database.")
        except Exception as e:
            print(f"An error occurred while uploading the orders DataFrame to the database: {e}")
    else:
        print("No cleaned orders data to upload.")

    # Extract and upload date details data from JSON
    json_url = "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json"
    date_details_df = data_extractor.extract_json_from_s3(json_url)
    print("Extracted date details data:")
    print(date_details_df.head())

    cleaned_date_details = data_cleaning.clean_date_details(date_details_df)
    print("Cleaned date details data:")
    print(cleaned_date_details.head())

    try:
        db_connector.upload_to_db(cleaned_date_details, 'public.dim_date_times')
        print("Cleaned date details data uploaded to dim_date_times table in sales_data database.")
    except Exception as e:
        print(f"An error occurred while uploading the date details DataFrame to the database: {e}")

    # Close any open connections
    if db_connector.connection:
        db_connector.close_connection()
