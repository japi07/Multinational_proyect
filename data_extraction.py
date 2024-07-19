import pandas as pd
import tabula
import requests
import boto3
import yaml
from io import StringIO
from database_utils import DatabaseConnector
from data_cleaning import DataCleaning

# Load API key from config.yaml
with open('config.yaml', 'r') as file:
    config = yaml.safe_load(file)

api_key = config['API_KEY']

class DataExtractor:
    """
    A class used to extract data from various sources including databases, PDFs, APIs, and S3.
    
    Attributes
    ----------
    db_connector : DatabaseConnector
        An instance of DatabaseConnector to handle database connections and operations.

    Methods
    -------
    extract_from_db(table_name, creds_file):
        Extracts data from a database table.
        
    retrieve_pdf_data(pdf_url):
        Extracts data from a PDF file located at the specified URL.
        
    list_number_of_stores(endpoint, headers):
        Retrieves the number of stores from an API endpoint.
        
    retrieve_stores_data(store_endpoint, headers, number_of_stores):
        Retrieves store data from an API endpoint for a specified number of stores.
        
    extract_from_s3(address):
        Extracts data from a CSV file stored in an S3 bucket.
        
    extract_json_from_s3(json_url):
        Extracts data from a JSON file located at the specified URL.
    """

    def __init__(self):
        """Initializes the DataExtractor with a DatabaseConnector instance."""
        self.db_connector = DatabaseConnector()

    def extract_from_db(self, table_name, creds_file):
        """
        Extracts data from a database table.

        Parameters
        ----------
        table_name : str
            The name of the table to extract data from.
        creds_file : str
            The path to the credentials file for database connection.

        Returns
        -------
        DataFrame
            A pandas DataFrame containing the data from the specified table.
        """
        engine = self.db_connector.init_db_engine(creds_file)
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql_query(query, engine)
        return df

    def retrieve_pdf_data(self, pdf_url):
        """
        Extracts data from a PDF file located at the specified URL.

        Parameters
        ----------
        pdf_url : str
            The URL of the PDF file.

        Returns
        -------
        DataFrame
            A pandas DataFrame containing the data extracted from the PDF.
        """
        tables = tabula.read_pdf(pdf_url, pages="all")
        pdf_data = pd.concat(tables, ignore_index=True)
        return pdf_data

    def list_number_of_stores(self, endpoint, headers):
        """
        Retrieves the number of stores from an API endpoint.

        Parameters
        ----------
        endpoint : str
            The API endpoint URL to retrieve the number of stores.
        headers : dict
            The headers to include in the API request.

        Returns
        -------
        int
            The number of stores.
        """
        response = requests.get(endpoint, headers=headers)
        if response.status_code == 200:
            data = response.json()
            return data['number_stores']
        else:
            response.raise_for_status()

    def retrieve_stores_data(self, store_endpoint, headers, number_of_stores):
        """
        Retrieves store data from an API endpoint for a specified number of stores.

        Parameters
        ----------
        store_endpoint : str
            The API endpoint URL template for retrieving store data.
        headers : dict
            The headers to include in the API request.
        number_of_stores : int
            The number of stores to retrieve data for.

        Returns
        -------
        DataFrame
            A pandas DataFrame containing the data for the specified number of stores.
        """
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
        """
        Extracts data from a CSV file stored in an S3 bucket.

        Parameters
        ----------
        address : str
            The S3 address of the CSV file.

        Returns
        -------
        DataFrame
            A pandas DataFrame containing the data from the CSV file.
        """
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
        """
        Extracts data from a JSON file located at the specified URL.

        Parameters
        ----------
        json_url : str
            The URL of the JSON file.

        Returns
        -------
        DataFrame
            A pandas DataFrame containing the data from the JSON file.
        """
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

    headers = {
        'x-api-key': api_key
    }

    # Extract and upload store data
    number_of_stores_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
    store_details_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}"
    
    try:
        number_of_stores = data_extractor.list_number_of_stores(number_of_stores_endpoint, headers)
        stores_data_df = data_extractor.retrieve_stores_data(store_details_endpoint, headers, number_of_stores)
        cleaned_stores_data = data_cleaning.clean_store_data(stores_data_df)
        db_connector.upload_to_db(cleaned_stores_data, 'dim_store_details', 'new_db_creds.yaml')
        print("Cleaned stores data uploaded to dim_store_details table in sales_data database.")
    except Exception as e:
        print(f"An error occurred while processing store data: {e}")

    # Extract and upload card data from PDF
    pdf_url = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
    
    try:
        pdf_data = data_extractor.retrieve_pdf_data(pdf_url)
        cleaned_pdf_data = data_cleaning.clean_card_data(pdf_data)
        db_connector.upload_to_db(cleaned_pdf_data, 'dim_card_details', 'new_db_creds.yaml')
        print("Cleaned PDF data uploaded to dim_card_details table in sales_data database.")
    except Exception as e:
        print(f"An error occurred while processing card data: {e}")

    # Extract and upload product data from S3
    s3_address = "s3://data-handling-public/products.csv"
    
    try:
        products_data_df = data_extractor.extract_from_s3(s3_address)
        cleaned_products_data = data_cleaning.clean_products_data(products_data_df)
        db_connector.upload_to_db(cleaned_products_data, 'dim_products', 'new_db_creds.yaml')
        print("Cleaned products data uploaded to dim_products table in sales_data database.")
    except Exception as e:
        print(f"An error occurred while processing product data: {e}")

    # Extract and upload orders data
    orders_table_name = 'orders_table'
    
    try:
        orders_df = data_extractor.extract_from_db(orders_table_name, 'db_creds.yaml')
        cleaned_orders_data = data_cleaning.clean_orders_data(orders_df)
        db_connector.upload_to_db(cleaned_orders_data, 'orders_table', 'new_db_creds.yaml')
        print("Cleaned orders data uploaded to orders_table in sales_data database.")
    except Exception as e:
        print(f"An error occurred while processing orders data: {e}")

    # Extract and upload date details data from JSON
    json_url = "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json"
    
    try:
        date_details_df = data_extractor.extract_json_from_s3(json_url)
        cleaned_date_details = data_cleaning.clean_date_details(date_details_df)
        db_connector.upload_to_db(cleaned_date_details, 'dim_date_times', 'new_db_creds.yaml')
        print("Cleaned date details data uploaded to dim_date_times table in sales_data database.")
    except Exception as e:
        print(f"An error occurred while processing date details data: {e}")
