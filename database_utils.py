import psycopg2
import yaml
import pandas as pd
from sqlalchemy import create_engine, inspect, types

class DatabaseConnector:
    def __init__(self, source_creds_file='db_creds.yaml', new_db_creds_file='new_db_creds.yaml'):
        self.source_creds_file = source_creds_file
        self.new_db_creds_file = new_db_creds_file

    def read_db_creds(self, creds_file):
        try:
            with open(creds_file, 'r') as file:
                creds = yaml.safe_load(file)
            print(f"Database credentials loaded from {creds_file}: {creds}")  # Debug print statement
            return creds
        except FileNotFoundError as e:
            print(f"An error occurred while connecting to the database: {e}")
            return None

    def init_db_engine(self, creds_file):
        creds = self.read_db_creds(creds_file)
        if creds:
            print(f"Connecting to PostgreSQL database {creds['RDS_DATABASE']} at {creds['RDS_HOST']}:{creds['RDS_PORT']} with user {creds['RDS_USER']}")  # Debug print statement
            engine = create_engine(
                f"postgresql+psycopg2://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}"
            )
            return engine
        else:
            return None

    def list_db_tables(self, creds_file):
        engine = self.init_db_engine(creds_file)
        if engine:
            inspector = inspect(engine)
            tables = inspector.get_table_names(schema='public')
            return tables
        else:
            return []

    def connect(self, creds_file):
        # Using init_db_engine to avoid repeating logic 
        engine = self.init_db_engine(creds_file)
        if engine:
            connection = engine.raw_connection()
            print(f"Connected to the database using SQLAlchemy engine.")  # Debug print statement
            return connection
        else:
            return None

    def query(self, sql_query, creds_file):
        connection = self.connect(creds_file)
        if connection:
            try:
                df = pd.read_sql_query(sql_query, connection)
                return df
            except Exception as e:
                print(f"An error occurred while executing the query: {e}")
                return None
            finally:
                connection.close()
        else:
            print("No database connection established.")
            return None

    def read_rds_table(self, table_name, creds_file):
        engine = self.init_db_engine(creds_file)
        if engine:
            try:
                query = f"SELECT * FROM {table_name}"
                df = pd.read_sql_query(query, engine)
                return df
            except Exception as e:
                print(f"An error occurred while reading the table: {e}")
                return None
        else:
            return None

    def upload_to_db(self, df, table_name, creds_file):
        engine = self.init_db_engine(creds_file)
        if engine:
            # Calculate max lengths for VARCHAR fields
            max_card_number_length = df['card_number'].str.len().max() if 'card_number' in df else 0
            max_store_code_length = df['store_code'].str.len().max() if 'store_code' in df else 0
            max_product_code_length = df['product_code'].str.len().max() if 'product_code' in df else 0
            max_country_code_length = df['country_code'].str.len().max() if 'country_code' in df else 0

            dtype = {
                'date_uuid': types.String,  # Use String for UUID
                'user_uuid': types.String,  # Use String for UUID
                'card_number': types.String(max_card_number_length),
                'store_code': types.String(max_store_code_length),
                'product_code': types.String(max_product_code_length),
                'product_quantity': types.SmallInteger,
                'first_name': types.VARCHAR(255),
                'last_name': types.VARCHAR(255),
                'date_of_birth': types.DATE,
                'country_code': types.String(max_country_code_length),
                'join_date': types.DATE,
            }
            try:
                df.to_sql(table_name, engine, schema='public', if_exists='replace', index=False, dtype=dtype)
                print(f"DataFrame successfully uploaded to table {table_name}.")
            except Exception as e:
                print(f"An error occurred while uploading the DataFrame to the database: {e}")

    def get_current_database(self, creds_file):
        connection = self.connect(creds_file)
        if connection:
            try:
                query = "SELECT current_database();"
                db_name = pd.read_sql_query(query, connection)
                connection.close()
                return db_name.iloc[0, 0]
            except Exception as e:
                print(f"An error occurred while fetching the current database name: {e}")
                return None
        else:
            return None

