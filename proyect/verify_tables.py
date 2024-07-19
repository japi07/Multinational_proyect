import psycopg2
import yaml
import pandas as pd
from sqlalchemy import create_engine

class DatabaseConnector:
    def __init__(self, creds_file='new_db_creds.yaml'):
        self.creds_file = creds_file

    def read_db_creds(self):
        try:
            with open(self.creds_file, 'r') as file:
                creds = yaml.safe_load(file)
            print(f"Database credentials loaded from {self.creds_file}: {creds}")
            return creds
        except FileNotFoundError as e:
            print(f"An error occurred while connecting to the database: {e}")
            return None

    def init_db_engine(self):
        creds = self.read_db_creds()
        if creds:
            print(f"Connecting to PostgreSQL database {creds['RDS_DATABASE']} at {creds['RDS_HOST']}:{creds['RDS_PORT']} with user {creds['RDS_USER']}")
            engine = create_engine(
                f"postgresql+psycopg2://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}"
            )
            return engine
        else:
            return None

    def list_db_tables(self):
        engine = self.init_db_engine()
        if engine:
            inspector = inspect(engine)
            tables = inspector.get_table_names(schema='public')
            return tables
        else:
            return []

    def connect(self):
        try:
            creds = self.read_db_creds()
            if creds:
                connection = psycopg2.connect(
                    host=creds['RDS_HOST'],
                    user=creds['RDS_USER'],
                    password=creds['RDS_PASSWORD'],
                    dbname=creds['RDS_DATABASE'],
                    port=creds['RDS_PORT']
                )
                print(f"Connected to the database {creds['RDS_DATABASE']} successfully.")
                return connection
            else:
                return None
        except Exception as e:
            print(f"An error occurred while connecting to the database: {e}")
            return None

    def get_table_schema(self, table_name):
        connection = self.connect()
        if connection:
            try:
                query = f"""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = '{table_name}'
                """
                df = pd.read_sql_query(query, connection)
                connection.close()
                return df
            except Exception as e:
                print(f"An error occurred while fetching the schema of {table_name}: {e}")
                return None
        else:
            print("No database connection established.")
            return None

if __name__ == "__main__":
    db_connector = DatabaseConnector('new_db_creds.yaml')
    db_name = db_connector.get_current_database()

    if db_name:
        print(f"Currently connected to target database: {db_name}")

        tables = db_connector.list_db_tables()
        print(f"Tables in the target database: {tables}")

        for table in tables:
            print(f"Schema of table {table}:")
            schema_df = db_connector.get_table_schema(table)
            if schema_df is not None:
                print(schema_df)
