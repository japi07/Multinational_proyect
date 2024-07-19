import psycopg2
import yaml

def read_db_creds(creds_file='new_db_creds.yaml'):
    with open(creds_file, 'r') as file:
        creds = yaml.safe_load(file)
    return creds

def connect_to_db(creds):
    try:
        connection = psycopg2.connect(
            host=creds['RDS_HOST'],
            user=creds['RDS_USER'],
            password=creds['RDS_PASSWORD'],
            dbname=creds['RDS_DATABASE'],
            port=creds['RDS_PORT']
        )
        print("Connected to the database successfully.")
        return connection
    except Exception as e:
        print(f"An error occurred while connecting to the database: {e}")
        return None

def drop_tables(connection, tables):
    try:
        cursor = connection.cursor()
        for table in tables:
            cursor.execute(f"DROP TABLE IF EXISTS {table};")
            print(f"Table {table} dropped successfully.")
        connection.commit()
    except Exception as e:
        print(f"An error occurred while dropping the tables: {e}")
        connection.rollback()
    finally:
        cursor.close()

if __name__ == "__main__":
    creds = read_db_creds()
    connection = connect_to_db(creds)
    if connection:
        tables_to_drop = ['dim_store_details', 'dim_card_details']
        drop_tables(connection, tables_to_drop)
        connection.close()
