import psycopg2
import yaml

def get_max_length(cursor, table_name, column_name):
    cursor.execute(f"SELECT MAX(LENGTH({column_name})) FROM {table_name};")
    max_length = cursor.fetchone()[0]
    print(f"Max length for {column_name} in {table_name} is {max_length}")
    return max_length

def execute_sql_commands(commands, creds_file):
    with open(creds_file, 'r') as file:
        creds = yaml.safe_load(file)

    conn = psycopg2.connect(
        host=creds['RDS_HOST'],
        user=creds['RDS_USER'],
        password=creds['RDS_PASSWORD'],
        dbname=creds['RDS_DATABASE'],
        port=creds['RDS_PORT']
    )
    
    cursor = conn.cursor()
    try:
        for command in commands:
            print(f"Executing: {command}")
            cursor.execute(command)
        conn.commit()
    except Exception as e:
        print(f"An error occurred: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    creds_file = 'new_db_creds.yaml'
    
    with open(creds_file, 'r') as file:
        creds = yaml.safe_load(file)

    conn = psycopg2.connect(
        host=creds['RDS_HOST'],
        user=creds['RDS_USER'],
        password=creds['RDS_PASSWORD'],
        dbname=creds['RDS_DATABASE'],
        port=creds['RDS_PORT']
    )
    
    cursor = conn.cursor()
    
    try:
        print("Calculating max lengths...")
        max_card_number_length = get_max_length(cursor, 'orders_table', 'card_number')
        max_store_code_length = get_max_length(cursor, 'orders_table', 'store_code')
        max_product_code_length = get_max_length(cursor, 'orders_table', 'product_code')
        
        sql_commands = [
            f"ALTER TABLE orders_table ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID;",
            f"ALTER TABLE orders_table ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID;",
            f"ALTER TABLE orders_table ALTER COLUMN card_number TYPE VARCHAR({max_card_number_length});",
            f"ALTER TABLE orders_table ALTER COLUMN store_code TYPE VARCHAR({max_store_code_length});",
            f"ALTER TABLE orders_table ALTER COLUMN product_code TYPE VARCHAR({max_product_code_length});",
            f"ALTER TABLE orders_table ALTER COLUMN product_quantity TYPE SMALLINT USING product_quantity::SMALLINT;"
        ]
        
        print("Executing SQL commands...")
        execute_sql_commands(sql_commands, creds_file)
        print("SQL commands executed successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        cursor.close()
        conn.close()
        print("Database connection closed.")
