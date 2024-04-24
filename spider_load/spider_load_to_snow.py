import sqlite3
import glob
import os
from snowflake.connector import connect


def _create_connection():
    account = os.getenv('SNOWFLAKE_ACCOUNT')
    user = os.getenv('SNOWFLAKE_USER')
    password = os.getenv('SNOWFLAKE_PASSWORD')
    database = os.getenv('SNOWFLAKE_DATABASE', None)
    warehouse = os.getenv('SNOWFLAKE_WAREHOUSE', None)
    role = os.getenv('SNOWFLAKE_ROLE', None)

    return connect(
        user=user,
        password=password,
        account=account,
        warehouse=warehouse,
        database=database,
        role=role
    ) 

def schema_exists(snowflake_connection, schema_name):
    """Check if a Snowflake schema exists in the SPIDER_DATA database."""
    try:
        cursor = snowflake_connection.cursor()
        cursor.execute("SHOW SCHEMAS IN DATABASE SPIDER_DATA")
        schemas = [schema[1] for schema in cursor.fetchall()]  # Assuming schema name is the second column
        return schema_name.upper() in schemas
    except Exception as e:
        print(f"Error checking if schema exists: {e}")
        return False
    

def delete_schema(snowflake_connection, schema_name):
    """Deletes a Snowflake schema in the SPIDER_DATA database."""
    try:
        cursor = snowflake_connection.cursor()
        cursor.execute(f"DROP SCHEMA IF EXISTS SPIDER_DATA.{schema_name}")
        snowflake_connection.commit()
        print(f"Deleted schema {schema_name} from SPIDER_DATA database.")
    except Exception as e:
        print(f"Error deleting schema {schema_name} from SPIDER_DATA database: {e}")
    finally:
        cursor.close()

def sqlite_to_bigquery(sqlite_file_path, bigquery_dataset_id):
    snowflake_client = _create_connection()
    data_uploaded = False  # Track if any data is uploaded to the dataset

    if schema_exists(snowflake_client, bigquery_dataset_id):
        #print(f"Dataset {bigquery_dataset_id} already exists. Deleting.")
        #delete_dataset(bigquery_client, bigquery_dataset_id)
        print(f"Schema {bigquery_dataset_id} already exists. Skipping dataset to avoid duplicates.")
        return

    conn = sqlite3.connect(sqlite_file_path)
    cursor = conn.cursor()

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()

    if not schema_exists(snowflake_client, bigquery_dataset_id):
        snowflake_client.cursor().execute(f"CREATE SCHEMA IF NOT EXISTS SPIDER_DATA.{bigquery_dataset_id}")

    for table_name in tables:
        table_name = table_name[0].lower()
        print(f"Processing table: {table_name}")

        cursor.execute(f"PRAGMA table_info({table_name})")
        columns_info = cursor.fetchall()
        #column_definitions = ', '.join([f"{column[1].lower()} STRING" for column in columns_info])
        column_definitions = ', '.join([f"\"{column[1].upper()}\" {column[2].upper().replace('BOOL','BOOLEAN')}" for column in columns_info])

        create_table_sql = f"CREATE TABLE IF NOT EXISTS SPIDER_DATA.{bigquery_dataset_id}.{table_name} ({column_definitions})"
        try:
            snowflake_client.cursor().execute(create_table_sql)
            cursor.execute(f"SELECT * FROM {table_name}")
            rows = cursor.fetchall()

            batch_size = 500  # Adjust based on your needs and row sizes
            for i in range(0, len(rows), batch_size):
                batch = rows[i:i+batch_size]
                if batch:
                    # Construct the INSERT statement for Snowflake
                    insert_columns = ', '.join([column[1].lower() for column in columns_info])
                    placeholders = ', '.join(['%s' for _ in columns_info])
                    
                    insert_sql = f"INSERT INTO SPIDER_DATA.{bigquery_dataset_id}.{table_name} ({insert_columns}) VALUES ({placeholders})"
                    
                    # Execute the batch insert
                    snowflake_cursor = snowflake_client.cursor()
                    try:
                        snowflake_cursor.executemany(insert_sql, batch)
                        snowflake_client.commit()
                        data_uploaded = True
                        print(f"Batch of {len(batch)} rows from table {table_name} uploaded to Snowflake successfully.")
                    except Exception as e:
                        print(f"Errors occurred while uploading a batch from table {table_name} data: {e}")
                    finally:
                        snowflake_cursor.close()
        except Exception as e:
            print(f"Errors occurred while creating table {table_name}  with {create_table_sql} {e}")
    if not data_uploaded:
        # If no data was uploaded because all tables were empty, delete the dataset
        delete_schema(snowflake_client, bigquery_dataset_id)

    conn.close()

def process_sqlite_files( folder_path):
    # Find all .sqlite files in the folder and subfolders recursively
    for sqlite_file_path in glob.glob(f"{folder_path}/**/*.sqlite", recursive=True):
        filename_without_extension = os.path.splitext(os.path.basename(sqlite_file_path))[0]
        bigquery_dataset_id = filename_without_extension.lower()
        print(f"Processing file: {sqlite_file_path} with dataset ID: {bigquery_dataset_id}")
        sqlite_to_bigquery( sqlite_file_path, bigquery_dataset_id)

# Usage
folder_path = './spider_load/database/'
process_sqlite_files(folder_path)
