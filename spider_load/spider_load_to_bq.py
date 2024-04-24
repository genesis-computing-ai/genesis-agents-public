import sqlite3
import glob
import os
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField
from google.cloud.exceptions import NotFound

def dataset_exists(bigquery_client, dataset_id):
    """Check if a BigQuery dataset exists."""
    try:
        bigquery_client.get_dataset(dataset_id)
        return True
    except NotFound:
        return False

def delete_dataset(bigquery_client, dataset_id):
    """Deletes a BigQuery dataset and its contents."""
    try:
        dataset_ref = bigquery_client.dataset(dataset_id)
        bigquery_client.delete_dataset(dataset_ref, delete_contents=True, not_found_ok=True)
        print(f"Deleted dataset {dataset_id} because it was empty.")
    except Exception as e:
        print(f"Error deleting dataset {dataset_id}: {e}")

def sqlite_to_bigquery(sqlite_file_path, bigquery_dataset_id):
    bigquery_client = bigquery.Client()
    data_uploaded = False  # Track if any data is uploaded to the dataset

    if dataset_exists(bigquery_client, bigquery_dataset_id):
        #print(f"Dataset {bigquery_dataset_id} already exists. Deleting.")
        #delete_dataset(bigquery_client, bigquery_dataset_id)
        print(f"Dataset {bigquery_dataset_id} already exists. Skipping dataset to avoid duplicates.")
        return

    conn = sqlite3.connect(sqlite_file_path)
    cursor = conn.cursor()

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()

    if not dataset_exists(bigquery_client, bigquery_dataset_id):
        dataset_ref = bigquery_client.dataset(bigquery_dataset_id)
        dataset = bigquery.Dataset(dataset_ref)
        bigquery_client.create_dataset(dataset, exists_ok=True)

    for table_name in tables:
        table_name = table_name[0].lower()
        print(f"Processing table: {table_name}")

        cursor.execute(f"PRAGMA table_info({table_name})")
        schema_fields = [SchemaField(column[1], "STRING") for column in cursor.fetchall()]

        table_ref = bigquery_client.dataset(bigquery_dataset_id).table(table_name)
        table = bigquery.Table(table_ref, schema=schema_fields)
        bigquery_client.create_table(table, exists_ok=True)

        cursor.execute(f"SELECT * FROM {table_name}")
        rows = cursor.fetchall()

        batch_size = 500  # Adjust based on your needs and row sizes
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i+batch_size]
            if batch:
                rows_to_insert = [dict(zip([column[0] for column in cursor.description], row)) for row in batch]
                errors = bigquery_client.insert_rows_json(table, rows_to_insert)
                if not errors:
                    data_uploaded = True
                    print(f"Batch of {len(batch)} rows from table {table_name} uploaded to BigQuery successfully.")
                else:
                    print(f"Errors occurred while uploading a batch from table {table_name} data: {errors}")

    if not data_uploaded:
        # If no data was uploaded because all tables were empty, delete the dataset
        delete_dataset(bigquery_client, bigquery_dataset_id)

    conn.close()

def process_sqlite_files(folder_path):
    # Find all .sqlite files in the folder and subfolders recursively
    for sqlite_file_path in glob.glob(f"{folder_path}/**/*.sqlite", recursive=True):
        filename_without_extension = os.path.splitext(os.path.basename(sqlite_file_path))[0]
        bigquery_dataset_id = filename_without_extension.lower()
        print(f"Processing file: {sqlite_file_path} with dataset ID: {bigquery_dataset_id}")
        sqlite_to_bigquery(sqlite_file_path, bigquery_dataset_id)

# Usage
folder_path = './spider_load/database/'
process_sqlite_files(folder_path)
