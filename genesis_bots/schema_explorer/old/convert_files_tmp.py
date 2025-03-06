

from google.cloud import bigquery
import json
import re
import os
from datetime import datetime
from genesis_bots.core.logging_config import logger

# Setup BigQuery client
client = bigquery.Client()

# Define the dataset and table
dataset_id = 'hello-prototype.ELSA_INTERNAL'
table_id = f"{dataset_id}.database_metadata"

def create_bigquery_table():
    schema = [
        bigquery.SchemaField("qualified_table_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("memory_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("memory_file_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("schema_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("table_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("file_contents", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("ddl", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("summary", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("sample_data_text", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("last_crawled_timestamp", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("crawl_status", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("role_used_for_crawl", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("column_details_json", "STRING", mode="REQUIRED"),
    ]
    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table, exists_ok=True)

def process_file(file_path):
    with open(file_path, 'r') as file:
        content = file.read()

    ddl_match = re.search(r'DDL:(.*?)Summary:', content, re.DOTALL)
    summary_match = re.search(r'Summary:(.*?)Sample Data:', content, re.DOTALL)
    sample_data_match = re.search(r'Sample Data:\s*(\[.*\])', content, re.DOTALL)

    ddl_section = ddl_match.group(1).strip() if ddl_match else ""
    summary_section = summary_match.group(1).strip() if summary_match else ""
    sample_data_json = sample_data_match.group(1).strip() if sample_data_match else "[]"
    sample_data = json.loads(sample_data_json)

    qualified_table_name_match = re.search(r'`(.+?)`', ddl_section)
    qualified_table_name = qualified_table_name_match.group(1) if qualified_table_name_match else "Unknown"
    schema_name, table_name = qualified_table_name.split('.')[-2:]

    column_details = re.findall(r'(\w+)\s(\w+),?', ddl_section)

    columns_info = {}
    for column_name, column_type in column_details:
        columns_info[column_name] = {
            "type": column_type,
            "description": "",  # Implement logic to extract description from summary_section
            "sample_values": [d.get(column_name) for d in sample_data if column_name in d]
        }

    current_timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

    row_to_insert = {
        "qualified_table_name": qualified_table_name,
        "memory_number": file_path.split("_")[-1].split(".")[0],
        "memory_file_name": file_path.split('/')[-1],
        "schema_name": schema_name,
        "table_name": table_name,
        "file_contents": content,
        "ddl": ddl_section,
        "summary": summary_section,
        "sample_data_text": sample_data_json,
        "last_crawled_timestamp": current_timestamp,
        "crawl_status": "CRAWLED",
        "role_used_for_crawl": "ADMIN",
        "column_details_json": json.dumps(columns_info, indent=2)
    }

    return row_to_insert

def insert_rows_to_bigquery(files_directory):
    for filename in os.listdir(files_directory):
        if filename.startswith("memory_") and filename.endswith(".txt"):
            file_path = os.path.join(files_directory, filename)
            row_to_insert = process_file(file_path)
            if row_to_insert:
                errors = client.insert_rows_json(table_id, [row_to_insert])
                if errors == []:
                    logger.info(f"New row has been added for {filename}.")
                else:
                    logger.info(f"Encountered errors while inserting rows for {filename}: {errors}")

if __name__ == "__main__":
    files_directory = './kb_vector/database_metadata'
    create_bigquery_table()
    insert_rows_to_bigquery(files_directory)
