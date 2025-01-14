from google.cloud import bigquery
import os
import json
from datetime import datetime
import re

from genesis_bots.core.logging_config import logger


# Setup BigQuery client
client = bigquery.Client()

# Define the dataset and table
dataset_id = 'hello-prototype.bot_data_dictionary'
table_id = f"{dataset_id}.crawl_output"

# Check if the dataset exists, create if not
dataset = bigquery.Dataset(dataset_id)
dataset.location = "US"
client.create_dataset(dataset, exists_ok=True)

# Define the schema of the table
schema = [
    bigquery.SchemaField("qualified_table_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("memory_number", "STRING", mode="REQUIRED"),
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

# Check if the table exists, create if not
table = bigquery.Table(table_id, schema=schema)
table = client.create_table(table, exists_ok=True)

import json
import re
from datetime import datetime

def process_file(file_path):
    with open(file_path, 'r') as file:
        content = file.read()

    # Extract sections
    ddl_section = re.search(r'DDL:(.*?)Summary:', content, re.DOTALL).group(1).strip()
    summary_section = re.search(r'Summary:(.*?)Sample Data:', content, re.DOTALL).group(1).strip()
    sample_data_json = content.split('Sample Data:')[1].strip()
    sample_data = json.loads(sample_data_json)

    # Extract table name and schema
    qualified_table_name_match = re.search(r'`(.+?)`', ddl_section)
    qualified_table_name = qualified_table_name_match.group(1) if qualified_table_name_match else "Unknown"
    schema_name, table_name = qualified_table_name.split('.')[-2:]

    # Extract column details from DDL
    column_details = re.findall(r'(\w+)\s(\w+)', ddl_section)
    column_descriptions = {}
    for column_detail in column_details:
        column_name, column_type = column_detail
        # Placeholder for extracting column descriptions from summary
        column_descriptions[column_name] = {
            "type": column_type,
            "description": "Add logic to extract specific column descriptions from the summary",
            "sample_values": []
        }

    # Append sample values to column details
    for sample in sample_data:
        for column_name, sample_value in sample.items():
            if column_name in column_descriptions:
                column_descriptions[column_name]["sample_values"].append(sample_value)

    # Convert datetime to the required format for BigQuery
    current_timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

    # Prepare row for insertion
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
        "column_details_json": json.dumps(column_descriptions, indent=2)
    }

    return row_to_insert


def insert_rows_to_bigquery(files_directory):
    """
    Iterate over files in the directory, process each, and insert rows into BigQuery.
    """
    for filename in os.listdir(files_directory):
        if filename.startswith("memory_") and filename.endswith(".txt"):
            file_path = os.path.join(files_directory, filename)
            row_to_insert = process_file(file_path)
            errors = client.insert_rows_json(table_id, [row_to_insert])
            if errors == []:
                logger.info(f"New row has been added.")
            else:
                logger.info(f"Encountered errors while inserting rows: {errors}")

# Replace 'your_directory_path' with the actual path where your memory files are stored
files_directory = './kb_vector/database_metadata'
insert_rows_to_bigquery(files_directory)

