from openai import OpenAI
import os
import csv

from genesis_bots.llm.llm_openai.openai_utils import get_openai_client
from genesis_bots.core.logging_config import logger

client = get_openai_client()

def get_embedding(text):
    response = client.embeddings.create(
        model="text-embedding-3-large",
        input=text.replace("\n", " ")  # Replace newlines with spaces
    )
    # Extracting the embedding from the response
    embedding = response.data[0].embedding
    return embedding

def write_embeddings_to_csv(embeddings, csv_file_path):
    with open(csv_file_path, mode='w', newline='') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(['File Name', 'Embedding'])
        for entry in embeddings:
            writer.writerow(entry)

def sorted_directory_listing_with_os_listdir(directory):
    items = os.listdir(directory)
    sorted_items = sorted(items)
    return sorted_items

def process_files(source_folder):
    embeddings = []
    i = 0
    for filename in sorted_directory_listing_with_os_listdir(source_folder):
        i = i + 1
        if i > 1000:
            break
        if filename.startswith("memory") and filename.endswith(".txt"):
            file_path = os.path.join(source_folder, filename)
            with open(file_path, 'r') as file:
                text_content = file.read().replace('/n','')[:8000]

            embedding = get_embedding(text_content)
            embeddings.append([filename, str(embedding)])
            logger.info(filename," processed")

    return embeddings

# Specify your source folder and the path for the output CSV
source_folder = './kb_annoy/database_metadata'
csv_file_path = './tmp/embedding_out_full.csv'


embeddings = process_files(source_folder)

# Write all embeddings to a single CSV file
write_embeddings_to_csv(embeddings, csv_file_path)
