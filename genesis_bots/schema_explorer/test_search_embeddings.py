import os
from openai import OpenAI
from annoy import AnnoyIndex
import json

from genesis_bots.llm.llm_openai.openai_utils import get_openai_client

from genesis_bots.core.logging_config import logger

# Assuming the embedding size is known (e.g., 4096 for "text-embedding-3-large" model)
embedding_size = 3072

annoy_index_file_path = './tmp/embeddings_full.ann'
metadata_file_path = './tmp/mappings_full.json'

# Load the Annoy index
annoy_index = AnnoyIndex(embedding_size, 'angular')
annoy_index.load(annoy_index_file_path)

# Load the metadata mapping
with open(metadata_file_path, 'r') as f:
    metadata_mapping = json.load(f)

# Function to get embedding (reuse or modify your existing get_embedding function)
def get_embedding(text):
    client = get_openai_client()
    response = client.embeddings.create(
        model="text-embedding-3-large",
        input=text.replace("\n", " ")  # Replace newlines with spaces
    )
    embedding = response.data[0].embedding
    return embedding

# Function to search and display results
def search_and_display_results(search_term):
    embedding = get_embedding(search_term)
    top_matches = annoy_index.get_nns_by_vector(embedding, 5, include_distances=True)

    paired_data = list(zip(top_matches[0], top_matches[1]))
    sorted_paired_data = sorted(paired_data, key=lambda x: x[1])

    for idx in sorted_paired_data:
        table_name = metadata_mapping[idx[0]]
        content = ""
        logger.info(f"Match: {table_name}, Score: {idx[1]}, Content Preview: {content[:100]}\n")

# Prompt the user for a search term
while True:
    search_term = input("Enter your search term: ")
    logger.info('\n\n\n\n\n\n\n\n\n\n')
    search_and_display_results(search_term)
