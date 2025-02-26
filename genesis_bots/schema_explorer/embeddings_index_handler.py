from annoy import AnnoyIndex
import csv, json
#from google.cloud import bigquery
from google.oauth2 import service_account
from genesis_bots.connectors import get_global_db_connector
import tempfile

from openai import OpenAI
from tqdm.auto import tqdm
import os
from tqdm.auto import tqdm
from datetime import datetime

from genesis_bots.core.logging_config import logger

from genesis_bots.llm.llm_openai.openai_utils import get_openai_client

index_file_path = './tmp/'

def fetch_embeddings_from_snow(table_id):
    # Initialize Snowflake connector

    # Initialize variables
    batch_size = 100
    offset = 0
    total_fetched = 0

    # Initialize lists to store results
    embeddings = []
    table_names = []

    # First, get the total number of rows to set up the progress bar
    total_rows_query = f"SELECT COUNT(*) as total FROM {table_id}"
    emb_db_adapter = get_global_db_connector()
    cursor = emb_db_adapter.connection.cursor()
   # logger.info('total rows query: ',total_rows_query)
    cursor.execute(total_rows_query)
    total_rows_result = cursor.fetchone()
    total_rows = total_rows_result[0]

    with tqdm(total=total_rows, desc="Fetching embeddings") as pbar:
        while True:
            #TODO update to use embedding_native column if cortex mode
            if os.environ.get("CORTEX_MODE", 'False') == 'True':
                embedding_column = 'embedding_native'
            else:
                embedding_column = 'embedding'
            # Modify the query to include LIMIT and OFFSET
            query = f"SELECT qualified_table_name, {embedding_column} FROM {table_id} LIMIT {batch_size} OFFSET {offset}"
#            logger.info('fetch query ',query)
            cursor.execute(query)
            rows = cursor.fetchall()

            # Temporary lists to hold batch results
            temp_embeddings = []
            temp_table_names = []

            for row in rows:
                try:
                    # Debug the raw string before parsing
                    if len(embeddings) == 0:
                        logger.info(f"Raw embedding string sample: {row[1][:100]}...")

                    # Current problematic parsing
                    # embedding = json.loads('['+row[1][5:-3]+']')

                    # New safer parsing approach
                    embedding_str = row[1]
                    # Remove any 'array' prefix if present
                    if embedding_str.lower().startswith('array['):
                        embedding_str = embedding_str[6:-1]  # Remove 'array[' and final ']'
                    elif embedding_str.startswith('['):
                        embedding_str = embedding_str[1:-1]  # Remove outer brackets

                    # Split and convert to float, handling scientific notation
                    embedding = [float(x.strip()) for x in embedding_str.split(',')]

                    # Verify the parsed values
                    if len(embeddings) == 0:
                        logger.info(f"First few parsed values: {embedding[:5]}")
                        import numpy as np
                        norm = np.linalg.norm(embedding)
                        logger.info(f"Parsed embedding norm: {norm}")

                    temp_embeddings.append(embedding)
                    temp_table_names.append(row[0])

                except Exception as e:
                    logger.error(f"Error parsing embedding: {str(e)}")
                    logger.error(f"Problematic row: {row[1][:100]}...")
                    continue
                  # Assuming qualified_table_name is the first column

            # Check if the batch was empty and exit the loop if so
            if not temp_embeddings:
                break

            # Append batch results to the main lists
            embeddings.extend(temp_embeddings)
            table_names.extend(temp_table_names)

            # Update counters and progress bar
            fetched = len(temp_embeddings)
            total_fetched += fetched
            pbar.update(fetched)

            if fetched < batch_size:
                # If less than batch_size rows were fetched, it's the last batch
                break

            # Increase the offset for the next batch
            offset += batch_size

    cursor.close()
 #   logger.info('table names ',table_names)
 #   logger.info('embeddings len ',len(embeddings))
    return table_names, embeddings



def load_embeddings_from_csv(csv_file_path):
    embeddings = []
    filenames = []
    with open(csv_file_path, mode='r', newline='') as csv_file:
        reader = csv.reader(csv_file)
        next(reader, None)  # Skip the header
        for row in reader:
            embedding = [float(x) for x in row[1].strip("[]").split(", ")]
            filename = row[0]
            filenames.append(filename)
            embeddings.append(embedding)
    return embeddings, filenames


def create_annoy_index(embeddings, n_trees=10):
    dimension = max(len(embedding) for embedding in embeddings)
    logger.info(f"Creating Annoy index with dimension {dimension}")

    # Verify embeddings are normalized
    import numpy as np
    norms = [np.linalg.norm(emb) for emb in embeddings]
    logger.info(f"Embedding norms min/max/mean: {min(norms):.3f}/{max(norms):.3f}/{np.mean(norms):.3f}")

    if any(abs(n - 1.0) > 0.01 for n in norms):
        logger.warning("Some embeddings are not normalized!")
        # Normalize embeddings
        embeddings = [np.array(emb)/(1e-5+np.linalg.norm(emb)) for emb in embeddings]

    index = AnnoyIndex(dimension, 'angular')
    import random
    try:
        print('starting i..')
        with tqdm(total=len(embeddings), desc="Indexing embeddings") as pbar:
            for i, embedding in enumerate(embeddings):
              #  logger.info(i)
                try:
                    index.add_item(i, embedding)
                except Exception as e:
                    print('embedding ',i,' failed, exception: ',e,' skipping...')
                pbar.update(1)
            print('index build real')
            index.build(n_trees)
    except Exception as e:
        logger.info('indexing exception: ',e)
    #logger.info('index 2 ',index)
    return index




def make_and_save_index(table_id, bot_id=None):
    emb_db_adapter = get_global_db_connector()
    table_names, embeddings = emb_db_adapter.fetch_embeddings(table_id, bot_id)

    logger.info(f"indexing {len(embeddings)} embeddings for bot {bot_id}...")

    if len(embeddings) == 0:
        embeddings = []
        if os.environ.get("CORTEX_MODE", 'False') == 'True':
            embedding_size = 768
        else:
            embedding_size = 3072
        embeddings.append( [0.0] * embedding_size)
        table_names = ['empty_index']
        logger.info("0 Embeddings found in database, saving a dummy index with size ",embedding_size," vectors")

    try:
        annoy_index = create_annoy_index(embeddings)
    except Exception as e:
        logger.info('Error on create_index: ',e)
        return None, None

    logger.info("saving index to file...")


    # Save the index to a file
    if not os.path.exists(index_file_path):
        os.makedirs(index_file_path)

    # save with timestamp filename
    emb_db_adapter = get_global_db_connector()
    index_file_name, meta_file_name = emb_db_adapter.generate_filename_from_last_modified(table_id, bot_id=bot_id)
    try:
        annoy_index.save(os.path.join(index_file_path,index_file_name))
    except Exception as e:
        logger.info('I cannot save save annoy index')
        logger.info(e)

    logger.info(f"saving mappings to timestamped cached file... {os.path.join(index_file_path,meta_file_name)}")
    with open(os.path.join(index_file_path,meta_file_name), 'w') as f:
        json.dump(table_names, f)


    # # save with default filename
    # index_file_name, meta_file_name = f'latest_cached_index_{bot_id}.ann', f'latest_cached_metadata_{bot_id}.json'
    # index_file_full_path = os.path.join(index_file_path, index_file_name)
    # if os.path.exists(index_file_full_path):
    #     try:
    #         os.remove(index_file_full_path)
    #     except Exception as e:
    #         logger.info(f"Failed to remove existing index file: {e}")
    # annoy_index.save(index_file_full_path)

    # logger.info("saving mappings to default cached files...")
    # with open(os.path.join(index_file_path,meta_filecreat_name), 'w') as f:
    #     json.dump(table_names, f)

    # logger.info(f"Annoy index saved to {os.path.join(index_file_path,index_file_name)}")
    # Save the size of the embeddings to a file

    embedding_size = len(embeddings[0])
    with open(os.path.join(index_file_path, 'index_size.txt'), 'w') as f:
        f.write(str(embedding_size))
    logger.info(f"Embedding size ({embedding_size}) saved to {os.path.join(index_file_path, 'index_size.txt')}")

    return annoy_index, table_names


# Function to get embedding (reuse or modify your existing get_embedding function)
def get_embedding(text):
    client = get_openai_client()
    #TODO if cortex mode use cortex
    response = client.embeddings.create(
        model="text-embedding-3-large",
        input=text.replace("\n", " ")  # Replace newlines with spaces
    )
    embedding = response.data[0].embedding
    return embedding

# Function to search and display results
def search_and_display_results(search_term, annoy_index, metadata_mapping):
    embedding = get_embedding(search_term)
    top_matches = annoy_index.get_nns_by_vector(embedding, 10, include_distances=True)

    paired_data = list(zip(top_matches[0], top_matches[1]))
    sorted_paired_data = sorted(paired_data, key=lambda x: x[1])

    for idx in sorted_paired_data:
        table_name = metadata_mapping[idx[0]]
        content = ""
        logger.info(f"Match: {table_name}, Score: {idx[1]}")


def load_or_create_embeddings_index(table_id, refresh=True, bot_id=None):

    # if cortex_mode then 768 else
    if os.environ.get("CORTEX_MODE", 'False') == 'True':
        embedding_size = 768
    else:
        embedding_size = 3072

    if bot_id is None:
        bot_id = 'default'

    index_file_path = './tmp/'
    # embedding_size = 3072

    emb_db_adapter = get_global_db_connector()
    index_file_name, meta_file_name = emb_db_adapter.generate_filename_from_last_modified(table_id, bot_id=bot_id)

    index_size_file = os.path.join(index_file_path, 'index_size.txt')
    if os.path.exists(index_size_file):
        with open(index_size_file, 'r') as f:
            embedding_size = int(f.read().strip())
#        logger.info(f"Embedding size ({embedding_size}) read from {index_size_file}")
    # Set the EMBEDDING_SIZE environment variable
    os.environ['EMBEDDING_SIZE'] = str(embedding_size)
   # logger.info(f"EMBEDDING_SIZE environment variable set to: {os.environ['EMBEDDING_SIZE']}")

    annoy_index = AnnoyIndex(embedding_size, 'angular')

  #  logger.info(f'loadtry  {os.path.join(index_file_path,index_file_name)}')
    if os.path.exists(os.path.join(index_file_path,index_file_name)):
        try:
      #      logger.info(f'load  {index_file_path+index_file_name}')
            try:
                annoy_index.load(os.path.join(index_file_path,index_file_name))
            except Exception as e:
                logger.debug('Error on annoy_index.load: ',e)
           # logger.info(f'index  now {annoy_index}')

            # Load the metadata mapping
       #     logger.info(f'load meta  {index_file_path+meta_file_name}')
            with open(os.path.join(index_file_path,meta_file_name), 'r') as f:
                metadata_mapping = json.load(f)
          #      logger.info(f'metadata_mapping meta  {metadata_mapping}')
          #      logger.info('metadata_mapping meta  ',metadata_mapping)

            if refresh:
                if not os.path.exists(index_file_path):
                    os.makedirs(index_file_path)
                # copy_index_file_name, copy_meta_file_name = f'latest_cached_index_{bot_id}.ann', f'latest_cached_metadata_{bot_id}.json'
                # try:
                #     annoy_index.save(os.path.join(index_file_path,copy_index_file_name))
                # except Exception as e:
                #     logger.debug('I cannot save save annoy index')
                #     logger.debug(e)

                # with open(os.path.join(index_file_path,copy_meta_file_name), 'w') as f:
                #     json.dump(metadata_mapping, f)

                #logger.info(f"Annoy Cache Manager: Existing certified fresh Annoy index copied to {index_file_path+copy_index_file_name}, {index_file_path+copy_meta_file_name}")
            else:
                pass
                #logger.info(f'Annoy Cache Manager: Existing locally cached index {index_file_path+index_file_name} loaded (may be slightly stale).')

        except OSError:
         #   logger.error("Annoy Cache Manager: Refreshing locally cached Annoy index as Harvest Results table has changed due to harvester activity")
            logger.info("Annoy Cache Manager: Refreshing locally cached Annoy index as Harvest Results table has changed due to harvester activity")
            annoy_index, metadata_mapping = make_and_save_index(table_id, bot_id=bot_id)
    else:
       # logger.error("Annoy Cache Manager: Refreshing locally cached Annoy index as Harvest Results table has changed due to harvester activity")
        logger.info(f"Annoy Cache Manager: Refreshing locally cached Annoy index for bot {bot_id} as Harvest Results table has changed due to harvester activity")
        annoy_index, metadata_mapping = make_and_save_index(table_id, bot_id=bot_id)

    return annoy_index, metadata_mapping
