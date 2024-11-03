from annoy import AnnoyIndex
import csv, json
from google.cloud import bigquery
from google.oauth2 import service_account
from connectors.snowflake_connector.snowflake_connector import SnowflakeConnector
from connectors.sqlite_connector import SqliteConnector
from connectors.bigquery_connector import BigQueryConnector
import tempfile

from openai import OpenAI
from tqdm.auto import tqdm
import os 
from tqdm.auto import tqdm
from datetime import datetime

from core.logging_config import setup_logger
logger = setup_logger(__name__)

from llm_openai.openai_utils import get_openai_client

genesis_source = os.getenv('GENESIS_SOURCE',default="BigQuery")
emb_connection = genesis_source
if genesis_source == 'BigQuery':
    emb_db_adapter = BigQueryConnector(connection_name="Sqlite")
elif genesis_source == 'Sqlite':
    emb_db_adapter = SqliteConnector(connection_name="Sqlite")
elif genesis_source == 'Snowflake':    
    emb_db_adapter = SnowflakeConnector(connection_name='Snowflake')
else:
    raise ValueError('Invalud Source')


index_file_path = './tmp/'
def _get_bigquery_connection():
    # Create a BigQuery client
    credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS',default=".secrets/gcp.json")
    with open(credentials_path) as f:
        connection_info = json.load(f)
    credentials = service_account.Credentials.from_service_account_info(connection_info)
    return bigquery.Client(credentials=credentials, project=connection_info['project_id'])


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
                    temp_embeddings.append(json.loads('['+row[1][5:-3]+']'))
                    temp_table_names.append(row[0])
#                    logger.info('temp_embeddings len: ',len(temp_embeddings))
#                    logger.info('temp table_names: ',temp_table_names)
                except:
                    try:
                        temp_embeddings.append(json.loads('['+row[1][5:-10]+']'))
                        temp_table_names.append(row[0])
                    except:
                        logger.info('Cant load array from Snowflake')
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

def fetch_embeddings_from_bq(table_id):
    client = _get_bigquery_connection()

    # Initialize variables
    batch_size = 100
    offset = 0
    total_fetched = 0

    # Initialize lists to store results
    embeddings = []
    table_names = []

    # First, get the total number of rows to set up the progress bar
    total_rows_query = f"""
        SELECT COUNT(*) as total
        FROM `{table_id}`
    """
    total_rows_result = client.query(total_rows_query).to_dataframe()
    total_rows = total_rows_result.total[0]

    with tqdm(total=total_rows, desc="Fetching embeddings") as pbar:
        while True:
            # Modify the query to include LIMIT and OFFSET
            query = f"""
                SELECT qualified_table_name, embedding
                FROM `{table_id}`
                LIMIT {batch_size} OFFSET {offset}
            """
            query_job = client.query(query)

            # Temporary lists to hold batch results
            temp_embeddings = []
            temp_table_names = []

            for row in query_job:
                temp_embeddings.append(row.embedding)
                temp_table_names.append(row.qualified_table_name)

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
    
   # logger.info('creating index')
   # logger.info('len embeddings ',len(embeddings))
    dimension = max(len(embedding) for embedding in embeddings)
    # Find the longest embedding length
   # logger.info('dimension ',dimension)

    index = AnnoyIndex(dimension, 'angular')  # Using angular distance
    #logger.info('index 1 ',index)

    try:
        with tqdm(total=len(embeddings), desc="Indexing embeddings") as pbar:
            for i, embedding in enumerate(embeddings):
              #  logger.info(i)
                try:
                    index.add_item(i, embedding)
                except Exception as e:
                    logger.info('embedding ',i,' failed, exception: ',e,' skipping...')
                pbar.update(1)
            index.build(n_trees)
    except Exception as e:
        logger.info('indexing exception: ',e)
    #logger.info('index 2 ',index)
    return index




def make_and_save_index(table_id):
    
    table_names, embeddings = emb_db_adapter.fetch_embeddings(table_id)
    
    logger.info("indexing ",len(embeddings)," embeddings...", end="")

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

    logger.info("saving index to file...")
    
 
    # Save the index to a file
    if not os.path.exists(index_file_path):
        os.makedirs(index_file_path)

    # save with timestamp filename
    
    index_file_name, meta_file_name = emb_db_adapter.generate_filename_from_last_modified(table_id)
    try:
        annoy_index.save(os.path.join(index_file_path,index_file_name))
    except Exception as e:
        logger.info('I cannot save save annoy index')
        logger.info(e)

    logger.info(f"saving mappings to timestamped cached file... {os.path.join(index_file_path,meta_file_name)}")
    with open(os.path.join(index_file_path,meta_file_name), 'w') as f:
        json.dump(table_names, f)

    # save with default filename
    index_file_name, meta_file_name = 'latest_cached_index.ann', 'latest_cached_metadata.json'
    annoy_index.save(os.path.join(index_file_path,index_file_name))

    logger.info("saving mappings to default cached files...")
    with open(os.path.join(index_file_path,meta_file_name), 'w') as f:
        json.dump(table_names, f)

    logger.info(f"Annoy index saved to {os.path.join(index_file_path,index_file_name)}")
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


def load_or_create_embeddings_index(table_id, refresh=True):

    import logging
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.WARN, format='%(asctime)s - %(levelname)s - %(message)s')

    # if cortex_mode then 768 else
    if os.environ.get("CORTEX_MODE", 'False') == 'True':
        embedding_size = 768
    else:
        embedding_size = 3072

    index_file_path = './tmp/'
    # embedding_size = 3072

    if refresh:
        index_file_name, meta_file_name = emb_db_adapter.generate_filename_from_last_modified(table_id)
    else:
        index_file_name, meta_file_name = 'latest_cached_index.ann', 'latest_cached_metadata.json'

    index_size_file = os.path.join(index_file_path, 'index_size.txt')
    if os.path.exists(index_size_file):
        with open(index_size_file, 'r') as f:
            embedding_size = int(f.read().strip())
#        logger.info(f"Embedding size ({embedding_size}) read from {index_size_file}")
    # Set the EMBEDDING_SIZE environment variable
    os.environ['EMBEDDING_SIZE'] = str(embedding_size)
   # logger.info(f"EMBEDDING_SIZE environment variable set to: {os.environ['EMBEDDING_SIZE']}")

    annoy_index = AnnoyIndex(embedding_size, 'angular')

    logger.info(f'loadtry  {os.path.join(index_file_path,index_file_name)}')
    if os.path.exists(os.path.join(index_file_path,index_file_name)):
        try:
      #      logger.info(f'load  {index_file_path+index_file_name}')
            try:
                annoy_index.load(os.path.join(index_file_path,index_file_name))
            except Exception as e:
                logger.info('Error on annoy_index.load: ',e)
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
                copy_index_file_name, copy_meta_file_name = 'latest_cached_index.ann', 'latest_cached_metadata.json'
                try:
                    annoy_index.save(os.path.join(index_file_path,copy_index_file_name))
                except Exception as e:
                    logger.info('I cannot save save annoy index')
                    logger.info(e)                

                with open(os.path.join(index_file_path,copy_meta_file_name), 'w') as f:
                    json.dump(metadata_mapping, f)

                #logger.info(f"Annoy Cache Manager: Existing certified fresh Annoy index copied to {index_file_path+copy_index_file_name}, {index_file_path+copy_meta_file_name}")
            else:
                pass
                #logger.info(f'Annoy Cache Manager: Existing locally cached index {index_file_path+index_file_name} loaded (may be slightly stale).')

        except OSError:
         #   logger.error("Annoy Cache Manager: Refreshing locally cached Annoy index as Harvest Results table has changed due to harvester activity")
            logger.info("Annoy Cache Manager: Refreshing locally cached Annoy index as Harvest Results table has changed due to harvester activity")
            annoy_index, metadata_mapping = make_and_save_index(table_id)
    else:
       # logger.error("Annoy Cache Manager: Refreshing locally cached Annoy index as Harvest Results table has changed due to harvester activity")
        logger.info("Annoy Cache Manager: Refreshing locally cached Annoy index as Harvest Results table has changed due to harvester activity")
        annoy_index, metadata_mapping = make_and_save_index(table_id)

    logger.info(f'returning  {annoy_index},{metadata_mapping}')
    # logger.info('returning  ',annoy_index,metadata_mapping)
    return annoy_index, metadata_mapping
#table_id = "hello-prototype.ELSA_INTERNAL.database_harvest"
#annoy_index, metadata_mapping = load_or_create_embeddings_index(table_id)

#while True:
#    search_term = input("Enter your search term: ")
#    logger.info('\n\n')
#    search_and_display_results(search_term, annoy_index, metadata_mapping)


