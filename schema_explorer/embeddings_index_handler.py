from annoy import AnnoyIndex
import csv, json
from google.cloud import bigquery
from google.oauth2 import service_account
from connectors.snowflake_connector import SnowflakeConnector
from connectors.sqlite_connector import SqliteConnector
from connectors.bigquery_connector import BigQueryConnector
import tempfile

from openai import OpenAI
from tqdm.auto import tqdm
import os 
from tqdm.auto import tqdm
from datetime import datetime

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
    
   # print('creating index')
   # print('len embeddings ',len(embeddings))
    dimension = len(embeddings[0])  # Assuming all embeddings have the same dimension
   # print('dimension ',dimension)

    index = AnnoyIndex(dimension, 'angular')  # Using angular distance
    #print('index 1 ',index)

    try:
        with tqdm(total=len(embeddings), desc="Indexing embeddings") as pbar:
            for i, embedding in enumerate(embeddings):
                index.add_item(i, embedding)
                pbar.update(i)
            index.build(n_trees)
    except Exception as e:
        print('indexing exception: ',e)
    #print('index 2 ',index)
    return index




def make_and_save_index(table_id):
    
    table_names, embeddings = emb_db_adapter.fetch_embeddings(table_id)
    
    print("indexing ",len(embeddings)," embeddings...", end="")

    if len(embeddings) == 0:
        embeddings = []
        embeddings.append( [0.0] * 3072)
        table_names = ['empty_index']
        print("0 Embeddings found in database, saving a dummy index")

    try:
        annoy_index = create_annoy_index(embeddings)
    except Exception as e:
        print('Error on create_index: ',e)

    print("saving index to file...")
 
    # Save the index to a file
    if not os.path.exists(index_file_path):
        os.makedirs(index_file_path)

    # save with timestamp filename
    
    index_file_name, meta_file_name = emb_db_adapter.generate_filename_from_last_modified(table_id)
    annoy_index.save(os.path.join(index_file_path,index_file_name))

    print(f"saving mappings to timestamped cached file... {os.path.join(index_file_path,meta_file_name)}")
    with open(os.path.join(index_file_path,meta_file_name), 'w') as f:
        json.dump(table_names, f)

    # save with default filename
    index_file_name, meta_file_name = 'latest_cached_index.ann', 'latest_cached_metadata.json'
    annoy_index.save(os.path.join(index_file_path,index_file_name))

    print("saving mappings to default cached files...")
    with open(os.path.join(index_file_path,meta_file_name), 'w') as f:
        json.dump(table_names, f)

    print(f"Annoy index saved to {os.path.join(index_file_path,index_file_name)}")

    return annoy_index, table_names


# Function to get embedding (reuse or modify your existing get_embedding function)
def get_embedding(text):
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
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
        print(f"Match: {table_name}, Score: {idx[1]}")


def load_or_create_embeddings_index(table_id, refresh=True):

    import logging
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.WARN, format='%(asctime)s - %(levelname)s - %(message)s')

    embedding_size = 3072

    if refresh:
        index_file_name, meta_file_name = emb_db_adapter.generate_filename_from_last_modified(table_id)
    else:
        index_file_name, meta_file_name = 'latest_cached_index.ann', 'latest_cached_metadata.json'

    annoy_index = AnnoyIndex(embedding_size, 'angular')

    logger.info(f'loadtry  {os.path.join(index_file_path,index_file_name)}')
    if os.path.exists(os.path.join(index_file_path,index_file_name)):
        try:
      #      logger.info(f'load  {index_file_path+index_file_name}')
            annoy_index.load(os.path.join(index_file_path,index_file_name))
           # logger.info(f'index  now {annoy_index}')

            # Load the metadata mapping
       #     logger.info(f'load meta  {index_file_path+meta_file_name}')
            with open(os.path.join(index_file_path,meta_file_name), 'r') as f:
                metadata_mapping = json.load(f)
          #      logger.info(f'metadata_mapping meta  {metadata_mapping}')
          #      print('metadata_mapping meta  ',metadata_mapping)

            if refresh:                                
                if not os.path.exists(index_file_path):
                    os.makedirs(index_file_path)
                copy_index_file_name, copy_meta_file_name = 'latest_cached_index.ann', 'latest_cached_metadata.json'
                annoy_index.save(os.path.join(index_file_path,copy_index_file_name))

                with open(os.path.join(index_file_path,copy_meta_file_name), 'w') as f:
                    json.dump(metadata_mapping, f)

                #logger.info(f"Annoy Cache Manager: Existing certified fresh Annoy index copied to {index_file_path+copy_index_file_name}, {index_file_path+copy_meta_file_name}")
            else:
                pass
                #logger.info(f'Annoy Cache Manager: Existing locally cached index {index_file_path+index_file_name} loaded (may be slightly stale).')

        except OSError:
            logger.error("Annoy Cache Manager: Refreshing locally cached Annoy index as Harvest Results table has changed due to harvester activity")
            print("Annoy Cache Manager: Refreshing locally cached Annoy index as Harvest Results table has changed due to harvester activity")
            annoy_index, metadata_mapping = make_and_save_index(table_id)
    else:
        logger.error("Annoy Cache Manager: Refreshing locally cached Annoy index as Harvest Results table has changed due to harvester activity")
        print("Annoy Cache Manager: Refreshing locally cached Annoy index as Harvest Results table has changed due to harvester activity")
        annoy_index, metadata_mapping = make_and_save_index(table_id)

    logger.info(f'returning  {annoy_index},{metadata_mapping}')
   # print('returning  ',annoy_index,metadata_mapping)
    return annoy_index, metadata_mapping
#table_id = "hello-prototype.ELSA_INTERNAL.database_harvest"
#annoy_index, metadata_mapping = load_or_create_embeddings_index(table_id)

#while True:
#    search_term = input("Enter your search term: ")
#    print('\n\n')
#    search_and_display_results(search_term, annoy_index, metadata_mapping)


