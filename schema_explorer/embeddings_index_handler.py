from annoy import AnnoyIndex
import csv, json
from google.cloud import bigquery
from google.oauth2 import service_account
from connectors.snowflake_connector import SnowflakeConnector
from connectors.sqlite_connector import SqliteConnector

from openai import OpenAI
from tqdm.auto import tqdm
import os 
from tqdm.auto import tqdm
from datetime import datetime

genesis_source = os.getenv('GENESIS_SOURCE',default="BigQuery")

if genesis_source == 'BigQuery':
    emb_connection = 'BigQuery'
elif genesis_source == 'Sqlite':
    emb_db_adapter = SqliteConnector(connection_name="Sqlite")
elif genesis_source == 'Snowflake':    
    emb_db_adapter = SnowflakeConnector(connection_name='Snowflake')
    emb_connection = 'Snowflake'
else:
    raise ValueError('Invalud Source')


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
   # print('total rows query: ',total_rows_query)
    cursor.execute(total_rows_query)
    total_rows_result = cursor.fetchone()
    total_rows = total_rows_result[0]

    with tqdm(total=total_rows, desc="Fetching embeddings") as pbar:
        while True:
            # Modify the query to include LIMIT and OFFSET
            query = f"SELECT qualified_table_name, embedding FROM {table_id} LIMIT {batch_size} OFFSET {offset}"
#            print('fetch query ',query)
            cursor.execute(query)
            rows = cursor.fetchall()

            # Temporary lists to hold batch results
            temp_embeddings = []
            temp_table_names = []

            for row in rows:
                try:
                    temp_embeddings.append(json.loads('['+row[1][5:-3]+']'))
                    temp_table_names.append(row[0])
#                    print('temp_embeddings len: ',len(temp_embeddings))
#                    print('temp table_names: ',temp_table_names)
                except:
                    try:
                        temp_embeddings.append(json.loads('['+row[1][5:-10]+']'))
                        temp_table_names.append(row[0])
                    except:
                        print('Cant load array from Snowflake')
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
 #   print('table names ',table_names)
 #   print('embeddings len ',len(embeddings))
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


def generate_filename_from_last_modified(table_id):
    client = _get_bigquery_connection()

    try:
        # Fetch the table
        table = client.get_table(table_id)

        # Ensure we have a valid datetime object for `modified`
        if table.modified is None:
            raise ValueError("Table modified time is None. Unable to generate filename.")

        # The `modified` attribute should be a datetime object. Format it.
        last_modified_time = table.modified
        timestamp_str = last_modified_time.strftime("%Y%m%dT%H%M%S") + "Z"

        # Create the filename with the .ann extension
        filename = f"{timestamp_str}.ann"
        metafilename = f"{timestamp_str}.json"
        return filename, metafilename
    except Exception as e:
        # Handle errors: for example, table not found, or API errors
        #print(f"An error occurred: {e}")
        # Return a default filename or re-raise the exception based on your use case
        return "default_filename.ann", "default_metadata.json"

def snow_generate_filename_from_last_modified(table_id):

    database, schema, table = table_id.split('.')

    try:
        # Fetch the maximum LAST_CRAWLED_TIMESTAMP from the harvest_results table
        query = f"SELECT MAX(LAST_CRAWLED_TIMESTAMP) AS last_crawled_time FROM {database}.{schema}.HARVEST_RESULTS"
        cursor = emb_db_adapter.connection.cursor()

        cursor.execute(query)
        bots = cursor.fetchall()
        if bots is not None:
            columns = [col[0].lower() for col in cursor.description]
            result = [dict(zip(columns, bot)) for bot in bots]
        else:
            result = None
        cursor.close()


        # Ensure we have a valid result and last_crawled_time is not None
        if not result or result[0]['last_crawled_time'] is None:
            raise ValueError("No data crawled - This is expected on fresh install.")
            return('NO_DATA_CRAWLED')
            #raise ValueError("Table last crawled timestamp is None. Unable to generate filename.")

        # The `last_crawled_time` attribute should be a datetime object. Format it.
        last_crawled_time = result[0]['last_crawled_time']
        timestamp_str = last_crawled_time.strftime("%Y%m%dT%H%M%S") + "Z"

        # Create the filename with the .ann extension
        filename = f"{timestamp_str}.ann"
        metafilename = f"{timestamp_str}.json"
        return filename, metafilename
    except Exception as e:
        # Handle errors: for example, table not found, or API errors
        #print(f"An error occurred: {e}, possibly no data yet harvested, using default name for index file.")
        # Return a default filename or re-raise the exception based on your use case
        return "default_filename.ann", "default_metadata.json"



def make_and_save_index(table_id):



    if emb_connection == 'Snowflake':
        table_names, embeddings = fetch_embeddings_from_snow(table_id)
    else:
        table_names, embeddings = fetch_embeddings_from_bq(table_id)

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

    index_file_path = './tmp/'

    if not os.path.exists(index_file_path):
        os.makedirs(index_file_path)

    # save with timestamp filename

    if emb_connection == 'Snowflake':
        index_file_name, meta_file_name = snow_generate_filename_from_last_modified(table_id)
    else:
        index_file_name, meta_file_name = generate_filename_from_last_modified(table_id)
    annoy_index.save(index_file_path+index_file_name)

    print(f"saving mappings to timestamped cached file... {index_file_path+meta_file_name}")
    with open(index_file_path+meta_file_name, 'w') as f:
        json.dump(table_names, f)

    # save with default filename
    index_file_name, meta_file_name = 'latest_cached_index.ann', 'latest_cached_metadata.json'
    annoy_index.save(index_file_path+index_file_name)

    print("saving mappings to default cached files...")
    with open(index_file_path+meta_file_name, 'w') as f:
        json.dump(table_names, f)

    print(f"Annoy index saved to {index_file_path+index_file_name}")

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

    index_file_path = './tmp/'

    if refresh:
        if emb_connection == 'Snowflake':
     #       logger.info(f'its snow... {emb_connection}')
            index_file_name, meta_file_name = snow_generate_filename_from_last_modified(table_id)
      #      logger.info(f'filenames {index_file_name},{meta_file_name}')
        else:
            index_file_name, meta_file_name = generate_filename_from_last_modified(table_id)
    else:
        index_file_name, meta_file_name = 'latest_cached_index.ann', 'latest_cached_metadata.json'

    annoy_index = AnnoyIndex(embedding_size, 'angular')

    logger.info(f'loadtry  {index_file_path+index_file_name}')
    if os.path.exists(index_file_path+index_file_name):
        try:
      #      logger.info(f'load  {index_file_path+index_file_name}')
            annoy_index.load(index_file_path+index_file_name)
           # logger.info(f'index  now {annoy_index}')

            # Load the metadata mapping
       #     logger.info(f'load meta  {index_file_path+meta_file_name}')
            with open(index_file_path+meta_file_name, 'r') as f:
                metadata_mapping = json.load(f)
          #      logger.info(f'metadata_mapping meta  {metadata_mapping}')
          #      print('metadata_mapping meta  ',metadata_mapping)

            if refresh:
                index_file_path = './tmp/'
                if not os.path.exists(index_file_path):
                    os.makedirs(index_file_path)
                copy_index_file_name, copy_meta_file_name = 'latest_cached_index.ann', 'latest_cached_metadata.json'
                annoy_index.save(index_file_path+copy_index_file_name)

                with open(index_file_path+copy_meta_file_name, 'w') as f:
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


