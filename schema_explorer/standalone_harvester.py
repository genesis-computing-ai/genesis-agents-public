import os
import json
import sys
import time

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from connectors.bigquery_connector import BigQueryConnector
from connectors.snowflake_connector import SnowflakeConnector
from schema_explorer import SchemaExplorer
#import schema_explorer.embeddings_index_handler as embeddings_handler
import logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.WARN, format='%(asctime)s - %(levelname)s - %(message)s')

genesis_source = os.getenv('GENESIS_SOURCE',default="BigQuery")

### LLM KEY STUFF
print('Starting harvester... ')
logger.info('Starting harvester... ')

logger.info('Starting DB connection...')
if genesis_source == 'BigQuery':
    credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS',default=".secrets/gcp.json")
    with open(credentials_path) as f:
        connection_info = json.load(f)
    # Initialize BigQuery client
    harvester_db_connector = BigQueryConnector(connection_info,'BigQuery')
else:    # Initialize BigQuery client
    harvester_db_connector = SnowflakeConnector(connection_name='Snowflake')


logger.info('Getting LLM API Key...')
api_key_from_env = False
default_llm_engine = os.getenv("BOT_OS_DEFAULT_LLM_ENGINE", "openai")
llm_api_key = None

while llm_api_key == None:
    logger.info('top of while loop')

    if default_llm_engine.lower() == "openai":
        llm_api_key = os.getenv("OPENAI_API_KEY", None)
        if llm_api_key == '':
            llm_api_key = None
        if llm_api_key:
            api_key_from_env = True
    elif default_llm_engine.lower() == "reka":
        llm_api_key = os.getenv("REKA_API_KEY", None)
        if llm_api_key: 
            api_key_from_env = True

    if genesis_source == 'BigQuery' and api_key_from_env == False:
        while True:
            print('!!!!! Loading LLM API Key from File No longer Supported -- Please provide via ENV VAR when using BigQuery Source')
            time.sleep(3)
    
    print('Checking database for LLM Key...', flush=True)
    logger.info('Checking database for LLM Key...', flush=True)
    if llm_api_key is None and genesis_source == 'Snowflake':
        llm_key, llm_type = harvester_db_connector.db_get_llm_key(project_id=None, dataset_name=None)
        logger.info('got a response')
        if llm_key == None or llm_key == '' or llm_key == 'NULL' or len(llm_key)<10:
            logger.info('Llm key is None')
            llm_key = None
            llm_type = None
        if llm_key and llm_type:
            default_llm_engine = llm_type
            llm_api_key = llm_key
            api_key_from_env = False
            logger.info("LLM Key loaded from Database")
        else:
            print("===========")
            print("NOTE: LLM Key not found in Env Var nor in Database LLM_CONFIG table.. starting without LLM Key, please provide via Streamlit")
            print("===========", flush=True)

    if llm_api_key is not None and default_llm_engine.lower() == 'openai':
        os.environ["OPENAI_API_KEY"] = llm_api_key
    if llm_api_key is not None and default_llm_engine.lower() == 'reka':
        os.environ["REKA_API_KEY"] = llm_api_key

    if llm_api_key is None:
        print('No LLM Key Available in ENV var or Snowflake database, sleeping 20 seconds before retry.', flush=True)
        time.sleep(20)


### END LLM KEY STUFF
logger.info('Out of LLM section .. calling ensure_table_exists -- ')

# Initialize the BigQueryConnector with your connection info
harvester_db_connector.ensure_table_exists()

# Initialize the SchemaExplorer with the BigQuery connector
schema_explorer = SchemaExplorer(harvester_db_connector)

# Now, you can call methods on your schema_ex
# 
#databases = bigquery_connector.get_databases()
# print all databases
#print("Databases:", databases)


# Check for new databases in Snowflake and add them to the harvest include list with schema_exclude of INFORMATION_SCHEMA
def update_harvest_control_with_new_databases(connector):
    available_databases = connector.get_visible_databases()
    controlled_databases = [db['database_name'] for db in connector.get_databases()]

    internal_schema = os.getenv('GENESIS_INTERNAL_DB_SCHEMA', None)
    internal_db, internal_sch = internal_schema.split('.') if '.' in internal_schema else None

    for db in available_databases:
        if db not in controlled_databases and db not in {'GENESISAPP_APP_PKG_EXT', 'GENESISAPP_APP_PKG'}:
            print(f"Adding new database to harvest control: {db}, the system db is {internal_db}", flush=True)
            schema_exclusions = ['INFORMATION_SCHEMA']
            if db.upper() == internal_db.upper():
                schema_exclusions.append(internal_sch)
                schema_exclusions.append('CORE')
                schema_exclusions.append('APP')
                
            connector.set_harvest_control_data(
                source_name='Snowflake',
                database_name=db,
                schema_exclusions=schema_exclusions
            )
# Check and update harvest control data if the source is Snowflake

refresh_seconds = os.getenv("HARVESTER_REFRESH_SECONDS", 120)
refresh_seconds = int(refresh_seconds)

print("     [-------]     ")
print("    [         ]    ")
print("   [  0   0  ]   ")
print("  [    ---    ]  ")
print(" [______] ")
print("     /     \\     ")
print("    /|  o  |\\    ")
print("   / |____| \\   ")
print("      |  |       ")
print("      |  |       ")
print("     /    \\      ")
print("    /      \\     ")
print("   /        \\    ")
print("  G E N E S I S ")
print("    B o t O S")
print(" ---- HARVESTER----")
print('Harvester Start Version 0.125',flush=True)
print("")

while True:
    if genesis_source == 'Snowflake' and os.getenv('AUTO_HARVEST', 'TRUE').upper() == 'TRUE':
        print('Checking for any newly granted databases to add to harvest...', flush=True)
        update_harvest_control_with_new_databases(harvester_db_connector)
    

    print(f"Checking for new tables... (once per {refresh_seconds} seconds)", flush=True)
    #embeddings_handler.load_or_create_embeddings_index(bigquery_connector.metadata_table_name, refresh=True)
    schema_explorer.explore_and_summarize_tables_parallel()
    #print("Checking Cached Annoy Index")
  #  logger.info(f"Checking for new semantic models... (once per {refresh_seconds} seconds)")
  #  schema_explorer.explore_semantic_models()
    #embeddings_handler.make_and_save_index(bigquery_connector.metadata_table_name)
    print(f'Pausing for {int(refresh_seconds)} seconds before next check.', flush=True)
    time.sleep(refresh_seconds)
