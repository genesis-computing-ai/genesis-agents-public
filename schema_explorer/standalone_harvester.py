import os
import json
import sys
import time

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from connectors.bigquery_connector import BigQueryConnector
from connectors.snowflake_connector import SnowflakeConnector
from connectors.sqlite_connector import SqliteConnector
from schema_explorer import SchemaExplorer
from core.bot_os_llm import LLMKeyHandler 
#import schema_explorer.embeddings_index_handler as embeddings_handler
import logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.WARN, format='%(asctime)s - %(levelname)s - %(message)s')

genesis_source = os.getenv('GENESIS_SOURCE',default="BigQuery")

print("waiting 60 seconds for other services to start first...", flush=True)
if os.getenv('HARVEST_TEST', 'FALSE') != 'TRUE':
    time.sleep(60)

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
elif genesis_source ==  'Sqlite':
    harvester_db_connector = SqliteConnector(connection_name='Sqlite') 
elif genesis_source == 'Snowflake':    # Initialize BigQuery client
    harvester_db_connector = SnowflakeConnector(connection_name='Snowflake')
else:
    raise ValueError('Invalid Source')

# from core.bot_os_llm import LLMKeyHandler 
# llm_key_handler = LLMKeyHandler()
logger.info('Getting LLM API Key...')
# api_key_from_env, llm_api_key = llm_key_handler.get_llm_key_from_db()

def get_llm_api_key():
    from core.bot_os_llm import LLMKeyHandler 
    logger.info('Getting LLM API Key...')
    api_key_from_env = False
    llm_type = os.getenv("BOT_OS_DEFAULT_LLM_ENGINE", "openai")
    llm_api_key = None

    i = 0
    c = 0

    while llm_api_key == None:

        i = i + 1
        if i > 100:
            c += 1
            print(f'Waiting on LLM key... (cycle {c})')
            i = 0 
        # llm_type = None
        llm_key_handler = LLMKeyHandler()
        logger.info('Getting LLM API Key...')

        api_key_from_env, llm_api_key, llm_type = llm_key_handler.get_llm_key_from_db()

        if llm_api_key is None and llm_api_key != 'cortex_no_key_needed':
        #   print('No LLM Key Available in ENV var or Snowflake database, sleeping 20 seconds before retry.', flush=True)
            time.sleep(20)
        else:
            logger.info(f"Using {llm_type} for harvester ")
        
        return llm_api_key, llm_type

llm_api_key, llm_type = get_llm_api_key()

### END LLM KEY STUFF
logger.info('Out of LLM check section .. calling ensure_table_exists -- ')

# Initialize the BigQueryConnector with your connection info
harvester_db_connector.ensure_table_exists()

# Initialize the SchemaExplorer with the BigQuery connector
schema_explorer = SchemaExplorer(harvester_db_connector,llm_api_key)

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
    if internal_schema is not None:
         internal_schema = internal_schema.upper()
    internal_db, internal_sch = internal_schema.split('.') if '.' in internal_schema else None

    for db in available_databases:
        if db not in controlled_databases and db not in {'GENESISAPP_APP_PKG_EXT', 'GENESISAPP_APP_PKG'}:
            print(f"Adding new database to harvest control -- the system db is {internal_db}", flush=True)
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

refresh_seconds = os.getenv("HARVESTER_REFRESH_SECONDS", 60)

refresh_seconds = int(refresh_seconds)
if os.getenv("HARVEST_TEST", "FALSE").upper() == "TRUE":
    refresh_seconds = 5


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
print('Harvester Start Version 0.150',flush=True)


while True:
    llm_api_key = get_llm_api_key()
    if genesis_source == 'Snowflake' and os.getenv('AUTO_HARVEST', 'TRUE').upper() == 'TRUE':
        print('Checking for any newly granted databases to add to harvest...', flush=True)
        update_harvest_control_with_new_databases(harvester_db_connector)
    
    print(f"Checking for new tables... (once per {refresh_seconds} seconds)",flush=True)
    sys.stdout.write(f"Checking for new tables... (once per {refresh_seconds} seconds)...\n")
    sys.stdout.flush()
    #embeddings_handler.load_or_create_embeddings_index(bigquery_connector.metadata_table_name, refresh=True)
    print('Checking if LLM API Key updated for harvester...')
    llm_key_handler = LLMKeyHandler()
    latest_llm_type = None
    api_key_from_env, llm_api_key, latest_llm_type = llm_key_handler.get_llm_key_from_db(harvester_db_connector)
    if latest_llm_type != llm_type:
        print(f"Now using {latest_llm_type} instead of {llm_type} for harvester ")
        
    schema_explorer.explore_and_summarize_tables_parallel()
    #print("Checking Cached Annoy Index")
  #  logger.info(f"Checking for new semantic models... (once per {refresh_seconds} seconds)")
  #  schema_explorer.explore_semantic_models()
    #embeddings_handler.make_and_save_index(bigquery_connector.metadata_table_name)
    sys.stdout.write(f'Pausing for {int(refresh_seconds)} seconds before next check.')
    sys.stdout.flush()
    time.sleep(refresh_seconds)
