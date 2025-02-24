import os
import json
import sys
import time
from datetime import datetime, timedelta

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

#from genesis_bots.connectors.bigquery_connector import BigQueryConnector
from genesis_bots.connectors.snowflake_connector.snowflake_connector import SnowflakeConnector
from genesis_bots.connectors.sqlite_connector import SqliteConnector
from genesis_bots.schema_explorer import SchemaExplorer
from genesis_bots.core.bot_os_llm import LLMKeyHandler
#import schema_explorer.embeddings_index_handler as embeddings_handler
from genesis_bots.core.logging_config import logger
genesis_source = os.getenv('GENESIS_SOURCE',default="Snowflake")

logger.info("waiting 60 seconds for other services to start first...")
if os.getenv('HARVEST_TEST', 'FALSE').upper() != 'TRUE' and os.getenv('HARVEST_NO_WAIT', 'FALSE').upper() != 'TRUE':
    time.sleep(60)

### LLM KEY STUFF
logger.info('Starting harvester... ')
logger.info('Starting harvester... ')

logger.info('Starting DB connection...')
if genesis_source == 'BigQuery':
    raise NotImplementedError("BigQueryConnector is not implemented")
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

def get_llm_api_key(db_adapter):
    from genesis_bots.core.bot_os_llm import LLMKeyHandler
    logger.info('Getting LLM API Key...')
    api_key_from_env = False
    llm_type = os.getenv("BOT_OS_DEFAULT_LLM_ENGINE", "openai")
    llm_api_key_struct = None

    i = 0
    c = 0

    while llm_api_key_struct == None:

        refresh_seconds = 180
        wake_up = False
        first_pass = True
        # skip the sleeping on bots inactive if we are running locally or on Sqlite metadata
        if db_adapter.source_name == "SQLite":
            wake_up = True
        if os.getenv("SPCS_MODE", "FALSE").upper() == "FALSE":
            wake_up = True
        while not wake_up:
            ii = 0

            try:
                cursor = db_adapter.client.cursor()
                check_bot_active = f"DESCRIBE TABLE {db_adapter.schema}.BOTS_ACTIVE"
                cursor.execute(check_bot_active)
                result = cursor.fetchone()

                bot_active_time_dt = datetime.strptime(result[0], '%Y-%m-%d %H:%M:%S %Z')
                current_time = datetime.now()
                time_difference = current_time - bot_active_time_dt


                ii += 1
                if ii >= 30:
                    logger.info(f"BOTS ACTIVE TIME: {result[0]} | CURRENT TIME: {current_time} | TIME DIFFERENCE: {time_difference} | producer")
                    ii = 0

                if time_difference < timedelta(minutes=5) or os.getenv("HARVEST_TEST", "false").lower() == "true":
                    wake_up = True
                else:
                    if first_pass:
                        logger.info("Waiting for bots to be active as running in non-local (Snowflake SCPS) mode. Set SPCS_MODE=FALSE to prevent this sleeping.")
                        first_pass = False
                    time.sleep(refresh_seconds)
            except:
                logger.info('Waiting for BOTS_ACTIVE table to be created...')
                time.sleep(refresh_seconds)

        i = i + 1
        if i > 100:
            c += 1
            logger.info(f'Waiting on LLM key... (cycle {c})')
            i = 0
        # llm_type = None
        llm_key_handler = LLMKeyHandler(db_adapter)
        logger.info('Getting LLM API Key...')

        api_key_from_env, llm_api_key_struct = llm_key_handler.get_llm_key_from_db()

        if llm_api_key_struct.llm_key is None and llm_api_key_struct.llm_key != 'cortex_no_key_needed':
        #   logger.info('No LLM Key Available in ENV var or Snowflake database, sleeping 20 seconds before retry.')
            time.sleep(180)
        else:
            logger.info(f"Using {llm_type} for harvester ")

    return llm_api_key_struct

llm_api_key_struct = get_llm_api_key(harvester_db_connector)

### END LLM KEY STUFF
logger.info('Out of LLM check section .. calling ensure_table_exists -- ')

# Initialize the BigQueryConnector with your connection info
harvester_db_connector.ensure_table_exists()

# Initialize the SchemaExplorer with the BigQuery connector
schema_explorer = SchemaExplorer(harvester_db_connector,llm_api_key_struct.llm_key)

# Now, you can call methods on your schema_ex
#
#databases = bigquery_connector.get_databases()
# print all databases
#logger.info("Databases:", databases)


# Check for new databases in Snowflake and add them to the harvest include list with schema_exclude of INFORMATION_SCHEMA
def update_harvest_control_with_new_databases(connector):
    available_databases = connector.get_visible_databases()
    controlled_databases = [db['database_name'] for db in connector.get_databases()]

    internal_schema = os.getenv('GENESIS_INTERNAL_DB_SCHEMA', None)
    if internal_schema is not None:
         internal_schema = internal_schema.upper()
    internal_db, internal_sch = internal_schema.split('.') if '.' in internal_schema else None

    for db in available_databases:
        if db not in controlled_databases and db not in {'GENESISAPP_APP_PKG_EXT', 'GENESISAPP_APP_PKG', 'SNOWFLAKE'}:
            logger.info(f"Adding new database to harvest control -- the system db is {internal_db}")
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


logger.info("    ┌───────┐     ")
logger.info("   ╔═════════╗    ")
logger.info("  ║   ◉   ◉   ║   ")
logger.info("  ║    ───    ║  ")
logger.info("  ╚═══════════╝ ")
logger.info("     ╱     ╲     ")
logger.info("    ╱│  ◯  │╲    ")
logger.info("   ╱ │_____│ ╲   ")
logger.info("      │   │      ")
logger.info("      │   │      ")
logger.info("     ╱     ╲     ")
logger.info("    ╱       ╲    ")
logger.info("   ╱         ╲   ")
logger.info("  G E N E S I S ")
logger.info("    B o t O S")
logger.info(" ---- HARVESTER----")
logger.info('****** GENBOT VERSION 0.300 *******')


while True:
    llm_api_key_struct = get_llm_api_key(harvester_db_connector)
    if genesis_source == 'Snowflake' and os.getenv('AUTO_HARVEST', 'TRUE').upper() == 'TRUE':
        logger.info('Checking for any newly granted databases to add to harvest...')
        update_harvest_control_with_new_databases(harvester_db_connector)

    logger.info(f"Checking for new tables... (once per {refresh_seconds} seconds)")
 #   sys.stdout.write(f"Checking for new tables... (once per {refresh_seconds} seconds)...\n")
 #   sys.stdout.flush()
    llm_key_handler = LLMKeyHandler(harvester_db_connector)
    latest_llm_type = None
    api_key_from_env, latest_llm_api_key_struct = llm_key_handler.get_llm_key_from_db(harvester_db_connector)
    if latest_llm_api_key_struct.llm_type != llm_api_key_struct.llm_type:
        logger.info(f"Now using {latest_llm_api_key_struct.llm_type} instead of {llm_api_key_struct.llm_type} for harvester ")

    schema_explorer.explore_and_summarize_tables_parallel()
    #logger.info("Checking Cached Annoy Index")
  #  logger.info(f"Checking for new semantic models... (once per {refresh_seconds} seconds)")
  #  schema_explorer.explore_semantic_models()
    #embeddings_handler.make_and_save_index(bigquery_connector.metadata_table_name)
  #  sys.stdout.write(f'Pausing for {int(refresh_seconds)} seconds before next check.')
    sys.stdout.flush()

    wake_up = False
    first_pass = True
    # skip the sleeping on bots inactive if we are running locally or on Sqlite metadata
    i = 0
    while not wake_up:
        time.sleep(refresh_seconds)
        if harvester_db_connector.source_name == "SQLite":
            wake_up = True
        if os.getenv("SPCS_MODE", "FALSE").upper() == "FALSE":
            wake_up = True

            cursor = harvester_db_connector.client.cursor()
            check_bot_active = f"DESCRIBE TABLE {harvester_db_connector.schema}.BOTS_ACTIVE"
            cursor.execute(check_bot_active)
            result = cursor.fetchone()

            bot_active_time_dt = datetime.strptime(result[0], '%Y-%m-%d %H:%M:%S %Z')
            current_time = datetime.now()
            time_difference = current_time - bot_active_time_dt
            i = i + 1
            if i >= 30:
                logger.info(f"BOTS ACTIVE TIME: {result[0]} | CURRENT TIME: {current_time} | TIME DIFFERENCE: {time_difference}")
            if i > 30:
                i = 0

            if time_difference < timedelta(minutes=5):
                wake_up = True
            else:
                if first_pass:
                    logger.info("Waiting for bots to be active as running in non-local (Snowflake SCPS) mode. Set SPCS_MODE=FALSE to prevent this sleeping.")
                    first_pass = False

       #     logger.info("Bot is active")