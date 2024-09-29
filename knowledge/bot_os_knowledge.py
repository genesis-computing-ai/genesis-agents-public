import os
import json
import sys
import time

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from connectors.bigquery_connector import BigQueryConnector
from connectors.snowflake_connector import SnowflakeConnector
from connectors.sqlite_connector import SqliteConnector
from knowledge.knowledge_server import KnowledgeServer
from core.bot_os_llm import LLMKeyHandler
import logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.WARN, format='%(asctime)s - %(levelname)s - %(message)s')

genesis_source = os.getenv('GENESIS_SOURCE', default="Snowflake")

#print("waiting 60 seconds for other services to start first...", flush=True)
#time.sleep(1)

### LLM KEY STUFF
print('Starting Knowledge Server... ')
logger.info('Starting Knowledge Server... ')

logger.info('Starting DB connection...')
if genesis_source == 'BigQuery':
    credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS',default=".secrets/gcp.json")
    with open(credentials_path) as f:
        connection_info = json.load(f)    
    knowledge_db_connector = BigQueryConnector(connection_info,'BigQuery') # Initialize BigQuery client
elif genesis_source ==  'Sqlite':
    knowledge_db_connector = SqliteConnector(connection_name='Sqlite')
elif genesis_source == 'Snowflake':   
    knowledge_db_connector = SnowflakeConnector(connection_name='Snowflake')
else:
    raise ValueError('Invalid Source')


logger.info('Getting LLM API Key...')
def get_llm_api_key(db_adapter=None):
    from datetime import datetime, timedelta
    from core.bot_os_llm import LLMKeyHandler 
    logger.info('Getting LLM API Key...')
    api_key_from_env = False
    llm_type = os.getenv("BOT_OS_DEFAULT_LLM_ENGINE", "openai")
    llm_key_struct = None

    i = 0
    c = 0

    while llm_key_struct == None:

        refresh_seconds = 180
        wake_up = True
        while not wake_up:

            try:
                cursor = db_adapter.client.cursor()
                check_bot_active = f"DESCRIBE TABLE {db_adapter.schema}.BOTS_ACTIVE"
                cursor.execute(check_bot_active)
                result = cursor.fetchone()

                bot_active_time_dt = datetime.strptime(result[0], '%Y-%m-%d %H:%M:%S %Z')
                current_time = datetime.now()
                time_difference = current_time - bot_active_time_dt

                print(f"BOTS ACTIVE TIME: {result[0]} | CURRENT TIME: {current_time} | TIME DIFFERENCE: {time_difference} | knowledge server", flush=True)

                if time_difference < timedelta(minutes=5):
                    wake_up = True
                else:
                    time.sleep(refresh_seconds)
            except:
                print('Waiting for BOTS_ACTIVE table to be created...')
                time.sleep(refresh_seconds)

        i = i + 1
        if i > 100:
            c += 1
            print(f'Waiting on LLM key... (cycle {c})')
            i = 0 
        # llm_type = None
        llm_key_handler = LLMKeyHandler(db_adapter)
        logger.info('Getting LLM API Key...')

        api_key_from_env, llm_key_struct = llm_key_handler.get_llm_key_from_db(i=i)

        if llm_key_struct.llm_key is None and llm_key_struct.llm_key != 'cortex_no_key_needed':
        #   print('No LLM Key Available in ENV var or Snowflake database, sleeping 20 seconds before retry.', flush=True)
            time.sleep(180)
        else:
            print(f"Using {llm_type} for Knowledge Server")
    
    return llm_key_struct


llm_key_struct = get_llm_api_key(knowledge_db_connector)

### END LLM KEY STUFF
logger.info('Out of LLM section .. calling ensure_table_exists -- ')

# Initialize the BigQueryConnector with your connection info
knowledge_db_connector.ensure_table_exists()

print("    ┌───────┐     ")
print("   ╔═════════╗    ")
print("  ║  ◉   ◉    ║   ")
print("  ║    ───    ║  ")
print("  ╚═══════════╝ ")
print("     ╱     ╲     ")
print("    ╱│  ◯  │╲    ")
print("   ╱ │_____│ ╲   ")
print("      │   │      ")
print("      │   │      ")
print("     ╱     ╲     ")
print("    ╱       ╲    ")
print("   ╱         ╲   ")
print("  G E N E S I S ")
print("    B o t O S")
print(" ---- KNOWLEDGE SERVER ----")


if __name__ == "__main__":    
    knowledge = KnowledgeServer(knowledge_db_connector, llm_key_struct.llm_type, maxsize=20)
    knowledge.start_threads()