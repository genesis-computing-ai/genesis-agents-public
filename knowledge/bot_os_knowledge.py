import os
import json
import sys
import time

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from connectors.bigquery_connector import BigQueryConnector
from connectors.snowflake_connector import SnowflakeConnector
from knowledge.knowledge_server import KnowledgeServer
from core.bot_os_llm import LLMKeyHandler
import logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.WARN, format='%(asctime)s - %(levelname)s - %(message)s')

genesis_source = os.getenv('GENESIS_SOURCE',default="BigQuery")

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
    # Initialize BigQuery client
    knowledge_db_connector = BigQueryConnector(connection_info,'BigQuery')
else:    # Initialize BigQuery client
    knowledge_db_connector = SnowflakeConnector(connection_name='Snowflake')


logger.info('Getting LLM API Key...')
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

        api_key_from_env, llm_api_key = llm_key_handler.get_llm_key_from_db()

        if llm_api_key is None and llm_api_key != 'cortex_no_key_needed':
        #   print('No LLM Key Available in ENV var or Snowflake database, sleeping 20 seconds before retry.', flush=True)
            time.sleep(20)
        else:
            logger.info(f"Using {llm_type} for harvester ")

llm_api_key = get_llm_api_key()


### END LLM KEY STUFF
logger.info('Out of LLM section .. calling ensure_table_exists -- ')

# Initialize the BigQueryConnector with your connection info
knowledge_db_connector.ensure_table_exists()

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
print(" ---- KNOWLEDGE SERVER ----")


if __name__ == "__main__":
    if os.getenv("OPENAI_API_KEY",'') == '':
        print("Cannot start knowledge service - no openai key found")
    else:
        knowledge = KnowledgeServer(knowledge_db_connector, maxsize=10)
        knowledge.start_threads()