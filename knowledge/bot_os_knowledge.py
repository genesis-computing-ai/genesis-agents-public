import os
import json
import sys
import time

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from connectors.bigquery_connector import BigQueryConnector
from connectors.snowflake_connector import SnowflakeConnector
from knowledge.knowledge_server import KnowledgeServer
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
api_key_from_env = False
default_llm_engine = os.getenv("BOT_OS_DEFAULT_LLM_ENGINE", "openai")
llm_api_key = None

i = 0
c = 0

while llm_api_key == None:

    i = i + 1
    if i > 100:
        c += 1
        print(f'Waiting on LLM key... (cycle {c})')
        i = 0 

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
    
    logger.info('Checking database for LLM Key...', flush=True)
    if llm_api_key is None and genesis_source == 'Snowflake':
        llm_keys_and_types = knowledge_db_connector.db_get_llm_key(project_id=None, dataset_name=None)
        if llm_keys_and_types:
            for llm_key, llm_type in llm_keys_and_types:
                if llm_key and llm_type:
                    if llm_type.lower() == "openai":
                        os.environ["OPENAI_API_KEY"] = llm_key
                    elif llm_type.lower() == "reka":
                        os.environ["REKA_API_KEY"] = llm_key
                    elif llm_type.lower() == "gemini":
                        os.environ["GEMINI_API_KEY"] = llm_key
                    api_key_from_env = False
                    llm_api_key = llm_key
                    default_llm_engine = llm_type
                    break        

    if llm_api_key is None:
     #   print('No LLM Key Available in ENV var or Snowflake database, sleeping 20 seconds before retry.', flush=True)
        time.sleep(20)


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
    knowledge = KnowledgeServer(knowledge_db_connector, maxsize=10)
    knowledge.start_threads()