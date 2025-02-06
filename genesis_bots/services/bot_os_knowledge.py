import os
import json
import sys
import time

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

#from genesis_bots.connectors.bigquery_connector import BigQueryConnector
from genesis_bots.connectors.snowflake_connector.snowflake_connector import SnowflakeConnector
from genesis_bots.connectors.sqlite_connector import SqliteConnector
from genesis_bots.knowledge.knowledge_server import KnowledgeServer
from genesis_bots.core.bot_os_llm import LLMKeyHandler

from genesis_bots.core.logging_config import logger

genesis_source = os.getenv('GENESIS_SOURCE', default="Snowflake")


### LLM KEY STUFF
logger.info('Starting Knowledge Server... ')
logger.info('Starting Knowledge Server... ')

logger.info('Starting DB connection...')
if genesis_source == 'BigQuery':
    raise NotImplementedError("BigQueryConnector is not implemented")
elif genesis_source ==  'Sqlite':
    knowledge_db_connector = SqliteConnector(connection_name='Sqlite')
elif genesis_source == 'Snowflake':
    knowledge_db_connector = SnowflakeConnector(connection_name='Snowflake')
else:
    raise ValueError('Invalid Source')


logger.info('Getting LLM API Key...')
def get_llm_api_key(db_adapter=None):
    from datetime import datetime, timedelta
    from genesis_bots.core.bot_os_llm import LLMKeyHandler
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

                logger.info(f"BOTS ACTIVE TIME: {result[0]} | CURRENT TIME: {current_time} | TIME DIFFERENCE: {time_difference} | knowledge server")

                if time_difference < timedelta(minutes=5):
                    wake_up = True
                else:
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

        api_key_from_env, llm_key_struct = llm_key_handler.get_llm_key_from_db(i=i)

        if llm_key_struct.llm_key is None and llm_key_struct.llm_key != 'cortex_no_key_needed':
            time.sleep(180)
        else:
            logger.info(f"Using {llm_type} for Knowledge Server")

    return llm_key_struct


llm_key_struct = get_llm_api_key(knowledge_db_connector)

### END LLM KEY STUFF
logger.info('Out of LLM section .. calling ensure_table_exists -- ')

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