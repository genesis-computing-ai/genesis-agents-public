import json
import os
import time
from connectors.snowflake_connector import SnowflakeConnector
from core.bot_os_tools import get_tools
from slack.slack_bot_os_adapter import SlackBotAdapter
from bot_genesis.make_baby_bot import make_baby_bot, get_available_tools, get_bot_details, update_bot_details, list_all_bots, get_all_bots_full_details, add_new_tools_to_bot, remove_tools_from_bot
from core.system_variables import SystemVariables
import core.global_flags as global_flags
import threading
import logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.WARN, format='%(asctime)s - %(levelname)s - %(message)s')

print("****** GENBOT VERSION 0.140 *******")

runner_id = os.getenv('RUNNER_ID','jl-local-runner')
print("Runner ID: ", runner_id )

genbot_internal_project_and_schema = os.getenv('GENESIS_INTERNAL_DB_SCHEMA','None')
if genbot_internal_project_and_schema == 'None':
    print("ENV Variable GENESIS_INTERNAL_DB_SCHEMA is not set.")
if genbot_internal_project_and_schema is not None:
    genbot_internal_project_and_schema = genbot_internal_project_and_schema.upper()
db_schema = genbot_internal_project_and_schema.split('.')
project_id = db_schema[0]
dataset_name = db_schema[1]

genesis_source = os.getenv('GENESIS_SOURCE',default="Snowflake")

if genesis_source == 'BigQuery':
    credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS',default=".secrets/gcp.json")
    with open(credentials_path) as f:
        connection_info = json.load(f)
    # Initialize BigQuery client
    #db_adapter = BigQueryConnector(connection_info,'BigQuery')
else:    # Initialize BigQuery client
    print('Starting Snowflake connector...')
    db_adapter = SnowflakeConnector(connection_name='Snowflake')
    connection_info = { "Connection_Type": "Snowflake" }
db_adapter.ensure_table_exists()#
print("---> CONNECTED TO DATABASE:: ",genesis_source)

Bots = list_all_bots(runner_id=runner_id, slack_details=False)
#print(Bots) 

# tools
available_tools = get_available_tools()
#print(f'available_tools: {available_tools}')
#first Bot Eve
print(f'Bot id: Eve-iFIeYj')
current_bot = get_bot_details('Eve-iFIeYj') 

print(f'Available tools')
print(current_bot['available_tools'])  # current tools in the table

# Add new tool
#new_Eve_tools=add_new_tools_to_bot(bot_id='Eve-iFIeYj',new_tools=['autonomous_functions'])
#print(new_Eve_tools)

#remove newly added tool from db, check the database
remove_Eve_tools=remove_tools_from_bot(bot_id='Eve-iFIeYj',remove_tools=['database_tools'])
print(remove_Eve_tools)

# 2nd Bot Eliza
print(f'Bot id: Eliza-Ixop6H')
current_bot = get_bot_details('Eliza-Ixop6H')
print(f'Available tools')
print(current_bot['available_tools'])
# Add new tool
#new_Eliza_tools=add_new_tools_to_bot(bot_id='Eliza-Ixop6H',new_tools=['autonomous_functions'])
#print(new_Eliza_tools)

#remove 2 tools. One does not exist. Check the return results and toll list in db
remove_Eliza_tools=remove_tools_from_bot(bot_id='Eliza-Ixop6H',remove_tools=['database_tools'])

print(remove_Eliza_tools)
 



