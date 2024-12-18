from api.genesis_api import GenesisAPI
from api.snowflake_local_server import GenesisLocalServer
import os

#requires these environment variables:
    #"PYTHONPATH": "${workspaceFolder}",
    #"OPENAI_API_KEY": "...",
    #"GENESIS_INTERNAL_DB_SCHEMA": "IGNORED.genesis",
    #"SQLITE_OVERRIDE": "True",
    #"BOT_OS_DEFAULT_LLM_ENGINE": "openai"

bot_id = "Janice"
client = GenesisAPI(server_type=GenesisLocalServer, 
                    scope = "IGNORED",
                    sub_scope = "genesis",
                    bot_list=[bot_id])
bots = client.get_all_bots()
print(bots)

request = client.add_message(bot_id, "hello")
response = client.get_response(bot_id, request["request_id"])
print(response)

client.shutdown()