from genesis_bots.api.genesis_api import GenesisAPI
from genesis_bots.api.snowflake_local_server import EmbeddedGenesisServerProxy
import os

#requires these environment variables:
    #"PYTHONPATH": "${workspaceFolder}",
    #"OPENAI_API_KEY": "...",
    #"GENESIS_INTERNAL_DB_SCHEMA": "IGNORED.genesis",
    #"BOT_OS_DEFAULT_LLM_ENGINE": "openai"

bot_id = "Janice"
client = GenesisAPI(server_proxy=EmbeddedGenesisServerProxy,
                    scope = "IGNORED",
                    sub_scope = "genesis",
                    bot_list=[bot_id])
bots = client.get_all_bots()
notes = client.get_all_notes()
print(notes)
if len(notes) > 0:
    first_note = notes[0]
    note = client.get_note(first_note)
    print("First note:", first_note)

print(bots)

request = client.submit_message(bot_id, "hello")
response = client.get_response(bot_id, request["request_id"])
print(response)

client.shutdown()