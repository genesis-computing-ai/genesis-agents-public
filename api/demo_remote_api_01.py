# Import the GenesisAPI class
from api.genesis_api import GenesisAPI
from api.snowflake_remote_server import GenesisSnowflakeServer

client = GenesisAPI(server_type=GenesisSnowflakeServer, scope="GENESIS_BOTS_ALPHA") 
bots = client.get_all_bots()
print(bots)

request = client.add_message("Janice", "hello. answer in spanish")
response = client.get_response("Janice", request["request_id"], timeout_seconds=10)
print(response)
request = client.add_message("Janice", "what is the capital of spain?", thread_id=request["thread_id"])
response = client.get_response("Janice", request["request_id"], timeout_seconds=10)
print(response)

projects = client.get_all_projects()
print(projects)

client.shutdown()