import time
from api.genesis_api import GenesisAPI

client = GenesisAPI("remote-snowflake", scope="GENESIS_BOTS_ALPHA") 
bots = client.get_all_bots()
print(bots)

request = client.add_message(bots[0], "hello")
response = None
while response is None:
    response = client.get_response(bots[0], request["request_id"])
    time.sleep(1)
print(response)

projects = client.get_all_projects()
print(projects)

client.shutdown()