import time
from api.genesis_api import GenesisAPI
from api.snowflake_local_server import GenesisLocalSnowflakeServer

client = GenesisAPI(server_type=GenesisLocalSnowflakeServer, scope="GENESIS_TEST", sub_scope="GENESIS_INTERNAL", 
                    bot_list=["Janice"]) # ["marty-l6kx7d"]
bots = client.get_all_bots()
print(bots)

request = client.add_message("Janice", "hello") # "Janice"
time.sleep(1)
response = None
while response is None:
    response = client.get_response("Janice", request["request_id"])
    time.sleep(1)
print(response)

tools = client.get_all_tools("Janice")
print(tools)
run_query_tool = client.get_tool("Janice", "_run_query")
print(run_query_tool)

result = client.run_tool("Janice", run_query_tool['name'], 
                         {"query": "select CURRENT_DATE()", "max_rows": 10})
print(result)

client.shutdown()