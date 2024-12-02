import time
from api.genesis_api import GenesisAPI

client = GenesisAPI("local-snowflake", scope="GENESIS_TEST", sub_scope="GENESIS_INTERNAL", 
                    bot_list=["Janice"]) # ["marty-l6kx7d"]
bots = client.get_all_bots()
print(bots)

request = client.add_message(bots[0], "hello") # "Janice"
time.sleep(1)
response = None
while response is None:
    response = client.get_response(bots[0], request["request_id"])
    time.sleep(1)
print(response)

tools = client.get_all_tools(bots[0])
print(tools)
run_query_tool = client.get_tool(bots[0], "_run_query")
print(run_query_tool)

result = client.run_tool(bots[0], run_query_tool['name'], 
                         {"query": "select CURRENT_DATE()", "max_rows": 10})
print(result)

client.shutdown()