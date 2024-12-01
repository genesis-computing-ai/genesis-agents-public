from api.genesis_api import GenesisAPI

client = GenesisAPI("local-snowflake", scope="GENESIS_TEST", sub_scope="GENESIS_INTERNAL") 
bots = client.get_all_bots()
print(bots)

tools = client.get_all_tools(bots[0])
print(tools)
run_query_tool = client.get_tool(bots[0], "_run_query")
print(run_query_tool)

result = client.run_tool(bots[0], run_query_tool['name'], 
                         {"query": "select CURRENT_DATE()", "max_rows": 10})
print(result)

client.shutdown()