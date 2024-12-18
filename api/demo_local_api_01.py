import time
from api.genesis_api import GenesisAPI
from api.genesis_base import GenesisToolDefinition
from api.snowflake_local_server import GenesisLocalServer

client = GenesisAPI(server_type=GenesisLocalServer, scope="GENESIS_TEST", sub_scope="GENESIS_INTERNAL", 
                    bot_list=["Janice"]) # ["marty-l6kx7d"]
bots = client.get_all_bots()
print(bots)

# request = client.add_message("Janice", "hello") # "Janice"
# response = None
# response = client.get_response("Janice", request["request_id"])

result = client.run_tool("Janice", "_run_query", #run_query_tool['name'], 
                         {"query": "select CURRENT_DATE()", "max_rows": 10})
print(result)

tools = client.get_all_user_defined_tools()
print(tools)

# client.register_tool(GenesisToolDefinition(TOOL_NAME="example_tool", TOOL_DESCRIPTION="returns the square of the input", 
#                                            PARAMETERS={"input": {"type": "int", "description": "the input to square"}}), 
#                      python_code="def example_tool(input: int) -> int: return input * input", return_type="INT")

# result = client.run_tool("Janice", "example_tool", {"input": 5})
# print(result)

client.shutdown()