import time
import os
from api.genesis_api import GenesisAPI
from api.genesis_base import GenesisToolDefinition
from api.snowflake_local_server import GenesisLocalServer

scope, sub_scope = os.getenv("GENESIS_INTERNAL_DB_SCHEMA").split(".")
with GenesisAPI(server_type=GenesisLocalServer, scope=scope, sub_scope=sub_scope, 
                    bot_list=["Janice"]) as client:
    bots = client.get_all_bots()
    print("Existing bots:", bots)
    print("-----------------------")
    print("\n>>>> Sending 'hello' to Janice")
    request = client.add_message("Janice", "hello") # "Janice"
    print("\n>>>> Getting response from Janice")
    response = client.get_response("Janice", request["request_id"])
    print("\n>>>>", response)

    result = client.run_tool("Janice", "_run_query", #run_query_tool['name'], 
                            {"query": "select CURRENT_DATE()", "max_rows": 10})
    print("\n>>>> run_tool result:", result)

    request = client.add_message("Janice", "Run a query to get the current date") # "Janice"
    response = client.get_response("Janice", request["request_id"])
    print("\n>>>>", response)

    tools = client.get_all_user_defined_tools()
    print("\n>>>> user defined tools:",  tools)

    # client.register_tool(GenesisToolDefinition(TOOL_NAME="example_tool", TOOL_DESCRIPTION="returns the square of the input", 
    #                                            PARAMETERS={"input": {"type": "int", "description": "the input to square"}}), 
    #                      python_code="def example_tool(input: int) -> int: return input * input", return_type="INT")

    # result = client.run_tool("Janice", "example_tool", {"input": 5})
    # print(result)
