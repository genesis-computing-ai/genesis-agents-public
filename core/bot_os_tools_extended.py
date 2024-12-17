from   core.logging_config      import logger

def create_stored_proc_lambda(tool_name, db_adapter, project_id):
    return lambda *args, **kwargs: db_adapter.run_query(f"CALL {project_id}.EXTENDED_TOOLS.{tool_name}({', '.join(['%s'] * len(args))})", args)

def load_user_extended_tools(db_adapter, project_id, dataset_name) -> tuple[list, dict]:
    # Dynamically define tool definitions based on entries in USER_EXTENDED_TOOLS database table
    user_extended_tools_definitions = []
    user_extended_tools = {}
    try:
        # Assuming db_adapter is defined and has a method to fetch data from the USER_EXTENDED_TOOLS table
        user_extended_tools_data = db_adapter.db_get_user_extended_tools(project_id, dataset_name)
        for tool in user_extended_tools_data:
            tool_definition = {
                "type": "function",
                "function": {
                    "name": tool["tool_name"],
                    "description": tool["tool_description"],
                    "parameters": {
                        "type": "object",
                        "properties": {}
                    }
                }
            }
            for parameter in tool.get("parameters", []):
                tool_definition["function"]["parameters"]["properties"][parameter["name"]] = {
                    "type": parameter["type"],
                    "description": parameter["description"]
                }
                if "required" in parameter and parameter["required"]:
                    tool_definition["function"]["parameters"]["required"] = []
                tool_definition["function"]["parameters"]["required"].append(parameter["name"])
            user_extended_tools_definitions.append(tool_definition)

        user_extended_tools = {tool["tool_name"]: create_stored_proc_lambda(tool["tool_name"], db_adapter, project_id=project_id) for tool in user_extended_tools_data}
    except Exception as e:
        logger.error(f"Failed to fetch user extended tools definitions: {e}")
        user_extended_tools_definitions = []
        user_extended_tools = {}

    return user_extended_tools_definitions, user_extended_tools