import json
import os
import threading
import time
import uuid
from jinja2 import Template
from bot_genesis.make_baby_bot import MAKE_BABY_BOT_DESCRIPTIONS, make_baby_bot_tools
from connectors import database_tools
from connectors.bigquery_connector import BigQueryConnector
from connectors.snowflake_connector import SnowflakeConnector
from slack.slack_tools import slack_tools, slack_tools_descriptions
from connectors.database_tools import image_functions, image_tools, bind_run_query, bind_search_metadata, bind_semantic_copilot, autonomous_functions, autonomous_tools, database_tool_functions, database_tools, snowflake_stage_functions, snowflake_stage_tools, snowflake_semantic_functions, snowflake_semantic_tools
from schema_explorer.harvester_tools import harvester_tools_list, harvester_tools_functions
from development.integration_tools import integration_tool_descriptions, integration_tools
from bot_genesis.make_baby_bot import get_bot_details
from core.bot_os import BotOsSession
from core.bot_os_corpus import URLListFileCorpus
from core.bot_os_defaults import BASE_BOT_INSTRUCTIONS_ADDENDUM, BASE_BOT_PRE_VALIDATION_INSTRUCTIONS, BASE_BOT_PROACTIVE_INSTRUCTIONS, BASE_BOT_VALIDATION_INSTRUCTIONS
from core.bot_os_input import BotOsInputAdapter, BotOsInputMessage, BotOsOutputMessage
from core.bot_os_memory import BotOsKnowledgeAnnoy_Metadata
#import sys
#sys.path.append('/Users/mglickman/helloworld/bot_os')  # Adjust the path as necessary
import logging
logger = logging.getLogger(__name__)

genesis_source = os.getenv('GENESIS_SOURCE',default="Snowflake")

if genesis_source == 'BigQuery':
    credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS',default=".secrets/gcp.json")
    with open(credentials_path) as f:
        connection_info = json.load(f)
    # Initialize BigQuery client
    db_adapter = BigQueryConnector(connection_info,'BigQuery')
else:    # Initialize BigQuery client
    db_adapter = SnowflakeConnector(connection_name='Snowflake')
    connection_info = { "Connection_Type": "Snowflake" }

def get_tools(which_tools, db_adapter, slack_adapter_local=None, include_slack=True):
    tools = []
    available_functions_load = {}
    function_to_tool_map = {}
    for tool in which_tools:
        try: 
            tool_name = tool.get("tool_name")
        except:
            tool_name = tool    

        if False: #tool_name == 'integration_tools':
            tools.extend(integration_tool_descriptions)
            available_functions_load.update(integration_tools)
            function_to_tool_map[tool_name]=integration_tool_descriptions
        elif include_slack and tool_name == 'slack_tools':
            tools.extend(slack_tools_descriptions)
            available_functions_load.update(slack_tools)
            function_to_tool_map[tool_name]=slack_tools_descriptions
        elif tool_name == 'harvester_tools':
            tools.extend(harvester_tools_functions)
            available_functions_load.update(harvester_tools_list)
            function_to_tool_map[tool_name]=harvester_tools_functions
        elif tool_name == 'make_baby_bot':
            tools.extend(MAKE_BABY_BOT_DESCRIPTIONS)
            available_functions_load.update(make_baby_bot_tools)
            function_to_tool_map[tool_name]=MAKE_BABY_BOT_DESCRIPTIONS
        elif tool_name == 'bot_dispatch':
            tools.extend(BOT_DISPATCH_DESCRIPTIONS)
            available_functions_load.update(bot_dispatch_tools)
            function_to_tool_map[tool_name]=BOT_DISPATCH_DESCRIPTIONS
        elif tool_name == 'database_tools':
            tools.extend(database_tool_functions)
            available_functions_load.update(database_tools)
            run_query_f = bind_run_query([connection_info])
            search_metadata_f = bind_search_metadata("./kb_vector")
            semantic_copilot_f = bind_semantic_copilot([connection_info])
            function_to_tool_map[tool_name]=database_tool_functions
        elif tool_name == 'image_tools':
            tools.extend(image_functions)
            available_functions_load.update(image_tools)
            function_to_tool_map[tool_name]=image_functions
        elif tool_name == 'snowflake_semantic_tools':
            tools.extend(snowflake_semantic_functions)
            available_functions_load.update(snowflake_semantic_tools)
            function_to_tool_map[tool_name]=snowflake_semantic_functions
        elif tool_name == 'snowflake_stage_tools':
            tools.extend(snowflake_stage_functions)
            available_functions_load.update(snowflake_stage_tools)
            function_to_tool_map[tool_name]=snowflake_stage_functions
        elif tool_name == 'autonomous_tools' or tool_name == 'autonomous_functions':
            tools.extend(autonomous_functions)
            available_functions_load.update(autonomous_tools)
            function_to_tool_map[tool_name]=autonomous_functions
        else:
            try:
                module_path = "generated_modules."+tool_name
                desc_func = "TOOL_FUNCTION_DESCRIPTION_"+tool_name.upper()
                functs_func = tool_name.lower()+'_action_function_mapping'
                module = __import__(module_path, fromlist=[desc_func, functs_func])
                # here's how to get the function for generated things even new ones... 
                func = [getattr(module, desc_func)]
                tools.extend(func)
                function_to_tool_map[tool_name]=func
                func_af = getattr(module, functs_func)
                available_functions_load.update(func_af)
            except:
                logger.warn(f"Functions for tool '{tool_name}' could not be found.")

    available_functions = {}
    for name, full_func_name in available_functions_load.items():
        if callable(full_func_name):
            available_functions[name]=full_func_name
        else:
            module_path, func_name = full_func_name.rsplit('.', 1)
            if module_path in locals():
                module = locals()[module_path]
                try:
                    func = getattr(module, func_name)
                except:
                    func = module
                #print("existing local: ",func)        
            elif module_path in globals():
                module = globals()[module_path]
                try:
                    func = getattr(module, func_name)
                except:
                    func = module
                #print("existing global: ",func)
            else:
                module = __import__(module_path, fromlist=[func_name])
                func = getattr(module, func_name)
                #print("imported: ",func)
            available_functions[name] = func
      # Insert additional code here if needed

    return tools, available_functions, function_to_tool_map
        #print("imported: ",func)

class BotOsDispatchInputAdapter(BotOsInputAdapter):
    def __init__(self, bot_id) -> None:
        bot_config = get_bot_details(bot_id=bot_id)
        self.session = make_session_for_dispatch(bot_config)
        self.tasks = {}

    # allows for polling from source
    def add_event(self, event):
        pass

    # allows for polling from source
    def get_input(self, thread_map=None,  active=None, processing=None, done_map=None):
        pass

    # allows response to be sent back with optional reply
    def handle_response(self, session_id:str, message:BotOsOutputMessage, in_thread=None, in_uuid=None, task_meta=None): 
        if message.status == "completed":
            self.tasks[message.thread_id]["result"] = message.output

    def dispatch_task(self, task):
        #thread_id = self.session.add_task(task, self)
        thread_id = self.session.create_thread(self)
        self.tasks[thread_id] = {"task": task, "result": None}
        self.session.add_message(BotOsInputMessage(thread_id=thread_id, msg=task))
        
    def check_tasks(self):
        self.session.execute()
        if all(task["result"] is not None for task in self.tasks.values()):
            return [task["result"] for task in self.tasks.values()]
        else:
            return False
        
def dispatch_to_bots(task_template, args_array, dispatch_bot_id=None):
    """
    Dispatches a task to multiple bots, each instantiated by creating a new thread with a specific task.
    The task is created by filling in the task template with arguments from the args_array using Jinja templating.

    Args:
        task_template (str): A natural language task template using Jinja templating.
        args_array (list of dict): An array of dictionaries to plug into the task template for each bot.

    Returns:
        list: An array of responses.
    """

    if len(args_array) < 2:
        return "Error: args_array size must be at least 2."
    
    template = Template(task_template)
    adapter = BotOsDispatchInputAdapter(bot_id=dispatch_bot_id)

    for s_args in args_array:
        # Fill in the task template with the current arguments
        args = json.loads(s_args)
        task = template.render(**args)
        adapter.dispatch_task(task)

    while True:
        responses = adapter.check_tasks()
        if responses:
            print(f"dispatch_to_bots - {responses}")
            return responses
        time.sleep(1)
            
BOT_DISPATCH_DESCRIPTIONS = [{
    "type": "function",
    "function": {
        "name": "dispatch_to_bots",
        "description": 'Specify an arry of templated natual language tasks you want to execute in parallel to a set of bots like you. for example, "Who is the president of {{ country_name }}". Never use this tool for arrays with < 2 items.',
        "parameters": {
            "type": "object",
            "properties": {
                "task_template": {
                    "type": "string",
                    "description": "Jinja template for the tasks you want to farm out to other bots"
                },
                "args_array": {
                    "type": "array",
                    "items": {
                        "type": "string"
                    },
                    "description": 'Arguments you want to fill in for each jinja template variable of the form [{"country_name": "france"}, {"country_name": "spain"}]'
                },
            },
            "required": ["task_template", "args_array"]
        }
    }
}]
                # "bot_id": {
                #     "type": "string",
                #     "description": "The unique identifier for an existing bot you are aware of to dispatch the tasks to. Should be the bot_name dash a 6 letter alphanumeric random code, for example mybot-w73hxg."
                # }


bot_dispatch_tools = {"dispatch_to_bots": "core.bot_os_tools.dispatch_to_bots"}

def make_session_for_dispatch(bot_config):
    input_adapters = []
    bot_tools = json.loads(bot_config["available_tools"])

    genesis_source = os.getenv('GENESIS_SOURCE',default="BigQuery")

    if genesis_source == 'BigQuery':
        credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS',default=".secrets/gcp.json")
        with open(credentials_path) as f:
            connection_info = json.load(f)
        # Initialize BigQuery client
        db_adapter = BigQueryConnector(connection_info,'BigQuery')
    else:    # Initialize BigQuery client
        db_adapter = SnowflakeConnector(connection_name='Snowflake')
        connection_info = { "Connection_Type": "Snowflake" }

    print("---> CONNECTED TO DATABASE: ",genesis_source)
    tools, available_functions, function_to_tool_map = get_tools(bot_tools, db_adapter, include_slack=False) #FixMe remove slack adapter if 
    
    instructions = bot_config["bot_instructions"] + "\n" + BASE_BOT_INSTRUCTIONS_ADDENDUM
    print(instructions, f'{bot_config["bot_name"]}, id: {bot_config["bot_id"]}' )
    
    # TESTING UDF ADAPTER W/EVE and ELSA
    # add a map here to track botid to adapter mapping

    bot_id = bot_config["bot_id"]
    if os.getenv("BOT_DO_PLANNING_REFLECTION"):
        pre_validation = BASE_BOT_PRE_VALIDATION_INSTRUCTIONS
        post_validation= BASE_BOT_VALIDATION_INSTRUCTIONS
    else:
        pre_validation = ""
        post_validation= None
    if os.getenv("BOT_BE_PROACTIVE", "False").lower() == "true":
        proactive_instructions = BASE_BOT_PROACTIVE_INSTRUCTIONS
    else:
        proactive_instructions = ""
    session = BotOsSession(bot_config["bot_id"], 
                           instructions=instructions + proactive_instructions + pre_validation,
                           validation_instructions=post_validation,
                           input_adapters=input_adapters,
                           knowledgebase_implementation=BotOsKnowledgeAnnoy_Metadata(f"./kb_{bot_config['bot_id']}"),
                           file_corpus=URLListFileCorpus(json.loads(bot_config["files"])) if bot_config["files"] else None,
                           log_db_connector=db_adapter, # Ensure connection_info is defined or fetched appropriately
                           tools = tools,
                           available_functions=available_functions,
                           all_tools = tools,
                           all_functions = available_functions,
                           all_function_to_tool_map=function_to_tool_map,
                           bot_id=bot_config["bot_id"],
                          )
    # if os.getenv("BOT_BE_PROACTIVE").lower() == "true" and slack_adapter_local:
    #      session.add_task("Check in with Michael Gold to see if he has any tasks for you to work on.",
    ##                       thread_id=session.create_thread(slack_adapter_local))
    #                       input_adapter=slack_adapter_local))
        
    return session#, api_app_id, udf_adapter_local, slack_adapter_local
