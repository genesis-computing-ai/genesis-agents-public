import json
import os
import time
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from openai import OpenAI

from jinja2 import Template
from bot_genesis.make_baby_bot import MAKE_BABY_BOT_DESCRIPTIONS, make_baby_bot_tools
from connectors import database_tools
from connectors.bigquery_connector import BigQueryConnector
from connectors.snowflake_connector import SnowflakeConnector
from slack.slack_tools import slack_tools, slack_tools_descriptions
from connectors.database_tools import (
    image_functions,
    image_tools,
    bind_run_query,
    bind_search_metadata,
    bind_semantic_copilot,
    autonomous_functions,
    autonomous_tools,
    process_manager_tools,
    process_manager_functions,
    database_tool_functions,
    database_tools,
    snowflake_stage_functions,
    snowflake_stage_tools,
    snowflake_semantic_functions,
    snowflake_semantic_tools,
)
from schema_explorer.harvester_tools import (
    harvester_tools_list,
    harvester_tools_functions,
)
from development.integration_tools import (
    integration_tool_descriptions,
    integration_tools,
)
from bot_genesis.make_baby_bot import get_bot_details
from core.bot_os import BotOsSession
from core.bot_os_corpus import URLListFileCorpus
from core.bot_os_defaults import (
    BASE_BOT_INSTRUCTIONS_ADDENDUM,
    BASE_BOT_PRE_VALIDATION_INSTRUCTIONS,
    BASE_BOT_PROACTIVE_INSTRUCTIONS,
    BASE_BOT_VALIDATION_INSTRUCTIONS,
)
from core.bot_os_input import BotOsInputAdapter, BotOsInputMessage, BotOsOutputMessage
from core.bot_os_memory import BotOsKnowledgeAnnoy_Metadata

from core.bot_os_tool_descriptions import (
    BOT_DISPATCH_DESCRIPTIONS,
    bot_dispatch_tools,
    process_runner_functions,
    process_runner_tools,
    webpage_downloader_functions,
    webpage_downloader_tools,
    webpage_downloader_action_function_mapping,
)


# import sys
# sys.path.append('/Users/mglickman/helloworld/bot_os')  # Adjust the path as necessary
import logging


logger = logging.getLogger(__name__)

genesis_source = os.getenv("GENESIS_SOURCE", default="Snowflake")


class ToolBelt:
    def __init__(self, db_adapter):
        self.db_adapter = db_adapter
        self.openai_api_key = os.getenv("OPENAI_API_KEY")
        self.client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        self.counter = 0

    # Function to make HTTP request and get the entire content
    def get_webpage_content(self, url):
        response = requests.get(url)
        response.raise_for_status()
        return response.content  # Return the entire content

    # Function for parsing HTML content, extracting links, and then chunking the beautified content
    def parse_and_chunk_content(self, content, base_url, chunk_size=256 * 1024):
        soup = BeautifulSoup(content, "html.parser")
        links = [urljoin(base_url, a["href"]) for a in soup.find_all("a", href=True)]
        pretty_content = soup.prettify()
        encoded_content = pretty_content.encode("utf-8")
        encoded_links = json.dumps(links).encode("utf-8")

        # Combine the content and links
        combined_content = encoded_content + encoded_links

        # Chunk the combined content
        chunks = []
        for i in range(0, len(combined_content), chunk_size):
            chunks.append({"content": combined_content[i : i + chunk_size]})

        if not chunks:
            raise ValueError("No content available within the size limit.")

        return chunks, len(chunks)  # Return chunks and total number of chunks

    # Main function to download webpage, extract links, and ensure each part is within the size limit
    def download_webpage(self, url, chunk_index=0):
        try:
            content = self.get_webpage_content(url)
            chunks, total_chunks = self.parse_and_chunk_content(content, url)
            if chunk_index >= total_chunks:
                return {"error": "Requested chunk index exceeds available chunks."}

            response = {
                "chunk": chunks[chunk_index],
                "next_chunk_index": (
                    chunk_index + 1 if chunk_index + 1 < total_chunks else None
                ),
                "total_chunks": total_chunks,
            }
            return response
        except Exception as e:
            return {"error": str(e)}

    def run_process(
        self,
        action,
        previous_response="",
        thread_id=None,
        # session=None,
        process_name="",
    ):
        print(f"Running processes Action: {action} | process_id: {process_name}")
        # Try to get process info from PROCESSES table
        process = self.db_adapter.get_process_info(process_name)
        if len(process) == 0:
            return {
                "Success": False,
                "Message": f"Process {process_name} not found in the PROCESSES table.",
            }

        if action == "GET_ANSWER":
            print("The meaning of life has been discovered - 42!")
            return {
                "Success": True,
                "Message": "The meaning of life has been discovered - 42!",
            }
        elif action == "KICKOFF_PROCESS":
            print("Kickoff process.")
            self.counter = 1

            print(
                f"Process {process_name} has been kicked off.  Process object: \n{process}"
            )

            extract_instructions = f"""
            These are the process instructions for the entire process.  Extract the section titled 'Objective' and 
            step {self.counter} and return the text.
            {process['PROCESS_INSTRUCTIONS']}
            """

            response = self.client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {
                        "role": "user",
                        "content": extract_instructions,
                    },
                ],
            )
            llm_return = response.choices[0].message.content
            try:
                instructions = llm_return.split("**Instruction:**")[1]
            except:
                instructions = llm_return

            # print("\n", instructions, "\n")

            instructions = f"""
               Hey @{process['BOT_ID']} <@{process['BOT_SLACK_USER_ID']}>, here is step {self.counter} of the process.
               {instructions}
                Execute these instructions now and then call the run process tool with action GET_NEXT_STEP to get the next step.
                Do not verify anything witht the user.  Just execute the instructions.
                """

            print("\n", instructions, "\n")
            # channel_id = "C076L59T21H"  # sessions.input_adapters[0].

            # session.input_adapters[0].send_slack_channel_message(
            #     channel_id,  # Needs to be channel id
            #     instructions,
            #     thread_id=thread_id,
            #     wait=True,
            # )

            return {
                "Success": True,
                "Message": f"<@{process['BOT_SLACK_USER_ID']}> {instructions}",
            }

        elif action == "GET_NEXT_STEP":
            print("GET NEXT STEP - process_runner.")

            previous_response += f"""
            These are the process instructions for the entire process.  
            {process['PROCESS_INSTRUCTIONS']}

            **Results from Step {self.counter}**:
            If the step and the results from the step do not look satisfactory, please return a request to
            run the process again with the action GET_NEXT_STEP and success = false.  If they are satisfactory, please return a request to
            run step {self.counter + 1} with the text of step {self.counter + 1} from process instructions and success = true.  
            Return the information as a JSON object with two fields: 'Success' and 'Message'.
            Do not return anything else in the message beside the JSON object.

            ** Results from previous step {self.counter}**
            {previous_response}
            """

            # conversation_history = self.slack_app.client.conversations_history(
            #     channel=channel, limit=2
            # ).data

            response = self.client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {
                        "role": "user",
                        "content": previous_response,
                    },
                ],
            )

            llm_return = (
                response.choices[0]
                .message.content.replace("json", "")
                .replace("true", "True")
                .replace("false", "False")
                .replace("```", "")
                .replace("\\", "")
                .replace("\n", "")
            )
            result = json.loads(llm_return)

            if result["Success"]:
                self.counter += 1

            print(result["Message"])

            # response = client.chat_postMessage(
            #     channel="general",
            #     text="This is a reply",
            #     thread_ts="1561764011.015500"
            # )
            # assert response["ok"]
            # print(response)

            return {
                "Success": True,
                "Message": f'@{process["BOT_SLACK_USER_ID"]} {result["Message"]}',
            }
        else:
            print("No action specified.")
            return {"Success": False, "Message": "No action specified."}


if genesis_source == "BigQuery":
    credentials_path = os.getenv(
        "GOOGLE_APPLICATION_CREDENTIALS", default=".secrets/gcp.json"
    )
    with open(credentials_path) as f:
        connection_info = json.load(f)
    # Initialize BigQuery client
    db_adapter = BigQueryConnector(connection_info, "BigQuery")
else:  # Initialize Snowflake client
    db_adapter = SnowflakeConnector(connection_name="Snowflake")
    connection_info = {"Connection_Type": "Snowflake"}
    tool_belt = ToolBelt(db_adapter)


def get_tools(which_tools, db_adapter, slack_adapter_local=None, include_slack=True):
    tools = []
    available_functions_load = {}
    function_to_tool_map = {}
    if "autonomous_functions" in which_tools and "autonomous_tools" not in which_tools:
        which_tools = [
            tool if tool != "autonomous_functions" else "autonomous_tools"
            for tool in which_tools
        ]
    which_tools = [tool for tool in which_tools if tool != "autonomous_functions"]
    for tool in which_tools:
        try:
            tool_name = tool.get("tool_name")
        except:
            tool_name = tool

        if False:  # tool_name == 'integration_tools':
            tools.extend(integration_tool_descriptions)
            available_functions_load.update(integration_tools)
            function_to_tool_map[tool_name] = integration_tool_descriptions
        elif include_slack and tool_name == "slack_tools":
            tools.extend(slack_tools_descriptions)
            available_functions_load.update(slack_tools)
            function_to_tool_map[tool_name] = slack_tools_descriptions
        elif tool_name == "harvester_tools":
            tools.extend(harvester_tools_functions)
            available_functions_load.update(harvester_tools_list)
            function_to_tool_map[tool_name] = harvester_tools_functions
        elif tool_name == "make_baby_bot":
            tools.extend(MAKE_BABY_BOT_DESCRIPTIONS)
            available_functions_load.update(make_baby_bot_tools)
            function_to_tool_map[tool_name] = MAKE_BABY_BOT_DESCRIPTIONS
        elif tool_name == "bot_dispatch":
            tools.extend(BOT_DISPATCH_DESCRIPTIONS)
            available_functions_load.update(bot_dispatch_tools)
            function_to_tool_map[tool_name] = BOT_DISPATCH_DESCRIPTIONS
        elif tool_name == "database_tools":
            tools.extend(database_tool_functions)
            available_functions_load.update(database_tools)
            run_query_f = bind_run_query([connection_info])
            search_metadata_f = bind_search_metadata("./kb_vector")
            semantic_copilot_f = bind_semantic_copilot([connection_info])
            function_to_tool_map[tool_name] = database_tool_functions
        elif tool_name == "image_tools":
            tools.extend(image_functions)
            available_functions_load.update(image_tools)
            function_to_tool_map[tool_name] = image_functions
        elif tool_name == "snowflake_semantic_tools":
            tools.extend(snowflake_semantic_functions)
            available_functions_load.update(snowflake_semantic_tools)
            function_to_tool_map[tool_name] = snowflake_semantic_functions
        elif tool_name == "snowflake_stage_tools":
            tools.extend(snowflake_stage_functions)
            available_functions_load.update(snowflake_stage_tools)
            function_to_tool_map[tool_name] = snowflake_stage_functions
        elif tool_name == "autonomous_tools" or tool_name == "autonomous_functions":
            tools.extend(autonomous_functions)
            available_functions_load.update(autonomous_tools)
            function_to_tool_map[tool_name] = autonomous_functions
        elif tool_name == "process_runner_tools":
            tools.extend(process_runner_functions)
            available_functions_load.update(process_runner_tools)
            function_to_tool_map[tool_name] = process_runner_functions
        elif tool_name == "process_manager_tools":
            tools.extend(process_manager_functions)
            available_functions_load.update(process_manager_tools)
            function_to_tool_map[tool_name] = process_manager_functions
        elif tool_name == "webpage_downloader":
            tools.extend(webpage_downloader_functions)
            available_functions_load.update(webpage_downloader_tools)
            function_to_tool_map[tool_name] = webpage_downloader_functions
        else:
            try:
                module_path = "generated_modules." + tool_name
                desc_func = "TOOL_FUNCTION_DESCRIPTION_" + tool_name.upper()
                functs_func = tool_name.lower() + "_action_function_mapping"
                module = __import__(module_path, fromlist=[desc_func, functs_func])
                # here's how to get the function for generated things even new ones...
                func = [getattr(module, desc_func)]
                tools.extend(func)
                function_to_tool_map[tool_name] = func
                func_af = getattr(module, functs_func)
                available_functions_load.update(func_af)
            except:
                logger.warn(f"Functions for tool '{tool_name}' could not be found.")

    available_functions = {}
    for name, full_func_name in available_functions_load.items():
        if callable(full_func_name):
            available_functions[name] = full_func_name
        else:
            module_path, func_name = full_func_name.rsplit(".", 1)
            if module_path in locals():
                module = locals()[module_path]
                try:
                    func = getattr(module, func_name)
                except:
                    func = module
                # print("existing local: ",func)
            elif module_path in globals():
                module = globals()[module_path]
                try:
                    func = getattr(module, func_name)
                except:
                    func = module
                # print("existing global: ",func)
            else:
                module = __import__(module_path, fromlist=[func_name])
                func = getattr(module, func_name)
                # print("imported: ",func)
            available_functions[name] = func
    # Insert additional code here if needed

    return tools, available_functions, function_to_tool_map
    # print("imported: ",func)


class BotOsDispatchInputAdapter(BotOsInputAdapter):
    def __init__(self, bot_id) -> None:
        bot_config = get_bot_details(bot_id=bot_id)
        self.session = make_session_for_dispatch(bot_config)
        self.tasks = {}

    # allows for polling from source
    def add_event(self, event):
        pass

    # allows for polling from source
    def get_input(self, thread_map=None, active=None, processing=None, done_map=None):
        pass

    # allows response to be sent back with optional reply
    def handle_response(
        self,
        session_id: str,
        message: BotOsOutputMessage,
        in_thread=None,
        in_uuid=None,
        task_meta=None,
    ):
        if message.status == "completed":
            self.tasks[message.thread_id]["result"] = message.output

    def dispatch_task(self, task):
        # thread_id = self.session.add_task(task, self)
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


def make_session_for_dispatch(bot_config):
    input_adapters = []
    bot_tools = json.loads(bot_config["available_tools"])

    genesis_source = os.getenv("GENESIS_SOURCE", default="BigQuery")

    if genesis_source == "BigQuery":
        credentials_path = os.getenv(
            "GOOGLE_APPLICATION_CREDENTIALS", default=".secrets/gcp.json"
        )
        with open(credentials_path) as f:
            connection_info = json.load(f)
        # Initialize BigQuery client
        db_adapter = BigQueryConnector(connection_info, "BigQuery")
    else:  # Initialize BigQuery client
        db_adapter = SnowflakeConnector(connection_name="Snowflake")
        connection_info = {"Connection_Type": "Snowflake"}

    print("---> CONNECTED TO DATABASE: ", genesis_source)
    tools, available_functions, function_to_tool_map = get_tools(
        bot_tools, db_adapter, include_slack=False
    )  # FixMe remove slack adapter if

    instructions = (
        bot_config["bot_instructions"] + "\n" + BASE_BOT_INSTRUCTIONS_ADDENDUM
    )
    print(instructions, f'{bot_config["bot_name"]}, id: {bot_config["bot_id"]}')

    # TESTING UDF ADAPTER W/EVE and ELSA
    # add a map here to track botid to adapter mapping

    bot_id = bot_config["bot_id"]
    if os.getenv("BOT_DO_PLANNING_REFLECTION"):
        pre_validation = BASE_BOT_PRE_VALIDATION_INSTRUCTIONS
        post_validation = BASE_BOT_VALIDATION_INSTRUCTIONS
    else:
        pre_validation = ""
        post_validation = None
    if os.getenv("BOT_BE_PROACTIVE", "False").lower() == "true":
        proactive_instructions = BASE_BOT_PROACTIVE_INSTRUCTIONS
    else:
        proactive_instructions = ""
    session = BotOsSession(
        bot_config["bot_id"],
        instructions=instructions + proactive_instructions + pre_validation,
        validation_instructions=post_validation,
        input_adapters=input_adapters,
        knowledgebase_implementation=BotOsKnowledgeAnnoy_Metadata(
            f"./kb_{bot_config['bot_id']}"
        ),
        file_corpus=(
            URLListFileCorpus(json.loads(bot_config["files"]))
            if bot_config["files"]
            else None
        ),
        log_db_connector=db_adapter,  # Ensure connection_info is defined or fetched appropriately
        tools=tools,
        available_functions=available_functions,
        all_tools=tools,
        all_functions=available_functions,
        all_function_to_tool_map=function_to_tool_map,
        bot_id=bot_config["bot_id"],
    )
    # if os.getenv("BOT_BE_PROACTIVE").lower() == "true" and slack_adapter_local:
    #      session.add_task("Check in with Michael Gold to see if he has any tasks for you to work on.",
    ##                       thread_id=session.create_thread(slack_adapter_local))
    #                       input_adapter=slack_adapter_local))

    return session  # , api_app_id, udf_adapter_local, slack_adapter_local
