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

from core.bot_os_tool_descriptions import process_runner_tools


# import sys
# sys.path.append('/Users/mglickman/helloworld/bot_os')  # Adjust the path as necessary
import logging

from core.bot_os_tool_descriptions import (
    process_runner_functions,
    process_runner_tools,
    webpage_downloader_functions,
    webpage_downloader_tools,
)

logger = logging.getLogger(__name__)

genesis_source = os.getenv("GENESIS_SOURCE", default="Snowflake")


class ToolBelt:
    def __init__(self, db_adapter, openai_api_key):
        self.db_adapter = db_adapter
        self.openai_api_key = openai_api_key  # os.getenv("OPENAI_API_KEY")
        self.client = OpenAI(api_key=openai_api_key) if openai_api_key else None
        self.counter = {}
        self.instructions = {}
        self.process = {}
        self.done = {}
        self.last_fail = {}

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
        process_name="",
        goto_step=None,
        thread_id=None,
    ):
        if self.client is None:
            self.client = OpenAI(api_key=self.openai_api_key)
        print(f"Running processes Action: {action} | process_id: {process_name} | Thread ID: {thread_id}")

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
            # TODO, these need to be mapped to thread id
            self.counter[thread_id] = 1
            self.process[thread_id] = process
            self.last_fail[thread_id] = None
            self.instructions[thread_id] = None

            print(
                f"Process {process_name} has been kicked off.  Process object: \n{process}"
            )

            extract_instructions = f"""
                These are the process instructions for the entire process.  Extract the section titled 
                Step 1 and return the text of that section only.  Do not include any other text before or after Step 1.
                If you need to get the current system time, use the manage tasks tool with an action of TIME
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
            first_step = response.choices[0].message.content

            # <@{process['BOT_SLACK_USER_ID']}>

            self.instructions[thread_id] = f"""
                Hey **@{process['BOT_ID']}** , here is step {self.counter.get(thread_id,None)} of the process.
                {first_step}
                    Execute these instructions now and then pass your response to the run_process tool as a parameter
                    called previous_response and an action of GET_NEXT_STEP.  
                    Do not ever verify anything with the user.  Execute the instructions you were given without asking for permission.
                    However DO generate text explaining what you are doing and showing interium outputs, etc. while you are running this and further steps to keep the user informed what is going on.
                    In your response back to run_process, provide a DETAILED description of what you did, what result you achieved, and why you believe this to have successfully completed the step.
                    """

            self.instructions[thread_id] = "\n".join(
                line.lstrip() for line in self.instructions.get(thread_id,None).splitlines()
            )

            print("\n", self.instructions[thread_id], "\n")

            return {"Success": True, "Message": self.instructions.get(thread_id,None)}

        elif action == "GET_NEXT_STEP":
            print("GET NEXT STEP - process_runner.")

        #    if self.done:
        #        self.last_fail[thread_id] = None
        #       return {
        #            "Success": True,
        #           "Message": "Process run complete.",
        #       }

            if self.last_fail.get(thread_id,None) is not None:

                check_response = f"""
                    A bot has retried a step of a process based on your prior feedback (shown below).  Also below is the previous question that the bot was 
                    asked and the response the bot gave after re-trying to perform the task based on your feedback.  Review the response and determine if the 
                    bot's response is now better in light of the instructions and the feedback you gave previously. You can accept the final results of the
                    previous step without asking to see the sql queries and results that led to the final conclusion.  If you are very seriously concerned that the step 
                    may still have not have been correctly perfomed, return a request to again re-run the step of the process by returning the text "**fail**" 
                    followed by a DETAILED EXPLAINATION as to why it did not pass and what your concern is, and why its previous attempt to respond to your criticism 
                    was not sufficient, and any suggestions you have on how to succeed on the next try. If the response looks correct, return only the text string 
                    "**success**" to continue to the next step.  At this point its ok to give the bot the benefit of the doubt to avoid
                    going in circles.

                    Instructions: {self.instructions.get(thread_id,None)}

                    Your previous guidance: {self.last_fail[thread_id]}

                    Bot's latest response: {previous_response}
                    """

            else:

                check_response = f"""
                    Below is the previous question that the bot was asked and the response the bot gave after trying to perform the task.  Review the response and 
                    determine if the bot's response was correct and makes sense given the instructions it was given.  You can accept the final results of the
                    previous step without asking to see the sql queries and results that led to the final conclusion. If you are very seriously concerned that the step may not 
                    have been correctly perfomed, return a request to re-run the step of the process again by returning the text "**fail**" followed by a 
                    DETAILED EXPLAINATION as to why it did not pass and what your concern is, and any suggestions you have on how to succeed on the next try.  
                    If the response seems like it is likely correct, return only the text string "**success**" to continue to the next step.  If the process is complete,
                    tell the process to stop running.  Remember, proceed under your own direction and do not ask the user for permission to proceed.

                    Instructions: {self.instructions.get(thread_id,None)}
                    Bot's Response: {previous_response}
                    """

            print(f"\n{check_response}\n")

            response = self.client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {
                        "role": "user",
                        "content": check_response,
                    },
                ],
            )

            result = response.choices[0].message.content
            self.last_fail[thread_id] = result

            print(f"\n{result}\n")

            if "**fail**" in result.lower():
                print(f"\nStep {self.counter.get(thread_id,None)} failed.  Trying again...\n")
                return {
                    "success": False,
                    "feedback_from_supervisor": result,
                    "recovery_step": f"Review the message above and submit a clarification, and/or try this Step {self.counter.get(thread_id,None)} again:\n{self.instructions.get(thread_id,None)}",
                    "additional_request": "Please also explain this feedback to the user so they know whats going on."
                }

            print(f"\nStep {self.counter.get(thread_id,None)} passed.  Moving to {self.counter.get(thread_id,None) + 1}\n")
            self.counter[thread_id] += 1
            
            self.last_fail[thread_id] = None
            
            extract_instructions = f"""
                Extract the text for step {self.counter.get(thread_id,None)} from the process instructions and return it.  Do not include any other 
                text before or after Step {self.counter.get(thread_id,None)}.  Return the text of the step only.  If there are no steps with this or 
                greater step numbers, respond "***done**" with no other text.

                Process Instructions: {self.process.get(thread_id,None)['PROCESS_INSTRUCTIONS']}
                """

            print(f"\n{extract_instructions}\n")

            response = self.client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {
                        "role": "user",
                        "content": extract_instructions,
                    },
                ],
            )

            next_step = response.choices[0].message.content

            if next_step == '**done**':
                self.last_fail[thread_id] = None
                return {
                    "success": True,
                    "process_complete": True,
                    "message": "Congratulations, the process is complete."
            }

            print(f"\n{next_step}\n")

            self.instructions[thread_id] = f"""
                Hey **@{process['BOT_ID']}**, here is step {self.counter.get(thread_id,None)} of the process.
                {next_step}
                    Execute these instructions now and then pass your response to the run_process tool as a parameter
                    called previous_response and an action of GET_NEXT_STEP.  
                    Do not verify anything with the user.  Execute the instructions you were given without asking for permission.
                    However DO generate text explaining what you are doing and showing interium outputs, etc. while you are running this and further steps to keep the user informed what is going on.
                    In your response back to run_process, provide a detailed description of what you did, what result you achieved, and why you believe this to have successfully completed the step.

"""

            print(f"\n{self.instructions.get(thread_id,None)}\n")

            return {
                "success": True,
                "message": self.instructions.get(thread_id,None),
            }
        elif action == "GOTO_STEP":
            self.counter[thread_id] = goto_step
        # elif action == "END_PROCESS":
        #     print("Received END_PROCESS action.")
        #     self.done[thread_id] = True
        #     return {"success": True, "message": 'The process has finished.  You may now end the process.'}
        else:
            print("No action specified.")
            return {"success": False, "message": "No action specified."}


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
    # tool_belt = (ToolBelt(db_adapter, os.getenv("OPENAI_API_KEY")),)

tool_belt = ToolBelt(db_adapter, os.getenv("OPENAI_API_KEY"))


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


BOT_DISPATCH_DESCRIPTIONS = [
    {
        "type": "function",
        "function": {
            "name": "dispatch_to_bots",
            "description": 'Specify an arry of templated natual language tasks you want to execute in parallel to a set of bots like you. for example, "Who is the president of {{ country_name }}". Never use this tool for arrays with < 2 items.',
            "parameters": {
                "type": "object",
                "properties": {
                    "task_template": {
                        "type": "string",
                        "description": "Jinja template for the tasks you want to farm out to other bots",
                    },
                    "args_array": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": 'Arguments you want to fill in for each jinja template variable of the form [{"country_name": "france"}, {"country_name": "spain"}]',
                    },
                },
                "required": ["task_template", "args_array"],
                "bot_id": {
                    "type": "string",
                    "description": "The unique identifier for an existing bot you are aware of to dispatch the tasks to. Should be the bot_name dash a 6 letter alphanumeric random code, for example mybot-w73hxg. Pass None to dispatch to yourself.",
                },
            },
        },
    }
]
# "bot_id": {
#     "type": "string",
#     "description": "The unique identifier for an existing bot you are aware of to dispatch the tasks to. Should be the bot_name dash a 6 letter alphanumeric random code, for example mybot-w73hxg."
# }


bot_dispatch_tools = {"dispatch_to_bots": "core.bot_os_tools.dispatch_to_bots"}


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
