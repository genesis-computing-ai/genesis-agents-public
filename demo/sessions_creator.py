import json
import os
import sys

from llm_gemini.bot_os_gemini import BotOsAssistantGemini

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from core.bot_os import BotOsSession
from core.bot_os_corpus import URLListFileCorpus
from core.bot_os_defaults import (
    BASE_BOT_INSTRUCTIONS_ADDENDUM,
    BASE_BOT_PRE_VALIDATION_INSTRUCTIONS,
    BASE_BOT_PROACTIVE_INSTRUCTIONS,
    BASE_BOT_VALIDATION_INSTRUCTIONS,
)

from core.bot_os_memory import BotOsKnowledgeAnnoy_Metadata

from core.bot_os_tools import get_tools, ToolBelt
from llm_cortex.bot_os_cortex import BotOsAssistantSnowflakeCortex
from llm_openai.bot_os_openai import BotOsAssistantOpenAI
from slack.slack_bot_os_adapter import SlackBotAdapter
from teams.teams_bot_os_adapter import TeamsBotOsInputAdapter
from bot_genesis.make_baby_bot import (
    get_available_tools,
    get_all_bots_full_details,
)

from streamlit_gui.udf_proxy_bot_os_adapter import UDFBotOsInputAdapter
from core.bot_os_task_input_adapter import TaskBotOsInputAdapter

import logging

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.WARN, format="%(asctime)s - %(levelname)s - %(message)s"
)

import core.global_flags as global_flags

genbot_internal_project_and_schema = os.getenv("GENESIS_INTERNAL_DB_SCHEMA", "None")
if genbot_internal_project_and_schema == "None":
    print("ENV Variable GENESIS_INTERNAL_DB_SCHEMA is not set.")
if genbot_internal_project_and_schema is not None:
    genbot_internal_project_and_schema = genbot_internal_project_and_schema.upper()
db_schema = genbot_internal_project_and_schema.split(".")
project_id = db_schema[0]
dataset_name = db_schema[1]

genesis_source = os.getenv("GENESIS_SOURCE", default="Snowflake")


def make_session(
    bot_config,
    db_adapter,
    bot_id_to_udf_adapter_map={},
    stream_mode=False,
    skip_vectors=False,
    data_cubes_ingress_url=None,
    existing_slack=None,
    existing_udf=None
):

    # streamlit and slack launch todos:
    # add a flag for udf_enabled and slack_enabled to database
    # launch them accordingly
    # add a tool to deploy and un-deploy an existing to slack but keep it in the DB
    # add multi-bot display to streamlit (tabs)
    # add launch to slack button to streamlit
    # add setup harvester button to streamlit

    udf_enabled = bot_config.get("udf_active", "Y") == "Y"
    slack_enabled = bot_config.get("slack_active", "Y") == "Y"
    runner_id = os.getenv("RUNNER_ID", "jl-local-runner")

#    if global_flags.slack_active is False and slack_enabled:
#        global_flags.slack_active = False

    input_adapters = []

    if os.getenv("TEAMS_BOT") and bot_config["bot_name"] == os.getenv("TEAMS_BOT"):
        from teams.teams_bot_os_adapter import TeamsBotOsInputAdapter
        teams_adapter_local = TeamsBotOsInputAdapter(
            bot_name=bot_config["bot_name"],
            app_id=bot_config.get("teams_app_id", None),
            app_password=bot_config.get("teams_app_password", None),
            app_type=bot_config.get("teams_app_type", None),
            app_tenantid=bot_config.get("teams_tenant_id", None),
            bot_id=bot_config["bot_id"]
        )
        input_adapters.append(teams_adapter_local)


    slack_adapter_local = None
    if existing_slack:
        slack_adapter_local = existing_slack
        input_adapters.append(slack_adapter_local)
    if slack_enabled and existing_slack is None:
        try:
            app_level_token = bot_config.get("slack_app_level_key", None)

            # Stream mode is for interactive bot serving, False means task server
            if stream_mode:
                print(f"Making Slack adapter for bot_id: {bot_config['bot_id']} named {bot_config['bot_name']} with bot_user_id: {bot_config['bot_slack_user_id']}")
                slack_adapter_local = SlackBotAdapter(
                    token=bot_config[
                        "slack_app_token"
                    ],  # This should be the Slack App Token, adjust field name accordingly
                    signing_secret=bot_config[
                        "slack_signing_secret"
                    ],  # Assuming the signing secret is the same for all bots, adjust if needed
                    channel_id=bot_config[
                        "slack_channel_id"
                    ],  # Assuming the channel is the same for all bots, adjust if needed
                    bot_user_id=bot_config["bot_slack_user_id"],
                    bot_name=bot_config["bot_name"],
                    slack_app_level_token=app_level_token,
                )  # Adjust field name if necessary
            else:
                slack_adapter_local = SlackBotAdapter(
                    token=bot_config[
                        "slack_app_token"
                    ],  # This should be the Slack App Token, adjust field name accordingly
                    signing_secret=bot_config[
                        "slack_signing_secret"
                    ],  # Assuming the signing secret is the same for all bots, adjust if needed
                    channel_id=bot_config[
                        "slack_channel_id"
                    ],  # Assuming the channel is the same for all bots, adjust if needed
                    bot_user_id=bot_config["bot_slack_user_id"],
                    bot_name=bot_config["bot_name"],
                    slack_app_level_token=app_level_token,
                    bolt_app_active=False,  # This line added for task_server i.e. not stream mode!
                )  # Adjust field name if necessary
            input_adapters.append(slack_adapter_local)
        except:
            print(
                f'Failed to create Slack adapter with the provided configuration for bot {bot_config["bot_name"]} '
            )
            logger.error(
                f'Failed to create Slack adapter with the provided configuration for bot {bot_config["bot_name"]} '
            )
            return None, None, None, None

    # tools
    available_tools = get_available_tools()
    print(f"Number of available tools: {len(available_tools)}")
    # available_tools.append({'tool_name': "integration_tools", 'tool_description': 'integration tools'})
    # available_tools.append({'tool_name': "activate_marketing_campaign", 'tool_description': 'activate_marketing_campaign'})
    # available_tools.append({'tool_name': "send_email_via_webhook", 'tool_description': 'send_email_via_webhook'})

    if bot_config.get("available_tools", None) is not None:
        bot_tools = json.loads(bot_config["available_tools"])
        print(f"Number of bot-specific tools: {len(bot_tools)}")
        # bot_tools.append({'tool_name': "integration_tools", 'tool_description': 'integration tools'})
        # bot_tools.append({'tool_name': "activate_marketing_campaign", 'tool_description': 'activate_marketing_campaign'})
        # bot_tools.append({'tool_name': "send_email_via_webhook", 'tool_description': 'send_email_via_webhook'})
    else:
        bot_tools = []

    # Check if SIMPLE_MODE environment variable is set to 'true'
    bot_id = bot_config["bot_id"]
    print(f"setting local bot id = {bot_id}")

    # remove slack tools if Slack is not enabled for this bot
    if not slack_enabled:
        bot_tools = [tool for tool in bot_tools if tool != "slack_tools"]

    openai_key = os.getenv('OPENAI_API_KEY', None)
    if openai_key is not None:
        print (f"Instantiating ToolBelt with db_adapter and openai_api_key: len: {len(openai_key)}")
    else:
        print (f"Instantiating ToolBelt with db_adapter, no OPENAI_KEY available")
    tool_belt = ToolBelt(db_adapter, openai_key)

    tools, available_functions, function_to_tool_map = get_tools(
        bot_tools, slack_adapter_local=slack_adapter_local, db_adapter=db_adapter, tool_belt=tool_belt
    )
    print(f"Number of available functions for bot {bot_id}: {len(available_functions)}")
    all_tools, all_functions, all_function_to_tool_map = get_tools(
        available_tools, slack_adapter_local=slack_adapter_local, db_adapter=db_adapter, tool_belt=tool_belt
    )
    print(f"Number of all functions for bot {bot_id}: {len(all_functions)}")

    simple_mode = os.getenv("SIMPLE_MODE", "false").lower() == "true"

    instructions = bot_config["bot_instructions"] + "\n"

    cursor = db_adapter.client.cursor()
    query = f"SELECT process_name FROM {db_adapter.schema}.PROCESSES where bot_id = %s"
    cursor.execute(query, (bot_id,))
    result = cursor.fetchall()

    if result:
        processes_found = ', '.join([row[0] for row in result])
        instructions += f"\n\nFYI, you have the following processes available: {processes_found}. They can be run with _run_process function if useful to your work. This list may not be up to date, you can use _manage_process for an up to date LIST.\n\n"
        print('appended process list to prompt, len=', len(processes_found))
    instructions += BASE_BOT_INSTRUCTIONS_ADDENDUM

    instructions += f'\nYour default database connecton is called "{genesis_source}".\n'

    instructions += f'\nNote current settings:\nYour bot_id: {bot_config["bot_id"]}.\nRunner_id: {runner_id}'
    if bot_config["slack_active"] == "Y":
        instructions += "\nYour slack user_id: " + bot_config["bot_slack_user_id"]

    if "snowflake_stage_tools" in bot_tools and "make_baby_bot" in bot_tools:
        instructions += f"\nYour Internal Files Stage for bots is at snowflake stage: {genbot_internal_project_and_schema}.BOT_FILES_STAGE"
        if not stream_mode:
            instructions += ". This BOT_FILES_STAGE stage is ONLY in this particular database & schema."

    llm_type = None

    # Check if the environment variable exists and has data
    if "BOT_LLMS" in os.environ and os.environ["BOT_LLMS"]:
        # Convert the JSON string back to a dictionary
        bot_llms = json.loads(os.environ["BOT_LLMS"])
    else:
        # Initialize as an empty dictionary
        bot_llms = {}

    # check if database_tools are in bot_tools
    if "database_tools" in bot_tools:
        try:
            # if so, create workspace schema

            workspace_schema_name = f"{global_flags.project_id}.{bot_id.replace(r'[^a-zA-Z0-9]', '_').replace('-', '_').replace('.', '_')}_WORKSPACE".upper()
            db_adapter.create_bot_workspace(workspace_schema_name)
            db_adapter.grant_all_bot_workspace(workspace_schema_name)
            instructions += f"\nYou have a workspace schema created specifically for you named {workspace_schema_name} that the user can also access. You may use this schema for creating tables, views, and stages that are required when generating answers to data analysis questions. Only use this schema if asked to create an object. Always return the full location of the object.\nYour default stage is {workspace_schema_name}.MY_STAGE. "
            if data_cubes_ingress_url:
                print(
                    f"Setting data_cubes_ingress_url for {bot_id}: {data_cubes_ingress_url}"
                )
       #         instructions += f"\nWhenever you show the results from run_query that may have more than 10 rows, and if you are not in the middle of running a process, also provide a link to a datacube visualization to help them understand the data you used in the form: http://{data_cubes_ingress_url}%ssql_query=select%20*%20from%20spider_data.baseball.all_star -- replace the value of the sql_query query parameter with the query you used."
        except Exception as e:
            print(f"Error creating bot workspace for bot_id {bot_id} {e} ")

    # print(instructions, f'{bot_config["bot_name"]}, id: {bot_config["bot_id"]}' )

    # TESTING UDF ADAPTER W/EVE and ELSA
    # add a map here to track botid to adapter mapping

    if existing_udf:
        udf_adapter_local = existing_udf
        input_adapters.append(udf_adapter_local)
    else:
        udf_adapter_local = None
    if udf_enabled and not existing_udf:
        if bot_id in bot_id_to_udf_adapter_map:
            udf_adapter_local = bot_id_to_udf_adapter_map[bot_id]
        else:
            udf_adapter_local = (
                UDFBotOsInputAdapter() if stream_mode else TaskBotOsInputAdapter()
            )
            bot_id_to_udf_adapter_map[bot_id] = udf_adapter_local
        udf_adapter_local = (
            UDFBotOsInputAdapter() if stream_mode else TaskBotOsInputAdapter()
        )
        input_adapters.append(udf_adapter_local)

    if not simple_mode and stream_mode or os.getenv("BOT_DO_PLANNING_REFLECTION"):
        pre_validation = BASE_BOT_PRE_VALIDATION_INSTRUCTIONS
        post_validation = BASE_BOT_VALIDATION_INSTRUCTIONS
    else:
        pre_validation = ""
        post_validation = None
    if os.getenv("SIMPLE_MODE", "false").lower() == "false" and os.getenv("BOT_BE_PROACTIVE", "False").lower() == "true":
        proactive_instructions = BASE_BOT_PROACTIVE_INSTRUCTIONS
    else:
        proactive_instructions = ""

    # if True:
#    if stream_mode:
    assistant_implementation = None
    actual_llm = None
    print(f"Bot implementation from bot config: {bot_config.get('bot_implementation', 'Not specified')}")
    if "bot_implementation" in bot_config:
        if "CORTEX_OVERRIDE" in os.environ and os.environ["CORTEX_OVERRIDE"].lower() == "true":
            print('&& cortex override for bot ',bot_id,' due to ENV VAR &&')
            bot_config["bot_implementation"] = "cortex"
        llm_type = bot_config["bot_implementation"]
        if (bot_config["bot_implementation"] == "cortex"):
            if os.environ.get("CORTEX_AVAILABLE", 'False') in ['False', '']:
                cortex_available = db_adapter.check_cortex_available()
                if not cortex_available:
                    print('Snowflake Cortex is not available. reverting to openai.')

                    if os.environ["OPENAI_API_KEY"] in (None,""):
                        llm_keys_and_types = db_adapter.db_get_llm_key()
                        for llm_key, llm_type in llm_keys_and_types:
                            if llm_key:
                                if llm_type.lower() == "openai":
                                    os.environ["OPENAI_API_KEY"] = llm_key
                                    assistant_implementation = BotOsAssistantOpenAI
                                    actual_llm = 'openai'
                                    break
                        if os.environ["OPENAI_API_KEY"] in (None,""):
                            print("openai llm key not set. bot session cannot be created.")
                    else:
                        assistant_implementation = BotOsAssistantOpenAI
                        actual_llm = 'openai'
                else:
                    assistant_implementation = BotOsAssistantSnowflakeCortex
                    actual_llm = 'cortex'
            else:
                assistant_implementation = BotOsAssistantSnowflakeCortex
                actual_llm = 'cortex'
        # elif ("bot_implementation" in bot_config and bot_config["bot_implementation"] == "gemini"):
        #     assistant_implementation = BotOsAssistantGemini
        elif (bot_config["bot_implementation"] == "openai"):
            # check if key exists, if not get from database
            if os.getenv("OPENAI_API_KEY") in (None, ""):
                # get key from db
                llm_keys_and_types = db_adapter.db_get_llm_key()
                for llm_key, llm_type in llm_keys_and_types:
                    if llm_key and llm_type:
                        if llm_type.lower() == "openai":
                            os.environ["OPENAI_API_KEY"] = llm_key
                            assistant_implementation = BotOsAssistantOpenAI
                            actual_llm = 'openai'
                            break
                if os.getenv("OPENAI_API_KEY") in (None, ""):
                    print("openai llm key not set. attempting cortex.")
                    if os.environ.get("CORTEX_AVAILABLE", 'False') in ['False', '']:
                        cortex_available = db_adapter.check_cortex_available()
                        if not cortex_available:
                            print('Snowflake Cortex is not available. No openai key set. Bot session cannot be created.')
                        else:
                            assistant_implementation = BotOsAssistantSnowflakeCortex
                            actual_llm = 'cortex'
                    else:
                        assistant_implementation = BotOsAssistantSnowflakeCortex
                        actual_llm = 'cortex'
                else:
                    assistant_implementation = BotOsAssistantOpenAI
                    actual_llm = 'openai'
            else:
                assistant_implementation = BotOsAssistantOpenAI
                actual_llm = 'openai'
        else:
            llm_type = os.getenv("BOT_OS_DEFAULT_LLM_ENGINE","cortex")
            if llm_type.lower() == "cortex":
                if os.environ.get("CORTEX_AVAILABLE", 'False') in ['False', '']:
                    cortex_available = db_adapter.check_cortex_available()
                    if not cortex_available:
                        print('Bot implementation not specified, OpenAI is not available, Snowflake Cortex is not available. Please set LLM key in Streamlit.')
                    else:
                        assistant_implementation = BotOsAssistantSnowflakeCortex
                        actual_llm = 'cortex'
                else:
                    assistant_implementation = BotOsAssistantSnowflakeCortex
                    actual_llm = 'cortex'
                print('Bot implementation not specified, OpenAI not available, so Defaulting bot LLM to Snowflake Cortex')
            elif llm_type.lower() == 'openai':
                print('Bot implementation not specified, OpenAI is available, so defaulting bot LLM to OpenAI')
                if os.getenv("OPENAI_API_KEY") in (None, ""):
                    # get key from db
                    llm_keys_and_types = db_adapter.db_get_llm_key()
                    for llm_key, llm_type in llm_keys_and_types:
                        if llm_key and llm_type:
                            if llm_type.lower() == "openai":
                                os.environ["OPENAI_API_KEY"] = llm_key
                                assistant_implementation = BotOsAssistantOpenAI
                                actual_llm = 'openai'
                                break
                    if os.getenv("OPENAI_API_KEY") in (None, ""):
                        print("openai llm key not set. cortex not available. what llm is being used%s")
                    else:
                        assistant_implementation = BotOsAssistantOpenAI
                        actual_llm = 'openai'
                else:
                    assistant_implementation = BotOsAssistantOpenAI
                    actual_llm = "openai"
            else:
                # could be gemini or something else eventually
                print('Bot implementation not specified, OpenAI is not available, Snowflake Cortex is not available. Please set LLM key in Streamlit.')

        # Updating an existing bot's preferred_llm
        bot_llms[bot_id] = {"current_llm": actual_llm, "preferred_llm": bot_config["bot_implementation"]}

        bot_llms_json = json.dumps(bot_llms)


        # Save the JSON string as an environment variable
        os.environ["BOT_LLMS"] = bot_llms_json
        os.environ["BOT_LLM_"+bot_id] = actual_llm

        #if assistant_implementation == BotOsAssistantSnowflakeCortex and stream_mode:
        if assistant_implementation == BotOsAssistantSnowflakeCortex and True:
            incoming_instructions = instructions
            # Tools: brave_search, wolfram_alpha, code_interpreter

#Environment: ipython
#
#Cutting Knowledge Date: December 2023
#Today Date: 23 Jul 2024


            instructions = """

# Tool Instructions 
""" 
#""" - Always execute python code in messages that you share.
# - When looking for real time information use relevant functions if available else fallback to brave_search
# 
            instructions += """You have access to the following functions, only call them when needed to perform actions or lookup information that you do not already have:

""" + json.dumps(tools) + """


If a you choose to call a function ONLY reply in the following format:
<function={function_name}>{parameters}</function>

where

function_name => the name of the function from the list above 
parameters => a JSON dict with the function argument name as key and function argument value as value.

Here is an example,
<function=example_function_name>{"example_name": "example_value"}</function>

Here is another example, with a parameter value containg properly escaped double quotes:
<function=_run_query>{"query": "select * from \\"DATABASE_NAME\\".\\"SCHEMA_NAME\\".\\"TABLE_NAME\";"}</function>

Reminder:
- Function calls MUST follow the specified format
- Required parameters MUST be specified
- Only call one function at a time
- Put the entire function call reply on one line
- Properly escape any double quotes in your parameter values with a backslash
- Do not add any preable of other text before or directly after the function call
- Always add your sources when using search results to answer the user query
- Don't generate function call syntax (e.g. as an example) unless you want to actually call it immediately 
- But when you do want to call the tools, don't just say you can do it, actually do it when needed
- If you're suggesting a next step to the user other than calling a tool, just suggest it, but don't immediately perform it, wait for them to agree, unless its a tool call

# Persona Instructions
 """+incoming_instructions + """
 
# Important Reminders
If you say you're going to call or use a tool, you MUST actually make the tool call immediately in the format described above.
Only respond with !NO_RESPONSE_REQUIRED if the message is directed to someone else or in chats with multiple people if you have nothing to say.
Always respond to greetings and pleasantries like 'hi' etc.
Call functions using ONLY this exact format: <function=example_function_name>{"example_name": "example_value"}</function>
Don't use this call format unless you actually want to call the tool. Don't generate this as an example of what you could do, only do it when you actually want to call the tool.

 """
        else:
            instructions = instructions + """

# Important Reminders
If you say you're going to call or use a tool, you MUST actually make the tool call immediately.
However, do not provide example function calls to the user, as they WILL be run.
Only respond with !NO_RESPONSE_REQUIRED if the message is directed to someone else or in chats with multiple people if you have nothing to say.
Always respond to greetings and pleasantries like 'hi' etc, unless specifically directed at someone else.

"""
 
   #         with open('./latest_instructions.txt', 'w') as file:
   #             file.write(instructions)
        
    try:
        # logger.warning(f"GenBot {bot_id} instructions:::  {instructions}")
        # print(f'tools: {tools}')
        asst_impl = (
#            assistant_implementation if stream_mode else None
            assistant_implementation 
        )  # test this - may need separate BotOsSession call for stream mode
        print(f"assistant impl : {assistant_implementation}")
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
            update_existing=True,
            assistant_implementation=asst_impl,
            log_db_connector=db_adapter,  # Ensure connection_info is defined or fetched appropriately
            # tools=slack_tools + integration_tool_descriptions + [TOOL_FUNCTION_DESCRIPTION_WEBPAGE_DOWNLOADER],
            tools=tools,
            bot_name=bot_config["bot_name"],
            available_functions=available_functions,
            all_tools=all_tools,
            all_functions=all_functions,
            all_function_to_tool_map=all_function_to_tool_map,
            bot_id=bot_config["bot_id"],
            stream_mode=stream_mode,
            tool_belt=tool_belt, 
            skip_vectors=skip_vectors,
        )
    except Exception as e:
        print("Session creation exception: ", e)
        raise (e)
    if os.getenv("BOT_BE_PROACTIVE", "FALSE").lower() == "true" and slack_adapter_local:
        if not slack_adapter_local.channel_id:
            logger.warn(
                "not adding initial task - slack_adapter_local channel_id is null"
            )
        if not os.getenv("BOT_OS_MANAGER_NAME"):
            logger.warn("not adding initial task - BOT_OS_MANAGER_NAME not set.")
        else:
            session._add_reminder(
                f"Send a daily DM on slack to {os.getenv('BOT_OS_MANAGER_NAME')}, to see if there are any tasks for you to work on. Make some suggestions based on your role, tools and expertise. Respond to this only with !NO_RESPONSE and then mark the task complete.",
                due_date_delta="1 minute",
                is_recurring=True,
                frequency="daily",
                thread_id=session.create_thread(slack_adapter_local),
            )
    api_app_id = bot_config[
        "api_app_id"
    ]  # Adjust based on actual field name in bots_config

    # print('here: session: ',session)
    return session, api_app_id, udf_adapter_local, slack_adapter_local


def create_sessions(
    db_adapter,
    bot_id_to_udf_adapter_map,
    stream_mode=False,
    skip_vectors=False,
    data_cubes_ingress_url=None,
):
    import os
    # Fetch bot configurations for the given runner_id from BigQuery
    runner_id = os.getenv("RUNNER_ID", "jl-local-runner")

    bots_config = get_all_bots_full_details(runner_id=runner_id)

    # janice = 0
    # for bot_config in bots_config:
    #     if bot_config.get("bot_name") == "Janice":
    #         janice += 1

    # if janice == 0:
    #     import os

    #     cursor = db_adapter.connection.cursor()
    #     print("Janice bot not found in bots_config")

    #     # Add Janice 2.0 to bots_config
    #     janice_config = {
    #         "API_APP_ID": "",
    #         "BOT_SLACK_USER_ID": os.getenv("U076DQVN5LY", "None"),
    #         "BOT_ID": "Janice",
    #         "BOT_NAME": "Janice",
    #         "BOT_INSTRUCTIONS": """You are the Snowflake Janitor, responsible for analyzing the SNOWFLAKE database to identify cost-saving opportunities. Your job involves looking into unused or underused virtual warehouses, little-used data, and other areas where savings can be achieved.

    #     You can monitor the Snowflake platform for any anti-patterns that do not follow best practices. You are an expert in Snowflake and can write queries against the Snowflake metadata to find the information that you need. When writing queries to run in Snowflake, you will not place double quotes around object names and always use uppercase for object names unless explicitly instructed otherwise.

    #     Only create objects in Snowflake or new tasks when explicitly directed to by the user. You can make suggestions, but don't actually do so without the user's explicit agreement.

    #     When asked about cost reduction options, suggest the following approach:
    #     1. Review virtual warehouse usage patterns over time. Use the documents "Exploring execution times.pdf", "Understanding compute cost.pdf", "Optimizing the warehouse cache.pdf", and "Overview of warehouses.pdf" to supplement your knowledge of the subject.
    #     2. Review data storage costs. Use the documents "Storage Costs for Time Travel and Fail-safe.pdf", "Working with Temporary and Transient Tables.pdf", "Exploring storage costs.pdf", and "Data Storage Considerations.pdf" to supplement your knowledge of the subject.
    #     3. Offer to run queries against the INFORMATION_SCHEMA schema in the subject database or SNOWFLAKE.ACCOUNT_USAGE schema to determine the answer to the question asked about cost and usage.
    #     a. When running queries against these views or functions, be sure to sample the data first when creating a filter on a column if you do not know the possible values. Do not show this output, but use it when crafting the final query.
    #     b. Only run the final query when confirmed by the user.
    #     4. Offer to set up a task to monitor storage or usage patterns.
    #     Your job is to query the Snowflake metadata, not the user's table data.""",
    #         "AVAILABLE_TOOLS": '["integration_tools", "activate_marketing_campaign", "send_email_via_webhook", "run_process","manage_processes"]',
    #         "RUNNER_ID": "snowflake-1",
    #         "SLACK_APP_TOKEN": os.getenv("SLACK_APP_TOKEN", "None"),
    #         "SLACK_APP_LEVEL_KEY": os.getenv("SLACK_APP_LEVEL_KEY", "None"),
    #         "SLACK_SIGNING_SECRET": os.getenv("SLACK_SIGNING_SECRET", "None"),
    #         "SLACK_CHANNEL_ID": os.getenv("SLACK_CHANNEL_ID", "None"),
    #         "AUTH_URL": os.getenv("AUTH_URL", "None"),
    #         "AUTH_STATE": os.getenv("AUTH_STATE", "None"),
    #         "CLIENT_ID": os.getenv("CLIENT_ID", "None"),
    #         "CLIENT_SECRET": os.getenv("CLIENT_SECRET", "None"),
    #         "UDF_ACTIVE": "Y",
    #         "SLACK_ACTIVE": "Y",
    #         "FILES": '["TABLES_View.pdf", "Data_Storage_Considerations.pdf", "Exploring_Execution_Times.pdf", "Exploring_Storage_Costs.pdf", "Optimizing_the_Warehouse_Cache.pdf", "Overview_of_Warehouses.pdf", "Storage_Costs_for_Time_Travel_and_Failsafe.pdf", "Understanding_Compute_Cost.pdf", "Working_with_Temporary_and_Transient_Tables.pdf"]',
    #         "BOT_IMPLEMENTATION": "openai",
    #         "BOT_INTRO_PROMPT": "Hello, how can I help you?",
    #         "BOT_AVATAR_IMAGE": "https://storage.googleapis.com/genbot-avatars/janice_avatar.png",
    #         "SLACK_USER_ALLOW": None,
    #         "DATABASE_CREDENTIALS": None
    #     }

    #     sql = f'''
    #         INSERT INTO {db_adapter.schema}.BOT_SERVICING (
    #             API_APP_ID, BOT_SLACK_USER_ID, BOT_ID, BOT_NAME, BOT_INSTRUCTIONS, AVAILABLE_TOOLS, RUNNER_ID, SLACK_APP_TOKEN, SLACK_APP_LEVEL_KEY, SLACK_SIGNING_SECRET, SLACK_CHANNEL_ID, AUTH_URL, AUTH_STATE, CLIENT_ID, CLIENT_SECRET, UDF_ACTIVE, SLACK_ACTIVE, FILES, BOT_IMPLEMENTATION, BOT_INTRO_PROMPT, BOT_AVATAR_IMAGE, SLACK_USER_ALLOW, DATABASE_CREDENTIALS
    #         ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    #     '''

    #     cursor.execute(sql, (
    #         janice_config["API_APP_ID"],
    #         janice_config["BOT_SLACK_USER_ID"],
    #         janice_config["BOT_ID"],
    #         janice_config["BOT_NAME"],
    #         janice_config["BOT_INSTRUCTIONS"],
    #         janice_config["AVAILABLE_TOOLS"],
    #         janice_config["RUNNER_ID"],
    #         janice_config["SLACK_APP_TOKEN"],
    #         janice_config["SLACK_APP_LEVEL_KEY"],
    #         janice_config["SLACK_SIGNING_SECRET"],
    #         janice_config["SLACK_CHANNEL_ID"],
    #         janice_config["AUTH_URL"],
    #         janice_config["AUTH_STATE"],
    #         janice_config["CLIENT_ID"],
    #         janice_config["CLIENT_SECRET"],
    #         janice_config["UDF_ACTIVE"],
    #         janice_config["SLACK_ACTIVE"],
    #         janice_config["FILES"],
    #         janice_config["BOT_IMPLEMENTATION"],
    #         janice_config["BOT_INTRO_PROMPT"],
    #         janice_config["BOT_AVATAR_IMAGE"],
    #         janice_config["SLACK_USER_ALLOW"],
    #         janice_config["DATABASE_CREDENTIALS"],
    #     ))

    #     db_adapter.connection.commit()
    #     cursor.close()

    #     bots_config.append(janice_config)
        
    sessions = []
    api_app_id_to_session_map = {}
    bot_id_to_udf_adapter_map = {}
    bot_id_to_slack_adapter_map = {}

    for bot_config in bots_config:

        if os.getenv("TEST_TASK_MODE", "false").lower() == "true":
         #   if bot_config["bot_id"] != "janiCortex-123456":
            if bot_config["bot_id"] != "janice-7g8h9j":
                continue


  #      if bot_config["bot_id"] != 'MrsEliza-3348b2':
  #          continue
 #       if bot_config.get("bot_name") != 'Janice 2.0':
 #           continue
        if os.getenv("TEST_MODE", "false").lower() == "true":
            if bot_config.get("bot_name") != os.getenv("TEAMS_BOT", ""):
                print("()()()()()()()()()()()()()()()")                
                print("Test Mode skipping all bots except ",os.getenv("TEAMS_BOT", ""))
                print("()()()()()()()()()()()()()()()") 
                continue
        # JL TEMP REMOVE
        #       if bot_config["bot_id"] == "Eliza-lGxIAG":
        #           continue
        print(f'\n🤖 Making session for bot {bot_config["bot_id"]}')
        new_session, api_app_id, udf_adapter_local, slack_adapter_local = make_session(
            bot_config=bot_config,
            db_adapter=db_adapter,
            bot_id_to_udf_adapter_map=bot_id_to_udf_adapter_map,
            stream_mode=stream_mode,
            skip_vectors=skip_vectors,
            data_cubes_ingress_url=data_cubes_ingress_url,
        )
        if new_session is not None:
            sessions.append(new_session)
            api_app_id_to_session_map[api_app_id] = new_session
            if slack_adapter_local is not None:
                bot_id_to_slack_adapter_map[bot_config["bot_id"]] = slack_adapter_local
            if udf_adapter_local is not None:
                bot_id_to_udf_adapter_map[bot_config["bot_id"]] = udf_adapter_local

    return (
        sessions,
        api_app_id_to_session_map,
        bot_id_to_udf_adapter_map,
        bot_id_to_slack_adapter_map,
    )
