import json
import os
import sys

from connectors.database_connector import DatabaseConnector
from llm_gemini.bot_os_gemini import BotOsAssistantGemini

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from core.bot_os import BotOsSession
from core.bot_os_corpus import URLListFileCorpus
from core.bot_os_defaults import (
    BASE_BOT_INSTRUCTIONS_ADDENDUM,
    BASE_BOT_PRE_VALIDATION_INSTRUCTIONS,
    BASE_BOT_PROACTIVE_INSTRUCTIONS,
    BASE_BOT_VALIDATION_INSTRUCTIONS,
    BASE_BOT_DB_CONDUCT_INSTRUCTIONS,
    BASE_BOT_PROCESS_TOOLS_INSTRUCTIONS,
    BASE_BOT_SLACK_TOOLS_INSTRUCTIONS
)

from core.bot_os_memory import BotOsKnowledgeAnnoy_Metadata

from core.bot_os_tools import get_tools, ToolBelt
from llm_cortex.bot_os_cortex import BotOsAssistantSnowflakeCortex
from llm_openai.bot_os_openai import BotOsAssistantOpenAI
from slack.slack_bot_os_adapter import SlackBotAdapter
from bot_genesis.make_baby_bot import (
    get_available_tools,
    get_all_bots_full_details,
)

from streamlit_gui.udf_proxy_bot_os_adapter import UDFBotOsInputAdapter
from core.bot_os_task_input_adapter import TaskBotOsInputAdapter



from core.logging_config import logger

import core.global_flags as global_flags

genbot_internal_project_and_schema = os.getenv("GENESIS_INTERNAL_DB_SCHEMA", "None")
if genbot_internal_project_and_schema == "None":
    logger.info("ENV Variable GENESIS_INTERNAL_DB_SCHEMA is not set.")
if genbot_internal_project_and_schema is not None:
    genbot_internal_project_and_schema = genbot_internal_project_and_schema.upper()
db_schema = genbot_internal_project_and_schema.split(".")
project_id = db_schema[0]
dataset_name = db_schema[1]

genesis_source = os.getenv("GENESIS_SOURCE", default="Snowflake")

def _configure_openai_or_azure_openai(db_adapter:DatabaseConnector) -> bool:
    llm_keys_and_types = db_adapter.db_get_active_llm_key()

    if llm_keys_and_types.llm_type is not None and llm_keys_and_types.llm_type.lower() == "openai":
            os.environ["OPENAI_API_KEY"] = llm_keys_and_types.llm_key
            os.environ["AZURE_OPENAI_API_ENDPOINT"] = llm_keys_and_types.llm_endpoint
            if llm_keys_and_types.llm_endpoint:
                os.environ["OPENAI_MODEL_NAME"] = llm_keys_and_types.model_name
                os.environ["OPENAI_HARVESTER_EMBEDDING_MODEL"] = llm_keys_and_types.embedding_model_name
            return True
    return False

def get_legacy_sessions(bot_id: str, db_adapter) -> dict:
    """
    Gets legacy thread_ts values for a bot by querying message_log table.

    Args:
        bot_id (str): ID of the bot to query legacy threads for
        db_adapter: Database adapter instance to execute query

    Returns:
        dict: Dictionary mapping thread_ts to max timestamp
    """
    sql = f"""
    select parse_json(message_metadata):thread_ts::varchar as thread_ts, max(timestamp) as max_ts
    from {db_adapter.genbot_internal_project_and_schema}.message_log
    where message_metadata is not null
    and message_metadata like '%"thread_ts"%'
    and message_metadata not like '%TextContentBlock%'
    and bot_id = '{bot_id}'
    group by bot_id, thread_ts
    order by max_ts desc
    limit 1000
    """
    threads = []
    try:
        results = db_adapter.run_query(sql)
        for row in results:
            threads.append(row['THREAD_TS'])
    except Exception as e:
        logger.error(f"Error getting legacy sessions for bot {bot_id}: {str(e)}")
        threads = []

    return threads

def make_session(
    bot_config,
    db_adapter,
    bot_id_to_udf_adapter_map={},
    stream_mode=False,
    skip_vectors=False,
    data_cubes_ingress_url=None,
    existing_slack=None,
    existing_udf=None,
    assistant_id=None,
    skip_slack=False
):
    """
    Create a single session for a bot based on the provided configuration.

    This function initializes a session for a bot using the given database adapter and configuration details.
    It sets up the necessary environment for the bot to operate, including input adapters and other configurations.

    Args:
        bot_config (dict): Configuration details for the bot.
        db_adapter: The database adapter used to interact with the database.
        bot_id_to_udf_adapter_map (dict, optional): A dictionary mapping bot IDs to their UDF adapters.
        stream_mode (bool, optional): Indicates whether the session should be created in stream mode.
        skip_vectors (bool, optional): If True, skips vector-related operations during session creation.
        data_cubes_ingress_url (str, optional): The URL for data cubes ingress, if applicable.
        existing_slack: An existing Slack adapter instance, if any.
        existing_udf: An existing UDF adapter instance, if any.

    Returns:
        tuple: A tuple containing the session, API app ID, UDF adapter, and Slack adapter.
    """

    # streamlit and slack launch todos:
    # add a flag for udf_enabled and slack_enabled to database
    # launch them accordingly
    # add a tool to deploy and un-deploy an existing to slack but keep it in the DB
    # add multi-bot display to streamlit (tabs)
    # add launch to slack button to streamlit
    # add setup harvester button to streamlit

    udf_enabled = bot_config.get("udf_active", "Y") == "Y"
    slack_enabled = bot_config.get("slack_active", "Y") == "Y"
    teams_enabled = bot_config.get("teams_active", "N") == "Y"
    runner_id = os.getenv("RUNNER_ID", "jl-local-runner")

#    if global_flags.slack_active is False and slack_enabled:
#        global_flags.slack_active = False

    input_adapters = []

    if teams_enabled:
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
    if not skip_slack and slack_enabled and existing_slack is None:
        try:
            app_level_token = bot_config.get("slack_app_level_key", None)

            # Stream mode is for interactive bot serving, False means task server
            if stream_mode:
                logger.info(f"Making Slack adapter for bot_id: {bot_config['bot_id']} named {bot_config['bot_name']} with bot_user_id: {bot_config['bot_slack_user_id']}")
                legacy_sessions = get_legacy_sessions(bot_config['bot_id'], db_adapter)
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
                    legacy_sessions = legacy_sessions
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
            logger.info(
                f'Failed to create Slack adapter with the provided configuration for bot {bot_config["bot_name"]} '
            )
            logger.error(
                f'Failed to create Slack adapter with the provided configuration for bot {bot_config["bot_name"]} '
            )
      #      return None, None, None, None

    # tools
    available_tools = get_available_tools()
    logger.info(f"Number of available tools: {len(available_tools)}")
    # available_tools.append({'tool_name': "integration_tools", 'tool_description': 'integration tools'})
    # available_tools.append({'tool_name': "activate_marketing_campaign", 'tool_description': 'activate_marketing_campaign'})
    # available_tools.append({'tool_name': "send_email_via_webhook", 'tool_description': 'send_email_via_webhook'})

    if bot_config.get("available_tools", None) is not None:
        bot_tools = json.loads(bot_config["available_tools"])
        logger.info(f"Number of bot-specific tools: {len(bot_tools)}")
        # bot_tools.append({'tool_name': "integration_tools", 'tool_description': 'integration tools'})
        # bot_tools.append({'tool_name': "activate_marketing_campaign", 'tool_description': 'activate_marketing_campaign'})
        # bot_tools.append({'tool_name': "send_email_via_webhook", 'tool_description': 'send_email_via_webhook'})
    else:
        bot_tools = []

    # Check if SIMPLE_MODE environment variable is set to 'true'
    bot_id = bot_config["bot_id"]
    logger.info(f"setting local bot id = {bot_id}")

    # remove slack tools if Slack is not enabled for this bot
    if not slack_enabled:
        bot_tools = [tool for tool in bot_tools if tool != "slack_tools"]


    # ToolBelt seems to be a local variable that is used as a global variable by some tools
    tool_belt = ToolBelt()

    tools, available_functions, function_to_tool_map = get_tools(
        bot_tools, slack_adapter_local=slack_adapter_local, db_adapter=db_adapter, tool_belt=tool_belt
    )
    logger.info(f"Number of available functions for bot {bot_id}: {len(available_functions)}")
    all_tools, all_functions, all_function_to_tool_map = get_tools(
        available_tools, slack_adapter_local=slack_adapter_local, db_adapter=db_adapter, tool_belt=tool_belt
    )
    logger.info(f"Number of all functions for bot {bot_id}: {len(all_functions)}")

    simple_mode = os.getenv("SIMPLE_MODE", "false").lower() == "true"

    instructions = bot_config["bot_instructions"] + "\n"

    cursor = db_adapter.client.cursor()
    query = f"SELECT process_name, process_id, process_description FROM {db_adapter.schema}.PROCESSES where bot_id = %s"
    cursor.execute(query, (bot_id,))
    result = cursor.fetchall()

    if result:
        process_info = ""
        for row in result:
            process_info += f"- Process ID: {row[1]}\n  Name: {row[0]}\n  Description: {row[2]}\n\n"
        instructions += process_info
        processes_found = ', '.join([row[0] for row in result])
        instructions += f"\n\nFYI, here are some of the processes you have available:\n{process_info}.\nThey can be run with _run_process function if useful to your work. This list may not be up to date, you can use _manage_process with action LIST to get a full list, especially if you are asked to run a process that is not on this list.\n\n"
        logger.info(f'appended process list to prompt, len={len(processes_found)}')

# TODO ADD INFO HERE
    instructions += BASE_BOT_INSTRUCTIONS_ADDENDUM

    instructions += f'\nYour default database connection is called "{genesis_source}".\n'

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
            instructions += "\n" + BASE_BOT_DB_CONDUCT_INSTRUCTIONS
            if data_cubes_ingress_url:
                logger.info(
                    f"Setting data_cubes_ingress_url for {bot_id}: {data_cubes_ingress_url}"
                )
       #         instructions += f"\nWhenever you show the results from run_query that may have more than 10 rows, and if you are not in the middle of running a process, also provide a link to a datacube visualization to help them understand the data you used in the form: http://{data_cubes_ingress_url}%ssql_query=select%20*%20from%20spider_data.baseball.all_star -- replace the value of the sql_query query parameter with the query you used."
        except Exception as e:
            logger.info(f"Error creating bot workspace for bot_id {bot_id}: {e} ")

    #add proces mgr instructions
    if "process_manager_tools" in bot_tools or "notebook_manager_tools" in bot_tools:
        instructions += "\n" + BASE_BOT_PROCESS_TOOLS_INSTRUCTIONS

    if "slack_tools" in bot_tools:
        instructions += "\n" + BASE_BOT_SLACK_TOOLS_INSTRUCTIONS

    # logger.info(instructions, f'{bot_config["bot_name"]}, id: {bot_config["bot_id"]}' )

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
    logger.info(f"Bot implementation from bot config: {bot_config.get('bot_implementation', 'Not specified')}")

    if "bot_implementation" in bot_config:
        # Override with Cortex if environment variable is set
        if os.environ.get("CORTEX_OVERRIDE", "").lower() == "true":
            logger.info(f'Cortex override for bot {bot_id} due to ENV VAR')
            bot_config["bot_implementation"] = "cortex"

        llm_type = bot_config["bot_implementation"]

        # Handle Cortex implementation
        if llm_type == "cortex":
            if db_adapter.check_cortex_available():
                assistant_implementation = BotOsAssistantSnowflakeCortex
                actual_llm = 'cortex'
            else:
                logger.info('Snowflake Cortex is not available. Falling back to OpenAI.')
                if _configure_openai_or_azure_openai(db_adapter=db_adapter):
                    assistant_implementation = BotOsAssistantOpenAI
                    actual_llm = 'openai'
                else:
                    logger.info("OpenAI LLM key not set. Bot session cannot be created.")

        # Handle OpenAI implementation
        elif llm_type == "openai":
            if _configure_openai_or_azure_openai(db_adapter):
                assistant_implementation = BotOsAssistantOpenAI
                actual_llm = 'openai'
            else:
                logger.info("OpenAI LLM key not set. Attempting Cortex.")
                if db_adapter.check_cortex_available():
                    assistant_implementation = BotOsAssistantSnowflakeCortex
                    actual_llm = 'cortex'
                else:
                    logger.info('Snowflake Cortex is not available. No OpenAI key set. Bot session cannot be created.')

        # Handle default case
        else:
            default_llm = os.getenv("BOT_OS_DEFAULT_LLM_ENGINE", "cortex").lower()
            if default_llm == "cortex" and db_adapter.check_cortex_available():
                assistant_implementation = BotOsAssistantSnowflakeCortex
                actual_llm = 'cortex'
            elif default_llm == "openai" and _configure_openai_or_azure_openai(db_adapter):
                assistant_implementation = BotOsAssistantOpenAI
                actual_llm = 'openai'
            else:
                logger.info('Bot implementation not specified, and no available LLM found. Please set LLM key in Streamlit.')

        if assistant_implementation:
            logger.info(f"Using {actual_llm} for bot {bot_id}")
        else:
            logger.info(f"No suitable LLM found for bot {bot_id}")

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
        # logger.info(f'tools: {tools}')
        asst_impl = (
#            assistant_implementation if stream_mode else None
            assistant_implementation
        )  # test this - may need separate BotOsSession call for stream mode
        logger.info(f"assistant impl : {assistant_implementation}")
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
            assistant_id=assistant_id
        )
    except Exception as e:
        logger.info("Session creation exception: ", e)
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

    # logger.info('here: session: ',session)
    return session, api_app_id, udf_adapter_local, slack_adapter_local


def create_sessions(
    db_adapter,
    UNUSEDbot_id_to_udf_adapter_map,
    stream_mode=False,
    skip_vectors=False,
    data_cubes_ingress_url=None,
    bot_list=None,
    skip_slack=False,
):
    """
    Create (multiple) sessions for bots based on the provided configurations.

    This function initializes sessions for each bot using the given database adapter and configuration details.
    It maps bot IDs to their respective UDF adapters and sets up the necessary environment for each bot to operate.

    Args:
        db_adapter: The database adapter used to interact with the database.
        bot_id_to_udf_adapter_map: A dictionary mapping bot IDs to their UDF adapters.
        stream_mode (bool): Indicates whether the sessions should be created in stream mode.
        skip_vectors (bool): If True, skips vector-related operations during session creation.
        data_cubes_ingress_url (str, optional): The URL for data cubes ingress, if applicable.

    Returns:
        tuple: A tuple containing the sessions, API app ID to session map, bot ID to UDF adapter map,
               and bot ID to Slack adapter map.
    """
    import os
    # Fetch bot configurations for the given runner_id from BigQuery
    runner_id = os.getenv("RUNNER_ID", "jl-local-runner")

    bots_config = get_all_bots_full_details(runner_id=runner_id)

    sessions = []
    api_app_id_to_session_map = {}
    bot_id_to_udf_adapter_map = {}
    bot_id_to_slack_adapter_map = {}

    for bot_config in bots_config:

        if bot_list is not None and bot_config["bot_id"] not in [bot["bot_id"] for bot in bot_list]:
            logger.info(f'Skipping bot {bot_config["bot_id"]} - not in bot_list')
            continue

        if os.getenv("TEST_TASK_MODE", "false").lower() == "true":
         #   if bot_config["bot_id"] != "janiCortex-123456":
            if bot_config["bot_id"] != "MrSpock-3762b2":
                continue


  #      if bot_config["bot_id"] != 'MrsEliza-3348b2':
  #          continue
 #       if bot_config.get("bot_name") != 'Janice 2.0':
 #           continue
        if os.getenv("TEST_MODE", "false").lower() == "true":
            if bot_config.get("bot_name") != os.getenv("TEST_BOT", "") and os.getenv("TEST_BOT", "").upper() != "ALL":
                print("()()()()()()()()()()()()()()()")
                print(f"Test Mode skipping bot {bot_config.get('bot_name')} except ",os.getenv("TEST_BOT", ""))
                print("()()()()()()()()()()()()()()()")
                continue
            # JL TEMP REMOVE
        #       if bot_config["bot_id"] == "Eliza-lGxIAG":
        #           continue
        logger.info(f'🤖 Making session for bot {bot_config["bot_id"]}')
        logger.telemetry('add_session:', bot_config['bot_name'], os.getenv("BOT_OS_DEFAULT_LLM_ENGINE", ""))

        bot_id = bot_config["bot_id"]
        assistant_id = None

        for bot in bot_list or []:
            if bot["bot_id"] == bot_id:
                assistant_id = bot.get("assistant_id", None)
                break

        new_session, api_app_id, udf_adapter_local, slack_adapter_local = make_session(
            bot_config=bot_config,
            db_adapter=db_adapter,
            bot_id_to_udf_adapter_map=bot_id_to_udf_adapter_map,
            stream_mode=stream_mode,
            skip_vectors=skip_vectors,
            data_cubes_ingress_url=data_cubes_ingress_url,
            assistant_id=assistant_id,
            skip_slack=skip_slack
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
