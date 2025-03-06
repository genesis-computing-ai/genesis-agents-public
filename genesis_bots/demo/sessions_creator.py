import json
import os
import sys
import types


from   genesis_bots.connectors.data_connector \
                                import DatabaseConnector

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from   genesis_bots.core.bot_os_corpus \
                                import URLListFileCorpus
from   genesis_bots.core.bot_os_defaults \
                                import (BASE_BOT_DB_CONDUCT_INSTRUCTIONS,
                                        BASE_BOT_INSTRUCTIONS_ADDENDUM,
                                        BASE_BOT_PRE_VALIDATION_INSTRUCTIONS,
                                        BASE_BOT_PROACTIVE_INSTRUCTIONS,
                                        BASE_BOT_PROCESS_TOOLS_INSTRUCTIONS,
                                        BASE_BOT_SLACK_TOOLS_INSTRUCTIONS,
                                        BASE_BOT_VALIDATION_INSTRUCTIONS)

from   genesis_bots.core.bot_os_memory \
                                import BotOsKnowledgeAnnoy_Metadata

from   genesis_bots.bot_genesis.make_baby_bot \
                                import get_all_bots_full_details
from   genesis_bots.core.bot_os_tools \
                                import ToolBelt, get_tools
from   genesis_bots.core.bot_os_tools2 \
                                import (get_global_tools_registry,
                                        get_tool_func_descriptor)
from   genesis_bots.llm.llm_cortex.bot_os_cortex \
                                import BotOsAssistantSnowflakeCortex
from   genesis_bots.slack.slack_bot_os_adapter \
                                import SlackBotAdapter

from   genesis_bots.core.bot_os_task_input_adapter \
                                import TaskBotOsInputAdapter
from   genesis_bots.core.bot_os_udf_proxy_input \
                                import UDFBotOsInputAdapter

from   genesis_bots.core.bot_os_tools \
                                import get_persistent_tools_descriptions

from   genesis_bots.core.logging_config \
                                import logger

from   genesis_bots.core        import global_flags

from   concurrent.futures       import ThreadPoolExecutor, as_completed


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

def get_legacy_sessions(bot_id: str, db_adapter) -> dict | list:
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


def _resolve_session_tools_info(bot_config, slack_adapter_local, db_adapter):
    '''helper function for make_session(...) to resolve tools & tool-functions info for a given bot '''

    #NOTE on naming: 'tools' are named groups of tool-functions


    # fetch some bot attributes that are needed for tool resolution
    bot_id = bot_config["bot_id"]
    slack_enabled = bot_config.get("slack_active", "Y") == "Y"

    # get the names of all tools (function groups) persistent in the DB (which are not bot-specific)
    all_p_tools_descriptions = get_persistent_tools_descriptions()
    all_p_tools_names = list(all_p_tools_descriptions.keys())

    logger.info(f"Number of all available persistent tools (listed in the DB) {len(all_p_tools_names)}")

    # get the list of tool (group) names that are configured for this bot.
    # Those currently do not include ephemetal tools
    if bot_config.get("available_tools", None) is not None:
        bot_p_tool_names = set(json.loads(bot_config["available_tools"]))
    else:
        bot_p_tool_names = set()

    logger.info(f"Number of bot-specific tools from bot config: {len(bot_p_tool_names)}")

    # remove slack tools if Slack is not enabled for this bot
    slack_enabled = bot_config.get("slack_active", "Y") == "Y"
    if not slack_enabled:
        bot_p_tool_names.discard("slack_tools")


    # 'old-style' tools require some legacy args to resolve to their callables
    tool_belt = ToolBelt()
    legacy_args = dict(
        tool_belt = tool_belt,
        db_adapter = db_adapter,
        slack_adapter_local = slack_adapter_local,
        include_slack = (slack_adapter_local is not None)
    )


    # get functions metadata for the (persistent) tools configured for this bot
    (available_p_func_descriptors,            # list of func descriptors dicts
     available_p_callables_map,               # map from func name to its callable
     _                                        # map from tool (group) name to a list of func descriptors
     ) = get_tools(which_tools=list(bot_p_tool_names), **legacy_args)

    logger.info(f"Number of available persistent functions for bot {bot_id}: {len(available_p_callables_map)}")

    # get ephemeral functions that are assigned to this bot and convert them to the same info structure as the persistent functions
    tools_registry = get_global_tools_registry()
    ephemeral_bot_callables = tools_registry.get_ephemeral_tool_funcs_for_bot(bot_id)
    logger.info(f"Number of available ephemeral functions for bot {bot_id}: {len(ephemeral_bot_callables)}")
    available_e_func_descriptors = [get_tool_func_descriptor(func).to_llm_description_dict()
                                    for func in ephemeral_bot_callables]
    available_e_callables_map = {get_tool_func_descriptor(func).name: func
                                 for func in ephemeral_bot_callables}

    # combine the avaialble persistent and ephemeral strcutures for the bot
    bot_e_tool_names = set(group.name
                           for func in ephemeral_bot_callables
                           for group in get_tool_func_descriptor(func).groups)
    bot_tool_names = bot_p_tool_names | bot_e_tool_names
    available_func_descriptors = available_p_func_descriptors + available_e_func_descriptors
    available_callables_map = {**available_p_callables_map, **available_e_callables_map}

    # get functions metadata for all the (persistent) tools (not bot-specific)
    all_tool_names = all_p_tools_names + list(bot_e_tool_names)
    (all_func_descriptions,                 # list of func descriptors dicts
     all_callables_map,                     # map from func name to its callable
     all_tool_to_func_descs_map             # map from tool (group) name to a list of func descriptors
     ) = get_tools(which_tools=all_tool_names, **legacy_args)

    logger.info(f"Number of all persistent functions: {len(all_callables_map)}")

    # Return a simple object with the resolved tools and functions
    info = types.SimpleNamespace(
        available_tool_names = bot_tool_names,
        available_func_descriptors = available_func_descriptors,
        available_callables_map = available_callables_map,
        all_func_descriptions = all_func_descriptions,
        all_callables_map = all_callables_map,
        all_tool_to_func_descs_map = all_tool_to_func_descs_map,
        ephemeral_bot_callables = ephemeral_bot_callables,
        tool_belt=tool_belt
    )

    return info


def make_session(
    bot_config,
    db_adapter,
    bot_id_to_udf_adapter_map={},
    stream_mode=False,
    skip_vectors=False,
    existing_slack=None,
    existing_udf=None,
    assistant_id=None,
    skip_slack=False
):
    from   genesis_bots.connectors               import get_global_db_connector
    from   textwrap                 import dedent, indent
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

    input_adapters = []

    if os.getenv("TEAMS_BOT") and bot_config["bot_name"] == os.getenv("TEAMS_BOT"):
        from genesis_bots.teams.teams_bot_os_adapter import TeamsBotOsInputAdapter
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
                logger.info(f"Starting Slack adapter creation for bot_id: {bot_config['bot_id']}")
                logger.info(f"Bot config details:")
                logger.info(f"- Bot name: {bot_config['bot_name']}")
                logger.info(f"- Bot user ID: {bot_config['bot_slack_user_id']}")
                logger.info(f"- Channel ID: {bot_config['slack_channel_id']}")
                logger.info(f"- Has app token: {'Yes' if bot_config.get('slack_app_token') else 'No'}")
                logger.info(f"- Has signing secret: {'Yes' if bot_config.get('slack_signing_secret') else 'No'}")
                logger.info(f"- Has app level token: {'Yes' if app_level_token else 'No'}")

                try:
                    logger.info("Fetching legacy sessions from database...")
                    legacy_sessions = get_legacy_sessions(bot_config['bot_id'], db_adapter)
                    logger.info(f"Found {len(legacy_sessions) if legacy_sessions else 0} legacy sessions")
                except Exception as e:
                    logger.error(f"Error getting legacy sessions: {str(e)}")
                    legacy_sessions = None

                try:
                    logger.info("Creating SlackBotAdapter instance...")
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
                    logger.info("Successfully created SlackBotAdapter instance")
                except Exception as e:
                    logger.error(f"Failed to create SlackBotAdapter: {str(e)}")
                    logger.error(f"Error details: {type(e).__name__}")
                    raise
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
            logger.info(f'Failed to create Slack adapter with the provided configuration for bot {bot_config["bot_name"]}')
            logger.error(f'Failed to create Slack adapter with the provided configuration for bot {bot_config["bot_name"]}')

    # Use _resolve_session_tools_info to get tool information
    tools_info = _resolve_session_tools_info(bot_config, slack_adapter_local, db_adapter)

    simple_mode = os.getenv("SIMPLE_MODE", "false").lower() == "true"

    instructions = bot_config["bot_instructions"] + "\n"
    bot_id = bot_config["bot_id"]

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
    # TODO ADD INFO HERE
    instructions += BASE_BOT_INSTRUCTIONS_ADDENDUM

    instructions += f'\nYour default database connection is called "{genesis_source}".\n'

    instructions += f'\nNote current settings:\nYour bot_id: {bot_config["bot_id"]}.\nRunner_id: {runner_id}'
    if bot_config["slack_active"] == "Y":
        instructions += "\nYour slack user_id: " + bot_config["bot_slack_user_id"]

    if "snowflake_stage_tools" in tools_info.available_tool_names and "make_baby_bot" in tools_info.available_tool_names:
        instructions += f"\nYour Internal Files Stage for bots is at snowflake stage: {db_adapter.genbot_internal_project_and_schema}.BOT_FILES_STAGE"
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

    if "data_connector_tools" in tools_info.available_tool_names:
        instructions += "\n" + dedent("""
            When working with database connections:
            - Use _list_database_connections to see available connections
            - Use _search_metadata to find relevant tables and columns
            - Use _get_full_table_details for complete table information
            - Always verify connection exists before trying to query it
            """)

        from genesis_bots.connectors.data_connector import DatabaseConnector
        connector = DatabaseConnector()
        connections = connector.list_database_connections(bot_id=bot_id)
        # Get list of available database connections for this bot
        try:
            if connections and len(connections) > 0:
                logger.info(f"Found {len(connections['connections'])} database connections for bot {bot_id}")
                conn_list = []
                for conn in connections['connections']:
                    conn_details = f"- Connection ID: {conn['connection_id']}\n"
                    conn_details += f"  Type: {conn['db_type']}\n"
                    if conn['description']:
                        conn_details += f"  Description: {conn['description']}\n"
                    conn_list.append(conn_details)

                conn_list_str = "\n".join(conn_list)
                instructions += f"\n\nYou have access to the following database connections:\n{conn_list_str}\n"
        except Exception as e:
            logger.info(f"Error getting database connections for bot_id {bot_id}: {e}")

    # check if snowflake_tools are in bot_tools
    if "snowflake_tools" in tools_info.available_tool_names:
        try:
            # if so, create workspace schema

            workspace_schema_name = f"{global_flags.project_id}.{bot_id.replace(r'[^a-zA-Z0-9]', '_').replace('-', '_').replace('.', '_')}_WORKSPACE".upper()
            db_adapter.create_bot_workspace(workspace_schema_name)
            db_adapter.grant_all_bot_workspace(workspace_schema_name)
            instructions += f"\nYou have a workspace schema created specifically for you named {workspace_schema_name} that the user can also access. You may use this schema for creating tables, views, and stages that are required when generating answers to data analysis questions. Only use this schema if asked to create an object. Always return the full location of the object.\nYour default stage is {workspace_schema_name}.MY_STAGE. "
            instructions += "\n" + BASE_BOT_DB_CONDUCT_INSTRUCTIONS
        except Exception as e:
            logger.info(f"Error creating bot workspace for bot_id {bot_id}: {e} ")

    #add proces mgr instructions
    if "process_manager_tools" in tools_info.available_tool_names or "notebook_manager_tools" in tools_info.available_tool_names:
        instructions += "\n" + BASE_BOT_PROCESS_TOOLS_INSTRUCTIONS

    if "slack_tools" in tools_info.available_tool_names:
        instructions += "\n" + BASE_BOT_SLACK_TOOLS_INSTRUCTIONS

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
                UDFBotOsInputAdapter(bot_id=bot_id) if stream_mode else TaskBotOsInputAdapter()
            )
            bot_id_to_udf_adapter_map[bot_id] = udf_adapter_local
     #   udf_adapter_local = (
     #       UDFBotOsInputAdapter(bot_id=bot_id) if stream_mode else TaskBotOsInputAdapter()
     #   )
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

    assistant_implementation = None
    actual_llm = None
    logger.info(f"Bot implementation from bot config: {bot_config.get('bot_implementation', 'Not specified')}")

    if "bot_implementation" in bot_config:
        # Override with Cortex if environment variable is set
        if os.environ.get("CORTEX_OVERRIDE", "").lower() == "true":
            logger.info(f'Cortex override for bot {bot_id} due to ENV VAR')
            bot_config["bot_implementation"] = "cortex"

        llm_type = bot_config["bot_implementation"]

        from   genesis_bots.llm.llm_openai.bot_os_openai import BotOsAssistantOpenAI

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
            logger.warning(f"Using {actual_llm} for bot {bot_id}")
        else:
            logger.warning(f"No suitable LLM found for bot {bot_id}")

        # Updating an existing bot's preferred_llm
        bot_llms[bot_id] = {"current_llm": actual_llm, "preferred_llm": bot_config["bot_implementation"]}

        bot_llms_json = json.dumps(bot_llms)

        # Save the JSON string as an environment variable
        os.environ["BOT_LLMS"] = bot_llms_json
        os.environ["BOT_LLM_"+bot_id] = actual_llm

        # if assistant_implementation == BotOsAssistantSnowflakeCortex and stream_mode:
        if assistant_implementation == BotOsAssistantSnowflakeCortex and True:
            incoming_instructions = instructions

            instructions = """

# Tool Instructions
"""
            instructions += """You have access to the following groups of functions, only call them when needed to perform actions or lookup information that you do not already have:

""" + json.dumps(tools_info.available_func_descriptors) + """


If a you choose to call a function ONLY reply in the following format:
<function={function_name}>{parameters}</function>

where

function_name => the name of the function from the list above
parameters => a JSON dict with the function argument name as key and function argument value as value.

Here is an example,
<function=example_function_name>{"example_name": "example_value"}</function>

Here is another example, with a parameter value containg properly escaped double quotes:
<function=_query_database>{"query": "select * from \\"DATABASE_NAME\\".\\"SCHEMA_NAME\\".\\"TABLE_NAME\";"}</function>

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

    try:
        from   genesis_bots.core.bot_os              import BotOsSession
        asst_impl = assistant_implementation # test this - may need separate BotOsSession call for stream mode
        logger.info(f"assistant impl : {assistant_implementation}")
        session = BotOsSession(
            bot_config["bot_id"],
            instructions=instructions + proactive_instructions + pre_validation,
            validation_instructions=post_validation,
            input_adapters=input_adapters,
            knowledgebase_implementation=BotOsKnowledgeAnnoy_Metadata(
                f"./kb_{bot_config['bot_id']}", refresh=True, bot_id=bot_config['bot_id']
            ),
            file_corpus=(
                URLListFileCorpus(json.loads(bot_config["files"]))
                if bot_config["files"]
                else None
            ),
            update_existing=True,
            assistant_implementation=asst_impl,
            log_db_connector=db_adapter,  # Ensure connection_info is defined or fetched appropriately
            tools=tools_info.available_func_descriptors,
            bot_name=bot_config["bot_name"],
            available_functions=tools_info.available_callables_map,
            all_tools=tools_info.all_func_descriptions,
            all_functions=tools_info.all_callables_map,
            all_function_to_tool_map=tools_info.all_tool_to_func_descs_map,
            bot_id=bot_config["bot_id"],
            stream_mode=stream_mode,
            tool_belt=tools_info.tool_belt,
            skip_vectors=skip_vectors,
            assistant_id=assistant_id
        )
    except Exception as e:
        logger.info("Session creation exception: ", e)
        raise (e)
    if os.getenv("BOT_BE_PROACTIVE", "FALSE").lower() == "true" and slack_adapter_local:
        if not slack_adapter_local.channel_id:
            logger.warn("not adding initial task - slack_adapter_local channel_id is null")
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
    bot_list=None,
    skip_slack=False,
    max_workers=5, # New parameter to control parallel execution
    llm_change=False
):
    """
    Create (multiple) sessions for bots in parallel based on the provided configurations.
    """
    runner_id = os.getenv("RUNNER_ID", "jl-local-runner")
    bots_config = get_all_bots_full_details(runner_id=runner_id)
    logger.info(f"Total configured bots: {len(bots_config)}")
    if bot_list:
        bot_names = [bot["bot_id"] for bot in bot_list]
        logger.info(f"Creating sessions only for bot(s): {', '.join(bot_names)}")
    # Check if there are any bots to start
    if len(bots_config) == 0 or (bot_list and not any(bot["bot_id"] in [b["bot_id"] for b in bot_list] for bot in bots_config)):
        logger.info("No bots will be started.")

    sessions = []
    api_app_id_to_session_map = {}
    bot_id_to_udf_adapter_map = {}
    bot_id_to_slack_adapter_map = {}

    # Filter bots based on test mode and bot list
    filtered_bots = []
    for bot_config in bots_config:
        if bot_list is not None and bot_config["bot_id"] not in [bot["bot_id"] for bot in bot_list]:
            logger.info(f'Skipping bot {bot_config["bot_id"]} - not in bot_list')
            continue

        if os.getenv("TEST_TASK_MODE", "false").lower() == "true":
            if bot_config["bot_id"] != "MrSpock-3762b2":
                continue

        if os.getenv("TEST_MODE", "false").lower() == "true":
            if bot_config.get("bot_name") != os.getenv("TEST_BOT", "") and os.getenv("TEST_BOT", "").upper() != "ALL":
                logger.info(f"Test Mode skipping bot {bot_config.get('bot_name')}")
                continue

        filtered_bots.append(bot_config)

    def create_single_session(bot_config):
        try:
            bot_id = bot_config["bot_id"]
            assistant_id = None

            # Add LLM change check here
            if llm_change:
                os.environ[f'RESET_BOT_SESSION_{bot_id}'] = 'True'

            for bot in bot_list or []:
                if bot["bot_id"] == bot_id:
                    assistant_id = bot.get("assistant_id", None)
                    break

            logger.info(f'🤖 Making session for bot_id={bot_config["bot_id"]} (bot_name={bot_config["bot_name"]})')
            logger.telemetry('add_session:', bot_config['bot_name'], os.getenv("BOT_OS_DEFAULT_LLM_ENGINE", ""))

            return make_session(
                bot_config=bot_config,
                db_adapter=db_adapter,
                bot_id_to_udf_adapter_map=bot_id_to_udf_adapter_map,
                stream_mode=stream_mode,
                skip_vectors=skip_vectors,
                assistant_id=assistant_id,
                skip_slack=skip_slack
            )
        except Exception as e:
            logger.error(f"Error creating session for bot {bot_id}: {str(e)}")
            return None, None, None, None

    # Create sessions in parallel
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_bot = {
            executor.submit(create_single_session, bot_config): bot_config
            for bot_config in filtered_bots
        }
        for future in as_completed(future_to_bot):
            bot_config = future_to_bot[future]
            try:
                new_session, api_app_id, udf_adapter_local, slack_adapter_local = future.result()
                if new_session is not None:
                    sessions.append(new_session)
                    api_app_id_to_session_map[api_app_id] = new_session
                    if slack_adapter_local is not None:
                        bot_id_to_slack_adapter_map[bot_config["bot_id"]] = slack_adapter_local
                    if udf_adapter_local is not None:
                        bot_id_to_udf_adapter_map[bot_config["bot_id"]] = udf_adapter_local
            except Exception as e:
                logger.error(f"Error processing results for bot {bot_config['bot_id']}: {str(e)}")

    return (
        sessions,
        api_app_id_to_session_map,
        bot_id_to_udf_adapter_map,
        bot_id_to_slack_adapter_map,
    )
