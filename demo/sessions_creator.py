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

genesis_source = os.getenv("GENESIS_SOURCE", default="Snowflake")

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
    bot_config, db_adapter, bot_id_to_udf_adapter_map={}, stream_mode=False, skip_vectors=False, data_cubes_ingress_url=None,
):

    if not stream_mode:
        test_task_mode = os.getenv("TEST_TASK_MODE", "false").lower() == "true"
        #if test_task_mode and bot_config["bot_name"] != "Eliza":
        #    return None, None, None, None

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

    if global_flags.slack_active is False and slack_enabled:
        global_flags.slack_active = False

    input_adapters = []

    slack_adapter_local = None
    if slack_enabled:
        try:
            app_level_token = bot_config.get("slack_app_level_key", None)

            if stream_mode:
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
    # available_tools.append({'tool_name': "integration_tools", 'tool_description': 'integration tools'})
    # available_tools.append({'tool_name': "activate_marketing_campaign", 'tool_description': 'activate_marketing_campaign'})
    # available_tools.append({'tool_name': "send_email_via_webhook", 'tool_description': 'send_email_via_webhook'})

    if bot_config.get("available_tools", None) is not None:
        bot_tools = json.loads(bot_config["available_tools"])
        # bot_tools.append({'tool_name': "integration_tools", 'tool_description': 'integration tools'})
        # bot_tools.append({'tool_name': "activate_marketing_campaign", 'tool_description': 'activate_marketing_campaign'})
        # bot_tools.append({'tool_name': "send_email_via_webhook", 'tool_description': 'send_email_via_webhook'})
    else:
        bot_tools = []

    # Check if SIMPLE_MODE environment variable is set to 'true'

    # remove slack tools if Slack is not enabled for this bot
    if not slack_enabled:
        bot_tools = [tool for tool in bot_tools if tool != "slack_tools"]

    tools, available_functions, function_to_tool_map = get_tools(
        bot_tools, slack_adapter_local=slack_adapter_local, db_adapter=db_adapter
    )
    all_tools, all_functions, all_function_to_tool_map = get_tools(
        available_tools, slack_adapter_local=slack_adapter_local, db_adapter=db_adapter
    )

    simple_mode = os.getenv("SIMPLE_MODE", "false").lower() == "true"
    if simple_mode and stream_mode:  # stream mode is for multibot, not task mode
        print("SIMPLE MODE TOOLS OVERRIDE *** ")
        # Code to execute in simple mode
        tools = [
            t
            for t in tools
            if t["function"]["name"]
            in [
                "run_query",
                "make_baby_bot",
                "search_metadata",
            ]
        ]

    instructions = (
        bot_config["bot_instructions"] + "\n" + BASE_BOT_INSTRUCTIONS_ADDENDUM
    )
    instructions += f'\nNote current settings:\nData source: {genesis_source}\nYour bot_id: {bot_config["bot_id"]}.\nRunner_id: {runner_id}'
    if bot_config["slack_active"] == "Y" and global_flags.slack_active:
        instructions += "\nYour slack user_id: " + bot_config["bot_slack_user_id"]

    if "snowflake_stage_tools" in bot_tools and "make_baby_bot" in bot_tools:
        instructions += f"\nYour Internal Files Stage for bots is at snowflake stage: {genbot_internal_project_and_schema}.BOT_FILES_STAGE"
        if not stream_mode:
            instructions += ". This BOT_FILES_STAGE stage is ONLY in this particular database & schema."
    
    bot_id = bot_config["bot_id"]

    # check if database_tools are in bot_tools
    if "database_tools" in bot_tools:
        try:
            #if so, create workspace schema
            
            workspace_schema_name = f"{project_id}.{bot_id}_WORKSPACE".replace('-', '_').upper()
            db_adapter.create_bot_workspace(workspace_schema_name)
            db_adapter.grant_all_bot_workspace(workspace_schema_name)
            instructions += f"\nYou have a workspace schema created specifically for you named {workspace_schema_name} that the user can also access. You may use this schema for creating tables, views, and stages that are required when generating answers to data analysis questions. Only use this schema if asked to create an object. Always return the full location of the object."
            if data_cubes_ingress_url:
                logger.warning(f"Setting data_cubes_ingress_url for {bot_id}: {data_cubes_ingress_url}")
                instructions += f"\nWhenever you show the results from run_query, always provide a link to a datacube visualization to help them understand the data you used in the form: http://{data_cubes_ingress_url}?sql_query=select%20*%20from%20spider_data.baseball.all_star -- replace the value of the sql_query query parameter with the query you used."
        except Exception as e:
            logger.warning(f"Error creating bot workspace for bot_id {bot_id} {e} ")


    if simple_mode and stream_mode:
        instructions = "You are a smart data analyst named Eliza. Use emojiis to express your fun personality. You have access to 2 tools, semantic_copilot to get SQL for a natural language prompt, and run_query to execute the sql you get. Use lots of emojis to express your personality. Return data grids and sql statements in three backticks example: ``` <data> or <sql> ```. DO NOT HALUCINATE tool calls or results of tools."

    # print(instructions, f'{bot_config["bot_name"]}, id: {bot_config["bot_id"]}' )

    # TESTING UDF ADAPTER W/EVE and ELSA
    # add a map here to track botid to adapter mapping

    udf_adapter_local = None
    if udf_enabled:
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

    if stream_mode or os.getenv("BOT_DO_PLANNING_REFLECTION"):
        pre_validation = BASE_BOT_PRE_VALIDATION_INSTRUCTIONS
        post_validation = BASE_BOT_VALIDATION_INSTRUCTIONS
    else:
        pre_validation = ""
        post_validation = None
    if os.getenv("BOT_BE_PROACTIVE", "True").lower() == "true":
        proactive_instructions = BASE_BOT_PROACTIVE_INSTRUCTIONS
    else:
        proactive_instructions = ""

    if stream_mode:
        if (
            "bot_implementation" in bot_config
            and bot_config["bot_implementation"] == "cortex"
        ):
            assistant_implementation = BotOsAssistantSnowflakeCortex
        elif (
            "bot_implementation" in bot_config
            and bot_config["bot_implementation"] == "gemini"
        ):
            assistant_implementation = BotOsAssistantGemini
        else:
            assistant_implementation = BotOsAssistantOpenAI

        if os.getenv("SIMPLE_MODE", "false").lower() == "true":
            assistant_implementation = BotOsAssistantSnowflakeCortex
        # assistant_implementation = BotOsAssistantOpenAI

    try:
        # logger.warning(f"GenBot {bot_id} instructions:::  {instructions}")
        # print(f'tools: {tools}')
        asst_impl = (
            assistant_implementation if stream_mode else None
        )  # test this - may need separate BotOsSession call for stream mode
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
            tool_belt=ToolBelt(),
            skip_vectors=skip_vectors
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
    # Fetch bot configurations for the given runner_id from BigQuery
    runner_id = os.getenv("RUNNER_ID", "jl-local-runner")

    bots_config = get_all_bots_full_details(runner_id=runner_id)
    sessions = []
    api_app_id_to_session_map = {}
    bot_id_to_udf_adapter_map = {}
    bot_id_to_slack_adapter_map = {}

    for bot_config in bots_config:
    # JL TEMP REMOVE
 #       if bot_config["bot_id"] == "Eliza-lGxIAG":
 #           continue
        print(f'Making session for bot {bot_config["bot_id"]}')
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
