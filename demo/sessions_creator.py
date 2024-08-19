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
    if False and simple_mode and stream_mode:  # stream mode is for multibot, not task mode
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

    instructions = bot_config["bot_instructions"] + "\n" + BASE_BOT_INSTRUCTIONS_ADDENDUM
    instructions += f'\nYour default database connecton is called "{genesis_source}".\n'

    instructions += f'\nNote current settings:\nYour bot_id: {bot_config["bot_id"]}.\nRunner_id: {runner_id}'
    if bot_config["slack_active"] == "Y":
        instructions += "\nYour slack user_id: " + bot_config["bot_slack_user_id"]

    if "snowflake_stage_tools" in bot_tools and "make_baby_bot" in bot_tools:
        instructions += f"\nYour Internal Files Stage for bots is at snowflake stage: {genbot_internal_project_and_schema}.BOT_FILES_STAGE"
        if not stream_mode:
            instructions += ". This BOT_FILES_STAGE stage is ONLY in this particular database & schema."

    bot_id = bot_config["bot_id"]
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

            workspace_schema_name = f"{project_id}.{bot_id}_WORKSPACE".replace(
                "-", "_"
            ).upper()
            db_adapter.create_bot_workspace(workspace_schema_name)
            db_adapter.grant_all_bot_workspace(workspace_schema_name)
            instructions += f"\nYou have a workspace schema created specifically for you named {workspace_schema_name} that the user can also access. You may use this schema for creating tables, views, and stages that are required when generating answers to data analysis questions. Only use this schema if asked to create an object. Always return the full location of the object."
            if data_cubes_ingress_url:
                logger.warning(
                    f"Setting data_cubes_ingress_url for {bot_id}: {data_cubes_ingress_url}"
                )
                instructions += f"\nWhenever you show the results from run_query that may have more than 10 rows, and if you are not in the middle of running a process, also provide a link to a datacube visualization to help them understand the data you used in the form: http://{data_cubes_ingress_url}?sql_query=select%20*%20from%20spider_data.baseball.all_star -- replace the value of the sql_query query parameter with the query you used."
        except Exception as e:
            logger.warning(f"Error creating bot workspace for bot_id {bot_id} {e} ")


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
    print(f"Bot implementation from bot config: {bot_config.get('bot_implementation', 'Not specified')}")
    if "bot_implementation" in bot_config:
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
                                    break
                        if os.environ["OPENAI_API_KEY"] in (None,""):
                            print("openai llm key not set. bot session cannot be created.")
                else:
                    assistant_implementation = BotOsAssistantSnowflakeCortex
            else:
                assistant_implementation = BotOsAssistantSnowflakeCortex
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
                            break
                if os.getenv("OPENAI_API_KEY") in (None, ""):
                    print("openai llm key not set. attempting cortex.")
                    if os.environ.get("CORTEX_AVAILABLE", 'False') in ['False', '']:
                        cortex_available = db_adapter.check_cortex_available()
                        if not cortex_available:
                            print('Snowflake Cortex is not available. No openai key set. Bot session cannot be created.')
                        else:
                            assistant_implementation = BotOsAssistantSnowflakeCortex
                    else:
                        assistant_implementation = BotOsAssistantSnowflakeCortex
                else:
                    assistant_implementation = BotOsAssistantOpenAI
            else:
                assistant_implementation = BotOsAssistantOpenAI
        else:
            llm_type = os.getenv("BOT_OS_DEFAULT_LLM_ENGINE","cortex")
            if llm_type.lower() == "cortex":
                if os.environ.get("CORTEX_AVAILABLE", 'False') in ['False', '']:
                    cortex_available = db_adapter.check_cortex_available()
                    if not cortex_available:
                        print('Bot implementation not specified, OpenAI is not available, Snowflake Cortex is not available. Please set LLM key in Streamlit.')
                    else:
                        assistant_implementation = BotOsAssistantSnowflakeCortex
                else:
                    assistant_implementation = BotOsAssistantSnowflakeCortex
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
                                break
                    if os.getenv("OPENAI_API_KEY") in (None, ""):
                        print("openai llm key not set. cortex not available. what llm is being used?")
                    else:
                        assistant_implementation = BotOsAssistantOpenAI
                else:
                    assistant_implementation = BotOsAssistantOpenAI
            else:
                # could be gemini or something else eventually
                print('Bot implementation not specified, OpenAI is not available, Snowflake Cortex is not available. Please set LLM key in Streamlit.')

        # Updating an existing bot's preferred_llm
        if not bot_config["bot_implementation"]:
            bot_llms[bot_id] = {"current_llm": os.getenv("BOT_OS_DEFAULT_LLM_ENGINE","cortex"), "preferred_llm": None}
        elif llm_type:
            if os.getenv("BOT_OS_DEFAULT_LLM_ENGINE","cortex").lower() != bot_config["bot_implementation"].lower() and llm_type.lower() != bot_config["bot_implementation"].lower():
                bot_llms[bot_id] = {"current_llm": os.getenv("BOT_OS_DEFAULT_LLM_ENGINE","cortex"), "preferred_llm": bot_config["bot_implementation"]}
            else:
                bot_llms[bot_id] = {"current_llm": bot_config["bot_implementation"], "preferred_llm": bot_config["bot_implementation"]}
        else:
            bot_llms[bot_id] = {"current_llm": bot_config["bot_implementation"], "preferred_llm": llm_type}

        bot_llms_json = json.dumps(bot_llms)

        # Save the JSON string as an environment variable
        os.environ["BOT_LLMS"] = bot_llms_json

        #if assistant_implementation == BotOsAssistantSnowflakeCortex and stream_mode:
        if assistant_implementation == BotOsAssistantSnowflakeCortex and True:
            incoming_instructions = instructions
            # Tools: brave_search, wolfram_alpha, code_interpreter

            instructions = """


Environment: ipython

Cutting Knowledge Date: December 2023
Today Date: 23 Jul 2024

# Tool Instructions 
""" 
#""" - Always execute python code in messages that you share.
# - When looking for real time information use relevant functions if available else fallback to brave_search
# 
            instructions += """You have access to the following functions, only call them when needed to perform actions or lookup information that you do not already have:

""" + json.dumps(tools) + """


If a you choose to call a function ONLY reply in the following format:
<{start_tag}={function_name}>{parameters}{end_tag}
where

start_tag => `<function>`
parameters => a JSON dict with the function argument name as key and function argument value as value.
end_tag => `</function>`

Here is an example,
<function=example_function_name>{"example_name": "example_value"}</function>

Reminder:
- Function calls MUST follow the specified format
- Required parameters MUST be specified
- Only call one function at a time
- Put the entire function call reply on one line
- Do not add any preable of other text before or directly after the function call
- Always add your sources when using search results to answer the user query
- Don't generate function call syntax (e.g. as an example) unless you want to actually call it immediately 
- Don't forget to call the tools, don't just say you can do it, actually do it when needed
- If you're suggestion a next step to the user, just suggest it, but don't immediately perform it, wait for them to agree

# Persona Instructions
 """+incoming_instructions + "\n\n"
                    
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
            tool_belt=ToolBelt(db_adapter, os.getenv("OPENAI_API_KEY",None)), 
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
    # Fetch bot configurations for the given runner_id from BigQuery
    runner_id = os.getenv("RUNNER_ID", "jl-local-runner")

    bots_config = get_all_bots_full_details(runner_id=runner_id)
    sessions = []
    api_app_id_to_session_map = {}
    bot_id_to_udf_adapter_map = {}
    bot_id_to_slack_adapter_map = {}

    for bot_config in bots_config:
        if os.getenv("TEST_MODE", "false").lower() == "true":
            if bot_config.get("bot_name") != os.getenv("TEAMS_BOT", ""):
                print("()()()()()()()()()()()()()()()")                
                print("Test Mode skipping all bots except ",os.getenv("TEAMS_BOT", ""))
                print("()()()()()()()()()()()()()()()") 
                continue
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
