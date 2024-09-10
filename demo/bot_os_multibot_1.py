# JL commented out, doesnt work on Mac
# from dotenv import load_dotenv
# load_dotenv()

import json
import os
import requests, time
from flask import Flask, request, jsonify, make_response
from core.bot_os_tools import ToolBelt
from core.bot_os import BotOsSession
from core.bot_os_corpus import URLListFileCorpus
from core.bot_os_defaults import (
    BASE_BOT_INSTRUCTIONS_ADDENDUM,
    BASE_BOT_PRE_VALIDATION_INSTRUCTIONS,
    BASE_BOT_PROACTIVE_INSTRUCTIONS,
    BASE_BOT_VALIDATION_INSTRUCTIONS,
)
from core.bot_os_input import BotOsInputMessage
from core.bot_os_llm import LLMKeyHandler
from core.bot_os_memory import BotOsKnowledgeAnnoy_Metadata
from core.bot_os_server import BotOsServer
from apscheduler.schedulers.background import BackgroundScheduler
from connectors.bigquery_connector import BigQueryConnector
from connectors.snowflake_connector import SnowflakeConnector
from connectors.sqlite_connector import SqliteConnector
from embed.embed_openbb import openbb_query
from slack.slack_bot_os_adapter import SlackBotAdapter

from bot_genesis.make_baby_bot import (
    make_baby_bot,
    update_slack_app_level_key,
    set_llm_key,
    get_llm_key,
    get_available_tools,
    get_ngrok_auth_token,
    set_ngrok_auth_token,
    get_bot_details,
    update_bot_details,
    set_remove_pointers,
    list_all_bots,
    get_all_bots_full_details,
    get_slack_config_tokens,
    rotate_slack_token,
    set_slack_config_tokens,
    test_slack_config_token,
)
from auto_ngrok.auto_ngrok import launch_ngrok_and_update_bots
from streamlit_gui.udf_proxy_bot_os_adapter import UDFBotOsInputAdapter
# from llm_reka.bot_os_reka import BotOsAssistantReka
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from apscheduler.executors.pool import ThreadPoolExecutor
import threading
from core.system_variables import SystemVariables

from demo.sessions_creator import create_sessions, make_session

from openai import OpenAI


# for Cortex testing
#os.environ['SIMPLE_MODE'] = 'true'

import logging

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.WARN, format="%(asctime)s - %(levelname)s - %(message)s"
)

import core.global_flags as global_flags

#import debugpy
#debugpy.listen(("0.0.0.0", 5678))
#import pydevd
#pydevd.settrace('0.0.0.0', port=5678, stdoutToServer=True, stderrToServer=True, suspend=False)
# import pdb_attach
# pdb_attach.listen(5679)  # Listen on port 5678.
# $ python -m pdb_attach <PID> 5678

    

print("****** GENBOT VERSION 0.161*******")

runner_id = os.getenv("RUNNER_ID", "jl-local-runner")
global_flags.runner_id = runner_id
print("Runner ID: ", runner_id)

def get_udf_endpoint_url(endpoint_name="udfendpoint"):

    alt_service_name = os.getenv("ALT_SERVICE_NAME", None)
    if alt_service_name:
        query1 = f"SHOW ENDPOINTS IN SERVICE {alt_service_name};"
    else:
        query1 = f"SHOW ENDPOINTS IN SERVICE {project_id}.{dataset_name}.GENESISAPP_SERVICE_SERVICE;"
    try:
        logger.warning(f"Running query to check endpoints: {query1}")
        results = db_adapter.run_query(query1)
        udf_endpoint_url = next(
            (
                endpoint["ingress_url"]
                for endpoint in results
                if endpoint["name"] == endpoint_name
            ),
            None,
        )
        return udf_endpoint_url
    except Exception as e:
        logger.warning(f"Failed to get {endpoint_name} endpoint URL with error: {e}")
        return None

genbot_internal_project_and_schema = os.getenv("GENESIS_INTERNAL_DB_SCHEMA", "None")
if genbot_internal_project_and_schema == "None":
    print("ENV Variable GENESIS_INTERNAL_DB_SCHEMA is not set.")
if genbot_internal_project_and_schema is not None:
    genbot_internal_project_and_schema = genbot_internal_project_and_schema.upper()
db_schema = genbot_internal_project_and_schema.split(".")
project_id = db_schema[0]
global_flags.project_id = project_id
dataset_name = db_schema[1]
global_flags.genbot_internal_project_and_schema = genbot_internal_project_and_schema

genesis_source = os.getenv("GENESIS_SOURCE", default="Snowflake")

if genesis_source == "BigQuery":
    credentials_path = os.getenv(
        "GOOGLE_APPLICATION_CREDENTIALS", default=".secrets/gcp.json"
    )
    with open(credentials_path) as f:
        connection_info = json.load(f)
    # Initialize BigQuery client
    db_adapter = BigQueryConnector(connection_info, "BigQuery")
elif genesis_source == 'Sqlite':
    db_adapter = SqliteConnector(connection_name="Sqlite")
elif genesis_source == 'Snowflake':  # Initialize BigQuery client
    print("Starting Snowflake connector...")
    db_adapter = SnowflakeConnector(connection_name="Snowflake")
    connection_info = {"Connection_Type": "Snowflake"}
else:
    raise ValueError('Invalid Source')
    

if os.getenv("TEST_MODE", "false").lower() == "true":
    print("()()()()()()()()()()()()()")
    print("TEST_MODE - ensure table exists skipped")
    print("()()()()()()()()()()()()()")
else:    
    db_adapter.ensure_table_exists()

bot_id_to_udf_adapter_map = {}
llm_api_key = None
llm_key_handler = LLMKeyHandler(db_adapter=db_adapter)

# set the system LLM type and key
print('Checking LLM_TOKENS for saved LLM Keys:')
try:
    api_key_from_env, llm_api_key, llm_type = llm_key_handler.get_llm_key_from_db()
except Exception as e:
    logger.error(f"Failed to get LLM key from database: {e}")
    llm_api_key = None

print("---> CONNECTED TO DATABASE:: ", genesis_source)
global_flags.source = genesis_source

# while True:
#    prompt = input('> ')
#    db_adapter.semantic_copilot(prompt, semantic_model='"!SEMANTIC"."GENESIS_TEST"."GENESIS_INTERNAL"."SEMANTIC_STAGE"."revenue.yaml"')




# Call the function to show endpoints
try:
    ep = get_udf_endpoint_url(endpoint_name="udfendpoint")
    data_cubes_ingress_url = get_udf_endpoint_url("streamlitdatacubes")
    data_cubes_ingress_url = data_cubes_ingress_url if data_cubes_ingress_url else "localhost:8501"
    logger.warning(f"data_cubes_ingress_url: {data_cubes_ingress_url}")
    logger.warning(f"udf endpoint: {ep}")
except Exception as e:
    logger.warning(f"Error on get_endpoints {e} ")


ngrok_active = False

# log where the remote debugger is listening
# debug_endpoint_url = get_udf_endpoint_url("debuggenesis") or "localhost"
# logger.warning(f"Remote debugger is listening on {debug_endpoint_url}:5678")

##########################
# Main stuff starts here
##########################

t, r = get_slack_config_tokens()
global_flags.slack_active = test_slack_config_token()
if global_flags.slack_active == 'token_expired':
    print('Slack Config Token Expired')
    global_flags.slack_active = False 
#global_flags.slack_active = True 

print("...Slack Connector Active Flag: ", global_flags.slack_active)
SystemVariables.bot_id_to_slack_adapter_map = {}

if llm_api_key is not None:
    (
        sessions,
        api_app_id_to_session_map,
        bot_id_to_udf_adapter_map,
        SystemVariables.bot_id_to_slack_adapter_map,
    ) = create_sessions(
        db_adapter,
        bot_id_to_udf_adapter_map,
        stream_mode=True,
        data_cubes_ingress_url=data_cubes_ingress_url,
    )
else:
    # wait to collect API key from Streamlit user, then make sessions later
    pass

app = Flask(__name__)

# add routers to a map of bot_ids if we allow multiple bots to talk this way via one UDF

# @app.route("/udf_proxy/lookup_ui", methods=["GET", "POST"])
# def lookup_fn():
#    return udf_adapter.lookup_fn()

# @app.route("/udf_proxy/submit_ui", methods=["GET", "POST"])
# def submit_fn():
#    return udf_adapter.submit_fn()


@app.get("/healthcheck")
def readiness_probe():
    return "I'm ready!"


@app.post("/echo")
def echo():
    """
    Main handler for input data sent by Snowflake.
    """
    message = request.json
    logger.debug(f"Received request: {message}")

    if message is None or not message["data"]:
        logger.info("Received empty message")
        return {}

    # input format:
    #   {"data": [
    #     [row_index, column_1_value, column_2_value, ...],
    #     ...
    #   ]}
    input_rows = message["data"]
    logger.info(f"Received {len(input_rows)} rows")

    # output format:
    #   {"data": [
    #     [row_index, column_1_value, column_2_value, ...}],
    #     ...
    #   ]}
    # output_rows = [[row[0], submit(row[1],row[2])] for row in input_rows]
    output_rows = [[row[0], "Hi there!"] for row in input_rows]
    logger.info(f"Produced {len(output_rows)} rows")

    response = make_response({"data": output_rows})
    response.headers["Content-type"] = "application/json"
    logger.debug(f"Sending response: {response.json}")
    return response


# @app.route("/healthcheck", methods=["GET", "POST"])
# def healthcheck():
#    #return udf_adapter.healthcheck()
#    pass


@app.route("/udf_proxy/submit_udf", methods=["POST"])
def submit_udf():

    message = request.json
    input_rows = message["data"]
    if type(input_rows[0][3]) == str:
        bot_id = json.loads(input_rows[0][3])["bot_id"]
    else:
        bot_id = input_rows[0][3]["bot_id"]
    row = input_rows[0]

    bots_udf_adapter = bot_id_to_udf_adapter_map.get(bot_id, None)
    if bots_udf_adapter is not None:
        return bots_udf_adapter.submit_udf_fn()
    else:
        # TODO LAUNCH
        bot_install_followup(bot_id, no_slack=True)
        bots_udf_adapter = bot_id_to_udf_adapter_map.get(bot_id, None)

        if bots_udf_adapter is not None:
            return bots_udf_adapter.submit_udf_fn()
        else:
            output_rows = [[row[0], "Bot UDF Adapter not found"]]
            response = make_response({"data": output_rows})
            response.headers["Content-type"] = "application/json"
            logger.debug(f"Sending response: {response.json}")
            return response


@app.route("/udf_proxy/lookup_udf", methods=["POST"])
def lookup_udf():

    message = request.json
    input_rows = message["data"]
    bot_id = input_rows[0][2]

    bots_udf_adapter = bot_id_to_udf_adapter_map.get(bot_id, None)
    if bots_udf_adapter is not None:
        return bots_udf_adapter.lookup_udf_fn()
    else:
        return None


@app.route("/udf_proxy/list_available_bots", methods=["POST"])
def list_available_bots_fn():

    print('-> streamlit called list available bots',flush=True)
    message = request.json
    input_rows = message["data"]
    row = input_rows[0]

    output_rows = []
    if "llm_api_key" not in globals() or llm_api_key is None:
        output_rows = [
            [row[0], {"Success": False, "Message": "Needs LLM Type and Key"}]
        ]
    else:
        runner = os.getenv("RUNNER_ID", "jl-local-runner")
        bots = list_all_bots(runner_id=runner, with_instructions=True)

        for bot in bots:
            bot_id = bot.get("bot_id")

            # Retrieve the session for the bot using the bot_id
            bot_slack_adapter = SystemVariables.bot_id_to_slack_adapter_map.get(
                bot_id, None
            )
            bot_slack_deployed = False
            if bot_slack_adapter:
                bot_slack_deployed = True
            for bot_info in bots:
                if bot_info.get("bot_id") == bot_id:
                    bot_info["slack_deployed"] = bot_slack_deployed
                    break
            else:
                pass
        output_rows = [[row[0], bots]]

    response = make_response({"data": output_rows})
    response.headers["Content-type"] = "application/json"
    logger.debug(f"Sending response: {response.json}")
    return response

import base64
from pathlib import Path
def img_to_bytes(img_path):
    img_bytes = Path(img_path).read_bytes()
    encoded = base64.b64encode(img_bytes).decode()
    return encoded

@app.route("/udf_proxy/get_metadata", methods=["POST"])
def get_metadata():
    try:
        message = request.json
        input_rows = message["data"]
        metadata_type = input_rows[0][1]

        if metadata_type == "harvest_control":
            result = db_adapter.get_harvest_control_data_as_json()
        elif metadata_type == "harvest_summary":
            result = db_adapter.get_harvest_summary()
        elif metadata_type == "available_databases":
            result = db_adapter.get_available_databases()
        elif metadata_type == "bot_images":
            result = db_adapter.get_bot_images()
        elif metadata_type == "llm_info":
            result = db_adapter.get_llm_info()
        elif metadata_type == "bot_llms":
            if "BOT_LLMS" in os.environ and os.environ["BOT_LLMS"]:
                result = {"Success": True, "Data": os.environ["BOT_LLMS"]}
            else:
                result = {"Success": False, "Message": result["Error"]}
        elif metadata_type.startswith('test_email '):
            email = metadata_type.split('test_email ')[1].strip()
            result = db_adapter.send_test_email(email) 
         
        elif 'png' in metadata_type:
            image_path = input_rows[0][1]
            result = {"Success": True, "Data": json.dumps(img_to_bytes(image_path))}
        else:
            raise ValueError(
                "Invalid metadata_type provided. Expected 'harvest_control' or 'harvest_summary' or 'available_databases'."
            )

        if result["Success"]:
            output_rows = [[input_rows[0][0], json.loads(result["Data"])]]
        else:
            output_rows = [[input_rows[0][0], {"Success": False, "Message": result["Error"]}]]

    except Exception as e:
        output_rows = [[input_rows[0][0], {"Success": False, "Message": str(e)}]]

    response = make_response({"data": output_rows})
    response.headers["Content-type"] = "application/json"
    logger.debug(f"Sending response: {response.json}")
    return response


@app.route("/udf_proxy/get_slack_tokens", methods=["POST"])
def get_slack_tokens():
    try:

        message = request.json
        input_rows = message["data"]
        # Retrieve the current slack app config token and refresh token
        slack_app_config_token, slack_app_config_refresh_token = (
            get_slack_config_tokens()
        )

        # Create display versions of the tokens
        slack_app_config_token_display = (
            f"{slack_app_config_token[:10]}...{slack_app_config_token[-10:]}"
        )
        slack_app_config_refresh_token_display = f"{slack_app_config_refresh_token[:10]}...{slack_app_config_refresh_token[-10:]}"

        # Prepare the response
        response = {
            "Success": True,
            "Message": "Slack tokens retrieved successfully.",
            "Token": slack_app_config_token_display,
            "RefreshToken": slack_app_config_refresh_token_display,
            "SlackActiveFlag": global_flags.slack_active,
        }
    except Exception as e:
        response = {
            "Success": False,
            "Message": f"An error occurred while retrieving Slack tokens: {str(e)}",
        }

    output_rows = [[input_rows[0][0], response]]

    response_var = make_response({"data": output_rows})
    response_var.headers["Content-type"] = "application/json"
    logger.debug(f"Sending response: {response_var.json}")
    return response_var


@app.route("/udf_proxy/get_ngrok_tokens", methods=["POST"])
def get_ngrok_tokens():
    try:

        message = request.json
        input_rows = message["data"]
        # Retrieve the current slack app config token and refresh token
        ngrok_auth_token, ngrok_use_domain, ngrok_domain = get_ngrok_auth_token()

        # Create display versions of the tokens
        ngrok_auth_token_display = f"{ngrok_auth_token[:10]}...{ngrok_auth_token[-10:]}"

        # Prepare the response
        response = {
            "Success": True,
            "Message": "Ngrok tokens retrieved successfully.",
            "ngrok_auth_token": ngrok_auth_token_display,
            "ngrok_use_domain": ngrok_use_domain,
            "ngrok_domain": ngrok_domain,
            "ngrok_active_flag": ngrok_active,
        }
    except Exception as e:
        response = {
            "Success": False,
            "Message": f"An error occurred while retrieving Slack tokens: {str(e)}",
        }

    output_rows = [[input_rows[0][0], response]]

    response_var = make_response({"data": output_rows})
    response_var.headers["Content-type"] = "application/json"
    logger.debug(f"Sending response: {response_var.json}")
    return response_var


def deploy_bot_to_slack(bot_id=None):
    # Retrieve the bot details
    bot_details = get_bot_details(bot_id)

    # Redeploy the bot by calling make_baby_bot
    deploy_result = make_baby_bot(
        bot_id=bot_id,
        bot_name=bot_details.get("bot_name"),
        bot_instructions=bot_details.get("bot_instructions"),
        available_tools=bot_details.get("available_tools"),
        runner_id=bot_details.get("runner_id"),
        slack_channel_id=bot_details.get("slack_channel_id"),
        confirmed=bot_details.get("confirmed"),
        files=bot_details.get("files"),
        activate_slack="Y",
        update_existing=True,
    )

    # Check if the deployment was successful
    if not deploy_result.get("success"):
        raise Exception(f"Failed to redeploy bot: {deploy_result.get('error')}")

    return deploy_result


@app.route("/udf_proxy/deploy_bot", methods=["POST"])
def deploy_bot():
    try:
        # Extract the data from the POST request's JSON body
        message = request.json
        input_rows = message["data"]
        bot_id = input_rows[0][
            1
        ]  # Assuming the bot_id is the second element in the row

        # Check if bot_id is provided
        if not bot_id:
            raise ValueError("Missing 'bot_id' in the input data.")

        # Call the deploy_bot_to_slack function with the provided bot_id
        deploy_result = deploy_bot_to_slack(bot_id=bot_id)

        new_bot_details = get_bot_details(bot_id=bot_id)

        # Prepare the response
        response = {
            "Success": deploy_result.get("success", True),
            "Message": deploy_result.get(
                "message",
                f"Bot {new_bot_details.get('bot_id')} deployed to Slack. Now authorize it by clicking {new_bot_details.get('auth_url')}.",
            ),
            "auth_url": new_bot_details.get("auth_url"),
        }
    except Exception as e:
        # Handle exceptions and prepare an error response
        response = {
            "Success": False,
            "Message": f"An error occurred during bot deployment: {str(e)}",
        }

    # Format the response into the expected output format
    output_rows = [
        [input_rows[0][0], response]
    ]  # Include the runner_id in the response

    # Create a Flask response object
    response_var = make_response({"data": output_rows})
    response_var.headers["Content-type"] = "application/json"
    logger.debug(f"Sending response: {response_var.json}")
    return response_var


@app.route("/udf_proxy/set_bot_app_level_key", methods=["POST"])
def set_bot_app_level_key():

    message = request.json
    input_rows = message["data"]
    bot_id = input_rows[0][1]
    slack_app_level_key = input_rows[0][2]

    try:
        # Set the new Slack app configuration tokens
        response = update_slack_app_level_key(
            bot_id=bot_id, slack_app_level_key=slack_app_level_key
        )

    except Exception as e:
        response = {
            "success": False,
            "error": f"An error occurred while updating a bots slack app level key: {str(e)}",
        }

    output_rows = [[input_rows[0][0], response]]

    response_var = make_response({"data": output_rows})
    response_var.headers["Content-type"] = "application/json"
    logger.debug(f"Sending response: {response_var.json}")
    return response_var


@app.route("/udf_proxy/configure_slack_app_token", methods=["POST"])
def configure_slack_app_token():

    message = request.json
    input_rows = message["data"]
    runner_id = input_rows[0][0]
    slack_app_config_token = input_rows[0][1]
    slack_app_config_refresh_token = input_rows[0][2]

    try:
        # Set the new Slack app configuration tokens
        new_token, new_refresh_token = set_slack_config_tokens(
            slack_app_config_token, slack_app_config_refresh_token
        )

        if new_token != "Error":
            new_token_display = f"{new_token[:10]}...{new_token[-10:]}"
            new_refresh_token_display = (
                f"{new_refresh_token[:10]}...{new_refresh_token[-10:]}"
            )
            response = {
                "Success": True,
                "Message": "Slack app configuration tokens updated successfully.",
                "Token": new_token_display,
                "Refresh": new_refresh_token_display,
            }
            global_flags.slack_active = True
        else:
            response = {
                "Success": False,
                "Message": f"Could not update Slack App Config Tokens. Error: {new_refresh_token}",
            }

    except Exception as e:
        response = {
            "Success": False,
            "Message": f"An error occurred while updating Slack app configuration tokens: {str(e)}",
        }

    output_rows = [[input_rows[0][0], response]]

    response_var = make_response({"data": output_rows})
    response_var.headers["Content-type"] = "application/json"
    logger.debug(f"Sending response: {response_var.json}")
    return response_var


@app.route("/udf_proxy/configure_ngrok_token", methods=["POST"])
def configure_ngrok_token():
    global ngrok_active

    message = request.json
    input_rows = message["data"]
    ngrok_auth_token = input_rows[0][1]
    ngrok_use_domain = input_rows[0][2]
    ngrok_domain = input_rows[0][3]

    ngrok_token_from_env = os.getenv("NGROK_AUTH_TOKEN")
    if ngrok_token_from_env:
        response = {
            "Success": False,
            "Message": "Ngrok token is set in an environment variable and cannot be set or changed using this method.",
        }
        output_rows = [[input_rows[0][0], response]]
    else:

        try:
            # Set the new ngrok configuration tokens
            res = set_ngrok_auth_token(ngrok_auth_token, ngrok_use_domain, ngrok_domain)
            if res:
                # if not ngrok_active:
                ngrok_active = launch_ngrok_and_update_bots(
                    update_endpoints=global_flags.slack_active
                )
                response = {
                    "Success": True,
                    "Message": "Ngrok configuration tokens updated successfully.",
                    "ngrok_active": ngrok_active,
                }
            else:
                response = {"Success": False, "Message": "Ngrok token invalid."}

        except Exception as e:
            response = {
                "Success": False,
                "Message": f"An error occurred while updating ngrok configuration tokens: {str(e)}",
            }

    output_rows = [[input_rows[0][0], response]]
    response_var = make_response({"data": output_rows})
    response_var.headers["Content-type"] = "application/json"
    logger.debug(f"Sending response: {response_var.json}")
    return response_var


@app.route("/udf_proxy/configure_llm", methods=["POST"])
def configure_llm():

    from openai import OpenAI, OpenAIError

    global llm_api_key, default_llm_engine, sessions, api_app_id_to_session_map, bot_id_to_udf_adapter_map, server
    try:

        message = request.json
        input_rows = message["data"]

        llm_type = input_rows[0][1]
        llm_key = input_rows[0][2]

        if not llm_key or not llm_type:
            response = {
                "Success": False,
                "Message": "Missing LLM API Key or LLM Model Name.",
            }
            llm_key = None
            llm_type = None

        #  if api_key_from_env:
        #      response = {"Success": False, "Message": "LLM type and API key are set in an environment variable and can not be set or changed using this method."}
        #      llm_key = None
        #      llm_type = None
        else:
        # if llm_type is not None:
            
            data_cubes_ingress_url = get_udf_endpoint_url("streamlitdatacubes")
            data_cubes_ingress_url = data_cubes_ingress_url if data_cubes_ingress_url else "localhost:8501"
            logger.warning(f"data_cubes_ingress_url(2) set to {data_cubes_ingress_url}")

            # os.environ["OPENAI_API_KEY"] = ''
            # os.environ["REKA_API_KEY"] = ''
            # os.environ["GEMINI_API_KEY"] = ''
            os.environ["CORTEX_MODE"] = 'False'

            if (llm_type.lower() == "openai"):
                os.environ["OPENAI_API_KEY"] = llm_key
                try:
                    client = OpenAI(api_key=llm_key)

                    completion = client.chat.completions.create(
                        model="gpt-4o",
                        messages=[{"role": "user", "content": "What is 1+1?"}],
                    )
                    # Success!  Update model and keys

                except OpenAIError as e:
                    response = {"Success": False, "Message": str(e)}
                    llm_key = None
            elif (llm_type.lower() == "reka"):
                os.environ["REKA_API_KEY"] = llm_key
            elif (llm_type.lower() == "gemini"):
                os.environ["GEMINI_API_KEY"] = llm_key
            elif (llm_type.lower() == "cortex"):
                os.environ["CORTEX_MODE"] = 'True'

            # set the system default LLM engine
            os.environ["BOT_OS_DEFAULT_LLM_ENGINE"] = llm_type.lower()
            default_llm_engine = llm_type
            llm_api_key = llm_key
            if llm_api_key is not None:
                try:
                    sessions, api_app_id_to_session_map, bot_id_to_udf_adapter_map, SystemVariables.bot_id_to_slack_adapter_map = create_sessions(
                        db_adapter,
                        bot_id_to_udf_adapter_map,
                        stream_mode=True,
                        data_cubes_ingress_url=data_cubes_ingress_url,
                    )
                except Exception as e:
                    logger.error(f"Failed to create sessions: {e}")
                    sessions = []
                    api_app_id_to_session_map = {}
                    bot_id_to_udf_adapter_map = {}
                    SystemVariables.bot_id_to_slack_adapter_map = {}
                    return None
            # (
            #     sessions,
            #     api_app_id_to_session_map,
            #     bot_id_to_udf_adapter_map,
            #     SystemVariables.bot_id_to_slack_adapter_map,
            # ) = create_sessions(
            #     db_adapter,
            #     bot_id_to_udf_adapter_map,
            #     stream_mode=True,
            #     data_cubes_ingress_url=data_cubes_ingress_url,
            # )
            server = BotOsServer(
                app,
                sessions=sessions,
                scheduler=scheduler,
                scheduler_seconds_interval=2,
                slack_active=global_flags.slack_active,
                db_adapter=db_adapter,
                bot_id_to_udf_adapter_map = bot_id_to_udf_adapter_map,
                api_app_id_to_session_map = api_app_id_to_session_map,
                data_cubes_ingress_url = data_cubes_ingress_url,
                bot_id_to_slack_adapter_map = SystemVariables.bot_id_to_slack_adapter_map,
            )
            BotOsServer.stream_mode = True
            set_remove_pointers(server, api_app_id_to_session_map)

            # Assuming 'babybot' is an instance of a class that has the 'set_llm_key' method
            # and it has been instantiated and imported above in the code.
            set_key_result = set_llm_key(
                llm_key=llm_key,
                llm_type=llm_type,
            )
            if set_key_result:
                response = {
                    "Success": True,
                    "Message": "LLM API Key and Model Name configured successfully.",
                }
            else:
                response = {
                    "Success": False,
                    "Message": "Failed to set LLM API Key and Model Name.",
                }
    except Exception as e:
        response = {"Success": False, "Message": str(e)}

    output_rows = [[input_rows[0][0], response]]

    response_var = make_response({"data": output_rows})
    response_var.headers["Content-type"] = "application/json"
    logger.debug(f"Sending response: {response_var.json}")
    return response_var


# scheduler = BackgroundScheduler(executors={'default': ThreadPoolExecutor(max_workers=100)})
scheduler = BackgroundScheduler(
    {
        "apscheduler.job_defaults.max_instances": 100,
        "apscheduler.job_defaults.coalesce": True,
    }
)
# Retrieve the number of currently running jobs in the scheduler
# Code to clear any threads that are stuck or crashed from BackgroundScheduler

server = None
if llm_api_key is not None:
    BotOsServer.stream_mode = True
    server = BotOsServer(
        app, sessions=sessions, scheduler=scheduler, scheduler_seconds_interval=1,    
        slack_active=global_flags.slack_active,
         db_adapter=db_adapter,
                bot_id_to_udf_adapter_map = bot_id_to_udf_adapter_map,
                api_app_id_to_session_map = api_app_id_to_session_map,
                data_cubes_ingress_url = data_cubes_ingress_url,
                bot_id_to_slack_adapter_map = SystemVariables.bot_id_to_slack_adapter_map,
    )
    set_remove_pointers(server, api_app_id_to_session_map)


@app.route("/zapier", methods=["POST"])
def zaiper_handler():
    try:
        api_key = request.args.get("api_key")
    except:
        return "Missing API Key"

    #  print("Zapier: ", api_key)
    return {"Success": True, "Message": "Success"}


@app.route("/slack/events/<bot_id>/install", methods=["GET"])
def bot_install_followup(bot_id=None, no_slack=False):
    # Extract the API App ID from the incoming request

    logger.info("HERE 1")
    bot_details = get_bot_details(bot_id=bot_id)

    if not no_slack:
        try:
            code = request.args.get("code")
            state = request.args.get("state")
        except:
            return "Unknown bot install error"

    # print(bot_id, 'code: ', code, 'state', state)

    # lookup via the bot map via bot_id

    # Save these mapped to the bot
    if not no_slack:

        client_id = bot_details["client_id"]
        client_secret = bot_details["client_secret"]
        expected_state = bot_details["auth_state"]

        if (
            bot_details["bot_slack_user_id"] != "Pending_OAuth"
            and bot_details["bot_slack_user_id"] != "Pending_APP_LEVEL_TOKEN"
        ):
            return "Bot is already installed to Slack."

        # validate app_id
        if state != expected_state:
            print("State error.. possible forgery")
            return "Error: Not Installed"

        # Define the URL for the OAuth request
        oauth_url = "https://slack.com/api/oauth.v2.access"

        # Make the POST request to exchange the code for an access token
        response = requests.post(
            oauth_url,
            data={"code": code, "client_id": client_id, "client_secret": client_secret},
        )
        # Check if the request was successful
        if response.status_code == 200:
            # Handle successful token exchange
            token_data = response.json()
            bot_user_id = token_data["bot_user_id"]
            access_token = token_data["access_token"]

            # Do something with token_data, like storing the access token
            if "error" in token_data:
                return "Error: Not Installed"

            update_bot_details(
                bot_id=bot_id,
                bot_slack_user_id=bot_user_id,
                slack_app_token=access_token,
            )

    runner = os.getenv("RUNNER_ID", "jl-local-runner")
    data_cubes_ingress_url = get_udf_endpoint_url("streamlitdatacubes")
    data_cubes_ingress_url = data_cubes_ingress_url if data_cubes_ingress_url else "localhost:8501"
    print(f"data_cubes_ingress_url(3) set to {data_cubes_ingress_url}")

    if runner == bot_details["runner_id"]:
        bot_config = get_bot_details(bot_id=bot_id)
        if no_slack:
            bot_config["slack_active"] = "N"
        new_session, api_app_id, udf_local_adapter, slack_adapter_local = make_session(
            bot_config=bot_config,
            db_adapter=db_adapter,
            bot_id_to_udf_adapter_map=bot_id_to_udf_adapter_map,
            stream_mode=True,
            data_cubes_ingress_url=data_cubes_ingress_url,
        )
        # check new_session
        if new_session is None:
            print("new_session is none")
            return "Error: Not Installed new session is none"
        if slack_adapter_local is not None:
            SystemVariables.bot_id_to_slack_adapter_map[bot_config["bot_id"]] = (
                slack_adapter_local
            )
        if udf_local_adapter is not None:
            bot_id_to_udf_adapter_map[bot_config["bot_id"]] = udf_local_adapter
        api_app_id_to_session_map[api_app_id] = new_session
        #    print("about to add session ",new_session)
        server.add_session(new_session, replace_existing=True)

        if no_slack:
            print(
                f"Genesis bot {bot_id} successfully installed and ready for use via Streamlit."
            )
        else:
            return f"Genesis bot {bot_id} successfully installed to Streamlit and Slack and ready for use."
    else:
        # Handle errors
        print("Failed to exchange code for access token:", response.text)
        return "Error: Not Installed"


@app.route("/slack/events", methods=["POST"])
@app.route("/slack/events/<bot_id>", methods=["POST"])
# @app.route('/',              methods=['POST'])
def slack_event_handle(bot_id=None):
    # Extract the API App ID from the incoming request
    request_data = request.json

    api_app_id = request_data.get(
        "api_app_id"
    )  # Adjust based on your Slack event structure

    if request_data is not None and request_data["type"] == "url_verification":
        # Respond with the challenge value
        return jsonify({"challenge": request_data["challenge"]})

    # Find the session using the API App ID
    session = api_app_id_to_session_map.get(api_app_id)

    if session:
        # If a matching session is found, handle the event

        try:
            slack_events = session.input_adapters[0].slack_events()
        except:
            return (
                jsonify(
                    {
                        "error": "Slack adapter not active for this bot session, set to N in bot_servicing table."
                    }
                ),
                404,
            )
        return slack_events
    else:
        # If no matching session, return an error
        return jsonify({"error": "No matching session found"}), 404


# first of many embedding integrations that use udfs under the covers
@app.route("/udf_proxy/openbb/v1/query", methods=["POST"])
def embed_openbb():
    return openbb_query(
        bot_id_to_udf_adapter_map,
        default_bot_id=list(bot_id_to_udf_adapter_map.keys())[0],
    )


BotOsServer.stream_mode = True
scheduler.start()

ngrok_active = launch_ngrok_and_update_bots(update_endpoints=global_flags.slack_active)

SERVICE_HOST = os.getenv("SERVER_HOST", "0.0.0.0")

logging.getLogger("werkzeug").setLevel(logging.WARN)

# Initialize Slack Bolt app
# tok = 'xapp-1-A06VCAXMAKA-6988391388305-458da01d3a1d9ea609d7727424db689eb402bc9e43d8aa8174a11e0ed02719e6'
# @slack_app = App(token=tok)
# bot_tok = 'xoxb-6550650260448-6961055350487-vf1D28VBQzemQHJr3fgNIaGL'


# Define Slack event handlers
# @slack_app.event("message")
# def handle_message_events(event, say):
#   say("Hello, I'm here to assist you!")
#    pass

# @slack_app.event("app_mention")
# def mention_handler(event, say):
#    print(event)
#  say('hi')


def run_flask_app():
    app.run(host=SERVICE_HOST, port=8080, debug=False, use_reloader=False)


# def run_slack_app():
#    handler = SocketModeHandler(slack_app, tok)
#    handler.start()
#    print('hi')


# Run Slack app in a separate thread
# slack_thread = threading.Thread(target=run_slack_app)
# slack_thread.start()

if __name__ == "__main__":
    # while True:
    #    time.sleep(60)
    # Run Flask app in the main thread
    run_flask_app()
