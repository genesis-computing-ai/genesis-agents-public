# JL commented out, doesnt work on Mac
# from dotenv import load_dotenv
# load_dotenv()

import json
import os
import time

from flask import Flask
from core.bot_os_server import BotOsServer
from apscheduler.schedulers.background import BackgroundScheduler
from connectors.bigquery_connector import BigQueryConnector
from connectors.snowflake_connector import SnowflakeConnector
from bot_genesis.make_baby_bot import (
    get_llm_key,
    set_remove_pointers,
    get_slack_config_tokens,
    rotate_slack_token,
    test_slack_config_token,
)
from auto_ngrok.auto_ngrok import launch_ngrok_and_update_bots
from core.system_variables import SystemVariables

from demo.sessions_creator import create_sessions
from streamlit_gui.flask_routes import register_routes


import logging

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.WARN, format="%(asctime)s - %(levelname)s - %(message)s"
)

import core.global_flags as global_flags


print("****** GENBOT VERSION 0.141 *******")

runner_id = os.getenv("RUNNER_ID", "jl-local-runner")
global_flags.runner_id = runner_id
print("Runner ID: ", runner_id)

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
else:  # Initialize BigQuery client
    print("Starting Snowflake connector...")
    db_adapter = SnowflakeConnector(connection_name="Snowflake")
    connection_info = {"Connection_Type": "Snowflake"}
db_adapter.ensure_table_exists()
print("---> CONNECTED TO DATABASE:: ", genesis_source)
global_flags.source = genesis_source


def get_udf_endpoint_url():
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
                if endpoint["name"] == "udfendpoint"
            ),
            None,
        )
        return udf_endpoint_url
    except Exception as e:
        logger.warning(f"Failed to get UDF endpoint URL with error: {e}")
        return None


# Call the function to show endpoints
try:
    ep = get_udf_endpoint_url()
    logger.warning(f"udf endpoint: {ep}")
except Exception as e:
    logger.warning(f"Error on get_endpoints {e} ")


ngrok_active = False

##########################
# Main stuff starts here
##########################

bot_id_to_udf_adapter_map = {}
api_key_from_env = False
default_llm_engine = os.getenv("BOT_OS_DEFAULT_LLM_ENGINE", "openai")
llm_api_key = None
if default_llm_engine.lower() == "openai":
    llm_api_key = os.getenv("OPENAI_API_KEY", None)
    if llm_api_key == "":
        llm_api_key = None
    if llm_api_key:
        api_key_from_env = True
elif default_llm_engine.lower() == "reka":
    llm_api_key = os.getenv("REKA_API_KEY", None)
    if llm_api_key:
        api_key_from_env = True

if genesis_source == "BigQuery" and api_key_from_env == False:
    while True:
        print(
            "!!!!! Loading LLM config from File No longer Supported -- Please provide via ENV VAR when using BigQuery Source"
        )
        time.sleep(3)

if llm_api_key is None and genesis_source == "Snowflake":
    llm_key, llm_type = get_llm_key()
    if llm_key and llm_type:
        default_llm_engine = llm_type
        llm_api_key = llm_key
        api_key_from_env = False
    #  print("LLM Key loaded from Database")
    else:
        print("===========")
        print("NOTE: Config via Streamlit to continue")
        print("===========")
#        logger.warn('LLM config not found in Env Var nor in Database LLM_CONFIG table.. starting without LLM Key, please provide via Streamlit')

if llm_api_key is not None and default_llm_engine.lower() == "openai":
    os.environ["OPENAI_API_KEY"] = llm_api_key
if llm_api_key is not None and default_llm_engine.lower() == "reka":
    os.environ["REKA_API_KEY"] = llm_api_key


global_flags.slack_active = test_slack_config_token()
if global_flags.slack_active == "token_expired":
    t, r = get_slack_config_tokens()
    tp, rp = rotate_slack_token(config_token=t, refresh_token=r)
    global_flags.slack_active = test_slack_config_token()
else:
    t, r = get_slack_config_tokens()
print("...Slack Connector Active Flag: ", global_flags.slack_active)


SystemVariables.bot_id_to_slack_adapter_map = {}

if llm_api_key is not None:
    (
        sessions,
        api_app_id_to_session_map,
        bot_id_to_udf_adapter_map,
        SystemVariables.bot_id_to_slack_adapter_map,
    ) = create_sessions(
        default_llm_engine,
        llm_api_key,
        db_adapter,
        bot_id_to_udf_adapter_map,
        stream_mode=True,
    )
else:
    # wait to collect API key from Streamlit user, then make sessions later
    pass

scheduler = BackgroundScheduler(
    {
        "apscheduler.job_defaults.max_instances": 100,
        "apscheduler.job_defaults.coalesce": True,
    }
)

app = Flask(__name__)
register_routes(app, db_adapter, scheduler)

# Retrieve the number of currently running jobs in the scheduler
# Code to clear any threads that are stuck or crashed from BackgroundScheduler
server = None
if llm_api_key is not None:
    BotOsServer.stream_mode = True
    server = BotOsServer(
        app, sessions=sessions, scheduler=scheduler, scheduler_seconds_interval=1
    )
    set_remove_pointers(server, api_app_id_to_session_map)


BotOsServer.stream_mode = True
scheduler.start()

ngrok_active = launch_ngrok_and_update_bots(update_endpoints=global_flags.slack_active)

SERVICE_HOST = os.getenv("SERVER_HOST", "0.0.0.0")

logging.getLogger("werkzeug").setLevel(logging.WARN)


def run_flask_app():
    app.run(host=SERVICE_HOST, port=8080, debug=False, use_reloader=False)


if __name__ == "__main__":
    # Run Flask app in the main thread
    run_flask_app()
