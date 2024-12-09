import os
from core.logging_config import logger
from connectors import get_global_db_connector
import core.global_flags as global_flags
from core.bot_os_llm import LLMKeyHandler
from bot_genesis.make_baby_bot import  get_slack_config_tokens, test_slack_config_token, set_remove_pointers
from core.system_variables import SystemVariables
from demo.sessions_creator import create_sessions
from apscheduler.schedulers.background import BackgroundScheduler
from core.bot_os_server import BotOsServer
from auto_ngrok.auto_ngrok import launch_ngrok_and_update_bots


logger.info("****** GENBOT VERSION 0.202 *******")

runner_id = os.getenv("RUNNER_ID", "jl-local-runner")
multbot_mode = True

global_flags.runner_id = runner_id
global_flags.multibot_mode = True

# Check if the index_size_file exists and delete it if it does
index_file_path = './tmp/'
index_size_file = os.path.join(index_file_path, 'index_size.txt')
if os.path.exists(index_size_file):
    try:
        os.remove(index_size_file)
        logger.info(f"Deleted {index_size_file} (this is expected on local test runs)")
    except Exception as e:
        logger.info(f"Error deleting {index_size_file}: {e}")




genbot_internal_project_and_schema = os.getenv("GENESIS_INTERNAL_DB_SCHEMA", "None")
if genbot_internal_project_and_schema == "None":
    logger.info("ENV Variable GENESIS_INTERNAL_DB_SCHEMA is not set.")
if genbot_internal_project_and_schema is not None:
    genbot_internal_project_and_schema = genbot_internal_project_and_schema.upper()
db_schema = genbot_internal_project_and_schema.split(".")
project_id = db_schema[0]
global_flags.project_id = project_id
dataset_name = db_schema[1]
global_flags.genbot_internal_project_and_schema = genbot_internal_project_and_schema

genesis_source = os.getenv("GENESIS_SOURCE", default="Snowflake")
db_adapter = get_global_db_connector(genesis_source)

if os.getenv("TEST_MODE", "false").lower() == "true":
    logger.info("()()()()()()()()()()()()()")
    logger.info("TEST_MODE - ensure table exists skipped")
    logger.info("()()()()()()()()()()()()()")
else:
    logger.info("NOT RUNNING TEST MODE - APPLYING ONE TIME DB FIXES AND CREATING TABLES")
    db_adapter.one_time_db_fixes()
    db_adapter.ensure_table_exists()
    db_adapter.create_google_sheets_creds()

bot_id_to_udf_adapter_map = {}
llm_api_key_struct = None
llm_key_handler = LLMKeyHandler(db_adapter=db_adapter)

# set the system LLM type and key
logger.info('Checking LLM_TOKENS for saved LLM Keys:')
try:
    api_key_from_env, llm_api_key_struct = llm_key_handler.get_llm_key_from_db()
except Exception as e:
    logger.error(f"Failed to get LLM key from database: {e}")
    llm_api_key_struct = None

logger.info(f"---> CONNECTED TO DATABASE:: {genesis_source}")
global_flags.source = genesis_source


# Fetch endpoint URLs
ep = data_cubes_ingress_url = None
if not db_adapter.is_using_local_runner:
    try:
        ep = db_adapter.db_get_endpoint_ingress_url(endpoint_name="udfendpoint")
        data_cubes_ingress_url = db_adapter.db_get_endpoint_ingress_url("streamlitdatacubes")
    except Exception as e:
        logger.warning(f"Error on get_endpoints {e} ")
data_cubes_ingress_url = data_cubes_ingress_url if data_cubes_ingress_url else "localhost:8501"
logger.info(f"Endpoints: {data_cubes_ingress_url=}; udf endpoint={ep}")

ngrok_active = False

##########################
# Main stuff starts here
##########################

t, r = get_slack_config_tokens()
global_flags.slack_active = test_slack_config_token()
if global_flags.slack_active == 'token_expired':
    logger.info('Slack Config Token Expired')
    global_flags.slack_active = False
#global_flags.slack_active = True

logger.info(f"...Slack Connector Active Flag: {global_flags.slack_active}")
SystemVariables.bot_id_to_slack_adapter_map = {}

if llm_api_key_struct is not None and llm_api_key_struct.llm_key is not None:
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
if llm_api_key_struct is not None and llm_api_key_struct.llm_key is not None:
    BotOsServer.stream_mode = True
    server = BotOsServer(
        flask_app=None, sessions=sessions, scheduler=scheduler, scheduler_seconds_interval=1,
        slack_active=global_flags.slack_active,
         db_adapter=db_adapter,
                bot_id_to_udf_adapter_map = bot_id_to_udf_adapter_map,
                api_app_id_to_session_map = api_app_id_to_session_map,
                data_cubes_ingress_url = data_cubes_ingress_url,
                bot_id_to_slack_adapter_map = SystemVariables.bot_id_to_slack_adapter_map,
    )
    set_remove_pointers(server, api_app_id_to_session_map)

ngrok_active = launch_ngrok_and_update_bots(update_endpoints=global_flags.slack_active)

