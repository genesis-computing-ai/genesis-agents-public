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






bot_id_to_udf_adapter_map = {}





# Fetch endpoint URLs

ngrok_active = False

##########################
# Main stuff starts here
##########################


#global_flags.slack_active = True






ngrok_active = launch_ngrok_and_update_bots(update_endpoints=global_flags.slack_active)

