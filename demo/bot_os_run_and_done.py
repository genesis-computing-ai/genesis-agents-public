import os, time, uuid
from core.bot_os_llm import LLMKeyHandler
from core.bot_os_server import BotOsServer
from apscheduler.schedulers.background import BackgroundScheduler
from connectors import get_global_db_connector
from core.system_variables import SystemVariables
from demo.sessions_creator import create_sessions, make_session
from core.logging_config import logger
import core.global_flags as global_flags

# startup stuff, we should find a better place for it

logger.info("****** GENBOT RUNNER *******")

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
global_flags.slack_active = False
SystemVariables.bot_id_to_slack_adapter_map = {}
ngrok_active = False

def prompt_bot_and_await_response(
    prompt: str,
    target_bot: str,
    timeout_seconds: int = 300):
    
    try:
        # Get target session
        target_session = None
        for session in sessions:
            if (target_bot is not None and session.bot_id.upper() == target_bot.upper()) or (target_bot is not None and session.bot_name.upper() == target_bot.upper()):
                target_session = session
                break

        if not target_session:
            # Get list of valid bot IDs and names
            valid_bots = [
                {
                    "id": session.bot_id,
                    "name": session.bot_name
                }
                for session in sessions
            ]

            return {
                "success": False,
                "error": f"Could not find target bot with ID: {target_bot}. Valid bots are: {valid_bots}"
            }

        # Create new thread
        # Find the UDFBotOsInputAdapter
        udf_adapter = None
        for adapter in target_session.input_adapters:
            if adapter.__class__.__name__ == "UDFBotOsInputAdapter":
                udf_adapter = adapter
                break

        if udf_adapter is None:
            raise ValueError("No UDFBotOsInputAdapter found in target session")

    
        validation_prompt = f"""
        You are being run in an unattended mode.  Here is what you should answer or do:

        Task(s):
        {prompt}
        """
        # Create thread ID for this task

        thread_id = 'standalone_' + str(uuid.uuid4())
        # Generate and store UUID for thread tracking
        uu = udf_adapter.submit(
            input=validation_prompt,
            thread_id=thread_id,
            bot_id={},
            file={}
        )

        # Wait for response with timeout
        start_time = time.time()
    
        while (time.time() - start_time) < timeout_seconds:
            # Check if response available
            response = udf_adapter.lookup_udf(uu)
            # Check if response ends with chat emoji
            if response and response.strip().endswith("💬"):
                time.sleep(.1)
                continue
            if response:
                return {
                    "success": True,
                    "result": response,
                }
   #         for s in sessions: 
   #             s.execute()
            time.sleep(.1)

        # If we've timed out, send stop command
        if (time.time() - start_time) >= timeout_seconds:
            # Send stop command to same thread
            udf_adapter.submit(
                input="!stop",
                thread_id=thread_id,
                bot_id={},
                file={}
            )

        if (time.time() - start_time) >= timeout_seconds:
            return {
                "success": False, 
                "error": f"Timed out after {timeout_seconds} seconds waiting for valid JSON response"
            }

    except Exception as e:
        return {
            "success": False,
            "error": f"Error: {str(e)}"
        }

if __name__ == "__main__":

    # provide the openai assistant id if you know it, to make startup faster.. it's output the first time you start without it in the INFO logging
    bot_list = [
        {"bot_id": "Janice-JL"},
        {"bot_id": "MrSpock-3762b2", "assistant_id": "asst_sbxRnnpMKosmf4cb3UeIVZZH"}
    ]

    scheduler = BackgroundScheduler(
        {
            "apscheduler.job_defaults.max_instances": 100,
            "apscheduler.job_defaults.coalesce": True,
        }
    )

    sessions, api_app_id_to_session_map, bot_id_to_udf_adapter_map, SystemVariables.bot_id_to_slack_adapter_map = create_sessions(
            db_adapter,
            bot_id_to_udf_adapter_map,
            stream_mode=True,
            skip_slack=True,
            bot_list = bot_list,    )

    BotOsServer.stream_mode = True
    server = BotOsServer(
        flask_app=None, sessions=sessions, scheduler=scheduler, scheduler_seconds_interval=1,
        slack_active=global_flags.slack_active,
        db_adapter=db_adapter,
                bot_id_to_udf_adapter_map = bot_id_to_udf_adapter_map,
                api_app_id_to_session_map = api_app_id_to_session_map,
                bot_id_to_slack_adapter_map = SystemVariables.bot_id_to_slack_adapter_map,
    )

    scheduler.start()

    result = prompt_bot_and_await_response(target_bot='Janice-JL', prompt="Who are you and what is your mission?")
    print(f"\n{result}")

    result = prompt_bot_and_await_response(target_bot='Janice-JL', prompt="Search metadata for data on baseball, then find the number of baseball teams in the data.")
    print(f"\n{result}")
    
    result = prompt_bot_and_await_response(target_bot='MrSpock-3762b2', prompt="Delegate to Janice: Run on Snowflake 'select current_account();")
    print(f"\n{result}")
    
    scheduler.shutdown()
    print("Done")
