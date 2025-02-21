from   datetime                 import datetime, timedelta
import json
import os
import requests
import sys
import time


from   apscheduler.schedulers.background \
                                import BackgroundScheduler
from   flask                    import Flask, jsonify, make_response, request
from   genesis_bots.core.bot_os_server \
                                import BotOsServer
from   genesis_bots.core.tools.process_scheduler \
                                import process_scheduler
# from connectors import get_global_db_connector
from   genesis_bots.bot_genesis.make_baby_bot \
                                import (get_all_bots_full_details,
                                        get_bot_details, get_ngrok_auth_token,
                                        get_slack_config_tokens, list_all_bots,
                                        make_baby_bot, rotate_slack_token,
                                        set_ngrok_auth_token,
                                        set_slack_config_tokens,
                                        test_slack_config_token,
                                        update_bot_details,
                                        update_slack_app_level_key)
from   genesis_bots.connectors.snowflake_connector.snowflake_connector \
                                import SnowflakeConnector
from   genesis_bots.connectors.sqlite_connector \
                                import SqliteConnector
from   genesis_bots.core.bot_os_tools \
                                import ToolBelt
from   genesis_bots.slack.slack_bot_os_adapter \
                                import SlackBotAdapter

# from auto_ngrok.auto_ngrok import launch_ngrok_and_update_bots
from   genesis_bots.core.bot_os_task_input_adapter \
                                import TaskBotOsInputAdapter

from   genesis_bots.auto_ngrok.auto_ngrok \
                                import launch_ngrok_and_update_bots
from   genesis_bots.core.system_variables \
                                import SystemVariables
from   genesis_bots.demo.sessions_creator \
                                import create_sessions, make_session

from   genesis_bots.core.logging_config \
                                import logger

from   genesis_bots.core        import global_flags

##### TEST MODE FLAG
#os.environ['TEST_TASK_MODE'] = 'true'
########################################

##### SET TASK FLAG (causes openAI init to not update or recreate the assistant, reuses existing one from multibot runnner)
os.environ['TASK_MODE'] = 'true'
os.environ['SHOW_COST'] = 'false'
########################################

logger.info("****** GENBOT VERSION 0.300 *******")
logger.info("****** TASK AUTOMATION SERVER *******")
runner_id = os.getenv("RUNNER_ID", "jl-local-runner")
logger.info("Runner ID: ", runner_id)
global_flags.runner_id = runner_id
snowflake_secure_value = os.getenv("SNOWFLAKE_SECURE")
# if snowflake_secure_value is not None:
#    logger.info("SNOWFLAKE_SECURE:", snowflake_secure_value)
#    logger.warning("SNOWFLAKE_SECURE: %s", snowflake_secure_value)
# else:
#   logger.info("SNOWFLAKE_SECURE: not set")
#    logger.warning("SNOWFLAKE_SECURE: not set")

# Check if TEST_TASK_MODE is false or not existent, then wait and print a message
if not os.getenv("TEST_TASK_MODE", "false").lower() == "true":
    logger.info("waiting 60 seconds for other services to start first...")
    time.sleep(60)
genbot_internal_project_and_schema = os.getenv("GENESIS_INTERNAL_DB_SCHEMA", "None")
if genbot_internal_project_and_schema == "None":
    logger.info("ENV Variable GENESIS_INTERNAL_DB_SCHEMA is not set.")
if genbot_internal_project_and_schema is not None:
    genbot_internal_project_and_schema = genbot_internal_project_and_schema.upper()
db_schema = genbot_internal_project_and_schema.split(".")
project_id = db_schema[0]
dataset_name = db_schema[1]
global_flags.project_id = project_id
global_flags.genbot_internal_project_and_schema = genbot_internal_project_and_schema

genesis_source = os.getenv('GENESIS_SOURCE', default="Snowflake")
if genesis_source ==  'Sqlite':
    db_adapter = SqliteConnector(connection_name='Sqlite')
elif genesis_source == 'Snowflake':
    db_adapter = SnowflakeConnector(connection_name='Snowflake')
else:
    raise ValueError('Invalid Source')
# db_adapter = get_global_db_connector()

if not os.getenv("TEST_TASK_MODE", "false").lower() == "true":
    db_adapter.ensure_table_exists()

tool_belt = ToolBelt()

def insert_task_history(
        task_id,
        work_done_summary,
        task_status,
        updated_task_learnings,
        report_message="",
        done_flag=False,
        needs_help_flag="N",
        task_clarity_comments="",
    ):
        """
        Inserts a row into the TASK_HISTORY table.

        Args:
            task_id (str): The unique identifier for the task.
            work_done_summary (str): A summary of the work done.
            task_status (str): The status of the task.
            updated_task_learnings (str): Any new learnings from the task.
            report_message (str): The message to report about the task.
            done_flag (bool): Flag indicating if the task is done.
            needs_help_flag (bool): Flag indicating if help is needed.
            task_clarity_comments (str): Comments on the clarity of the task.
        """
        insert_query = f"""
            INSERT INTO {db_adapter.schema}.TASK_HISTORY (
                task_id, work_done_summary, task_status, updated_task_learnings,
                report_message, done_flag, needs_help_flag, task_clarity_comments
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s
            )
        """
        cursor = None
        try:
            cursor = db_adapter.client.cursor()
            cursor.execute(
                insert_query,
                (
                    task_id,
                    work_done_summary,
                    task_status,
                    updated_task_learnings,
                    report_message,
                    done_flag,
                    needs_help_flag,
                    task_clarity_comments,
                ),
            )
            db_adapter.client.commit()
            cursor.close()
            logger.info(f"Task history row inserted successfully for task_id: {task_id}")
        except Exception as e:
            logger.info(f"An error occurred while inserting the task history row: {e}")
            if cursor is not None:
                cursor.close()

def get_udf_endpoint_url(endpoint_name="udfendpoint"):
    # TODO: Duplicated code. Use the (newer) db_connector.db_get_endpoint_ingress_url
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
                endpoint["INGRESS_URL"]
                for endpoint in results
                if endpoint["NAME"] == endpoint_name
            ),
            None,
        )
        return udf_endpoint_url
    except Exception as e:
        logger.warning(f"Failed to get {endpoint_name} endpoint URL with error: {e}")
        return None

# Call the function to show endpoints
try:
    ep = get_udf_endpoint_url("udfendpoint")
    logger.warning(f"udf endpoint: {ep}")
except Exception as e:
    logger.warning(f"Error on get_endpoints {e} ")

ngrok_active = False

#old
if False:
    logger.info(f"Checking LLM key...")
    def get_llm_api_key():
        from genesis_bots.core.bot_os_llm import LLMKeyHandler
        logger.info('Getting LLM API Key...')
        api_key_from_env = False
        llm_type = os.getenv("BOT_OS_DEFAULT_LLM_ENGINE", "openai")
        llm_api_key = None

        i = 0
        c = 0

        while llm_api_key == None:

            i = i + 1
            if i > 100:
                c += 1
                logger.info(f'Waiting on LLM key... (cycle {c})')
                i = 0
            # llm_type = None
            llm_key_handler = LLMKeyHandler()
            logger.info('Getting LLM API Key...')

            api_key_from_env, llm_api_key = llm_key_handler.get_llm_key_from_db()

            if llm_api_key is None and llm_api_key != 'cortex_no_key_needed':
            #   logger.info('No LLM Key Available in ENV var or Snowflake database, sleeping 20 seconds before retry.')
                time.sleep(20)
            else:
                logger.info(f"Using {llm_type} for task server ")

    llm_api_key = get_llm_api_key()


# new llm stuff
logger.info('Getting LLM API Key...')
# api_key_from_env, llm_api_key = llm_key_handler.get_llm_key_from_db()


def get_llm_api_key(db_adapter=None):
    from genesis_bots.core.bot_os_llm import LLMKeyHandler
    logger.info('Getting LLM API Key...')
    api_key_from_env = False
    llm_type = os.getenv("BOT_OS_DEFAULT_LLM_ENGINE", "openai")
    llm_api_key_struct = None

    i = 0
    c = 0

    while llm_api_key_struct == None:

        refresh_seconds = 180
        wake_up = False
        while not wake_up:

            ii = 0
            try:
                cursor = db_adapter.client.cursor()
                check_bot_active = f"DESCRIBE TABLE {db_adapter.schema}.BOTS_ACTIVE"
                cursor.execute(check_bot_active)
                result = cursor.fetchone()

                bot_active_time_dt = datetime.strptime(result[0], '%Y-%m-%d %H:%M:%S %Z')
                current_time = datetime.now()
                time_difference = current_time - bot_active_time_dt

                ii += 1
                if ii >= 30:
                    logger.info(f"BOTS ACTIVE TIME: {result[0]} | CURRENT TIME: {current_time} | TIME DIFFERENCE: {time_difference} | task server")
                    ii = 0

                if time_difference < timedelta(minutes=5):
                    wake_up = True
                else:
                    time.sleep(refresh_seconds)
            except:
                logger.info('Waiting for BOTS_ACTIVE table to be created...')
                time.sleep(refresh_seconds)

        i = i + 1
        if i > 100:
            c += 1
            logger.info(f'Waiting on LLM key... (cycle {c})')
            i = 0
        # llm_type = None
        llm_key_handler = LLMKeyHandler(db_adapter=db_adapter)
       # logger.info('Getting LLM API Key...')

        not_used_api_key_from_env, llm_api_key_struct = llm_key_handler.get_llm_key_from_db()

        if llm_api_key_struct.llm_key is None and llm_api_key_struct.llm_key != 'cortex_no_key_needed':
        #   logger.info('No LLM Key Available in ENV var or Snowflake database, sleeping 20 seconds before retry.')
            time.sleep(180)
        else:
            logger.info(f"Using {llm_type} for task server ")

    return llm_api_key_struct

llm_api_key_struct = get_llm_api_key(db_adapter)

### END LLM KEY STUFF
logger.info('Out of LLM check section ..')


global_flags.slack_active = test_slack_config_token()
if global_flags.slack_active == "token_expired":
    t, r = get_slack_config_tokens()
    tp, rp = rotate_slack_token(config_token=t, refresh_token=r)
    global_flags.slack_active = test_slack_config_token()
logger.info("...Slack Connector Active Flag: ", global_flags.slack_active)


bot_id_to_udf_adapter_map = {}

if llm_api_key_struct.llm_key is not None:
    (
        sessions,
        api_app_id_to_session_map,
        bot_id_to_udf_adapter_map,
        SystemVariables.bot_id_to_slack_adapter_map,
    ) = create_sessions(
        db_adapter,
        bot_id_to_udf_adapter_map,
        stream_mode=False,
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
    bot_id = input_rows[0][3]
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
        bots = list_all_bots(runner_id=runner)

        for bot in bots:
            bot_id = bot.get("bot_id")

            # Retrieve the session for the bot using the bot_id
            bot_slack_adapter = bot_id_to_slack_adapter_map.get(bot_id, None)
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
        else:
            raise ValueError(
                "Invalid metadata_type provided. Expected 'harvest_control' or 'harvest_summary' or 'available_databases'."
            )

        if result["Success"]:
            output_rows = [[input_rows[0][0], json.loads(result["Data"])]]
        else:
            output_rows = [
                [input_rows[0][0], {"Success": False, "Message": result["Error"]}]
            ]

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

scheduler = BackgroundScheduler(
    {
        "apscheduler.job_defaults.max_instances": 40,
        "apscheduler.job_defaults.coalesce": True,
    }
)

server = None
if llm_api_key_struct is not None:
    server = BotOsServer(
        flask_app=None, sessions=sessions, scheduler=scheduler, scheduler_seconds_interval=2, db_adapter=db_adapter, bot_id_to_udf_adapter_map=bot_id_to_udf_adapter_map, api_app_id_to_session_map=api_app_id_to_session_map
    )


@app.route("/zapier", methods=["POST"])
def zaiper_handler():

    try:
        api_key = request.args.get("api_key")
    except:
        return "Missing API Key"

    #   logger.info("Zapier: ", api_key)
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

    # logger.info(bot_id, 'code: ', code, 'state', state)

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
            logger.info("State error.. possible forgery")
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

    if runner == bot_details["runner_id"]:
        bot_config = get_bot_details(bot_id=bot_id)
        if no_slack:
            bot_config["slack_active"] = "N"
        new_session, api_app_id, udf_local_adapter, slack_adapter_local = make_session(
            bot_config=bot_config,
            db_adapter=db_adapter,
            bot_id_to_udf_adapter_map=bot_id_to_udf_adapter_map,
            stream_mode=False,
            skip_vectors=True,
        )
        # check new_session
        if new_session is None:
            logger.info("new_session is none")
            return "Error: Not Installed new session is none"
        if slack_adapter_local is not None:
            bot_id_to_slack_adapter_map[bot_config["bot_id"]] = slack_adapter_local
        if udf_local_adapter is not None:
            bot_id_to_udf_adapter_map[bot_config["bot_id"]] = udf_local_adapter
        api_app_id_to_session_map[api_app_id] = new_session
        #   logger.info("about to add session ",new_session)
        server.add_session(new_session, replace_existing=True)

        if no_slack:
            logger.info(
                f"Genesis bot {bot_id} successfully installed and ready for use via Streamlit."
            )
        else:
            return f"Genesis bot {bot_id} successfully installed to Streamlit and Slack and ready for use."
    else:
        # Handle errors
        logger.info("Failed to exchange code for access token:", response.text)
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


scheduler.start()


def add_bot_session(bot_id=None, no_slack=False):
    # Extract the API App ID from the incoming request
    global llm_api_key, default_llm_engine, sessions, api_app_id_to_session_map, bot_id_to_slack_adapter_map, bot_id_to_udf_adapter_map, server

    logger.info("HERE 1")
    bot_details = get_bot_details(bot_id=bot_id)

    # logger.info(bot_id, 'code: ', code, 'state', state)

    # lookup via the bot map via bot_id

    # Save these mapped to the bot
    runner = os.getenv("RUNNER_ID", "jl-local-runner")
    if runner == bot_details["runner_id"]:
        bot_config = get_bot_details(bot_id=bot_id)
        if no_slack:
            bot_config["slack_active"] = "N"
        new_session, api_app_id, udf_local_adapter, slack_adapter_local = make_session(
            bot_config=bot_config,
            db_adapter=db_adapter,
            bot_id_to_udf_adapter_map=bot_id_to_udf_adapter_map,
            stream_mode=False,
            skip_vectors=True,
        )
        # check new_session
        if new_session is None:
            logger.info("new_session is none")
            return "Error: Not Installed new session is none"
        if slack_adapter_local is not None:
            bot_id_to_slack_adapter_map[bot_config["bot_id"]] = slack_adapter_local
        if udf_local_adapter is not None:
            bot_id_to_udf_adapter_map[bot_config["bot_id"]] = udf_local_adapter
        api_app_id_to_session_map[api_app_id] = new_session
        #   logger.info("about to add session ",new_session)
        server.add_session(new_session, replace_existing=True)
        sessions.append(new_session)

        if no_slack:
            logger.info(
                f"Genesis bot {bot_id} successfully installed and ready for use via Streamlit."
            )
        else:
            return f"Genesis bot {bot_id} successfully installed to Streamlit and Slack and ready for use."
    else:
        # Handle errors
        return "Error: Not new bot added"


def generate_task_prompt(bot_id, task):
    # Retrieve task details from the database using bot_id and task_id
    task_details = task

    # Construct the prompt based on the task details and the template provided
#    Task description:
#    {task_details['task_instructions']}

    learnings = task_details.get('task_learnings','No learnings yet.')
    prompt = f"""
    You have been woken up automatically to run a predefined process using the process runner tool in unattended mode.  You are not to create a process nor create a new schedule for a process.

    Process name to run:
    {task_details['task_name']}

    Reporting instructions:
    {task_details['reporting_instructions']}

    The user who gave you this task is:
    {task_details['primary_report_to_type']}: {task_details['primary_report_to_id']}

    Here is the last status you noted the last time you ran this task:
    {task_details['last_task_status']}

    Here are some things you've noted that you've learned in past runs of this task about how to do it better:
    {learnings}

    Here is the 'schedule' noting how often this scheduled process (aka 'task') should be run (if recurring), and possibly until what date/time. If this notes that it is to be run 'one time', 'non-recurring' or similar, or notes a date/time for the task to stop and its past that time now, then do not run it again after this run, and set the stop_task_flag to TRUE in your response:
    {task_details['action_trigger_type']} {task_details['action_trigger_details']}

    Here is the current server time:
    {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

    Use the process runner tool to run the process named above and follow the instructions that it gives you. Call mulitple tools if needed to complete the task.
    Do NOT create a new process, or a new schedule for an existing process. You are to execute the steps described above for this existing task.
    If you send an email or a slack direct message or slack channel message as part of the task, include at the end of the message and the user they can ask you about this scheduled process for more details: \n[This messages is from Genesis bot {bot_id} running process:{task_details['task_name']} on schedule_id:{task_details['task_id']}]
    Do not call process_scheduler while performing this work. Only generate an image if specifically told to.
    When you are DONE with this task and have FULLY completed it, return only a JSON document with these items, no other text:

    {{
        "work_done_summary": <a summary of the work you did to complete the task during this run, including any tools you called and outbound communications you made>,
        "task_status": <write a summary of the current status of the task, if its working fine and ongoing just say OK, if a specific next step is needed, state what should happen next>,
        "updated_task_learnings": <the task_learnings text you received at the start of this task, updated or appended with anything new you learned about how to perform this task during this run. Include anything you had to figure out (channel name, user name, which tool to use, etc) that you could skip next time if you knew something in advance that isn't subject to frequent change, like tables you found or SQL you used or Slack IDs of people you communicated with, or slack channel names you looked up.>,
        "report_message": <include this if you are supposed to report back based on reporting_instructions based on what happened, otherwise omit for no report back.",
        "stop_task_flag": <TRUE if this was a one-time non-recurring task, if the "how often the task should run" has a date or time specificed and the current system time is past that time, or there is something wrong and the process and it should NOT be run again on its schedule (if any), FALSE if everything is Ok and the task should continue to trigger if scheduled to do so>,
        "needs_help_flag": <true if you need help from the administrator, are encountering repeated errors, etc., false if assistance is not needed before the next task run>,
        "task_clarity_comments": <state any problems you are having running the task, or any help you need, errors youre getting. omit this if task is clear and working properly>
        "next_run_time": <date_timestamp for when to run this task next in %Y-%m-%d %H:%M:%S format. Figure this out based on the information above. Omit this parameter if stop_task_flag is TRUE>
    }}

    If you respond back with anything other than a JSON document like the above, I will simply remind you of the required response format, as this thread is being supervised by an unattended runner.
    Reminder: do not include any other text with your response, just the JSON document.
    """

    return prompt.strip()


def submit_task(session=None, bot_id=None, task=None):
    # Use a prompt similar to tmp/tmp_task_thoughts.txt to interact with the bot
    # Perform the task and construct the response JSON

    # Check if the next_check_ts is in the past    current_time = datetime.now()
    next_check_ts_str = task.get("next_check_ts")
    if next_check_ts_str:
        next_check_ts = datetime.strptime(
            next_check_ts_str, "%Y-%m-%d %H:%M:%S"
        )

        if os.getenv("TEST_TASK_MODE", "false").lower() != "true":
            if datetime.now() < next_check_ts:
                return {
                    "task_skipped": True,
                    "reason": "Next check timestamp is in the future.",
                }
            else:
                '!!! TEST TASK MODE SKIPPED TIME CHECK !!!'

    if not task.get("task_active", False):
        return {"task_skipped": True, "reason": "Task is not active."}

    # Call the function to generate the LLM prompt
    prompt = generate_task_prompt(bot_id, task)

    # Insert the current timestamp into a string
    current_timestamp_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    task_meta = {
        "bot_id": bot_id,
        "task_id": task["task_id"],
        "submited_time": current_timestamp_str,
    }

    event = {"thread_id": None, "msg": prompt, "task_meta": task_meta}

    if find_replace_updated_bot_service(bot_id):
        logger.info(f"Definition for bot {bot_id} has changed and needs to be restarted.")
        # add logic here to force re-load

    input_adapter = bot_id_to_udf_adapter_map.get(bot_id, None)
    if input_adapter:
        input_adapter.add_event(event)

        return task_meta
    else:
        return {"error": "No input adapter available for bot_id: {}".format(bot_id)}

    # add a queue of pending task runs


def task_log_and_update(bot_id, task_id, task_result):

    insert_task_history(
        task_id=task_id,
        work_done_summary=task_result["work_done_summary"],
        task_status=task_result["task_status"],
        updated_task_learnings=task_result["updated_task_learnings"],
        report_message=task_result.get("report_message", ""),
        done_flag=task_result["stop_task_flag"],
        needs_help_flag=task_result["needs_help_flag"],
        task_clarity_comments=task_result.get("task_clarity_comments", ""),
    )
    # Update the task in the TASKS table
    if task_result.get("stop_task_flag", False) == True:
        task_active = False
    else:
        task_active = True
    process_scheduler(
        action="UPDATE_CONFIRMED",
        bot_id=bot_id,
        task_id=task_id,
        task_details={
            "next_check_ts": task_result.get("next_run_time"),
            "last_task_status": task_result.get("task_status"),
            "task_learnings": task_result.get("updated_task_learnings"),
            "task_active": task_active,
        },
    )


def tasks_loop():

    from collections import deque
    from sortedcontainers import SortedList

    # Initialize a deque for pending tasks
    pending_tasks = deque()
    task_retry_attempts_map = {}

    def task_sort_key(task):
        return task["next_check_ts"]

    if os.getenv("TEST_TASK_MODE", "false").lower() != "true":
        backup_bot_servicing()

    i = 10
    cycle = 0
    while True:
        i = i + 1
        cycle += 1
        iteration_start_time = datetime.now()
        if i >= 10:
            #   logger.info(f'Checking tasks... cycle={cycle}, {iteration_start_time}')
            sys.stdout.write(
                f"Checking tasks... cycle={cycle}, {iteration_start_time}\n"
            )
            sys.stdout.flush()
            i = 0

        # Get all bot details
        all_bots_details = get_all_bots_full_details(runner_id=runner_id)

        # Extract bot IDs from the details
        all_bot_ids = [bot['bot_id'] for bot in all_bots_details]

        # global sessions

        if os.getenv("TEST_TASK_MODE", "false").lower() != "true":
            add_sessions(all_bot_ids, all_bots_details, sessions)

        # Retrieve the list of bots and their tasks

        # Check for tasks submitted more than 10 minutes ago
        ten_minutes_ago = datetime.now() - timedelta(minutes=30)
        overdue_tasks = [
            task
            for task in pending_tasks
            if datetime.strptime(task["submited_time"], "%Y-%m-%d %H:%M:%S")
            < ten_minutes_ago
        ]
        for task in overdue_tasks:
            logger.info(
                f"Task {task['task_id']} from bot {task['bot_id']} is overdue, running for more than 30 minutes, removing from queue. Can we cancel the run?."
            )
            pending_tasks.remove(task)
            # set to failed status

        skipped_tasks = SortedList(key=task_sort_key)
        active_sessions = sessions

        # Assuming sessions is a list of session objects and sessions_to_recreate is a list of bot_ids
        # sessions = [session for session in sessions if session.bot_id not in sessions_to_recreate]

        # if len(sessions_to_recreate) > 0:
        #     all_bots_details = get_all_bots_full_details(runner_id=runner_id)
        #     add_sessions(all_bot_ids, all_bots_details, sessions)

        for session in active_sessions:
            bot_id = session.bot_id
            if os.getenv("TEST_TASK_MODE", "false").lower() == "true":
                logger.info('test task mode - looking for tasks for bot ',bot_id)
            tasks = process_scheduler(action="LIST", bot_id=bot_id, task_id=None)
           # if os.getenv("TEST_TASK_MODE", "false").lower() == "true":
           #     logger.info('test task mode - tasks are: ',tasks)
            if tasks.get("Success"):
                for task in [
                    t for t in tasks.get("Scheduled Processes", []) if t.get("task_active", False)
                ]:
              #      if os.getenv("TEST_TASK_MODE", "false").lower() == "true":
              #          if task['task_id'] != 'janiCortex-123456_monitor_unused_data_tables_6WIDsb_HDBgeH':
               #             continue
                 #       logger.info('test task mode - task is: ',task)
                    # If an instance of the task is not alreday running, Process the task using the bot
                    if not any(
                        pending_task["task_id"] == task["task_id"]
                        for pending_task in pending_tasks
                    ):
                        task_result = submit_task(
                            session=session, bot_id=bot_id, task=task
                        )
                        if task_result.get('task_skipped', False) and task_result['reason'] == "Next check timestamp is in the future.":
                            skipped_tasks.add(task)
                        if "bot_id" in task_result:
                            pending_tasks.append(task_result)
                            logger.info(f"Task {task['task_id']} has been started.")
                    else:
                        submitted_time = next(
                            (
                                pt["submited_time"]
                                for pt in pending_tasks
                                if pt["task_id"] == task["task_id"]
                            ),
                            None,
                        )
                        if submitted_time:
                            logger.info(
                                f"Task {task['task_id']} from bot {bot_id} is already running. It has been running for {(datetime.now() - datetime.strptime(submitted_time, '%Y-%m-%d %H:%M:%S')).total_seconds() / 60:.2f} minutes."
                            )
        #  i = input('Check for done? >')

        # for testing, make

        next_runs = []
        for session in active_sessions:
            # Find the input adapter that is an instance of BotOsInputAdapter
            input_adapter = next(
                (
                    adapter
                    for adapter in session.input_adapters
                    if isinstance(adapter, TaskBotOsInputAdapter)
                ),
                None,
            )
            response_map = input_adapter.response_map
            bot_id = session.bot_id
            tasks = process_scheduler(action="LIST", bot_id=bot_id, task_id=None)
            processed_tasks = []
            for task_id, response in response_map.items():

                logger.info(
                    f"Processing response for task {task_id}: output len: {len(response.output)}"
                )
                # Process the response for each task
                # This could involve updating task status, logging the response, etc.
                # The exact processing will depend on the application's requirements
                error_msg = ""
                response_valid = True
                try:
                    if response.output.endswith("`'"):
                        dollar_index = response.output.rfind("  `$")
                        if dollar_index != -1:
                            response.output = response.output[:dollar_index].strip()
                    if response.output.startswith(
                        "```json"
                    ) and response.output.endswith("```"):
                        response.output = response.output[6:-3].strip()
                    if "{" in response.output:
                        first_brace_position = response.output.find("{")
                        if first_brace_position != 0:
                            response.output = response.output[first_brace_position:]

                    task_response_data = json.loads(response.output)
                except Exception as e:
                    response_valid = False
                    error_msg += f"The JSON response you provided couldnt be parsed with error {e}\n"
                    task_response_data = None

                # Check if stop_task_flag is True (case-insensitive)
                if task_response_data and isinstance(task_response_data.get("stop_task_flag"), str):
                    if task_response_data["stop_task_flag"].lower() == "true":
                        task_response_data["next_run_time"] = None
                elif task_response_data and task_response_data.get("stop_task_flag") is True:
                    task_response_data["next_run_time"] = None

                if response_valid and task_response_data:
                    required_fields = [
                        "work_done_summary",
                        "task_status",
                        "updated_task_learnings",
                        "stop_task_flag",
                        "needs_help_flag",
                        "next_run_time",
                    ]
                    missing_fields = [
                        field
                        for field in required_fields
                        if field not in task_response_data
                    ]
                    invalid_fields = []
                    if not missing_fields:
                        # Validate boolean fields
                        # Convert stop_task_flag to boolean if it's a string
                        if isinstance(task_response_data["stop_task_flag"], str):
                            stop_flag_upper = task_response_data["stop_task_flag"].upper()
                            if stop_flag_upper == "TRUE":
                                task_response_data["stop_task_flag"] = True
                            elif stop_flag_upper == "FALSE":
                                task_response_data["stop_task_flag"] = False
                        # Convert needs_help_flag to boolean if it's a string
                        if isinstance(task_response_data["needs_help_flag"], str):
                            needs_help_flag_upper = task_response_data["needs_help_flag"].upper()
                            if needs_help_flag_upper == "TRUE":
                                task_response_data["needs_help_flag"] = True
                            elif needs_help_flag_upper == "FALSE":
                                task_response_data["needs_help_flag"] = False
                        if not isinstance(task_response_data["stop_task_flag"], bool):
                            invalid_fields.append("stop_task_flag must be a boolean")
                        if not isinstance(task_response_data["needs_help_flag"], bool):
                            invalid_fields.append("needs_help_flag must be a boolean")
                        # Validate timestamp
                        try:
                            if not task_response_data["stop_task_flag"]:
                                datetime.strptime(
                                    task_response_data["next_run_time"],
                                    "%Y-%m-%d %H:%M:%S",
                                )
                        except ValueError:
                            invalid_fields.append(
                                "next_run_time must be a valid timestamp in the format YYYY-MM-DD HH:MM:SS"
                            )
                    if missing_fields or invalid_fields:
                        response_valid = False
                        error_msg += f'Missing or invalid fields: {", ".join(missing_fields + invalid_fields)}'

                if (
                    response_valid
                    and task_response_data
                    and task_response_data["needs_help_flag"]
                ):
                    # Retrieve the creator of the task
                    # for now, have it suspend any task that needs help
                    task_response_data["stop_task_flag"] = True
                    try:
                        try:
                            task = next(
                                (t for t in tasks["Scheduled Processes"] if t["task_id"] == task_id), None
                            )
                            task_creator_id = task.get("primary_report_to_id", None)
                            task_name = task.get("task_name", None)
                            slack_adapter = next(
                                (
                                    adapter
                                    for adapter in session.input_adapters
                                    if isinstance(adapter, SlackBotAdapter)
                                ),
                                None,
                            )
                        except Exception as e:
                            slack_adapter = None
                            task_creator_id = None
                            logger.info("Error finding task in process result to lookup slack user to notify: ",e)
                        # Send a direct message to the creator of the task
                        if (slack_adapter is not None) and task_creator_id:
                            help_message = f":exclamation: Task needs your help -- Task: {task_name} ({task_id}) for bot {bot_id} requires your attention.\n Issues/Suggestions: {task_response_data.get('task_clarity_comments', 'No suggestions provided.')}\nPlease discuss this with {bot_id}."
                            task_json_pretty = json.dumps(task, indent=4)
                            help_message += (
                                f"\n\nTask details:\n```{task_json_pretty}```"
                            )
                            help_message += (
                                f"\n\nWhat happened this run:```{response.output}```"
                            )
                            if task_response_data.get("stop_task_flag", True):
                                help_message += "\n_Note: The task has been set to inactive pending your review._"
                            else:
                                help_message += "\n_Note: The task will stay active, but you may want to adjust its instructions to make it more clear._"
                            slack_adapter.send_slack_direct_message(
                                slack_user_id=task_creator_id, message=help_message
                            )
                            logger.info(
                                f"Sent help message to task creator {task_creator_id} for task {task_id}."
                            )
                        else:
                            logger.info(
                                f"Slack adapter not available to send help message for task {task_id}."
                            )
                    except Exception as e:
                        logger.info(f"Error seeking help for task {task_id} - {e}")

                if response_valid and task_response_data:
                    # Ensure next_run_time is at least 5 minutes from now
                    if not task_response_data.get("stop_task_flag", False):
                        next_run_time = datetime.strptime(     task_response_data["next_run_time"], "%Y-%m-%d %H:%M:%S"   )
                        if (next_run_time - datetime.now()).total_seconds() < 300:
                            task_response_data["next_run_time"] = (
                                datetime.now() + timedelta(minutes=5)
                            ).strftime("%Y-%m-%d %H:%M:%S")
                            logger.info(
                                f"Changed next_run_time for task {task_id} from bot {bot_id} to ensure it's at least 5 minutes from now."
                            )
                        next_run_time = datetime.strptime(     task_response_data["next_run_time"], "%Y-%m-%d %H:%M:%S"   )
                        next_runs.append(next_run_time)



                if not response_valid:
                    # count retries stop after 3
                    logger.info(error_msg)

                    task_retry_attempts_map = task_retry_attempts_map or {}
                    if task_id not in task_retry_attempts_map:
                        task_retry_attempts_map[task_id] = 1
                    else:
                        task_retry_attempts_map[task_id] += 1

                    if task_retry_attempts_map[task_id] > 3:
                        # Make the task inactive after 3 retries
                        logger.info(
                            f"Task {task_id} has exceeded the maximum number of retries. Marking as inactive."
                        )

                        processed_tasks.append(task_id)

                        process_scheduler(
                            action="UPDATE_CONFIRMED",
                            bot_id=bot_id,
                            task_id=task_id,
                            task_details={
                                "next_check_ts": (
                                    datetime.now()
                                    - timedelta(minutes=5)
                                ).strftime("%Y-%m-%d %H:%M:%S"),
                                "last_task_status": "Task failed to respond with a proper JSON after 3 tries.",
                                "task_learnings": task_response_data.get(
                                    "updated_task_learnings", ""
                                ),
                                "task_active": False,
                            },
                        )
                        insert_task_history(
                            task_id=task_id,
                            work_done_summary="Task failed to respond with a proper JSON after 3 tries.",
                            task_status="Inactive after retries",
                            updated_task_learnings=task_response_data.get(
                                "updated_task_learnings", ""
                            ),
                            report_message="Task marked as inactive due to invalid responses.",
                            done_flag=True,
                            needs_help_flag=False,
                            task_clarity_comments="Unknon; issue was with response content.",
                        )
                    else:
                        # Check if thread_id is in the response
                        # Error: argument of type 'BotOsOutputMessage' is not iterable
                        thread = None
                        try:
                            thread = response.thread_id
                        except:
                            try:
                                if 'thread_id' in response:
                                    thread = response['thread_id']
                                else:
                                    thread = response.messages.data[0].thread_id
                            except:
                                try:
                                    thread = response.messages.data[0].thread_id
                                except:
                                    thread = None
                        if thread == None:
                            logger.info('!!! Thread_id not found in response for callback')
                        current_timestamp_str = datetime.now().strftime(
                            "%Y-%m-%d %H:%M:%S"
                        )
                        task_meta = {
                            "bot_id": bot_id,
                            "task_id": task["task_id"],
                            "submited_time": current_timestamp_str,
                        }
                        event = {
                            "thread_id": thread,
                            "msg": f"Your response generated an error, please try to fix it. Error: {error_msg}",
                            "current_time": current_timestamp_str,
                            "task_meta": task_meta,
                        }
                        input_adapter.add_event(event=event)

                else:
                    task_log_and_update(bot_id, task_id, task_response_data)

                # Here you would include the logic to handle the response
                # For example, updating the task status in the database
                # This is a placeholder for the response processing logic
                # ...

                if response_valid:
                    processed_tasks.append(task_id)

            for task_id in processed_tasks:
                pending_tasks = deque(
                    [task for task in pending_tasks if task["task_id"] != task_id]
                )
                response_map.pop(task_id, None)

        #   i = input('Next round? >')

        # time_to_sleep = 60 - (datetime.now() - iteration_start_time).seconds
        # if os.getenv("TEST_TASK_MODE", "false").lower() == "true":
        #    time_to_sleep = 0
        # else:
        #     logger.info('Waiting 60 seconds before checking tasks again...')

        # for testing, set skipped_tasks[0]["next_check_ts"] to datetime.now() + timedelta(seconds=30)
        # skipped_tasks[0]["next_check_ts"] = datetime.now() + timedelta(seconds=30)

        wake_up = False
        i = 0
        while not wake_up:
             # Check for a task within the next two minutes

            seconds_until_next_check = None

            if len(skipped_tasks) > 0:
                try:
                    seconds_until_next_check = (datetime.strptime(skipped_tasks[0]["next_check_ts"], '%Y-%m-%d %H:%M:%S') - datetime.now()).total_seconds()
                except:
                    seconds_until_next_check = (datetime.strptime(skipped_tasks[0]["next_check_ts"], '%Y-%m-%d %H:%M:%S %Z') - datetime.now()).total_seconds()
                if seconds_until_next_check and seconds_until_next_check < 120:
                    wake_up = True
                    logger.info(f"Seconds until next check: {seconds_until_next_check:.2f}")
                    logger.info(f"Task {task['task_id']} is due to run soon.")
            if len(pending_tasks) > 0:
                wake_up = True
            for next_run in next_runs:
                if next_run < datetime.now() + timedelta(minutes=2):
                    wake_up = True
                    logger.info(f"A task is due to run soon.")
                    next_runs.remove(next_run)
                    seconds_until_next_run = (next_run - datetime.now()).total_seconds()
                    if seconds_until_next_check is None or seconds_until_next_run < seconds_until_next_check:
                        seconds_until_next_check = seconds_until_next_run
                    logger.info(f"Task due to run in {seconds_until_next_check:.2f} seconds.")
                    #break

            if seconds_until_next_check is not None:
                wait_time = max(0, min(120, seconds_until_next_check))
            else:
                wait_time = 120
            if len(pending_tasks) > 0:
                    wait_time = 15

            if os.getenv("TEST_TASK_MODE", "false").lower() == "true":
                logger.info("TEST_TASK_MODE -> overriding sleep to 5 seconds...")
                wait_time = 5
                wake_up = True
            time.sleep(wait_time)

            if os.getenv("TEST_TASK_MODE", "false").lower() == "false":
                cursor = db_adapter.client.cursor()
                check_bot_active = f"DESCRIBE TABLE {db_adapter.schema}.BOTS_ACTIVE"
                cursor.execute(check_bot_active)
                result = cursor.fetchone()

                bot_active_time_dt = datetime.strptime(result[0], '%Y-%m-%d %H:%M:%S %Z')
                current_time = datetime.now()
                time_difference = current_time - bot_active_time_dt

                i = i + 1
                if i == 1:
                    logger.info(f"BOTS ACTIVE TIME: {result[0]} | CURRENT TIME: {current_time} | TIME DIFFERENCE: {time_difference}")
                if i > 30:
                    i = 0

                if time_difference < timedelta(minutes=5):
                    wake_up = True
        #         logger.info("Bot is active")


        # if time_to_sleep > 0:
        #     for remaining in range(time_to_sleep, 0, -5):
        #         # sys.stdout.write("\r")
        #         #           sys.stdout.write("Waiting for {:2d} seconds before next check of tasks".format(remaining))
        #         # sys.stdout.flush()
        #         time.sleep(5)
    #      sys.stdout.write("\rComplete! Waiting over.          \n")

    # now go through the queue of pending task runs, and check input adapter maps for responses
    # process responses
    # make sure properly answered if not resubmit
    # see if task should stay active
    # get next timestamp
    # save task history
    # update task definition

    # Sleep for a specified interval before checking for tasks again
    # Check for tasks every minute

def backup_bot_servicing():
    backup_sql = f"""CREATE OR REPLACE TABLE {db_adapter.schema}.bot_servicing_backup AS SELECT * FROM {db_adapter.schema}.bot_servicing;"""
    cursor = db_adapter.client.cursor()
    try:
        cursor.execute(backup_sql)
        logger.info("Debug: Table bot_servicing_backup created or replaced successfully.")
    except Exception as e:
        logger.info(f"Error: Failed to create or replace table bot_servicing_backup. {e}")
    finally:
        cursor.close()


def find_replace_updated_bot_service(bot_id):
    query = f"""
        SELECT bs.*
        FROM {db_adapter.schema}.bot_servicing bs
        LEFT JOIN {db_adapter.schema}.bot_servicing_backup bsb
        ON bs.bot_id = bsb.bot_id
        WHERE bs.bot_id = %s
        AND (bs.bot_instructions != bsb.bot_instructions
        OR bs.available_tools != bsb.available_tools
        OR bs.bot_intro_prompt != bsb.bot_intro_prompt)
    """
    cursor = db_adapter.client.cursor()
    try:
        cursor.execute(query, (bot_id,))
        non_identical_rows = cursor.fetchall()

        # Copy the changed rows to bot_servicing_backup
        if non_identical_rows:
            logger.info("Debug: Retrieved non-identical rows successfully.")
            insert_query = f"""
                INSERT INTO {db_adapter.schema}.bot_servicing_backup
                SELECT * FROM {db_adapter.schema}.bot_servicing
                EXCEPT
                SELECT * FROM {db_adapter.schema}.bot_servicing_backup;
            """
            cursor.execute(insert_query)
            db_adapter.client.commit()
            logger.info("Debug: Copied changed rows to bot_servicing_backup successfully.")

            bot_ids_to_update = [row[2] for row in non_identical_rows]

            os.environ[f'RESET_BOT_SESSION_{bot_id}'] = 'True'
            return True

    except Exception as e:
        logger.info(f"Error: Failed to retrieve non-identical rows. {e}")
        return False
    finally:
        cursor.close()

def add_sessions(all_bot_ids, all_bots_details, sessions):
    # Check if any new sessions need to be added
    for bot_id in all_bot_ids:
        if bot_id not in [session.bot_id for session in sessions]:
            bot_details = next(bot for bot in all_bots_details if bot['bot_id'] == bot_id)
            logger.info(f"New bot found: ID={bot_id}, Name={bot_details['bot_name']}. Adding to sessions.")
            # Determine the Slack status of the bot
            bot_details = next(bot for bot in all_bots_details if bot['bot_id'] == bot_id)
            if bot_details.get('bot_slack_user_id', False) == False:
                no_slack = True
            else:
                no_slack = False

            # Add a new session for the bot with the appropriate no_slack flag
            add_bot_session(bot_id, no_slack=no_slack)
            logger.info(f"New session added for bot_id: {bot_id} with no_slack={no_slack}")
    return


# Start the task servicing loop
while True:
    try:
        tasks_loop()
    except Exception as e:
        logger.info("Task Loop Exception!!!!")
        logger.info(f"Error: {e}")
        logger.info('Starting loop again in 180 seconds...')
        logger.error(f"Task Loop Exception: {e}", exc_info=True)
        time.sleep(180)
