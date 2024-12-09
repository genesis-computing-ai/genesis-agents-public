import os
from flask import Blueprint
from core.logging_config import logger
from flask import request, jsonify, make_response
from bot_genesis.make_baby_bot import get_bot_details
from demo.config import get_global_db_connector, sessions
from demo.config import db_adapter, global_flags, scheduler
from demo.config import bot_id_to_udf_adapter_map, server
from demo.config import llm_api_key_struct, api_app_id_to_session_map, ngrok_active

import json
from core.system_variables import SystemVariables

import tempfile
import base64
from pathlib import Path

from core.bot_os_server import BotOsServer
from core.bot_os_artifacts import get_artifacts_store
from embed.embed_openbb import openbb_query
from llm_openai.openai_utils import get_openai_client

from bot_genesis.make_baby_bot import (
    make_baby_bot,
    update_slack_app_level_key,
    set_llm_key,
    get_ngrok_auth_token,
    set_ngrok_auth_token,
    get_bot_details,
    set_remove_pointers,
    list_all_bots,
    get_slack_config_tokens,
    set_slack_config_tokens,
)
from auto_ngrok.auto_ngrok import launch_ngrok_and_update_bots
from core.system_variables import SystemVariables
from demo.sessions_creator import create_sessions
from demo.routes.slack import bot_install_followup

udf_routes = Blueprint('udf_routes', __name__)


@udf_routes.route("/udf_proxy/submit_udf", methods=["POST"])
def submit_udf():
    # print("\nReached submit_udf endpoint\n")
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


@udf_routes.route("/udf_proxy/lookup_udf", methods=["POST"])
def lookup_udf():
    # print("\nReached lookup_udf endpoint\n")
    message = request.json
    input_rows = message["data"]
    bot_id = input_rows[0][2]

    bots_udf_adapter = bot_id_to_udf_adapter_map.get(bot_id, None)
    if bots_udf_adapter is not None:
        return bots_udf_adapter.lookup_udf_fn()
    else:
        return None


@udf_routes.route("/udf_proxy/list_available_bots", methods=["POST"])
def list_available_bots_fn():

    logger.info('-> streamlit called list available bots')
    message = request.json
    input_rows = message["data"]
    row = input_rows[0]

    output_rows = []
    if "llm_api_key_struct" not in globals() or llm_api_key_struct.llm_key is None:
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


def file_to_bytes(file_path):
    logger.info('inside file_path')
    file_bytes = Path(file_path).read_bytes()
    logger.info('inside file_path - after file_bytes')
    encoded = base64.b64encode(file_bytes).decode()
    logger.info('inside file_path - after encoded')
    return encoded

@udf_routes.route("/udf_proxy/get_metadata", methods=["POST"])
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
        elif metadata_type == 'cortex_search_services':
            result = db_adapter.get_cortex_search_service()
        elif metadata_type == "bot_llms":
            if "BOT_LLMS" in os.environ and os.environ["BOT_LLMS"]:
                result = {"Success": True, "Data": os.environ["BOT_LLMS"]}
            else:
                result = {"Success": False, "Message": result["Error"]}
        elif metadata_type.startswith('test_email '):
            email = metadata_type.split('test_email ')[1].strip()
            result = db_adapter.send_test_email(email)
        elif metadata_type.startswith('get_email'):
            result = db_adapter.get_email()
        elif metadata_type.startswith('check_eai_assigned'):
            result = db_adapter.check_eai_assigned()
        elif metadata_type.startswith('get_endpoints'):
            result = db_adapter.get_endpoints()
        elif metadata_type.startswith('delete_endpoint_group '):
            metadata_parts = metadata_type.split()
            if len(metadata_parts) == 2:
                group_name = metadata_parts[1].strip()
            else:
                logger.info("missing group name to delete")
            result = db_adapter.delete_endpoint_group(group_name)
        elif metadata_type.startswith('set_endpoint '):
            metadata_parts = metadata_type.split()
            if len(metadata_parts) == 4:
                group_name = metadata_parts[1].strip()
                endpoint = metadata_parts[2].strip()
                type = metadata_parts[3].strip()
            result = db_adapter.set_endpoint(group_name, endpoint, type)
        elif metadata_type.startswith('set_model_name '):
            model_name, embedding_model_name = metadata_type.split('set_model_name ')[1].split(' ')[:2]
            # model_name = metadata_type.split('set_model_name ')[1].strip()
            # embedding_model_name = metadata_type.split('set_model_name ')[1].strip()
            result = db_adapter.update_model_params(model_name, embedding_model_name)
        elif metadata_type.startswith('logging_status'):
            status = db_adapter.check_logging_status()
            result = {"Success": True, "Data": status}
        elif metadata_type.startswith('check_eai '):
            metadata_parts = metadata_type.split()
            if len(metadata_parts) == 2:
                site = metadata_parts[1].strip()
            else:
                logger.info("missing metadata")
            result = db_adapter.eai_test(site=site)
        elif 'sandbox' in metadata_type:
            _, bot_id, thread_id_in, file_name = metadata_type.split('|')
            logger.info('****get_metadata, file_name', file_name)
            logger.info('****get_metadata, thread_id_in', thread_id_in)
            logger.info('****get_metadata, bot_id', bot_id)
            bots_udf_adapter = bot_id_to_udf_adapter_map.get(bot_id, None)
            logger.info('****get_metadata, bots_udf_adapter', bots_udf_adapter)
            try:
                logger.info(f'**** in to out map: {bots_udf_adapter.in_to_out_thread_map}')
                thread_id_out = bots_udf_adapter.in_to_out_thread_map[thread_id_in]
                logger.info('****get_metadata, thread_id_out', thread_id_out)
                file_path = f'./downloaded_files/{thread_id_out}/{file_name}'
                logger.info('****get_metadata, file_path', file_path)
                result = {"Success": True, "Data": json.dumps(file_to_bytes(file_path))}
                logger.info('result: Success len ', len(json.dumps(file_to_bytes(file_path))))
            except Exception as e:
                logger.info('****get_metadata, thread_id_out exception ',e)
                result = {"Success": False, "Error": e}
        elif metadata_type.lower().startswith("artifact"):
            parts = metadata_type.split('|')
            if len(parts) != 2:
                raise ValueError(f"Invalid params for artifact metadata: expected 'artifact|<artifact_id>', got {metadata_type}")
            _, artifact_id = parts
            af = get_artifacts_store(db_adapter)
            try:
                m = af.get_artifact_metadata()
                result = {"Success": True, "Metadata": m}
            except Exception as e:
                result = {"Success": False, "Error": e}
        else:
            raise ValueError(
                "Invalid metadata_type provided."
            )

        if result["Success"]:
            output_rows = [[input_rows[0][0], json.loads(result["Data"])]]
        else:
            output_rows = [[input_rows[0][0], {"Success": False, "Message": result["Error"]}]]

    except Exception as e:
        logger.info(f"***** error in metadata: {str(e)}")
        output_rows = [[input_rows[0][0], {"Success": False, "Message": str(e)}]]

    response = make_response({"data": output_rows})
    response.headers["Content-type"] = "application/json"
    logger.debug(f"Sending response: {response.json}")
    return response

@udf_routes.route("/udf_proxy/set_metadata", methods=["POST"])
def set_metadata():
    try:
        message = request.json
        input_rows = message["data"]
        metadata_type = input_rows[0][1]

        if metadata_type.startswith('set_endpoint '):
            metadata_parts = metadata_type.split()
            if len(metadata_parts) == 4:
                group_name = metadata_parts[1].strip()
                endpoint = metadata_parts[2].strip()
                type = metadata_parts[3].strip()
            result = db_adapter.set_endpoint(group_name, endpoint, type)
        elif metadata_type.startswith('api_config_params '):
            metadata_parts = metadata_type.split()
            if len(metadata_parts) > 3:
                service_name = metadata_parts[1].strip()
                key_pairs = " ".join(metadata_parts[2:])
            result = db_adapter.set_api_config_params(service_name, key_pairs)
        elif metadata_type.startswith('set_model_name '):
            model_name, embedding_model_name = metadata_type.split('set_model_name ')[1].split(' ')[:2]
            # model_name = metadata_type.split('set_model_name ')[1].strip()
            # embedding_model_name = metadata_type.split('set_model_name ')[1].strip()
            result = db_adapter.update_model_params(model_name, embedding_model_name)
        else:
            raise ValueError(
                "Invalid metadata_type provided."
            )

        if result["Success"]:
            output_rows = [[input_rows[0][0], json.loads(result["Data"])]]
        else:
            output_rows = [[input_rows[0][0], {"Success": False, "Message": result["Error"]}]]

    except Exception as e:
        logger.info(f"***** error in metadata: {str(e)}")
        output_rows = [[input_rows[0][0], {"Success": False, "Message": str(e)}]]

    response = make_response({"data": output_rows})
    response.headers["Content-type"] = "application/json"
    logger.debug(f"Sending response: {response.json}")
    return response


@udf_routes.route("/udf_proxy/get_artifact", methods=["POST"])
def get_artifact_data():
    """
    Endpoint to retrieve artifact data and metadata.

    It expects a JSON payload containing an 'artifact_id'. The function retrieves
    the metadata and data of the specified artifact from the SnowflakeStageArtifactsStore.

    Returns:
        JSON response containing:
        - Success: A boolean indicating the success of the operation.
        - Metadata: The metadata of the artifact if successful.
        - Data: The base64-encoded data of the artifact if successful.
        - [Message]: An error message if the operation fails.
    """
    try:
        message = request.json
        artifact_id = message.get("artifact_id")
        if not artifact_id:
            return jsonify({"Success": False, "Message": "Missing 'artifact_id' parameter."}), 400

        af = get_artifacts_store(db_adapter)
        # Retrieve artifact metadata
        metadata = af.get_artifact_metadata(artifact_id)
        # Retrieve artifact data into a temporary file and encode it with base64
        with tempfile.TemporaryDirectory() as tmp_dir:
            downloaded_filename = af.read_artifact(artifact_id, tmp_dir)
            with open(Path(tmp_dir)/downloaded_filename, 'rb') as inp:
                artifact_data = base64.b64encode(inp.read()).decode('utf-8')

        response = {
            "Success": True,
            "Metadata": metadata,
            "Data": artifact_data
        }

    except Exception as e:
        response = {
            "Success": False,
            "Message": f"An error occurred while retrieving artifact data: {str(e)}"
        }

    return jsonify(response)



@udf_routes.route("/udf_proxy/get_slack_tokens", methods=["POST"])
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


@udf_routes.route("/udf_proxy/get_ngrok_tokens", methods=["POST"])
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


@udf_routes.route("/udf_proxy/deploy_bot", methods=["POST"])
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


@udf_routes.route("/udf_proxy/set_bot_app_level_key", methods=["POST"])
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


@udf_routes.route("/udf_proxy/configure_slack_app_token", methods=["POST"])
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


@udf_routes.route("/udf_proxy/configure_ngrok_token", methods=["POST"])
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


@udf_routes.route("/udf_proxy/configure_llm", methods=["POST"])
def configure_llm():

    from openai import OpenAI, OpenAIError
    global llm_api_key_struct, sessions, api_app_id_to_session_map, bot_id_to_udf_adapter_map, server
    
    try:

        message = request.json
        input_rows = message["data"]

        llm_type = input_rows[0][1] # llm type means llm engine (e.g. 'cortex', 'openai')
        llm_key_endpoint = input_rows[0][2].split('|')
        llm_key = llm_key_endpoint[0]
        llm_endpoint = llm_key_endpoint[1]

        # llm_key = input_rows[0][2]
        # llm_endpoint = input_rows[0][3]

        if not llm_key or not llm_type:
            response = {
                "Success": False,
                "Message": "Missing LLM API Key or LLM Model Name",
            }
            llm_key = None
            llm_type = None
            llm_endpoint = None

        #  if api_key_from_env:
        #      response = {"Success": False, "Message": "LLM type and API key are set in an environment variable and can not be set or changed using this method."}
        #      llm_key = None
        #      llm_type = None
        else:
        # if llm_type is not None:

            data_cubes_ingress_url = db_adapter.db_get_endpoint_ingress_url("streamlitdatacubes")
            data_cubes_ingress_url = data_cubes_ingress_url if data_cubes_ingress_url else "localhost:8501"
            logger.warning(f"data_cubes_ingress_url(2) set to {data_cubes_ingress_url}")

            # os.environ["OPENAI_API_KEY"] = ''
            # os.environ["REKA_API_KEY"] = ''
            # os.environ["GEMINI_API_KEY"] = ''
            os.environ["CORTEX_MODE"] = 'False'

            if (llm_type.lower() == "openai"):
                os.environ["OPENAI_API_KEY"] = llm_key
                os.environ["AZURE_OPENAI_API_ENDPOINT"] = llm_endpoint
                # logger.info(f"key: {llm_key}, endpoint: {llm_endpoint}")
                try:
                    client = get_openai_client()
                    logger.info(f"client: {client}")
                    completion = client.chat.completions.create(
                        model="gpt-4o",
                        messages=[{"role": "user", "content": "What is 1+1?"}],
                    )
                    # Success!  Update model and keys
                    logger.info(f"completion: {completion}")
                except Exception as e:
                    if "Connection" in str(e):
                        check_eai = " - please ensure the External Access Integration is setup properly."
                    else:
                        check_eai = ""
                    response = {"Success": False, "Message": f"{str(e)}{check_eai}"}
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
            llm_api_key_struct.llm_key = llm_key
            llm_api_key_struct.llm_type = llm_type
            llm_api_key_struct.llm_endpoint = llm_endpoint

            set_key_result = set_llm_key(
                llm_key=llm_key,
                llm_type=llm_type,
                llm_endpoint=llm_endpoint,
            )

            if llm_api_key_struct.llm_key is not None:
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
                flask_app=None,
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

            if set_key_result:
                response = {
                    "Success": True,
                    "Message": "LLM API Key and Model Name configured successfully.",
                }
            else:
                if not response:
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


@udf_routes.route("/udf_proxy/openbb/v1/query", methods=["POST"])
def embed_openbb():
    return openbb_query(
        bot_id_to_udf_adapter_map,
        default_bot_id=list(bot_id_to_udf_adapter_map.keys())[0],
    )

