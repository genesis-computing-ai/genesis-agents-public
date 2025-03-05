from   flask                    import (Blueprint, Response, current_app,
                                        jsonify, make_response, request)
from   genesis_bots.core        import global_flags
from   genesis_bots.core.logging_config \
                                import logger
from   genesis_bots.demo.app    import genesis_app
import os

import json

import base64
from   pathlib                  import Path
import tempfile

from   genesis_bots.core.bot_os_artifacts \
                                import get_artifacts_store
from   genesis_bots.core.bot_os_tools2 \
                                import (add_api_client_tool,
                                        remove_api_client_tool)
from   genesis_bots.embed.embed_openbb \
                                import openbb_query
from   genesis_bots.llm.llm_openai.openai_utils \
                                import get_openai_client

from   genesis_bots.auto_ngrok.auto_ngrok \
                                import launch_ngrok_and_update_bots
from   genesis_bots.bot_genesis.make_baby_bot \
                                import (get_bot_details, get_ngrok_auth_token,
                                        get_slack_config_tokens, list_all_bots,
                                        make_baby_bot, set_llm_key,
                                        set_ngrok_auth_token,
                                        set_slack_config_tokens,
                                        update_slack_app_level_key)
from   genesis_bots.core.bot_os import BotOsSession
from   genesis_bots.core.system_variables \
                                import SystemVariables
from   genesis_bots.demo.routes.slack \
                                import bot_install_followup

from genesis_bots.connectors.data_connector import DatabaseConnector
from genesis_bots.core.tools.project_manager import project_manager

udf_routes = Blueprint('udf_routes', __name__)


@udf_routes.route("/udf_proxy/submit_udf", methods=["POST"])
def submit_udf():
    logger.info('Flask invocation: /udf_proxy/submit_udf')
    message = request.json
    input_rows = message["data"]
    if type(input_rows[0][3]) == str:
        bot_id = json.loads(input_rows[0][3])["bot_id"]
    else:
        bot_id = input_rows[0][3]["bot_id"]
    row = input_rows[0]

    # lookup the adapater and invoke its handler function (the Flask request context will be read there)
    bots_udf_adapter = genesis_app.bot_id_to_udf_adapter_map.get(bot_id, None)
    if bots_udf_adapter is not None:
        return bots_udf_adapter.submit_udf_fn()
    else:
        # TODO LAUNCH
        bot_install_followup(bot_id, no_slack=True)
        bots_udf_adapter = genesis_app.bot_id_to_udf_adapter_map.get(bot_id, None)

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
    logger.debug('Flask invocation: /udf_proxy/lookup_udf')
    message = request.json
    input_rows = message["data"]
    bot_id = input_rows[0][2]

    bots_udf_adapter = genesis_app.bot_id_to_udf_adapter_map.get(bot_id, None)
    if bots_udf_adapter is not None:
        return bots_udf_adapter.lookup_udf_fn()
    else:
        return None


@udf_routes.route("/udf_proxy/list_available_bots", methods=["POST"])
def list_available_bots_fn():
  #  logger.info('Flask invocation: /udf_proxy/list_available_bots')
    message = request.json
    input_rows = message["data"]
    row = input_rows[0]

    output_rows = []
    if genesis_app.llm_api_key_struct.llm_key is None:
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


@udf_routes.route("/udf_proxy/create_baby_bot", methods=["POST"])
def create_baby_bot():
    """
    Endpoint to create a new 'baby bot' using the Genesis server.

    This endpoint expects a JSON payload with the following structure:
    {
        "data": {
            "bot_name": <bot_name>,
            "bot_implementation": <bot_implementation>,
            "bot_id": <bot_id>,
            "files": <files>,
            "available_tools": <available_tools>,
            "bot_instructions": <bot_instructions>
        }
    }

    Returns:
        A JSON response indicating success or failure of the bot creation process.
        On success, the response will include the data returned by the bot creation process.
        On failure, the response will include an error message.
    """
   # logger.info('Flask invocation: /udf_proxy/create_baby_bot')
    try:
        data = request.get_json()['data']
        # TODO: validate the json schema

        bot_name = data.get("bot_name")
        bot_implementation = data.get("bot_implementation")
        bot_id = data.get("bot_id")
        files = data.get("files")
        available_tools = data.get("available_tools")
        bot_instructions = data.get("bot_instructions")

        if not bot_name or not bot_implementation:
            return make_response({"Success": False, "Message": "Bot name and implementation are required"}), 400

        result = genesis_app.server.make_baby_bot_wrapper(
            bot_id=bot_id,
            bot_name=bot_name,
            bot_implementation=bot_implementation,
            files=files,
            available_tools=available_tools,
            bot_instructions=bot_instructions
        )
        if result:
            return make_response({"Success": True, "Data": 'Bot created successfully'}), 200
        else:
            return make_response({"Success": False, "Message": "Bot creation failed"}), 500

    except Exception as e:
        logger.error(f"Error in create_baby_bot: {str(e)}")
        return make_response({"Success": False, "Message": str(e)}), 500



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
            result = genesis_app.db_adapter.get_harvest_control_data_as_json()
        elif metadata_type == "project_dashboard":
            pass
        elif metadata_type == "db_connections":
            db_connector = DatabaseConnector()
            db_result = db_connector.list_database_connections(bot_id = '', bot_id_override=True)
            # Convert the result to JSON string if it's successful
            if db_result["success"]:
                result = {"Success": True, "Data": json.dumps(db_result["connections"])}
            else:
                result = {"Success": False, "Error": db_result.get("Error", "Unknown error")}
        elif metadata_type == "check_db_source":
            metadata_response = os.getenv("SNOWFLAKE_METADATA", "False").upper() == "TRUE"
            result = {"Success": True, "Data": json.dumps(metadata_response)}
        elif metadata_type == "harvest_summary":
            result = genesis_app.db_adapter.get_harvest_summary()
        elif metadata_type == "available_databases":
            result = genesis_app.db_adapter.get_available_databases()
        elif metadata_type == "bot_images":
            result = genesis_app.db_adapter.get_bot_images()
        elif metadata_type == "llm_info":
            result = genesis_app.db_adapter.get_llm_info()
        elif metadata_type == 'cortex_search_services':
            result = genesis_app.db_adapter.get_cortex_search_service()
        elif metadata_type.startswith('list_projects '):
            bot_id = metadata_type.split('list_projects ')[1].strip()
            result = {"Success": True, "Data": json.dumps(project_manager.manage_projects(
                action="LIST",
                bot_id=bot_id,
                thread_id=None
            ))}
        elif metadata_type.startswith('list_todos '):
            project_id = metadata_type.split('list_todos ')[1].strip()
            result = {"Success": True, "Data": json.dumps(project_manager.get_project_todos(
                bot_id=None,  # Not needed for listing todos
                project_id=project_id
            ))}
        elif metadata_type.startswith('list_todo_history '):
            todo_id = metadata_type.split('list_todo_history ')[1].strip()
            result = {"Success": True, "Data": json.dumps(project_manager.get_todo_history(
                todo_id=todo_id
            ))}
        elif metadata_type.startswith('list_project_artifacts '):
            project_id = metadata_type.split('list_project_artifacts ')[1].strip()
            result = {"Success": True, "Data": json.dumps(project_manager.manage_project_assets(
                action="LIST",
                bot_id=None,  # Not needed for listing assets
                project_id=project_id
            ))}
        elif metadata_type == "bot_llms":
            if "BOT_LLMS" in os.environ and os.environ["BOT_LLMS"]:
                result = {"Success": True, "Data": os.environ["BOT_LLMS"]}
            else:
                result = {"Success": False, "Error": "Environment variable not set"}
        elif metadata_type.startswith('test_email '):
            email = metadata_type.split('test_email ')[1].strip()
            result = genesis_app.db_adapter.send_test_email(email)
        elif metadata_type.startswith('get_email'):
            result = genesis_app.db_adapter.get_email()
        elif metadata_type.startswith('check_eai '):  # Check for site-specific EAI first
            metadata_parts = metadata_type.split()
            if len(metadata_parts) == 2:
                site = metadata_parts[1].strip()
            else:
                logger.info("get_metadata - missing metadata")
            result = genesis_app.db_adapter.eai_test(site=site)
        elif metadata_type == 'check_eai_assigned':  # Then check for exact match
            result = genesis_app.db_adapter.check_eai_assigned()
        elif metadata_type.startswith('get_endpoints'):
            result = genesis_app.db_adapter.get_endpoints()
        elif metadata_type.startswith('delete_endpoint_group '):
            metadata_parts = metadata_type.split()
            if len(metadata_parts) == 2:
                group_name = metadata_parts[1].strip()
            else:
                logger.info("missing group name to delete")
            result = genesis_app.db_adapter.delete_endpoint_group(group_name)
        elif metadata_type.startswith('set_endpoint '):
            metadata_parts = metadata_type.split()
            if len(metadata_parts) == 4:
                group_name = metadata_parts[1].strip()
                endpoint = metadata_parts[2].strip()
                type = metadata_parts[3].strip()
            result = genesis_app.db_adapter.set_endpoint(group_name, endpoint, type)
        elif metadata_type.startswith('set_model_name '):
            model_name, embedding_model_name = metadata_type.split('set_model_name ')[1].split(' ')[:2]
            # model_name = metadata_type.split('set_model_name ')[1].strip()
            # embedding_model_name = metadata_type.split('set_model_name ')[1].strip()
            result = genesis_app.db_adapter.update_model_params(model_name, embedding_model_name)
        elif metadata_type.startswith('logging_status'):
            status = genesis_app.db_adapter.check_logging_status()
            result = {"Success": True, "Data": status}
        elif 'sandbox' in metadata_type:
            _, bot_id, thread_id_in, file_name = metadata_type.split('|')
            logger.info('****get_metadata, file_name', file_name)
            logger.info('****get_metadata, thread_id_in', thread_id_in)
            logger.info('****get_metadata, bot_id', bot_id)
            try:
                thread_id_out = BotOsSession._shared_in_to_out_thread_map.get(bot_id, {}).get(thread_id_in)
                if not thread_id_out:
                    result = {"Success": False, "Error": 'Thread id not found'}
                else:
                    logger.info('****get_metadata, thread_id_out', thread_id_out)
                    file_path = f'./runtime/downloaded_files/{thread_id_out}/{file_name}'
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
            af = get_artifacts_store(genesis_app.db_adapter)
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
        logger.info(f"get_metadata - Error in metadata: {str(e)}")
        output_rows = [[input_rows[0][0], {"Success": False, "Message": str(e)}]]

    response = make_response({"data": output_rows})
    response.headers["Content-type"] = "application/json"
    # logger.info(f"get_metadata - Sending response: {response.json}")
    return response


@udf_routes.route("/udf_proxy/set_metadata", methods=["POST"])
def set_metadata():
    try:
        message = request.json
        input_rows = message["data"]
        metadata_type = input_rows[0][1]

        # print('=====')
        # print('message: ', message)
        # print('input_rows: ', input_rows)
        # print('metadata_type: ', metadata_type)

        if metadata_type.startswith('add_todo '):
            # Split on first 3 spaces to get PROJECT_ID, BOT_ID, TODO_NAME
            parts = metadata_type.split(' ', 3)
            if len(parts) < 4:
                raise ValueError("add_todo requires PROJECT_ID BOT_ID TODO_NAME WHAT_TO_DO")
            
            _, project_id, bot_id, rest = parts
            # Split remaining text on first space to separate TODO_NAME from WHAT_TO_DO
            todo_parts = rest.split(' ', 1)
            if len(todo_parts) < 2:
                raise ValueError("add_todo requires both TODO_NAME and WHAT_TO_DO")
                
            todo_name, what_to_do = todo_parts

            # URL decode todo_name if it's URL encoded
            try:
                from urllib.parse import unquote
                todo_name = unquote(todo_name)
            except Exception as e:
                logger.warning(f"Failed to URL decode todo_name: {str(e)}")
            
            # Call project manager to add todo
            todo_details = {
                "project_id": project_id,
                "todo_name": todo_name,
                "what_to_do": what_to_do,
                "assigned_to_bot_id": bot_id
            }
            result = project_manager.manage_todos(
                action="CREATE",
                bot_id=bot_id,
                todo_details=todo_details,
                thread_id=None
            )

        elif metadata_type.startswith('create_project '):
            # Split on first 3 spaces to get BOT_ID, PROJECT_NAME, DESCRIPTION
            parts = metadata_type.split(' ', 3)
            if len(parts) < 4:
                raise ValueError("create_project requires BOT_ID PROJECT_NAME DESCRIPTION")
            
            _, bot_id, project_name, description = parts

            # URL decode project_name if it's URL encoded
            try:
                from urllib.parse import unquote
                project_name = unquote(project_name)
            except Exception as e:
                logger.warning(f"Failed to URL decode project_name: {str(e)}")

            # Call project manager to create project
            project_details = {
                "project_name": project_name,
                "description": description,
                "requested_by_user": bot_id
            }
            result = project_manager.manage_projects(
                action="CREATE",
                bot_id=bot_id,
                project_details=project_details
            )



        elif metadata_type.startswith('set_endpoint '):
            metadata_parts = metadata_type.split()
            if len(metadata_parts) == 4:
                group_name = metadata_parts[1].strip()
                endpoint = metadata_parts[2].strip()
                type = metadata_parts[3].strip()
            result = genesis_app.db_adapter.set_endpoint(group_name, endpoint, type)
        elif metadata_type.startswith('ngrok '):
            ngrok_auth_key = metadata_type.split('ngrok ')[1].strip()
            os.environ["NGROK_AUTH_TOKEN"] = ngrok_auth_key

            # Store the token in Snowflake using db_set_ngrok_auth_token
            db_response = genesis_app.db_adapter.db_set_ngrok_auth_token(
                ngrok_auth_token=ngrok_auth_key,
                ngrok_use_domain='N',
                ngrok_domain='',
                project_id=genesis_app.db_adapter.genbot_internal_project_and_schema.split('.')[0],
                dataset_name=genesis_app.db_adapter.genbot_internal_project_and_schema.split('.')[1]
            )
            if not db_response:
                result = {"Success": False, "Data": json.dumps({"Message": "Failed to store NGROK token in database"})}
            else:
                # Start ngrok with the new token
                ngrok_active = genesis_app.run_ngrok()
                message = "NGROK auth token set successfully and ngrok " + ("activated" if ngrok_active else "failed to activate")
                result = {"Success": True, "Data": json.dumps({"Message": message})}
        elif metadata_type.startswith('api_config_params '):
            metadata_parts = metadata_type.split()
            if len(metadata_parts) > 3:
                service_name = metadata_parts[1].strip()
                key_pairs = " ".join(metadata_parts[2:])
            result = genesis_app.db_adapter.set_api_config_params(service_name, key_pairs)
        elif metadata_type.startswith('set_model_name '):
            model_name, embedding_model_name = metadata_type.split('set_model_name ')[1].split(' ')[:2]
            # model_name = metadata_type.split('set_model_name ')[1].strip()
            # embedding_model_name = metadata_type.split('set_model_name ')[1].strip()
            result = genesis_app.db_adapter.update_model_params(model_name, embedding_model_name)
        else:
            raise ValueError(
                "Invalid metadata_type provided."
            )



        if result.get("Success", False) or result.get("success", False):
            if "Data" not in result:
                output_rows = [[input_rows[0][0], result]]
            else:
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
    artifact_id = None
    is_udf_proxy_mode = False
    try:
        message = request.json
        # extract the artifact_id from the request. We handle either a direct REST call or a UDF proxy call
        if "artifact_id" in message: # direct REST call
            artifact_id = message.get("artifact_id")
        elif "data" in message: # UDF proxy call
            input_rows = message["data"]
            if len(input_rows) == 1 and len(input_rows[0]) == 2: # UDF proxy call will look like this: [[0, artifact_id]]
                artifact_id = input_rows[0][1]
                is_udf_proxy_mode = True
        if not artifact_id:
            logger.warning(f"/udf_proxy/get_artifact - Could not resolve 'artifact_id' from. payload: {message}")
            return jsonify({"Success": False, "Message": f"Missing 'artifact_id' or 'data' parameter. payload: {message}"}), 400
        logger.info(f"/udf_proxy/get_artifact - artifact_id: {artifact_id}")
        af = get_artifacts_store(genesis_app.db_adapter)
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
        logger.error(f"/udf_proxy/get_artifact - Error: {str(e)}")
        response = {
            "Success": False,
            "Message": f"An error occurred while retrieving artifact data: {str(e)}"
        }
    logger.info(f"/udf_proxy/get_artifact - Response: Success: {response['Success']}, "
                f"mime type: {response['Metadata'].get('mime_type')}, "
                f"Data len: {len(response['Data'])} bytes")
    if not is_udf_proxy_mode:
        # we were called in simple REST mode.
        return jsonify(response)
    else:
        # we were called in UDF proxy mode. Snowflake is expecting a certain output format for UDF calls
        output_rows = [[input_rows[0][0], response]]
        response = make_response({"data": output_rows})
        response.headers["Content-type"] = "application/json"
        return response


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

        else:

            os.environ["CORTEX_MODE"] = 'False'

            if (llm_type.lower() == "openai"):
                os.environ["OPENAI_API_KEY"] = llm_key
                os.environ["AZURE_OPENAI_API_ENDPOINT"] = llm_endpoint
                # logger.info(f"key: {llm_key}, endpoint: {llm_endpoint}")

                try:
                    # Get current model params
                    model_params = genesis_app.db_adapter.get_model_params()
                    if model_params["Success"] and llm_endpoint is not None and llm_endpoint != "":
                        model_data = json.loads(model_params["Data"])
                        model_name = model_data.get("model_name", "gpt-4o")
                    else:
                        model_name = "gpt-4o"

                    client = get_openai_client()
                    logger.info(f"client: {client}")
                    completion = client.chat.completions.create(
                        model=model_name,
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
            genesis_app.llm_api_key_struct.llm_key = llm_key
            genesis_app.llm_api_key_struct.llm_type = llm_type
            genesis_app.llm_api_key_struct.llm_endpoint = llm_endpoint

            set_key_result = set_llm_key(
                llm_key=llm_key,
                llm_type=llm_type,
                llm_endpoint=llm_endpoint,
            )

            genesis_app.create_app_sessions(llm_change=True)
            genesis_app.start_server()

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
        genesis_app.bot_id_to_udf_adapter_map,
        default_bot_id=list(genesis_app.bot_id_to_udf_adapter_map.keys())[0],
    )


@udf_routes.route("/udf_proxy/register_client_tool", methods=["POST"])
def register_client_tool():
    """
    Endpoint to add a client tool function dynamically, for a specific bot.

    Returns:
        A JSON response indicating success or failure of the tool function registration.
    """
   # logger.info('Flask invocation: /udf_proxy/register_client_tool')
    try:
        # Parse the JSON payload
        data = request.get_json()
        bot_id = data.get("bot_id")
        tool_func_descriptor = data.get("tool_func_descriptor")
        timeout_seconds = data.get("timeout_seconds")
        if not bot_id or not tool_func_descriptor:
            raise ValueError("Both 'bot_id' and 'tool_func_descriptor' are required.")

        # Delegate the core logic to the bot_os_tools2 module
        response = add_api_client_tool(bot_id, tool_func_descriptor, genesis_app.server, timeout_seconds)

    except Exception as e:
        logger.error(f"Error adding client tool: {str(e)}")
        response = {
            "Success": False,
            "Message": f"An error occurred while adding the client tool: {str(e)}"
        }

    return jsonify(response)


@udf_routes.route("/udf_proxy/unregister_client_tool", methods=["POST"])
def unregister_client_tool():
    """
    Endpoint to remove a client tool function dynamically, for a specific bot or all bots.

    Returns:
        A JSON response indicating success or failure of the tool function unregistration.
    """
    try:
        # Parse the JSON payload
        data = request.get_json()
        bot_id = data.get("bot_id")
        if not bot_id:
            raise ValueError("'bot_id' is required.")
        tool_name = data.get("tool_name")
        if not tool_name:
            raise ValueError("'tool_name' is required.")

        # Delegate the core logic to the bot_os_tools2 module
        response = remove_api_client_tool(bot_id, tool_name, genesis_app.server)

    except Exception as e:
        logger.error(f"Error removing client tool: {str(e)}")
        response = {
            "Success": False,
            "Message": f"An error occurred while removing the client tool: {str(e)}"
        }

    return jsonify(response)


@udf_routes.route("/udf_proxy/endpoint_router", methods=["POST"])
def endpoint_router():
    try:
        # Parse the incoming JSON request
        message = request.json
        if message is None:
            logger.error("Invalid JSON payload")
            raise ValueError("Invalid JSON payload")

        op_name = None
        endpoint_name = None
        payload_str = None
        headers = None
        use_udf_proxy_format = False

        if "data" in message:
            input_rows = message["data"]
            if len(input_rows) != 1:
                logger.error(f"/udf_proxy/endpoint_router: Expected 1 row, got {len(input_rows)}")
                raise ValueError(f"/udf_proxy/endpoint_router: Expected 1 row, got {len(input_rows)}")
            params_arr = input_rows[0]
            if len(params_arr) == 3:
                _, op_name, endpoint_name = params_arr
            elif len(params_arr) == 4:
                _, op_name, endpoint_name, payload_str = params_arr
            else:
                logger.error(f"/udf_proxy/endpoint_router: Expected 3 or 4 params, got {len(params_arr)}")
                raise ValueError(f"/udf_proxy/endpoint_router: Expected 3 or 4 params, got {len(params_arr)}")
            use_udf_proxy_format = True
            logger.debug(f"Using UDF proxy format with params: op={op_name}, endpoint={endpoint_name}, payload={payload_str[:100] if payload_str else None}")
        else:
            op_name = message.get("op_name")
            endpoint_name = message.get("endpoint_name")
            headers = message.get("headers")
            payload_str = message.get("payload")
            logger.debug(f"Using direct format with params: op={op_name}, endpoint={endpoint_name}, headers={headers}")

        op_name = op_name.lower()
        headers = headers or {"Content-Type": "application/json"}

        payload = None
        if payload_str:
            try:
                payload = json.loads(payload_str)
                logger.debug(f"Successfully parsed payload JSON: {str(payload)[:100]}...")
            except json.JSONDecodeError:
                logger.error(f"Failed to parse payload JSON: {payload_str[:100]}")
                raise ValueError("Invalid JSON in payload")

        if op_name not in ["post", "get", "put", "delete"]:
            logger.error(f"Invalid operation name: {op_name}")
            raise ValueError("Invalid operation name")

        flask_app = current_app
        rule = next((r for r in flask_app.url_map.iter_rules() if r.rule == endpoint_name), None)
        if not rule:
            error_msg = f"Endpoint name {endpoint_name} is not registered with Flask app {flask_app.name}"
            logger.error(error_msg)
            return jsonify({"Success": False, "Message": error_msg}), 400

        endpoint_func = flask_app.view_functions[rule.endpoint]
        logger.debug(f"Found endpoint function: {endpoint_func.__name__}")

        # Call the endpoint
        with flask_app.test_request_context(endpoint_name, method=op_name.upper(), json=payload):
            response = endpoint_func()
            logger.debug(f"Raw response from endpoint: type={type(response)}, value={str(response)[:200]}")

        if use_udf_proxy_format:
            logger.debug("Converting response to UDF proxy format")
            # convert the response to the format expected by a caller usind a UDF:
            #  {"data": [
            #      [row_id, col1_vlaue, col2_value, ...],
            #      ...
            #  ]}
            # Handle different response types
            if isinstance(response, tuple):
                logger.debug(f"Handling tuple response with length {len(response)}: {response}")
                # Response with status code
                response_data = response[0]
                if isinstance(response_data, Response):
                    logger.debug("Tuple contains Response object, extracting JSON")
                    response_data = response_data.get_json(force=True, silent=True)
                elif isinstance(response_data, str):
                    logger.debug("Tuple contains string, parsing as JSON")
                    response_data = json.loads(response_data)
                elif isinstance(response_data, dict):
                    logger.debug("Tuple contains dictionary, using directly")
                else:
                    logger.debug(f"Tuple contains unexpected type: {type(response_data)}")
                    response_data = {"data": response_data}
            elif isinstance(response, Response):
                logger.debug("Handling Flask Response object")
                # Flask Response object
                response_data = response.get_json(force=True, silent=True)
                logger.debug(f"Extracted JSON from Response: {response_data}")
            else:
                logger.debug(f"Handling direct return value of type {type(response)}")
                # Direct return value
                if isinstance(response, dict):
                    response_data = response
                elif isinstance(response, str):
                    try:
                        response_data = json.loads(response)
                    except json.JSONDecodeError:
                        response_data = {"data": response}
                else:
                    response_data = {"data": response}

            logger.debug(f"Final response_data before formatting: {response_data}")
            output_rows = [[0, response_data]]
            response = make_response({"data": output_rows})
            response.headers['Content-type'] = 'application/json'
            logger.debug("Successfully created UDF proxy format response")

        logger.debug(f"Flask endpoint_router: Final response from {endpoint_name}: {response}")
        return response

    except Exception as e:
        logger.error(f"Error in endpoint_router: {str(e)}")
        logger.error(f"Exception type: {type(e)}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        error_response = {"Success": False, "Message": str(e)}
        if use_udf_proxy_format:
            logger.debug("Returning error in UDF proxy format")
            return make_response({"data": [[0, error_response]]})
        logger.debug("Returning error in direct format")
        return jsonify(error_response), 500




