from flask import Flask, request, jsonify, make_response
from demo.sessions_creator import create_sessions, make_session
import requests
import json
import logging
import os

from core.bot_os_server import BotOsServer
from auto_ngrok.auto_ngrok import launch_ngrok_and_update_bots
from core.system_variables import SystemVariables
from bot_genesis.make_baby_bot import (
    make_baby_bot,
    update_slack_app_level_key,
    set_llm_key,
    get_ngrok_auth_token,
    set_ngrok_auth_token,
    get_bot_details,
    update_bot_details,
    set_remove_pointers,
    list_all_bots,
    get_slack_config_tokens,
    set_slack_config_tokens,
)
from embed.embed_openbb import openbb_query

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.WARN, format="%(asctime)s - %(levelname)s - %(message)s"
)

import core.global_flags as global_flags


global llm_api_key, api_app_id_to_session_map, bot_id_to_udf_adapter_map, server


def register_routes(
    app: Flask,
    db_adapter,
    scheduler,
    llm_api_key_param,
    bot_id_to_udf_adapter_map_param,
    default_llm_engine_param,
):
    global llm_api_key, default_llm_engine, sessions, api_app_id_to_session_map, bot_id_to_udf_adapter_map, server
    llm_api_key = llm_api_key_param
    bot_id_to_udf_adapter_map = bot_id_to_udf_adapter_map_param
    default_llm_engine = default_llm_engine_param
    server = None

    if llm_api_key is not None:
        (
            sessions,
            api_app_id_to_session_map,
            bot_id_to_udf_adapter_map,
            SystemVariables.bot_id_to_slack_adapter_map,
        ) = create_sessions(
            llm_api_key,
            default_llm_engine,
            db_adapter,
            bot_id_to_udf_adapter_map,
            stream_mode=True,
        )
        server = BotOsServer(
            app,
            sessions=sessions,
            scheduler=scheduler,
            scheduler_seconds_interval=2,
            slack_active=global_flags.slack_active,
        )
        BotOsServer.stream_mode = (
            True  # i think this should be server.stream_mode = True ?
        )
        set_remove_pointers(server, api_app_id_to_session_map)

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

        input_rows = message["data"]
        logger.info(f"Received {len(input_rows)} rows")

        output_rows = [[row[0], "Hi there!"] for row in input_rows]
        logger.info(f"Produced {len(output_rows)} rows")

        response = make_response({"data": output_rows})
        response.headers["Content-type"] = "application/json"
        logger.debug(f"Sending response: {response.json}")
        return response

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
            ngrok_auth_token_display = (
                f"{ngrok_auth_token[:10]}...{ngrok_auth_token[-10:]}"
            )

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
                res = set_ngrok_auth_token(
                    ngrok_auth_token, ngrok_use_domain, ngrok_domain
                )
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
            llm_api_key = None
            message = request.json
            input_rows = message["data"]

            default_llm_engine_candidate = input_rows[0][1]
            llm_api_key_candidate = input_rows[0][2]

            if not llm_api_key_candidate or not llm_api_key_candidate:
                response = {
                    "Success": False,
                    "Message": "Missing LLM API Key or LLM Model Name.",
                }
                llm_api_key_candidate = None
                default_llm_engine_candidate = None

            if default_llm_engine_candidate is not None:
                if default_llm_engine_candidate.lower() == "openai":
                    try:
                        client = OpenAI(api_key=llm_api_key_candidate)

                        completion = client.chat.completions.create(
                            model="gpt-4o-mini",
                            messages=[{"role": "user", "content": "What is 1+1?"}],
                        )
                        # Success!  Update model and keys
                        if llm_api_key != llm_api_key_candidate:
                            default_llm_engine = default_llm_engine_candidate
                            llm_api_key = llm_api_key_candidate
                        else:
                            default_llm_engine_candidate = None

                    except OpenAIError as e:
                        response = {"Success": False, "Message": str(e)}
                        llm_api_key_candidate = None

            if llm_api_key_candidate is not None:
                if (
                    llm_api_key_candidate is not None
                    and default_llm_engine.lower() == "openai"
                ):
                    os.environ["OPENAI_API_KEY"] = llm_api_key_candidate

                if (
                    llm_api_key_candidate is not None
                    and default_llm_engine.lower() == "reka"
                ):
                    os.environ["REKA_API_KEY"] = llm_api_key_candidate

                if bot_id_to_udf_adapter_map:
                    (
                        sessions,
                        api_app_id_to_session_map,
                        bot_id_to_udf_adapter_map,
                        SystemVariables.bot_id_to_slack_adapter_map,
                    ) = create_sessions(
                        llm_api_key,
                        default_llm_engine,
                        db_adapter,
                        bot_id_to_udf_adapter_map,
                        stream_mode=True,
                    )
                    server = BotOsServer(
                        app,
                        sessions=sessions,
                        scheduler=scheduler,
                        scheduler_seconds_interval=2,
                        slack_active=global_flags.slack_active,
                    )
                    BotOsServer.stream_mode = True
                    set_remove_pointers(server, api_app_id_to_session_map)

                    # Assuming 'babybot' is an instance of a class that has the 'set_llm_key' method
                    # and it has been instantiated and imported above in the code.
                    set_key_result = set_llm_key(
                        llm_key=llm_api_key,
                        llm_type=default_llm_engine,
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
            else:
                response = {
                    "Success": False,
                    "Message": "Failed to find global variable bot_id_to_udf_adapter_map.",
                }
        except Exception as e:
            response = {"Success": False, "Message": str(e)}
            return None

        output_rows = [[input_rows[0][0], response]]

        response_var = make_response({"data": output_rows})
        response_var.headers["Content-type"] = "application/json"
        logger.debug(f"Sending response: {response_var.json}")
        return response_var

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
                data={
                    "code": code,
                    "client_id": client_id,
                    "client_secret": client_secret,
                },
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
            new_session, api_app_id, udf_local_adapter, slack_adapter_local = (
                make_session(
                    bot_config=bot_config,
                    db_adapter=db_adapter,
                    bot_id_to_udf_adapter_map=bot_id_to_udf_adapter_map,
                    stream_mode=True,
                )
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

    return server
