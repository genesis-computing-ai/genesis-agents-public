import os
from flask import Blueprint
from genesis_bots.core.logging_config import logger
from flask import request, jsonify
import requests
from genesis_bots.bot_genesis.make_baby_bot import get_bot_details, update_bot_details
from genesis_bots.demo.sessions_creator import make_session
from genesis_bots.demo.app import genesis_app
from genesis_bots.core import global_flags
from genesis_bots.core.system_variables import SystemVariables

slack_routes = Blueprint('slack_routes', __name__)

@slack_routes.route("/slack/events/<bot_id>/install", methods=["GET"])
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
            db_adapter=genesis_app.db_adapter,
            bot_id_to_udf_adapter_map=genesis_app.bot_id_to_udf_adapter_map,
            stream_mode=True,
        )
        # check new_session
        if new_session is None:
            logger.info("new_session is none")
            return "Error: Not Installed new session is none"
        if slack_adapter_local is not None:
            SystemVariables.bot_id_to_slack_adapter_map[bot_config["bot_id"]] = (
                slack_adapter_local
            )
        if udf_local_adapter is not None:
            genesis_app.bot_id_to_udf_adapter_map[bot_config["bot_id"]] = udf_local_adapter
        genesis_app.api_app_id_to_session_map[api_app_id] = new_session
        #    logger.info("about to add session ",new_session)
        genesis_app.server.add_session(new_session, replace_existing=True)

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


@slack_routes.route("/slack/events", methods=["POST"])
@slack_routes.route("/slack/events/<bot_id>", methods=["POST"])
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
    session = genesis_app.api_app_id_to_session_map.get(api_app_id)

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