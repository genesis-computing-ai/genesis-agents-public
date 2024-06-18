import json
import os
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from flask import Flask, request, jsonify
from core.bot_os import BotOsSession
from core.bot_os_defaults import (
    EVE_MOTHER_OF_BOTS_INSTRUCTIONS,
    EVE_VALIDATION_INSTRUCTIONS,
    ESTER_MANAGER_OF_BOTS_INSTRUCTIONS,
)
from core.bot_os_memory import BotOsKnowledgeAnnoy_Metadata
from core.bot_os_server import BotOsServer
from apscheduler.schedulers.background import BackgroundScheduler
from connectors.bigquery_connector import BigQueryConnector
from slack.slack_bot_os_adapter import SlackBotAdapter
from slack.slack_tools import bind_slack_available_functions, slack_tools

app_token = "xapp-1-A06MNQTQT7S-6847571907894-fca147c1daf78e6c6f814bf7979a55945067388146f208a87969db5771f48ae6"
app = App(token=app_token)

import logging

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.WARN, format="%(asctime)s - %(levelname)s - %(message)s"
)

slack_adapter = SlackBotAdapter(
    token=os.getenv("SLACK_APP_TOKEN"),
    signing_secret=os.getenv("SLACK_APP_SIGNING_SECRET"),  # type: ignore
    channel_id=os.getenv("SLACK_CHANNEL"),  # type: ignore
    bot_user_id=os.getenv("SLACK_BOT_USERID"),
)  # type: ignore

credentials_path = os.getenv(
    "GOOGLE_APPLICATION_CREDENTIALS", default=".secrets/gcp.json"
)
with open(credentials_path) as f:
    connection_info = json.load(f)

# Initialize BigQuery client
meta_database_connector = BigQueryConnector(connection_info, "BigQuery")

# Fetch bot configurations for the given runner_id from BigQuery
runner_id = os.getenv("RUNNER_ID", "jl-local-runner")
query = f"""
    SELECT  api_app_id, bot_slack_user_id, bot_id, bot_name, bot_instructions, runner_id, slack_app_token, slack_signing_secret, slack_channel_id
    FROM `hello-prototype.ELSA_INTERNAL.BOT_SERVICING`
    WHERE runner_id = '{runner_id}'
"""
bots_config = meta_database_connector.run_query(query=query)

sessions = []
api_app_id_to_session_map = {}

for bot_config in bots_config:
    slack_adapter = SlackBotAdapter(
        token=bot_config[
            "slack_app_token"
        ],  # This should be the Slack App Token, adjust field name accordingly
        signing_secret=bot_config[
            "slack_signing_secret"
        ],  # Assuming the signing secret is the same for all bots, adjust if needed
        channel_id=bot_config[
            "slack_channel_id"
        ],  # Assuming the channel is the same for all bots, adjust if needed
        bot_user_id=bot_config["bot_slack_user_id"],
    )  # Adjust field name if necessary

    session = BotOsSession(
        bot_config["bot_name"],
        instructions=bot_config["bot_instructions"],
        input_adapters=[slack_adapter],
        knowledgebase_implementation=BotOsKnowledgeAnnoy_Metadata(
            f"./kb_{bot_config['bot_id']}"
        ),
        update_existing=True,
        log_db_connector=BigQueryConnector(
            connection_info, "BigQuery"
        ),  # Ensure connection_info is defined or fetched appropriately
        tools=slack_tools,
        available_functions=bind_slack_available_functions(slack_adapter),
        bot_id=bot_config["bot_id"],
    )

    api_app_id = bot_config[
        "api_app_id"
    ]  # Adjust based on actual field name in bots_config
    api_app_id_to_session_map[api_app_id] = session

    sessions.append(session)


scheduler = BackgroundScheduler(
    {
        "apscheduler.job_defaults.max_instances": 20,
        "apscheduler.job_defaults.coalesce": True,
    }
)
server = BotOsServer(
    app, sessions=sessions, scheduler=scheduler, scheduler_seconds_interval=1
)


@app.message()
def slack_event_handle(message, say):
    # Extract the API App ID from the incoming request
    # request_data = request.json
    # api_app_id = request_data.get('api_app_id')  # Adjust based on your Slack event structure

    api_app_id = "A06MNQTQT7S"

    # Find the session using the API App ID
    session = api_app_id_to_session_map.get(api_app_id)

    if session:
        # If a matching session is found, handle the event
        return session.input_adapters[0].slack_events()
    else:
        # If no matching session, return an error
        return jsonify({"error": "No matching session found"}), 404


scheduler.start()

if __name__ == "__main__":
    # Establishes the connection
    SocketModeHandler(app, app_token).start()
