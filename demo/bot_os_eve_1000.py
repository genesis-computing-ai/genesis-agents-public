import json
import os
from flask import Flask
from core.bot_os import BotOsSession
from core.bot_os_defaults import (
    EVE_MOTHER_OF_BOTS_INSTRUCTIONS,
    EVE_VALIDATION_INSTRUCTIONS,
)
from core.bot_os_memory import BotOsKnowledgeAnnoy_Metadata
from core.bot_os_server import BotOsServer
from apscheduler.schedulers.background import BackgroundScheduler
from connectors.bigquery_connector import BigQueryConnector
from slack.slack_bot_os_adapter import SlackBotAdapter
from slack.slack_tools import bind_slack_available_functions, slack_tools
from development.integration_tools import (
    integration_tool_descriptions,
    integration_tools,
)
from generated_modules.webpage_downloader import (
    webpage_tools,
    TOOL_FUNCTION_DESCRIPTION_WEBPAGE_DOWNLOADER,
)
from generated_modules.vision_chat_analysis import (
    vision_chat_analysis_action_function_mapping,
    TOOL_FUNCTION_DESCRIPTION_VISION_CHAT_ANALYSIS,
)
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

session = BotOsSession(
    "Eve 1000",
    instructions=EVE_MOTHER_OF_BOTS_INSTRUCTIONS,
    # validation_intructions=EVE_VALIDATION_INSTRUCTIONS,
    input_adapters=[slack_adapter],  # , email_adapter],
    knowledgebase_implementation=BotOsKnowledgeAnnoy_Metadata("./kb_eve_1000"),
    update_existing=True,
    log_db_connector=BigQueryConnector(connection_info, "BigQuery"),
    tools=slack_tools
    + integration_tool_descriptions
    + [
        TOOL_FUNCTION_DESCRIPTION_WEBPAGE_DOWNLOADER,
        TOOL_FUNCTION_DESCRIPTION_VISION_CHAT_ANALYSIS,
    ],
    available_functions=bind_slack_available_functions(slack_adapter)
    | integration_tools
    | webpage_tools
    | vision_chat_analysis_action_function_mapping,
    bot_id=os.getenv("BOT_ID", default="local-test-default-bot"),
    bot_name=os.getenv("BOT_NAME", default="local-test-default-bot-name"),
)
session.add_task(
    "Check in with your manager to see if they have tasks for you to work on.",
    # thread_id=session.create_thread(slack_adapter))
    input_adapter=slack_adapter,
)
app = Flask(__name__)

scheduler = BackgroundScheduler(
    {
        "apscheduler.job_defaults.max_instances": 20,
        "apscheduler.job_defaults.coalesce": True,
    }
)  # wish we could move this inside BotOsServer
server = BotOsServer(
    app, sessions=[session], scheduler=scheduler, scheduler_seconds_interval=1
)


@app.route("/slack/events", methods=["POST"])
@app.route("/", methods=["POST"])
def slack_event_handle():
    return slack_adapter.slack_events()


scheduler.start()

if __name__ == "__main__":
    app.run(port=8080, debug=True, use_reloader=True)
