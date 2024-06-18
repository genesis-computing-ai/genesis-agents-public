import os
import json
from flask import Flask
from core.bot_os import BotOsSession
from core.bot_os_defaults import (
    ELSA_DATA_ANALYST_INSTRUCTIONS,
    ELSA_DATA_ANALYST_INSTRUCTIONS_MISTRAL,
)
from core.bot_os_memory import BotOsKnowledgeAnnoy_Metadata, BotOsKnowledgeLocal
from core.bot_os_server import BotOsServer
from apscheduler.schedulers.background import BackgroundScheduler
from connectors.database_tools import (
    bind_run_query,
    bind_search_metadata,
    database_tool_functions,
)
from slack.slack_bot_os_adapter import SlackBotAdapter
from connectors.bigquery_connector import BigQueryConnector
from llm_mistral.bot_os_mistral import BotOsAssistantMistral
from llm_anthropic.bot_os_anthropic import BotOsAssistantAnthropic
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.ERROR,
    format="%(asctime)s - %(levelname)s - %(filename)s - %(message)s",
)

# Assuming your BigQuery credentials are stored in a JSON file
# Update this path according to your environment variable setup
credentials_path = os.getenv(
    "GOOGLE_APPLICATION_CREDENTIALS", default=".secrets/gcp.json"
)
with open(credentials_path) as f:
    connection_info = json.load(f)

# Initialize the BigQueryConnector with your connection info
bigquery_connector = BigQueryConnector(connection_info, "BigQuery")

# a = bigquery_connector.run_query('SELECT schema_name FROM INFORMATION_SCHEMA.SCHEMATA', max_rows=5)
# print("database test: ", a)

slack_adapter = SlackBotAdapter(
    token=os.getenv("SLACK_APP_TOKEN"),
    signing_secret=os.getenv("SLACK_APP_SIGNING_SECRET"),  # type: ignore
    channel_id=os.getenv("SLACK_CHANNEL"),  # type: ignore
    bot_user_id=os.getenv("SLACK_BOT_USERID"),
)  # type: ignore
# email_adapter = EmailAdapter(account="elsa.1000-helloworld@gmail.com", password=os.getenv("EMAIL_ACCOUNT_PASSWORD"))

session = BotOsSession(
    "Elsa_Anthropic",
    instructions=ELSA_DATA_ANALYST_INSTRUCTIONS_MISTRAL,
    # validation_intructions="Please double check and improve your answer if necessary.",
    input_adapters=[slack_adapter],  # , email_adapter],
    asistant_implementaion=BotOsAssistantAnthropic,
    available_functions={
        "run_query": bind_run_query([bigquery_connector]),
        "search_metadata": bind_search_metadata(
            BotOsKnowledgeAnnoy_Metadata("./kb_vector")
        ),
    },
    tools=database_tool_functions,
    update_existing=True,
)

app = Flask(__name__)

scheduler = BackgroundScheduler(
    {
        "apscheduler.job_defaults.max_instances": 1,
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
    app.run(port=8080, debug=True)
