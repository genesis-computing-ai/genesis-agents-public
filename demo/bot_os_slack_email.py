import os
import json
from flask import Flask
from core.bot_os import BotOsSession
from core.bot_os_defaults import ELSA_DATA_ANALYST_INSTRUCTIONS
from core.bot_os_server import BotOsServer
from apscheduler.schedulers.background import BackgroundScheduler
from connectors.database_tools import bind_run_query, bind_search_metadata, database_tool_functions
from slack.slack_bot_os_adapter import SlackBotAdapter
from core.bot_os_memory import BotOsKnowledgeAnnoy_Metadata
from connectors.bigquery_connector import BigQueryConnector
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(filename)s - %(message)s')

# Assuming your BigQuery credentials are stored in a JSON file
# Update this path according to your environment variable setup
credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS',default=".secrets/gcp.json")
with open(credentials_path) as f:
    connection_info = json.load(f)

slack_adapter = SlackBotAdapter(token=os.getenv("SLACK_APP_TOKEN"), signing_secret=os.getenv("SLACK_APP_SIGNING_SECRET"), # type: ignore
                                channel_id=os.getenv("SLACK_CHANNEL"), # type: ignore
                                bot_user_id=os.getenv("SLACK_BOT_USERID")) # type: ignore
#email_adapter = EmailAdapter(account="elsa.1000-helloworld@gmail.com", password=os.getenv("EMAIL_ACCOUNT_PASSWORD"))

meta_database_connector = BigQueryConnector(connection_info,'BigQuery')

meta_database_connector.ensure_table_exists()
a = meta_database_connector.run_query('SELECT count(*) from '+meta_database_connector.metadata_table_name, max_rows=5)
print("---> metadata memories in database: ", a)

kb = BotOsKnowledgeAnnoy_Metadata("./kb_vector")

bot_id = os.getenv('BOT_ID',default="local-test-default-bot")
bot_name = os.getenv('BOT_NAME',default="local-test-default-bot-name")

run_query_f = bind_run_query([connection_info])
search_metadata_f = bind_search_metadata("./kb_vector")

available_functions = {}
available_functions["run_query"] =  run_query_f
available_functions["search_metadata"] =  search_metadata_f

session = BotOsSession("Elsa_SE", instructions=ELSA_DATA_ANALYST_INSTRUCTIONS, 
                       #validation_intructions="Please double check and improve your answer if necessary.",
                       input_adapters=[slack_adapter], #, email_adapter],
                       knowledgebase_implementation=kb,
                      # available_functions={"run_query": bind_run_query([connection_info]),
                      #                      "search_metadata": bind_search_metadata("./kb_vector"),},
                       available_functions=available_functions,
                       tools=database_tool_functions,
                       log_db_connector=meta_database_connector,
                       update_existing=True,
                       bot_id=bot_id, bot_name=bot_name,)

app = Flask(__name__)

function_names = {name: func.__name__ for name, func in available_functions.items()}
function_names_str = json.dumps(function_names)
# then to get back:
#function_names_restored = json.loads(function_names_str)


scheduler = BackgroundScheduler({'apscheduler.job_defaults.max_instances': 20, 'apscheduler.job_defaults.coalesce': True}) # wish we could move this inside BotOsServer
server = BotOsServer(app, sessions=[session], scheduler=scheduler, scheduler_seoconds_interval=1)

@app.route('/slack/events',  methods=['POST'])
@app.route('/',              methods=['POST'])
def slack_event_handle():
    return slack_adapter.slack_events() 

scheduler.start()

if __name__ == "__main__":
    app.run(port=8080, debug=True, use_reloader=True)