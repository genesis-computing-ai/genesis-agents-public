import os
from flask import Flask
from core.bot_os import BotOsSession
from core.bot_os_assistant_base import BotOsAssistantTester
from core.bot_os_defaults import ELSA_DATA_ANALYST_INSTRUCTIONS
from core.bot_os_server import BotOsServer
from apscheduler.schedulers.background import BackgroundScheduler
from snowflake.snowflake_bot_os_app import SnowflakeNativeAppAdapter
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

snow_adapter = SnowflakeNativeAppAdapter(
    account_name="demoaccount.aws-east.snowflakecomputing.com"
)
tool1, function_description1 = snow_adapter.create_udf_tool(
    "db1.schemaA.my_favorite_udf_1"
)
tool2, function_description2 = snow_adapter.create_udf_tool(
    "db1.schemaA.my_favorite_udf_2"
)

session = BotOsSession(
    "Elsa_Test",
    instructions=ELSA_DATA_ANALYST_INSTRUCTIONS,
    asistant_implementaion=BotOsAssistantTester,
    input_adapters=[snow_adapter],
    available_functions={"my_favorite_udf_1": tool1, "my_favorite_udf_2": tool2},
    tools=[function_description1, function_description2],
)
app = Flask(__name__)
scheduler = BackgroundScheduler()  # wish we could move this inside BotOsServer
server = BotOsServer(
    app, sessions=[session], scheduler=scheduler, scheduler_seconds_interval=10
)
scheduler.start()

if __name__ == "__main__":
    app.run(port=8080, debug=True)
