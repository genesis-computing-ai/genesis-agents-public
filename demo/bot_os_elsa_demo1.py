from flask import Flask
from core.bot_os import BotOsSession
from core.bot_os_input import BotInputAdapterCLI
from core.bot_os_defaults import ELSA_DATA_ANALYST_INSTRUCTIONS
from core.bot_os_memory import BotOsKnowledgeLocal
from core.bot_os_server import BotOsServer
from apscheduler.schedulers.background import BackgroundScheduler

import logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# create local KB
kb = BotOsKnowledgeLocal("./kb")

#kb.reset()

#initial_message = "Hello Elsa.  Search the metadata for data about baseball."
#works

initial_message = "Hello Elsa.  Generate a SQL statement based on my database \
                  metadata to count how many baseball players have appeared in Braves games."

input_adapter = BotInputAdapterCLI(initial_message=initial_message)

session = BotOsSession("Elsa_04", instructions=ELSA_DATA_ANALYST_INSTRUCTIONS, 
                       knowledgebase_implementation=kb,
                       input_adapters=[input_adapter],
                       update_existing=True)
session.create_thread(input_adapter)

app = Flask(__name__)
scheduler = BackgroundScheduler() # wish we could move this inside BotOsServer
server = BotOsServer(app, sessions=[session], scheduler=scheduler, scheduler_seoconds_interval=3)
scheduler.start()

if __name__ == "__main__":
    app.run(port=8080, debug=True)