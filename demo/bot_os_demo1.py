from flask import Flask
from core.bot_os import BotOsSession
from core.bot_os_input import BotInputAdapterCLI
from core.bot_os_defaults import ELSA_DATA_ANALYST_INSTRUCTIONS
from core.bot_os_memory import BotOsKnowledgeLocal
from core.bot_os_reminders import RemindersTest
from core.bot_os_server import BotOsServer
from apscheduler.schedulers.background import BackgroundScheduler

import logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# create local KB
kb = BotOsKnowledgeLocal("./kb")

kb.reset()  # if you accidently remove the database crawl, its in /kb_with_crawl as a backup.. Would like to make reset() have a scope option.

kb.store_memory("My name is Matt.", scope="user_preferences")
kb.store_memory("I live in NYC", scope="user_preferences")
kb.store_memory("The world is flat", scope="general")
kb.store_memory("The moon is made of cheese", scope="general")

#initial_message = "Hello Elsa. Can you please introduce yourself?"
#initial_message = "Hello Elsa. Can you remind me to call my mom in 1 minute from now?"
initial_message = "Hello Elsa.  Do you know my name and what the moon is made of?"


input_adapter = BotInputAdapterCLI(initial_message=initial_message)

session = BotOsSession("Elsa_03", instructions=ELSA_DATA_ANALYST_INSTRUCTIONS, 
                       #+ "Please incorporate the attached file and try to incorporate the style of Michael Gold.", files=["~/Downloads/Takeout/Mail/All mail Including Spam and Trash.mbox"]
                       validation_intructions="Please double check and improve your answer if necessary.",
                       knowledgebase_implementation=kb,
                       reminder_implementation=RemindersTest,
                       input_adapters=[input_adapter],
                       update_existing=True)
session.create_thread(input_adapter)

app = Flask(__name__)
scheduler = BackgroundScheduler() # wish we could move this inside BotOsServer
server = BotOsServer(app, sessions=[session], scheduler=scheduler, scheduler_seoconds_interval=3)
scheduler.start()

if __name__ == "__main__":
    app.run(port=8080, debug=True)