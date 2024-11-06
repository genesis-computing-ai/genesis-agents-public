import os
import json
from flask import Flask
from core.bot_os import BotOsSession
from core.bot_os_defaults import ELSA_DATA_ANALYST_INSTRUCTIONS
from core.bot_os_memory import BotOsKnowledgeAnnoy_Metadata, BotOsKnowledgeLocal
from core.bot_os_server import BotOsServer
from apscheduler.schedulers.background import BackgroundScheduler
from connectors.database_tools import (
    bind_run_query,
    bind_search_metadata,
    database_tool_functions,
)

# from slack.slack_bot_os_adapter import SlackBotAdapter
from connectors.bigquery_connector import BigQueryConnector
import streamlit as st
from streamlit_gui.old.streamlit_bot_os_app import BotInputStreamlit
from openai import OpenAI
from streamlit_autorefresh import st_autorefresh

from core.logging_config import logger

if "messages" not in st.session_state:
    st.session_state.messages = []


@st.cache_resource
def setup_adapter():
    streamlit_adapter = BotInputStreamlit("Hi!")
    return streamlit_adapter


streamlit_adapter = setup_adapter()

if "initialized" not in st.session_state:

    # Assuming your BigQuery credentials are stored in a JSON file
    # Update this path according to your environment variable setup
    credentials_path = os.getenv(
        "GOOGLE_APPLICATION_CREDENTIALS", default=".secrets/gcp.json"
    )
    with open(credentials_path) as f:
        connection_info = json.load(f)

    # Initialize the BigQueryConnector with your connection info
    bigquery_connector = BigQueryConnector(connection_info, "BigQuery")

    a = bigquery_connector.run_query(
        "SELECT schema_name FROM INFORMATION_SCHEMA.SCHEMATA", max_rows=5
    )
    logger.info("database test: ", a)

    app = Flask(__name__)

    session = BotOsSession(
        "Elsa_Streamlit",
        instructions=ELSA_DATA_ANALYST_INSTRUCTIONS,
        # validation_intructions="Please double check and improve your answer if necessary.",
        input_adapters=[streamlit_adapter],  # , email_adapter],
        available_functions={
            "run_query": bind_run_query([bigquery_connector]),
            "search_metadata": bind_search_metadata(
                BotOsKnowledgeAnnoy_Metadata("./kb_vector")
            ),
        },
        tools=database_tool_functions,
        update_existing=True,
    )

    scheduler = BackgroundScheduler(
        {
            "apscheduler.job_defaults.max_instances": 1,
            "apscheduler.job_defaults.coalesce": True,
        }
    )  # wish we could move this inside BotOsServer
    server = BotOsServer(
        app, sessions=[session], scheduler=scheduler, scheduler_seconds_interval=5
    )

    scheduler.start()

    # Mark the app as initialized
    st.session_state["initialized"] = True

st.title("Elsa App")

st_autorefresh(interval=1000)

# client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

while len(streamlit_adapter.responses) > 0:
    response = streamlit_adapter.responses.popleft()
    st.session_state.messages.append({"role": "assistant", "content": response})

for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

if prompt := st.chat_input("Enter text..."):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):

        st.markdown(prompt)

        streamlit_adapter.submit_chat_line(prompt)
