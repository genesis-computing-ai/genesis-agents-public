import streamlit as st
from utils import NativeMode, check_status, get_session, app_name, prefix
from pages.welcome import welcome
from pages.chat_page import chat_page
from pages.llm_config import llm_config
from pages.setup_slack import setup_slack
from pages.grant_data import grant_data
from pages.db_harvester import db_harvester
from pages.bot_config import bot_config
from pages.start_stop import start_stop
from pages.show_server_logs import show_server_logs
from pages.support import support
from pages.config_wh import config_wh
from pages.config_pool import config_pool
from pages.config_eai import config_eai
from pages.start_service import start_service

# Initialize data in session state if it doesn't exist
if 'data' not in st.session_state:
    st.session_state.data = None

# ... (keep the initialization code)

if NativeMode:
    try:
        service_status_result = check_status()
        if service_status_result is None:
            st.session_state["data"] = "Local Mode"
        # ... (keep the service status checking code)
    except Exception as e:
        st.session_state["data"] = None
else:
    st.session_state["data"] = "Local Mode"

if st.session_state.data:
    pages = {
        "Chat with Bots": chat_page,
        "LLM Model & Key": llm_config,
        "Setup Slack Connection": setup_slack,
        "Grant Data Access": grant_data,
        "Harvester Status": db_harvester,
        "Bot Configuration": bot_config,
        "Server Stop/Start": start_stop,
        "Server Logs": show_server_logs,
        "Support and Community": support,
    }

    if st.session_state.get("needs_keys", False):
        del pages["Chat with Bots"]

    st.sidebar.title("Genesis Bots Configuration")
    
    # Set the default selection to "Chat with Bots"
    default_selection = "Chat with Bots" if "Chat with Bots" in pages else list(pages.keys())[0]
    
    selection = st.sidebar.radio(
        "Go to:",
        list(pages.keys()),
        index=list(pages.keys()).index(
            st.session_state.get("radio", default_selection)
        ),
    )
    
    # Update the session state with the current selection
    st.session_state["radio"] = selection
    
    if selection in pages:
        pages[selection]()

else:
    pages = {
        "Welcome!": welcome,
        "1: Configure Warehouse": config_wh,
        "2: Configure Compute Pool": config_pool,
        "3: Configure EAI": config_eai,
        "4: Start Genesis Server": start_service,
        "Support and Community": support,
    }

    st.sidebar.title("Genesis Bots Installation")
    selection = st.sidebar.radio(
        "Go to:",
        list(pages.keys()),
        index=list(pages.keys()).index(
            st.session_state.get("radio", list(pages.keys())[0])
        ),
    )
    if selection in pages:
        pages[selection]()