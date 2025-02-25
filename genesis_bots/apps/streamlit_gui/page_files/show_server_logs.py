import streamlit as st
import pandas as pd

from utils import get_session
from .components import config_page_header

def show_server_logs():
    config_page_header("Server Logs")

    st.header("Server Status and Logs")

    st.session_state.session = get_session()

    st.markdown("""
    <style>
    .big-font {
        font-size:16px !important;
        font-weight: bold;
    }
    .info-box {
        padding: 20px;
        border-radius: 5px;
        margin-bottom: 20px;
    }
    .log-box {
        background-color: #f0f0f0;
        padding: 20px;
        border-radius: 5px;
        margin-bottom: 20px;
        height: 400px;
        overflow-y: scroll;
    }
    </style>
    """, unsafe_allow_html=True)

    st.markdown('<p class="big-font">View Genesis Server Logs</p>', unsafe_allow_html=True)

    st.write('You can view the logs for the components of the Genesis Server.\nSelect the log type you want to view from the dropdown menu below.')

    # Dropdown for log type selection
    log_type = st.selectbox(
        "Select log type:",
        ["Bot Service", "Harvester", "Task Service", "Knowledge Service"]
    )

    # Get prefix from session state
    prefix = st.session_state.get('prefix', '')
    if not prefix:
        st.error("Application name not found in session state. Please ensure you've completed the setup process.")
        return
    if log_type == "Bot Service":
        service_name = f"{prefix}.GENESISAPP_SERVICE_SERVICE"
        log_name = "genesis"
    elif log_type == "Harvester":
        service_name = f"{prefix}.GENESISAPP_HARVESTER_SERVICE"
        log_name = "genesis-harvester"
    elif log_type == "Task Service":
        service_name = f"{prefix}.GENESISAPP_TASK_SERVICE"
        log_name = "genesis-task-server"
    elif log_type == "Knowledge Service":
        service_name = f"{prefix}.GENESISAPP_KNOWLEDGE_SERVICE"
        log_name = "genesis-knowledge"

    try:
        # Get service status
        status_result = st.session_state.session.sql(
            f"SELECT SYSTEM$GET_SERVICE_STATUS('{service_name}')"
        ).collect()

        # Get service logs
        logs_result = st.session_state.session.sql(
            f"SELECT SYSTEM$GET_SERVICE_LOGS('{service_name}',0,'{log_name}',1000)"
        ).collect()

        # Display the results
        st.markdown(f"<p class='big-font'>Status for {log_type}</p>", unsafe_allow_html=True)
        st.json(status_result[0][0])

        st.markdown(f"<p class='big-font'>Logs for {log_type}</p>", unsafe_allow_html=True)
        st.text(logs_result[0][0])

    # Add a refresh button
        if st.button("Refresh"):
            st.rerun()


    except Exception as e:
        st.error(f"Error retrieving logs: {str(e)}")

    st.info("If you need any assistance interpreting these logs, please check our [documentation](https://genesiscomputing.ai/docs/) or join our [Slack community](https://communityinviter.com/apps/genesisbotscommunity/genesis-bots-community).")