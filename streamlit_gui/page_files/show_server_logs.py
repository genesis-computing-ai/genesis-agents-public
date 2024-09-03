import streamlit as st
import pandas as pd

def show_server_logs():
    st.set_page_config(layout="wide")
    
    st.title("Server Logs")
    
    st.markdown("""
    <style>
    .big-font {
        font-size:20px !important;
        font-weight: bold;
    }
    .info-box {
        background-color: #e1f5fe;
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
    
    st.markdown("""
    <div class="info-box">
    Here you can view the logs for different components of the Genesis Server. 
    Select the log type you want to view from the dropdown menu below.
    </div>
    """, unsafe_allow_html=True)

    # Dropdown for log type selection
    log_type = st.selectbox(
        "Select log type:",
        ["Bot Service", "Harvester", "Task Service", "Knowledge Service"]
    )

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
        st.markdown('<div class="log-box">', unsafe_allow_html=True)
        st.text(logs_result[0][0])
        st.markdown('</div>', unsafe_allow_html=True)

    except Exception as e:
        st.error(f"Error retrieving logs: {str(e)}")

    st.info("If you need any assistance interpreting these logs, please check our [documentation](https://genesiscomputing.ai/docs/) or join our [Slack community](https://communityinviter.com/apps/genesisbotscommunity/genesis-bots-community).")