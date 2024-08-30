import streamlit as st
from utils import NativeMode, session, app_name, prefix, core_prefix, check_status
from pages import (
    welcome, chat_page, llm_config, setup_slack, grant_data, db_harvester,
    bot_config, start_stop, show_server_logs, support, config_wh, config_pool,
    config_eai, start_service
)

st.set_page_config(layout="wide")

if "wh_name" not in st.session_state:
    st.session_state["wh_name"] = "XSMALL"

if "last_response" not in st.session_state:
    st.session_state["last_response"] = ""

if NativeMode:
    try:
        service_status_result = check_status()
        if service_status_result != "READY":
            with st.spinner("Waiting on Genesis Services to start..."):
                service_status = st.empty()
                while True:
                    service_status.text("Genesis Service status: " + service_status_result)
                    if service_status_result == "SUSPENDED":
                        if st.button("Click to start Genesis Service"):
                            with st.spinner("Genesis Services is starting..."):
                                try:
                                    session.sql(f"call {app_name}.core.start_app_instance('APP1','GENESIS_POOL','GENESIS_EAI','{st.session_state.wh_name}')")
                                    time.sleep(15)
                                except Exception as e:
                                    st.error(f"Error connecting to Snowflake: {e}")
                    service_status_result = check_status()
                    service_status.text("Genesis Service status: " + service_status_result)
                    if service_status_result == "READY":
                        service_status.text("")
                        st.experimental_rerun()
                    time.sleep(10)

        sql = f"select {prefix}.list_available_bots() "
        st.session_state["data"] = session.sql(sql).collect()

    except Exception as e:
        st.session_state["data"] = None
else:
    st.session_state["data"] = "Local Mode"

if "data" in st.session_state:
    data = st.session_state["data"]
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
    selection = st.sidebar.radio(
        "Go to:",
        list(pages.keys()),
        index=list(pages.keys()).index(st.session_state.get("radio", list(pages.keys())[0])),
    )
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
        index=list(pages.keys()).index(st.session_state.get("radio", list(pages.keys())[0])),
    )
    if selection in pages:
        pages[selection]()