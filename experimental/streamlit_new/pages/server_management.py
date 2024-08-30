import streamlit as st
from utils.snowflake_connector import check_service_status, start_genesis_service, stop_genesis_service

def server_management_page(session, run_mode):
    st.title("Server Management")

    if run_mode == "local":
        st.info("Running in local mode. Service management is not available.")
        return

    status = check_service_status(session, run_mode)
    st.write(f"Current Genesis Service Status: {status}")

    if status == "NOT_INSTALLED":
        st.error("The Genesis Service is not installed in your Snowflake account.")
        st.info("Please follow the installation instructions to set up the Genesis Service.")
    elif status == "SUSPENDED":
        if st.button("Start Genesis Service"):
            if start_genesis_service(session):
                st.success("Genesis Service started successfully!")
            else:
                st.error("Failed to start Genesis Service.")
    elif status == "READY":
        if st.button("Stop Genesis Service"):
            if stop_genesis_service(session):
                st.success("Genesis Service stopped successfully!")
            else:
                st.error("Failed to stop Genesis Service.")
    elif status in ["ERROR", "UNKNOWN"]:
        st.warning("Unable to determine the current state of the Genesis Service.")
        st.info("Please check your Snowflake account settings and permissions.")
    else:
        st.info(f"The Genesis Service is currently in the {status} state.")
        st.warning("Unable to start/stop service in the current state.")

    # Add more server management functionality as needed