import time
import streamlit as st
from utils import (get_session, get_metadata)
import snowflake.permissions as permissions


def config_logging():
    session = get_session()
    if not session:
        st.error("Unable to connect to Snowflake. Please check your connection.")
        # return
        pass

    st.title("Configure Application Event Logging")

    st.markdown("""
    <style>
    .big-font {
        font-size:20px !important;
        font-weight: bold;
    }
    .info-box {
        padding: 20px;
        border-radius: 5px;
        margin-bottom: 20px;
    }
    .code-box {
        background-color: #f0f0f0;
        padding: 20px;
        border-radius: 5px;
        margin-bottom: 20px;
    }
    </style>
    """, unsafe_allow_html=True)

    st.markdown('<p class="big-font">Why event logging?</p>', unsafe_allow_html=True)

    st.markdown("""
    <div class="info-box">
    The Genesis Server can capture and output events to a Snowflake Event Table, allowing you to track what is happening inside the server. Optionally, these logs can be shared back to the Genesis Provider for enhanced support for your GenBots.
    </div>
    """, unsafe_allow_html=True)

    try:
        # Execute the command and collect the results
        check_status_result = get_metadata('logging_status')
        # if permissions.is_event_sharing_enabled():
        if check_status_result == True:
            st.markdown("""
            Logging already enabled and shared with Genesis.
            """)
        else:
            if st.session_state.NativeMode:
                import snowflake.permissions as permissions
                permissions.request_event_sharing()
            st.markdown('<p class="big-font">Configuration Steps</p>', unsafe_allow_html=True)

            st.markdown("""
            Please go to a Snowflake worksheet and run these commands to create an event logging table and optionally share the events back to the Genesis provider.
            """)

            wh_text = f"""-- select authorized role to use

            -- set the name of the installed application
            set APP_DATABASE = '{st.session_state.get("app_name", "")}';

            -- (optional steps for event logging)
            -- create a schema to hold the event table
            CREATE SCHEMA IF NOT EXISTS GENESIS_LOCAL_DB.EVENTS;

            -- create an event table to capture events from the Genesis Server
            CREATE EVENT TABLE  IF NOT EXISTS GENESIS_LOCAL_DB.EVENTS.GENESIS_APP_EVENTS;

            -- set the event table on your account, this is optional
            -- this requires an authorized role, and may already be set, skip if it doesnt work
            ALTER ACCOUNT SET EVENT_TABLE=GENESIS_LOCAL_DB.EVENTS.GENESIS_APP_EVENTS;

            -- allow sharing of the captured events with the Genesis Provider
            -- optional, skip if it doesn't work
            ALTER APPLICATION IDENTIFIER($APP_DATABASE) SET SHARE_EVENTS_WITH_PROVIDER = TRUE;
            """

            st.markdown('<div class="code-box">', unsafe_allow_html=True)
            st.code(wh_text, language="sql")
            st.markdown('</div>', unsafe_allow_html=True)


    except Exception as e:
        st.error(f"Error checking event logging: {e}")



    st.info("If you need any assistance, please check our [documentation](https://genesiscomputing.ai/docs/) or join our [Slack community](https://communityinviter.com/apps/genesisbotscommunity/genesis-bots-community).")