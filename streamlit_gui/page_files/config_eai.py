import streamlit as st

def config_eai():
    
    st.title("Step 3: Configure External Access Integration (EAI)")
    
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
    .code-box {
        background-color: #f0f0f0;
        padding: 20px;
        border-radius: 5px;
        margin-bottom: 20px;
    }
    </style>
    """, unsafe_allow_html=True)

    st.markdown('<p class="big-font">Why do we need EAI?</p>', unsafe_allow_html=True)
    
    st.markdown("""
    <div class="info-box">
    Genesis uses Snowflake Cortex by default as its LLM. Currently, the Llama3.1-405b model is the best performing model for bots.
    This model is available in some but not all Snowflake Regions. You can check the availability in each region on the [Snowflake regional LLM availability page](https://docs.snowflake.com/en/user-guide/snowflake-cortex/llm-functions#availability).
    To allow calling models from other Snowflake regions, you enable cross-region calling, as shown below. For more information, see the [Snowflake documentation on cross-region inference](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cross-region-inference).

    Genesis Bots can optionally use OpenAI as an LLM. To access OpenAI from the Genesis Server, you'll need to create a Snowflake External Access Integration so that the Genesis Server can call OpenAI.
    
    Genesis can also optionally connect to Slack, with some additional configuration, to allow your bots to interact via Slack.
    
    The Genesis Server can also capture and output events to a Snowflake Event Table, allowing you to track what is happening inside the server. Optionally, these logs can be shared back to the Genesis Provider for enhanced support for your GenBots.
    </div>
    """, unsafe_allow_html=True)

    st.markdown('<p class="big-font">Configuration Steps</p>', unsafe_allow_html=True)
    
    st.markdown("""
    Please go back to the worksheet one more time, and run these commands to create an external access integration, and grant Genesis the rights to use it. Genesis will only be able to access the endpoints listed, OpenAI, and optionally Slack. The steps for adding the event logging are optional as well, but recommended.
    """)

    wh_text = f"""-- select role to use, generally Accountadmin or Sysadmin
use role ACCOUNTADMIN;

-- set the name of the installed application
set APP_DATABASE = '{st.session_state.get("app_name", "")}';

-- create a local database to store the network rule (you can change these to an existing database and schema if you like)
CREATE DATABASE IF NOT EXISTS GENESIS_LOCAL_DB; 
CREATE SCHEMA IF NOT EXISTS GENESIS_LOCAL_DB.SETTINGS;

-- Create a network rule that allows Genesis Server to optionally access OpenAI's API, and optionally Slack API and Azure Blob (for DALL-E image generation) 
-- OpenAI and Slack(+Azure) endpoints can be removed if you will not be using Genesis with OpenAI and/or Slack
-- OpenAI will only be used if enabled here if you later provide an OpenAI API Key in the Genesis Server configuration
CREATE OR REPLACE NETWORK RULE GENESIS_LOCAL_DB.SETTINGS.GENESIS_RULE
MODE = EGRESS TYPE = HOST_PORT
VALUE_LIST = ('api.openai.com', 'slack.com', 'www.slack.com', 'wss-primary.slack.com',
'wss-backup.slack.com',  'wss-primary.slack.com:443','wss-backup.slack.com:443', 'slack-files.com',
'oaidalleapiprodscus.blob.core.windows.net:443', 'downloads.slack-edge.com', 'files-edge.slack.com',
'files-origin.slack.com', 'files.slack.com', 'global-upload-edge.slack.com','universal-upload-edge.slack.com');

-- create an external access integration that surfaces the above network rule
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION GENESIS_EAI
ALLOWED_NETWORK_RULES = (GENESIS_LOCAL_DB.SETTINGS.GENESIS_RULE) ENABLED = true;

-- grant Genesis Server the ability to use this external access integration
GRANT USAGE ON INTEGRATION GENESIS_EAI TO APPLICATION   IDENTIFIER($APP_DATABASE);

-- This allows Slack to callback into the Genbots service to active new Genbots on Slack
GRANT BIND SERVICE ENDPOINT ON ACCOUNT TO APPLICATION  IDENTIFIER($APP_DATABASE); 

-- (option steps to enable cross-region inference to Llama3.1-405b)
-- This allows calling models from other Snowflake regions, as described above, for running Genesis in regions other than AWS US West 2 (Oregon) or AWS US East 1 (N. Virginia). 
-- Not needed if you will be using Genesis in the region where it is available, or if you are using OpenAI as your LLM.
ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION';

-- (optional steps for event logging) 
-- create a schema to hold the event table
CREATE SCHEMA IF NOT EXISTS GENESIS_LOCAL_DB.EVENTS;

-- create an event table to capture events from the Genesis Server
CREATE EVENT TABLE  IF NOT EXISTS GENESIS_LOCAL_DB.EVENTS.GENESIS_APP_EVENTS;

-- set the event table on your account, this is optional
-- this requires ACCOUNTADMIN, and may already be set, skip if it doesnt work
ALTER ACCOUNT SET EVENT_TABLE=GENESIS_LOCAL_DB.EVENTS.GENESIS_APP_EVENTS;

-- allow sharing of the captured events with the Genesis Provider
-- optional, skip if it doesn't work
ALTER APPLICATION IDENTIFIER($APP_DATABASE) SET SHARE_EVENTS_WITH_PROVIDER = TRUE;
"""

    st.markdown('<div class="code-box">', unsafe_allow_html=True)
    st.code(wh_text, language="sql")
    st.markdown('</div>', unsafe_allow_html=True)

    st.success("Once you run the above, you can proceed to the next step to start the Genesis Server.")
    
    if "proceed_button_start_clicked" not in st.session_state:
        if st.button("Proceed to Start Genesis Server", key="proceed_button_start"):
            st.session_state["radio"] = "4: Start Genesis Server"
            st.session_state["proceed_button_start_clicked"] = True
            st.rerun()
    else:
        st.write("<<--- Use the selector on the left to select 4: Start Genesis Server")

    st.info("If you need any assistance, please check our [documentation](https://genesiscomputing.ai/docs/) or join our [Slack community](https://communityinviter.com/apps/genesisbotscommunity/genesis-bots-community).")