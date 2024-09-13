import streamlit as st
from utils import get_session, get_metadata, upgrade_services
import subprocess

def config_eai():
    session = get_session()
    if not session:
        st.error("Unable to connect to Snowflake. Please check your connection.")
        # return
        pass 
    st.title("Configure External Access Integration (EAI)")
    
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

    st.markdown('<p class="big-font">Why do we need EAI?</p>', unsafe_allow_html=True)
    
    st.markdown("""
    Genesis uses Snowflake Cortex by default as its LLM. Currently, the Llama3.1-405b model is the best performing model for bots.
    This model is available in some but not all Snowflake Regions. You can check the availability in each region on the [Snowflake regional LLM availability page](https://docs.snowflake.com/en/user-guide/snowflake-cortex/llm-functions#availability).
    To allow calling models from other Snowflake regions, you enable cross-region calling, as shown below. For more information, see the [Snowflake documentation on cross-region inference](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cross-region-inference).

    Genesis Bots can optionally use OpenAI as an LLM. To access OpenAI from the Genesis Server, you'll need to create a Snowflake External Access Integration so that the Genesis Server can call OpenAI.
    
    Genesis can also optionally connect to Slack, with some additional configuration, to allow your bots to interact via Slack.
    
    The Genesis Server can also capture and output events to a Snowflake Event Table, allowing you to track what is happening inside the server. Optionally, these logs can be shared back to the Genesis Provider for enhanced support for your GenBots.
    """, unsafe_allow_html=True)

    st.markdown('<p class="big-font">Configuration Steps</p>', unsafe_allow_html=True)
    
    st.markdown("""
    Please open a Snowflake worksheet and run these commands to create an external access integration, grant Genesis the rights to use it, and test the access. Genesis will only be able to access the endpoints listed, OpenAI, and optionally Slack. The steps for adding the event logging are optional as well, but recommended.
    """)

    wh_text = f"""-- select authorized role to use

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
-- This allows calling models from other Snowflake regions, as described above, 
-- for running Genesis in regions other than AWS US West 2 (Oregon) or AWS US East 1 (N. Virginia). 
-- Not needed if you will be using Genesis in the region where it is available, or if you are using OpenAI as your LLM.
ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION';

"""

    st.markdown('<div class="code-box">', unsafe_allow_html=True)
    st.code(wh_text, language="sql")
    st.markdown('</div>', unsafe_allow_html=True)

    st.success("Once you run the above, you can proceed to the next step to assign the EAI to the Genesis services.")
       
    st.write("Click the button to assign the external access integration to the Genesis Bots services. This will restart your service and takes 3-5 minutes to complete.")

    input_eai = st.text_input("External Access Integration name:", value="GENESIS_EAI")

    if st.button("Assign EAI to Genesis", key="upgrade_button_app"):
        try:
            eai_result = get_metadata('custom_config '+input_eai+'|EAI')
            if isinstance(eai_result, list) and len(eai_result) > 0:
                if 'Success' in eai_result[0] and eai_result[0]['Success']==True:
                    core_prefix = st.session_state.get('core_prefix', '')
                    select_query = f"SELECT {core_prefix}.CHECK_URL_STATUS('slack');"
                    eai_test_result = session.sql(select_query).collect()
                    st.success(f"EAI test result: {eai_test_result[0][0]}")
                    try:
                        upgrade_result = upgrade_services()
                        st.success(f"Genesis Bots upgrade result: {upgrade_result}")
                        # st.rerun()
                    except Exception as e:
                        st.error(f"Error upgrading services: {e}")       
                           
        except Exception as e:
            st.error(f"Error testing EAI on Snowflake: {e} : {eai_result}")

    st.info("If you need any assistance, please check our [documentation](https://genesiscomputing.ai/docs/) or join our [Slack community](https://communityinviter.com/apps/genesisbotscommunity/genesis-bots-community).")