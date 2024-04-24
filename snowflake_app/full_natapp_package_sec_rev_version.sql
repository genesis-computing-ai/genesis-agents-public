use use database app_local_db;


SHOW VERSIONS IN APPLICATION PACKAGE GENESISAPP_APP_PKG_EXT;



select current_region(), current_account();

show compute pools;
drop compute pool genesis_pool;

-- ########## BEGIN ENVIRONMENT  ######################################


SET APP_OWNER_ROLE = 'ACCOUNTADMIN';
SET APP_WAREHOUSE = 'XSMALL';
SET APP_COMPUTE_POOL = 'genesis_test_pool';
SET APP_DISTRIBUTION = 'INTERNAL';
SET APP_COMPUTE_POOL_FAMILY = 'CPU_X64_XS';


-- ########## END   ENVIRONMENT  ######################################






USE ROLE ACCOUNTADMIN;


CREATE ROLE GENESIS_PROVIDER_ROLE;
GRANT CREATE DATABASE ON ACCOUNT TO ROLE GENESIS_PROVIDER_ROLE;
GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE GENESIS_PROVIDER_ROLE;
GRANT CREATE COMPUTE POOL ON ACCOUNT TO ROLE GENESIS_PROVIDER_ROLE;
GRANT CREATE INTEGRATION ON ACCOUNT TO ROLE GENESIS_PROVIDER_ROLE;
GRANT CREATE APPLICATION PACKAGE ON ACCOUNT TO ROLE GENESIS_PROVIDER_ROLE;
GRANT CREATE APPLICATION  ON ACCOUNT TO ROLE GENESIS_PROVIDER_ROLE ;
GRANT CREATE DATA EXCHANGE LISTING  ON ACCOUNT TO ROLE GENESIS_PROVIDER_ROLE;
GRANT IMPORT SHARE ON ACCOUNT TO GENESIS_PROVIDER_ROLE;
GRANT CREATE SHARE ON ACCOUNT TO GENESIS_PROVIDER_ROLE;
GRANT MANAGE EVENT SHARING ON ACCOUNT TO GENESIS_PROVIDER_ROLE;
GRANT BIND SERVICE ENDPOINT ON ACCOUNT TO ROLE GENESIS_PROVIDER_ROLE WITH GRANT OPTION;


GRANT ROLE GENESIS_PROVIDER_ROLE to USER JUSTIN;


CREATE ROLE GENESIS_CONSUMER_ROLE;
GRANT CREATE DATABASE ON ACCOUNT TO ROLE GENESIS_CONSUMER_ROLE;
GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE GENESIS_CONSUMER_ROLE;
GRANT CREATE COMPUTE POOL ON ACCOUNT TO ROLE GENESIS_CONSUMER_ROLE;
GRANT CREATE INTEGRATION ON ACCOUNT TO ROLE GENESIS_CONSUMER_ROLE;
GRANT CREATE APPLICATION  ON ACCOUNT TO ROLE GENESIS_CONSUMER_ROLE ;
GRANT IMPORT SHARE ON ACCOUNT TO GENESIS_CONSUMER_ROLE;
GRANT CREATE SHARE ON ACCOUNT TO GENESIS_CONSUMER_ROLE;
GRANT MANAGE EVENT SHARING ON ACCOUNT TO GENESIS_CONSUMER_ROLE;
GRANT BIND SERVICE ENDPOINT ON ACCOUNT TO ROLE GENESIS_CONSUMER_ROLE WITH GRANT OPTION;


GRANT ROLE GENESIS_CONSUMER_ROLE to USER JUSTIN;


-- ########## END ROLES (OPTIONAL)  ######################################


-- ########## BEGIN INITIALIZATION  ######################################


USE ROLE identifier($APP_OWNER_ROLE);
select current_role();


--use role partner_apps_owner_role;


--DROP DATABASE IF EXISTS GENESISAPP_APP_PKG_EXT ;
--drop database genesisapp_master;




/*
CREATE WAREHOUSE IT NOT EXISTS IDENTIFIER($APP_WAREHOUSE)
 MIN_CLUSTER_COUNT=1
 MAX_CLUSTER_COUNT=1
 WAREHOUSE_SIZE=XSMALL
 AUTO_RESUME = TRUE
 INITIALLY_SUSPENDED = FALSE
 AUTO_SUSPEND = 60;


CREATE COMPUTE POOL IF NOT EXISTS IDENTIFIER($APP_COMPUTE_POOL)
 MIN_NODES=1
 MAX_NODES=1
 INSTANCE_FAMILY=STANDARD_1
 AUTO_RESUME = TRUE
 INITIALLY_SUSPENDED = FALSE
 AUTO_SUSPEND_SECS = 600;
*/


USE WAREHOUSE identifier($APP_WAREHOUSE);
drop database genesisapp_master;
use warehouse xsmall;
use role accountadmin;


CREATE DATABASE IF NOT EXISTS GENESISAPP_MASTER;
USE DATABASE GENESISAPP_MASTER;
CREATE SCHEMA IF NOT EXISTS CODE_SCHEMA;
USE SCHEMA CODE_SCHEMA;
CREATE IMAGE REPOSITORY IF NOT EXISTS SERVICE_REPO;

show image repositories;


CREATE APPLICATION PACKAGE IF NOT EXISTS GENESISAPP_APP_PKG_EXT;


USE DATABASE GENESISAPP_APP_PKG_EXT;
CREATE SCHEMA IF NOT EXISTS CODE_SCHEMA;
CREATE STAGE IF NOT EXISTS APP_CODE_STAGE;


-- ##########  END INITIALIZATION   ######################################


--
-- STOP HERE AND UPLOAD ALL REQUIRED CONTAINERS INTO THE IMAGE REPO
--
show image repositories;

use schema genesisapp_master.code_schema;


show image repositories;
//sfengineering-ss-lprpr-test1.registry.snowflakecomputing.com/genesisapp_master/code_schema/service_repo


-- ########## UTILITY FUNCTIONS  #########################################
USE SCHEMA GENESISAPP_APP_PKG_EXT.CODE_SCHEMA;
use warehouse xsmall;


CREATE OR REPLACE PROCEDURE PUT_TO_STAGE_SUBDIR(STAGE VARCHAR,SUBDIR VARCHAR,FILENAME VARCHAR, CONTENT VARCHAR)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION=3.8
PACKAGES=('snowflake-snowpark-python')
HANDLER='put_to_stage'
AS $$
import io
import os


def put_to_stage(session, stage, subdir, filename, content):
   local_path = '/tmp'
   local_file = os.path.join(local_path, filename)
   f = open(local_file, "w")
   f.write(content)
   f.close()
   session.file.put(local_file, '@'+stage+'/'+subdir, auto_compress=False, overwrite=True)
   return "saved file "+filename+" in stage "+stage
$$;


CREATE OR REPLACE PROCEDURE GET_FROM_STAGE_SUBDIR(STAGE VARCHAR,SUBDIR VARCHAR, FILENAME VARCHAR)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION=3.8
PACKAGES=('snowflake-snowpark-python')
HANDLER='get_from_stage'
AS $$
import io
import os
from pathlib import Path


def get_from_stage(session, stage, subdir, filename):
   local_path = '/tmp'
   local_file = os.path.join(local_path, filename)
   session.file.get('@'+stage+'/'+subdir+'/'+filename, local_path)
   content=Path(local_file).read_text()
   return content
$$;


CREATE  PROCEDURE PUT_TO_STAGE(STAGE VARCHAR,FILENAME VARCHAR, CONTENT VARCHAR)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION=3.8
PACKAGES=('snowflake-snowpark-python')
HANDLER='put_to_stage'
AS $$
import io
import os


def put_to_stage(session, stage, filename, content):
   local_path = '/tmp'
   local_file = os.path.join(local_path, filename)
   f = open(local_file, "w")
   f.write(content)
   f.close()
   session.file.put(local_file, '@'+stage, auto_compress=False, overwrite=True)
   return "saved file "+filename+" in stage "+stage
$$;


--
-- Python stored procedure to return the content of a file in a stage
--
CREATE OR REPLACE PROCEDURE GET_FROM_STAGE(STAGE VARCHAR,FILENAME VARCHAR)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION=3.8
PACKAGES=('snowflake-snowpark-python')
HANDLER='get_from_stage'
AS $$
import io
import os
from pathlib import Path


def get_from_stage(session, stage, filename):
   local_path = '/tmp'
   local_file = os.path.join(local_path, filename)
   session.file.get('@'+stage+'/'+filename, local_path)
   content=Path(local_file).read_text()
   return content
$$;


-- ########## END UTILITY FUNCTIONS  #####################################


-- ########## SCRIPTS CONTENT  ###########################################
USE SCHEMA GENESISAPP_APP_PKG_EXT.CODE_SCHEMA;
CREATE OR REPLACE TABLE SCRIPT (NAME VARCHAR, VALUE VARCHAR);
DELETE FROM SCRIPT;






INSERT INTO SCRIPT (NAME , VALUE)
VALUES ('MANIFEST',
$$
manifest_version: 1 # required
artifacts:
 setup_script: setup_script.sql
 readme: readme.md
 container_services:
   images:
   - /genesisapp_master/code_schema/service_repo/genesis_app:latest  
 extension_code: true
 default_streamlit: core.sis_launch
privileges:
  - BIND SERVICE ENDPOINT:
      description: "Allow access to application endpoints"
$$)
;
--privileges:
--  - IMPORTED PRIVILEGES ON SNOWFLAKE DB:
--      description: "to see table metadata of granted tables"


--privileges:
--  - BIND SERVICE ENDPOINT:
--      description: "a service can serve requests from public endpoint"



use schema genesisapp_app_pkg_ext.code_schema;
delete from script;


INSERT INTO SCRIPT (NAME , VALUE)
VALUES ('SIS_ENV',
$$
name: sis_launch
channels:
 - snowflake
dependencies:
 - streamlit=1.26.0
 - pandas
 - snowflake-snowpark-python
$$)
;
delete from script;
INSERT INTO SCRIPT (NAME , VALUE)
VALUES ('SIS_APP',
$$
import streamlit as st
import os, json

prefix = 'genesis_bots_alpha.app1'
core_prefix = 'genesis_bots_alpha.CORE'

app_name = 'GENESIS_BOTS_ALPHA'

st.set_page_config(layout="wide")

if 'wh_name' not in st.session_state:
    st.session_state['wh_name'] = 'XSMALL'

SnowMode = True

import time
import uuid
import datetime
import pandas as pd

try:
    if SnowMode:
        from snowflake.snowpark.context import get_active_session
        session = get_active_session()
except:
    SnowMode = False

def get_slack_tokens():

    import requests
    import json

    if SnowMode:

        sql = f"select {prefix}.get_slack_endpoints() "
        data = session.sql(sql).collect()
        response = json.loads(data[0][0])
        return response

    # add SnowMode
    url = "http://127.0.0.1:8080/udf_proxy/get_slack_tokens"
    headers = {"Content-Type": "application/json"}

    data = json.dumps({"data": [[0]]})

    response = requests.post(url, headers=headers, data=data)
    #st.write(response.json())
    if response.status_code == 200:
        return response.json()['data'][0][1]
    else:
        raise Exception(f"Failed to reach bot server to get list of available bots")

def get_ngrok_tokens():

    import requests
    import json
    
    if SnowMode:
        sql = f"select {prefix}.get_ngrok_tokens() "
        data = session.sql(sql).collect()
        response = json.loads(data[0][0])
        return response

    else:
        
        # add SnowMode
        url = "http://127.0.0.1:8080/udf_proxy/get_ngrok_tokens"
        headers = {"Content-Type": "application/json"}
    
        data = json.dumps({"data": [[0]]})
    
        response = requests.post(url, headers=headers, data=data)
        #st.write(response.json())
        if response.status_code == 200:
            return response.json()['data'][0][1]
        else:
            raise Exception(f"Failed to reach bot server to get list of available bots")

def set_ngrok_token(ngrok_auth_token, ngrok_use_domain, ngrok_domain):
    """
    Calls the /udf_proxy/configure_ngrok_token endpoint to validate and set the ngrok auth token, use domain, and domain.

    Args:
        ngrok_auth_token (str): The ngrok Auth Token.
        ngrok_use_domain (bool): Flag to determine if a custom domain is used.
        ngrok_domain (str): The custom domain to be used if ngrok_use_domain is True.
    """
    import requests
    import json

    print (SnowMode)
    if SnowMode:

        sql = f"select {prefix}.configure_ngrok_token('{ngrok_auth_token}','{ngrok_use_domain}','{ngrok_domain}') "
        data = session.sql(sql).collect()
        response = json.loads(data[0][0])
        return response

    # add SnowMode
    url = "http://127.0.0.1:8080/udf_proxy/configure_ngrok_token"
    headers = {"Content-Type": "application/json"}

    data = json.dumps({"data": [[0, ngrok_auth_token, ngrok_use_domain, ngrok_domain]]})
    response = requests.post(url, headers=headers, data=data)
    if response.status_code == 200:
        return response.json()['data'][0][1] # response message
    else:
        return "Error", f"Failed to set ngrok tokens: {response.Message}"


def set_slack_tokens(slack_app_token, slack_app_refresh_token):
    """
    Calls the /udf_proxy/configure_slack_app_token endpoint to validate and set the Slack app token and refresh token.

    Args:
        slack_app_token (str): The Slack App Config Token.
        slack_app_refresh_token (str): The Slack App Refresh Token.
    """
    import requests
    import json

    if SnowMode:

        sql = f"select {prefix}.configure_slack_app_token('{slack_app_token}','{slack_app_refresh_token}') "
        data = session.sql(sql).collect()
        response = json.loads(data[0][0])
        return response

    else:
        
        url = "http://127.0.0.1:8080/udf_proxy/configure_slack_app_token"
        headers = {"Content-Type": "application/json"}
    
        data = json.dumps({"data": [[0, slack_app_token, slack_app_refresh_token]]})
    
        response = requests.post(url, headers=headers, data=data)
        if response.status_code == 200:
            return response.json()['data'][0][1] # new tokens 
        else:
            return "Error", f"Failed to set Slack Tokens: {response.text}"


def get_bot_details():

    import requests
    import json

    # add SnowMode

    if SnowMode:

       sql = f"select {prefix}.list_available_bots() "
       data = session.sql(sql).collect()
       response = json.loads(data[0][0])
       return response

    else:
        url = "http://127.0.0.1:8080/udf_proxy/list_available_bots"
        headers = {"Content-Type": "application/json"}
    
        data = json.dumps({"data": [[0]]})
    
        response = requests.post(url, headers=headers, data=data)
        if response.status_code == 200:
            return response.json()['data'][0][1]
        else:
            raise Exception(f"Failed to reach bot server to get list of available bots")


def configure_llm(llm_model_name, llm_api_key):
    """
    Configures the LLM (Language Learning Model) by calling the /udf_proxy/configure_llm endpoint.
    """
    import requests
    import json

    if SnowMode:

        sql = f"select {prefix}.configure_llm('{llm_model_name}', '{llm_api_key}') "
        data = session.sql(sql).collect()
        response = data[0][0]
        return json.loads(response)
    
    url = "http://127.0.0.1:8080/udf_proxy/configure_llm"
    headers = {"Content-Type": "application/json"}
    data = json.dumps({"data": [[0, llm_model_name, llm_api_key]]})

    response = requests.post(url, headers=headers, data=data)

    if response.status_code == 200:
        return response.json()['data'][0][1]
    else:
        raise Exception(f"Failed to configure LLM: {response.text}")

def get_metadata(metadata_type):
    """
    Calls the /udf_proxy/get_metadata endpoint with the given metadata_type and returns the JSON results.
    """
    import requests
    import json

    if SnowMode:

        sql = f"select {prefix}.get_metadata('{metadata_type}') "
        data = session.sql(sql).collect()
        response = data[0][0]
        response = json.loads(response)
        return response
    else:
    
        url = "http://127.0.0.1:8080/udf_proxy/get_metadata"
        headers = {"Content-Type": "application/json"}
        data = json.dumps({"data": [[0, metadata_type ]]})
    
        response = requests.post(url, headers=headers, data=data)
        if response.status_code == 200:
            return response.json()['data'][0][1]
        else:
            raise Exception(f"Failed to get metadata: {response.text}")
    


def submit_to_udf_proxy(input_text, thread_id, bot_id):
    """
    Submits a string to the UDF proxy and returns a UUID for the submission.
    """
    import requests
    import json

    if SnowMode:

        sql = f"select {prefix}.submit_udf('{input_text}', '{thread_id}', '{bot_id}') "
        data = session.sql(sql).collect()
        response = data[0][0]
        return response
    
    # add snowmode

    url = f"http://127.0.0.1:8080/udf_proxy/submit_udf"
    headers = {"Content-Type": "application/json"}
    data = json.dumps({"data": [[1, input_text, thread_id, bot_id]]})

    response = requests.post(url, headers=headers, data=data)
    if response.status_code == 200:
        return response.json()['data'][0][1]  # UUID of the submission
    else:
        raise Exception(f"Failed to submit to UDF proxy: {response.text}")

def get_response_from_udf_proxy(uu, bot_id):
    """
    Retrieves the response for a given UUID from the UDF proxy.
    """
    import requests
    import json

    # add snowmode 

    if SnowMode:

        sql = f"select {prefix}.lookup_udf('{uu}', '{bot_id}') "
        data = session.sql(sql).collect()
        response = data[0][0]
        return response


    url = f"http://127.0.0.1:8080/udf_proxy/lookup_udf"
    headers = {"Content-Type": "application/json"}
    data = json.dumps({"data": [[1, uu, bot_id]]})

    response = requests.post(url, headers=headers, data=data)
    if response.status_code == 200:
        return response.json()['data'][0][1]  # Response from the UDF proxy
    else:
        return("not found")


def deploy_bot(bot_id):
    """
    Calls the /udf_proxy/deploy_bot endpoint with the given bot_id and returns the response.
    """
    import requests
    import json

    if SnowMode:

        sql = f"select {prefix}.deploy_bot('{bot_id}') "
        data = session.sql(sql).collect()
        response = data[0][0]
        return response
    
    url = "http://127.0.0.1:8080/udf_proxy/deploy_bot"
    headers = {"Content-Type": "application/json"}
    data = json.dumps({"data": [[0, bot_id]]})

    response = requests.post(url, headers=headers, data=data)
    if response.status_code == 200:
        return response.json()['data'][0][1]  # Response from the UDF proxy
    else:
        raise Exception(f"Failed to deploy bot: {response.text}")

def llm_config(): # Check if data is not empty

    bot_details = get_bot_details()
    #st.write(bot_details)

    cur_key = '<existing key present on server>'

    if bot_details == {'Success': False, 'Message': 'Needs LLM Type and Key'}:
        st.success('Welcome! Before you chat with bots, please setup your LLM Keys')
        time.sleep(.3)
        cur_key = ''

    if True:
        st.header("LLM Model & API Key Setup")
        st.write("Genesis Bots require access to OpenAI, as these are the only LLMs currently powerful enough to service the bots. Please visit https://platform.openai.com/signup to get a paid API key for OpenAI before proceeding.")
        llm_model = st.selectbox("Choose LLM Model:", ["OpenAI"])
        llm_api_key = st.text_input("Enter API Key:", value=cur_key)

        if "disable_submit" not in st.session_state:
            st.session_state.disable_submit = False

        if st.button("Submit API Key", key="sendllm", disabled=st.session_state.disable_submit):
            
            # Code to handle the API key submission will go here
            # This is a placeholder for the actual submission logic

            with st.spinner('Validating API key and launching bots...'):
                config_response = configure_llm(llm_model, llm_api_key)
            
                if config_response['Success'] is False:
                    resp = config_response["Message"]
                    st.error(f"Failed to set LLM token: {resp}")
                else:
                    st.session_state.disable_submit = True
                    st.success("API key validated!")

                with st.spinner('Getting active bot details...'):
                    bot_details = get_bot_details()
                    if bot_details:
                        st.success("Bot details validated.")

            if cur_key == '<existing key present on server>':
                st.write("Reload this page to chat with your apps.")
            else:
                if st.button("Next -> Click here to chat with your bots!"):
                    st.experimental_rerun()
                    # This button will be used to talk to the bot directly via Streamlit interface
                    # Placeholder for direct bot communication logic
                    #st.session_state['radio'] = "Chat with Bots"

def chat_page():

    def submit_button(prompt, chatmessage):

        # Display user message in chat message container
        with chatmessage:
            st.markdown(prompt)
            
        # Add user message to chat history
        st.session_state[f"messages_{selected_bot_id}"].append({"role": "user", "content": prompt})

    
        request_id = submit_to_udf_proxy(input_text=prompt, thread_id=st.session_state[f"thread_id_{selected_bot_id}"], bot_id=selected_bot_id)
        response = 'not found'

        with st.spinner('Thinking...'):
            while response == 'not found':
                response = get_response_from_udf_proxy(uu=request_id, bot_id=selected_bot_id)
                time.sleep(0.5)
        # Display assistant response in chat message container
        with st.chat_message("assistant"):
            st.markdown(response)
        # Add assistant response to chat history
        st.session_state[f"messages_{selected_bot_id}"].append({"role": "assistant", "content": response})


    try:
        bot_details = get_bot_details()
    except Exception as e:
        bot_details = {'Success': False, 'Message': 'Genesis Server Offline'}
        return
   #st.write(bot_details)
    if bot_details == {'Success': False, 'Message': 'Needs LLM Type and Key'}:
        #st.success('Welcome! Before you chat with bots, please setup your LLM Keys')
        #time.sleep(.3)
        #st.session_state['radio'] = "Setup LLM Model & Key"
        llm_config()
        #st.experimental_rerun()
    else: 

        try:
    
            bot_details.sort(key=lambda x: (not "Eve" in x["bot_name"], x["bot_name"]))
            bot_names = [bot["bot_name"] for bot in bot_details]
    
            #st.write(bot_names)
        
            bot_ids = [bot["bot_id"] for bot in bot_details]
            if len(bot_names) > 0:
                selected_bot_name = st.selectbox("Active Bots", bot_names)
                selected_bot_index = bot_names.index(selected_bot_name)
            selected_bot_id = bot_ids[selected_bot_index]
    
            if st.button("New Chat", key="new_chat_button"):
            # Reset the chat history and thread ID for the selected bot
                st.session_state[f"messages_{selected_bot_id}"] = [{"role": "assistant", "content": f"Hi, I'm {selected_bot_name}! How can I help you today?"}]
                st.session_state[f"thread_id_{selected_bot_id}"] = str(uuid.uuid4())
                # Clear the chat input
                #st.session_state[f"chat_input_{selected_bot_id}"] = ""
                # Rerun the app to reflect the changes
                st.experimental_rerun()
            
    
            if f"thread_id_{selected_bot_id}" not in st.session_state:
                st.session_state[f"thread_id_{selected_bot_id}"] = str(uuid.uuid4())
    
            # Initialize chat history
            if f"messages_{selected_bot_id}" not in st.session_state:
                st.session_state[f"messages_{selected_bot_id}"] = []
                st.session_state[f"messages_{selected_bot_id}"].append({"role": "assistant", "content": f"Hi, I'm {selected_bot_name}! How can I help you today?"})
    
            # Display chat messages from history on app rerun
            for message in st.session_state[f"messages_{selected_bot_id}"]:
                with st.chat_message(message["role"]):
                    st.markdown(message["content"])
    
            # React to user input
            if prompt := st.chat_input("What is up?", key=f"chat_input_{selected_bot_id}"):
                pass
    
            #response = f"Echo: {prompt}"
            if prompt != None:
                submit_button(prompt, st.chat_message("user"))
        except Exception as e:

            st.subheader('Chat GUI Currently not working via SiS in Native App')
            sql = f"SHOW ENDPOINTS IN SERVICE {prefix}.GENESISAPP_SERVICE_SERVICE "
            data = session.sql(sql).collect()
            response = data[0][4]
            st.write(f'Paste this into your browser to use via external Streamlit:')
            st.write(response)


def setup_slack():

    tokens = get_slack_tokens()

    #st.write(tokens)

    tok = tokens.get("Token",None)
    ref = tokens.get("RefreshToken",None)
    slack_active = tokens.get("SlackActiveFlag",False)

    if slack_active:
        st.success("Slack Connector is Currently Active")
    else:
        st.warning("Slack Connector is not currently active, please complete the form below to activate.")

    st.title('Setup Slack Tokens')
    st.write('By providing a Slack App Refresh Token, Genesis Bots can create, update, and remove Genesis Bots from your Slack environment.')
    st.write('Go to https://api.slack.com/apps and create an App Config Refresh Token, paste it below, and press Update. ')
    # Text input for Slack App Token

    if tok == "...": 
        tok = ""
    if ref == "...":
        ref = ""
   
    if tok:
        slack_app_token = st.text_input("Slack App Token", value=tok)
    # Text input for Slack App Refresh Token
    slack_app_refresh_token = st.text_input("Slack App REFRESH Token", value=ref if ref else "")
    # Button to submit new tokens

    if st.button("Update Slack Token"):
        # Call function to update tokens (functionality to be implemented)
        # Call set_slack_tokens and display the result
        resp = set_slack_tokens('NOT NEEDED', slack_app_refresh_token)
        t = resp.get("Token", "Error")
        r = resp.get("Refresh", "Error")
        if t == "Error":
            st.error(f"Failed to update Slack tokens: {resp}")
        else:
            st.success("Slack tokens updated and refreshed successfully. Your new refreshed tokens are:")
            st.json({"Token": t, "RefreshToken": r})
            st.success("These will be different than the ones you provided, as they have been rotated successfully for freshness.")        
            st.success("You can now activate your bots on Slack from the Bot Configuration page, on the left Nav.")        
        pass

def setup_ngrok():

    tokens = get_ngrok_tokens()
    #st.write(tokens

    tok = tokens.get("ngrok_auth_token",None)
    ngrok_active = tokens.get("ngrok_active_flag",False)

    if ngrok_active:
        st.success("Ngrok is Currently Active")
    else:
        st.warning("Ngrok is not currently active, please complete the form below to activate.")

    st.title('Ngrok Tokens Setup')
    st.write('By providing a Ngrok Auth Token, the Slack Connnector will be able to talk to your Genesis Bots. Go to https://dashboard.ngrok.com/signup and create an free Ngrok account, and paste the Ngrok Auth Token below.')
    # Text input for Slack App Token

    if tok == "...": 
        tok = ""
   
    ngrok_token = st.text_input("Ngrok Auth Token", value=tok if tok else "")
    # Text input for Slack App Refresh Token


    if st.button("Update Ngrok Tokens"):
        # Call function to update tokens (functionality to be implemented)
        # Call set_slack_tokens and display the result
        resp = set_ngrok_token(ngrok_token,'N','')
        #st.write(resp)
        t = resp.get("ngrok_auth_token", "Error")
        if resp.get('Success') == False or resp.get('ngrok_active')==False:
            st.error(f"Invalid Ngrok Token or other Activation Error: {resp.get('Message','')}")
        else:
            st.success("Ngrok token updated.")        
        pass


def db_harvester():
    
    harvest_control = get_metadata('harvest_control')
    harvest_summary = get_metadata('harvest_summary')

    if harvest_control == []:
        harvest_control = None
    if harvest_summary == []:
        harvest_summary = None

    # Initialize empty DataFrames with appropriate columns if no data is present
    if harvest_control:
        harvest_control_df = pd.DataFrame(harvest_control).rename(columns=str.lower)
        harvest_control_df['schema_exclusions'] = harvest_control_df['schema_exclusions'].apply(lambda x: ["None"] if not x else x)
        harvest_control_df['schema_inclusions'] = harvest_control_df['schema_inclusions'].apply(lambda x: ["All"] if not x else x)
    else:
        harvest_control_df = pd.DataFrame(columns=['source_name', 'database_name', 'schema_name', 'schema_exclusions', 'schema_inclusions', 'status', 'refresh_interval', 'initial_crawl_complete'])

    column_order = ['source_name', 'database_name', 'schema_name', 'role_used_for_crawl', 'last_change_ts', 'objects_crawled']

    if harvest_summary:
        harvest_summary_df = pd.DataFrame(harvest_summary).rename(columns=str.lower)
        # Reordering columns instead of sorting rows
        harvest_summary_df = harvest_summary_df[column_order]
        # Calculate the sum of objects_crawled using the DataFrame
        total_objects_crawled = harvest_summary_df['objects_crawled'].sum()
        # Find the most recent change timestamp using the DataFrame
        most_recent_change_str = str(harvest_summary_df['last_change_ts'].max()).split(".")[0]
    else:
        harvest_summary_df = pd.DataFrame(columns=column_order)
        total_objects_crawled = 0
        most_recent_change_str = "N/A"

    harvester_status = 'Active' if harvest_control and harvest_summary else 'Inactive'
    # Display metrics at the top
    col0, col1, col2 = st.columns(3)
    with col0:
        st.metric(label="Harvester Status", value=harvester_status)
    with col1:
        st.metric(label="Total Objects Crawled", value=total_objects_crawled)
    with col2:
        st.metric(label="Most Recent Change", value=most_recent_change_str)

    st.subheader("Sources and Databases being Harvested")
    #st.dataframe(harvest_control_df.astype(str))
    # Convert JSON strings in 'schema_inclusions' to Python lists, handling nulls and non-list values
    if not harvest_control_df.empty:
        harvest_control_df['schema_inclusions'] = harvest_control_df['schema_inclusions'].apply(
            lambda x: json.loads(x) if isinstance(x, str) else (["All"] if x is None else x)
        )
        # Display the DataFrame in Streamlit
        st.dataframe(harvest_control_df, use_container_width=True)

    st.subheader("Database and Schema Harvest Status")
    if not harvest_summary_df.empty:
        st.dataframe(harvest_summary_df, use_container_width=True, height=300)
    else:
        st.write("No data available for Database and Schema Harvest Status.")


def db_add_to_harvester():

    st.subheader("Available Databases for Harvesting")

    try:
        with st.spinner('Fetching available databases...'):
            available_databases = get_metadata('available_databases')
            
        if available_databases == []:
            available_databases = None

        if available_databases:
            available_databases_df = pd.DataFrame(available_databases)
            st.dataframe(available_databases_df[['DatabaseName', 'Schemas']], use_container_width=True, height=600)
    
            # Streamlit widget to select a database to add to the harvest
            
            if available_databases_df.shape[0] > 0:
                first_database = available_databases_df.iloc[0]['DatabaseName']
                schemas = available_databases_df.iloc[0]['Schemas']
                if 'INFORMATION_SCHEMA' in schemas:    
                    example_text = f'Eve, please add {first_database} and include all schemas except the INFORMATION_SCHEMA'
                else:
                    example_text = f'Eve, please add {first_database} and include all schemas'
            st.write(f'To add a database to the harvest, just ask Eve and provide the database name and schemas to include or exclude.')
            st.write(f'For example, "{example_text}"') 

        else:
            st.write("No other data currently visible but not already being harvested. By default Genesis auto-harvests visible data, so see Harvester Status to see if granted data may already be in the harvest process. See the Grant Data Access tab on left for instructions to grant additional access to data to this application.")
    except:
        st.write("No other data currently visible but not already being harvested. By default Genesis auto-harvests visible data, so see Harvester Status to see if granted data may already be in the harvest process. See the Grant Data Access tab on left for instructions to grant additional access to data to this application.")


def grant_data():

    st.subheader('Grant Data Access')
    st.write('The Genesis application can help you with your data in Snowflake. To do so, you need to grant this application access to your data. The helper procedure below can help you grant access to everything in a database to this application. This application runs securely inside your Snowflake account.')
    wh_text = f'''-- select role to use, generally Accountadmin or Sysadmin
use role ACCOUNTADMIN;

-- set the name of the installed application
set APP_DATABASE = '{app_name}';

USE SCHEMA GENESIS_LOCAL_DB.SETTINGS;
USE WAREHOUSE XSMALL; -- or use your warehouse if not XSMALL
'''+'''

CREATE OR REPLACE PROCEDURE GENESIS_LOCAL_DB.SETTINGS.grant_schema_usage_and_select_to_app(database_name STRING, APP_NAME STRING)
RETURNS STRING LANGUAGE JAVASCRIPT EXECUTE AS CALLER
AS ''' + chr(36) + chr(36) + '''
    var connection = snowflake.createStatement({
        sqlText: `SELECT SCHEMA_NAME FROM ${DATABASE_NAME}.INFORMATION_SCHEMA.SCHEMATA`
    });
    var result = connection.execute();
    
    while (result.next()) {
        var schemaName = result.getColumnValue(1);
        if (schemaName === 'INFORMATION_SCHEMA') {
            continue;
        }
        var sqlCommands = [
            `GRANT USAGE ON DATABASE ${DATABASE_NAME} TO APPLICATION ${APP_NAME}`,
            `GRANT USAGE ON SCHEMA ${DATABASE_NAME}.${schemaName} TO APPLICATION ${APP_NAME}`,
            `GRANT SELECT ON ALL TABLES IN SCHEMA ${DATABASE_NAME}.${schemaName} TO APPLICATION ${APP_NAME}`,
            `GRANT SELECT ON ALL VIEWS IN SCHEMA ${DATABASE_NAME}.${schemaName} TO APPLICATION ${APP_NAME}`,
        ];
        
        for (var i = 0; i < sqlCommands.length; i++) {
            try {
                var stmt = snowflake.createStatement({sqlText: sqlCommands[i]});
                stmt.execute();
            } catch(err) {
                // Return error message if any command fails
                return `Error executing command: ${sqlCommands[i]} - ${err.message}`;
            }
        }
    }  
    return "Successfully granted USAGE and SELECT on all schemas, tables, and views to role " + APP_NAME;
''' + chr(36) + chr(36) + ''';

-- see your databases
show databases;

-- to use, call with the name of the database to grant
call GENESIS_LOCAL_DB.SETTINGS.grant_schema_usage_and_select_to_app('<your db name>',$APP_DATABASE);

--- once granted, Genesis will automatically start to catalog this data so you can use it with Genesis bots
'''
   
    st.text_area("Commands to allow this application to see your data:", wh_text, height=800)


  
def bot_config():
    
    bot_details = get_bot_details()

    if bot_details == {'Success': False, 'Message': 'Needs LLM Type and Key'}:
        #st.success('Welcome! Before you configure your bots, please setup your LLM Keys')
        #time.sleep(.3)
        #st.session_state['radio'] = "Setup LLM Model & Key"
        llm_config()
        #st.experimental_rerun()

    else: 
        st.title('Bot Configuration')
        st.write('Here you can see the details of your bots, and you can deploy them to Slack.  To create or remove bots, ask your Eve bot to do it for you in chat.')
        bot_details.sort(key=lambda x: (not "Eve" in x["bot_name"], x["bot_name"]))

        slack_tokens = get_slack_tokens()
        slack_ready = False 
        if slack_tokens.get('Token') != '...':
            slack_ready = True

    # st.write(slack_tokens)

        # Display bot_details in a pretty grid using Streamlit
        if bot_details:
            try:
                for bot in bot_details:
                    st.subheader(bot['bot_name'])
                    col1, col2 = st.columns(2)
                    with col1:
                        st.caption("Bot ID: " + bot['bot_id'])
                        available_tools = bot['available_tools'].strip("[]").replace('"', '').replace("'", "")
                        st.caption(f"Available Tools: {available_tools}")
                        user_id = bot.get('bot_slack_user_id','None')
                        if user_id is None:
                            user_id = 'None'
                        api_app_id = bot.get('api_app_id','None')
                        if api_app_id is None:
                            api_app_id = 'None'
                        st.caption("Slack User ID: " + user_id)
                        st.caption("API App ID: " + api_app_id)
                        if bot['auth_url'] is not None and bot['slack_deployed'] is False:
                            st.markdown(f"**Authorize on Slack:** [Link]({bot['auth_url']})")
                #st.caption("Runner ID: " + bot['runner_id'], unsafe_allow_html=True)
                        if bot['slack_active'] == 'N' or bot['slack_deployed'] == False:
                            if slack_ready and (bot['auth_url'] is None or bot['auth_url']==''):
                                if st.button(f"Deploy {bot['bot_name']} on Slack", key=f"deploy_{bot['bot_id']}"):
                                     # Call function to deploy bot on Slack (functionality to be implemented)
                                    deploy_response = deploy_bot(bot['bot_id'])
                                    if deploy_response.get('Success'):
                                     #   st.write(deploy_response)
                                     #   st.write(bot)
                                        st.success(f"Bot {bot.get('bot_name')} deployed to Slack successfully. [Click here to Authorize on Slack]({deploy_response.get('auth_url')})")
                                    else:
                                        st.error(f"Failed to deploy {bot['bot_name']} to Slack: {deploy_response.get('Message')}")
                                    pass
                            else:
                                if slack_ready is False:
                                    if st.button("Activate Slack Keys Here",  key=f"activate_{bot['bot_id']}"):
                                        # Code to change the page based on a button click
                                        st.session_state['radio'] = "Setup Slack Connection"
                                        st.experimental_rerun()

                    with col2:
                        st.caption("UDF Active: " + ('Yes' if bot['udf_active'] == 'Y' else 'No'))
                        st.caption("Slack Active: " + ('Yes' if bot['slack_active'] == 'Y' else 'No'))
                        st.caption("Slack Deployed: " + ('Yes' if bot['slack_deployed'] else 'No'))
                        st.text_area(label="Instructions", value=bot['bot_instructions'], height=100)



            except ValueError as e:
                st.error(f"Failed to parse bot details: {e}")
        else:
            st.write("No bot details available.")
    

def config_wh():

    st.subheader('Step 1: Configure Warehouse')

    st.write('Genesis Bots needs rights to use a Snowflake compute engine, known as a Virtual Warehouse, to run queries on Snowflake. Please open a new Snowflake worksheet and run these commands to grant Genesis access to an existing Warehouse, or to make a new one for its use. This step does not provide Genesis Bots with access to any of your data, just the ability to run SQL on Snowflake in general.')
    
    wh_text = f'''-- select role to use, generally Accountadmin or Sysadmin
use role ACCOUNTADMIN;

-- set the name of the installed application
set APP_DATABASE = '{app_name}';

-- set warehouse name to use
set APP_WAREHOUSE = '{st.session_state.wh_name}'; 

-- create the warehouse if needed
CREATE WAREHOUSE IF NOT EXISTS IDENTIFIER($APP_WAREHOUSE)
 MIN_CLUSTER_COUNT=1 MAX_CLUSTER_COUNT=1
 WAREHOUSE_SIZE=XSMALL AUTO_RESUME = TRUE AUTO_SUSPEND = 60;

-- allow Genesis to use the warehouse
GRANT USAGE ON WAREHOUSE  IDENTIFIER($APP_WAREHOUSE) TO APPLICATION  IDENTIFIER($APP_DATABASE);
'''

    st.text_area("Warehouse configuration script:", wh_text, height=420)

    # Button to test Snowflake connection by showing warehouses
    if st.button('TEST Access to Warehouse'):
        try:
            # Execute the command and collect the results
            warehouses_result = session.sql('SHOW WAREHOUSES').collect()
            
            # Check if any warehouses were returned
            if warehouses_result:
                # Convert the list of Row objects to a Pandas DataFrame for display
                warehouses_df = pd.DataFrame([row.as_dict() for row in warehouses_result])
                warehouse_names = warehouses_df['name'].tolist()  # Adjust 'name' if necessary to match your column name
                
                # Check if 'XSMALL' is in the list of warehouse names
                if st.session_state.wh_name not in warehouse_names:
                    # Notify the user about the naming discrepancy and suggest setting APP_WAREHOUSE
                    first_warehouse_name = warehouse_names[0]
                    st.session_state.wh_name = first_warehouse_name

                # Display success message with list of warehouses
                st.success(f'Success: Found the following warehouses - {", ".join(warehouse_names)}, Thanks!')
                st.write('**<< Now, click 2. Configure Compute Pool, on left <<**')
        
            else:
                st.error('Error: No warehouses found.  Please open a new worksheet, copy and paste the commands above, and run them.  Then return here and press "TEST Access to Warehouse" above.')
        except Exception as e:
            st.error(f'Error connecting to Snowflake: {e}')


def config_pool():
 
    st.subheader('Step 2: Configure Compute Pool')

    st.write('Genesis Bots has a server component that runs securely inside your Snowflake account, that coordinates the actions of your Genesis Bots, and manages their interactions with other users and bots. To run this server, you need to create and grant Genesis Server access to a Snowflake Compute Pool.')
    st.write('Please go back to your Snowflake worksheet and run these commands to create a new compute pool and grant Genesis the rights to use it.  This uses the smallest Snowflake compute pool, which costs about 0.11 Snowflake Credits per hour, or about $5/day.  Once you start the server, you will be able to suspend it and it when not in use.')
    
    wh_text = f'''-- select role to use, generally Accountadmin or Sysadmin
use role ACCOUNTADMIN;

-- set the name of the installed application
set APP_DATABASE = '{app_name}';

-- remove an existing pool, if you've installed this app before
DROP COMPUTE POOL IF EXISTS GENESIS_POOL;

-- create the compute pool and associate it to this application
CREATE COMPUTE POOL IF NOT EXISTS GENESIS_POOL FOR APPLICATION IDENTIFIER($APP_DATABASE)
 MIN_NODES=1 MAX_NODES=1 INSTANCE_FAMILY='CPU_X64_XS' AUTO_SUSPEND_SECS=3600 INITIALLY_SUSPENDED=FALSE;

-- give Genesis the right to use the compute pool
GRANT USAGE ON COMPUTE POOL GENESIS_POOL TO APPLICATION  IDENTIFIER($APP_DATABASE);
'''

    st.text_area("Compute Pool configuration script:", wh_text, height=420)

    st.write("We can't automatically test this, but if you've performed it same way you did on Step 1, you can now proceed to the next step.")
    st.write('**<< Now, click 3. Configure EAI <<**')
        

def config_eai():
 
    st.subheader('Step 3: Configure External Access Integration (EAI)')

    st.write("Genesis Bots currently uses OpenAI GPT4-Turbo as its main LLM, as it is the only model that we've found powerful and reliable enough to power our bots. To access OpenAI from the Genesis Server, you'll need to create a Snowflake External Access Integration so that the Genesis Server can call OpenAI. Genesis can also optionally connect to Slack via Ngrok, to allow your bots to interact via Slack.")
    st.write('So please go back to the worksheet one more time, and run these commands to create a external access integration, and grant Genesis the rights to use it. Genesis will only be able to access the endpoints listed, OpenAI, and optionally Slack.')
    
    wh_text = f'''-- select role to use, generally Accountadmin or Sysadmin
use role ACCOUNTADMIN;

-- set the name of the installed application
set APP_DATABASE = '{app_name}';

-- create a local database to store the network rule (you can change these to an existing database and schema if you like)
CREATE DATABASE IF NOT EXISTS GENESIS_LOCAL_DB; 
CREATE SCHEMA IF NOT EXISTS GENESIS_LOCAL_DB.SETTINGS;

-- create a network rule that allows Genesis Server to access OpenAI's API, and optionally Slack 
CREATE OR REPLACE NETWORK RULE GENESIS_LOCAL_DB.SETTINGS.GENESIS_RULE
 MODE = EGRESS TYPE = HOST_PORT
VALUE_LIST = ('api.openai.com', 'slack.com', 'www.slack.com', 'wss-primary.slack.com',
'wss-backup.slack.com',  'wss-primary.slack.com:443','wss-backup.slack.com:443');

-- create an external access integration that surfaces the above network rule
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION GENESIS_EAI
   ALLOWED_NETWORK_RULES = (GENESIS_LOCAL_DB.SETTINGS.GENESIS_RULE) ENABLED = true;

-- TEMPORARY: This is needed until st.chat works in SiS, planned for April 18-19, then this can be removed
GRANT BIND SERVICE ENDPOINT ON ACCOUNT TO APPLICATION  IDENTIFIER($APP_DATABASE); 

-- grant Genesis Server the ability to use this external access integration
GRANT USAGE ON INTEGRATION GENESIS_EAI TO APPLICATION   IDENTIFIER($APP_DATABASE);
   
'''

    st.text_area("EAI configuration script:", wh_text, height=520)

    st.write("Once you run the above, you can proceed to the next step to start the Genesis Server.")
    st.write('**<< Now, click 4. Start Genesis Server <<**')


def start_service():

    st.subheader('Step 4: Start Genesis Server')

    st.write("If you've performed the 3 other steps, you're ready to start the Genesis Server.  Press the START SERVER button below to get started.")
    
    if st.button('Start Genesis Server'):
        try:
            # Execute the command and collect the results
            try:
                st.write('Checking for previous installations...')
                drop_result = session.sql(f"call {core_prefix}.drop_app_instance('APP1')")
                if drop_result:
                    st.write(drop_result)
                    a = 1+1
            except Exception as e:
                #st.write(e)
                pass

            wh_test = False
            try:
                st.write('Checking virtual warehouse...')
                warehouses_result = session.sql('SHOW WAREHOUSES').collect()
                # Check if any warehouses were returned
                if warehouses_result:
                    # Convert the list of Row objects to a Pandas DataFrame for display
                    warehouses_df = pd.DataFrame([row.as_dict() for row in warehouses_result])
                    warehouse_names = warehouses_df['name'].tolist()  # Adjust 'name' if necessary to match your column name
                    #st.write(warehouses_result)
                    # Check if 'XSMALL' is in the list of warehouse names
                    if st.session_state.wh_name not in warehouse_names:
                        first_warehouse_name = warehouse_names[0]
                        st.session_state.wh_name = first_warehouse_name
                    st.write(f'Found warehouse {st.session_state.wh_name}.')
                    wh_test = True
            except Exception as e:
                st.write(e)
                st.write(f'No warehouses assigned to app. Please check Step 1 on left.')

            if wh_test:
                try:
                    with st.spinner('Starting Genesis Server (can take 3-5 minutes the first time)...'):
                        start_result = session.sql(f"call {core_prefix}.INITIALIZE_APP_INSTANCE('APP1','GENESIS_POOL','GENESIS_EAI','{st.session_state.wh_name}')")
                        st.write(start_result)
                except Exception as e:
                    st.write(e)

           # st.write('here!')
            # Check if any services were returned
            if wh_test and start_result:
                # Convert the list of Row objects to a Pandas DataFrame for display
                #st.write('here!!!')
                # Display success message with list of warehouses
                st.success(f'Success: Server Started')

                # Add a button to the app
                st.write("**Now push the botton below, you're one step away from making and chatting with your bots!**")
                if st.button('Continue Setup!'):
                    # When the button is clicked, rerun the app from the top
                    st.experimental_rerun()
            else:
                st.error('Server not started.')
        except Exception as e:
            st.error(f'Error connecting to Snowflake: {e}')

   # sql = f"show services "
   # data = session.sql(sql).collect()
   # st.dataframe(data)

def welcome():
    
    st.subheader('Welcome to Genesis Bots!')

    st.write("Before you get started using Genesis Bots, you need to perform 4 steps to give your Genesis App access to things it needs in your Snowflake account. I'll walk you through the steps.  They are:")
    st.write('1. Configure Warehouse -- gives Genesis the ability to run queries on Snowflake')
    st.write('2. Configure Compute Pool -- gives a Snowflake Container Services Compute Pool to use to run its bot server securely inside your Snowflake account')
    st.write('3. Configure EAI -- gives Genesis the ability to connect to external systems like OpenAI (required) and Slack (optional)')
    st.write("4. Start Genesis Server -- this starts up the Genesis Server inside the Compute Pool.")

    st.write()
    st.write("After completing these steps, you'll be able to start talking to Genesis Bots, creating your own Bots, analyzing data with your Bots, and more!")
    st.write('**<< Now, click 1. Configure Warehouse, on left <<**')

def start_stop():
    
    st.subheader('Start / Stop Genesis Server')

    try:

        warehouses_result = session.sql('SHOW WAREHOUSES').collect()
        # Check if any warehouses were returned
        if warehouses_result:
            # Convert the list of Row objects to a Pandas DataFrame for display
            warehouses_df = pd.DataFrame([row.as_dict() for row in warehouses_result])
            warehouse_names = warehouses_df['name'].tolist()  # Adjust 'name' if necessary to match your column name
            #st.write(warehouses_result)
            # Check if 'XSMALL' is in the list of warehouse names
            if st.session_state.wh_name not in warehouse_names:
                first_warehouse_name = warehouse_names[0]
                st.session_state.wh_name = first_warehouse_name
            #st.write(f'Found warehouse {st.session_state.wh_name}.')

    except Exception as e:

        st.session_state.wh_name  = '<your warehouse name>'

    st.write('You can use the below commands in a worksheet to stop, start, and monitor the Gensis Server:')
    start_stop_text = f'''USE DATABASE IDENTIFIER("{app_name}");

// reinitialize
call {app_name}.core.drop_app_instance('APP1');
CALL {app_name}.CORE.INITIALIZE_APP_INSTANCE('APP1','GENESIS_POOL','GENESIS_EAI','{st.session_state.wh_name}');

// pause service

call {app_name}.core.stop_app_instance('APP1');
alter compute pool GENESIS_POOL SUSPEND;

// resume service

alter compute pool GENESIS_POOL RESUME;
call {app_name}.core.start_app_instance('APP1');

// check service

USE DATABASE IDENTIFIER($APP_DATABASE);
USE SCHEMA APP1;

SELECT SYSTEM$GET_SERVICE_STATUS('GENESISAPP_SERVICE_SERVICE');
SELECT SYSTEM$GET_SERVICE_LOGS('GENESISAPP_SERVICE_SERVICE',0,'chattest',1000);

// get endpoints

SHOW ENDPOINTS IN SERVICE GENESISAPP_SERVICE_SERVICE;  --temporary

    '''
    st.text_area("", start_stop_text, height=620)




if SnowMode:
    try:
        sql = f"select {prefix}.list_available_bots() "
        data = session.sql(sql).collect()
    except Exception as e:
        data = None
else:
    data = 'Local Mode'

if data:
    
    pages = {
    #    "Talk to Your Bots": "/",
        "Chat with Bots": chat_page,
        "Setup LLM Model & Key": llm_config,
      #  "Setup Ngrok": setup_ngrok, 
        "Setup Slack Connection": setup_slack,
        "Grant Data Access": grant_data,
        "Harvestable Data": db_add_to_harvester,
        "Harvester Status": db_harvester,
        "Bot Configuration": bot_config,
        "Server Stop/Start": start_stop,
    }
    
    if st.session_state.get('needs_keys', False):
        del pages["Chat with Bots"]
    
 #   if SnowMode == True:
 #       del pages["Setup Ngrok"]
    
    st.sidebar.title("Genesis Bots Configuration")
      
    selection = st.sidebar.radio("Go to:", list(pages.keys()), index=list(pages.keys()).index(st.session_state.get('radio', list(pages.keys())[0])))
    if selection in pages: 
        pages[selection]()
    
else:

    pages = {
    #   
        "Welcome!": welcome,
        "1: Configure Warehouse": config_wh,
        "2: Configure Compute Pool": config_pool,
        "3: Configure EAI": config_eai,
        "4: Start Genesis Server": start_service
    }
    
    st.sidebar.title("Genesis Bots Installation")
      
    selection = st.sidebar.radio("Go to:", list(pages.keys()), index=list(pages.keys()).index(st.session_state.get('radio', list(pages.keys())[0])))
    if selection in pages: 
        pages[selection]()
        

$$)
;
select * from script_tmp;


CREATE OR REPLACE TEMPORARY TABLE script_tmp AS SELECT 'README' NAME,REGEXP_REPLACE($$

# Genesis
Genesis Bots are AI-powered workers that can perform jobs for your company.

## Permissions
In the setup guide, you'll be asked to grant additional privileges from your account.

Once you install Genesis, you will be directed to a Streamlit app, which will walk you through running commands
in your Snowflake account to grant the application access to the following resources:

1. A Snowflake Virtual Warehouse to power Snowflake queries run by Genesis
2. A Snowflake Compute Pool to run the Genesis Server containers
3. A Network Rule and External Access Integration, to allow Genesis to access the following external endpoints:
    - OpenAI API
    - Slack
4. Optionally, acccess to any of your existing Databases, Schemas, and Tables you'd like to use with Genesis.

### Account level privileges

`BIND SERVICE ENDPOINT` on **ACCOUNT**
To allow Genesis to open two endpoints, one for Slack to authorize new Apps via OAuth, and one for inbound
access to the Streamlit Genesis GUI

### Privileges to objects
`USAGE` on **COMPUTE POOL**
To run the Genesis Server containers in Snowpark Conrainer Services

`USAGE` on **WAREHOUSE**
For Genesis to run queries on Snowflake

`USAGE` on **EXTERNAL ACCESS INTEGRATION**
To allow Genesis to access external OpenAI and Slack API endpoints

`USAGE` on **DATABASES, SCHEMAS**
To optionally allow Genesis to work with some of your data

`SELECT` on **TABLES, VIEWS**
To optionally allow Genesis to work with some of your data

---

## Object creation
In the setup guide, you'll be asked to create the following object(s) in your account. 

`WAREHOUSE`XSMALL
For Genesis to use to run queries on Snowflake

`COMPUTE POOL`GENESIS_POOL
For Genesis to use to run its Genesis Server containers

`DATABASE`GENESIS_LOCAL_DB
To store the network rule

`SCHEMA`GENESIS_LOCAL_DB.SETTINGS
To store the network rule

`NETWORK RULE`GENESIS_LOCAL_DB.SETTINGS.GENESIS_RULE
To allow Genesis to access to required external APIs (OpenAI and Slack)

`EXTERNAL ACCESS INTEGRATION`GENESIS_EAI
To allow Genesis to access to required external APIs (OpenAI and Slack)

---

## Setup code

-- Note: Please use the default Streamlit App for a full walkthrough of these steps

-- use a role with sufficient priviliges for the

use role ACCOUNTADMIN;

-- set the name of the installed application and warehouse to use

set APP_DATABASE = 'GENESIS_BOTS_ALPHA';
set APP_WAREHOUSE = 'XSMALL';  -- ok to use an existing warehouse

-- create the warehouse if needed

CREATE WAREHOUSE IF NOT EXISTS IDENTIFIER($APP_WAREHOUSE)
 MIN_CLUSTER_COUNT=1 MAX_CLUSTER_COUNT=1
 WAREHOUSE_SIZE=XSMALL AUTO_RESUME = TRUE AUTO_SUSPEND = 60;

-- allow Genesis to use the warehouse

GRANT USAGE ON WAREHOUSE  IDENTIFIER($APP_WAREHOUSE) TO APPLICATION  IDENTIFIER($APP_DATABASE);

-- remove an existing pool, if you've installed this app before

DROP COMPUTE POOL IF EXISTS GENESIS_POOL;

-- create the compute pool and associate it to this application

CREATE COMPUTE POOL IF NOT EXISTS GENESIS_POOL FOR APPLICATION IDENTIFIER($APP_DATABASE)
 MIN_NODES=1 MAX_NODES=1 INSTANCE_FAMILY='CPU_X64_XS' AUTO_SUSPEND_SECS=3600 INITIALLY_SUSPENDED=FALSE;

-- give Genesis the right to use the compute pool

GRANT USAGE ON COMPUTE POOL GENESIS_POOL TO APPLICATION  IDENTIFIER($APP_DATABASE);

-- create a local database to store the network rule (you can change these to an existing database and schema if you like)

CREATE DATABASE IF NOT EXISTS GENESIS_LOCAL_DB; 
CREATE SCHEMA IF NOT EXISTS GENESIS_LOCAL_DB.SETTINGS;

-- create a network rule that allows Genesis Server to access OpenAI's API, and optionally Slack 

CREATE OR REPLACE NETWORK RULE GENESIS_LOCAL_DB.SETTINGS.GENESIS_RULE
 MODE = EGRESS TYPE = HOST_PORT
VALUE_LIST = ('api.openai.com', 'slack.com', 'www.slack.com', 'wss-primary.slack.com',
'wss-backup.slack.com',  'wss-primary.slack.com:443','wss-backup.slack.com:443');

-- create an external access integration that surfaces the above network rule

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION GENESIS_EAI
   ALLOWED_NETWORK_RULES = (GENESIS_LOCAL_DB.SETTINGS.GENESIS_RULE) ENABLED = true;

-- Allows Slack to access the Genesis server to approve new Genesis Slack Applications

GRANT BIND SERVICE ENDPOINT ON ACCOUNT TO APPLICATION  IDENTIFIER($APP_DATABASE); 

-- grant Genesis Server the ability to use this external access integration

GRANT USAGE ON INTEGRATION GENESIS_EAI TO APPLICATION   IDENTIFIER($APP_DATABASE);

## Setup instructions

Please use the default Streamlit provided with this native application for a fully-guided setup experience.

## Usage Snippets

Please use the default Streamlit to interact with the Genesis application.

$$,':::','$$') VALUE;


INSERT INTO SCRIPT SELECT * FROM SCRIPT_TMP;
select * from script;


CREATE OR REPLACE TEMPORARY TABLE script_tmp AS SELECT 'SETUP' NAME,REGEXP_REPLACE($$
CREATE OR ALTER VERSIONED SCHEMA APP;


CREATE OR REPLACE TABLE APP.YAML (name varchar, value varchar);

INSERT INTO APP.YAML (NAME , VALUE)
VALUES ('GENESISAPP_SERVICE_SERVICE',
:::
    spec:
      containers:
      - name: chattest
        image: /genesisapp_master/code_schema/service_repo/genesis_app:latest
        env:
            OPENAI_MODEL_NAME: gpt-4-1106-preview
            RUNNER_ID: snowflake-1
            GENESIS_INTERNAL_DB_SCHEMA: {{app_db_sch}}
            GENESIS_SOURCE: Snowflake
            SNOWFLAKE_SECURE: FALSE
            OPENAI_HARVESTER_EMBEDDING_MODEL: text-embedding-3-large
            OPENAI_HARVESTER_MODEL: gpt-4-1106-preview
        readinessProbe:
          port: 8080
          path: /healthcheck
      endpoints:
      - name: genesisui
        port: 8501
        public: true
      - name: udfendpoint
        port: 8080
        public: true
:::)
;

INSERT INTO APP.YAML (NAME , VALUE)
VALUES ('GENESISAPP_HARVESTER_SERVICE',
:::
    spec:
      containers:
      - name: genesis-harvester
        image: /genesisapp_master/code_schema/service_repo/genesis_app:latest
        env:
            GENESIS_MODE: HARVESTER
            AUTO_HARVEST: TRUE
            OPENAI_HARVESTER_EMBEDDING_MODEL: text-embedding-3-large
            OPENAI_HARVESTER_MODEL: gpt-4-1106-preview
            HARVESTER_REFRESH_SECONDS: 20
            RUNNER_ID: snowflake-1
            SNOWFLAKE_SECURE: FALSE
            GENESIS_INTERNAL_DB_SCHEMA: {{app_db_sch}}
            GENESIS_SOURCE: Snowflake
      endpoints:
      - name: udfendpoint
        port: 8080
        public: false
:::)
;


--        secrets:
--         - snowflakeSecret: APP_LOCAL_DB.EGRESS.OPENAI_API_KEY
--           secretKeyRef: SECRET_STRING
--           envVarName: OPENAI_API_KEY
--         - snowflakeSecret: APP_LOCAL_DB.EGRESS.NGROK_AUTHTOKEN
--           secretKeyRef: SECRET_STRING
--           envVarName: NGROK_AUTH_TOKEN


CREATE OR REPLACE PROCEDURE APP.WAIT_FOR_STARTUP(INSTANCE_NAME VARCHAR, SERVICE_NAME VARCHAR, MAX_WAIT INTEGER)
RETURNS VARCHAR NOT NULL
LANGUAGE SQL
AS
:::
DECLARE
 SERVICE_STATUS VARCHAR DEFAULT 'READY';
 WAIT INTEGER DEFAULT 0;
 result VARCHAR DEFAULT '';
 C1 CURSOR FOR
   select
     v.value:containerName::varchar container_name
     ,v.value:status::varchar status
     ,v.value:message::varchar message
   from (select parse_json(system$get_service_status(?))) t,
   lateral flatten(input => t.$1) v
   order by container_name;
 SERVICE_START_EXCEPTION EXCEPTION (-20002, 'Failed to start Service. ');
BEGIN
 REPEAT
   LET name VARCHAR := INSTANCE_NAME||'.'||SERVICE_NAME;
   OPEN c1 USING (:name);
   service_status := 'READY';
   FOR record IN c1 DO
     IF ((service_status = 'READY') AND (record.status != 'READY')) THEN
        service_status := record.status;
        result := result || '\n' ||lpad(wait,5)||' '|| record.container_name || ' ' || record.status;
     END IF;
   END FOR;
   CLOSE c1;
   wait := wait + 1;
   SELECT SYSTEM$WAIT(1);
 UNTIL ((service_status = 'READY') OR (service_status = 'FAILED' ) OR ((:max_wait-wait) <= 0))          
 END REPEAT;
 IF (service_status != 'READY') THEN
   RAISE SERVICE_START_EXCEPTION;
 END IF;
 RETURN result || '\n' || service_status;
END;
:::
;


CREATE OR REPLACE PROCEDURE APP.CREATE_SERVER_SERVICE(INSTANCE_NAME VARCHAR,SERVICE_NAME VARCHAR, POOL_NAME VARCHAR, EAI_NAME VARCHAR , WAREHOUSE_NAME VARCHAR, APP_DATABASE VARCHAR)
RETURNS VARCHAR NOT NULL
LANGUAGE SQL
AS
:::
    BEGIN
 LET spec VARCHAR := (
      SELECT REGEXP_REPLACE(VALUE
        ,'{{app_db_sch}}',lower(:APP_DATABASE)||'.'||lower(:INSTANCE_NAME)) AS VALUE
      FROM APP.YAML WHERE NAME=:SERVICE_NAME);
 EXECUTE IMMEDIATE
   'CREATE SERVICE '|| :INSTANCE_NAME ||'.'|| :SERVICE_NAME ||
   ' IN COMPUTE POOL  '|| :POOL_NAME ||
   ' FROM SPECIFICATION  '||chr(36)||chr(36)||'\n'|| :spec ||'\n'||chr(36)||chr(36) ||
   ' QUERY_WAREHOUSE = '||:WAREHOUSE_NAME||
   ' EXTERNAL_ACCESS_INTEGRATIONS = ('||:EAI_NAME||')';
 EXECUTE IMMEDIATE
   'GRANT USAGE ON SERVICE '|| :INSTANCE_NAME ||'.'|| :SERVICE_NAME ||' TO APPLICATION ROLE APP_PUBLIC';
 EXECUTE IMMEDIATE
   'GRANT MONITOR ON SERVICE '|| :INSTANCE_NAME ||'.'|| :SERVICE_NAME || ' TO APPLICATION ROLE APP_PUBLIC';

  
 EXECUTE IMMEDIATE
   'CREATE or replace FUNCTION '|| :INSTANCE_NAME ||'.submit_udf (INPUT_TEXT VARCHAR, THREAD_ID VARCHAR, BOT_ID VARCHAR)  RETURNS varchar SERVICE='|| :INSTANCE_NAME ||'.'|| :SERVICE_NAME ||' ENDPOINT=udfendpoint AS '||chr(39)||'/udf_proxy/submit_udf'||chr(39);

  
 EXECUTE IMMEDIATE
   'CREATE or replace FUNCTION '|| :INSTANCE_NAME ||'.lookup_udf (UU VARCHAR, BOT_ID VARCHAR)  RETURNS varchar SERVICE='|| :INSTANCE_NAME ||'.'|| :SERVICE_NAME ||' ENDPOINT=udfendpoint AS '||chr(39)||'/udf_proxy/lookup_udf'||chr(39);

  
 EXECUTE IMMEDIATE
   'CREATE or replace FUNCTION '|| :INSTANCE_NAME ||'.get_slack_endpoints ()  RETURNS varchar SERVICE='|| :INSTANCE_NAME ||'.'|| :SERVICE_NAME ||' ENDPOINT=udfendpoint AS '||chr(39)||'/udf_proxy/get_slack_tokens'||chr(39);


 EXECUTE IMMEDIATE
   'CREATE or replace FUNCTION '|| :INSTANCE_NAME ||'.list_available_bots ()  RETURNS varchar SERVICE='|| :INSTANCE_NAME ||'.'|| :SERVICE_NAME ||' ENDPOINT=udfendpoint AS '||chr(39)||'/udf_proxy/list_available_bots'||chr(39);

 EXECUTE IMMEDIATE
   'CREATE or replace FUNCTION '|| :INSTANCE_NAME ||'.get_ngrok_tokens ()  RETURNS varchar SERVICE='|| :INSTANCE_NAME ||'.'|| :SERVICE_NAME ||' ENDPOINT=udfendpoint AS '||chr(39)||'/udf_proxy/get_ngrok_tokens'||chr(39);

 EXECUTE IMMEDIATE
   'CREATE or replace FUNCTION '|| :INSTANCE_NAME ||'.get_metadata (metadata_type varchar)  RETURNS varchar SERVICE='|| :INSTANCE_NAME ||'.'|| :SERVICE_NAME ||' ENDPOINT=udfendpoint AS '||chr(39)||'/udf_proxy/get_metadata'||chr(39);
 
 EXECUTE IMMEDIATE
   'CREATE or replace FUNCTION '|| :INSTANCE_NAME ||'.configure_llm (llm_type varchar, api_key varchar)  RETURNS varchar SERVICE='|| :INSTANCE_NAME ||'.'|| :SERVICE_NAME ||' ENDPOINT=udfendpoint AS '||chr(39)||'/udf_proxy/configure_llm'||chr(39);

  EXECUTE IMMEDIATE
   'CREATE or replace FUNCTION '|| :INSTANCE_NAME ||'.configure_slack_app_token (token varchar, refresh varchar)  RETURNS varchar SERVICE='|| :INSTANCE_NAME ||'.'|| :SERVICE_NAME ||' ENDPOINT=udfendpoint AS '||chr(39)||'/udf_proxy/configure_slack_app_token'||chr(39);

     EXECUTE IMMEDIATE
   'CREATE or replace FUNCTION '|| :INSTANCE_NAME ||'.configure_ngrok_token (auth_token varchar, use_domain varchar, domain varchar)  RETURNS varchar SERVICE='|| :INSTANCE_NAME ||'.'|| :SERVICE_NAME ||' ENDPOINT=udfendpoint AS '||chr(39)||'/udf_proxy/configure_ngrok_token'||chr(39);

     EXECUTE IMMEDIATE
   'CREATE or replace FUNCTION '|| :INSTANCE_NAME ||'.deploy_bot (bot_id varchar)  RETURNS varchar SERVICE='|| :INSTANCE_NAME ||'.'|| :SERVICE_NAME ||' ENDPOINT=udfendpoint AS '||chr(39)||'/udf_proxy/deploy_bot'||chr(39);
  
EXECUTE IMMEDIATE
   'GRANT USAGE ON FUNCTION '|| :INSTANCE_NAME ||'.deploy_bot ( varchar )  TO APPLICATION ROLE APP_PUBLIC';

 
EXECUTE IMMEDIATE
   'GRANT USAGE ON FUNCTION '|| :INSTANCE_NAME ||'.configure_ngrok_token ( varchar, varchar, varchar)  TO APPLICATION ROLE APP_PUBLIC';

 EXECUTE IMMEDIATE
   'GRANT USAGE ON FUNCTION '|| :INSTANCE_NAME ||'.configure_slack_app_token ( varchar, varchar)  TO APPLICATION ROLE APP_PUBLIC';

 EXECUTE IMMEDIATE
   'GRANT USAGE ON FUNCTION '|| :INSTANCE_NAME ||'.configure_llm ( varchar, varchar)  TO APPLICATION ROLE APP_PUBLIC';
 EXECUTE IMMEDIATE
   'GRANT USAGE ON FUNCTION '|| :INSTANCE_NAME ||'.submit_udf ( varchar, varchar, varchar)  TO APPLICATION ROLE APP_PUBLIC';
 EXECUTE IMMEDIATE
   'GRANT USAGE ON FUNCTION '|| :INSTANCE_NAME ||'.lookup_udf ( varchar, varchar)  TO APPLICATION ROLE APP_PUBLIC';
 EXECUTE IMMEDIATE
   'GRANT USAGE ON FUNCTION '|| :INSTANCE_NAME ||'.get_slack_endpoints ( )  TO APPLICATION ROLE APP_PUBLIC';
 EXECUTE IMMEDIATE
   'GRANT USAGE ON FUNCTION '|| :INSTANCE_NAME ||'.list_available_bots ( )  TO APPLICATION ROLE APP_PUBLIC';
 EXECUTE IMMEDIATE
   'GRANT USAGE ON FUNCTION '|| :INSTANCE_NAME ||'.get_ngrok_tokens ( )  TO APPLICATION ROLE APP_PUBLIC';
 EXECUTE IMMEDIATE
   'GRANT USAGE ON FUNCTION '|| :INSTANCE_NAME ||'.get_metadata (varchar )  TO APPLICATION ROLE APP_PUBLIC';


 RETURN 'service created';
END
:::
;



CREATE OR REPLACE PROCEDURE APP.CREATE_HARVESTER_SERVICE(INSTANCE_NAME VARCHAR,SERVICE_NAME VARCHAR, POOL_NAME VARCHAR, EAI_NAME VARCHAR , WAREHOUSE_NAME VARCHAR, APP_DATABASE VARCHAR)
RETURNS VARCHAR NOT NULL
LANGUAGE SQL
AS
:::
    BEGIN
 LET spec VARCHAR := (
      SELECT REGEXP_REPLACE(VALUE
        ,'{{app_db_sch}}',lower(:APP_DATABASE)||'.'||lower(:INSTANCE_NAME)) AS VALUE
      FROM APP.YAML WHERE NAME=:SERVICE_NAME);
 EXECUTE IMMEDIATE
   'CREATE SERVICE '|| :INSTANCE_NAME ||'.'|| :SERVICE_NAME ||
   ' IN COMPUTE POOL  '|| :POOL_NAME ||
   ' FROM SPECIFICATION  '||chr(36)||chr(36)||'\n'|| :spec ||'\n'||chr(36)||chr(36) ||
   ' QUERY_WAREHOUSE = '||:WAREHOUSE_NAME||
   ' EXTERNAL_ACCESS_INTEGRATIONS = ('||:EAI_NAME||')';
 EXECUTE IMMEDIATE
   'GRANT USAGE ON SERVICE '|| :INSTANCE_NAME ||'.'|| :SERVICE_NAME ||' TO APPLICATION ROLE APP_PUBLIC';
 EXECUTE IMMEDIATE
   'GRANT MONITOR ON SERVICE '|| :INSTANCE_NAME ||'.'|| :SERVICE_NAME || ' TO APPLICATION ROLE APP_PUBLIC';

 RETURN 'service created';
END
:::
;

CREATE APPLICATION ROLE IF NOT EXISTS APP_PUBLIC;
CREATE OR ALTER VERSIONED SCHEMA CORE;
GRANT USAGE ON SCHEMA CORE TO APPLICATION ROLE APP_PUBLIC;


CREATE OR REPLACE PROCEDURE CORE.INITIALIZE_APP_INSTANCE( INSTANCE_NAME VARCHAR, POOL_NAME VARCHAR, EAI_NAME VARCHAR, APP_WAREHOUSE VARCHAR)
RETURNS STRING
LANGUAGE SQL
AS
:::
DECLARE
    v_current_database STRING;
BEGIN
  SELECT CURRENT_DATABASE() INTO :v_current_database;

  EXECUTE IMMEDIATE 'CREATE SCHEMA '||:INSTANCE_NAME;
  EXECUTE IMMEDIATE 'GRANT USAGE ON SCHEMA '||:INSTANCE_NAME||' TO APPLICATION ROLE APP_PUBLIC';


  EXECUTE IMMEDIATE 'CREATE STAGE IF NOT EXISTS '||:INSTANCE_NAME||'.'||'WORKSPACE DIRECTORY = ( ENABLE = true ) ENCRYPTION = (TYPE = '||CHR(39)||'SNOWFLAKE_SSE'||chr(39)||')';
  EXECUTE IMMEDIATE 'GRANT READ ON STAGE '||:INSTANCE_NAME||'.'||'WORKSPACE TO APPLICATION ROLE APP_PUBLIC';


  CALL APP.CREATE_SERVER_SERVICE(:INSTANCE_NAME,'GENESISAPP_SERVICE_SERVICE',:POOL_NAME, :EAI_NAME, :APP_WAREHOUSE, :v_current_database);
  CALL APP.CREATE_HARVESTER_SERVICE(:INSTANCE_NAME,'GENESISAPP_HARVESTER_SERVICE',:POOL_NAME, :EAI_NAME, :APP_WAREHOUSE, :v_current_database);
  CALL APP.WAIT_FOR_STARTUP(:INSTANCE_NAME,'GENESISAPP_SERVICE_SERVICE',600);
  
  RETURN :v_current_database||'.'||:INSTANCE_NAME||'.GENESISAPP_SERVICE_SERVICE';
  
END
:::
;


GRANT USAGE ON PROCEDURE CORE.INITIALIZE_APP_INSTANCE(VARCHAR, VARCHAR, VARCHAR, VARCHAR) TO  APPLICATION ROLE APP_PUBLIC;



CREATE OR REPLACE PROCEDURE CORE.GET_APP_ENDPOINT(INSTANCE_NAME VARCHAR)
RETURNS TABLE(VARCHAR, INTEGER, VARCHAR, VARCHAR, VARCHAR  )
LANGUAGE SQL
AS
:::
BEGIN
 EXECUTE IMMEDIATE 'create or replace table '||:INSTANCE_NAME||'.ENDPOINT (name varchar, port integer, protocol varchar, ingress_enabled varchar, ingress_url varchar)';
 LET stmt VARCHAR := 'SELECT "name" AS SERVICE_NAME, "schema_name" AS SCHEMA_NAME FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))';
 LET RS0 RESULTSET := (EXECUTE IMMEDIATE 'SHOW SERVICES IN SCHEMA '||:INSTANCE_NAME);
 LET RS1 RESULTSET := (EXECUTE IMMEDIATE :stmt);
 LET C1 CURSOR FOR RS1;
 FOR REC IN C1 DO
   LET RS2 RESULTSET := (EXECUTE IMMEDIATE 'SHOW ENDPOINTS IN SERVICE '||rec.schema_name||'.'||rec.service_name);
   EXECUTE IMMEDIATE 'INSERT INTO '||:INSTANCE_NAME||'.ENDPOINT SELECT "name","port","protocol","ingress_enabled","ingress_url" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))';
 END FOR;
 LET RS3 RESULTSET := (EXECUTE IMMEDIATE 'SELECT name, port, protocol, ingress_enabled, ingress_url FROM '||:INSTANCE_NAME||'.ENDPOINT');
 RETURN TABLE(RS3); 
END;
:::
;
GRANT USAGE ON PROCEDURE CORE.GET_APP_ENDPOINT(VARCHAR) TO  APPLICATION ROLE APP_PUBLIC;


CREATE OR REPLACE PROCEDURE CORE.START_APP_INSTANCE(INSTANCE_NAME VARCHAR)
RETURNS TABLE(SERVICE_NAME VARCHAR,CONTAINER_NAME VARCHAR,STATUS VARCHAR, MESSAGE VARCHAR)
LANGUAGE SQL
AS
:::
BEGIN
 LET stmt VARCHAR := 'SELECT "name" as SERVICE_NAME, "schema_name" AS SCHEMA_NAME FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))';
 EXECUTE IMMEDIATE 'SHOW SERVICES IN SCHEMA ' ||:INSTANCE_NAME;
 LET RS1 RESULTSET := (EXECUTE IMMEDIATE :stmt);
 LET c1 CURSOR FOR RS1;
 FOR rec IN c1 DO
   EXECUTE IMMEDIATE 'ALTER SERVICE IF EXISTS '||rec.schema_name||'.'||rec.service_name||' resume';
   EXECUTE IMMEDIATE 'CALL APP.WAIT_FOR_STARTUP(\''||rec.schema_name||'\',\''||rec.service_name||'\',300)';
 END FOR;
 LET RS3 RESULTSET := (CALL CORE.LIST_APP_INSTANCE(:INSTANCE_NAME));
 RETURN TABLE(RS3);
END;
:::
;
GRANT USAGE ON PROCEDURE CORE.START_APP_INSTANCE(VARCHAR) TO  APPLICATION ROLE APP_PUBLIC;


CREATE OR REPLACE PROCEDURE CORE.STOP_APP_INSTANCE(INSTANCE_NAME VARCHAR)
RETURNS TABLE(SERVICE_NAME VARCHAR,CONTAINER_NAME VARCHAR,STATUS VARCHAR, MESSAGE VARCHAR)
LANGUAGE SQL
AS
:::
BEGIN
 LET stmt VARCHAR := 'SELECT "name" as SERVICE_NAME, "schema_name" AS SCHEMA_NAME FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))';
 EXECUTE IMMEDIATE 'SHOW SERVICES IN SCHEMA ' ||:INSTANCE_NAME;
 LET RS1 RESULTSET := (EXECUTE IMMEDIATE :stmt);
 LET c1 CURSOR FOR RS1;
 FOR rec IN c1 DO
   EXECUTE IMMEDIATE 'ALTER SERVICE IF EXISTS '||rec.schema_name||'.'||rec.service_name||' suspend';
 END FOR;
 LET RS3 RESULTSET := (CALL CORE.LIST_APP_INSTANCE(:INSTANCE_NAME));
 RETURN TABLE(RS3);
END;
:::
;
GRANT USAGE ON PROCEDURE CORE.STOP_APP_INSTANCE(VARCHAR) TO  APPLICATION ROLE APP_PUBLIC;


CREATE OR REPLACE PROCEDURE CORE.DROP_APP_INSTANCE(INSTANCE_NAME VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
AS
:::
BEGIN
 LET stmt VARCHAR := 'SELECT "name" as SERVICE_NAME, "schema_name" AS SCHEMA_NAME FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))';
 EXECUTE IMMEDIATE 'SHOW SERVICES IN SCHEMA ' ||:INSTANCE_NAME;
 LET RS1 RESULTSET := (EXECUTE IMMEDIATE :stmt);
 LET c1 CURSOR FOR RS1;
 FOR rec IN c1 DO
   EXECUTE IMMEDIATE 'DROP SERVICE IF EXISTS '||rec.schema_name||'.'||rec.service_name;
 END FOR;
 DROP SCHEMA IDENTIFIER(:INSTANCE_NAME);
 RETURN 'The instance with name '||:INSTANCE_NAME||' has been dropped';
END;
:::
;
GRANT USAGE ON PROCEDURE CORE.DROP_APP_INSTANCE(VARCHAR) TO APPLICATION ROLE APP_PUBLIC;


CREATE OR REPLACE PROCEDURE CORE.RESTART_APP_INSTANCE(INSTANCE_NAME VARCHAR)
RETURNS TABLE(SERVICE_NAME VARCHAR,CONTAINER_NAME VARCHAR,STATUS VARCHAR, MESSAGE VARCHAR)
LANGUAGE SQL
AS
:::
BEGIN
 LET stmt VARCHAR := 'SELECT "name" as SERVICE_NAME, "schema_name" AS SCHEMA_NAME FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))';
 EXECUTE IMMEDIATE 'SHOW SERVICES IN SCHEMA ' ||:INSTANCE_NAME;
 LET RS1 RESULTSET := (EXECUTE IMMEDIATE :stmt);
 LET c1 CURSOR FOR RS1;
 FOR rec IN c1 DO
   EXECUTE IMMEDIATE 'ALTER SERVICE IF EXISTS '||rec.schema_name||'.'||rec.service_name||' suspend';
   SELECT SYSTEM$WAIT(5);   
   EXECUTE IMMEDIATE 'ALTER SERVICE IF EXISTS '||rec.schema_name||'.'||rec.service_name||' resume';
   EXECUTE IMMEDIATE 'CALL APP.WAIT_FOR_STARTUP(\''||rec.schema_name||'\',\''||rec.service_name||'\',300)';
 END FOR;
 LET RS3 RESULTSET := (CALL CORE.LIST_APP_INSTANCE(:INSTANCE_NAME));
 RETURN TABLE(RS3);
END;
:::
;
GRANT USAGE ON PROCEDURE CORE.RESTART_APP_INSTANCE(VARCHAR) TO APPLICATION ROLE APP_PUBLIC;








CREATE OR REPLACE PROCEDURE CORE.TEST_BILLING_EVENT()
RETURNS VARCHAR
LANGUAGE SQL
AS
:::
BEGIN
 EXECUTE IMMEDIATE 'SELECT SYSTEM$CREATE_BILLING_EVENT(\'TEST_BILL_EVENT\',\'\',CURRENT_TIMESTAMP(),CURRENT_TIMESTAMP(),10,\'\',\'\')';
 RETURN 'BILLED'; 
END;
:::
;


GRANT USAGE ON PROCEDURE CORE.TEST_BILLING_EVENT() TO  APPLICATION ROLE APP_PUBLIC;



CREATE OR REPLACE PROCEDURE CORE.LIST_APP_INSTANCE(INSTANCE_NAME VARCHAR)
RETURNS TABLE(SERVICE_NAME VARCHAR,CONTAINER_NAME VARCHAR,STATUS VARCHAR, MESSAGE VARCHAR)
LANGUAGE SQL
AS
:::
BEGIN
 EXECUTE IMMEDIATE 'create or replace table '||:INSTANCE_NAME||'.CONTAINER (service_name varchar, container_name varchar, status varchar, message varchar)';
 LET stmt VARCHAR := 'SELECT "name" AS SERVICE_NAME, "schema_name" AS SCHEMA_NAME FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))';
 LET RS0 RESULTSET := (EXECUTE IMMEDIATE 'SHOW SERVICES IN SCHEMA '||:INSTANCE_NAME);
 LET RS1 RESULTSET := (EXECUTE IMMEDIATE :stmt);
 LET C1 CURSOR FOR RS1;
 FOR REC IN C1 DO
   EXECUTE IMMEDIATE 'INSERT INTO '||:INSTANCE_NAME||'.CONTAINER '||
                     '  SELECT \''||rec.schema_name||'.'||rec.service_name||'\'::varchar service_name'||
                     '         , value:containerName::varchar container_name, value:status::varchar status, value:message::varchar message '||
                     '  FROM TABLE(FLATTEN(PARSE_JSON(SYSTEM$GET_SERVICE_STATUS(\''||rec.schema_name||'.'||rec.service_name||'\'))))'; 
 END FOR;
 LET RS3 RESULTSET := (EXECUTE IMMEDIATE 'SELECT service_name, container_name, status, message FROM '||:INSTANCE_NAME||'.CONTAINER');
 RETURN TABLE(RS3); 
END;
:::
;

GRANT USAGE ON PROCEDURE CORE.LIST_APP_INSTANCE(VARCHAR) TO APPLICATION ROLE APP_PUBLIC;


CREATE OR REPLACE PROCEDURE CORE.GET_POOLS()
RETURNS TABLE(NAME VARCHAR, STATE VARCHAR)
LANGUAGE SQL
AS
:::
BEGIN
 LET stmt VARCHAR := 'SELECT NAME, STATE FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))';
 EXECUTE IMMEDIATE 'SHOW COMPUTE POOLS';
 LET RS1 RESULTSET := (EXECUTE IMMEDIATE :stmt);
 RETURN TABLE(RS1);
END;
:::
;
GRANT USAGE ON PROCEDURE CORE.GET_POOLS() TO APPLICATION ROLE APP_PUBLIC;



CREATE OR REPLACE STREAMLIT CORE.SIS_LAUNCH
    FROM '/code_artifacts/streamlit'
    MAIN_FILE = '/sis_launch.py';
 

GRANT USAGE ON STREAMLIT CORE.SIS_LAUNCH TO APPLICATION ROLE app_public;

CREATE OR REPLACE PROCEDURE CORE.RUN_ARBITRARY(sql_query VARCHAR)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
:::
    // Prepare a statement using the provided SQL query
    var statement = snowflake.createStatement({sqlText: SQL_QUERY});
    
    // Execute the statement
    var result_set = statement.execute();
    
    // Initialize an array to hold each row's data
    var rows = [];
    
    // Iterate over each row in the result set
    while (result_set.next()) {
        // Initialize an object to store the current row's data
        var row = {};
        
        // Iterate over each column in the current row
        for (var colIdx = 1; colIdx <= result_set.getColumnCount(); colIdx++) {
            // Get the column name and value
            var columnName = result_set.getColumnName(colIdx);
            var columnValue = result_set.getColumnValue(colIdx);
            
            // Add the column name and value to the current row's object
            row[columnName] = columnValue;
        }
        
        // Add the current row's object to the rows array
        rows.push(row);
    }
    
    // Convert the rows array to a JSON string
    var jsonResult = JSON.stringify(rows);
    
    // Return the JSON string
    // Note: Snowflake automatically converts the returned string to a VARIANT (JSON) data type
    return JSON.parse(jsonResult);
:::;


 
GRANT USAGE ON PROCEDURE CORE.RUN_ARBITRARY(VARCHAR) TO APPLICATION ROLE app_public;

$$,':::','$$') VALUE;


USE SCHEMA GENESISAPP_APP_PKG_EXT.CODE_SCHEMA;
DELETE FROM SCRIPT;
INSERT INTO SCRIPT SELECT * FROM SCRIPT_TMP;

--delete from script;
INSERT INTO SCRIPT SELECT * FROM SCRIPT_TMP;


-- ########## SCRIPTS CONTENT  ###########################################
select * from script;


-- ########## BEGIN REPO PERMISSIONS  ####################################


USE SCHEMA GENESISAPP_APP_PKG_EXT.CODE_SCHEMA;


-- ########## END REPO PERMISSIONS  ######################################


-- ########## BEGIN UPLOAD FILES TO APP STAGE ############################


rm @app_code_stage;


CALL CODE_SCHEMA.PUT_TO_STAGE('APP_CODE_STAGE','manifest.yml',(SELECT VALUE FROM CODE_SCHEMA.SCRIPT WHERE NAME = 'MANIFEST'));
CALL CODE_SCHEMA.GET_FROM_STAGE('APP_CODE_STAGE','manifest.yml');
CALL CODE_SCHEMA.PUT_TO_STAGE('APP_CODE_STAGE','setup_script.sql', (SELECT VALUE FROM CODE_SCHEMA.SCRIPT WHERE NAME = 'SETUP'));
CALL CODE_SCHEMA.GET_FROM_STAGE('APP_CODE_STAGE','setup_script.sql');
CALL CODE_SCHEMA.PUT_TO_STAGE('APP_CODE_STAGE','readme.md', (SELECT VALUE FROM CODE_SCHEMA.SCRIPT WHERE NAME = 'README'));
CALL CODE_SCHEMA.GET_FROM_STAGE('APP_CODE_STAGE','readme.md');
CALL CODE_SCHEMA.PUT_TO_STAGE_SUBDIR('APP_CODE_STAGE','code_artifacts/streamlit','sis_launch.py', (SELECT VALUE FROM CODE_SCHEMA.SCRIPT WHERE NAME = 'SIS_APP'));
CALL CODE_SCHEMA.GET_FROM_STAGE_SUBDIR('APP_CODE_STAGE','code_artifacts/streamlit','sis_launch.py');
CALL CODE_SCHEMA.PUT_TO_STAGE_SUBDIR('APP_CODE_STAGE','code_artifacts/streamlit','environment.yml', (SELECT VALUE FROM CODE_SCHEMA.SCRIPT WHERE NAME = 'SIS_ENV'));
CALL CODE_SCHEMA.GET_FROM_STAGE_SUBDIR('APP_CODE_STAGE','code_artifacts/streamlit','environment.yml');


ls @APP_CODE_STAGE;


-- ########## END UPLOAD FILES TO APP STAGE ##############################


-- ########## BEGIN CREATE RELEASE / PATCH  ##############################


BEGIN
LET rs0 RESULTSET := (EXECUTE IMMEDIATE 'ALTER APPLICATION PACKAGE GENESISAPP_APP_PKG_EXT ADD VERSION V0_1 USING @GENESISAPP_APP_PKG_EXT.CODE_SCHEMA.APP_CODE_STAGE');
RETURN TABLE(rs0);
EXCEPTION
 WHEN OTHER THEN
   LET rs1 RESULTSET := (EXECUTE IMMEDIATE 'ALTER APPLICATION PACKAGE GENESISAPP_APP_PKG_EXT ADD PATCH FOR VERSION V0_1 USING @GENESISAPP_APP_PKG_EXT.CODE_SCHEMA.APP_CODE_STAGE');
   RETURN TABLE(rs1);
END;
;


-- ########## END CREATE RELEASE / PATCH  ################################




// provider test

select current_role();

DROP APPLICATION IF EXISTS GENESIS_BOTS_ALPHA;
show applications;

show compute pools;
drop compute pool genesis_test_pool;
drop service genesis_server;
SHOW SERVICES IN COMPUTE POOL GENESIS_TEST_POOL;
ALTER COMPUTE POOL GENESIS_TEST_POOL STOP ALL;

SET APP_DATABASE='GENESIS_BOTS_ALPHA';
CREATE APPLICATION GENESIS_BOTS_ALPHA FROM APPLICATION PACKAGE GENESISAPP_APP_PKG_EXT USING VERSION V0_1;

call GENESISAPP_APP.core.get_eai();
// to get streamlit up and running

// none?

// settings

set APP_DATABASE = 'GENESIS_BOTS_ALPHA';
use database IDENTIFIER($APP_DATABASE);
set APP_INSTANCE='APP1'; -- Do not change
set APP_COMPUTE_POOL='APP_COMPUTE_POOL'||$APP_INSTANCE;
set APP_INSTANCE_FAMILY='CPU_X64_XS';
set APP_LOCAL_DB='APP_LOCAL_DB'; -- For now, do not change, Secrets are hard-wired to this Database in YAML
set APP_LOCAL_SCHEMA=$APP_LOCAL_DB||'.'||'EGRESS'; -- For now, do not change, Secrets are hard-wired to this Schema in YAML
set APP_LOCAL_EGRESS_RULE=$APP_LOCAL_SCHEMA||'.'||'APP_RULE';
set APP_LOCAL_EAI = $APP_DATABASE||'_EAI';
set APP_WAREHOUSE = 'XSMALL'; -- change to an existing Warehouse if desired

// compute pool
use role accountadmin;


CREATE DATABASE IF NOT EXISTS GENESIS_LOCAL_DB; 
CREATE SCHEMA IF NOT EXISTS GENESIS_LOCAL_DB.SETTINGS;

-- create a network rule that allows Genesis Server to access OpenAI's API, and optionally Slack 
CREATE OR REPLACE NETWORK RULE GENESIS_LOCAL_DB.SETTINGS.GENESIS_RULE
 MODE = EGRESS TYPE = HOST_PORT
VALUE_LIST = ('api.openai.com', 'slack.com', 'www.slack.com', 'wss-primary.slack.com',
'wss-backup.slack.com',  'wss-primary.slack.com:443','wss-backup.slack.com:443');

-- create an external access integration that surfaces the above network rule
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION GENESIS_EAI
   ALLOWED_NETWORK_RULES = (GENESIS_LOCAL_DB.SETTINGS.GENESIS_RULE) ENABLED = true;

-- TEMPORARY: This is needed until st.chat works in SiS, planned for April 18-19, then this can be removed
GRANT BIND SERVICE ENDPOINT ON ACCOUNT TO APPLICATION  IDENTIFIER($APP_DATABASE); 

-- grant Genesis Server the ability to use this external access integration
GRANT USAGE ON INTEGRATION GENESIS_EAI TO APPLICATION   IDENTIFIER($APP_DATABASE);
   


DROP COMPUTE POOL IF EXISTS IDENTIFIER($APP_COMPUTE_POOL);
CREATE COMPUTE POOL IF NOT EXISTS IDENTIFIER($APP_COMPUTE_POOL) FOR APPLICATION IDENTIFIER($APP_DATABASE)
 MIN_NODES=1
 MAX_NODES=1
 INSTANCE_FAMILY='CPU_X64_XS';

// network egress for openai, ngrok, slack
 
CREATE OR REPLACE NETWORK RULE IDENTIFIER($APP_LOCAL_EGRESS_RULE)
 MODE = EGRESS
 TYPE = HOST_PORT
  VALUE_LIST = ('api.openai.com', 'connect.ngrok-agent.com:443', 'slack.com', 'api.slack.com')
  //('0.0.0.0:443','0.0.0.0:80');

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION IDENTIFIER($APP_LOCAL_EAI)
   ALLOWED_NETWORK_RULES = (APP_LOCAL_DB.EGRESS.APP_RULE, APP_LOCAL_DB.EGRESS.APP_INGRES_RULE)  -- update from above if necessary
   ALLOWED_AUTHENTICATION_SECRETS = (APP_LOCAL_DB.EGRESS.OPENAI_API_KEY, APP_LOCAL_DB.EGRESS.NGROK_AUTHTOKEN) -- update from above if necessary
   ENABLED = true;

// grants
use role app_owner_role;
grant all  on INTEGRATION IDENTIFIER($APP_LOCAL_EAI) to role accountadmin;


GRANT USAGE ON DATABASE IDENTIFIER($APP_LOCAL_DB) TO APPLICATION IDENTIFIER($APP_DATABASE);
GRANT USAGE ON SCHEMA IDENTIFIER($APP_LOCAL_SCHEMA) TO APPLICATION IDENTIFIER($APP_DATABASE);
GRANT USAGE ON INTEGRATION GENESIS_EAI TO APPLICATION   IDENTIFIER($APP_DATABASE);
GRANT USAGE ON COMPUTE POOL GENESIS_TEST_POOL TO APPLICATION  IDENTIFIER($APP_DATABASE);
GRANT BIND SERVICE ENDPOINT ON ACCOUNT TO APPLICATION  IDENTIFIER($APP_DATABASE);
GRANT USAGE ON WAREHOUSE  IDENTIFIER($APP_WAREHOUSE) TO APPLICATION  IDENTIFIER($APP_DATABASE);

// start
use role accountadmin;

show compute pools;

USE DATABASE IDENTIFIER($APP_DATABASE);
CALL CORE.INITIALIZE_APP_INSTANCE($APP_INSTANCE,'GENESIS_TEST_POOL','GENESIS_EAI',$APP_WAREHOUSE);
call core.drop_app_instance('APP1');
// check service

show services;

USE DATABASE IDENTIFIER($APP_DATABASE);
use schema app1;
SELECT SYSTEM$GET_SERVICE_STATUS('GENESISAPP_SERVICE_SERVICE');
SELECT SYSTEM$GET_SERVICE_LOGS('GENESISAPP_SERVICE_SERVICE',0,'chattest',1000);



/////////////

   
/// consumer instructions

d
SELECT CURRENT_ROLE(); -- Use the same role that installed the Application
-- USE ROLE ACCOUNTADMIN; -- Use the same role that installed the Application
-- USE DATABASE CEB_TEST;


set OPENAI_API_KEY = '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';
set NGROK_AUTHTOKEN = '2ce4bWGvzt5lBCDn6c2WsymnVSr_3m7QssHXhUHLi1BVCguRN';

set APP_DATABASE = 'GENESISAPP_APP';
use database IDENTIFIER($APP_DATABASE);
set APP_INSTANCE='APP1'; -- Do not change
set APP_COMPUTE_POOL='APP_COMPUTE_POOL'||$APP_INSTANCE;
set APP_INSTANCE_FAMILY='CPU_X64_XS';
set APP_LOCAL_DB='APP_LOCAL_DB'; -- For now, do not change, Secrets are hard-wired to this Database in YAML
set APP_LOCAL_SCHEMA=$APP_LOCAL_DB||'.'||'EGRESS'; -- For now, do not change, Secrets are hard-wired to this Schema in YAML
set APP_LOCAL_EGRESS_RULE=$APP_LOCAL_SCHEMA||'.'||'APP_RULE';
set OPENAI_SECRET_NAME=$APP_LOCAL_SCHEMA||'.'||'OPENAI_API_KEY';  -- Do not change
set NGROK_SECRET_NAME=$APP_LOCAL_SCHEMA||'.'||'NGROK_AUTHTOKEN';  -- Do not change
set APP_LOCAL_EAI = $APP_DATABASE||'_EAI';
set EXAMPLE_DATA_DB = 'MY_DATA';
set EXAMPLE_DATA_SCHEMA=$EXAMPLE_DATA_DB||'.'||'EXAMPLE';
set EXAMPLE_DATA_TABLE=$EXAMPLE_DATA_SCHEMA||'.'||'CUSTOMERS';
set APP_WAREHOUSE = 'XSMALL'; -- change to an existing Warehouse if desired


CREATE WAREHOUSE IF NOT EXISTS IDENTIFIER($APP_WAREHOUSE)
 WITH WAREHOUSE_SIZE = 'XSMALL'
 AUTO_SUSPEND = 60
 AUTO_RESUME = TRUE
 INITIALLY_SUSPENDED = TRUE;


USE WAREHOUSE IDENTIFIER($APP_WAREHOUSE);




DROP COMPUTE POOL IF EXISTS IDENTIFIER($APP_COMPUTE_POOL);
CREATE COMPUTE POOL IF NOT EXISTS IDENTIFIER($APP_COMPUTE_POOL) FOR APPLICATION IDENTIFIER($APP_DATABASE)
 MIN_NODES=1
 MAX_NODES=1
 INSTANCE_FAMILY='CPU_X64_XS';

describe compute pool IDENTIFIER($APP_COMPUTE_POOL);

CREATE DATABASE IF NOT EXISTS IDENTIFIER($APP_LOCAL_DB);
CREATE SCHEMA IF NOT EXISTS IDENTIFIER($APP_LOCAL_SCHEMA);

CREATE OR REPLACE NETWORK RULE IDENTIFIER($APP_LOCAL_EGRESS_RULE)
 MODE = EGRESS
 TYPE = HOST_PORT
  VALUE_LIST = ('api.openai.com', 'connect.ngrok-agent.com:443', 'slack.com', 'api.slack.com')
  
//('0.0.0.0:443','0.0.0.0:80');

  
CREATE OR REPLACE SECRET IDENTIFIER($OPENAI_SECRET_NAME)
 TYPE = GENERIC_STRING
 SECRET_STRING = $OPENAI_API_KEY;


CREATE OR REPLACE SECRET IDENTIFIER($NGROK_SECRET_NAME)
 TYPE = GENERIC_STRING
 SECRET_STRING = $NGROK_AUTHTOKEN;


SELECT $APP_LOCAL_EGRESS_RULE, $OPENAI_SECRET_NAME, $NGROK_SECRET_NAME; -- update below CREATE statement if necessary
 CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION IDENTIFIER($APP_LOCAL_EAI)
   ALLOWED_NETWORK_RULES = (APP_LOCAL_DB.EGRESS.APP_RULE, APP_LOCAL_DB.EGRESS.APP_INGRES_RULE)  -- update from above if necessary
   ALLOWED_AUTHENTICATION_SECRETS = (APP_LOCAL_DB.EGRESS.OPENAI_API_KEY, APP_LOCAL_DB.EGRESS.NGROK_AUTHTOKEN) -- update from above if necessary
   ENABLED = true;



CREATE DATABASE IF NOT EXISTS IDENTIFIER($EXAMPLE_DATA_DB);
CREATE SCHEMA IF NOT EXISTS IDENTIFIER($EXAMPLE_DATA_SCHEMA);



CREATE OR REPLACE TABLE IDENTIFIER($EXAMPLE_DATA_TABLE) (
   customer_id INTEGER,
   first_name VARCHAR,
   last_name VARCHAR,
   email VARCHAR,
   signup_date DATE,
   is_active BOOLEAN,
   city VARCHAR,
   state VARCHAR,
   customer_segment VARCHAR
);


SELECT $APP_WAREHOUSE;
USE WAREHOUSE IDENTIFIER($APP_WAREHOUSE);


INSERT INTO IDENTIFIER($EXAMPLE_DATA_TABLE) (customer_id, first_name, last_name, email, signup_date, is_active, city, state, customer_segment) VALUES
(1, 'John', 'Doe', 'john.doe@example.com', '2023-01-01', TRUE, 'New York', 'NY', 'Premium'),
(2, 'Jane', 'Doe', 'jane.doe@example.com', '2023-02-01', TRUE, 'Los Angeles', 'CA', 'Standard'),
(3, 'Jim', 'Beam', 'jim.beam@example.com', '2023-03-01', FALSE, 'Chicago', 'IL', 'Standard'),
(4, 'Jack', 'Daniels', 'jack.daniels@example.com', '2023-01-15', TRUE, 'Houston', 'TX', 'Premium'),
(5, 'Jill', 'Hill', 'jill.hill@example.com', '2023-02-15', FALSE, 'Phoenix', 'AZ', 'Standard');


GRANT USAGE ON DATABASE IDENTIFIER($EXAMPLE_DATA_DB) TO APPLICATION IDENTIFIER($APP_DATABASE);
GRANT USAGE ON SCHEMA IDENTIFIER($EXAMPLE_DATA_SCHEMA) TO APPLICATION IDENTIFIER($APP_DATABASE);
GRANT SELECT ON ALL TABLES IN SCHEMA IDENTIFIER($EXAMPLE_DATA_SCHEMA) TO APPLICATION IDENTIFIER($APP_DATABASE);
GRANT SELECT ON ALL VIEWS IN SCHEMA IDENTIFIER($EXAMPLE_DATA_SCHEMA) TO APPLICATION IDENTIFIER($APP_DATABASE);
GRANT USAGE ON DATABASE IDENTIFIER($APP_LOCAL_DB) TO APPLICATION IDENTIFIER($APP_DATABASE);
GRANT USAGE ON SCHEMA IDENTIFIER($APP_LOCAL_SCHEMA) TO APPLICATION IDENTIFIER($APP_DATABASE);
GRANT USAGE ON INTEGRATION IDENTIFIER($APP_LOCAL_EAI) TO APPLICATION   IDENTIFIER($APP_DATABASE);
GRANT READ ON SECRET  IDENTIFIER($OPENAI_SECRET_NAME) TO APPLICATION  IDENTIFIER($APP_DATABASE);
GRANT READ ON SECRET IDENTIFIER($NGROK_SECRET_NAME) TO APPLICATION   IDENTIFIER($APP_DATABASE);
GRANT USAGE ON COMPUTE POOL  IDENTIFIER($APP_COMPUTE_POOL) TO APPLICATION  IDENTIFIER($APP_DATABASE);
GRANT BIND SERVICE ENDPOINT ON ACCOUNT TO ROLE ACCOUNTADMIN WITH GRANT OPTION;
GRANT BIND SERVICE ENDPOINT ON ACCOUNT TO APPLICATION  IDENTIFIER($APP_DATABASE);
GRANT USAGE ON WAREHOUSE  IDENTIFIER($APP_WAREHOUSE) TO APPLICATION  IDENTIFIER($APP_DATABASE);

--set TMP_INTERNAL_DB = 'GENESIS_TEST';
--set TMP_INTERNAL_SCH = 'GENESIS_TEST.GENESIS_INTERNAL';
--GRANT USAGE ON DATABASE IDENTIFIER($TMP_INTERNAL_DB) TO APPLICATION IDENTIFIER($APP_DATABASE);
--GRANT USAGE ON SCHEMA IDENTIFIER($TMP_INTERNAL_SCH) TO APPLICATION IDENTIFIER($APP_DATABASE);
--GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA IDENTIFIER($TMP_INTERNAL_SCH) TO APPLICATION IDENTIFIER($APP_DATABASE);
--GRANT SELECT, INSERT, UPDATE, DELETE ON ALL VIEWS IN SCHEMA IDENTIFIER($TMP_INTERNAL_SCH) TO APPLICATION IDENTIFIER($APP_DATABASE);
set TMP_SPIDER_DB = 'SPIDER_DATA';
set TMP_INTERNAL_SCH = 'SPIDER_DATA.GENESIS_INTERNAL';
GRANT USAGE ON DATABASE IDENTIFIER($TMP_INTERNAL_DB) TO APPLICATION IDENTIFIER($APP_DATABASE);
GRANT USAGE ON SCHEMA IDENTIFIER($TMP_INTERNAL_SCH) TO APPLICATION IDENTIFIER($APP_DATABASE);
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA IDENTIFIER($TMP_INTERNAL_SCH) TO APPLICATION IDENTIFIER($APP_DATABASE);
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL VIEWS IN SCHEMA IDENTIFIER($TMP_INTERNAL_SCH) TO APPLICATION IDENTIFIER($APP_DATABASE);

use schema IDENTIFIER($APP_LOCAL_SCHEMA);


CREATE OR REPLACE PROCEDURE grant_select_on_database_to_app(database_name STRING, APP_NAME STRING)
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    var connection = snowflake.createStatement({
        sqlText: `SELECT SCHEMA_NAME FROM ${DATABASE_NAME}.INFORMATION_SCHEMA.SCHEMATA`
    });
    var result = connection.execute();
    
    while (result.next()) {
        var schemaName = result.getColumnValue(1);
        if (schemaName === 'INFORMATION_SCHEMA') {
            continue;
        }
        var sqlCommands = [
            `GRANT USAGE ON DATABASE ${DATABASE_NAME} TO APPLICATION ${APP_NAME}`,
            `GRANT USAGE ON SCHEMA ${DATABASE_NAME}.${schemaName} TO APPLICATION ${APP_NAME}`,
            `GRANT SELECT ON ALL TABLES IN SCHEMA ${DATABASE_NAME}.${schemaName} TO APPLICATION ${APP_NAME}`,
            `GRANT SELECT ON ALL VIEWS IN SCHEMA ${DATABASE_NAME}.${schemaName} TO APPLICATION ${APP_NAME}`,
        ];
        
        for (var i = 0; i < sqlCommands.length; i++) {
            try {
                var stmt = snowflake.createStatement({sqlText: sqlCommands[i]});
                stmt.execute();
            } catch(err) {
                // Return error message if any command fails
                return `Error executing command: ${sqlCommands[i]} - ${err.message}`;
            }
        }
    }
    
    return "Successfully granted USAGE and SELECT on all schemas, tables, and views in database " + DATABASE_NAME + " to application " + APP_NAME;
$$;

 
call grant  _select_on_database_to_app('SPIDER_DATA',$APP_DATABASE);

revoke usage on database spider_data from application identifier($APP_DATABASE);


select current_role();



USE DATABASE IDENTIFIER($APP_DATABASE);


CALL CORE.INITIALIZE_APP_INSTANCE($APP_INSTANCE,$APP_COMPUTE_POOL,$APP_LOCAL_EAI,$APP_WAREHOUSE);
--CALL CORE.INITIALIZE_APP_INSTANCE_TEST($APP_INSTANCE,$APP_COMPUTE_POOL,$APP_LOCAL_EAI,$APP_WAREHOUSE);

-- call core.start_app_instance($APP_INSTANCE);
-- call core.stop_app_instance($APP_INSTANCE);
-- call core.drop_app_instance($APP_INSTANCE);
-- call core.list_app_instance($APP_INSTANCE);
-- call core.restart_app_instance($APP_INSTANCE);
-- call core.get_app_endpoint($APP_INSTANCE);

show services;
show compute pools;
drop compute pool GENESIS_TEST_POOL;

use role accountadmin;
show compute pools;

select current_version();


DROP COMPUTE POOL IDENTIFIER($APP_COMPUTE_POOL);
ALTER COMPUTE POOL GENESIS_TEST_POOL STOP ALL;

drop compute pool APP_COMPUTE_POOLAPP1;

select app1.get_slack_endpoints();

select current_schema();

CREATE or replace FUNCTION app_local_db.public.get_slack_endpoints ()
  RETURNS varchar
  SERVICE=genesisapp_app.app1.GENESISAPP_SERVICE_SERVICE
  ENDPOINT=udfendpoint
  AS '/udf_proxy/get_slack_tokens';

CREATE or replace FUNCTION app_test_schema.get_slack_endpoints ()
  RETURNS varchar
  SERVICE=genesis_server
  ENDPOINT=udfendpoint
  AS '/udf_proxy/get_slack_tokens';

CREATE or replace FUNCTION app_test_schema.list_available_bots()
  RETURNS varchar
  SERVICE=genesis_server
  ENDPOINT=udfendpoint
  AS '/udf_proxy/list_available_bots';

select submit_udf('hi how are you?','111','jl-local-eve-test-1');
select lookup_udf('6c04a3b6-ccc3-417a-b9c1-cb9c6b6dff40','jl-local-eve-test-1');


CREATE or replace FUNCTION app_test_schema.get_ngrok_tokens()
  RETURNS varchar
  SERVICE=genesis_server
  ENDPOINT=udfendpoint
  AS '/udf_proxy/get_ngrok_tokens';


CREATE or replace FUNCTION app_test_schema.get_metadata(metadata_type varchar)
  RETURNS varchar
  SERVICE=genesis_server
  ENDPOINT=udfendpoint
  AS '/udf_proxy/get_metadata';
  
CREATE or replace FUNCTION g_healthcheck ()
  RETURNS varchar
  SERVICE=genesis_server
  ENDPOINT=udfendpoint
  AS '/healthcheck';

select get_slack_endpoints();


select SYSTEM$ALLOWLIST();

use schema app1;
show tables;

describe service GENESISAPP_SERVICE_SERVICE;
SELECT SYSTEM$GET_SERVICE_STATUS('GENESISAPP_SERVICE_SERVICE');
SELECT SYSTEM$GET_SERVICE_LOGS('GENESISAPP_SERVICE_SERVICE',0,'chattest',1000);



////
// manual install (like cybersyn ai utilities)
////


DROP APPLICATION IF EXISTS GENESISAPP_APP;


SET APP_DATABASE='GENESISAPP_APP';


CREATE APPLICATION GENESISAPP_APP FROM APPLICATION PACKAGE GENESISAPP_APP_PKG_EXT USING VERSION V0_1;


create or replace database genesisapp_local_db;


CREATE OR REPLACE NETWORK RULE genesisapp_local_db.public.GENESISAPP_RULE
 MODE = EGRESS
 TYPE = HOST_PORT
   VALUE_LIST = ('0.0.0.0:443','0.0.0.0:80');
// VALUE_LIST = ('api.openai.com', 'connect.ngrok-agent.com:443');


CREATE OR REPLACE SECRET genesisapp_local_db.public.OPENAI_API_KEY
 TYPE = GENERIC_STRING
 SECRET_STRING = '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';


CREATE OR REPLACE SECRET genesisapp_local_db.public.NGROK_AUTHTOKEN
 TYPE = GENERIC_STRING
 SECRET_STRING = '2ce4bWGvzt5lBCDn6c2WsymnVSr_3m7QssHXhUHLi1BVCguRN';


CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION GENESISAPP_EAI
   ALLOWED_NETWORK_RULES = (genesisapp_local_db.public.GENESISAPP_RULE)
   ALLOWED_AUTHENTICATION_SECRETS = (genesisapp_local_db.public.OPENAI_API_KEY, genesisapp_local_db.public.NGROK_AUTHTOKEN)
   ENABLED = true;


GRANT USAGE ON DATABASE genesisapp_local_db TO APPLICATION GENESISAPP_APP;
GRANT USAGE ON SCHEMA genesisapp_local_db.public TO APPLICATION GENESISAPP_APP;
GRANT USAGE ON INTEGRATION GENESISAPP_EAI TO APPLICATION GENESISAPP_APP;
GRANT READ ON SECRET genesisapp_local_db.public.OPENAI_API_KEY TO APPLICATION GENESISAPP_APP;
GRANT READ ON SECRET genesisapp_local_db.public.NGROK_AUTHTOKEN TO APPLICATION GENESISAPP_APP;
--GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO APPLICATION GENESISAPP_APP;


GRANT SELECT ON ALL TABLES IN SCHEMA genesisapp_local_db.public TO APPLICATION GENESISAPP_APP;
GRANT SELECT ON ALL VIEWS IN SCHEMA genesisapp_local_db.public TO APPLICATION GENESISAPP_APP;


GRANT USAGE ON COMPUTE POOL TESTPOOL_SNOWCAT_STANDARD_2 TO APPLICATION GENESISAPP_APP;
GRANT USAGE ON WAREHOUSE APP_WH TO APPLICATION GENESISAPP_APP;
--GRANT BIND SERVICE ENDPOINT ON ACCOUNT TO APPLICATION GENESISAPP_APP;


create table genesisapp_local_db.public.tables_t as select * from information_schema.tables;
create table genesisapp_local_db.public.test as select 'hi' as hello;
create view genesisapp_local_db.public.tables_v as select * from genesisapp_local_db.information_schema.tables;
select table_catalog, table_schema, table_name from genesisapp_local_db.public.tables_v;
GRANT SELECT ON ALL TABLES IN SCHEMA genesisapp_local_db.public TO APPLICATION GENESISAPP_APP;
GRANT SELECT ON ALL VIEWS IN SCHEMA genesisapp_local_db.public TO APPLICATION GENESISAPP_APP;


grant usage on database JUSTIN to application genesisapp_app;
grant usage on schema JUSTIN.public to application genesisapp_app;
GRANT SELECT ON ALL TABLES IN SCHEMA JUSTIN.public TO APPLICATION GENESISAPP_APP;
GRANT SELECT ON ALL VIEWS IN SCHEMA JUSTIN.public TO APPLICATION GENESISAPP_APP;




show databases;
use role accountadmin;


// grant another DB, see if it sees it


CALL genesisapp_app.CORE.INITIALIZE_APP_INSTANCE('APP1','TESTPOOL_SNOWCAT_STANDARD_2','GENESISAPP_EAI');






use role test_role_2;
grant role test_role_2 to user justin;
select * from genesisapp_local_db.tables_v;
use database genesisapp_local_db;
use schema public;
show views;
select * from tables_v;
use role accountadmin;
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO APPLICATION GENESISAPP_APP;


grant usage on warehouse app_wh to role test_role_2;
use role accountadmin;
grant role test_role_2 to application genesisapp_app;


select table_name from genesisapp_local_db.information_schema.tables;




create materialized view genesisapp_local_db.public.tables_mv as
select 1 as one;


create secure view genesisapp_local_db.public.tables_sv as select * from information_schema.tables;
create table genesisapp_local_db.public.tables_t as select * from information_schema.tables;


create table genesisapp_local_db.public.test as select 'hi' as hello;


select * from genesisapp_local_db.public.test;


select get_ddl('table','genesisapp_local_db.public.test');
describe table genesisapp_local_db.public.tables_t;


// SQL show databases;
// SQL select get_ddl(\'table\',\'genesisapp_local_db.public.test\');
// SQL describe table genesisapp_local_db.public.tables_t;




select table_schema, table_name from genesisapp_local_db.public.tables_t;
create secure view genesisapp_local_db.public.tables_sv as select * from information_schema.tables;
show databases;




select count(*) from genesisapp_local_db.public.tables_t;
select count(*) from genesisapp_local_db.information_schema.tables;






select table_name from genesisapp_local_db.information_schema.tables;


select 'hello' HI;




use database genesisapp_app;
use role accountadmin;
show compute pools;


CALL genesisapp_app.CORE.INITIALIZE_APP_INSTANCE('APP1','TESTPOOL_SNOWCAT_STANDARD_2','GENESISAPP_EAI');


call genesisapp_app.core.drop_app_instance('APP1');
show compute pools;


// next read secret inside app setup for the container start


/*CALL CYBERSYN_AI_UTILITIES.cybersyn.init_openai(
   api_key => 'JUSTIN_DB.PUBLIC.OPENAI_API_KEY',
   external_integration => 'OPENAI_EXTERNAL_ACCESS_INTEGRATION');




SELECT CYBERSYN_AI_UTILITIES.cybersyn.evaluate_openai_prompt(
   'gpt-3.5-turbo',
   'You are financial market expert',
   'What is the name of the company with symbol SNOW. Just the name in json format.'
);
 */




-- ########## BEGIN CREATE/PATCH TEST APP   ##############################
DECLARE
 APP_DATABASE := 'GENESISAPP_APP';
 APP_COMPUTE_POOL VARCHAR DEFAULT $APP_COMPUTE_POOL;
 APP_INSTANCE VARCHAR DEFAULT 'APP1';


 APP_LOCAL_DB := (:APP_DATABASE||'_LOCAL_DB')::VARCHAR;
 APP_LOCAL_SCHEMA := (:APP_LOCAL_DB||'.'||'EGRESS')::VARCHAR;
 APP_LOCAL_EGRESS_RULE := (:APP_LOCAL_SCHEMA||'.'||'APP_RULE')::VARCHAR;
 APP_LOCAL_EAI := (:APP_DATABASE||'_EAI')::VARCHAR;
BEGIN
 BEGIN
   CREATE APPLICATION GENESISAPP_APP FROM APPLICATION PACKAGE GENESISAPP_APP_PKG_EXT USING VERSION V0_1;
 EXCEPTION
   WHEN OTHER THEN
     BEGIN
       ALTER APPLICATION GENESISAPP_APP UPGRADE USING VERSION V0_1;
       BEGIN
         CALL GENESISAPP_APP.CORE.DROP_APP_INSTANCE(:APP_INSTANCE);
       EXCEPTION
         WHEN OTHER THEN
           NULL;
       END;
     EXCEPTION
       WHEN OTHER THEN
         DROP APPLICATION IF EXISTS GENESISAPP_APP;
         CREATE APPLICATION GENESISAPP_APP FROM APPLICATION PACKAGE GENESISAPP_APP_PKG_EXT USING VERSION V0_1;
     END;
 END;


 CREATE DATABASE IF NOT EXISTS IDENTIFIER(:APP_LOCAL_DB);
 CREATE SCHEMA IF NOT EXISTS IDENTIFIER(:APP_LOCAL_SCHEMA);
    
 CREATE NETWORK RULE IF NOT EXISTS IDENTIFIER(:APP_LOCAL_EGRESS_RULE)
   TYPE = 'HOST_PORT'
   MODE= 'EGRESS'
   VALUE_LIST = ('0.0.0.0:443','0.0.0.0:80');
  
 CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION IDENTIFIER(:APP_LOCAL_EAI)
   ALLOWED_NETWORK_RULES = (GENESISAPP_APP_LOCAL_DB.EGRESS.APP_RULE)
   ENABLED = true;


 GRANT USAGE ON DATABASE IDENTIFIER(:APP_LOCAL_DB) TO APPLICATION IDENTIFIER(:APP_DATABASE);
 GRANT USAGE ON SCHEMA IDENTIFIER(:APP_LOCAL_SCHEMA) TO APPLICATION IDENTIFIER(:APP_DATABASE);
 GRANT USAGE ON NETWORK RULE IDENTIFIER(:APP_LOCAL_EGRESS_RULE) TO APPLICATION IDENTIFIER(:APP_DATABASE);


 GRANT USAGE ON INTEGRATION IDENTIFIER(:APP_LOCAL_EAI) TO APPLICATION IDENTIFIER(:APP_DATABASE);
 GRANT USAGE ON COMPUTE POOL IDENTIFIER(:APP_COMPUTE_POOL) TO APPLICATION IDENTIFIER(:APP_DATABASE);
 GRANT BIND SERVICE ENDPOINT ON ACCOUNT TO APPLICATION IDENTIFIER(:APP_DATABASE);


 GRANT USAGE ON COMPUTE POOL IDENTIFIER(:APP_COMPUTE_POOL) TO APPLICATION IDENTIFIER(:APP_DATABASE);


 USE DATABASE IDENTIFIER(:APP_DATABASE);
 LET RS1 RESULTSET := (CALL GENESISAPP_APP.CORE.INITIALIZE_APP_INSTANCE(:APP_INSTANCE,:APP_COMPUTE_POOL, :APP_LOCAL_EAI)); 
 RETURN TABLE(rs1);
END;


use database genesisapp_app;


drop application genesisapp_app;




select genesisapp_app.app1.submit_udf('hi', '123');
select genesisapp_app.app1.response_udf('10d12f42-fa6c-4807-9948-91a6ae8b9986');


call genesisapp_app.CORE.TEST_BILLING_EVENT();




call genesisapp_app.core.start_app_instance('APP1');


DESCRIBE SERVICE GENESISAPP_APP.APP1.GENESISAPP_SERVICE_SERVICE;
SELECT SYSTEM$GET_SERVICE_LOGS('GENESISAPP_APP.APP1.GENESISAPP_SERVICE_SERVICE',0,'echo',100);
SHOW ENDPOINTS IN SERVICE GENESISAPP_APP.APP1.GENESISAPP_SERVICE_SERVICE;


show streamlits;
desc streamlit genesisapp_app.core.SIS_LAUNCH;


// edcyofwr-sfengineering-ss-lprpr-test1.snowflakecomputing.app


call genesisapp_app.core.stop_app_instance('APP1');
call genesisapp_app.core.drop_app_instance('APP1');
call genesisapp_app.core.restart_app_instance('APP1');
-- call core.list_app_instance('APP1');
call genesisapp_app.core.get_app_endpoint('APP1');


-- ########## END CREATE TEST APP   ######################################


-- ##### BEGIN CREATE/PATCH TEST APP (DO NOT REBUILD THE APP)  ###########


DECLARE
 APP_INSTANCE VARCHAR DEFAULT 'APP1';
BEGIN
 ALTER APPLICATION GENESISAPP_APP UPGRADE USING VERSION V0_1;
 CALL GENESISAPP_APP.CORE.RESTART_APP_INSTANCE(:APP_INSTANCE);
 LET rs1 RESULTSET := (CALL GENESISAPP_APP.CORE.GET_APP_ENDPOINT(:APP_INSTANCE));
 RETURN TABLE(rs1);
END;


ALTER APPLICATION GENESISAPP_APP UPGRADE USING VERSION V0_1;
call genesisapp_app.core.restart_app_instance('APP1');


-- ########## END CREATE TEST APP   ######################################




-- ########## BEGIN PUBLISH   ############################################


ALTER APPLICATION PACKAGE GENESISAPP_APP_PKG_EXT
  SET DISTRIBUTION ="EXTERNAL";

select $APP_DISTRIBUTION;

show application packages;


DECLARE
 max_patch VARCHAR;
BEGIN
 show versions in application package GENESISAPP_APP_PKG_EXT;
 select max("patch") INTO :max_patch FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) where "version" = 'V0_1';
 LET rs RESULTSET := (EXECUTE IMMEDIATE 'ALTER APPLICATION PACKAGE GENESISAPP_APP_PKG_EXT SET DEFAULT RELEASE DIRECTIVE VERSION = V0_1 PATCH = '||:max_patch);
 RETURN TABLE(rs);
END;



-- ########## END PUBLISH   ##############################################


