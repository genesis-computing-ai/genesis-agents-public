import streamlit as st
from snowflake.snowpark import Session
import os
import json
import pandas as pd
import requests
import time

# Global variables
APP_NAME = "GENESIS_BOTS"
PREFIX = APP_NAME + ".app1"
CORE_PREFIX = APP_NAME + ".CORE"

import streamlit as st
from snowflake.snowpark import Session
import os
import json
import pandas as pd

# Global variables
APP_NAME = "GENESIS_BOTS"
PREFIX = APP_NAME + ".app1"
CORE_PREFIX = APP_NAME + ".CORE"


def initialize_session():
    try:
        # First, try to get the active session (for Snowflake Streamlit)
        from snowflake.snowpark.context import get_active_session
        session = get_active_session()
        return session, "snowflake"
    except:
        # If that fails, create a new session (for local development)
        try:
            connection_parameters = {
                "account": os.getenv("SNOWFLAKE_ACCOUNT_OVERRIDE", "eqb52188"),
                "user": os.getenv("SNOWFLAKE_USER_OVERRIDE", "JL_LOCAL_RUNNER"),
                "password": os.getenv("SNOWFLAKE_PASSWORD_OVERRIDE", "pass"),
                "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE_OVERRIDE", "XSMALL"),
                "database": os.getenv("SNOWFLAKE_DATABASE_OVERRIDE", "GENESIS_TEST"),
                "schema": os.getenv("SNOWFLAKE_SCHEMA")
            }
            session = Session.builder.configs(connection_parameters).create()
            return session, "local"
        except Exception as e:
            st.error(f"Error creating Snowflake session: {e}")
            return None, "error"

def check_service_status(session, mode):
    if mode == "local":
        return "LOCAL_MODE"
    if not session:
        return "NOT_CONNECTED"
    try:
        # Check if the service exists
        service_exists_query = f"""
        SELECT COUNT(*) 
        FROM information_schema.services 
        WHERE service_name = 'GENESISAPP_SERVICE_SERVICE' 
        AND service_owner = '{APP_NAME}.APP1'
        """
        result = session.sql(service_exists_query).collect()
        if result[0][0] == 0:
            return "NOT_INSTALLED"

        # If the service exists, check its status
        status_query = f"""
        SELECT SYSTEM$GET_SERVICE_STATUS('{PREFIX}.GENESISAPP_SERVICE_SERVICE') as status
        """
        result = session.sql(status_query).collect()
        status_json = json.loads(result[0]['STATUS'])
        
        if 'status' in status_json:
            return status_json['status']
        else:
            return "UNKNOWN"
    except Exception as e:
        st.error(f"Error checking service status: {e}")
        return "ERROR"


def get_bot_details(session, mode):
    if mode == "local":
        url = "http://127.0.0.1:8080/udf_proxy/list_available_bots"
        headers = {"Content-Type": "application/json"}
        data = json.dumps({"data": [[0]]})

        try:
            response = requests.post(url, headers=headers, data=data)
            if response.status_code == 200:
                return response.json()["data"][0][1]
            else:
                st.error(f"Failed to reach bot server: Status code {response.status_code}")
                return []
        except requests.exceptions.RequestException as e:
            st.error(f"Error connecting to local server: {e}")
            return []

    elif mode == "snowflake":
        if not session:
            st.error("No active Snowflake session")
            return []

        try:
            for _ in range(5):
                sql = f"select {PREFIX}.list_available_bots()"
                data = session.sql(sql).collect()
                if data:
                    return json.loads(data[0][0])
                time.sleep(2)
            st.error("Failed to retrieve bot details after multiple attempts")
            return []
        except Exception as e:
            st.error(f"Error getting bot details: {e}")
            return []

    else:
        st.error(f"Unknown mode: {mode}")
        return []

def submit_to_udf_proxy(session, run_mode, input_text, thread_id, bot_id):
    if run_mode == "local":
        url = "http://127.0.0.1:8080/udf_proxy/submit_udf"
        headers = {"Content-Type": "application/json"}
        data = json.dumps({"data": [[1, input_text, thread_id, {"bot_id": bot_id}]]})
        try:
            response = requests.post(url, headers=headers, data=data)
            if response.status_code == 200:
                return response.json()["data"][0][1]  # UUID of the submission
            else:
                st.error(f"Failed to submit to UDF proxy: {response.text}")
                return None
        except requests.exceptions.RequestException as e:
            st.error(f"Error connecting to local server: {e}")
            return None
    elif run_mode == "snowflake":
        # Existing Snowflake logic
        if not session:
            return None
        try:
            user_info = st.experimental_user.to_dict()
            primary_user = {
                "user_id": user_info.get("email", "unknown_id"),
                "user_name": user_info.get("user_name", "unknown_name"),
                "bot_id": bot_id,
            }
            sql = f"select {PREFIX}.submit_udf(?, ?, ?)"
            data = session.sql(sql, (input_text, thread_id, json.dumps(primary_user))).collect()
            return data[0][0]
        except Exception as e:
            st.error(f"Error submitting to UDF proxy: {e}")
            return None


import streamlit as st
import requests
import json

def get_response_from_udf_proxy(session=None, run_mode=None, uu=None, bot_id=None):
  #  st.write(f"Getting response from UDF proxy in {run_mode} mode for UUID: {uu}")
    if run_mode == "local":
        url = f"http://127.0.0.1:8080/udf_proxy/lookup_udf"
        headers = {"Content-Type": "application/json"}
        data = json.dumps({"data": [[1, uu, bot_id]]})

            
        try:
        #    st.write(f"Sending POST request to {url}")
            response = requests.post(url, headers=headers, data=data)
         #   st.write(f"Received response with status code: {response.status_code}")
            
            if response.status_code == 200:
                result = response.json()["data"][0][1]
          #      st.write(f"Raw result: {response.json()}")
                if result is not None and result != "not found":
                    return result
                else:
           #         st.write("Result not found")
                    return "not found"
            else:
                st.error(f"Failed to get response from UDF proxy: {response.text}")
                return "!!EXCEPTION_NEEDS_RETRY!!"
        except requests.exceptions.RequestException as e:
            st.error(f"Error connecting to local server: {e}")
            return "!!EXCEPTION_NEEDS_RETRY!!"
    
    elif run_mode == "snowflake":
        # Existing Snowflake logic
        try:
            sql = f"""
                SELECT message from {PREFIX}.LLM_RESULTS
                WHERE uu = '{uu}'"""
            data = session.sql(sql).collect()
            return data[0][0]
        except Exception as e:
            st.error(f"Error getting response from UDF proxy: {e}")
            return "!!EXCEPTION_NEEDS_RETRY!!"

def deploy_bot(session, bot_id):
    if not session:
        return {"Success": False, "Message": "No active session"}
    try:
        sql = f"select {PREFIX}.deploy_bot('{bot_id}')"
        data = session.sql(sql).collect()
        return json.loads(data[0][0])
    except Exception as e:
        st.error(f"Error deploying bot: {e}")
        return {"Success": False, "Message": str(e)}

def get_metadata(session, metadata_type):
    if not session:
        return []
    try:
        sql = f"select {PREFIX}.get_metadata('{metadata_type}')"
        data = session.sql(sql).collect()
        return json.loads(data[0][0])
    except Exception as e:
        st.error(f"Error getting metadata: {e}")
        return []

def configure_llm(session, llm_model_name, llm_api_key):
    if not session:
        return {"Success": False, "Message": "No active session"}
    try:
        sql = f"select {PREFIX}.configure_llm('{llm_model_name}', '{llm_api_key}')"
        data = session.sql(sql).collect()
        return json.loads(data[0][0])
    except Exception as e:
        st.error(f"Error configuring LLM: {e}")
        return {"Success": False, "Message": str(e)}

def get_slack_tokens(session):
    if not session:
        return {}
    try:
        sql = f"select {PREFIX}.get_slack_endpoints()"
        data = session.sql(sql).collect()
        return json.loads(data[0][0])
    except Exception as e:
        st.error(f"Error getting Slack tokens: {e}")
        return {}

def set_slack_tokens(session, slack_app_token, slack_app_refresh_token):
    if not session:
        return {"Success": False, "Message": "No active session"}
    try:
        sql = f"select {PREFIX}.configure_slack_app_token('{slack_app_token}','{slack_app_refresh_token}')"
        data = session.sql(sql).collect()
        return json.loads(data[0][0])
    except Exception as e:
        st.error(f"Error setting Slack tokens: {e}")
        return {"Success": False, "Message": str(e)}

def start_genesis_service(session):
    if not session:
        return False
    try:
        sql = f"call {CORE_PREFIX}.start_app_instance('APP1','GENESIS_POOL','GENESIS_EAI','XSMALL')"
        session.sql(sql).collect()
        return True
    except Exception as e:
        st.error(f"Error starting Genesis service: {e}")
        return False

def stop_genesis_service(session):
    if not session:
        return False
    try:
        sql = f"call {CORE_PREFIX}.stop_app_instance('APP1')"
        session.sql(sql).collect()
        return True
    except Exception as e:
        st.error(f"Error stopping Genesis service: {e}")
        return False

def get_harvester_status(session):
    if not session:
        return None, None
    try:
        harvest_control = get_metadata(session, "harvest_control")
        harvest_summary = get_metadata(session, "harvest_summary")
        
        if harvest_control:
            harvest_control_df = pd.DataFrame(harvest_control).rename(columns=str.lower)
            harvest_control_df["schema_exclusions"] = harvest_control_df["schema_exclusions"].apply(lambda x: ["None"] if not x else x)
            harvest_control_df["schema_inclusions"] = harvest_control_df["schema_inclusions"].apply(lambda x: ["All"] if not x else x)
        else:
            harvest_control_df = pd.DataFrame(columns=["source_name", "database_name", "schema_name", "schema_exclusions", "schema_inclusions", "status", "refresh_interval", "initial_crawl_complete"])

        if harvest_summary:
            harvest_summary_df = pd.DataFrame(harvest_summary).rename(columns=str.lower)
            column_order = ["source_name", "database_name", "schema_name", "role_used_for_crawl", "last_change_ts", "objects_crawled"]
            harvest_summary_df = harvest_summary_df[column_order]
        else:
            harvest_summary_df = pd.DataFrame(columns=["source_name", "database_name", "schema_name", "role_used_for_crawl", "last_change_ts", "objects_crawled"])

        return harvest_control_df, harvest_summary_df
    except Exception as e:
        st.error(f"Error getting harvester status: {e}")
        return None, None

def get_available_databases(session):
    if not session:
        return []
    try:
        available_databases = get_metadata(session, "available_databases")
        if available_databases:
            return pd.DataFrame(available_databases)
        else:
            return pd.DataFrame(columns=["DatabaseName", "Schemas"])
    except Exception as e:
        st.error(f"Error getting available databases: {e}")
        return pd.DataFrame(columns=["DatabaseName", "Schemas"])

# Add any other necessary functions here