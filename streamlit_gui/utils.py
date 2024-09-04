import streamlit as st
import json
import time
import uuid
import datetime
import pandas as pd
import requests

def get_session():

    if st.session_state.NativeMode:
        try:
            from snowflake.snowpark.context import get_active_session
            return get_active_session()
        except:
            st.session_state.NativeMode = False
  #  st.write('NativeMode', NativeMode)
    return None

def check_status():
    session = get_session()
    if session:
        prefix = st.session_state.get('prefix', '')
        status_query = f"select v.value:status::varchar status from (select parse_json(system$get_service_status('{prefix}.GENESISAPP_SERVICE_SERVICE'))) t, lateral flatten(input => t.$1) v"
        service_status_result = session.sql(status_query).collect()
        return service_status_result[0][0]
    return None

def provide_slack_level_key(bot_id=None, slack_app_level_key=None):
    if st.session_state.NativeMode:
        session = get_session()
        prefix = st.session_state.get('prefix', '')
        sql = f"select {prefix}.set_bot_app_level_key('{bot_id}','{slack_app_level_key}') "
        data = session.sql(sql).collect()
        response = json.loads(data[0][0])
        return response
    else:
        url = "http://127.0.0.1:8080/udf_proxy/set_bot_app_level_key"
        headers = {"Content-Type": "application/json"}
        data = json.dumps({"data": [[0, bot_id, slack_app_level_key]]})
        response = requests.post(url, headers=headers, data=data)
        if response.status_code == 200:
            return response.json()["data"][0][1]
        else:
            return "Error", f"Failed to set bot app_level_key tokens: {response.text}"

@st.cache_data(ttl=1800)  # Cache for 30 minutes
def get_slack_tokens_cached():
    """
    Cached version of get_slack_tokens function. Retrieves Slack tokens from the server
    and caches the result for 30 minutes to reduce API calls.
    """
    return get_slack_tokens()

def get_slack_tokens():
    if st.session_state.NativeMode:
        prefix = st.session_state.get('prefix', '')
        sql = f"select {prefix}.get_slack_endpoints() "
        session = get_session()
        data = session.sql(sql).collect()
        response = json.loads(data[0][0])
        return response
    else:
        url = "http://127.0.0.1:8080/udf_proxy/get_slack_tokens"
        headers = {"Content-Type": "application/json"}
        data = json.dumps({"data": [[0]]})
        response = requests.post(url, headers=headers, data=data)
        if response.status_code == 200:
            return response.json()["data"][0][1]
        else:
            raise Exception(f"Failed to reach bot server to get list of available bots")

def get_ngrok_tokens():
    if st.session_state.NativeMode:
        session = get_session()
        prefix = st.session_state.get('prefix', '')
        sql = f"select {prefix}.get_ngrok_tokens() "
        data = session.sql(sql).collect()
        response = json.loads(data[0][0])
        return response
    else:
        url = "http://127.0.0.1:8080/udf_proxy/get_ngrok_tokens"
        headers = {"Content-Type": "application/json"}
        data = json.dumps({"data": [[0]]})
        response = requests.post(url, headers=headers, data=data)
        if response.status_code == 200:
            return response.json()["data"][0][1]
        else:
            raise Exception(f"Failed to reach bot server to get list of available bots")

def set_ngrok_token(ngrok_auth_token, ngrok_use_domain, ngrok_domain):
    if st.session_state.NativeMode:
        prefix = st.session_state.get('prefix', '')
        sql = f"select {prefix}.configure_ngrok_token('{ngrok_auth_token}','{ngrok_use_domain}','{ngrok_domain}') "
        session = get_session()
        data = session.sql(sql).collect()
        response = json.loads(data[0][0])
        return response
    else:
        url = "http://127.0.0.1:8080/udf_proxy/configure_ngrok_token"
        headers = {"Content-Type": "application/json"}
        data = json.dumps({"data": [[0, ngrok_auth_token, ngrok_use_domain, ngrok_domain]]})
        response = requests.post(url, headers=headers, data=data)
        if response.status_code == 200:
            return response.json()["data"][0][1]
        else:
            return "Error", f"Failed to set ngrok tokens: {response.text}"

def set_slack_tokens(slack_app_token, slack_app_refresh_token):
    if st.session_state.NativeMode:
        session = get_session()
        prefix = st.session_state.get('prefix', '')
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
            return response.json()["data"][0][1]
        else:
            return "Error", f"Failed to set Slack Tokens: {response.text}"

@st.cache_data
def get_bot_details():
    if st.session_state.NativeMode:
        prefix = st.session_state.get('prefix', '')
        session = get_session()
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
            return response.json()["data"][0][1]
        else:
            raise Exception(f"Failed to reach bot server to get list of available bots")

def configure_llm(llm_model_name, llm_api_key):
    if st.session_state.NativeMode:
        prefix = st.session_state.get('prefix', '')
        session = get_session()
        sql = f"select {prefix}.configure_llm('{llm_model_name}', '{llm_api_key}') "
        data = session.sql(sql).collect()
        response = data[0][0]
        return json.loads(response)
    else:
        url = "http://127.0.0.1:8080/udf_proxy/configure_llm"
        headers = {"Content-Type": "application/json"}
        data = json.dumps({"data": [[0, llm_model_name, llm_api_key]]})
        response = requests.post(url, headers=headers, data=data)
        if response.status_code == 200:
            return response.json()["data"][0][1]
        else:
            raise Exception(f"Failed to configure LLM: {response.text}")

def get_metadata(metadata_type):
    if st.session_state.NativeMode:
        session = get_session()
        prefix = st.session_state.get('prefix', '')
        sql = f"select {prefix}.get_metadata('{metadata_type}') "
        data = session.sql(sql).collect()
        response = data[0][0]
        response = json.loads(response)
        return response
    else:
        url = "http://127.0.0.1:8080/udf_proxy/get_metadata"
        headers = {"Content-Type": "application/json"}
        data = json.dumps({"data": [[0, metadata_type]]})
        response = requests.post(url, headers=headers, data=data)
        if response.status_code == 200:
            return response.json()["data"][0][1]
        else:
            raise Exception(f"Failed to get metadata: {response.text}")

def submit_to_udf_proxy(input_text, thread_id, bot_id):
    user_info = st.experimental_user.to_dict()
    primary_user = {
        "user_id": user_info.get("email", "Unknown User ID"),
        "user_name": user_info.get("user_name", "Unknown User"),
        "bot_id": bot_id,
    }

    if st.session_state.NativeMode:
        try:
            prefix = st.session_state.get('prefix', '')
            sql = f"select {prefix}.submit_udf(?, ?, ?)".format(prefix)
            session = get_session()
            data = session.sql(sql, (input_text, thread_id, json.dumps(primary_user))).collect()
            response = data[0][0]
            return response
        except Exception as e:
            st.write("error on submit: ", e)
    else:
        url = f"http://127.0.0.1:8080/udf_proxy/submit_udf"
        headers = {"Content-Type": "application/json"}
        data = json.dumps({"data": [[1, input_text, thread_id, primary_user]]})
        response = requests.post(url, headers=headers, data=data)
        if response.status_code == 200:
            return response.json()["data"][0][1]
        else:
            raise Exception(f"Failed to submit to UDF proxy: {response.text}")

def get_response_from_udf_proxy(uu, bot_id):
    if st.session_state.NativeMode:
        try:
            session = get_session()
            prefix = st.session_state.get('prefix', '')
            sql = f"""
                SELECT message from {prefix}.LLM_RESULTS
                WHERE uu = '{uu}'"""
            data = session.sql(sql).collect()
            if data and len(data) > 0 and len(data[0]) > 0:
                response = data[0][0]
            else:
                response = "not found"
            return response
        except Exception as e:
            st.write("!! Exception on get_response_from_udf_proxy: ", e)
            return "!!EXCEPTION_NEEDS_RETRY!!"
    else:
        url = f"http://127.0.0.1:8080/udf_proxy/lookup_udf"
        headers = {"Content-Type": "application/json"}
        data = json.dumps({"data": [[1, uu, bot_id]]})
        response = requests.post(url, headers=headers, data=data)
        if response.status_code == 200:
            return response.json()["data"][0][1]
        else:
            return "not found"

def deploy_bot(bot_id):
    if st.session_state.NativeMode:
        prefix = st.session_state.get('prefix', '')
        sql = f"select {prefix}.deploy_bot('{bot_id}') "
        session = get_session()
        data = session.sql(sql).collect()
        response = json.loads(data[0][0])
        return response
    else:
        url = "http://127.0.0.1:8080/udf_proxy/deploy_bot"
        headers = {"Content-Type": "application/json"}
        data = json.dumps({"data": [[0, bot_id]]})
        response = requests.post(url, headers=headers, data=data)
        if response.status_code == 200:
            return response.json()["data"][0][1]
        else:
            raise Exception(f"Failed to deploy bot: {response.text}")