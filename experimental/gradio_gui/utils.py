import json
import time
import uuid
import requests
from core.logging_config import logger

app_name = "GENESIS_BOTS"
prefix = app_name + ".app1"
core_prefix = app_name + ".CORE"

NativeMode = True


def get_session():
    global NativeMode
    print("NativeMode: ", NativeMode)
    if NativeMode:
        try:
            from snowflake.snowpark.context import get_active_session
            return get_active_session()
        except:
            NativeMode = False
    return None

def check_status():
    session = get_session()
    if session:
        status_query = f"select v.value:status::varchar status from (select parse_json(system$get_service_status('{prefix}.GENESISAPP_SERVICE_SERVICE'))) t, lateral flatten(input => t.$1) v"
        service_status_result = session.sql(status_query).collect()
        return service_status_result[0][0]
    return None

def provide_slack_level_key(bot_id=None, slack_app_level_key=None):
    if NativeMode:
        session = get_session()
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

def get_slack_tokens():
    if NativeMode:
        session = get_session()
        sql = f"select {prefix}.get_slack_endpoints() "
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

# ... (keep other utility functions like get_ngrok_tokens, set_ngrok_token, set_slack_tokens, etc.)

def get_bot_details():
    try:
        session = get_snowflake_session()
        if session is None:
            logger.info("Error: Unable to establish Snowflake session.")
            return []  # Return an empty list if session is None

        sql = """
        SELECT BOT_ID, BOT_NAME
        FROM GENESIS.PUBLIC.BOTS
        WHERE IS_ACTIVE = TRUE
        ORDER BY BOT_NAME
        """
        data = session.sql(sql).collect()
        return [{"bot_id": row["BOT_ID"], "bot_name": row["BOT_NAME"]} for row in data]
    except Exception as e:
        logger.info(f"Error in get_bot_details: {str(e)}")
        return []  # Return an empty list in case of any error

def submit_to_udf_proxy(input_text, thread_id, bot_id):
    global NativeMode
    primary_user = {
        "user_id": "unknown_id",
        "user_name": "unknown_name",
        "bot_id": bot_id,
    }

    if NativeMode:
        try:
            session = get_session()
            sql = "select {}.submit_udf(?, ?, ?)".format(prefix)
            data = session.sql(sql, (input_text, thread_id, json.dumps(primary_user))).collect()
            response = data[0][0]
            return response
        except Exception as e:
            logger.info("error on submit: ", e)
            NativeMode = False  # Switch to non-native mode if there's an error

    if not NativeMode:
        try:
            url = f"http://127.0.0.1:8080/udf_proxy/submit_udf"
            headers = {"Content-Type": "application/json"}
            data = json.dumps({"data": [[1, input_text, thread_id, primary_user]]})
            response = requests.post(url, headers=headers, data=data, timeout=5)
            if response.status_code == 200:
                return response.json()["data"][0][1]
            else:
                return "Error: Unable to submit to UDF proxy"
        except requests.exceptions.RequestException as e:
            logger.info(f"Error connecting to local server: {e}")
            return "Error: Unable to connect to the bot server. Please try again later."

def get_response_from_udf_proxy(uu, bot_id):
    global NativeMode
    if NativeMode:
        try:
            session = get_session()
            sql = f"""
                SELECT message from {prefix}.LLM_RESULTS
                WHERE uu = '{uu}'"""
            data = session.sql(sql).collect()
            response = data[0][0]
            return response
        except Exception as e:
            logger.info("!! Exception on get_response_from_udf_proxy: ", e)
            NativeMode = False  # Switch to non-native mode if there's an error

    if not NativeMode:
        try:
            url = f"http://127.0.0.1:8080/udf_proxy/lookup_udf"
            headers = {"Content-Type": "application/json"}
            data = json.dumps({"data": [[1, uu, bot_id]]})
            response = requests.post(url, headers=headers, data=data, timeout=5)
            if response.status_code == 200:
                return response.json()["data"][0][1]
            else:
                return "Error: Unable to get response from UDF proxy"
        except requests.exceptions.RequestException as e:
            logger.info(f"Error connecting to local server: {e}")
            return "Error: Unable to connect to the bot server. Please try again later."

# ... (keep other utility functions like deploy_bot, etc.)

def get_metadata(metadata_type):
    if NativeMode:
        session = get_session()
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

__all__ = ['NativeMode', 'check_status', 'get_session', 'app_name', 'prefix',
           'provide_slack_level_key', 'get_slack_tokens', 'get_bot_details',
           'submit_to_udf_proxy', 'get_response_from_udf_proxy', 'get_metadata']