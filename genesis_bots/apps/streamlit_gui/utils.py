import json
import requests
import streamlit as st


LOCAL_SERVER_URL = "http://127.0.0.1:8080/"

def get_session():
   # print(f"Get session: {st.session_state.NativeMode}")
    if st.session_state.NativeMode:
        try:
            from snowflake.snowpark.context import get_active_session
            return get_active_session()
        except:
            st.session_state.NativeMode = False
            st.session_state.eai_available = True
    #  st.write('NativeMode', NativeMode)
    return None

# def get_permissions():
#     if st.session_state.NativeMode:
#         try:
#             from snowflake.permissions import permissions
#             return permissions()
#         except:
#             st.session_state.NativeMode = False
#     return None

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
        url = LOCAL_SERVER_URL + "udf_proxy/set_bot_app_level_key"
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
        url = LOCAL_SERVER_URL + "udf_proxy/get_slack_tokens"
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
        url = LOCAL_SERVER_URL + "udf_proxy/get_ngrok_tokens"
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
        url = LOCAL_SERVER_URL + "udf_proxy/configure_ngrok_token"
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
        url = LOCAL_SERVER_URL + "udf_proxy/configure_slack_app_token"
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
        url = LOCAL_SERVER_URL + "udf_proxy/list_available_bots"
        headers = {"Content-Type": "application/json"}
        data = json.dumps({"data": [[0]]})
        response = requests.post(url, headers=headers, data=data)
        if response.status_code == 200:
            return response.json()["data"][0][1]
        else:
            raise Exception(f"Failed to reach bot server to get list of available bots")

def configure_llm(llm_model_name, llm_api_key, llm_base_url):
    if st.session_state.NativeMode:
        prefix = st.session_state.get('prefix', '')
        session = get_session()
        sql = f"select {prefix}.configure_llm('{llm_model_name}', '{llm_api_key}|{llm_base_url}') "
        data = session.sql(sql).collect()
        response = data[0][0]
        return json.loads(response)
    else:
        url = LOCAL_SERVER_URL + "udf_proxy/configure_llm"
        headers = {"Content-Type": "application/json"}
        data = json.dumps({"data": [[0, llm_model_name, llm_api_key+'|'+llm_base_url]]})
        response = requests.post(url, headers=headers, data=data)
        if response.status_code == 200:
            return response.json()["data"][0][1]
        else:
            raise Exception(f"Failed to configure LLM: {response.text}")


def get_metadata2(metadata_type):
    if st.session_state.NativeMode:
        session = get_session()
        prefix = st.session_state.get('prefix', '')
        sql = f"select {prefix}.get_metadata('{metadata_type}') "
        data = session.sql(sql).collect()
        response = data[0][0]
        return response
    else:
        url = LOCAL_SERVER_URL + "udf_proxy/get_metadata"
        headers = {"Content-Type": "application/json"}
        data = json.dumps({"data": [[0, metadata_type]]})
        response = requests.post(url, headers=headers, data=data)
        if response.status_code == 200:
            return response.json()["data"][0][1]
        else:
            raise Exception(f"Failed to get metadata: {response.text}")


@st.cache_data(ttl=3600)  # Cache the result for 1 hour
def get_metadata_cached(metadata_type):
    if st.session_state.NativeMode:
        session = get_session()
        prefix = st.session_state.get('prefix', '')
        sql = f"select {prefix}.get_metadata('{metadata_type}') "
        data = session.sql(sql).collect()
        response = data[0][0]
        response = json.loads(response)
        return response
    else:
        url = LOCAL_SERVER_URL + "udf_proxy/get_metadata"
        headers = {"Content-Type": "application/json"}
        data = json.dumps({"data": [[0, metadata_type]]})
        response = requests.post(url, headers=headers, data=data)
        if response.status_code == 200:
            return response.json()["data"][0][1]
        else:
            raise Exception(f"Failed to get metadata: {response.text}")

def set_metadata(metadata_type):
    if st.session_state.NativeMode:
        session = get_session()
        prefix = st.session_state.get('prefix', '')
        sql = f"select {prefix}.set_metadata('{metadata_type}') "
        data = session.sql(sql).collect()
        response = data[0][0]
        response = json.loads(response)
        return response
    else:
        url = LOCAL_SERVER_URL + "udf_proxy/set_metadata"
        headers = {"Content-Type": "application/json"}
        data = json.dumps({"data": [[0, metadata_type]]})
        response = requests.post(url, headers=headers, data=data)
        if response.status_code == 200:
            return response.json()["data"][0][1]
        else:
            raise Exception(f"Failed to set metadata: {response.text}")


def get_metadata(metadata_type):
    session = get_session()
    if st.session_state.NativeMode:
        prefix = st.session_state.get('prefix', '')
        sql = f"select {prefix}.get_metadata('{metadata_type}') "
        data = session.sql(sql).collect()
        response = data[0][0]
        response = json.loads(response)
        return response
    else:
        url = LOCAL_SERVER_URL + "udf_proxy/get_metadata"
        headers = {"Content-Type": "application/json"}
        data = json.dumps({"data": [[0, metadata_type]]})
        response = requests.post(url, headers=headers, data=data)
        if response.status_code == 200:
            return response.json()["data"][0][1]
        else:
            raise Exception(f"Failed to get metadata: {response.text}")


def get_artifact(artifact_id):
    """
    Retrieve an artifact data and matadata, given an artifact_id.

    Args:
        artifact_id (str): The unique identifier for the artifact to be retrieved.

    Returns:
        A 2-tuple: (metadata json, base64 encoded data).

    Raises:
        Exception: If the artifact retrieval fails, an exception is raised with
                   the error message from the server response.
    """
    if st.session_state.NativeMode:
        session = get_session()
        prefix = st.session_state.get('prefix', '')
        sql = f"select {prefix}.get_artifact('{artifact_id}')"
        data = session.sql(sql).collect()
        response = data[0][0]
        payload = json.loads(response)
        is_success = payload.get("Success")
        if is_success:
            metadata = payload.get("Metadata")
            data = payload.get("Data")
            return metadata, data
        else:
            raise Exception(f"Failed to get artifact {artifact_id} (Native mode): {payload.get('Error', 'No error details provided')}")
    else:
        url = LOCAL_SERVER_URL + "udf_proxy/get_artifact"
        headers = {"Content-Type": "application/json"}
        data = json.dumps({"artifact_id": artifact_id})
        response = requests.post(url, headers=headers, data=data)
        if response.status_code == 200:
            payload = response.json()
            metadata = payload["Metadata"]
            data = payload["Data"]
            return metadata, data
        else:
            raise Exception(f"Failed to get artifact {artifact_id}: {response.text}")


def submit_to_udf_proxy(input_text, thread_id, bot_id, file={}):
    user_info = st.experimental_user.to_dict()
    primary_user = {
        "user_id": user_info.get("email", "unknown_id"),
        "user_name": user_info.get("user_name", "unknown_name"),
        "user_email": user_info.get("email", "unknown_email"),
        "bot_id": bot_id,
    }

    if st.session_state.NativeMode:
        try:
            prefix = st.session_state.get('prefix', '')
            sql = f"select {prefix}.submit_udf(?, ?, ?)".format(prefix)
            session = get_session()
            data = session.sql(sql, (input_text, thread_id, json.dumps(primary_user), json.dumps(file))).collect()
            response = data[0][0]
            return response
        except Exception as e:
            st.write("error on submit: ", e)
    else:
        url = LOCAL_SERVER_URL + "udf_proxy/submit_udf"
        headers = {"Content-Type": "application/json"}
        data = json.dumps({"data": [[1, input_text, thread_id, primary_user, json.dumps(file)]]})
        response = requests.post(url, headers=headers, data=data)
        if response.status_code == 200:
            return response.json()["data"][0][1]
        else:
            raise Exception(f"Failed to submit to UDF proxy: {response.text}")


ACTION_MSG_DELIM = "<!!-ACTION_MSG-!!>"
'''prefix/suffix delimeter for special 'action' messages that distinguish them from normal chat messages'''

ACTION_MSG_TYPES = ("action_required", # we are requesting the client to run some action (e.g. a tool function)
                    "action_result")   # the client has run the action and is submitting the result


def format_action_msg(action_type, **action_params):
    '''
    Builds a special 'action' message that distinguishes it from normal chat messages.
    '''
    assert action_type in ACTION_MSG_TYPES, f"Unrecognized action_type value: {action_type}. Expected one of: {ACTION_MSG_TYPES}"
    d = dict(action_type=action_type, **action_params)
    dj = json.dumps(d)
    return f"{ACTION_MSG_DELIM}{dj}{ACTION_MSG_DELIM}"


def parse_action_msg(msg):
    '''
    Parses a special 'action' message that distinguishes it from normal chat messages.
    Will raise a ValueError if the message is not a valid action message.
    '''
    if not msg.startswith(ACTION_MSG_DELIM) or not msg.endswith(ACTION_MSG_DELIM):
        raise ValueError(f"Expected action message to start with {ACTION_MSG_DELIM} and end with {ACTION_MSG_DELIM}, got: {msg}")
    dj = msg[len(ACTION_MSG_DELIM):-len(ACTION_MSG_DELIM)]
    try:
        d = json.loads(dj)
    except json.JSONDecodeError as e:
        raise ValueError(f"Failed to parse JSON from action message: {e}")
    if 'action_type' not in d:
        raise ValueError(f"Expected action message to contain an action_type field, got: {d}")
    return d

def get_response_from_udf_proxy(uu, bot_id) -> str:
    not_found_msg = "not found" # TODO: use a constant shared with the Chat page logic
    response = None

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
                response = not_found_msg
        except Exception as e:
            st.write("!! Exception on get_response_from_udf_proxy: ", e)
            response = "!!EXCEPTION_NEEDS_RETRY!!"
    else:
        url = LOCAL_SERVER_URL + "udf_proxy/lookup_udf"
        headers = {"Content-Type": "application/json"}
        data = json.dumps({"data": [[1, uu, bot_id]]})
        response = requests.post(url, headers=headers, data=data)
        if response.status_code != 200:
            response = not_found_msg
        else:
            response = response.json()["data"][0][1]

    # See if this reposnse is a special 'action' message. This can happen if we assigned an ephemeral tool to this bot in some other chat context
    # and the user prompted the bot such that it decided to use that tool.
    # In Streamlit chat we currently do not support actions, so we need to send a special message back to the LLM to let it know that the tool is not available.
    try:
        action_msg = parse_action_msg(response)
    except ValueError as e:
        pass  # regular response
    else:
        # handle the action request
        invocation_id = action_msg.get("invocation_id", None)
        action_result = f"ERROR: actions not supported by the current client interface which is an interactive chat UI"
        # send the result back to the LLM
        result_msg = format_action_msg("action_result",
                                                            invocation_id=invocation_id,
                                                            func_result=action_result)
        submit_to_udf_proxy(result_msg, thread_id="null", bot_id=bot_id)
        response = not_found_msg

    return response


def deploy_bot(bot_id):
    if st.session_state.NativeMode:
        prefix = st.session_state.get('prefix', '')
        sql = f"select {prefix}.deploy_bot('{bot_id}') "
        session = get_session()
        data = session.sql(sql).collect()
        response = json.loads(data[0][0])
        return response
    else:
        url = LOCAL_SERVER_URL + "udf_proxy/deploy_bot"
        headers = {"Content-Type": "application/json"}
        data = json.dumps({"data": [[0, bot_id]]})
        response = requests.post(url, headers=headers, data=data)
        if response.status_code == 200:
            return response.json()["data"][0][1]
        else:
            raise Exception(f"Failed to deploy bot: {response.text}")

def upgrade_services(eai_type=None, eai_name=None):
    session = get_session()
    try:
        core_prefix = st.session_state.get('core_prefix', '')
        if eai_type and eai_name:
            if session:
                #TODO move to connecter?
                prefix = st.session_state.get('prefix', '')
                # "reference('CONSUMER_EXTERNAL_ACCESS')"
                update_eai_list_query = f"""
                    MERGE INTO {prefix}.EAI_CONFIG AS target
                    USING (SELECT '{eai_type}' AS EAI_TYPE, 'reference(''{eai_name}'')' AS EAI_NAME) AS source
                    ON target.EAI_TYPE = source.EAI_TYPE
                    WHEN MATCHED THEN
                        UPDATE SET
                            target.EAI_NAME = source.EAI_NAME
                    WHEN NOT MATCHED THEN
                        INSERT (EAI_TYPE, EAI_NAME)
                        VALUES (source.EAI_TYPE, source.EAI_NAME);
                    """
                update_eai_list_result = session.sql(update_eai_list_query).collect()

        upgrade_services_query = f"call {core_prefix}.UPGRADE_SERVICES() "
        upgrade_services_result = session.sql(upgrade_services_query).collect()
        return upgrade_services_result[0][0]
    except Exception as e:
        st.error(f"Error updating EAI config table: {e}")
    return None

def check_eai_assigned(reference_name):
    try:
        eai_data = get_metadata("check_eai_assigned")
        if not eai_data or not isinstance(eai_data, list) or not eai_data[0]:
            return False

        eai_str = eai_data[0].get('eai_list')
        if not eai_str:
            return False

        if reference_name.upper() in eai_str.upper():
            return True
        return False
    except Exception as e:
        st.error(f"Error checking eai assigned: {e}")
        return False

def check_eai_status(site):
    result = False
    try:
        # eai_result = get_metadata(f"check_eai {site}")
        # if isinstance(eai_result, list) and len(eai_result) > 0:
        #     # The response is a list containing a dictionary
        #     # The dictionary has a 'Success' key that's a boolean
        #     result = bool(eai_result[0].get('Success', False))
        return result
    except Exception as e:
        st.error(f"Error checking eai status: {e}")

def get_references(reference_name):
    ref_associations = None
    if st.session_state.NativeMode:
        try:
            import snowflake.permissions as permissions
            ref_associations = permissions.get_reference_associations(reference_name)
        except Exception as e:
            st.error(f"Error checking references: {e}")
    return ref_associations


def check_log_status():
    try:
        log_status = get_metadata('log_status')

    except Exception as e:
        st.error(f"Error checking log status: {e}")
