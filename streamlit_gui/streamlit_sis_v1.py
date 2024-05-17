import streamlit as st
import os, json
import base64

app_name = 'GENESIS_BOTS'
prefix = app_name+'.app1'
core_prefix = app_name+'.CORE'

# Retrieve the current database name from Snowflake and set it to app_name

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


def provide_slack_level_key(bot_id=None, slack_app_level_key=None):

    import requests
    import json

    print (SnowMode)
    if SnowMode:

        sql = f"select {prefix}.set_bot_app_level_key('{bot_id}','{slack_app_level_key}') "
        data = session.sql(sql).collect()
        response = json.loads(data[0][0])
        return response

    # add SnowMode
    url = "http://127.0.0.1:8080/udf_proxy/set_bot_app_level_key"
    headers = {"Content-Type": "application/json"}

    data = json.dumps({"data": [[0, bot_id, slack_app_level_key]]})
    response = requests.post(url, headers=headers, data=data)
    if response.status_code == 200:
        return response.json()['data'][0][1] # response message
    else:
        return "Error", f"Failed to set bot app_level_key tokens: {response.Message}"


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

        sql = "select {}.submit_udf(?, ?, ?)".format(prefix)
        data = session.sql(sql, (input_text, thread_id, bot_id)).collect()
        response = data[0][0]
        return response
    
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
        response = json.loads(data[0][0])
        #st.write(response)
        return response
    
    url = "http://127.0.0.1:8080/udf_proxy/deploy_bot"
    headers = {"Content-Type": "application/json"}
    data = json.dumps({"data": [[0, bot_id]]})

    response = requests.post(url, headers=headers, data=data)
    if response.status_code == 200:
        return response.json()['data'][0][1]  # Response from the UDF proxy
    else:
        raise Exception(f"Failed to deploy bot: {response.text}")

# Add the new page function above the existing pages dictionary
def show_server_logs():
    st.subheader('Show Server Logs')

    # Dropdown for log type selection
    log_type = st.selectbox("Select log type:", ["Bot Service", "Harvester", "Task Service"])

    if log_type == "Bot Service":
        # Run Snowflake SQL commands for Bot Service
        status_result = session.sql(f"SELECT SYSTEM$GET_SERVICE_STATUS('{prefix}.GENESISAPP_SERVICE_SERVICE')").collect()
        logs_result = session.sql(f"SELECT SYSTEM$GET_SERVICE_LOGS('{prefix}.GENESISAPP_SERVICE_SERVICE',0,'genesis',1000)").collect()

        # Display the results in textareas
        st.markdown( status_result[0][0])
        st.text_area("Service Logs", logs_result[0][0], height=600)

    elif log_type == "Harvester":
        # Run Snowflake SQL commands for Harvester
        status_result = session.sql(f"SELECT SYSTEM$GET_SERVICE_STATUS('{prefix}.GENESISAPP_HARVESTER_SERVICE')").collect()
        logs_result = session.sql(f"SELECT SYSTEM$GET_SERVICE_LOGS('{prefix}.GENESISAPP_HARVESTER_SERVICE',0,'genesis-harvester',1000)").collect()

        # Display the results in textareas
        st.markdown(status_result[0][0])
        st.text_area("Harvester Logs", logs_result[0][0], height=600)

    elif log_type == "Task Service":
        # Run Snowflake SQL commands for Harvester
        status_result = session.sql(f"SELECT SYSTEM$GET_SERVICE_STATUS('{prefix}.GENESISAPP_TASK_SERVICE')").collect()
        logs_result = session.sql(f"SELECT SYSTEM$GET_SERVICE_LOGS('{prefix}.GENESISAPP_TASK_SERVICE',0,'genesis-harvester',1000)").collect()

        # Display the results in textareas
        st.markdown(status_result[0][0])
        st.text_area("Harvester Logs", logs_result[0][0], height=600)


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
        if cur_key == '<existing key present on server>':
            st.success('Your key is already set and active. If you want to change it, you can do so below.')
        st.write("Genesis Bots require access to OpenAI, as these are the only LLMs currently powerful enough to service the bots. Please visit https://platform.openai.com/signup to get a paid API key for OpenAI before proceeding.")
        llm_model = st.selectbox("Choose LLM Model:", ["OpenAI"])
        llm_api_key = st.text_input("Enter API Key:", value=cur_key)

        if "disable_submit" not in st.session_state:
            st.session_state.disable_submit = False

        if st.button("Submit API Key", key="sendllm", disabled=st.session_state.disable_submit):
            
            # Code to handle the API key submission will go here
            # This is a placeholder for the actual submission logic

            st.write('One moment while I validate the key and launch the bots...')
            with st.spinner('Validating API key and launching bots...'):
                config_response = configure_llm(llm_model, llm_api_key)
            
                if config_response['Success'] is False:
                    resp = config_response["Message"]
                    st.error(f"Failed to set LLM token: {resp}")
                    cur_key = ''
                else:
                    st.session_state.disable_submit = True
                    st.success("API key validated!")

                if config_response['Success']:
                    with st.spinner('Getting active bot details...'):
                        bot_details = get_bot_details()
                    if bot_details:
                        st.success("Bot details validated.")
                       # st.success("Reload this page to chat with your bots!")
                        if st.button("Next -> Click here to chat with your bots!"):
                            st.experimental_rerun()

            if cur_key == '<existing key present on server>':
                st.write("Reload this page to chat with your apps.")
            else:
                if cur_key is not None and cur_key != '':
                    if st.button("Next -> Click here to chat with your bots!"):
                        st.experimental_rerun()
                        # This button will be used to talk to the bot directly via Streamlit interface
                        # Placeholder for direct bot communication logic
                        #st.session_state['radio'] = "Chat with Bots"

def chat_page():

    def submit_button(prompt, chatmessage, intro_prompt = False):

        if not intro_prompt:
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
        
        if bot_avatar_image_url:
            # Display assistant response in chat message container
            with st.chat_message("assistant", avatar=bot_avatar_image_url):
                st.markdown(response)
            # Add assistant response to chat history
            st.session_state[f"messages_{selected_bot_id}"].append({"role": "assistant", "content": response, "avatar": bot_avatar_image_url})
        else:
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
            # get bot details
            bot_details.sort(key=lambda x: (not "Eve" in x["bot_name"], x["bot_name"]))
            bot_names = [bot["bot_name"] for bot in bot_details]
            bot_ids = [bot["bot_id"] for bot in bot_details]
            bot_intro_prompts = [bot["bot_intro_prompt"] for bot in bot_details]

            if len(bot_names) > 0:
                selected_bot_name = st.selectbox("Active Bots", bot_names)
                selected_bot_index = bot_names.index(selected_bot_name)
            selected_bot_id = bot_ids[selected_bot_index]
            selected_bot_intro_prompt = bot_intro_prompts[selected_bot_index]
            if not selected_bot_intro_prompt:
                selected_bot_intro_prompt = "Please provide a brief introduction of yourself and your capabilities."

            # get avatar images
            bot_images = get_metadata('bot_images')
            bot_avatar_image_url = ""
            if len(bot_images) > 0:
                bot_avatar_images = [bot["bot_avatar_image"] for bot in bot_images]
                bot_names = [bot["bot_name"] for bot in bot_images]
                selected_bot_image_index = bot_names.index(selected_bot_name)
                if selected_bot_image_index < 0:
                    bot_avatar_image_url = ""
                else:
                    encoded_bot_avatar_image = bot_avatar_images[selected_bot_image_index]
                    if not encoded_bot_avatar_image:
                        bot_avatar_image_url = "" 
                    else:
                        # Create data URL for the avatar image
                        bot_avatar_image_url = f"data:image/png;base64,{encoded_bot_avatar_image}"
                
                
            if st.button("New Chat", key="new_chat_button"):
            # Reset the chat history and thread ID for the selected bot
                if bot_avatar_image_url:
                    st.session_state[f"messages_{selected_bot_id}"] = [{"role": "assistant", "content": f"Hi, I'm {selected_bot_name}! How can I help you today?",  "avatar": bot_avatar_image_url}]
                else:
                    st.session_state[f"messages_{selected_bot_id}"] = [{"role": "assistant", "content": f"Hi, I'm {selected_bot_name}! How can I help you today?"}]
                st.session_state[f"thread_id_{selected_bot_id}"] = str(uuid.uuid4())
                # Clear the chat input
                #st.session_state[f"chat_input_{selected_bot_id}"] = ""
                # Rerun the app to reflect the changes
                # st.experimental_rerun()
            
    
            if f"thread_id_{selected_bot_id}" not in st.session_state:
                st.session_state[f"thread_id_{selected_bot_id}"] = str(uuid.uuid4())
    
            # Initialize chat history
            if f"messages_{selected_bot_id}" not in st.session_state:
                st.session_state[f"messages_{selected_bot_id}"] = []
                # st.session_state[f"messages_{selected_bot_id}"].append({"role": "assistant", "content": selected_bot_intro})
                submit_button(selected_bot_intro_prompt, st.chat_message("user"), True)
                st.experimental_rerun()
    
            # Display chat messages from history on app rerun
            for message in st.session_state[f"messages_{selected_bot_id}"]:
                if message["role"] == "assistant" and bot_avatar_image_url:
                    with st.chat_message(message["role"], avatar=bot_avatar_image_url):
                        st.markdown(message["content"])
                else:
                    with st.chat_message(message["role"]):
                        st.markdown(message["content"])
    
            # React to user input
            if prompt := st.chat_input("What is up?", key=f"chat_input_{selected_bot_id}"): 
                pass
    
            #response = f"Echo: {prompt}"
            if prompt != None:
                submit_button(prompt, st.chat_message("user"), False)
        except Exception as e:

            st.subheader(f"Error running Genesis GUI {e}")


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
    wh_text = f'''-- select role to use, generally ACCOUNTADMIN.  See documentation for required permissions if not using ACCOUNTADMIN.
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

-- to use on a local database in your account, call with the name of the database to grant
call GENESIS_LOCAL_DB.SETTINGS.grant_schema_usage_and_select_to_app('<your db name>',$APP_DATABASE);

-- see inbound shares 
show shares;

-- to grant an inbound shared database to the Genesis application 
grant imported privileges on database <inbound_share_db_name> to application IDENTIFIER($APP_DATABASE);

-- to grant access to the SNOWFLAKE share (Account Usage, etc.) to the Genesis application 
grant imported privileges on database SNOWFLAKE to application IDENTIFIER($APP_DATABASE);

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
        slack_ready = slack_tokens.get("SlackActiveFlag",False)

    # st.write(slack_tokens)

        # Display bot_details in a pretty grid using Streamlit
        if bot_details:
            try:
                for bot in bot_details:
                    #st.write(bot)
                    st.subheader(bot['bot_name'])
                    col1, col2 = st.columns(2)
                    with col1:
                        st.caption("Bot ID: " + bot['bot_id'])
                        available_tools = bot['available_tools'].strip("[]").replace('"', '').replace("'", "")
                        st.caption(f"Available Tools: {available_tools}")
                        bot_implementation = bot.get('bot_implementation', None)
                        if bot_implementation is not None:
                            st.caption(f"LLM Engine: {bot_implementation}")
                        # Display the files associated with the bot
                        bot_files = bot.get('files',None)
                        if bot_files == 'null' or bot_files == '' or bot_files == '[]':
                            bot_files = None
                        if bot_files is not None:
                            st.caption(f"Files: {bot_files}")
                        else:
                            st.caption("Files: None assigned")
                        user_id = bot.get('bot_slack_user_id','None')
                        if user_id is None:
                            user_id = 'None'
                        api_app_id = bot.get('api_app_id','None')
                        if api_app_id is None:
                            api_app_id = 'None'
                        st.caption("Slack User ID: " + user_id)
                        st.caption("API App ID: " + api_app_id)
              #          if bot['slack_app_level_key'] is None and bot['slack_deployed'] is False:
              #              st.markdown(f"**NO APP LEVEL KEY - NEED TO PROVIDE**")

                        if user_id == 'Pending_APP_LEVEL_TOKEN':
                            st.markdown(f"**To complete the setup on Slack for this bot, there are two more steps, first is to go to: https://api.slack.com/apps/{api_app_id}/general, scroll to App Level Tokens, add a token called 'app_token' with scope 'connections-write', and provide the results in the box below.**")
                            slack_app_level_key = st.text_input("Enter Slack App Level Key", key=f"slack_key_{bot['bot_id']}")
                            if st.button("Submit Slack App Level Key", key=f"submit_{bot['bot_id']}"):
                                provide_slack_level_key_response = provide_slack_level_key(bot['bot_id'], slack_app_level_key)
                                #st.write(provide_slack_level_key_response)
                                if provide_slack_level_key_response.get('success',False):
                                    st.success("Slack App Level Key provided successfully.")
                                    st.markdown(f"**To complete setup on Slack, there is one more step.  Cut and paste this link in a new browser window (appologies that it can't be clickable here):**")
                                    #st.text(f"{bot['auth_url']}")
                                    a = st.text_area("Link to use to authorize:", value=bot['auth_url'], height=200, disabled=True)
                                    st.markdown(f"**You may need to log into both Slack and Snowflake to complete this process.**")
                                else:
                                    st.error(f"Failed to provide Slack App Level Key: {provide_slack_level_key_response.get('error')}")

                        if bot['auth_url'] is not None and bot['slack_deployed'] is False and user_id != 'Pending_APP_LEVEL_TOKEN':
                            st.markdown(f"**To complete setup on Slack, there is one more step.  Cut and paste this link in a new browser window (appologies that it can't be clickable here).  You may need to log into both Slack and Snowflake to complete this process:**")
                            #st.text(f"{bot['auth_url']}")
                            st.text_area("Link to use to authorize:", value=bot['auth_url'], height=200, disabled=True)
                            st.markdown(f"**Once you do that, you should see a Successfully Deployed message.  Then go to Slack's Apps area at the bottom of the left-hand channel list panel, and press Add App, and search for the new bot by name. **")
                        
                 #st.caption("Runner ID: " + bot['runner_id'], unsafe_allow_html=True)
                        if bot['slack_active'] == 'N' or bot['slack_deployed'] == False:
                            if slack_ready and (bot['auth_url'] is None or bot['auth_url']==''):
                                if st.button(f"Deploy {bot['bot_name']} on Slack", key=f"deploy_{bot['bot_id']}"):
                                     # Call function to deploy bot on Slack (functionality to be implemented)
                                    deploy_response = deploy_bot(bot['bot_id'])
                                    #st.write(deploy_response)
                                    if deploy_response.get('Success') or deploy_response.get('success'):
                                        #st.write(deploy_response)
                                        #st.write(bot)
                                        st.success(f"The first of 3 steps to deploy {bot.get('bot_name')} to Slack is complete.  Refresh this page to see the next 2 steps to complete deployment to Slack. ")
                                       # st.write(deploy_response)
                                        if st.button("Press to Refresh Page for Next Steps", key=f"refresh_{bot['bot_id']}"):
                                            st.experimental_rerun()
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

    st.write('Genesis Bots needs rights to use a Snowflake compute engine, known as a Virtual Warehouse, to run queries on Snowflake. Please open another Snowflake window, go to Projects, and make a new Snowflake worksheet and run these commands to grant Genesis access to an existing Warehouse, or to make a new one for its use. This step does not provide Genesis Bots with access to any of your data, just the ability to run SQL on Snowflake in general.')
    
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
    st.write('Please go back to your Snowflake worksheet and run these commands to create a new compute pool and grant Genesis the rights to use it.  This uses the Snowflake small compute pool, which costs about 0.22 Snowflake Credits per hour, or about $10/day.  Once you start the server, you will be able to suspend it when not in use.')
    
    wh_text = f'''-- select role to use, generally Accountadmin or Sysadmin
use role ACCOUNTADMIN;

-- set the name of the installed application
set APP_DATABASE = '{app_name}';

-- remove an existing pool, if you've installed this app before
DROP COMPUTE POOL IF EXISTS GENESIS_POOL;

-- create the compute pool and associate it to this application
CREATE COMPUTE POOL IF NOT EXISTS GENESIS_POOL FOR APPLICATION IDENTIFIER($APP_DATABASE)
 MIN_NODES=1 MAX_NODES=1 INSTANCE_FAMILY='CPU_X64_S' AUTO_SUSPEND_SECS=3600 INITIALLY_SUSPENDED=FALSE;

-- give Genesis the right to use the compute pool
GRANT USAGE, OPERATE ON COMPUTE POOL GENESIS_POOL TO APPLICATION  IDENTIFIER($APP_DATABASE);
'''

    st.text_area("Compute Pool configuration script:", wh_text, height=420)

    st.write("We can't automatically test this, but if you've performed it same way you did on Step 1, you can now proceed to the next step.")
    st.write('**<< Now, click 3. Configure EAI <<**')
        

def config_eai():
 
    st.subheader('Step 3: Configure External Access Integration (EAI)')

    st.write("Genesis Bots currently uses OpenAI GPT4 as its main LLM, as it is the only model that we've found powerful and reliable enough to power our bots. To access OpenAI from the Genesis Server, you'll need to create a Snowflake External Access Integration so that the Genesis Server can call OpenAI. Genesis can also optionally connect to Slack, with some additional configuration, to allow your bots to interact via Slack.")
    st.write('The Genesis Server can also capture and output events to a Snowflake Event Table, allowing you to track what is happening inside the server. Optionally, these logs can be shared back to the Genesis Provider for enhanced support for your GenBots.')
    st.write('So please go back to the worksheet one more time, and run these commands to create a external access integration, and grant Genesis the rights to use it. Genesis will only be able to access the endpoints listed, OpenAI, and optionally Slack. The steps for adding the event logging are optional as well, but recommended.')
    
    wh_text = f'''-- select role to use, generally Accountadmin or Sysadmin
use role ACCOUNTADMIN;

-- set the name of the installed application
set APP_DATABASE = '{app_name}';

-- create a local database to store the network rule (you can change these to an existing database and schema if you like)
CREATE DATABASE IF NOT EXISTS GENESIS_LOCAL_DB; 
CREATE SCHEMA IF NOT EXISTS GENESIS_LOCAL_DB.SETTINGS;

-- create a network rule that allows Genesis Server to access OpenAI's API, and optionally Slack API and Azure Blob (for image generation) 
CREATE OR REPLACE NETWORK RULE GENESIS_LOCAL_DB.SETTINGS.GENESIS_RULE
 MODE = EGRESS TYPE = HOST_PORT
VALUE_LIST = ('api.openai.com', 'slack.com', 'www.slack.com', 'wss-primary.slack.com',
'wss-backup.slack.com',  'wss-primary.slack.com:443','wss-backup.slack.com:443',
'oaidalleapiprodscus.blob.core.windows.net:443');

-- create an external access integration that surfaces the above network rule
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION GENESIS_EAI
   ALLOWED_NETWORK_RULES = (GENESIS_LOCAL_DB.SETTINGS.GENESIS_RULE) ENABLED = true;

-- This allows Slack to callback into the Genbots service to active new Genbots on Slack
GRANT BIND SERVICE ENDPOINT ON ACCOUNT TO APPLICATION  IDENTIFIER($APP_DATABASE); 

-- grant Genesis Server the ability to use this external access integration
GRANT USAGE ON INTEGRATION GENESIS_EAI TO APPLICATION   IDENTIFIER($APP_DATABASE);

-- create a workspace schema for the data analysis bot Eliza to create object in when directed by a user
CREATE SCHEMA IF NOT EXISTS GENESIS_LOCAL_DB.ELIZA_WORKSPACE;
GRANT USAGE ON DATABASE GENESIS_LOCAL_DB TO APPLICATION IDENTIFIER($APP_DATABASE);
GRANT ALL ON SCHEMA GENESIS_LOCAL_DB.ELIZA_WORKSPACE TO APPLICATION IDENTIFIER($APP_DATABASE);

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
                    with st.spinner('Starting Compute Pool & Genesis Server (can take 3-15 minutes the first time for compute pool startup, use "show compute pools;" to see status)...'):
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
                st.write("**Now push the button below, you're one step away from making and chatting with your bots!**")
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
    start_stop_text = f'''USE DATABASE IDENTIFIER('{app_name}');

// pause service

call {app_name}.core.stop_app_instance('APP1');
alter compute pool GENESIS_POOL SUSPEND; -- to also pause the compute pool

// resume service

alter compute pool GENESIS_POOL RESUME; -- if you paused the compute pool
call {app_name}.core.start_app_instance('APP1','GENESIS_POOL','GENESIS_EAI','{st.session_state.wh_name}'); 

// check service

USE DATABASE IDENTIFIER($APP_DATABASE);
USE SCHEMA APP1;

SELECT SYSTEM$GET_SERVICE_STATUS('GENESISAPP_SERVICE_SERVICE');
SELECT SYSTEM$GET_SERVICE_LOGS('GENESISAPP_SERVICE_SERVICE',0,'genesis',1000);

// reinitialize -- note: this wipes out the app metadata and existing harvests and bots

call {app_name}.core.drop_app_instance('APP1');
CALL {app_name}.CORE.INITIALIZE_APP_INSTANCE('APP1','GENESIS_POOL','GENESIS_EAI','{st.session_state.wh_name}');


    '''
    st.text_area("", start_stop_text, height=620)

if SnowMode:
    try:
        status_query = f"select v.value:status::varchar status from (select parse_json(system$get_service_status('{prefix}.GENESISAPP_SERVICE_SERVICE'))) t, lateral flatten(input => t.$1) v"
        service_status_result = session.sql(status_query).collect()
        if service_status_result[0][0] != 'READY':
            with st.spinner('Waiting on Genesis Services to start...'):
                service_status = st.empty()
                while True:
                    service_status.text('Genesis Service status: ' + service_status_result[0][0])
                    if service_status_result[0][0] == 'SUSPENDED':
                        # show button to start service
                        if st.button('Click to start Genesis Service'):
                            with st.spinner('Genesis Services is starting...'):
                                try:
                                    # Execute the command and collect the results
                                    time.sleep(15)
                                    service_start_result = session.sql(f"call {app_name}.core.start_app_instance('APP1','GENESIS_POOL','GENESIS_EAI','{st.session_state.wh_name}')").collect()
                                    if service_start_result:
                                        service_status.text('Genesis Service status: ' + service_status_result[0][0])
                                    else:
                                        time.sleep(10)
                                except Exception as e:
                                    st.error(f'Error connecting to Snowflake: {e}')
                    service_status_result = session.sql(status_query).collect()
                    service_status.text('Genesis Service status: ' + service_status_result[0][0])
                    if service_status_result[0][0] == 'READY':
                        service_status.text('')
                        st.experimental_rerun()
                        
                    time.sleep(10)         

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
        "LLM Model & Key": llm_config,
      #  "Setup Ngrok": setup_ngrok, 
        "Setup Slack Connection": setup_slack,
        "Grant Data Access": grant_data,
        "Harvestable Data": db_add_to_harvester,
        "Harvester Status": db_harvester,
        "Bot Configuration": bot_config,
        "Server Stop/Start": start_stop,
        "Server Logs": show_server_logs,
    }
    
    if st.session_state.get('needs_keys', False):
        del pages["Chat with Bots"]
    
#    if SnowMode == True:
#        del pages["Setup Ngrok"]
    
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
        
