import streamlit as st
import os, json

st.set_page_config(layout="wide")

SnowMode = False

import time
import uuid
import datetime
import pandas as pd
if SnowMode:
    from snowflake.snowpark.context import get_active_session
    session = get_active_session()


def get_slack_tokens():

    import requests
    import json

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

    # add SnowMode
    url = "http://127.0.0.1:8080/udf_proxy/configure_ngrok_token"
    headers = {"Content-Type": "application/json"}

    data = json.dumps({"data": [[0, ngrok_auth_token, ngrok_use_domain, ngrok_domain]]})

    response = requests.post(url, headers=headers, data=data)
    if response.status_code == 200:
        return response.json()['data'][0][1] # response message
    else:
        return "Error", f"Failed to set ngrok tokens: {response.text}"


def set_slack_tokens(slack_app_token, slack_app_refresh_token):
    """
    Calls the /udf_proxy/configure_slack_app_token endpoint to validate and set the Slack app token and refresh token.

    Args:
        slack_app_token (str): The Slack App Config Token.
        slack_app_refresh_token (str): The Slack App Refresh Token.
    """
    import requests
    import json

    # add SnowMode
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

    url = "http://127.0.0.1:8080/udf_proxy/deploy_bot"
    headers = {"Content-Type": "application/json"}
    data = json.dumps({"data": [[0, bot_id]]})

    response = requests.post(url, headers=headers, data=data)
    if response.status_code == 200:
        return response.json()['data'][0][1]  # Response from the UDF proxy
    else:
        raise Exception(f"Failed to deploy bot: {response.text}")


if SnowMode:
    try:
        sql = f"select app1.response_udf('test') "
        data = session.sql(sql).collect()
    except Exception as e:
        #st.write(e)
        data = None
else:
    data = 'Local Mode'

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
        st.write("Genesis Bots require access to OpenAI or Reka LLMs, as these are the only models currently powerful enough to service the bots. Please visit https://platform.openai.com/signup or https://platform.reka.ai/ to get a paid API key for OpenAI or Reka before proceeding.")
        llm_model = st.selectbox("Choose LLM Model:", ["OpenAI", "Reka"])
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

        if SnowMode:
            sql = f"select chattest_app.app1.submit_udf('{prompt}', '{st.session_state[f'thread_id_{selected_bot_id}']}', '{selected_bot_id}')"
            data = session.sql(sql).collect()
            # TODO ^^ make this a bind query 
            request_id = data[0][0]
        else:
            request_id = submit_to_udf_proxy(input_text=prompt, thread_id=st.session_state[f"thread_id_{selected_bot_id}"], bot_id=selected_bot_id)
        response = 'not found'

        with st.spinner('Thinking...'):
            while response == 'not found':
                if SnowMode:
                    sql = f"select chattest_app.app1.response_udf('{request_id}', '{selected_bot_id}') "
                    data = session.sql(sql).collect()
                    #st.write(data)
                    if data:  # Check if data is not empty
                        response = data[0][0]
                    else:
                        respose = 'not found'
                else:
                    response = get_response_from_udf_proxy(uu=request_id, bot_id=selected_bot_id)
                time.sleep(0.5)
        # Display assistant response in chat message container
        with st.chat_message("assistant"):
            st.markdown(response)
        # Add assistant response to chat history
        st.session_state[f"messages_{selected_bot_id}"].append({"role": "assistant", "content": response})


    bot_details = get_bot_details() 
   #st.write(bot_details)
    if bot_details == {'Success': False, 'Message': 'Needs LLM Type and Key'}:
        #st.success('Welcome! Before you chat with bots, please setup your LLM Keys')
        #time.sleep(.3)
        #st.session_state['radio'] = "Setup LLM Model & Key"
        llm_config()
        #st.experimental_rerun()
    else: 




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
    st.write('By providing a Slack App Config Token and Refresh Token, Genesis Bots can create, update, and remove Genesis Bots from your Slack environment.  Go to https://api.slack.com/apps and create an App Config Token and App Config Refresh Token, paste them below, and press Update. ')
    # Text input for Slack App Token

    if tok == "...": 
        tok = ""
    if ref == "...":
        ref = ""
   
    slack_app_token = st.text_input("Slack App Token", value=tok if tok else "")
    # Text input for Slack App Refresh Token
    slack_app_refresh_token = st.text_input("Slack App Refresh Token", value=ref if ref else "")
    # Button to submit new tokens

    if st.button("Update Slack Tokens"):
        # Call function to update tokens (functionality to be implemented)
        # Call set_slack_tokens and display the result
        resp = set_slack_tokens(slack_app_token, slack_app_refresh_token)
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
            st.error(f"Invalid Ngrok Token or other Activation Error")
        else:
            st.success("Ngrok token updated.")        
        pass


def db_harvester():
    
    
    harvest_control = get_metadata('harvest_control')
    harvest_summary = get_metadata('harvest_summary')
    # Check 
    st.write(harvest_control)
    
    harvest_control_df = pd.DataFrame(harvest_control)
    harvest_control_df['schema_exclusions'] = harvest_control_df['schema_exclusions'].apply(lambda x: ["None"] if not x else x)
    harvest_control_df['schema_inclusions'] = harvest_control_df['schema_inclusions'].apply(lambda x: ["All"] if not x else x)

    harvest_summary_df = pd.DataFrame(harvest_summary)
    # Reordering columns instead of sorting rows
    column_order = ['source_name', 'database_name', 'schema_name', 'role_used_for_crawl', 'last_change_ts', 'objects_crawled']
    harvest_summary_df = harvest_summary_df[column_order]

    # Calculate the sum of objects_crawled using the DataFrame
    total_objects_crawled = harvest_summary_df['objects_crawled'].sum()
    # Find the most recent change timestamp using the DataFrame
    most_recent_change_str = str(harvest_summary_df['last_change_ts'].max()).split(".")[0]

    harvester_status = 'Offline'
    # Display metrics at the top
    col0, col1, col2 = st.columns(3)
    with col0:
        st.metric(label="Harvester Status", value=harvester_status)
    with col1:
        st.metric(label="Total Objects Crawled", value=total_objects_crawled)
    with col2:
        st.metric(label="Most Recent Change", value=most_recent_change_str)

    st.subheader("Sources and Databases being Harvested")
    st.dataframe(harvest_control_df, use_container_width=True)

    st.subheader("Database and Schema Harvest Status")
    st.dataframe(harvest_summary_df, use_container_width=True, height=500)
    
    st.subheader("Available Databases for Harvesting")
    available_databases = get_metadata('available_databases')
    if available_databases:
        available_databases_df = pd.DataFrame(available_databases)
        st.dataframe(available_databases_df, use_container_width=True)
        
        for index, row in available_databases_df.iterrows():
            database_name = row['DatabaseName']
            if st.button(f"Add {database_name} to Harvest", key=f"add_{database_name}"):
                add_to_harvest_response = add_to_harvest(database_name)
                if add_to_harvest_response.get('Success'):
                    st.success(f"Database {database_name} added to harvest successfully.")
                else:
                    st.error(f"Failed to add {database_name} to harvest: {add_to_harvest_response.get('Error')}")
    else:
        st.write("No available databases to display.")

    
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
                        st.caption("Bot ID: " + bot['bot_id'], unsafe_allow_html=True)
                        available_tools = bot['available_tools'].strip("[]").replace('"', '').replace("'", "")
                        st.caption(f"Available Tools: {available_tools}", unsafe_allow_html=True)
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
                        st.caption("UDF Active: " + ('Yes' if bot['udf_active'] == 'Y' else 'No'), unsafe_allow_html=True)
                        st.caption("Slack Active: " + ('Yes' if bot['slack_active'] == 'Y' else 'No'), unsafe_allow_html=True)
                        st.caption("Slack Deployed: " + ('Yes' if bot['slack_deployed'] else 'No'), unsafe_allow_html=True)
                        st.text_area(label="Instructions", value=bot['bot_instructions'], height=100)



            except ValueError as e:
                st.error(f"Failed to parse bot details: {e}")
        else:
            st.write("No bot details available.")
    

pages = {
#    "Talk to Your Bots": "/",
    "Chat with Bots": chat_page,
    "Setup LLM Model & Key": llm_config,
    "Setup Ngrok": setup_ngrok,
    "Setup Slack Connection": setup_slack,
    "Database Harvester": db_harvester,
    "Bot Configuration": bot_config,
}

if st.session_state.get('needs_keys', False):
    del pages["Chat with Bots"]

st.sidebar.title("Genesis Bots Configuration")
  
selection = st.sidebar.radio("Go to:", list(pages.keys()), index=list(pages.keys()).index(st.session_state.get('radio', list(pages.keys())[0])))
if selection in pages: 
    pages[selection]()


