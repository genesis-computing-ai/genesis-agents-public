#docs:  https://docs.snowflake.com/LIMITEDACCESS/cortex-llm-rest-api
import streamlit as st
import streamlit as st
import sseclient
import os
import jsonstream
import json
import requests
import snowflake.connector
st.title("Cortex LLM Rest API")

SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT_OVERRIDE", None)
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER_OVERRIDE", None)
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD_OVERRIDE", None)
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE_OVERRIDE", None)
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE_OVERRIDE", None)
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE_OVERRIDE", None)

CONN = snowflake.connector.connect(
    user=SNOWFLAKE_USER,
    password=SNOWFLAKE_PASSWORD,
    account=SNOWFLAKE_ACCOUNT, 
    port=443,
    warehouse=SNOWFLAKE_WAREHOUSE,
    role=SNOWFLAKE_ROLE,
    database=SNOWFLAKE_DATABASE,
)
SNOWFLAKE_HOST = CONN.host
st.write(CONN.host)
url=f"https://{SNOWFLAKE_HOST}/api/v2/cortex/inference/complete"
headers = {
    "Accept": "text/event-stream",
    "Content-Type": "application/json",
    "Authorization": f'Snowflake Token="{CONN.rest.token}"',
}


model = st.sidebar.selectbox("Select base model:",["llama3.1-405b","mixtral-8x7b", "llama3-8b", "mistral-large","mistral-7b","gemma-7b","llama2-70b-chat", "snowflake-arctic", "llama3-70b", "reka-flash"])



if "messages" not in st.session_state:
    st.session_state.messages = []

for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

full_response = ""
def stream_data(response):
    global full_response
    client = sseclient.SSEClient(response)
    for event in client.events():
        d = json.loads(event.data)
        try:
            response = d['choices'][0]['delta']['content']
            full_response += response
            yield response
        except Exception:
            pass
    

if prompt := st.chat_input("What is up?"):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        prompt = prompt.replace("'","\\'") #account for pesky quotes
        data = {
            "model": model,
            "messages": [{"content": st.session_state.messages[-1]["content"]}],
            "stream": False,
        }
        response = requests.post(url, json=data, stream=False, headers=headers)
        st.write(response.content)
      #  st.write_stream(stream_data(response))
    st.session_state.messages.append({"role": "assistant", "content": full_response})
    