#docs:  https://docs.snowflake.com/LIMITEDACCESS/cortex-llm-rest-api
import streamlit as st
import streamlit as st
import sseclient
import os
import jsonstream
import pprint
import json
import requests
import snowflake.connector

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
url=f"https://{SNOWFLAKE_HOST}/api/v2/cortex/inference/complete"
headers = {
    "Accept": "text/event-stream",
    "Content-Type": "application/json",
    "Authorization": f'Snowflake Token="{CONN.rest.token}"',
}


model = "llama3.1-405b"


full_response = ""
def stream_data(response):
    global full_response
    client = sseclient.SSEClient(response)
    #st.write(client.events)
    st.write("hi ya ")
    for event in client.events():
        st.write("hi")
        d = json.loads(event.data)
        try:
            response = d['choices'][0]['delta']['content']
            full_response += response
            yield response
        except Exception:
            pass
    

prompt = "What is up?"
prompt = prompt.replace("'","\\'") #account for pesky quotes
data = {
    "model": model,
    "messages": [{"content": prompt}],
    "stream": True,
}
response = requests.post(url, json=data, stream=True, headers=headers)
client = sseclient.SSEClient(response)
for event in client.events():
    d = json.loads(event.data)
    r = d['choices'][0]['delta']['content']
    pprint.pprint(r)
  