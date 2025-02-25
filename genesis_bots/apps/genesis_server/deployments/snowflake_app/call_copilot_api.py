import json
import pandas as pd
import requests
import streamlit as st
import time
import os
from snowflake.connector import connect



DATABASE = "<database>"
SCHEMA = "<schema>"
STAGE = "<stage>"
FILE = "<file>"

HOST = "mmb84124.snowflakecomputing.com"

CONN = connect(
    user=os.getenv('SNOWFLAKE_USER_OVERRIDE',None),
    password=os.getenv('SNOWFLAKE_PASSWORD_OVERRIDE', None),
    host=HOST,
    account=os.getenv('SNOWFLAKE_ACCOUNT_OVERRIDE',None),
    warehouse=os.getenv('SNOWFLAKE_WAREHOUSE_OVERRIDE', None),
    role=os.getenv('SNOWFLAKE_ROLE_OVERRIDE', None),
)


def send_message(prompt: str) -> dict:
    """Calls the REST API and returns the response."""
    request_body = {
        "role": "user",
        "content": [{"type": "text", "text": prompt}],
        "modelPath": FILE,
    }
    num_retry, max_retries = 0, 10
    while True:
        st.write(CONN.rest.token)
        resp = requests.post(
            (
                f"https://{HOST}/api/v2/databases/{DATABASE}/"
                f"schemas/{SCHEMA}/copilots/{STAGE}/chats/-/messages"
            ),
            json=request_body,
            headers={
                "Authorization": f'Snowflake Token="{CONN.rest.token}"',
                "Content-Type": "application/json",
            },
        )
        if resp.status_code < 400:
            return resp.json()
        else:
            if num_retry >= max_retries:
                resp.raise_for_status()
            num_retry += 1
        time.sleep(1)


def process_message(prompt: str) -> None:
    """Processes a message and adds the response to the chat."""
    st.session_state.messages.append(
        {"role": "user", "content": [{"type": "text", "text": prompt}]}
    )
    with st.chat_message("user"):
        st.markdown(prompt)
    with st.chat_message("assistant"):
        with st.spinner("Generating response..."):
            response = send_message(prompt=prompt)
            content = response["messages"][-1]["content"]
            display_content(content=content)
    st.session_state.messages.append({"role": "assistant", "content": content})


def display_content(content: list, message_index: int = None) -> None:
    """Displays a content item for a message."""
    message_index = message_index or len(st.session_state.messages)
    for item in content:
        if item["type"] == "text":
            if "<SUGGESTION>" in item["text"]:
                suggestion_response = json.loads(item["text"][12:])[0]
                st.markdown(suggestion_response["explanation"])
                with st.expander("Suggestions", expanded=True):
                    for suggestion_index, suggestion in enumerate(
                        suggestion_response["suggestions"]
                    ):
                        if st.button(
                            suggestion, key=f"{message_index}_{suggestion_index}"
                        ):
                            st.session_state.active_suggestion = suggestion
            else:
                st.markdown(item["text"])
        elif item["type"] == "sql":
            with st.expander("SQL Query", expanded=False):
                st.code(item["statement"], language="sql")
            with st.spinner("Running SQL..."):
                df = pd.read_sql(item["statement"], CONN)
                st.dataframe(df)


st.title("Cortex Copilot API")
st.markdown(f"Semantic Model: `{FILE}`")

if "messages" not in st.session_state:
    st.session_state.messages = []
    st.session_state.active_suggestion = None

for message_index, message in enumerate(st.session_state.messages):
    with st.chat_message(message["role"]):
        display_content(content=message["content"], message_index=message_index)

if user_input := st.chat_input("What is your question?"):
    process_message(prompt=user_input)

if st.session_state.active_suggestion:
    process_message(prompt=st.session_state.active_suggestion)
    st.session_state.active_suggestion = None