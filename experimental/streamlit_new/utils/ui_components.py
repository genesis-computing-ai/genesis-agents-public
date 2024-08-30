import streamlit as st

def render_chat_message(message):
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

def create_bot_card(bot):
    with st.expander(bot["bot_name"], expanded=False):
        st.write(f"Bot ID: {bot['bot_id']}")
        st.write(f"Available Tools: {', '.join(bot['available_tools'])}")
        st.write(f"Slack Active: {'Yes' if bot['slack_active'] == 'Y' else 'No'}")
        st.write(f"Instructions: {bot['bot_instructions']}")

def render_status_indicator(status):
    if status == "READY":
        st.success("Genesis Service is Ready")
    elif status == "SUSPENDED":
        st.warning("Genesis Service is Suspended")
    else:
        st.info(f"Genesis Service Status: {status}")

def create_sidebar():
    st.sidebar.title("Genesis Bots Configuration")
    pages = [
        "Chat with Bots",
        "LLM Model & Key",
        "Setup Slack Connection",
        "Grant Data Access",
        "Harvester Status",
        "Bot Configuration",
        "Server Stop/Start",
        "Server Logs",
        "Support and Community"
    ]
    return st.sidebar.radio("Go to:", pages)