import streamlit as st
from datetime import datetime
from genesis_bots.api.genesis_api import GenesisAPI  # Import your GenesisAPI class

# Initialize GenesisAPI
api = GenesisAPI(server_type="remote-snowflake", scope="GENESIS_BOTS_ALPHA")

# Tabs for the dashboard
st.title("Genesis Dashboard")
tabs = st.tabs(["Bots", "Projects", "Processes", "Knowledge"])

# Bots Tab
with tabs[0]:
    st.header("Bots")
    bots = api.get_all_bots()  # Directly fetch bots
    selected_bot = st.selectbox("Select a Bot", bots)
    if selected_bot:
        bot = api.get_bot(selected_bot)
        st.write(f"**Instructions:** {bot.BOT_INSTRUCTIONS}")
        st.write(f"**Intro Prompt:** {bot.BOT_INTRO_PROMPT}")
        st.write(f"**Slack User ID:** {bot.SLACK_USER_ALLOW}")

        # Chat interaction
        #st.write("### Chat History")
        with st.expander("Chat History"):
            message_log = api.get_message_log(bot.BOT_ID)  # Directly fetch message log
            for index, msg in message_log.iterrows():
                if msg.MESSAGE_TYPE == "user":
                    st.markdown(f"**You:** {msg.MESSAGE_PAYLOAD}")
                else:
                    st.markdown(f"**{bot.BOT_NAME}:** {msg.MESSAGE_PAYLOAD}")

        # Input for new messages
        message = st.text_input(f"Send a message to {bot.BOT_NAME}", key=f"input_{bot.BOT_ID}")
        if st.button(f"Send Message to {bot.BOT_NAME}", key=f"send_{bot.BOT_ID}"):
            response = api.add_message(bot.BOT_ID, message)  # Send the message
            bot_response = api.get_response(bot.BOT_ID, response.get("request_id"))  # Get bot's response
            st.success(f"Bot Response: {bot_response}")

# Projects Tab
with tabs[1]:
    st.header("Projects")
    projects = api.get_all_projects()  # Directly fetch projects
    selected_project = st.selectbox("Select a Project", projects)
    if selected_project:
        project = api.get_project(selected_project)
        st.write(f"**Description:** {project.DESCRIPTION}")
        st.write(f"**Requested By:** {project.REQUESTED_BY_USER}")
        st.write(f"**Target Completion Date:** {project.TARGET_COMPLETION_DATE}")
        st.write(f"**Last Updated:** {project.UPDATED_AT}")

# Processes Tab
with tabs[2]:
    st.header("Processes")
    processes = api.get_all_processes()  # Directly fetch processes
    selected_process = st.selectbox("Select a Process", processes)
    if selected_process:
        process = api.get_process(selected_process)
        st.write(f"**Created At:** {process.CREATED_AT}")
        st.write(f"**Updated At:** {process.UPDATED_AT}")
        st.write(f"**Process ID:** {process.PROCESS_ID}")
        st.write(f"**Bot ID:** {process.BOT_ID}")
        st.write(f"**Process Name:** {process.PROCESS_NAME}")
        st.write(f"**Process Instructions:** {process.PROCESS_INSTRUCTIONS}")
        st.write(f"**Process Description:** {process.PROCESS_DESCRIPTION}")
        st.write(f"**Note ID:** {process.NOTE_ID}")
        st.write(f"**Process Config:** {process.PROCESS_CONFIG}")
        st.write(f"**Hidden:** {process.HIDDEN}")

# Knowledge Tab
with tabs[3]:
    st.header("Knowledge Threads")
    knowledge_threads = api.get_all_knowledge()  # Directly fetch knowledge threads
    selected_thread = st.selectbox("Select a Knowledge Thread", knowledge_threads)
    if selected_thread:
        thread = api.get_knowledge(selected_thread)
        st.write(f"**Summary:** {thread.THREAD_SUMMARY}")
        st.write(f"**Last Updated:** {thread.LAST_TIMESTAMP}")
        st.write(f"**User Learning:** {thread.USER_LEARNING}")
        st.write(f"**Tool Learning:** {thread.TOOL_LEARNING}")
        st.write(f"**Data Learning:** {thread.DATA_LEARNING}")