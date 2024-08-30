import streamlit as st
import uuid
from utils.snowflake_connector import get_bot_details
from .get_bot_response import get_bot_response

def chat_page(session, run_mode, bot_details):
    st.title("Chat with Bots")

    bot_details = get_bot_details(session, run_mode)
    if not bot_details:
        st.error("No bots available. Please configure your bots first.")
        return

    bot_details.sort(key=lambda x: (not "Eve" in x["bot_name"], x["bot_name"]))
    bot_names = [bot["bot_name"] for bot in bot_details]
    bot_ids = [bot["bot_id"] for bot in bot_details]

    col1, col2 = st.columns([3, 1])
    with col1:
        selected_bot_name = st.selectbox("Select a bot to chat with:", bot_names)
    with col2:
        if st.button("New Chat"):
            selected_bot_id = bot_ids[bot_names.index(selected_bot_name)]
            st.session_state[f"messages_{selected_bot_id}"] = []
            st.session_state[f"thread_id_{selected_bot_id}"] = str(uuid.uuid4())
            st.experimental_rerun()

    selected_bot_index = bot_names.index(selected_bot_name)
    selected_bot_id = bot_ids[selected_bot_index]

    if f"messages_{selected_bot_id}" not in st.session_state:
        st.session_state[f"messages_{selected_bot_id}"] = []
        st.session_state[f"thread_id_{selected_bot_id}"] = str(uuid.uuid4())

    if "last_response" not in st.session_state:
        st.session_state["last_response"] = ""
   
    for message in st.session_state[f"messages_{selected_bot_id}"]:
        if st.session_state["last_response"] != message["content"]:
            with st.chat_message(message["role"]):
                st.markdown(message["content"])

    prompt = st.chat_input("What is up?")
    if prompt:
     #   st.write(f"User input received: {prompt}")  # Debug log
        st.session_state[f"messages_{selected_bot_id}"].append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        with st.chat_message("assistant"):
            full_response = st.write_stream(get_bot_response(session, run_mode, prompt, selected_bot_id, None))
        
      #  st.write(f"Full response received: {full_response[:50]}...")  # Debug log
        st.session_state[f"messages_{selected_bot_id}"].append({"role": "assistant", "content": full_response})

        if st.session_state["last_response"] == "":
            st.session_state["last_response"] = full_response

        # Handle bot avatar if needed
        bot_avatar_image_url = next((bot.get('bot_avatar_image') for bot in bot_details if bot['bot_id'] == selected_bot_id), None)
        if bot_avatar_image_url:
            st.session_state[f"messages_{selected_bot_id}"][-1]["avatar"] = bot_avatar_image_url

if __name__ == "__main__":
    st.write("This page should be run as part of the main Streamlit app.")