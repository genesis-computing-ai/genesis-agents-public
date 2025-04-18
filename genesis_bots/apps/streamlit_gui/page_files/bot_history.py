import streamlit as st
from utils import get_metadata, get_metadata_cached
import pandas as pd
import os
import random
import traceback
from page_files.chat_page import set_initial_chat_sesssion_data, ChatMessage
import uuid


def bot_history():
    # Custom CSS for back button styling
    uploader_key = random.randint(0, 1 << 32)
    st.markdown("""
        <style>
        .back-button {
            margin-bottom: 1.5rem;
        }
        
        .back-button .stButton > button {
            width: 500px !important;    
            text-align: left !important;
            justify-content: flex-start !important;
            background-color: transparent !important;
            border: none !important;
            color: #FF4B4B !important;
            margin: 0 !important;
            font-weight: 600 !important;
            box-shadow: none !important;
            font-size: 1em !important;
            padding: 0.5rem 1rem !important;
        }
        
        .back-button .stButton > button:hover {
            background-color: rgba(255, 75, 75, 0.1) !important;
            box-shadow: none !important;
            transform: none !important;
        }
        </style>
    """, unsafe_allow_html=True)

    # Back button
    st.markdown('<div class="back-button">', unsafe_allow_html=True)
    if st.button("← Back to Chat", key="back_to_chat", use_container_width=True):
        st.session_state["selected_page_id"] = "chat_page"
        st.session_state["radio"] = "Chat with Bots"
        st.session_state['hide_chat_elements'] = False
        st.rerun()
    st.markdown('</div>', unsafe_allow_html=True)

    st.title("Bot History")
    
    try:
        
        from utils import get_bot_details
        bot_details = get_bot_details()
        bot_details.sort(key=lambda bot: (not "Eve" in bot["bot_name"], bot["bot_name"]))
        available_bots = [bot["bot_name"] for bot in bot_details]
        
        selected_bot = st.selectbox("Select a Bot:", available_bots)
        
        # Convert connections list directly to DataFrame
        if selected_bot:
            threads = get_metadata(f"get_list_threads {selected_bot}") 

            threads_list = [f"{item['TIMESTAMP']} - {item['THREAD_ID']}" for item in threads]       

            selected_thread = st.selectbox("Select A thread:", threads_list)

            if selected_thread:
                _, selected_thread = selected_thread.split(" - ")
                thread_id = selected_thread

                thread_data = get_metadata(f"get_thread {thread_id}")
            
                # Get bot avatar image like in chat_page
                bot_images = get_metadata_cached("bot_images")
                bot_avatar_image_url = None
                if len(bot_images) > 0:
                    # Use the default G logo image for all bots
                    encoded_bot_avatar_image = bot_images[0]["bot_avatar_image"]
                    if encoded_bot_avatar_image:
                        bot_avatar_image_url = f"data:image/png;base64,{encoded_bot_avatar_image}"
                
                # Override file_path and display raw thread data
                file_path = f"Thread {thread_id}"
                
                # Format thread data as chat messages
                st.markdown(f"### {file_path}")
                
                # Parse thread data into messages
                messages = eval(thread_data) if isinstance(thread_data, str) else thread_data
                # Display each message in the thread
                first_bot = None
                for message in messages:
                    message_type = message[0]
                    message_content = message[1]
                    bot_id = message[2]
                    if message_type == "User Prompt":
                        with st.chat_message("user"):
                            st.markdown(message_content)
                    elif message_type == f"Assistant Response":
                        if first_bot is None:
                            first_bot = bot_id
                        with st.chat_message("assistant", avatar=bot_avatar_image_url):
                            st.markdown(message_content)
                

                if st.button("Continue this Thread", use_container_width=True):
                    try:
                        # Get current bot ID from thread data and look up proper name
                        current_bot_id = selected_bot                        

                        # Look up the proper bot name
                        bot_details = get_bot_details()
                        proper_bot_name = next((bot["bot_name"] for bot in bot_details if bot["bot_id"].lower() == current_bot_id.lower()), current_bot_id)
                        
                        # Set up new session - all these use bot NAME
                        new_thread_id = str(uuid.uuid4())
                        new_session = f"🤖 {proper_bot_name} ({new_thread_id[:8]})"
                        if 'active_sessions' not in st.session_state:
                            st.session_state.active_sessions = []
                        st.session_state.active_sessions = [new_session] + [
                            s for s in st.session_state.active_sessions
                            if s != new_session
                        ]

                        # Session state variables
                        st.session_state["current_thread_id"] = new_thread_id
                        st.session_state["current_bot"] = proper_bot_name  # Uses bot NAME
                        st.session_state["current_session"] = new_session  # Uses bot NAME                        
                        st.session_state["show_new_chat_selector"] = False
                        st.session_state["active_chat_started"] = True
                        history = []
                        for chat in messages:
                            role = 'assistant' if chat[0] == 'Assistant Response' else 'user'
                            history.append(ChatMessage(role=role, content=chat[1]))
                        st.session_state[f"messages_{new_thread_id}"] = history

                        # Set up the thread continuation data
                        initial_message = f"!thread {thread_id}"
                        set_initial_chat_sesssion_data(
                            bot_name=proper_bot_name,  # Uses bot NAME
                            initial_prompt=initial_message,
                            initial_message=None
                        )

                        # Make sure chat elements are visible
                        st.session_state['hide_chat_elements'] = False
                        
                        # Navigation
                        st.session_state["radio"] = "Chat with Bots"
                        st.session_state["selected_page_id"] = "chat_page"
                        
                        st.rerun()
                    except Exception as e:
                        st.error(f"Failed to create chat session: {str(e)}")
    
    except Exception as e:
        st.error(f"An error occurred while managing bot document: {str(e)}")
        st.error(traceback.format_exc())
    
