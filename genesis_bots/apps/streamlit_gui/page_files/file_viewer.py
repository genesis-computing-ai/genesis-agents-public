import streamlit as st
import os
import re
import uuid
from utils import get_bot_details

def set_initial_chat_sesssion_data(bot_name, initial_prompt, initial_message):
    """
    Sets the initial chat session data in the session state.

    This function is used to initialize the chat session with a specific bot,
    an initial prompt, and an initial bot message.
    """
    if initial_message:
        # Mark the initial welcome message as an intro prompt so
        # that later the system knows an introductory message is already present.
        initial_message = ChatMessage(role="assistant", content=initial_message, is_intro_prompt=False)
    st.session_state.initial_chat_session_data = dict(
        bot_name=bot_name,
        initial_prompt=initial_prompt,
        initial_message=initial_message
    )

def strip_ansi_codes(text):
    """Remove ANSI escape codes from text"""
    ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
    return ansi_escape.sub('', text)

def file_viewer():
    # Add CSS for code block styling
    st.markdown("""
        <style>
        .back-button {
            margin-bottom: 1.5rem;
        }
        
        .back-button .stButton > button {
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

        /* Updated code block styling */
        .stCodeBlock {
            max-width: 90% !important;
            margin: 0 auto !important;
        }
        
        .stCodeBlock > div > pre {
            white-space: pre-wrap !important;
            word-wrap: break-word !important;
            overflow-wrap: break-word !important;
        }

        code {
            white-space: pre-wrap !important;
            word-wrap: break-word !important;
            overflow-wrap: break-word !important;
        }
        </style>
    """, unsafe_allow_html=True)

    st.markdown('<div class="back-button">', unsafe_allow_html=True)
    if st.button("← Back to Todo Details", key="back_from_file_viewer"):
        # Restore previous state
        st.session_state["selected_page_id"] = "todo_details"
        st.session_state["radio"] = "Todo Details"
        st.session_state['hide_chat_elements'] = False
        
        # Restore todo details state
        if "previous_todo_id" in st.session_state:
            todo_id = st.session_state["previous_todo_id"]
            st.session_state[f"history_{todo_id}"] = True  # Restore history expander state
            
        st.rerun()
    st.markdown('</div>', unsafe_allow_html=True)

    # Get file path from session state
    file_path = st.session_state.get("file_path_to_view", None)
    
    # Check if this is a thread ID request
    if file_path and file_path.startswith("Thread:"):
        thread_id = file_path.split(":", 1)[1]
        try:
            from utils import get_metadata, get_metadata_cached
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
            
            # Add "Continue this Thread" button
            if st.button("Continue this Thread", use_container_width=True):
                try:
                    # Get current bot ID from thread data and look up proper name
                    current_bot_id = first_bot
                    if not current_bot_id:
                        current_bot_id = "Eve"

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
                    st.session_state[f"messages_{new_thread_id}"] = []
                    st.session_state["show_new_chat_selector"] = False
                    st.session_state["active_chat_started"] = True

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
            return
        except Exception as e:
            st.error(f"Error retrieving thread data: {str(e)}")
            return
    elif file_path:
        # Ensure the file path is within the genesis/tmp directory for security
        base_path = "/Users/justin/Documents/Code/genesis/"
        full_path = os.path.join(base_path, file_path)
        
        if os.path.exists(full_path) and os.path.isfile(full_path):
            try:
                with open(full_path, 'r') as f:
                    content = f.read()
                
                # Strip ANSI codes from content
                cleaned_content = strip_ansi_codes(content)
                
                st.markdown(f"### File: {os.path.basename(file_path)}")
                st.code(cleaned_content)
            except Exception as e:
                st.error(f"Error reading file: {str(e)}")
        else:
            st.error("File not found or access denied.")
    else:
        st.error("No file specified.")
