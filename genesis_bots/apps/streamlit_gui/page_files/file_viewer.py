import streamlit as st
import os
import re

def strip_ansi_codes(text):
    """Remove ANSI escape codes from text"""
    ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
    return ansi_escape.sub('', text)

def file_viewer():
    # Back button styling
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
        </style>
    """, unsafe_allow_html=True)

    st.markdown('<div class="back-button">', unsafe_allow_html=True)
    if st.button("← Back to Projects", key="back_from_file_viewer"):
        # Restore previous state
        st.session_state["selected_page_id"] = "bot_projects"
        st.session_state["radio"] = "Bot Projects"
        st.session_state['hide_chat_elements'] = False
        
        # Restore bot project state
        if "previous_bot" in st.session_state:
            st.session_state.current_bot = st.session_state["previous_bot"]
        if "previous_project" in st.session_state:
            st.session_state.selected_project = st.session_state["previous_project"]
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
            for msg_type, msg_content in messages:
                if msg_type == "User Prompt":
                    with st.chat_message("user"):
                        st.markdown(msg_content)
                elif msg_type == "Assistant Response":
                    with st.chat_message("assistant", avatar=bot_avatar_image_url):
                        st.markdown(msg_content)
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
