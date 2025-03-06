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
    
    if file_path:
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
