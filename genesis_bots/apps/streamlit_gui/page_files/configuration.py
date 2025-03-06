import streamlit as st

def hide_sidebar():
    """Helper function to hide the sidebar"""
    st.markdown("""
        <style>
        [data-testid="stSidebar"][aria-expanded="true"]{
            display: none;
        }
        [data-testid="stSidebar"][aria-expanded="false"]{
            display: none;
        }
        </style> 
    """, unsafe_allow_html=True)

def configuration():
    # Set flag to hide chat elements in sidebar
    st.session_state['hide_chat_elements'] = True
    
    # Hide sidebar
    hide_sidebar()
    
    # Custom CSS for dark mode styling
    st.markdown("""
        <style>
        /* Page layout and spacing */
        .block-container {
            padding: 8rem 1rem 1rem !important;
            max-width: 46rem !important;
        }
        
        /* Subtitle styling */
        .subtitle {
            color: var(--text-color);
            font-size: 1.2em;
            margin-bottom: 1rem;
            opacity: 0.9;
            font-weight: 500;
        }
        
        /* Button styling */
        .stButton > button {
            width: 100%;
            text-align: left !important;
            justify-content: flex-start !important;
            color: var(--text-color) !important;
            background-color: var(--secondary-background-color) !important;
            border: 1px solid var(--primary-border-color) !important;
            padding: 0.6rem 1rem !important;
            margin: 0 0 0.3rem 0 !important;
            border-radius: 0.5rem !important;
            transition: all 0.2s ease;
            font-size: 1.1em !important;
            line-height: 1.2 !important;
            height: auto !important;
            font-weight: 400 !important;
            opacity: 1 !important;
            box-shadow: 0 1px 2px rgba(0,0,0,0.1) !important;
        }
        
        /* Light mode button styling */
        [data-theme="light"] .stButton > button {
            background-color: #f0f2f6 !important;
            border: 1px solid rgba(49, 51, 63, 0.2) !important;
        }
        
        /* Dark mode button styling */
        [data-theme="dark"] .stButton > button {
            background-color: rgba(255, 255, 255, 0.1) !important;
            border: 1px solid rgba(250, 250, 250, 0.2) !important;
        }
        
        .stButton > button:hover {
            transform: translateY(-1px);
            box-shadow: 0 4px 6px rgba(0,0,0,0.1) !important;
        }
        
        /* Light mode hover */
        [data-theme="light"] .stButton > button:hover {
            background-color: #e6e9ef !important;
            border-color: rgba(49, 51, 63, 0.3) !important;
        }
        
        /* Dark mode hover */
        [data-theme="dark"] .stButton > button:hover {
            background-color: rgba(255, 255, 255, 0.15) !important;
            border-color: rgba(250, 250, 250, 0.3) !important;
        }
        
        /* Back button styling */
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
        
        .back-button .stButton > button:hover {
            background-color: rgba(255, 75, 75, 0.1) !important;
            box-shadow: none !important;
            transform: none !important;
        }

        /* Button text alignment */
        button p {
            color: var(--text-color) !important;
            text-align: left !important;
            margin-left: 0 !important;
            padding-left: 0 !important;
        }
        </style>
    """, unsafe_allow_html=True)

    # Just the subtitle

    # Back button below subtitle
    st.markdown('<div class="back-button">', unsafe_allow_html=True)
 
    if st.button("← Back to Chat", key="back_to_chat", use_container_width=True):
        st.session_state["selected_page_id"] = "chat_page"
        st.session_state["radio"] = "Chat with Bots"
        st.session_state['hide_chat_elements'] = False  # Clear the flag when going back
        st.rerun()
    st.markdown('</div>', unsafe_allow_html=True)

    # Build configuration options list
    config_options = [
        ("llm_config", "LLM Model & Key"),
        ("setup_slack", "Setup Slack Connection"),
        ("bot_config", "Bot Configuration"),
   
    ]
    
    config_options.extend([
        ("db_harvester", "Harvester Status"),
        ("config_jira", "Setup Jira API Params"),
        # ("config_github", "Setup GitHub API Params"),
        ("config_web_access", "Setup WebAccess API Params"),
        ("config_g_sheets", "Setup Google Workspace API"),
    ])

    # Conditionally add options based on state
    if st.session_state.get("data_source") == "snowflake":
        config_options.append(("config_email", "Setup Email Integration"))

    if st.session_state.get("NativeMode"):
        config_options.extend([
            ("config_wh", "Setup Custom Warehouse"),
            ("config_custom_eai", "Setup Custom Endpoints"),
            ("config_cortex_search", "Setup Cortex Search"),
            ("grant_data", "Grant Data Access"),
            ("start_stop", "Server Stop-Start"),
            ("show_server_logs", "Server Logs"),
        ])
    

    # Display options
    for page_id, display_name in config_options:
        if st.button(display_name, key=f"config_{page_id}", use_container_width=True):
            st.session_state["selected_page_id"] = page_id
            st.session_state["radio"] = display_name
            st.rerun()

if __name__ == "__main__":
    configuration() 