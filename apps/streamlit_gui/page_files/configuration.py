import streamlit as st

def configuration():
    # Custom CSS for dark mode styling
    st.markdown("""
        <style>
        /* Main title styling */
        h1 {
            color: var(--text-color, #FFFFFF) !important;
            margin-bottom: 1rem;
            font-size: 2.2em !important;
        }
        
        /* Description text styling */
        .description {
            color: var(--text-color, #FFFFFF);
            font-size: 1.1em;
            margin-bottom: 1.5rem;
            opacity: 0.9;
        }
        
        /* Button styling */
        .stButton > button {
            width: 100%;
            text-align: left !important;
            justify-content: flex-start !important;
            color: var(--text-color, #FFFFFF) !important;
            background-color: var(--secondary-background-color, #2E3440) !important;
            border: 1px solid var(--border-color, rgba(255, 255, 255, 0.2)) !important;
            padding: 0.75rem 1rem !important;
            margin: 0 0 0.5rem 0 !important;
            border-radius: 0.3rem !important;
            transition: all 0.2s ease;
            font-size: 1.1em !important;
            line-height: 1.2 !important;
            height: auto !important;
            font-weight: 400 !important;
            opacity: 1 !important;
        }
        
        .stButton > button:hover {
            background-color: var(--hover-color, #3B4252) !important;
            border-color: var(--border-color-hover, rgba(255, 255, 255, 0.4)) !important;
            transform: translateY(-1px);
        }
        
        /* Back button specific styling */
        .back-button .stButton > button {
            text-align: left !important;
            justify-content: flex-start !important;
            background-color: transparent !important;
            border: 2px solid #FF4B4B !important;
            color: #FF4B4B !important;
            margin-top: 1.5rem !important;
            font-weight: 600 !important;
        }
        
        .back-button .stButton > button:hover {
            background-color: rgba(255, 75, 75, 0.1) !important;
        }

        /* Remove extra padding from button container */
        .stButton {
            margin: 0 !important;
            padding: 0 !important;
        }

        /* Container styling */
        .main-container {
            padding: 0;
            max-width: 600px;
        }

        /* Remove default streamlit padding */
        .block-container {
            padding-top: 1rem !important;
            padding-bottom: 0 !important;
        }

        /* Ensure text is always properly colored and left-aligned */
        button p {
            color: var(--text-color, #FFFFFF) !important;
            text-align: left !important;
            margin-left: 0 !important;
            padding-left: 0 !important;
        }
        </style>
    """, unsafe_allow_html=True)

    st.title("Configuration Options")
    st.markdown('<p class="description">Select one of the configuration options below:</p>', unsafe_allow_html=True)

    # Build a list of configuration options
    config_options = [
        ("llm_config", "LLM Model & Key"),
    ]
    if st.session_state.get("data_source") == "snowflake":
        config_options.append(("config_email", "Setup Email Integration"))
    config_options.append(("setup_slack", "Setup Slack Connection"))
    if st.session_state.get("NativeMode"):
        config_options.append(("config_wh", "Setup Custom Warehouse"))
    config_options.append(("grant_data", "Grant Data Access"))
    if st.session_state.get("NativeMode"):
        config_options.append(("config_custom_eai", "Setup Custom Endpoints"))
    config_options.extend([
        ("config_jira", "Setup Jira API Params"),
        ("config_web_access", "Setup WebAccess API Params"),
        ("config_g_sheets", "Setup Google Workspace API"),
        ("db_harvester", "Harvester Status"),
        ("bot_config", "Bot Configuration"),
    ])
    if st.session_state.get("NativeMode"):
        config_options.append(("config_cortex_search", "Setup Cortex Search"))
        config_options.append(("start_stop", "Server Stop-Start"))
        config_options.append(("show_server_logs", "Server Logs"))

    # Create a container with max width
    container = st.container()
    with container:
        # Display each option as a button
        for page_id, display_name in config_options:
            if st.button(display_name, key=f"config_{page_id}", use_container_width=True):
                st.session_state["selected_page_id"] = page_id
                st.session_state["radio"] = display_name
                st.rerun()

        # Back button
        st.markdown('<div class="back-button">', unsafe_allow_html=True)
        if st.button("← Back to Chat", key="back_to_chat", use_container_width=True):
            st.session_state["selected_page_id"] = "chat_page"
            st.session_state["radio"] = "Chat with Bots"
            st.rerun()
        st.markdown('</div>', unsafe_allow_html=True)

if __name__ == "__main__":
    configuration() 