import streamlit as st
from utils import NativeMode, check_status, get_session, app_name, prefix

# Set Streamlit to wide mode
st.set_page_config(layout="wide")


# Initialize data in session state if it doesn't exist
if 'data' not in st.session_state:
    st.session_state.data = None

# ... (keep the initialization code)

if NativeMode:
    try:
        service_status_result = check_status()
        if service_status_result is None:
            st.session_state["data"] = "Local Mode"
        # ... (keep the service status checking code)
    except Exception as e:
        st.session_state["data"] = None
else:
    st.session_state["data"] = "Local Mode"

if st.session_state.data:
    pages = {
        "Chat with Bots": lambda: __import__('page_files.chat_page').chat_page.chat_page(),
        "LLM Model & Key": lambda: __import__('page_files.llm_config').llm_config.llm_config(),
        "Setup Slack Connection": lambda: __import__('page_files.setup_slack').setup_slack.setup_slack(),
        "Grant Data Access": lambda: __import__('page_files.grant_data').grant_data.grant_data(),
        "Harvester Status": lambda: __import__('page_files.db_harvester').db_harvester.db_harvester(),
        "Bot Configuration": lambda: __import__('page_files.bot_config').bot_config.bot_config(),
        "Server Stop/Start": lambda: __import__('page_files.start_stop').start_stop.start_stop(),
        "Server Logs": lambda: __import__('page_files.show_server_logs').show_server_logs.show_server_logs(),
        "Support and Community": lambda: __import__('page_files.support').support.support(),
    }

    if st.session_state.get("needs_keys", False):
        del pages["Chat with Bots"]

    st.sidebar.title("Genesis Bots Configuration")
    
    # Set the default selection to "Chat with Bots"
    default_selection = "Chat with Bots" if "Chat with Bots" in pages else list(pages.keys())[0]
    
    # Use a dropdown for page selection
    selection = st.sidebar.selectbox(
        "Select Page:",
        list(pages.keys()),
        index=list(pages.keys()).index(
            st.session_state.get("radio", default_selection)
        ),
    )
    
    # Update the session state with the current selection
    st.session_state["radio"] = selection

    # Add placeholder for active chat sessions when on "Chat with Bots" page
    if selection == "Chat with Bots":
        st.sidebar.markdown("### Active Chat Sessions")
        st.sidebar.info("Active chat sessions will be displayed here.")

    if selection in pages:
        pages[selection]()

else:
    pages = {
        "Welcome!": lambda: __import__('page_files.welcome').welcome.welcome(),
        "1: Configure Warehouse": lambda: __import__('page_files.config_wh').config_wh.config_wh(),
        "2: Configure Compute Pool": lambda: __import__('page_files.config_pool').config_pool.config_pool(),
        "3: Configure EAI": lambda: __import__('page_files.config_eai').config_eai.config_eai(),
        "4: Start Genesis Server": lambda: __import__('page_files.start_service').start_service.start_service(),
        "Support and Community": lambda: __import__('page_files.support').support.support(),
    }

    st.sidebar.title("Genesis Bots Installation")
    selection = st.sidebar.radio(
        "Go to:",
        list(pages.keys()),
        index=list(pages.keys()).index(
            st.session_state.get("radio", list(pages.keys())[0])
        ),
    )
    if selection in pages:
        pages[selection]()