import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
import streamlit as st
from utils import check_status, get_session, get_references, get_metadata, get_slack_tokens, get_slack_tokens_cached
import time
import base64
from streamlit import config
from collections import namedtuple
from textwrap import dedent
import uuid
import random

PageDesc = namedtuple('_PageEntry', ['page_id', 'display_name', 'module_name', 'entry_func_name'])

class Pages:
    """
    An internal helper structure serving as a poor man's page registry

    The 'all' attribute maps from page_id to its PageDesc entry
    """
    # Note: no validation checks (e.g. uniqueness) are done here
    def __init__(self):
        self.all = {} # maps page_id to a PageDesc object
        self._by_display = {} # seconday (hidden) index


    def add_page(self, *args, **kwargs):
        entry = PageDesc(*args, **kwargs)
       # print(f"Adding page: {entry}")
        assert entry.page_id not in self.all # prevent duplicates
        self.all[entry.page_id] = entry
        self._by_display[entry.display_name] = entry


    def lookup_pages(self, attr_name, attr_value):
        if attr_name == 'page_id':
            entry = self.all.get(attr_value)
            res = [entry] if entry else []
        elif attr_name == "display_name":
            entry = self._by_display.get(attr_value)
            res = [entry] if entry else []
        else:
            res = [x for x in self.all.values() if getattr(x, attr_name) == attr_value]
        return res


    def lookup_page(self, attr_name, attr_value):
        res = self.lookup_pages(attr_name, attr_value)
        if len(res) != 1:
            raise ValueError(f"Page with {attr_name}={attr_value} not found")
        return res[0]


    def get_module(self, page_id):
        desc = self.all[page_id]
        return getattr(__import__(f'page_files.{desc.module_name}'), desc.module_name)


    def dispatch_page(self, page_id):
        desc = self.all[page_id]
        func = getattr(self.get_module(page_id), desc.entry_func_name)
        func()


# Set minCachedMessageSize to 500 MB to disable forward message cache:
config.set_option("global.minCachedMessageSize", 500 * 1e6)


# Set Streamlit to wide mode
st.set_page_config(layout="wide")

st.session_state.app_name = "GENESIS_BOTS"
st.session_state.prefix = st.session_state.app_name + ".app1"
st.session_state.core_prefix = st.session_state.app_name + ".CORE"

if 'NativeMode' not in st.session_state:
    st.session_state.NativeMode = True

if 'data_source' not in st.session_state:
    st.session_state.data_source = "other"

if "wh_name" not in st.session_state:
    st.session_state["wh_name"] = "XSMALL" # TODO fix warehouse name

# Initialize uploader key if it doesn't exist
if 'uploader_key' not in st.session_state:
    st.session_state['uploader_key'] = random.randint(0, 1 << 32)

# Main content of the app
def is_running_from_package():
    """Check if we're running from an installed package"""
    try:
        from importlib.metadata import version
        version('genesis_bots')
        return True
    except ImportError:  # For Python < 3.8
        try:
            from importlib.metadata import distribution
            distribution('genesis_bots')
            return True
        except Exception:
            return False
    except Exception:  # catches PackageNotFoundError from importlib.metadata
        return False

def render_image(filepath: str, width = None):
    """
    filepath: path to the image. Must have a valid file extension.
    Handles both package and direct execution paths.
    """
    try:
        image_path = None

        # List of possible paths to try
        paths_to_try = [
            # Direct path for development
            filepath,
            os.path.join("genesis_bots/apps/streamlit_gui", filepath),
            os.path.join("./genesis_bots/apps/streamlit_gui", filepath),
            # Path relative to current file
            os.path.join(os.path.dirname(os.path.abspath(__file__)), filepath),
        ]

        # Try package resources if we're running from package
        try:
            from importlib import resources
            with resources.path('apps.streamlit_gui', filepath) as path:
                paths_to_try.append(str(path))
        except Exception:
            pass

        # Try each path until we find one that exists
        for path in paths_to_try:
            if os.path.exists(path):
                image_path = path
                break

        if not image_path:
            return

        mime_type = filepath.split('.')[-1:][0].lower()
        with open(image_path, "rb") as f:
            content_bytes = f.read()
            content_b64encoded = base64.b64encode(content_bytes).decode()
            image_string = f'data:image/{mime_type};base64,{content_b64encoded}'
            st.sidebar.image(image_string, width=width)
    except Exception as e:
        # Silently handle any errors without showing warnings
        pass

a = """
    <style>
    .block-container {
        padding-top: 2rem;
        padding-bottom: 0rem;
        padding-left: 2rem;
        padding-right: 2rem;
    }
    .stSidebar > div:first-child {
        padding-top: 0rem;
    }
    .stSidebar .block-container {
        padding-top: 2rem;
        padding-left: 0.5rem;
        padding-right: 0.5rem;
    }
    .stTextInput > div > div > input
        padding-top: 0.25rem;
        padding-bottom: 0.25rem;
    }
    .stButton > button {
        padding-top: 1rem;
        padding-bottom: 0.0rem;
    }
    </style>
"""
# st.markdown(a, unsafe_allow_html=True)

# Initialize data in session state if it doesn't exist
if 'data' not in st.session_state:
    st.session_state.data = None

# ... (keep the initialization code)

# st.success('NativeMode1 '+str(st.session_state.NativeMode))
session = None
if st.session_state.NativeMode:
    try:
    #    st.success('NativeMode2a')
        service_status_result = check_status()
     #   st.success('NativeMode2b '+str(service_status_result))
        if service_status_result is None:
            st.session_state["data"] = "Local Mode"
            st.session_state.NativeMode = False
        else:
            st.session_state["data"] = service_status_result
            session = get_session()
    except Exception as e:
        st.session_state["data"] = None
else:
    st.session_state["data"] = "Local Mode"


if 'show_log_config' not in st.session_state:
    check_status_result = False
    if st.session_state.NativeMode:
        check_status_result = get_metadata('logging_status')

    if check_status_result == False:
        st.session_state.show_log_config = True
        if st.session_state.NativeMode:
            import snowflake.permissions as permissions
            permissions.request_event_sharing()
    else:
        st.session_state.show_log_config = False

# Initialize session state for the modal
if "show_modal" not in st.session_state:
    st.session_state.show_modal = True  # Default to showing the modal

# check for configured email
if 'show_email_config' not in st.session_state:
    st.session_state.show_email_config = False
    email_info = get_metadata("get_email")
    if len(email_info) > 0:
        if 'Success' in email_info and email_info['Success']==False:
            st.session_state.show_email_config = True

# check for openai llm token
if 'show_openai_config' not in st.session_state:
    st.session_state.show_openai_config = False
    llm_info = get_metadata("llm_info")
    openai_set = False
    if len(llm_info) > 0:
        # Check if openai exists
        openai_set = [True for llm in llm_info if llm["llm_type"].lower() == 'openai']
        openai_set = openai_set[0] if openai_set else False
    if openai_set == False:
        st.session_state.show_openai_config = True

# check for slack token
if 'show_slack_config' not in st.session_state:
    st.session_state.show_slack_config = False
    tokens = get_slack_tokens()
    get_slack_tokens_cached.clear()
    slack_active = tokens.get("SlackActiveFlag", False)
    if slack_active == False:
        st.session_state.show_slack_config = True

def hide_modal():
    st.session_state.show_modal = False

# Define the modal logic
def show_modal():
    with st.expander("Enable Cool Genesis features:", expanded=True):

        if st.session_state.show_email_config == True and  st.session_state.NativeMode:
            if st.button(" 📧 Let your Genbots Email you", key="modal_email_btn"):
                st.session_state["radio"] = "Setup Email Integration"

        if st.session_state.show_slack_config == True:
            if st.button(" 💬 Connect your bots to Slack", key="modal_slack_btn"):
                st.session_state["radio"] = "Setup Slack Connection"

        if st.session_state.show_openai_config == True:
            if st.button(" 🧠 Enable OpenAI LLM with your Key", key="modal_openai_btn"):
                st.session_state["radio"] = "LLM Model & Key"

        if st.checkbox("Ignore this message for the rest of the session", key="modal_ignore_checkbox"):
            hide_modal()
            st.rerun()


if st.session_state.NativeMode:
    try:
        # status_query = f"select v.value:status::varchar status from (select parse_json(system$get_service_status('{prefix}.GENESISAPP_SERVICE_SERVICE'))) t, lateral flatten(input => t.$1) v"
        # service_status_result = session.sql(status_query).collect()
        service_status_result = check_status()

    #    st.success('NativeMode3 '+str(service_status_result))
       # st.success('NativeMode3 '+str(service_status_result))
        if service_status_result != "READY":
        #    st.success('NativeMode4 '+str(service_status_result))
            with st.spinner("Waiting on Genesis Services to start..."):
                service_status = st.empty()
                while True:
                    service_status.text(
                        "Genesis Service status: " + service_status_result
                    )
                    if service_status_result == "SUSPENDED":
                        # show button to start service
                        if st.button("Click to start Genesis Service"):
                            with st.spinner("Genesis Services is starting..."):
                                try:
                                    # Execute the command and collect the results
                                    time.sleep(15)
                                    service_start_result = session.sql(
                                        f"call {st.session_state.app_name}.core.start_app_instance('APP1','GENESIS_POOL','{st.session_state.wh_name}')"
                                    ).collect()
                                    if service_start_result:
                                        service_status.text(
                                            "Genesis Service status: " + service_status_result
                                        )
                                    else:
                                        time.sleep(10)
                                except Exception as e:
                                    st.error(f"Error connecting to Snowflake: {e}")
                    service_status_result = check_status()
                    service_status.text(
                        "Genesis Service status: " + service_status_result
                    )
                    if service_status_result == "READY":
                        service_status.text("")
                        st.rerun()

                    time.sleep(10)

       # sql = f"select {prefix}.list_available_bots() "
      #  st.session_state["data"] = session.sql(sql).collect()

    except Exception as e:
        st.session_state["data"] = None
else:
    st.session_state["data"] = "Local Mode"

if "data" in st.session_state:
    data = st.session_state["data"]

if "last_response" not in st.session_state:
    st.session_state["last_response"] = ""

# if st.session_state.show_email_config == False and st.session_state.show_openai_config == False and st.session_state.show_slack_config == False:
#     hide_modal()
# elif st.session_state.show_modal:
#     # Show modal if the session state allows
#     show_modal()

# st.success(st.session_state.data)
if st.session_state.data:
    pages = Pages()

    # Check if Snowflake metadata or not
    metadata_response = get_metadata('check_db_source')
    st.session_state.data_source = "other"
    if metadata_response == True:
        st.session_state.data_source = "snowflake"

    pages.add_page('chat_page', 'Chat with Bots', 'chat_page', 'chat_page')
    pages.add_page('configuration', 'Configuration', 'configuration', 'configuration')
    pages.add_page('llm_config', 'LLM Model & Key', 'llm_config', 'llm_config')
    if st.session_state.data_source == "snowflake": pages.add_page('config_email', 'Setup Email Integration', 'config_email', 'setup_email')
    pages.add_page('setup_slack', 'Setup Slack Connection', 'setup_slack', 'setup_slack')
    if st.session_state.NativeMode: pages.add_page('config_wh', 'Setup Custom Warehouse', 'config_wh', 'config_wh')
    pages.add_page('grant_data', 'Grant Data Access', 'grant_data', 'grant_data')
    if st.session_state.NativeMode: pages.add_page('config_custom_eai', 'Setup Custom Endpoints', 'config_custom_eai', 'config_custom_eai')
    pages.add_page('config_jira', 'Setup Jira API Params', 'config_jira', 'config_jira')
    # pages.add_page('config_github', 'Setup GitHub API Params', 'config_github', 'config_github')
    pages.add_page('config_web_access', 'Setup WebAccess API Params', 'config_web_access', 'config_web_access')
    pages.add_page('config_g_sheets','Setup Google Workspace API','config_g_sheets','config_g_sheets')
    pages.add_page('db_harvester', 'Harvester Status', 'db_harvester', 'db_harvester')
    pages.add_page('bot_config', 'Bot Configuration', 'bot_config', 'bot_config')
    if st.session_state.NativeMode: pages.add_page('config_cortex_search', 'Setup Cortex Search', 'config_cortex_search', 'setup_cortex_search')
    if st.session_state.NativeMode: pages.add_page('start_stop', 'Server Stop-Start', 'start_stop', 'start_stop')
    if st.session_state.NativeMode: pages.add_page('show_server_logs', 'Server Logs', 'show_server_logs', 'show_server_logs')
    pages.add_page('support', 'Support and Community', 'support', 'support')
    pages.add_page('db_connections', 'Database Connections', 'db_connections', 'db_connections')
    pages.add_page('bot_projects', 'Bot Projects', 'bot_projects', 'bot_projects')
    pages.add_page('projects_dashboard','Projects Dashboard','projects_dashboard','projects_dashboard')

    #    st.sidebar.subheader("**Genesis App**")

    # Check the current theme
    current_theme = st.get_option("theme.base")

    # Choose the image based on the theme
    if current_theme == "dark":
        image_name = "Genesis-Computing-Logo-White.png"
    else:
        image_name = "Genesis-Computing-Logo-Black.png"

    # Get NativeMode from session state
    native_mode = st.session_state.get("NativeMode", False)
    if native_mode:
        render_image(image_name, width=250)
    else:
        if is_running_from_package():
            from importlib import resources
            try:
                # Don't use context manager with Path object
                image_path = resources.files('apps.streamlit_gui').joinpath(image_name)
                st.sidebar.image(str(image_path), width=250)
            except Exception:
                # Fallback for older Python versions
                with resources.path('apps.streamlit_gui', image_name) as image_path:
                    st.sidebar.image(str(image_path), width=250)
        else:
            # Direct development path
            st.sidebar.image(f"./genesis_bots/apps/streamlit_gui/{image_name}", width=250)

    # Set the default selection page
    selected_page_id = None

    # set the default chat bot id for chat_page
    initial_bot_name = None

    # Handle URL params which are used, for example, to drop user into a specific page or chat session.
    # We expect a param named 'action' followed by action-specific params.
    # This logic will be triggered only once (since we pop the params from the URL)
    url_params = st.query_params.to_dict()
    if url_params:
        action = url_params.pop('action', None)
        if action == "show_artifact_context":
            bot_name = url_params.pop('bot_name', None)
            artifact_id = url_params.pop('artifact_id', None)
            if bot_name and artifact_id:
                # Force the selected page to chat_page and inject the initial bot_name and initial prompt so it gets picked up on the next chat_page load
                selected_page_id = 'chat_page'
                initial_bot_name = bot_name
                module = pages.get_module(selected_page_id)
                module.set_initial_chat_sesssion_data(
                    bot_name=bot_name,
                    initial_message="Fetching information about your request...",
                    initial_prompt=dedent(f'''
                        1.Briefly state your name, followed by 'let me help you explore an item previously generated by me...'.
                        2.Fetch metadata for {artifact_id=}.
                        3.Using the artifact's metadata, describe its original purpose and the time it was generated.
                        Refer to this artifact as the 'item'. DO NOT mention the artifact ID unless requested explicitly, as it is mosly used for internal references.
                        4. Render the artifact's content by using its markdown notation and offer to help further explore this item.
                        5. If the metadata indicates that this artifact contains other artifact, offer the user to explore the contained artifact.
                        ''')
                )
            else:
                # TODO: handle missing  params
                pass
        else:
            pass # silently ignore unrecognized requests
        st.query_params.clear() # Always clear the URL once we inspected it. This will clear the user's browser URL.

    if selected_page_id is None:
        # If not forced by the URL, use the selection saved in session state; default selection to "Chat with Bots"
        saved_selection = st.session_state.get("radio") # We save the Display name, not the id. TODO: refactor to use page ID (safer, stable, cleaner)
        if saved_selection:
            selected_page_id = pages.lookup_page("display_name", saved_selection).page_id # if it raises we have an internal logic error
        else:
            selected_page_id = "chat_page" if "chat_page" in pages.all else list(pages.all.keys())[0]
    assert selected_page_id is not None

    # --- NEW: Initialize default chat session for chat_page if not already initialized ---
    if selected_page_id == "chat_page" and ('current_bot' not in st.session_state or 'current_thread_id' not in st.session_state):
        try:
            from utils import get_bot_details
            bot_details = get_bot_details()
            if isinstance(bot_details, list) and bot_details:
                # Sort bot details to choose the first based on your criteria (e.g. "Eve" appears first if present)
                bot_details.sort(key=lambda bot: (not "Eve" in bot["bot_name"], bot["bot_name"]))
                bot_names = [bot["bot_name"] for bot in bot_details]
                if initial_bot_name and initial_bot_name not in bot_names:
                    # if we already have an initial bot name (see above) but it's no longer a valid one (e.g. bot was deleted) then fallback to the default
                    initial_bot_name = None
                if not initial_bot_name:
                    # default to choose the first one in the list
                    initial_bot_name = bot_names[0]
            else:
                initial_bot_name = "ChatBot"
        except Exception:
            initial_bot_name = "ChatBot"
        import uuid
        # start a new thread_id and settion for the initial bot and update the state
        new_thread_id = str(uuid.uuid4())
        st.session_state.current_bot = initial_bot_name
        st.session_state.current_thread_id = new_thread_id
        new_session = f"🤖 {st.session_state.current_bot} ({new_thread_id[:8]})"
        st.session_state.current_session = new_session
        if "active_sessions" not in st.session_state:
            st.session_state.active_sessions = []
        if new_session not in st.session_state.active_sessions:
            st.session_state.active_sessions.append(new_session)
        st.session_state.active_chat_started = True
        st.session_state[f"messages_{new_thread_id}"] = []
    # --- END NEW ---

    # Create a vertical navigation bar in the sidebar with grouping:
    with st.sidebar:
        # Minimal CSS to left-align sidebar buttons (using default styling otherwise)
        st.markdown(
            """
            <style>
            div.stButton > button {
                text-align: left !important;
                width: 100% !important;
                justify-content: flex-start !important;
                display: flex !important;
                align-items: center !important;
                padding: 0.3em !important;
            }
            /* Target Streamlit's inner button elements */
            div.stButton > button > div {
                display: flex !important;
                justify-content: flex-start !important;
                width: 100% !important;
                text-align: left !important;
                margin: 0 !important;
            }
            /* Target the actual text container */
            div.stButton > button > div > p {
                text-align: left !important;
                margin-left: 0 !important;
                padding-left: 0 !important;
                margin: 0 !important;
            }
            /* Reduce spacing around horizontal rules */
            hr {
                margin: 0.2em 0 !important;
            }
            /* Adjust spacing for Upload File button and container */
            [data-testid="stButton"] {
                margin: 0.1em 0 !important;
            }
            .element-container {
                margin: 0.1em 0 !important;
                padding: 0 !important;
            }
            /* Remove extra padding from expander */
            .streamlit-expanderHeader {
                padding: 0.2em !important;
            }
            /* Reduce spacing in headings */
            .sidebar .markdown-text-container {
                margin: 0 !important;
                padding: 0.1em 0 !important;
            }
            /* Adjust info message spacing */
            .stAlert {
                padding: 0.2em !important;
                margin: 0.1em 0 !important;
            }
            /* Reduce spacing in columns */
            [data-testid="column"] {
                padding: 0 !important;
                margin: 0 !important;
            }
            /* Reduce spacing in stMarkdown */
            .stMarkdown {
                margin: 0 !important;
                padding: 0 !important;
            }
            /* Adjust heading margins */
            h4 {
                margin: 0.2em 0 !important;
                padding: 0 !important;
            }
            </style>
            """,
            unsafe_allow_html=True,
        )

        # Show chat elements only if not hidden
        if not st.session_state.get('hide_chat_elements', False):
            # Show New Chat button first with custom styling (light orange background)
            if "chat_page" in pages.all:
                page = pages.all["chat_page"]
                st.markdown("""
                    <style>
                    div[data-testid="stHorizontalBlock"] button[kind="primary"],
                    .new-chat-btn-container button[kind="primary"] {
                        background-color: #E67300 !important;
                        border-color: #FFA500 !important;
                        width: 100% !important;
                    }

                    /* Target the button's inner structure */
                    .new-chat-btn-container button[kind="primary"] > div {
                        display: flex !important;
                        justify-content: center !important;
                        width: 100% !important;
                    }

                    .new-chat-btn-container button[kind="primary"] > div > p {
                        text-align: center !important;
                        width: 100% !important;
                        margin: 0 !important;
                    }

                    div[data-testid="stHorizontalBlock"] button[kind="primary"]:hover,
                    .new-chat-btn-container button[kind="primary"]:hover {
                        background-color: #E67300 !important;
                        border-color: #FF8C00 !important;
                    }

                    /* Override any Streamlit default primary button styles */
                    button[kind="primary"] {
                        background-color: #E67300 !important;
                        border-color: #FFA500 !important;
                        width: 100% !important;
                    }

                    button[kind="primary"] > div {
                        display: flex !important;
                        justify-content: center !important;
                        width: 100% !important;
                    }

                    button[kind="primary"] > div > p {
                        text-align: center !important;
                        width: 100% !important;
                        margin: 0 !important;
                    }

                    button[kind="primary"]:hover {
                        background-color: #E67300 !important;
                        border-color: #FF8C00 !important;
                    }
                    </style>
                """, unsafe_allow_html=True)
                st.markdown('<div class="new-chat-btn-container">', unsafe_allow_html=True)
                if st.button(" ⚡  New Chat", type="primary", key="new_chat_main"):
                    st.session_state["show_new_chat_selector"] = True
                    st.cache_data.clear()
                    st.rerun()
                st.markdown('</div>', unsafe_allow_html=True)

            if st.session_state.get("show_new_chat_selector", False):
                with st.expander("Select a bot to chat with:", expanded=True):
                    try:
                        from utils import get_bot_details
                        bot_details = get_bot_details()
                        bot_details.sort(key=lambda bot: (not "Eve" in bot["bot_name"], bot["bot_name"]))
                        available_bots = [bot["bot_name"] for bot in bot_details]
                    except Exception as e:
                        available_bots = []
                    selected_bot = st.selectbox("Select Bot", available_bots, key="new_chat_select")
                    col1, col2 = st.columns([2, 1])
                    with col1:
                        if st.button("Start Chat", type="primary", key="start_new_chat"):
                            new_thread_id = str(uuid.uuid4())
                            new_session = f"🤖 {selected_bot} ({new_thread_id[:8]})"
                            # Initialize active_sessions if needed
                            if 'active_sessions' not in st.session_state:
                                st.session_state.active_sessions = []
                            # Always add the new session
                            st.session_state.active_sessions = [new_session] + [
                                s for s in st.session_state.active_sessions
                                if s != new_session
                            ]
                            st.session_state["current_thread_id"] = new_thread_id
                            st.session_state["current_bot"] = selected_bot
                            st.session_state["current_session"] = new_session
                            st.session_state[f"messages_{new_thread_id}"] = []
                            st.session_state["show_new_chat_selector"] = False
                            st.session_state["active_chat_started"] = True  # Flag to indicate active chat
                            st.rerun()
                    with col2:
                        if st.button("⨂", key="cancel_new_chat"):
                            st.session_state["show_new_chat_selector"] = False
                            st.rerun()

            # Show active chat sessions
            st.markdown("#### Active Chat Sessions:")
            if 'active_sessions' not in st.session_state:
                st.session_state.active_sessions = []

            # Show active sessions if we have any or if a chat has been started
            if st.session_state.active_sessions or st.session_state.get("active_chat_started"):
                st.markdown(
                    """
                    <style>
                    .element-container:has(style){
                        display: none;
                    }
                    #button-after {
                        display: none;
                    }
                    .element-container:has(#button-after) {
                        display: none;
                    }
                    .element-container:has(#button-after) + div button {
                        background: #fff;
                        border: 1px solid #ccc;
                        padding: 0.5em 1em;
                        font: inherit;
                        cursor: pointer;
                        outline: inherit;
                        color: inherit;
                        text-align: left;
                        margin: 5px 0;
                        font-weight: normal;
                        font-size: 0.8em;
                    }
                    </style>
                    """,
                    unsafe_allow_html=True,
                )

                # Add global CSS for session buttons at the top of the sidebar
                st.markdown(
                    """
                    <style>
                    .session-button {
                        width: 100%;
                        padding: 0.5rem;
                        margin: 0.25rem 0;
                        border: 1px solid #cccccc;
                        background-color: transparent;
                        cursor: pointer;
                        text-align: left;
                        border-radius: 4px;
                    }
                    .session-button:hover {
                        background-color: #f0f0f0;
                        border: 1px solid #cccccc;
                    }
                    .session-button-active {
                        background-color: #E67300 !important;
                        border: 1px solid #E67300 !important;
                    }
                    .session-button-active:hover {
                        background-color: #E67300 !important;
                        border: 1px solid #E67300 !important;
                    }
                    </style>
                    """,
                    unsafe_allow_html=True
                )

                # Add a session counter to session state if it doesn't exist
                if 'session_numbers' not in st.session_state:
                    st.session_state.session_numbers = {}

                # Number sessions that don't have a number yet
                for session in st.session_state.active_sessions:
                    if session not in st.session_state.session_numbers:
                        next_num = len(st.session_state.session_numbers) + 1
                        st.session_state.session_numbers[session] = next_num

                # Sort sessions by their number
                sorted_sessions = sorted(
                    st.session_state.active_sessions,
                    key=lambda x: st.session_state.session_numbers.get(x, float('inf'))
                )

                for session in sorted_sessions:
                    bot_name, thread_id = session.split(' (')
                    bot_name = bot_name.split('🤖 ')[1]
                    thread_id = thread_id[:-1]  # Remove the closing parenthesis
                    full_thread_id = next((key.split('_')[1] for key in st.session_state.keys() if key.startswith(f"messages_{thread_id}")), thread_id)

                    # Get the session number
                    session_num = st.session_state.session_numbers[session]

                    # Check if this is the current session
                    is_current = session == st.session_state.get('current_session', '')

                    # Create columns
                    col1, col2 = st.columns([4, 1])
                    with col1:
                        if st.button(f"⚡ {session_num}: {bot_name}",
                                   key=f"session_btn_{full_thread_id}",
                                   use_container_width=True,
                                   type="secondary" if not is_current else "primary"):
                            st.session_state.current_bot = bot_name
                            st.session_state.selected_session = {
                                'bot_name': bot_name,
                                'thread_id': full_thread_id
                            }
                            st.session_state.current_session = session
                            st.session_state.current_thread_id = full_thread_id
                            st.session_state.load_history = True
                            st.rerun()
                    with col2:
                        if st.button("⨂", type="secondary", key=f"remove_btn_{full_thread_id}"):
                            # Remove the session number when removing the session
                            if session in st.session_state.session_numbers:
                                del st.session_state.session_numbers[session]
                            st.session_state.active_sessions.remove(session)
                            if f"messages_{full_thread_id}" in st.session_state:
                                del st.session_state[f"messages_{full_thread_id}"]
                            if st.session_state.get('current_session') == session:
                                st.session_state.pop('current_session', None)
                            st.rerun()
            else:
                st.info("No active chat sessions.")

            # Add divider after active sessions
            st.markdown("---")

            # Add the file uploader section
            with st.expander("Upload File", expanded=False):
                uploaded_file = st.file_uploader("FILE UPLOADER", key=st.session_state['uploader_key'])
                st.session_state["uploaded_file_main"] = uploaded_file

        # Always show configuration and support buttons
        st.markdown("---")  # Add a visual separator
        desired_sidebar = ["configuration", "db_connections", "bot_projects", "support", "projects_dashboard"]
        for key in desired_sidebar:
            if key in pages.all:
                page = pages.all[key]
                # Determine selected page (default to chat_page if nothing selected)
                is_selected = (selected_page_id == key) or (selected_page_id is None and key == "chat_page")
                if not is_selected:
                    if st.button(page.display_name, key=f"nav_bottom_{key}", use_container_width=True,
                                help=f"Navigate to {page.display_name}", type="secondary"):
                        st.session_state["selected_page_id"] = key
                        st.session_state["radio"] = page.display_name
                        st.session_state["previous_selection"] = page.display_name
                        st.rerun()

    try:
        # Use page_id directly instead of looking up by display name
        pages.dispatch_page(selected_page_id)
    except ValueError as e:
        st.error(f"Error loading page: {e}")

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
    try:
        selection = st.sidebar.radio(
            "Go to:",
            list(pages.keys()),
            index=list(pages.keys()).index(
                st.session_state.get("radio", list(pages.keys())[0])
            ),
        )
        if selection in pages:
            pages[selection]()
    except Exception as e:
        st.error(f"Error accessing page {st.session_state.get('radio')}: {e}")
        st.rerun()
