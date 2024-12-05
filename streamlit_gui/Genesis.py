import streamlit as st
from utils import check_status, get_session, get_references, get_metadata, get_slack_tokens, get_slack_tokens_cached
import time
import base64
from streamlit import config
from collections import namedtuple
from textwrap import dedent

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

if "wh_name" not in st.session_state:
    st.session_state["wh_name"] = "XSMALL" # TODO fix warehouse name

# Main content of the app

def render_image(filepath: str, width = None):
   """
   filepath: path to the image. Must have a valid file extension.
   """
   mime_type = filepath.split('.')[-1:][0].lower()
   with open(filepath, "rb") as f:
    content_bytes = f.read()
    content_b64encoded = base64.b64encode(content_bytes).decode()
    image_string = f'data:image/{mime_type};base64,{content_b64encoded}'
    st.sidebar.image(image_string, width=width)

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
#st.markdown(a, unsafe_allow_html=True)

# Initialize data in session state if it doesn't exist
if 'data' not in st.session_state:
    st.session_state.data = None

# ... (keep the initialization code)

#st.success('NativeMode1 '+str(st.session_state.NativeMode))
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
                background: none;
                border: none;
                padding: 0;
                font: inherit;
                cursor: pointer;
                outline: inherit;
                color: inherit;
                text-align: left;
                margin: 0;
                font-weight: normal;
                font-size: 0.7em;
            }
            button:hover {
                color: #FFB3B3 !important;
            }
            .element-container:has(#button-after) + div button {
                line-height: 0.4 !important;
                margin-top: -30px !important;
                margin-bottom: 0px !important;
            }
            </style>

            """,
            unsafe_allow_html=True,
        )


        if st.session_state.show_email_config == True:
            st.markdown('<span id="button-after"></span>', unsafe_allow_html=True)
            if st.button(" 📧 Let your Genbots Email you"):
                st.session_state["radio"] = "Setup Email Integration"

        if st.session_state.show_slack_config == True:
            st.markdown('<span id="button-after"></span>', unsafe_allow_html=True)
            if st.button(" 💬 Connect your bots to Slack"):
                st.session_state["radio"] = "Setup Slack Connection"

        if st.session_state.show_openai_config == True:
            st.markdown('<span id="button-after"></span>', unsafe_allow_html=True)
            if st.button(" 🧠 Enable OpenAI LLM with your Key"):
                st.session_state["radio"] = "LLM Model & Key"

        if st.checkbox("Ignore this message for the rest of the session"):
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
                        st.experimental_rerun()

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

if st.session_state.show_email_config == False and st.session_state.show_openai_config == False and st.session_state.show_slack_config == False:
    hide_modal()
elif st.session_state.show_modal:
    # Show modal if the session state allows
    show_modal()

# st.success(st.session_state.data)
if st.session_state.data:
    pages = Pages()

    pages.add_page('chat_page', 'Chat with Bots', 'chat_page', 'chat_page')
    pages.add_page('llm_config', 'LLM Model & Key', 'llm_config', 'llm_config')
    pages.add_page('config_email', 'Setup Email Integration', 'config_email', 'setup_email')
    pages.add_page('config_cortex_search', 'Setup Cortex Search', 'config_cortex_search', 'setup_cortex_search')
    pages.add_page('setup_slack', 'Setup Slack Connection', 'setup_slack', 'setup_slack')
    pages.add_page('config_wh', 'Setup Custom Warehouse', 'config_wh', 'config_wh')
    pages.add_page('grant_data', 'Grant Data Access', 'grant_data', 'grant_data')
    pages.add_page('config_custom_eai', 'Setup Custom Endpoints', 'config_custom_eai', 'config_custom_eai')
    pages.add_page('config_jira', 'Setup Jira API Params', 'config_jira', 'config_jira')
    pages.add_page(
        "config_g_sheets",
        "Setup Google Workspace API",
        "config_g_sheets",
        "config_g_sheets",
    )
    pages.add_page('db_harvester', 'Harvester Status', 'db_harvester', 'db_harvester')
    pages.add_page('bot_config', 'Bot Configuration', 'bot_config', 'bot_config')
    pages.add_page('start_stop', 'Server Stop-Start', 'start_stop', 'start_stop')
    # pages.add_page('config_logging', 'Setup Event Logging', 'config_logging', 'config_logging')
    pages.add_page('show_server_logs', 'Server Logs', 'show_server_logs', 'show_server_logs')
    pages.add_page('support', 'Support and Community', 'support', 'support')


#    st.sidebar.subheader("**Genesis App**")

    # Get NativeMode from session state
    native_mode = st.session_state.get("NativeMode", False)
    if native_mode:
        render_image("Genesis-Computing-Logo-White.png", width=250)
    else:
        st.sidebar.image("./streamlit_gui/Genesis-Computing-Logo-White.png", width=250)

    # Set the default selection page
    selected_page_id = None

    # Handle URL params which are used, for example, to drop user into a specific page or chat session.
    # We expect a param named 'action' followd by action-specific params
    url_params = st.query_params.to_dict()
    if url_params:
        action = url_params.pop('action', None)
        if action == "show_artifact_context":
            bot_name = url_params.pop('bot_name', None)
            artifact_id = url_params.pop('artifact_id', None)
            if bot_name and artifact_id:
                # Force the selected page to the chat page and inject the initial bot_name and initial prompt
                selected_page_id = 'chat_page'
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
                        5. If the metadata indicates that this artifct contains other artifact, offer the user to explore the contained artifact.
                        ''')
                )
            else:
                #TODO: handle missing  params
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

    # Use a dropdown for page selection, Use page display names
    selection = st.sidebar.selectbox(
        "#### Menu:",  # Added ### to make it bigger in Markdown
        [page.display_name for page in pages.all.values()],
        index=list(pages.all).index(selected_page_id),
        key="page_selection"
    )

    # Check if the selection has changed
    if "previous_selection" not in st.session_state or st.session_state.previous_selection != selection:
        st.session_state.previous_selection = selection
        st.session_state["radio"] = selection
        st.rerun()

    try:
        page_desc = pages.lookup_page("display_name", selection) # TODO: again, refactor to use page IDs.
        pages.dispatch_page(page_desc.page_id)
    except ValueError:
        pass

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