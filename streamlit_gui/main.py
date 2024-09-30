import streamlit as st
from utils import check_status, get_session, get_references, get_metadata, get_slack_tokens, get_slack_tokens_cached
import time
import base64
#from streamlit import config 
# Set minCachedMessageSize to 500 MB to disable forward message cache: 
#config.set_option("global.minCachedMessageSize", 500 * 1e6)


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
        openai_set = [True for llm in llm_info if llm["llm_type"] == 'OpenAI']
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
                                        f"call {st.session_state.app_name}.core.start_app_instance('APP1','GENESIS_POOL',FALSE,'{st.session_state.wh_name}')"
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

if st.session_state.data:
    pages = {
        "Chat with Bots": lambda: __import__('page_files.chat_page').chat_page.chat_page(),
        "LLM Model & Key": lambda: __import__('page_files.llm_config').llm_config.llm_config(),
        "Setup Email Integration": lambda: __import__('page_files.config_email').config_email.setup_email(),
        "Setup Slack Connection": lambda: __import__('page_files.setup_slack').setup_slack.setup_slack(),
        "Setup Custom Warehouse": lambda: __import__('page_files.config_wh').config_wh.config_wh(),
        "Grant Data Access": lambda: __import__('page_files.grant_data').grant_data.grant_data(),
        "Harvester Status": lambda: __import__('page_files.db_harvester').db_harvester.db_harvester(),
        "Bot Configuration": lambda: __import__('page_files.bot_config').bot_config.bot_config(),
        "Server Stop-Start": lambda: __import__('page_files.start_stop').start_stop.start_stop(),
        # "Setup Event Logging": lambda: __import__('page_files.config_logging').config_logging.config_logging(),
        "Server Logs": lambda: __import__('page_files.show_server_logs').show_server_logs.show_server_logs(),
        "Support and Community": lambda: __import__('page_files.support').support.support(),
    }

    if st.session_state.get("needs_keys", False):
        del pages["Chat with Bots"]


#    st.sidebar.subheader("**Genesis App**")



    # Get NativeMode from session state
    native_mode = st.session_state.get("NativeMode", False)
    if native_mode:
        render_image("Genesis-Computing-Logo-White.png", width=250)
    else:
        st.sidebar.image("./streamlit_gui/Genesis-Computing-Logo-White.png", width=250)
    # Set the default selection to "Chat with Bots"
    default_selection = "Chat with Bots" if "Chat with Bots" in pages else list(pages.keys())[0]
    
    # Use a dropdown for page selection
    selection = st.sidebar.selectbox(
        "#### Menu:",  # Added ### to make it bigger in Markdown
        list(pages.keys()),
        index=list(pages.keys()).index(
            st.session_state.get("radio", default_selection)
        ),
        key="page_selection"
    )
    
    # Check if the selection has changed
    if "previous_selection" not in st.session_state or st.session_state.previous_selection != selection:
        st.session_state.previous_selection = selection
        st.session_state["radio"] = selection
        st.rerun()

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