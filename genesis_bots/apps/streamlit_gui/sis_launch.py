import streamlit as st
from ...apps.streamlit_gui.utils import check_status, get_session
import time
import base64

# Set Streamlit to wide mode
st.set_page_config(layout="wide")

st.session_state.app_name = "GENESIS_BOTS"
st.session_state.prefix = st.session_state.app_name + ".app1"
st.session_state.core_prefix = st.session_state.app_name + ".CORE"

if 'NativeMode' not in st.session_state:
    st.session_state.NativeMode = True

if "wh_name" not in st.session_state:
    st.session_state["wh_name"] = "XSMALL"

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

st.markdown("""
    <style>
    .block-container {
        padding-top: 0rem;
        padding-bottom: 0rem;
        padding-left: 2rem;
        padding-right: 2rem;
    }
    .stSidebar > div:first-child {
        padding-top: 0rem;
    }
    .stSidebar .block-container {
        padding-top: 0.0rem;
        padding-left: 0.5rem;
        padding-right: 0.5rem;
    }
    .stTextInput > div > div > input
        padding-top: 0.25rem;
        padding-bottom: 0.25rem;
    }
    .stButton > button {
        padding-top: 0.25rem;
        padding-bottom: 0.25rem;
    }
    </style>
""", unsafe_allow_html=True)

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


if st.session_state.data:
    pages = {
        "Chat with Bots": lambda: __import__('page_files.chat_page').chat_page.chat_page(),
        "LLM Model & Key": lambda: __import__('page_files.llm_config').llm_config.llm_config(),
        "Setup Slack Connection": lambda: __import__('page_files.setup_slack').setup_slack.setup_slack(),
        "Grant Data Access": lambda: __import__('page_files.grant_data').grant_data.grant_data(),
        "Harvester Status": lambda: __import__('page_files.db_harvester').db_harvester.db_harvester(),
        "Bot Configuration": lambda: __import__('page_files.bot_config').bot_config.bot_config(),
        "Server Stop-Start": lambda: __import__('page_files.start_stop').start_stop.start_stop(),
        "Server Logs": lambda: __import__('page_files.show_server_logs').show_server_logs.show_server_logs(),
        "Support and Community": lambda: __import__('page_files.support').support.support(),
    }

    if st.session_state.get("needs_keys", False):
        del pages["Chat with Bots"]


#    st.sidebar.subheader("**Genesis App**")



    # Get NativeMode from session state
    native_mode = st.session_state.get("NativeMode", False)
    if native_mode:
        render_image("Genesis-Computing-Logo-White.png", width=200)
    else:
        st.sidebar.image("./streamlit_gui/Genesis-Computing-Logo-White.png", width=200)
    # Set the default selection to "Chat with Bots"
    default_selection = "Chat with Bots" if "Chat with Bots" in pages else list(pages.keys())[0]

    # Use a dropdown for page selection
    selection = st.sidebar.selectbox(
        "#### Menu:",  # Added ### to make it bigger in Markdown
        list(pages.keys()),
        index=list(pages.keys()).index(
            st.session_state.get("radio", default_selection)
        ),
    )

    # Update the session state with the current selection
    st.session_state["radio"] = selection


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