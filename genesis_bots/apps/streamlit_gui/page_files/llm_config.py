import streamlit as st
import time
from utils import (
    check_eai_assigned,
    get_bot_details,
    get_metadata,
    configure_llm,
    check_eai_status,
    upgrade_services,
)
from .components import config_page_header
from .configuration import hide_sidebar

def llm_config():
    hide_sidebar()
    # Add the header with back button
    config_page_header("LLM Model & Key Configuration")

    # Initialize session state variables with defaults
    session_defaults = {
        "openai_eai_available": not st.session_state.get("NativeMode", False),
        "azureopenai_eai_available": not st.session_state.get("NativeMode", False),
        "set_endpoint": False,
        "eai_reference_name": None,
        "assign_disabled": True,
        "disable_create": False,
        "disable_submit": True,
        "data_source": "snowflake",
    }
    for key, value in session_defaults.items():
        st.session_state.setdefault(key, value)

    # Check if Snowflake metadata or not
    metadata_response = get_metadata('check_db_source')
    # st.write("Debug - metadata_response:", metadata_response)  # Debug print
    st.session_state.data_source = "other"
    if metadata_response == True:
        st.session_state.data_source = "snowflake"

    # Check External Access Integration status
    if st.session_state.get("NativeMode", False) == True:
        check_eai_availability('openai', 'openai_external_access')
        check_eai_availability('azureopenai', 'azure_openai_external_access')
    else:
        st.session_state.update({
            "openai_eai_available": True,
            "eai_reference_name": 'openai_external_access',
            "azureopenai_eai_available": True,
            "eai_reference_name": 'azure_openai_external_access',
            "disable_submit": False,
        })

    # Get bot details and LLM info
    bot_details = get_bot_details()
    llm_info = get_metadata("llm_info")
    llm_types, active_llm_type = prepare_llm_info(llm_info)

    # Display setup messages
    display_setup_messages(bot_details, active_llm_type, llm_types)

    # LLM selection and API key input
    # llm_model = st.selectbox("Choose LLM Model:", ["OpenAI", "Azure OpenAI", "Cortex"])
    llm_options = ["OpenAI", "Azure OpenAI"]
    if st.session_state.get("data_source", "").lower() == "snowflake":
        llm_options.append("Cortex")
    llm_model = st.selectbox("Choose LLM Model:", llm_options)
    st.session_state.llm_type = llm_model.lower().replace(' ', '')
    llm_api_key, llm_api_endpoint = "", ""

    if st.session_state.llm_type == "openai":
        llm_api_key = handle_openai_configuration()
    elif st.session_state.llm_type == "azureopenai":
        if st.session_state.azureopenai_eai_available == False:
            st.session_state.disable_submit = True
        llm_api_key, llm_api_endpoint = handle_azure_openai_configuration()
    else:
        st.session_state.disable_submit = False
        llm_api_key = 'cortex_no_key_needed'

    # Submit button
    if st.button("Submit Model Selection", key="sendllm"):
        st.write("One moment while I validate the key and launch the bots...")
        process_llm_configuration(llm_api_key, llm_api_endpoint, llm_model)

def check_eai_availability(llm_type, reference_name):
    if not st.session_state.get(f"{llm_type}_eai_available", False):
        if check_eai_assigned(reference_name) or st.session_state.NativeMode == False:
            st.session_state.update({
                f"{llm_type}_eai_available": True,
                "eai_reference_name": reference_name,
                "disable_submit": False,
            })
            st.write(f"{llm_type.capitalize()} External Access Integration available")

def prepare_llm_info(llm_info):
    replace_map = {"openai": "OpenAI", "cortex": "Cortex"}
    llm_types = []
    active_llm_type = None

    for llm in llm_info:
        llm["llm_type"] = replace_map.get(llm["llm_type"], llm["llm_type"])
    if llm_info:
        active_llm = next((llm for llm in llm_info if llm["active"]), {})
        active_llm_type = active_llm.get("llm_type")
        llm_types = [
            {"LLM Type": llm["llm_type"], "Active": "✔" if llm["active"] else ""}
            for llm in llm_info
        ]
    return llm_types, active_llm_type

def display_setup_messages(bot_details, active_llm_type, llm_types):
    if bot_details == {"Success": False, "Message": "Needs LLM Type and Key"}:
        st.success("Welcome! Before you chat with bots, please set up your LLM Keys")
        time.sleep(0.3)
    elif active_llm_type:
        st.success(f"You already have an LLM active: **{active_llm_type}**. You can change it below.")
  #  st.header("LLM Model & API Key Setup")
    if st.session_state.get("NativeMode", False) == False:
        st.write(
            "Genesis Bots use OpenAI LLM models to operate. Please choose your OpenAI provider "
            "(OpenAI or Azure OpenAI) and API key. If you need an OpenAI API key, you can get one at "
            "[OpenAI's website](https://platform.openai.com/api-keys)."
        )
    else:
        st.write(
            "Genesis Bots use LLM models to operate. You can choose between OpenAI, Azure OpenAI, "
            "and Snowflake Cortex models. To add or update a key for OpenAI or Azure OpenAI, enter it below. "
            "For Snowflake Cortex, you'll need to assign the External Access Integration (EAI) to Genesis "
            "by clicking the 'Assign EAI to Genesis' button before entering your key."
        )
    if llm_types:
        st.markdown("**Currently Stored LLMs**")
        st.markdown(
            "<style>.dataframe { width: auto !important; }</style>",
            unsafe_allow_html=True
        )
        st.dataframe(llm_types, use_container_width=False)

def handle_openai_configuration():
    llm_api_key = ''
    if st.session_state.get("NativeMode", False) and not st.session_state.openai_eai_available:
        if st.button("Create External Access Integration", key="createeai", disabled=st.session_state.disable_create):
            st.session_state.update({
                "eai_reference_name": 'openai_external_access',
                "assign_disabled": False,
                "disable_create": True,
            })
            import snowflake.permissions as permissions
            permissions.request_reference("openai_external_access")
        if not st.session_state.assign_disabled:
            if st.button("Assign EAI to Genesis"):
                assign_eai_to_genesis()
    else:
        llm_api_key = st.text_input("Enter OpenAI API Key:", value=llm_api_key, key="oaikey")
    return llm_api_key

def handle_azure_openai_configuration():
    llm_api_key, llm_api_endpoint = '',''
    if not st.session_state.azureopenai_eai_available or st.session_state.get("NativeMode", False) == False:
        endpoint = st.text_input("Enter Azure API endpoint name for your organization (e.g., genesis-azureopenai-1):")
        azure_openai_model = st.text_input("Enter Azure OpenAI Model Deployment Name (e.g., gpt-4o):", value="gpt-4o")
        azure_openai_embed_model = st.text_input("Enter Azure OpenAI Embedding Model Deployment Name (e.g., text-embedding-3-large):", value="text-embedding-3-large")

        if st.session_state.get("NativeMode", False):
            if st.button("Create External Access Integration", key="createaeai", disabled=st.session_state.disable_create):
                set_endpoint = get_metadata(f"set_endpoint Azure_OpenAI {endpoint} AZURE")
                if set_endpoint and set_endpoint[0].get('Success'):
                    set_model_names = get_metadata(f"set_model_name {azure_openai_model} {azure_openai_embed_model}")
                    if set_model_names and set_model_names[0].get('Success'):
                        st.session_state.update({
                            "assign_disabled": False,
                            "eai_reference_name": 'azure_openai_external_access',
                            "disable_create": True,
                        })
                        import snowflake.permissions as permissions
                        permissions.request_reference("azure_openai_external_access")
            if not st.session_state.assign_disabled:
                if st.button("Assign EAI to Genesis"):
                    assign_eai_to_genesis()
        else:
            if st.button("Save Azure LLM Model Params", key="saveparams"):
                set_endpoint = get_metadata(f"set_endpoint Azure_OpenAI {endpoint} AZURE")
                if set_endpoint and set_endpoint[0].get('Success'):
                    set_model_names = get_metadata(f"set_model_name {azure_openai_model} {azure_openai_embed_model}")
                    if set_model_names and set_model_names[0].get('Success'):
                        st.success("Azure LLM parameters saved successfully!")
                        st.session_state.update({
                            "disable_submit": False,
                            "azureopenai_eai_available": True,
                        })
            llm_api_key = st.text_input("Enter Azure OpenAI API Key:", key="aoaikey1")
            llm_api_endpoint = st.text_input("Enter Azure OpenAI API Endpoint URL (e.g., https://genesis-azureopenai-1.openai.azure.com):")


    elif st.session_state.azureopenai_eai_available or st.session_state.get("NativeMode", False) == False:
        llm_api_key = st.text_input("Enter Azure OpenAI API Key:", value=llm_api_key, key="aoaikey")
        llm_api_endpoint = st.text_input("Enter Azure OpenAI API Endpoint URL (e.g., https://genesis-azureopenai-1.openai.azure.com):")
    return llm_api_key, llm_api_endpoint

def assign_eai_to_genesis():
    if st.session_state.eai_reference_name:
        # Add safety check before using upper()
        eai_type = st.session_state.eai_reference_name.split('_')[0]
        if eai_type:  # Make sure we have a value before calling upper()
            eai_type = eai_type.upper()
            upgrade_result = upgrade_services(eai_type, st.session_state.eai_reference_name)
            if upgrade_result:
                st.success(f"Genesis Bots upgrade result: {upgrade_result}")
                st.session_state.update({
                    "assign_disabled": True,
                    f"{st.session_state.llm_type}_eai_available": True,
                    "disable_submit": False,
                })
                st.rerun()
            else:
                st.error("Upgrade services failed to return a valid response.")
        else:
            st.error("Invalid EAI reference name format")
    else:
        st.error("No EAI reference set")

def process_llm_configuration(llm_api_key, llm_api_endpoint, llm_model):
    #set llm type to openai for azure openai to use same logic throughout application
    if st.session_state.llm_type == 'azureopenai':
        st.session_state.llm_type = 'openai'
    config_response = configure_llm(st.session_state.llm_type, llm_api_key, llm_api_endpoint)
    if config_response["Success"]:
        st.session_state.disable_submit = True
        st.success(f"{llm_model} LLM validated!")
        with st.spinner("Getting active bot details..."):
            bot_details = get_bot_details()
        if bot_details:
            st.success("Bot details validated.")
            time.sleep(0.5)
            st.success("-> Please refresh this browser page to chat with your bots!")
            st.cache_data.clear()
            st.cache_resource.clear()
            get_bot_details.clear()
    else:
        st.error(f"Failed to set LLM token: {config_response['Message']}")
        configure_llm('cortex', 'cortex_no_key_needed', '')
