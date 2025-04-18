import json
import streamlit as st
from utils import (check_eai_assigned, get_references, get_session, set_metadata, upgrade_services)
from .components import config_page_header

def config_web_access():
    config_page_header("Setup WebAccess API Key Params")
    # Initialize session state variables - use direct assignment instead of conditional checks
    st.session_state["serper_eai_available"] = st.session_state.get("serper_eai_available", False)
    st.session_state["eai_reference_name"] = "serper_external_access"  # Always set this correctly for this page
    st.session_state["NativeMode"] = st.session_state.get("NativeMode", False)

    # Page Title
   # st.title("Configure WebAccess API settings")

    # Custom styles
    st.markdown("""
    <style>
    .big-font {
        font-size:20px !important;
        font-weight: bold;
    }
    .info-box {
        padding: 20px;
        border-radius: 5px;
        margin-bottom: 20px;
    }
    .code-box {
        background-color: #f0f0f0;
        padding: 20px;
        border-radius: 5px;
        margin-bottom: 20px;
    }
    </style>
    """, unsafe_allow_html=True)

    st.markdown('<p class="big-font">Google Serper Search API</p>', unsafe_allow_html=True)
    st.markdown("""Follow the insruction <a href="https://serper.dev/" target="_blank">here</a> to get an API key""", unsafe_allow_html=True)
    serper_api_key = st.text_input("Serper API Key")

    # Handle submission of Serper parameters
    if st.button("Add Serper API Key"):
        if not serper_api_key:
            st.error("Serper API Key is required.")
        else:
            try:
                key_pairs = {"api_key": serper_api_key}
                # Send data to metadata
                api_config_result = set_metadata(f"api_config_params serper {json.dumps(key_pairs)}")
                # Check if the result indicates success
                if (isinstance(api_config_result, list) and api_config_result and
                    api_config_result[0].get('Success') is True):
                    st.success("Serper API parameters configured successfully!")
                else:
                    st.error(f"Failed to configure Serper API parameters: {api_config_result}")
            except Exception as e:
                st.error(f"Error configuring Serper params: {e}")

    # st.markdown('<p class="big-font">Spider Cloud API</p>', unsafe_allow_html=True)
    # st.markdown("""Spider is the fastest open source scraper and crawler that returns LLM-ready data.
    #             It converts any website into pure HTML, markdown, metadata or text while enabling you to
    #             crawl with custom actions using AI. Follow the instructions <a href="https://spider.cloud/" target="_blank">here</a> to get an API key""", unsafe_allow_html=True)
    # spider_api_key = st.text_input("Spider API Key")

    # # Handle submission of Spider parameters
    # if st.button("Add Spider API Key"):
    #     if not spider_api_key:
    #         st.error("Spider API Key is required.")
    #     else:
    #         try:
    #             key_pairs = {"api_key": spider_api_key}
    #             # Send data to metadata
    #             api_config_result = set_metadata(f"api_config_params spider {json.dumps(key_pairs)}")
    #             # Check if the result indicates success
    #             if (isinstance(api_config_result, list) and api_config_result and
    #                 api_config_result[0].get('Success') is True):
    #                 st.success("Spider API parameters configured successfully!")
    #             else:
    #                 st.error(f"Failed to configure Spider API parameters: {api_config_result}")
    #         except Exception as e:
    #             st.error(f"Error configuring Spider params: {e}")

    # Check if Web Access EAI is available and we're in Native Mode
    if not st.session_state.serper_eai_available and st.session_state.get("NativeMode", False) == True:
        try:
            eai_status = check_eai_assigned("serper_external_access")  # Use direct string instead of session state
            if eai_status:
                st.session_state.serper_eai_available = True
                st.success("Web Access External Access Integration is available.")
            else:
                # If EAI is not available offer options
                try:
                    ref = get_references("serper_external_access")  # Use direct string instead of session state
                    if not ref:
                        # If no reference found, allow creating a new one
                        if st.button("Create External Access Integration", key="create_eai"):
                            from snowflake import permissions
                            permissions.request_reference("serper_external_access")  # Use direct string
                            st.info("Request sent. Please rerun the app or try again to see updates.")
                    else:
                        # Reference exists but not assigned, allow assigning now
                        if st.button("Assign EAI to Genesis", key="assign_eai"):
                            # Use direct string literals for EAI type and name
                            eai_type = "SERPER"
                            upgrade_result = upgrade_services(eai_type, "serper_external_access")
                            st.success(f"Genesis Bots upgrade result: {upgrade_result}")
                            st.session_state.serper_eai_available = True
                            st.rerun()
                except Exception as e:
                    st.error(f"Failed to process references: {e}")
        except Exception as e:
            st.error(f"Failed to check EAI status: {e}")