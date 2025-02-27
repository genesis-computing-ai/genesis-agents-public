import json
import streamlit as st
from utils import (check_eai_assigned, get_references, get_session, set_metadata, upgrade_services)
from .components import config_page_header

def config_jira():
    config_page_header("Setup Jira API Params")
    # Initialize session state variables
    if "jira_eai_available" not in st.session_state:
        st.session_state.jira_eai_available = False
    if "eai_reference_name" not in st.session_state:
        st.session_state.eai_reference_name = "jira_external_access"
    if "NativeMode" not in st.session_state:
        st.session_state.NativeMode = False  # Or set this based on your environment

    # Page Title
    #st.title("Configure Jira API settings")

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

    # Instructions
    st.markdown(
        '<p class="big-font">Add your Jira URL, email address, and API key below</p>',
        unsafe_allow_html=True
    )

    # User input fields
    jira_url = st.text_input("Your Jira URL (e.g. https://genesiscomputing.atlassian.net):")
    jira_email = st.text_input("Your Jira email address:")
    jira_api_key = st.text_input("Your Jira API key:")

    # Handle submission of Jira parameters
    if st.button("Add Jira API parameters to access Jira from Genesis"):
        if not jira_url:
            st.error("Jira URL is required.")
        elif not jira_email:
            st.error("Jira email address is required.")
        elif not jira_api_key:
            st.error("Jira API key is required.")
        else:
            try:
                # Extract the site name from the Jira URL
                # Assumes URL in form: https://<site_name>.atlassian.net
                site_name = jira_url.split("//")[1].split(".")[0]

                # Prepare key-value pairs for metadata
                key_pairs = {
                    "site_name": site_name,
                    "jira_url": jira_url,
                    "jira_email": jira_email,
                    "jira_api_key": jira_api_key
                }
                # Send data to metadata
                jira_api_config_result = set_metadata(f"api_config_params jira {json.dumps(key_pairs)}")
                # Check if the result indicates success
                if (isinstance(jira_api_config_result, list) and jira_api_config_result and
                    jira_api_config_result[0].get('Success') is True):
                    st.success("Jira API parameters configured successfully!")
                else:
                    st.error(f"Failed to configure Jira API parameters: {jira_api_config_result}")

            except Exception as e:
                st.error(f"Error configuring Jira params: {e}")

    # Check if Jira EAI is available and we're in Native Mode
    if not st.session_state.jira_eai_available and st.session_state.get("NativeMode", False) == True:
        try:
            eai_status = check_eai_assigned("jira_external_access")
            if eai_status:
                st.session_state.jira_eai_available = True
                st.success("Jira External Access Integration is available.")
            else:
                # If EAI is not available offer options
                try:
                    ref = get_references(st.session_state.eai_reference_name)
                    if not ref:
                        # If no reference found, allow creating a new one
                        if st.button("Create External Access Integration", key="create_eai"):
                            import snowflake.permissions as permissions
                            permissions.request_reference(st.session_state.eai_reference_name)
                            st.info("Request sent. Please rerun the app or try again to see updates.")
                    else:
                        # Reference exists but not assigned, allow assigning now
                        if st.button("Assign EAI to Genesis", key="assign_eai"):
                            if st.session_state.eai_reference_name:
                                # Upgrade services for the EAI reference
                                eai_type = st.session_state.eai_reference_name.split("_")[0].upper()
                                upgrade_result = upgrade_services(eai_type, st.session_state.eai_reference_name)
                                st.success(f"Genesis Bots upgrade result: {upgrade_result}")
                                st.session_state.jira_eai_available = True
                                st.rerun()
                            else:
                                st.error("No EAI reference set. Cannot assign EAI.")
                except Exception as e:
                    st.error(f"Failed to process references: {e}")
        except Exception as e:
            st.error(f"Failed to check EAI status: {e}")
