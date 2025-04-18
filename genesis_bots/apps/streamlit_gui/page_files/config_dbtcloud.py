import json
import streamlit as st
from utils import (check_eai_assigned, get_references, get_session, set_metadata, upgrade_services)
from .components import config_page_header

def config_dbtcloud():
    config_page_header("Setup DBT Cloud API Params")
    # Initialize session state variables - use direct assignment
    st.session_state["dbtcloud_eai_available"] = st.session_state.get("dbtcloud_eai_available", False)
    st.session_state["eai_reference_name"] = "dbtcloud_external_access"  # Always set correctly for this page
    st.session_state["NativeMode"] = st.session_state.get("NativeMode", False)

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
        '<p class="big-font">Add your DBT Cloud API Key below</p>',
        unsafe_allow_html=True
    )

    # Add info about how to create a PAT
    with st.expander("How to create a DBT Cloud API Key"):
        st.markdown("""
        1. TBD
        """)

    # User input fields
    dbtcloud_acct_id = st.text_input("DBT Cloud Account ID:")
    dbtcloud_access_url = st.text_input("DBT Cloud Access URL:")
    dbtcloud_svc_token = st.text_input("DBT Cloud Service Token:", type="password")
    github_user = st.text_input("GitHub Username:")
    github_token = st.text_input("GitHub Token:", type="password")

    # Handle submission of GitHub parameters
    if st.button("Add DBT Cloud parameters to access DBT from Genesis"):
        if not all([dbtcloud_acct_id, dbtcloud_access_url, dbtcloud_svc_token]):
            st.error("All fields are required.")
        else:
            try:
                # Prepare key-value pairs for metadata
                key_pairs = {
                    "dbtcloud_acct_id": dbtcloud_acct_id,
                    "dbtcloud_access_url": dbtcloud_access_url,
                    "dbtcloud_svc_token": dbtcloud_svc_token,
                    "github_user": github_user,
                    "github_token": github_token
                }
                # Send data to metadata
                dbtcloud_api_config_result = set_metadata(f"api_config_params dbtcloud {json.dumps(key_pairs)}")
                # Check if the result indicates success
                if (isinstance(dbtcloud_api_config_result, list) and dbtcloud_api_config_result and
                    dbtcloud_api_config_result[0].get('Success') is True):
                    st.success("DBT Cloud API parameters configured successfully!")
                else:
                    st.error(f"Failed to configure DBT Cloud API parameters: {dbtcloud_api_config_result}")

            except Exception as e:
                st.error(f"Error configuring DBT Cloud params: {e}")

    # Check if DBT Cloud EAI is available and we're in Native Mode
    if not st.session_state.dbtcloud_eai_available and st.session_state.get("NativeMode", False) == True:
        try:
            eai_status = check_eai_assigned("dbtcloud_external_access")  # Use direct string
            if eai_status:
                st.session_state.dbtcloud_eai_available = True
                st.success("DBT Cloud External Access Integration is available.")
            else:
                # If EAI is not available offer options
                try:
                    ref = get_references("dbtcloud_external_access")  # Use direct string
                    if not ref:
                        # If no reference found, allow creating a new one
                        if st.button("Create External Access Integration", key="create_eai"):
                            import snowflake.permissions as permissions
                            permissions.request_reference("dbtcloud_external_access")  # Use direct string
                            st.info("Request sent. Please rerun the app or try again to see updates.")
                    else:
                        # Reference exists but not assigned, allow assigning now
                        if st.button("Assign EAI to Genesis", key="assign_eai"):
                            # Use direct string literals
                            eai_type = "DBTCLOUD"
                            upgrade_result = upgrade_services(eai_type, "dbtcloud_external_access")
                            st.success(f"Genesis Bots upgrade result: {upgrade_result}")
                            st.session_state.dbtcloud_eai_available = True
                            st.rerun()
                except Exception as e:
                    st.error(f"Failed to process references: {e}")
        except Exception as e:
            st.error(f"Failed to check EAI status: {e}") 