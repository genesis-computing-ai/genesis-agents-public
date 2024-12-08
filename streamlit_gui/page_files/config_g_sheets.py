import json
import sys
sys.path.append(".")

import streamlit as st
from utils import check_eai_status, get_references, get_session, set_metadata, upgrade_services
from snowflake.connector import SnowflakeConnection
# from connectors import get_global_db_connector

def config_g_sheets():
    # Initialize session state variables
    st.session_state.setdefault("google_eai_available", False)
    st.session_state.setdefault("eai_reference_name", "google_external_access")

    # Check if Slack External Access Integration (EAI) is available
    if not st.session_state.google_eai_available:
        try:
            eai_status = check_eai_status("google")
            if eai_status:
                st.session_state.google_eai_available = True
                st.success("Google External Access Integration is available.")
            else:
                # Request EAI if not available and in Native Mode
                if st.session_state.get("NativeMode", False) == True:
                    ref = get_references(st.session_state.eai_reference_name)
                    if not ref:
                        import snowflake.permissions as permissions
                        permissions.request_reference(st.session_state.eai_reference_name)
        except Exception as e:
            st.error(f"Failed to check EAI status: {e}")

    local = False
    session = get_session()
    if not session:
        local = True

    st.title("Configure Google Worksheets API settings")

    st.markdown(
        """
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
    """,
        unsafe_allow_html=True,
    )

    st.markdown(
        '<p class="big-font">Add information from your Google Worksheets service account. \n(#TODO) Explainer on how to set up Google Projects/Service account</p>',
        unsafe_allow_html=True,
    )

    if not st.session_state.google_eai_available and st.session_state.get("NativeMode", False) == True:
        if st.button("Assign EAI to Genesis", key="assigneai"):
            if st.session_state.eai_reference_name:
                eai_type = st.session_state.eai_reference_name.split("_")[0].upper()
                upgrade_result = upgrade_services(eai_type, st.session_state.eai_reference_name)
                st.success(f"Genesis Bots upgrade result: {upgrade_result}")
                st.session_state.google_eai_available = True
                st.rerun()
            else:
                st.error("No EAI reference set.")
    else:
        project_id = st.text_input("Project ID*:")
        client_id = st.text_input("Client ID*:")
        client_email = st.text_input("Client Email*:")
        private_key_id = st.text_input("Private Key ID*:")
        private_key = st.text_area("Private Key*:")
        shared_folder_id = st.text_input("Shared Folder ID:")

        if st.button("Add Google Worksheet API parameters to access Google Worksheet account from Genesis"):
            if not client_id:
                st.error("Client ID is required.")
            elif not client_email:
                st.error("Client email is required.")
            elif not project_id:
                st.error("Project ID is required.")
            elif not private_key_id:
                st.error("Private Key ID is required.")
            elif not private_key:
                st.error("Private Key is required.")
            else:
                key_pairs = {
                    "type": "service_account",
                    "project_id": project_id,
                    "private_key_id": private_key_id,
                    "private_key": private_key,
                    "client_email": client_email,
                    "client_id": client_id,
                    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                    "token_uri": "https://oauth2.googleapis.com/token",
                    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
                    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/genesis-workspace-creds%40" + project_id + ".iam.gserviceaccount.com",
                    "universe_domain": "googleapis.com",
                    "shared_folder_id": shared_folder_id,
                }
                try:
                    key_pairs_str = json.dumps(key_pairs)
                    google_api_config_result = set_metadata(f"api_config_params g-sheets {key_pairs_str}")
                    if isinstance(google_api_config_result, list) and len(google_api_config_result) > 0:
                        if 'Success' in google_api_config_result[0] and google_api_config_result[0]['Success']==True:
                            st.success("Google API params configured successfully")
                        else:
                            st.error(google_api_config_result)

                except Exception as e:
                    st.error(f"Error configuring Google API params: {e}")


                    st.success("Google Worksheet API parameters configured successfully.")

        st.info(
            "If you need any assistance, please check our [documentation](https://genesiscomputing.ai/docs/) or join our [Slack community](https://communityinviter.com/apps/genesisbotscommunity/genesis-bots-community)."
        )
