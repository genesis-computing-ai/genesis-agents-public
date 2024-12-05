import json
import sys
sys.path.append(".")

import streamlit as st
from utils import get_session, set_metadata
from snowflake.connector import SnowflakeConnection
# from connectors import get_global_db_connector
import os
from datetime import datetime

def config_g_sheets():

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

    project_id = st.text_input("Project ID:")
    client_id = st.text_input("Client ID:")
    client_email = st.text_input("Client Email:")
    private_key_id = st.text_input("Private Key ID:")
    private_key = st.text_area("Private Key:")
    # client_secret = st.text_input("Client Secret:")

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
        # elif not client_secret:
        #     st.error("Client Secret is required.")
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
                "universe_domain": "googleapis.com"
            }
            try:
                key_pairs_str = json.dumps(key_pairs)
                google_api_config_result = set_metadata(f"google_api_config_params google {key_pairs_str}")
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
