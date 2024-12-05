import sys
sys.path.append(".")

import streamlit as st
from utils import get_session
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
        '<p class="big-font">Add your the information from your Google Worksheets service account. \n(#TODO) Explainer on how to set up Google Projects/Service account</p>',
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
                "universe_domain": "googleapis.com",
            }
            try:
                genesis_source = os.getenv("GENESIS_SOURCE", default="Snowflake")
                # db_adapter = get_global_db_connector(genesis_source)
                # cursor = db_adapter.client.cursor()
                conn = SnowflakeConnection(
                    account=os.getenv("SNOWFLAKE_ACCOUNT_OVERRIDE"),
                    user=os.getenv("SNOWFLAKE_USER_OVERRIDE"),
                    password=os.getenv("SNOWFLAKE_PASSWORD_OVERRIDE"),
                    database=os.getenv("SNOWFLAKE_DATABASE_OVERRIDE"),
                    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE_OVERRIDE"),
                    role=os.getenv("SNOWFLAKE_ROLE_OVERRIDE"),
                )

                cursor = conn.cursor()
                database = os.getenv("GENESIS_INTERNAL_DB_SCHEMA")

                for key, value in key_pairs.items():
                    if isinstance(value, str):
                        print(
                            f"Storing {key} in database '{database}'.EXT_SERVICE_CONFIG"
                        )
                        value = value.replace("\n", "")

                        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                        create_creds_query = f"""
                        INSERT INTO {database}.EXT_SERVICE_CONFIG 
                        (ext_service_name, parameter, value, created, updated)
                        VALUES 
                        ('g-sheets', '{key}', '{value}', '{timestamp}', '{timestamp}');
                        """

                        result = cursor.execute(create_creds_query)
                        result = conn.commit()

            except Exception as e:
                st.error(f"Error configuring Google Worksheet params: {e}")
            finally:
                cursor.close()

                st.success("Google Worksheet API parameters configured successfully.")

        st.info(
            "If you need any assistance, please check our [documentation](https://genesiscomputing.ai/docs/) or join our [Slack community](https://communityinviter.com/apps/genesisbotscommunity/genesis-bots-community)."
        )
