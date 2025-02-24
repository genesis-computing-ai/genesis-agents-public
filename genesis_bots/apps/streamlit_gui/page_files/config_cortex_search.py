import streamlit as st
from utils import (
 get_session, get_metadata)
import pandas as pd
from .components import config_page_header

def setup_cortex_search():
    config_page_header("Setup Cortex Search")

    local=False
    session = get_session()
    if not session:
        local = True

    st.title("Configure Cortex Search")

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

    st.markdown('<p class="big-font">Why do we need to configure Cortex Search?</p>', unsafe_allow_html=True)

    st.markdown("""
    Genesis Bots can use Cortex Search wich enables low-latency, high-quality "fuzzy" search over your Snowflake data. Cortex Search powers a broad array of search experiences for Snowflake users including Retrieval Augmented Generation (RAG) applications leveraging Large Language Models (LLMs).
    """, unsafe_allow_html=True)

    st.markdown('<p class="big-font">Cortex Search Configuration Steps</p>', unsafe_allow_html=True)

    st.markdown("""1. Set up key-pair authentication. As part of this process, you must: <br>
                        a. Generate a public-private key pair. The generated private key should be in a file (e.g. named rsa_key.p8). <br>
                        b. Assign the public key to your Snowflake user. After you assign the key to the user, run the DESCRIBE USER command. In the output, the RSA_PUBLIC_KEY_FP property should be set to the fingerprint of the public key assigned to the user.
                """, unsafe_allow_html=True)

    setup_text = """
// Run the following on your local system to create the private key. You can update the location of the key based on preference.
// NOTE: add -nocrypt to the end of the command to create an unencrypted key.

openssl genrsa 2048 | openssl pkcs8 -topk8 -v2 des3 -inform PEM -out ~/Documents/keys/rsa_key.p8

// Run the following to create the public key. If the private key was encrypted, enter the same passphrase used.

openssl rsa -in ~/Documents/keys/rsa_key.p8 -pubout -out ~/Documents/keys/rsa_key.pub

// In a Snowflake worksheet, alter the user to which the key will be applied to store the public key with the user. Replace <username> with your Snowflake username.
// NOTE: rsa_public_key is the content of the public key file (rsa_key.pub) generated in the previous step

USE ROLE ACCOUNTADMIN;
alter user <username> set rsa_public_key = 'exampleKey0RealKeyismuchLONGER...andmulTIPLElines';

    """

    st.code(setup_text, language="sql")

    st.markdown("""2. Add your private key path to Snowflake Secret during App setup.""", unsafe_allow_html=True)
    # st.markdown("""2. Copy and paste the content of your private key (e.g. named rsa_key.p8) below:""", unsafe_allow_html=True)
    # private_key = st.text_input("Private Key Content", value='')
    # if st.button("Update Private Key"):
    #     if not private_key:
    #         st.error("Please provide private_key.")
    #     else:
    #         print(private_key)


    secret_text = """
// Run the following in a Snowflake worksheet to create a Secret that will store the previously generated private key.
USE ROLE ACCOUNTADMIN;
CREATE SECRET IF NOT EXISTS cortex_search_secret
  TYPE = GENERIC_STRING
  SECRET_STRING = 'PrivateKeyValueIsMuchLonger';

    """

    st.code(secret_text, language="sql")

    st.markdown("""3. Click the button below to create a reference on the Secret object which allows the Genesis application to use the Secret to login to the Cortex Search service. """)
    if st.button("Add Secret reference"):
        import snowflake.permissions as permissions
        permissions.request_reference("private_key_secret")

    st.markdown("""4. Below is the list of available Cortex Search Services:""", unsafe_allow_html=True)

    cortex_search_services = get_metadata("cortex_search_services")
    print(cortex_search_services)
    if cortex_search_services:
        cortex_search_services_df = pd.DataFrame(cortex_search_services).rename(columns=str.lower)
    else:
        cortex_search_services_df = pd.DataFrame(
            columns=['attribute_columns', 'columns', 'comment', 'created_on', 'database_name', 'definition', 'name', 'schema_name', 'search_column', 'target_lag', 'warehouse']
        )
    st.dataframe(cortex_search_services_df, use_container_width=True)

    st.markdown("""4. Follow the instructions <a href="https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-search/cortex-search-overview" target="_blank">here</a> to add a new Cortex Search Service to your schemas""", unsafe_allow_html=True)

    st.info("If you need any assistance, please check our [documentation](https://genesiscomputing.ai/docs/) or join our [Slack community](https://communityinviter.com/apps/genesisbotscommunity/genesis-bots-community).")


