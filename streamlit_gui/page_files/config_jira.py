import streamlit as st
from utils import get_session, set_metadata

def config_jira():

    local=False
    session = get_session()
    if not session:
        local = True

    st.title("Configure Jira API settings")

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

    st.markdown('<p class="big-font">Add your Jira URL, email address, and API key below</p>', unsafe_allow_html=True)


    jira_url = st.text_input("Your Jira URL:")
    jira_email = st.text_input("Your Jira email address:")
    jira_api_key = st.text_input("Your Jira API key:")


    if st.button("Add Jira API parameters to access Jira from Genesis"):
        try:
            # Execute the command and collect the results
            jira_config_result = set_metadata(f"set_jira_config_params {jira_url} {jira_email} {jira_api_key}")
            if isinstance(jira_config_result, list) and len(jira_config_result) > 0:
                if 'Success' in jira_config_result[0] and jira_config_result[0]['Success']==True:
                    st.success("Jira params configured successfully")
            else:
                st.error(jira_config_result)
        except Exception as e:
            st.error(f"Error configuring Jira params: {e}")


    st.info("If you need any assistance, please check our [documentation](https://genesiscomputing.ai/docs/) or join our [Slack community](https://communityinviter.com/apps/genesisbotscommunity/genesis-bots-community).")

