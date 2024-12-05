import json
import streamlit as st
from utils import check_eai_status, get_references, get_session, set_metadata, upgrade_services

def config_jira():
    # Initialize session state variables
    st.session_state.setdefault("jira_eai_available", False)
    st.session_state.setdefault("eai_reference_name", "jira_external_access")

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

    jira_url = st.text_input("Your Jira URL (e.g. https://genesiscomputing.atlassian.net):")
    jira_email = st.text_input("Your Jira email address:")
    jira_api_key = st.text_input("Your Jira API key:")

    if st.button("Add Jira API parameters to access Jira from Genesis"):
        try:
            # Execute the command and collect the results
            if not jira_url:
                st.error("Jira URL is required.")
            elif not jira_email:
                st.error("Jira email address is required.")
            elif not jira_api_key:
                st.error("Jira API key is required.")
            else:
                site_name = jira_url.split("//")[1].split(".")[0]
                key_pairs = {
                    "site_name": site_name,
                    "jira_url": jira_url,
                    "jira_email": jira_email,
                    "jira_api_key": jira_api_key
                }

                key_pairs_str = json.dumps(key_pairs)
                jira_api_config_result = set_metadata(f"api_config_params jira {key_pairs_str}")
                if isinstance(jira_api_config_result, list) and len(jira_api_config_result) > 0:
                    if 'Success' in jira_api_config_result[0] and jira_api_config_result[0]['Success']==True:
                        st.success("Jira API params configured successfully")
                    else:
                        st.error(jira_api_config_result)

        except Exception as e:
            st.error(f"Error configuring Jira params: {e}")

    # Check if Slack External Access Integration (EAI) is available
    if not st.session_state.jira_eai_available:
        try:
            eai_status = check_eai_status("jira")
            if eai_status:
                st.session_state.jira_eai_available = True
                st.success("Jira External Access Integration is available.")
            else:
                # Request EAI if not available and in Native Mode
                if st.session_state.get("NativeMode", False) == True:
                    ref = get_references(st.session_state.eai_reference_name)
                    if not ref:
                        if st.button("Create External Access Integration", key="createaeai"):
                            import snowflake.permissions as permissions
                            permissions.request_reference(st.session_state.eai_reference_name)

                    else:
                        if not st.session_state.jira_eai_available and st.session_state.get("NativeMode", False) == True:
                            if st.button("Assign EAI to Genesis", key="assigneai"):
                                if st.session_state.eai_reference_name:
                                    eai_type = st.session_state.eai_reference_name.split("_")[0].upper()
                                    upgrade_result = upgrade_services(eai_type, st.session_state.eai_reference_name)
                                    st.success(f"Genesis Bots upgrade result: {upgrade_result}")
                                    st.session_state.jira_eai_available = True
                                    st.rerun()
                                else:
                                    st.error("No EAI reference set.")

        except Exception as e:
            st.error(f"Failed to check EAI status: {e}")



    st.info("If you need any assistance, please check our [documentation](https://genesiscomputing.ai/docs/) or join our [Slack community](https://communityinviter.com/apps/genesisbotscommunity/genesis-bots-community).")

