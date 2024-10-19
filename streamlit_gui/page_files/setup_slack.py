import streamlit as st
from utils import get_slack_tokens, set_slack_tokens, get_slack_tokens_cached, check_eai_status, get_references, upgrade_services

def setup_slack():
    if "slack_eai_available" not in st.session_state:
        st.session_state.slack_eai_available = False
    if "eai_reference_name" not in st.session_state:
        st.session_state.eai_reference_name = 'slack_external_access' 

    if st.session_state.slack_eai_available == False:

        # check for custom EAI
        eai_status = False
        try:
            eai_status = check_eai_status('slack')
        except Exception as e:
            st.write("Failed to check EAI status: ", e)

        if eai_status == False:
            if st.session_state.NativeMode:
                import snowflake.permissions as permissions
                permissions.request_reference("slack_external_access")
        else:
            st.session_state.slack_eai_available = True
            st.write(f"Slack External Access Integration available")
            

    tokens = get_slack_tokens()
    get_slack_tokens_cached.clear()

    tok = tokens.get("Token", None)
    ref_tok = tokens.get("RefreshToken", None)
    slack_active = tokens.get("SlackActiveFlag", False)

    if slack_active:
        st.success("Slack Connector is Currently Active")
    else:
        st.warning(
            "Slack Connector is not currently active, please complete the form below to activate."
        )

    st.title("Setup Slack Tokens")
    st.write(
        "By providing a Slack App Refresh Token, Genesis Bots can create, update, and remove Genesis Bots from your Slack environment. If you have not yet assigned the External Access Integration to Genesis, click the Assign EAI to Genesis button and then you can enter your Slack token."
    )
    if st.session_state.slack_eai_available == False:

        if st.button("Assign EAI to Genesis", key="assigneai"):
            if st.session_state.eai_reference_name:
                eai_type = st.session_state.eai_reference_name.split('_')[0].upper()
                upgrade_result = upgrade_services(eai_type, st.session_state.eai_reference_name)
                st.success(f"Genesis Bots upgrade result: {upgrade_result}")
                st.session_state.slack_eai_available = True
                # st.session_state.clear()
                
                st.rerun()
            else:
                st.error("No EAI reference set")
    else:

        st.write(
            "Go to https://api.slack.com/apps and create an App Config Refresh Token, paste it below, and press Update. "
        )

        if tok == "...":
            tok = ""
        if ref_tok == "...":
            ref_tok = ""

        if tok:
            slack_app_token = st.text_input("Slack App Token", value=tok)
        # Text input for Slack App Refresh Token
        slack_app_refresh_token = st.text_input(
            "Slack App REFRESH Token", value=ref_tok if ref_tok else ""
        )
        if st.button("Update Slack Token"):
            # Call function to update tokens
            resp = set_slack_tokens("NOT NEEDED", slack_app_refresh_token)
            t = resp.get("Token", "Error")
            r = resp.get("Refresh", "Error")
            if t == "Error":
                st.error(f"Failed to update Slack tokens: {resp}")
            else:
                # Clear the cache of get_slack_tokens_cached
                get_slack_tokens_cached.clear()
                st.success(
                    "Slack tokens updated and refreshed successfully. Your new refreshed tokens are:"
                )
                st.json({"Token": t, "RefreshToken": r})
                st.success(
                    "These will be different than the ones you provided, as they have been rotated successfully for freshness."
                )
                st.success(
                    "You can now activate your bots on Slack from the Bot Configuration page, on the left Nav."
                )
                st.session_state.show_slack_config = False
