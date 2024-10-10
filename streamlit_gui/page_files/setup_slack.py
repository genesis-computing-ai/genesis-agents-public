import streamlit as st
from utils import get_slack_tokens, set_slack_tokens, get_slack_tokens_cached, check_eai_status, get_references, upgrade_services

def setup_slack():
    if "eai_available" not in st.session_state:
        st.session_state.eai_available = False
    #TODO make work like the llm config
    if st.session_state.eai_available == False:
        ref = get_references("consumer_external_access")
        # check for custom EAI
        eai_status = False
        if ref:
            eai_status = check_eai_status('slack')
        if not ref and eai_status == False:
            if st.session_state.NativeMode:
                import snowflake.permissions as permissions
                permissions.request_reference("consumer_external_access")
        else:
            # eai_status = check_eai_status('openai')
            st.session_state.eai_available = eai_status
            if eai_status == True:
                st.write(f"External Access Integration available")

    tokens = get_slack_tokens()
    get_slack_tokens_cached.clear()

    tok = tokens.get("Token", None)
    ref = tokens.get("RefreshToken", None)
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
    if st.session_state.eai_available == False:

        if st.button("Assign EAI to Genesis", key="assigneai"):
            upgrade_result = upgrade_services(True)
            st.success(f"Genesis Bots upgrade result: {upgrade_result}")
            # st.session_state.clear()
            
            st.rerun()
    else:


        st.write(
            "Go to https://api.slack.com/apps and create an App Config Refresh Token, paste it below, and press Update. "
        )

        if tok == "...":
            tok = ""
        if ref == "...":
            ref = ""

        if tok:
            slack_app_token = st.text_input("Slack App Token", value=tok)
        # Text input for Slack App Refresh Token
        slack_app_refresh_token = st.text_input(
            "Slack App REFRESH Token", value=ref if ref else ""
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
