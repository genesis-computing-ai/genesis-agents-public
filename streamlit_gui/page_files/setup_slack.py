import streamlit as st
from utils import (
    get_references,
    get_slack_tokens,
    set_slack_tokens,
    get_slack_tokens_cached,
    check_eai_status,
    upgrade_services,
)

def setup_slack():
    # Initialize session state variables
    st.session_state.setdefault("slack_eai_available", False)
    st.session_state.setdefault("eai_reference_name", "slack_external_access")

    # Check if Slack External Access Integration (EAI) is available
    if not st.session_state.slack_eai_available:
        try:
            eai_status = check_eai_status("slack")
            if eai_status:
                st.session_state.slack_eai_available = True
                st.success("Slack External Access Integration is available.")
            else:
                # Request EAI if not available and in Native Mode
                if st.session_state.get("NativeMode", False) == True:
                    ref = get_references(st.session_state.eai_reference_name)
                    if not ref:
                        import snowflake.permissions as permissions
                        permissions.request_reference(st.session_state.eai_reference_name)
        except Exception as e:
            st.error(f"Failed to check EAI status: {e}")

    # Fetch Slack tokens and clear cached tokens
    tokens = get_slack_tokens()
    get_slack_tokens_cached.clear()

    tok = tokens.get("Token", "")
    ref_tok = tokens.get("RefreshToken", "")
    slack_active = tokens.get("SlackActiveFlag", False)

    # Display Slack Connector status
    if slack_active:
        st.success("Slack Connector is currently active.")
    else:
        st.warning("Slack Connector is not active. Please complete the form below to activate it.")

    # Display page title and description
    st.title("Setup Slack Tokens")
    st.write("""
        By providing a Slack App Refresh Token, Genesis Bots can create, update, and remove bots from your Slack environment.
        If you have not yet assigned the External Access Integration to Genesis, click the **Assign EAI to Genesis** button below.
    """)

    if not st.session_state.slack_eai_available and st.session_state.get("NativeMode", False) == True:
        if st.button("Assign EAI to Genesis", key="assigneai"):
            if st.session_state.eai_reference_name:
                eai_type = st.session_state.eai_reference_name.split("_")[0].upper()
                upgrade_result = upgrade_services(eai_type, st.session_state.eai_reference_name)
                st.success(f"Genesis Bots upgrade result: {upgrade_result}")
                st.session_state.slack_eai_available = True
                st.experimental_rerun()
            else:
                st.error("No EAI reference set.")
    else:
        st.write("Go to [Slack Apps](https://api.slack.com/apps) and create an App Config Refresh Token. Paste it below and press **Update**.")

        if tok == "...":
            tok = ""
        if ref == "...":
            ref = ""

        if tok:
            slack_app_token = st.text_input("Slack App Token", value=tok)
        else:
            slack_app_token = "NOT NEEDED"

        # Show text input for the Slack App Refresh Token
        slack_app_refresh_token = st.text_input("Slack App Refresh Token", value=ref_tok)

        if st.button("Update Slack Tokens"):
            if not slack_app_token or not slack_app_refresh_token:
                st.error("Please provide both the Slack App Token and Refresh Token.")
            else:
                # Update tokens
                resp = set_slack_tokens(slack_app_token=slack_app_token, slack_app_refresh_token=slack_app_refresh_token)
                t = resp.get("Token")
                r = resp.get("RefreshToken")
                if not t or not r:
                    st.error(f"Failed to update Slack tokens: {resp}")
                else:
                    # Clear cached tokens
                    get_slack_tokens_cached.clear()
                    st.success("Slack tokens updated and refreshed successfully. Your new tokens are:")
                    st.json({"Token": t, "RefreshToken": r})
                    st.info("These tokens are refreshed for security purposes.")
                    st.success("You can now activate your bots on Slack from the Bot Configuration page.")
                    st.session_state.show_slack_config = False
