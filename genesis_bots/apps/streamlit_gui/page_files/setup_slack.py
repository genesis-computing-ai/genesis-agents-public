import streamlit as st
from utils import (
    get_references,
    get_slack_tokens,
    set_slack_tokens,
    get_slack_tokens_cached,
    check_eai_assigned,
    upgrade_services,
    get_metadata,
    set_metadata,
    get_ngrok_tokens,
)
from .components import config_page_header

def setup_slack():
    # Add the header with back button
    # Check if in native mode
    if st.session_state.get("NativeMode", True):
        config_page_header("Setup Slack Connection")
    else:
        config_page_header("Setup Slack and ngrok Connections")

    # Initialize session state variables
    st.session_state.setdefault("slack_eai_available", False)
    st.session_state["eai_reference_name"] = "slack_external_access"

    # Check if Slack External Access Integration (EAI) is available
    if not st.session_state.slack_eai_available and st.session_state.get("NativeMode", False) == True:
        try:
            eai_status = check_eai_assigned("slack_external_access")
            if eai_status is not None and eai_status:
                st.session_state.slack_eai_available = True
                st.success("Slack External Access Integration is available.")
            else:
                # Request EAI if not available
                ref = get_references("slack_external_access")
                if not ref:
                    import snowflake.permissions as permissions
                    permissions.request_reference("slack_external_access")
        except Exception as e:
            st.error(f"Failed to check EAI status: {e}")
            st.session_state.slack_eai_available = False

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
    if st.session_state.get("NativeMode", True):
        st.write("Configure your Slack integration settings below:")
    else:
        st.write("Configure your Slack and ngrok integration settings below - Slack allows your bots to communicate through channels while ngrok creates the required secure tunnel for Slack to send messages to your bots.")

    st.write("---")  # Add a visual separator

    if not st.session_state.slack_eai_available and st.session_state.get("NativeMode", False) == True:
        if st.button("Assign EAI to Genesis", key="assigneai"):
            if st.session_state.eai_reference_name:
                eai_type = "SLACK"
                upgrade_result = upgrade_services(eai_type, "slack_external_access")
                st.success(f"Genesis Bots upgrade result: {upgrade_result}")
                st.session_state.slack_eai_available = True
                st.rerun()
            else:
                st.error("No EAI reference set.")
    else:
        st.subheader("Slack Configuration")
        st.write("1. Go to [Slack Apps API](https://api.slack.com/apps)")
        st.write("2. Scroll down to \"Your App Configuration Tokens\"") 
        st.write("3. Press \"Generate Token\" to create Slack tokens")
        st.write("4. Press \"Copy\" on your \"Refresh Token\"")
        st.write("5. Paste the token below and press **Update**")

        if tok == "...":
            tok = ""
        if ref_tok == "...":
            ref_tok = ""

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
                r = resp.get("Refresh")
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

        # NGROK section for non-NativeMode deployments
        if st.session_state.get("NativeMode", False) == False:
            st.write("---")  # Add a visual separator

            st.subheader("NGROK Configuration")
            st.write("Go to [NGROK Auth Token Page](https://dashboard.ngrok.com/get-started/your-authtoken) to get a free NGROK auth token. This token allows Slack to securely communicate with your local Genesis deployment for bot activation.")
            st.write("1. Create a free NGROK account if you don't have one")
            st.write("2. Copy your auth token from the dashboard") 
            st.write("3. Paste it below and click Update")
            
            # Get existing ngrok tokens
            existing_tokens = get_ngrok_tokens()
            existing_key = existing_tokens.get('ngrok_auth_token', '') if isinstance(existing_tokens, dict) else ''
            ngrok_auth_key = st.text_input("Add NGROK Auth Key", value=existing_key)

            if st.button("Update NGROK Auth Key"):
                response = set_metadata(f"ngrok {ngrok_auth_key}")
                if isinstance(response, dict) and 'NGROK auth token set successfully' in response.get('Message', ''):
                    st.success(response.get('Message'))
                else:
                    st.error(f"Failed to update NGROK auth key: {response}")
