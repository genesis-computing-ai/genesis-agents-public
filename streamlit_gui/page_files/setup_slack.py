import streamlit as st
from utils import get_slack_tokens, set_slack_tokens, get_slack_tokens_cached

def setup_slack():
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
        "By providing a Slack App Refresh Token, Genesis Bots can create, update, and remove Genesis Bots from your Slack environment."
    )
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