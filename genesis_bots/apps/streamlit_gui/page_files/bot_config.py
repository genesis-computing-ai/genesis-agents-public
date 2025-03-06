import streamlit as st
from utils import get_bot_details, get_slack_tokens, get_metadata, deploy_bot, provide_slack_level_key
from .components import config_page_header

def bot_config():
    config_page_header("Bot Configuration")
    get_bot_details.clear()
    bot_details = get_bot_details()

    if bot_details == {"Success": False, "Message": "Needs LLM Type and Key"}:
        from .llm_config import llm_config
        llm_config()
    else:
       # st.title("Bot Configuration")
        st.write("Here you can see the details of your bots, and you can deploy them to Slack. To create or remove bots, ask your Eve bot to do it for you in chat.")
        bot_details.sort(key=lambda x: (not "Eve" in x["bot_name"], x["bot_name"]))

        slack_tokens = get_slack_tokens()
        slack_ready = slack_tokens.get("SlackActiveFlag", False)

        llm_info = get_metadata("llm_info")
        if len(llm_info) > 0:
            current_llm = [llm["llm_type"] for llm in llm_info if llm["active"]][0]
        else:
            current_llm = 'unknown'

        if bot_details:
            try:
                for bot in bot_details:
                    st.subheader(bot["bot_name"])
                    col1, col2 = st.columns(2)
                    with col1:
                        st.caption("Bot ID: " + bot["bot_id"])
                        available_tools = bot["available_tools"].strip("[]").replace('"', "").replace("'", "")
                        st.caption(f"Available Tools: {available_tools}")
                        bot_llms = get_metadata("bot_llms")
                        preferred_llm = None     # Initialize preferred_llm
                        if len(bot_llms) > 0:
                            for bot_id, llm_info in bot_llms.items():
                                if bot_id == bot["bot_id"]:
                                    current_llm = llm_info.get('current_llm')
                                    preferred_llm = llm_info.get('preferred_llm')
                        if preferred_llm:
                            st.caption(f"Preferred LLM Engine: {preferred_llm} (current LLM engine: {current_llm})")
                        else:
                            st.caption(f"Current LLM Engine: {current_llm} (default)")

                        bot_files = bot.get("files", None)
                        if bot_files == "null" or bot_files == "" or bot_files == "[]":
                            bot_files = None
                        if bot_files is not None:
                            st.caption(f"Files: {bot_files}")
                        else:
                            st.caption("Files: None assigned")
                        user_id = bot.get("bot_slack_user_id", "None")
                        if user_id is None:
                            user_id = "None"
                        api_app_id = bot.get("api_app_id", "None")
                        if api_app_id is None:
                            api_app_id = "None"
                        st.caption("Slack User ID: " + user_id)
                        st.caption("API App ID: " + api_app_id)

                        if user_id == "Pending_APP_LEVEL_TOKEN":
                            st.markdown(
                                f"**To complete the setup on Slack for this bot, there are two more steps, first is to go to: https://api.slack.com/apps/{api_app_id}/general, scroll to App Level Tokens, add a token called 'app_token' with scope 'connections-write', and provide the results in the box below.**"
                            )
                            slack_app_level_key = st.text_input(
                                "Enter Slack App Level Key",
                                key=f"slack_key_{bot['bot_id']}",
                            )
                            if st.button(
                                "Submit Slack App Level Key",
                                key=f"submit_{bot['bot_id']}",
                            ):
                                provide_slack_level_key_response = provide_slack_level_key(
                                    bot["bot_id"], slack_app_level_key
                                )
                                if provide_slack_level_key_response.get("success", False):
                                    st.success("Slack App Level Key provided successfully.")
                                    st.markdown(
                                        f"**To complete setup on Slack, there is one more step. Cut and paste this link in a new browser window (apologies that it can't be clickable here):**"
                                    )
                                    a = st.text_area(
                                        "Link to use to authorize:",
                                        value=bot["auth_url"],
                                        height=200,
                                        disabled=True,
                                    )
                                    st.markdown(
                                        f"**You may need to log into both Slack and Snowflake to complete this process. NOTE: Once the bot is deployed, all users on Slack will be able to access it. To limit access to certain users, tell Eve you'd like to do that once the bot is deployed to Slack and she will walk you through the process.**"
                                    )
                                else:
                                    st.error(
                                        f"Failed to provide Slack App Level Key: {provide_slack_level_key_response.get('error')}"
                                    )

                        if (
                            bot["auth_url"] is not None
                            and bot["slack_deployed"] is False
                            and user_id != "Pending_APP_LEVEL_TOKEN"
                        ):
                            st.markdown(
                                f"**To complete setup on Slack, there is one more step. Cut and paste this link in a new browser window (apologies that it can't be clickable here). You may need to log into both Slack and Snowflake to complete this process:**"
                            )
                            st.text_area(
                                "Link to use to authorize:",
                                value=bot["auth_url"],
                                height=200,
                                disabled=True,
                            )
                            st.markdown(
                                f"**Once you do that, you should see a Successfully Deployed message. Then go to Slack's Apps area at the bottom of the left-hand channel list panel, and press Add App, and search for the new bot by name. **"
                            )

                        if bot["slack_active"] == "N" or bot["slack_deployed"] == False:
                            if slack_ready and (
                                bot["auth_url"] is None or bot["auth_url"] == ""
                            ):
                                if st.button(
                                    f"Deploy {bot['bot_name']} on Slack",
                                    key=f"deploy_{bot['bot_id']}",
                                ):
                                    deploy_response = deploy_bot(bot["bot_id"])
                                    if deploy_response.get("Success") or deploy_response.get("success"):
                                        st.success(
                                            f"The first of 3 steps to deploy {bot.get('bot_name')} to Slack is complete. Press the button below to see the next 2 steps to complete deployment to Slack. "
                                        )
                                        if st.button("Press for Next Steps", key=f"refresh_{bot['bot_id']}"):
                                            st.rerun()
                                    else:
                                        st.error(f"Failed to deploy {bot['bot_name']} to Slack: {deploy_response.get('Message')}")
                            else:
                                if slack_ready is False:
                                    if "radio" in st.session_state:
                                        if st.session_state["radio"] != "Setup Slack Connection":
                                            if st.button(
                                                "Activate Slack Keys Here",
                                                key=f"activate_{bot['bot_id']}",
                                            ):
                                                st.session_state["radio"] = "Setup Slack Connection"
                                                st.rerun()
                                        else:
                                            st.markdown("###### Activate on Slack by clicking the Setup Slack Connection radio button")
                                    else:
                                        if st.button("Activate Slack Keys Here", key=f"activate_{bot['bot_id']}"):
                                            st.session_state["radio"] = "Setup Slack Connection"
                                            st.rerun()

                    with col2:
                        st.caption(
                            "UDF Active: "
                            + ("Yes" if bot["udf_active"] == "Y" else "No")
                        )
                        slack_user_allow = bot.get("slack_user_allow", None)
                        if slack_user_allow is not None:
                            allowed_users = slack_user_allow.strip("[]").replace('"', "").replace("'", "")
                            if allowed_users is not None:
                                if "!BLOCK_ALL" in allowed_users:
                                    st.caption(f"Allowed Slack Users: None - All Blocked")
                                else:
                                    st.caption(f"Allowed Slack Users IDs: {allowed_users} (Eve can tell you who these are)")
                            else:
                                st.caption("Allowed Slack Users: All Users Allowed")
                        elif bot["slack_active"] == "Y":
                            st.caption("Allowed Slack Users: All Users Allowed")
                        else:
                            st.caption("Allowed Slack Users: N/A")
                        st.caption(
                            "Slack Active: "
                            + ("Yes" if bot["slack_active"] == "Y" else "No")
                        )
                        st.caption(
                            "Slack Deployed: "
                            + ("Yes" if bot["slack_deployed"] else "No")
                        )
                        st.text_area(
                            label="Instructions",
                            value=bot["bot_instructions"],
                            height=100,
                            key=bot["bot_id"],
                        )

            except ValueError as e:
                st.error(f"Failed to parse bot details: {e}")
        else:
            st.write("No bot details available.")