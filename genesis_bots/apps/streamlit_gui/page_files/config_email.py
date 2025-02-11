import streamlit as st
from utils import (
     get_session, get_metadata
)
import pandas as pd
from .components import config_page_header

def setup_email():
    config_page_header("Setup Email Integration")

    # local=False
    # session = get_session()
    # if not session:
    #     local = True

    st.title("Configure Email")

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

    st.markdown('<p class="big-font">Why do we need to configure Email?</p>', unsafe_allow_html=True)

    st.markdown("""
    Genesis Bots can send you email when they run autonomous tasks for you.  For example, Janice can email you when she identifies any concerns with the security of your Snowflake account, or if she has ideas for Snowflake cost savings or performance improvement opportunities.
    """, unsafe_allow_html=True)

    st.markdown('<p class="big-font">Email Configuration Steps</p>', unsafe_allow_html=True)

    st.markdown("""
    1. Please open the Snowflake Web UI in a new window/tab in your browser (this should be an additional window/tab to the one where you're following these instructions).
    2. If an email address has not yet been added and validated for your user profile, go to the menu at the lower left hand corner of the Snowflake UI (this should have your name and role and an arrow pointing up), click on this to expand the menu, select the "My profile" item, and, in the "Profile" window that pops up, make sure that the "Email" field is populated and it does NOT have a message asking you to validate the email. If there is no email, add one. If you do see the message asking you to validate the email, then follow the steps in the [Snowflake documentation](https://docs.snowflake.com/en/user-guide/ui-snowsight-profile#verify-your-email-address).
    3. Next, go to Projects and make a new Snowflake worksheet.
    4. Copy and paste the below SQL into the worksheet. Run these commands to set up an email integration and grant Genesis access to use it to send you emails. This will also test that Snowflake has validated your email and can send you email, and will also set your email as the default for notifications from the system. Note: Genesis can send a message to any validated email that has been added to a Snowflake user profile.
    """)

    try:
        user_email = st.experimental_user.email
    except:
        user_email = '<your_email>@<your_domain>.com'

    wh_text = f"""-- use an authorized role

create notification integration genesis_email_int type=EMAIL enabled=TRUE;
grant usage on integration genesis_email_int to application {st.session_state.get("app_name", "")};
// Please ensure the email address is in your Snowflake user profile and has been verified by Snowflake
call SYSTEM$SEND_EMAIL('genesis_email_int','{user_email}','Testing Email','This is a test.');
call {st.session_state.get("app_name", "")}.core.set_default_email('{user_email}');
"""

#    st.markdown('<div class="code-box">', unsafe_allow_html=True)
    st.code(wh_text, language="sql")
#    st.markdown('</div>', unsafe_allow_html=True)
    # Add a text input for the user's email
    user_email = st.text_input("Your email address:", value=user_email)


    if st.button("TEST Email Integration from Genesis Server"):
        try:
            # Execute the command and collect the results
            email_result = get_metadata('test_email '+user_email)
            if isinstance(email_result, list) and len(email_result) > 0:
                if 'system$send_email' in email_result[0] and email_result[0]['system$send_email']==True:
                    st.success("Email Setup Successful")
                    st.success("You can now use the dropdown on the left to chat with the bots")
            elif isinstance(email_result, dict) and 'Either these email addresses are not yet validated ' in email_result.get('Message', ''):
                st.error(f'Could not send email to {user_email} as Snowflake has not verified that email address. Please follow [this process](https://docs.snowflake.com/en/user-guide/ui-snowsight-profile#verify-your-email-address) to verify your email on Snowflake.')
            elif 'does not exist or not authorized' in email_result['Message']:
                st.error(f'Email integration genesis_email_int was not available to Genesis App.  Please double-check that you have run the commands in the box above in a Snowflake worksheet with an authorized role.')
            else:
                st.error(email_result)
        except Exception as e:
            st.error(f"Error testing Email on Snowflake: {e}")


    st.info("If you need any assistance, please check our [documentation](https://genesiscomputing.ai/docs/) or join our [Slack community](https://communityinviter.com/apps/genesisbotscommunity/genesis-bots-community).")


# st.session_state.app_name = "GENESIS_BOTS"
# st.session_state.prefix = st.session_state.app_name + ".app1"
# st.session_state.core_prefix = st.session_state.app_name + ".CORE"
# st.session_state["wh_name"] = "XSMSALL"
# import pandas as pd
# from snowflake.snowpark.context import get_active_session
# session = get_active_session()
# config_wh()
