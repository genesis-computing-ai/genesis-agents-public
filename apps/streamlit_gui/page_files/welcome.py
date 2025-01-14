import streamlit as st

def welcome():
    st.subheader("Welcome to Genesis Bots!")

    st.write(
        "Before you get started using Genesis Bots, you need to perform 4 steps to give your Genesis App access to things it needs in your Snowflake account. I'll walk you through the steps. They are:"
    )
    st.write(
        "1. Configure Warehouse -- gives Genesis the ability to run queries on Snowflake"
    )
    st.write(
        "2. Configure Compute Pool -- gives a Snowflake Container Services Compute Pool to use to run its bot server securely inside your Snowflake account"
    )
    st.write(
        "3. Configure EAI -- gives Genesis the ability to connect to external systems like OpenAI (required) and Slack (optional)"
    )
    st.write(
        "4. Start Genesis Server -- this starts up the Genesis Server inside the Compute Pool."
    )

    st.write()
    st.write(
        "After completing these steps, you'll be able to start talking to Genesis Bots, creating your own Bots, analyzing data with your Bots, and more!"
    )

    if "proceed_button_wh_clicked" not in st.session_state:
        if st.button("Proceed to Configure Warehouses", key="proceed_button_wh"):
            st.session_state["radio"] = "1: Configure Warehouse"
            st.session_state["proceed_button_wh_clicked"] = True
            st.rerun()
    else:
        st.write("<<--- Use the selector on the left to select 1: Configure Warehouse")