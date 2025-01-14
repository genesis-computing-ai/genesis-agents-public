import streamlit as st

def config_pool():
    
    st.title("Step 2: Configure Compute Pool")
    
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

  
    st.markdown('<p class="big-font">Why do we need a Compute Pool?</p>', unsafe_allow_html=True)
    
    st.markdown("""
    <div class="info-box">
    Genesis Bots has a server component that runs securely inside your Snowflake account, coordinating the actions of your Genesis Bots and managing their interactions with other users and bots. To run this server, you need to create and grant Genesis Server access to a Snowflake Compute Pool.
    
    This uses the Snowflake small compute pool, which costs about 0.22 Snowflake Credits per hour, or about $10/day. Once you start the server, you will be able to suspend it when not in use.
    </div>
    """, unsafe_allow_html=True)


    st.markdown('<p class="big-font">Configuration Steps</p>', unsafe_allow_html=True)
    
    st.markdown("""
    Please go back to your Snowflake worksheet and run these commands to create a new compute pool and grant Genesis the rights to use it.
    """)

    wh_text = f"""-- select authorized role to use

-- set the name of the installed application
set APP_DATABASE = '{st.session_state.get("app_name", "")}';

-- remove an existing pool, if you've installed this app before
DROP COMPUTE POOL IF EXISTS GENESIS_POOL;

-- create the compute pool and associate it to this application
CREATE COMPUTE POOL IF NOT EXISTS GENESIS_POOL FOR APPLICATION IDENTIFIER($APP_DATABASE)
MIN_NODES=1 MAX_NODES=1 INSTANCE_FAMILY='CPU_X64_S' AUTO_SUSPEND_SECS=3600 INITIALLY_SUSPENDED=FALSE;

-- give Genesis the right to use the compute pool
GRANT USAGE, OPERATE ON COMPUTE POOL GENESIS_POOL TO APPLICATION  IDENTIFIER($APP_DATABASE);
"""

    st.code(wh_text, language="sql")

    st.success("We can't automatically test this, but if you've performed it the same way you did on Step 1, you can now proceed to the next step.")
    
    if "proceed_button_eai_clicked" not in st.session_state:
        if st.button("Proceed to Configure EAI", key="proceed_button_eai"):
            st.session_state["radio"] = "3: Configure EAI"
            st.session_state["proceed_button_eai_clicked"] = True
            st.rerun()
    else:
        st.write("<<--- Use the selector on the left to select 3: Configure EAI")

    st.info("If you need any assistance, please check our [documentation](https://genesiscomputing.ai/docs/) or join our [Slack community](https://communityinviter.com/apps/genesisbotscommunity/genesis-bots-community).")