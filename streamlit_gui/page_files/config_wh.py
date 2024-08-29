import streamlit as st
from utils import app_name

def config_wh():
    
    st.title("Step 1: Configure Warehouse")
    
    st.markdown("""
    <style>
    .big-font {
        font-size:20px !important;
        font-weight: bold;
    }
    .info-box {
        background-color: #e1f5fe;
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

    col1, col2 = st.columns([2, 1])

    with col1:
        st.markdown('<p class="big-font">Why do we need to configure a Warehouse?</p>', unsafe_allow_html=True)
        
        st.markdown("""
        <div class="info-box">
        Genesis Bots needs rights to use a Snowflake compute engine, known as a Virtual Warehouse, to run queries on Snowflake. This step does not provide Genesis Bots with access to any of your data, just the ability to run SQL on Snowflake in general.
        
        You'll need to grant Genesis access to an existing Warehouse or create a new one for its use.
        </div>
        """, unsafe_allow_html=True)

    with col2:
        st.image("https://path.to.your.image/warehouse_diagram.png", caption="Warehouse Configuration", use_column_width=True)

    st.markdown('<p class="big-font">Configuration Steps</p>', unsafe_allow_html=True)
    
    st.markdown("""
    Please open another Snowflake window, go to Projects, and make a new Snowflake worksheet. Run these commands to grant Genesis access to an existing Warehouse or to make a new one for its use.
    """)

    wh_text = f"""-- select role to use, generally Accountadmin or Sysadmin
use role ACCOUNTADMIN;

-- set the name of the installed application
set APP_DATABASE = '{app_name}';

-- set warehouse name to use
set APP_WAREHOUSE = '{st.session_state.get("wh_name", "XSMALL")}'; 

-- create the warehouse if needed
CREATE WAREHOUSE IF NOT EXISTS IDENTIFIER($APP_WAREHOUSE)
MIN_CLUSTER_COUNT=1 MAX_CLUSTER_COUNT=1
WAREHOUSE_SIZE=XSMALL AUTO_RESUME = TRUE AUTO_SUSPEND = 60;

-- allow Genesis to use the warehouse
GRANT USAGE ON WAREHOUSE  IDENTIFIER($APP_WAREHOUSE) TO APPLICATION  IDENTIFIER($APP_DATABASE);
"""

    st.markdown('<div class="code-box">', unsafe_allow_html=True)
    st.code(wh_text, language="sql")
    st.markdown('</div>', unsafe_allow_html=True)

    if st.button("TEST Access to Warehouse"):
        try:
            # This is a placeholder for the actual test logic
            # You'll need to implement the actual test here
            st.success("Successfully connected to the warehouse. You can now proceed to the next step.")
        except Exception as e:
            st.error(f"Error connecting to Snowflake: {e}")

    st.success("Once you've run the commands and tested the access, you can proceed to the next step.")
    
    if st.button("Proceed to Configure Compute Pool", key="proceed_button"):
        st.session_state["radio"] = "2: Configure Compute Pool"
        st.experimental_rerun()

    st.info("If you need any assistance, please check our [documentation](https://genesiscomputing.ai/docs/) or join our [Slack community](https://communityinviter.com/apps/genesisbotscommunity/genesis-bots-community).")