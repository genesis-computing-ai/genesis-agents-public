import streamlit as st
from utils import ( get_session, upgrade_services, check_eai_status, get_references)
import pandas as pd
from .components import config_page_header

def config_wh():
    config_page_header("Setup Custom Warehouse")

    session = get_session()
    if not session:
        st.error("Unable to connect to Snowflake. Please check your connection.")
        # return
        pass
    st.title("Configure Warehouse")

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

    st.markdown('<p class="big-font">Why do we need to configure a Warehouse?</p>', unsafe_allow_html=True)

    st.markdown("""
    <div class="info-box">
    Genesis Bots needs rights to use a Snowflake compute engine, known as a Virtual Warehouse, to run queries on Snowflake. By default, Genesis Bots created a Virtual Warehouse for use by the application.
    However, you can create your own custom warehouse or assign an existing warehouse for use by the application.
    This step does not provide Genesis Bots with access to any of your data, just the ability to run SQL on Snowflake in general.
    You'll need to grant Genesis access to an existing Warehouse or create a new one for its use.
    </div>
    """, unsafe_allow_html=True)

    st.markdown('<p class="big-font">Configuration Steps</p>', unsafe_allow_html=True)

    st.markdown("""
    Please open another Snowflake window/tab, go to Projects, and make a new Snowflake worksheet. Run these commands to grant Genesis access to an existing Warehouse or to make a new one for its use.
    """)

    wh_text = f"""-- select authorized role to use

-- set the name of the installed application
set APP_DATABASE = '{st.session_state.get("app_name", "")}';

-- set warehouse name to use
set APP_WAREHOUSE = '{st.session_state.get("wh_name", "XSMALL")}';

-- create the warehouse if needed
CREATE WAREHOUSE IF NOT EXISTS IDENTIFIER($APP_WAREHOUSE)
MIN_CLUSTER_COUNT=1 MAX_CLUSTER_COUNT=1
WAREHOUSE_SIZE=XSMALL AUTO_RESUME = TRUE AUTO_SUSPEND = 60;

-- allow Genesis to use the warehouse
GRANT USAGE ON WAREHOUSE  IDENTIFIER($APP_WAREHOUSE) TO APPLICATION  IDENTIFIER($APP_DATABASE);
"""

#    st.markdown('<div class="code-box">', unsafe_allow_html=True)
    st.code(wh_text, language="sql")
#    st.markdown('</div>', unsafe_allow_html=True)

    if st.button("TEST Access to Warehouse"):
        try:
            # Execute the command and collect the results
            warehouses_result = session.sql("SHOW WAREHOUSES").collect()

            # Check if any warehouses were returned
            if warehouses_result:
                # Convert the list of Row objects to a Pandas DataFrame for display
                warehouses_df = pd.DataFrame(
                    [row.as_dict() for row in warehouses_result]
                )
                warehouse_names = warehouses_df[
                    "name"
                ].tolist()  # Adjust 'name' if necessary to match your column name

                if 'SYSTEM$STREAMLIT_NOTEBOOK_WH' in warehouse_names:
                    warehouse_names.remove('SYSTEM$STREAMLIT_NOTEBOOK_WH')
                if 'APP_XSMALL' in warehouse_names:
                    warehouse_names.remove('APP_XSMALL')
                if 'APP_XSMALL_1' in warehouse_names:
                    warehouse_names.remove('APP_XSMALL_1')

                # Check if 'XSMALL' is in the list of warehouse names
                if st.session_state.wh_name not in warehouse_names:
                    # Notify the user about the naming discrepancy and suggest setting APP_WAREHOUSE
                    st.session_state.wh_name = warehouse_names[0]

                # Display success message with list of warehouses
                st.success(
                    f'Success: Found the following warehouses - {", ".join(warehouse_names)}, Thanks!'
                )
            else:
                st.error(
                    'Error: No warehouses found.  Please open a new worksheet, copy and paste the commands above, and run them.  Then return here and press "TEST Access to Warehouse" above.'
                )
        except Exception as e:
            st.error(f"Error connecting to Snowflake: {e}")

    st.write("Click the button to assign the warehouse to the Genesis Bots services. This will restart your service and takes 3-5 minutes to complete.")
    if st.button("Assign Warehouse to Genesis", key="upgrade_button_app"):
        try:
            upgrade_result = upgrade_services()
            st.success(f"Genesis Bots upgrade result: {upgrade_result}")
        except Exception as e:
            st.error(f"Error upgrading services: {e}")


    st.info("If you need any assistance, please check our [documentation](https://genesiscomputing.ai/docs/) or join our [Slack community](https://communityinviter.com/apps/genesisbotscommunity/genesis-bots-community).")


# st.session_state.app_name = "GENESIS_BOTS"
# st.session_state.prefix = st.session_state.app_name + ".app1"
# st.session_state.core_prefix = st.session_state.app_name + ".CORE"
# st.session_state["wh_name"] = "XSMSALL"
# import pandas as pd
# from snowflake.snowpark.context import get_active_session
# session = get_active_session()
# config_wh()
