import streamlit as st

from utils import get_session
import time
import pandas as pd

def start_service():
    session = get_session()
    if not session:
        st.error("Unable to connect to Snowflake. Please check your connection.")
        return

    st.subheader("Step 4: Start Genesis Server")

    st.write(
        "If you've performed the 3 other steps, you're ready to start the Genesis Server. Press the START SERVER button below to get started."
    )

    if st.button("Start Genesis Server"):
        try:
            # Check for previous installations
            # Get core_prefix from session state
            core_prefix = st.session_state.get('core_prefix')
            if not core_prefix:
                st.error("Core prefix not found in session state. Please ensure you've completed the previous setup steps.")
                return
            st.write("Checking for previous installations...")
            try:
                drop_result = session.sql(
                    f"call {core_prefix}.drop_app_instance('APP1')"
                )
                if drop_result:
                    st.write(drop_result)
            except Exception as e:
                pass

            # Check virtual warehouse
            wh_test = False
            try:
                st.write("Checking virtual warehouse...")
                warehouses_result = session.sql("SHOW WAREHOUSES").collect()
                if warehouses_result:
                    warehouses_df = pd.DataFrame(
                        [row.as_dict() for row in warehouses_result]
                    )
                    warehouse_names = warehouses_df["name"].tolist()
                    if 'SYSTEM$STREAMLIT_NOTEBOOK_WH' in warehouse_names:
                        warehouse_names.remove('SYSTEM$STREAMLIT_NOTEBOOK_WH')

                    if st.session_state.wh_name not in warehouse_names:
                        st.session_state.wh_name = warehouse_names[0]
                    st.write(f"Found warehouse {st.session_state.wh_name}.")
                    wh_test = True
            except Exception as e:
                st.write(e)
                st.write(f"No warehouses assigned to app. Please check Step 1 on left.")

            # Start Compute Pool & Genesis Server
            if wh_test:
                try:
                    st.write(
                        'Starting Compute Pool & Genesis Server (can take 3-15 minutes the first time for compute pool startup, use "show compute pools;" to see status)...'
                    )
                    with st.spinner("..."):
                        start_result = session.sql(
                            f"call {core_prefix}.INITIALIZE_APP_INSTANCE('APP1','GENESIS_POOL','{st.session_state.wh_name}')"
                        )
                        st.write(start_result)
                except Exception as e:
                    st.write(e)

            # Check if services were returned
            if wh_test and start_result:
                st.success(f"Success: Server Started")

                st.write(
                    "**Now push the button below, youre ready to start chatting with your bots!**"
                )
                if st.button("Chat with your bots!", key='gotochat'):
                    st.rerun()
            else:
                st.error("Server not started.")
        except Exception as e:
            st.error(f"Error connecting to Snowflake: {e}")

    # Add a section to check the status of the Genesis Service
 #   st.subheader("Genesis Service Status")
 #   if st.button("Check Genesis Service Status"):
 #       try:
 #           # Get app name and prefix from session state
 #           app_name = st.session_state.get('app_name', '')
 #           prefix = st.session_state.get('prefix', '')

#            status_query = f"select v.value:status::varchar status from (select parse_json(system$get_service_status('{prefix}.GENESISAPP_SERVICE_SERVICE'))) t, lateral flatten(input => t.$1) v"
#            service_status_result = session.sql(status_query).collect()
#            status = service_status_result[0][0]
#            st.write(f"Genesis Service status: {status}")

 #           if status == "SUSPENDED":
 #               if st.button("Start Genesis Service"):
 #                   start_command = f"call {app_name}.core.start_app_instance('APP1','GENESIS_POOL','GENESIS_EAI','{st.session_state.wh_name}')"
 #                   session.sql(start_command).collect()
 #                   st.success("Genesis Service start command executed. Please check the status again in a few moments.")
 #           elif status == "READY":
 #               st.success("Genesis Service is running and ready.")
 #           else:
 #               st.info(f"Genesis Service status: {status}. Please wait or check the configuration if it's not becoming READY.")
 #       except Exception as e:
 #           st.error(f"Error checking Genesis Service status: {e}")

# st.session_state.app_name = "GENESIS_BOTS"
# st.session_state.prefix = st.session_state.app_name + ".app1"
# st.session_state.core_prefix = st.session_state.app_name + ".CORE"
# st.session_state["wh_name"] = "XSMSALL"
# import pandas as pd
# from snowflake.snowpark.context import get_active_session
# session = get_active_session()
# start_service()
