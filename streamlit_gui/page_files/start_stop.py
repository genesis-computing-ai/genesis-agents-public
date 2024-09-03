import streamlit as st
from utils import get_session

def start_stop():
    st.subheader("Start / Stop Genesis Server")

    session = get_session()
    if not session:
        st.error("Unable to connect to Snowflake. Please check your connection.")
        return

    try:
        warehouses_result = session.sql("SHOW WAREHOUSES").collect()
        if warehouses_result:
            warehouses_df = pd.DataFrame([row.as_dict() for row in warehouses_result])
            warehouse_names = warehouses_df["name"].tolist()
            if st.session_state.wh_name not in warehouse_names:
                st.session_state.wh_name = warehouse_names[0]
    except Exception as e:
        st.session_state.wh_name = "<your warehouse name>"

    st.write(
        "You can use the buttons to stop or start each service - or copy/paste the below commands to a worksheet to stop, start, and monitor the Genesis Server:"
    )
    
    start_stop_text = f"""USE DATABASE IDENTIFIER('{app_name}');

    // pause service

    call {app_name}.core.stop_app_instance('APP1');
    alter compute pool GENESIS_POOL SUSPEND; -- to also pause the compute pool

    // resume service

    alter compute pool GENESIS_POOL RESUME; -- if you paused the compute pool
    call {app_name}.core.start_app_instance('APP1','GENESIS_POOL','GENESIS_EAI','{st.session_state.wh_name}'); 

    """
    st.text_area("", start_stop_text, height=400)

    if st.button("Stop Genesis Server"):
        try:
            stop_result = session.sql(f"call {app_name}.core.stop_app_instance('APP1')")
            st.write(stop_result.collect())
            st.success("Genesis Server stopped successfully.")
        except Exception as e:
            st.error(f"Error stopping Genesis Server: {e}")

    if st.button("Start Genesis Server"):
        try:
            start_result = session.sql(f"call {app_name}.core.start_app_instance('APP1','GENESIS_POOL','GENESIS_EAI','{st.session_state.wh_name}')")
            st.write(start_result.collect())
            st.success("Genesis Server started successfully.")
        except Exception as e:
            st.error(f"Error starting Genesis Server: {e}")

    if st.button("Check Genesis Server Status"):
        try:
            status_query = f"select v.value:status::varchar status from (select parse_json(system$get_service_status('{app_name}.app1.GENESISAPP_SERVICE_SERVICE'))) t, lateral flatten(input => t.$1) v"
            status_result = session.sql(status_query).collect()
            status = status_result[0][0]
            st.write(f"Genesis Server status: {status}")
        except Exception as e:
            st.error(f"Error checking Genesis Server status: {e}")

    st.write("Note: It may take a few moments for the status to update after starting or stopping the server.")