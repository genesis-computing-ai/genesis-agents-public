import streamlit as st
import pandas as pd
from utils import (
    check_eai_assigned,
    get_metadata,
    upgrade_services,
)
import json
from .components import config_page_header

def assign_eai_to_genesis():
    eai_type = 'CUSTOM'
    upgrade_result = upgrade_services(eai_type, 'custom_external_access')
    if upgrade_result:
        st.success(f"Genesis Bots upgrade result: {upgrade_result}")
        st.session_state.update({
            "eai_generated": False,
        })
        st.rerun()
    else:
        st.error("Upgrade services failed to return a valid response.")


def config_custom_eai():
    config_page_header("Setup Custom Endpoints")
    st.title('Custom Endpoints Management')

    # Initialize session state - use direct assignment
    st.session_state["eai_generated"] = st.session_state.get("eai_generated", False)
    st.session_state["disable_assign"] = st.session_state.get("disable_assign", False)
    st.session_state["eai_reference_name"] = "custom_external_access"  # Always set correctly for this page

    # Form to add new endpoint
    st.header('Add a New Endpoint')

    with st.form(key='endpoint_form'):
        group_name = st.text_input('Group Name').replace(' ', '_')
        endpoint = st.text_input('Endpoint').replace(' ', '')
        submit_button = st.form_submit_button(label='Add Endpoint')

    if submit_button:
        if group_name and endpoint:
            set_endpoint = get_metadata(f"set_endpoint {group_name} {endpoint} CUSTOM")
            if set_endpoint and set_endpoint[0].get('Success'):
                st.success('Endpoint added successfully!')
        else:
            st.error('Please provide both Group Name and Endpoint.')

    # Display grouped endpoints
    st.header('Endpoints by Group')

    # return [("Group A", "endpoint1, endpoint2"), ("Group B", "endpoint3, endpoint4")]

    # Fetching the data
    endpoint_data = get_metadata("get_endpoints")

    # # Convert the list into a DataFrame
    df = pd.DataFrame(endpoint_data)

    # Display the DataFrame in Streamlit
    column_mapping = {
        "group_name": "Group Name",
        "endpoints": "Endpoints"
    }
    st.dataframe(df.rename(columns=column_mapping),hide_index=True, column_order=("Group Name","Endpoints"))

    # "Generate EAI" Button
    if st.button('Generate EAI'):
        st.session_state['eai_generated'] = True
        try:
            import snowflake.permissions as permissions
            permissions.request_reference("custom_external_access")  # Use direct string
        except Exception as e:
            st.error(f"Failed to request reference: {e}")

    # "Assign to Genesis" Button
    if st.session_state['eai_generated']:
        st.success('EAI generated successfully!')
        try:
            # Use direct string for check
            if check_eai_assigned('custom_external_access'):
                st.session_state.disable_assign = True
            else:
                st.session_state.disable_assign = False
            
            if st.session_state.disable_assign == False:
                if st.button('Assign to Genesis'):
                    assign_eai_to_genesis()
                    st.success('Services updated successfully!')
        except Exception as e:
            st.error(f"Failed to check EAI assignment: {e}")

    # Dropdown and "Delete Group" button
    st.header("Delete Group")
    group_names = df['group_name'].tolist() if 'group_name' in df.columns else []
    selected_group = st.selectbox("Select Group to Delete", group_names)

    if st.button("Delete Group"):
        if selected_group:
            delete_group = get_metadata(f"delete_endpoint_group {selected_group}")
            if delete_group and delete_group[0].get('Success'):
                st.success('Endpoint group deleted successfully!')
                st.success('Click Generate EAI above to remove the endpoint from your network rule')
            else:
                st.error('Error deleting group')