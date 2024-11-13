import streamlit as st
import pandas as pd
from utils import (
    get_metadata,
    upgrade_services,
)
import json

def check_eai_assigned():
    eai_data = get_metadata("check_eai_assigned")
    eai_str = eai_data[0]['eai_list']
    if eai_str and 'CUSTOM_EXTERNAL_ACCESS' in eai_str:
        st.session_state.disable_assign = True
    else:
        st.session_state.disable_assign = False

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
    st.title('Custom Endpoints Management')

    # Initialize session state
    if 'eai_generated' not in st.session_state:
        st.session_state['eai_generated'] = False
    if 'disable_assign' not in st.session_state:
        st.session_state['disable_assign'] = False

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
        import snowflake.permissions as permissions
        permissions.request_reference("custom_external_access")

    # "Assign to Genesis" Button
    if st.session_state['eai_generated']:
        st.success('EAI generated successfully!')
        check_eai_assigned()
        if st.session_state.disable_assign == False:
            if st.button('Assign to Genesis'):
                assign_eai_to_genesis()
                st.success('Services updated successfully!')

