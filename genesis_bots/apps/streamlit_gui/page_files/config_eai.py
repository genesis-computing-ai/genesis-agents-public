import streamlit as st
import pandas as pd
from utils import (
    check_eai_assigned,
    get_metadata,
    upgrade_services,
    set_metadata,
)
import json
from .components import config_page_header

# Define constant for EAI reference name
EAI_REFERENCE_NAME = "genesis_external_access"

def assign_eai_to_genesis():
    eai_type = 'CUSTOM'
    upgrade_result = upgrade_services(eai_type, EAI_REFERENCE_NAME)
    if upgrade_result:
        st.success(f"Genesis Bots upgrade result: {upgrade_result}")
        st.session_state.update({
            "eai_generated": False,
        })
        st.rerun()
    else:
        st.error("Upgrade services failed to return a valid response.")


def config_eai():
    config_page_header("Setup Endpoints")
    st.title('Endpoint Management')

    # Initialize session state
    if "eai_generated" not in st.session_state:
        st.session_state["eai_generated"] = False
    if "disable_assign" not in st.session_state:
        st.session_state["disable_assign"] = False

    # Add Custom Endpoint Section
    st.header('Add Custom Endpoint')
    with st.form(key='custom_endpoint_form'):
        group_name = st.text_input('Group Name').replace(' ', '_')
        endpoint = st.text_input('Endpoint').replace(' ', '')
        submit_button = st.form_submit_button(label='Add Custom Endpoint')
    
    if submit_button:
        if group_name and endpoint:
            result = set_metadata(f"set_endpoint {group_name} {endpoint} CUSTOM")
            if result and result[0].get('Success'):
                st.success('Custom endpoint added successfully!')
                st.session_state["eai_generated"] = False
        else:
            st.error('Please provide both Group Name and Endpoint.')

    st.divider()  # Add visual separation

    # Get currently configured endpoints
    configured_endpoints = get_metadata("get_configured_endpoints")

    # Define available services and their endpoints
    services = {
        'AZURE': {
            'description': 'Azure OpenAI and related services',
            'endpoints': get_metadata("get_endpoints AZURE")
        },
        'GOOGLE': {
            'description': 'Google API services including Sheets',
            'endpoints': [
                'accounts.google.com',
                'oauth2.googleapis.com',
                'www.googleapis.com',
                'googleapis.com',
                'sheets.googleapis.com'
            ]
        },
        'SERPER': {
            'description': 'Serper.dev search API',
            'endpoints': ['google.serper.dev', 'scrape.serper.dev']
        },
        'DBTCLOUD': {
            'description': 'dbt Cloud integration',
            'endpoints': get_metadata("get_endpoints DBTCLOUD")
        },
        'JIRA': {
            'description': 'Jira integration',
            'endpoints': get_metadata("get_endpoints JIRA")
        },
        'OPENAI': {
            'description': 'OpenAI API services',
            'endpoints': [
                'api.openai.com',
                'oaidalleapiprodscus.blob.core.windows.net'
            ]
        },
        'SLACK': {
            'description': 'Slack integration',
            'endpoints': [
                'slack.com',
                'www.slack.com',
                'wss-primary.slack.com',
                'wss-backup.slack.com',
                'slack-files.com',
                'downloads.slack-edge.com',
                'files.slack.com'
            ]
        }
    }

    # Create a list to hold table data
    table_data = []
    
    # Prepare data for the table
    for service, config in services.items():
        is_configured = any(endpoint['type'] == service for endpoint in configured_endpoints)
        endpoints_str = '\n'.join(config['endpoints']) if config['endpoints'] else 'No endpoints configured'
        
        table_data.append({
            "Service": service,
            "Description": config['description'],
            "Endpoints": endpoints_str,
            "Status": "Configured" if is_configured else "Not Configured",
            "Action": service  # We'll use this to create unique button keys
        })
    
    # Convert to DataFrame for display
    df_services = pd.DataFrame(table_data)
    
    # Display table
    st.header('Available Services')
    st.write("Select which services to enable for your Genesis bot")
    
    # Create two columns - one for the table, one for the buttons
    col1, col2 = st.columns([4, 1])
    
    with col1:
        # Display all columns except Action
        st.dataframe(
            df_services[["Service", "Description", "Endpoints", "Status"]],
            hide_index=True,
            column_config={
                "Service": st.column_config.TextColumn("Service", width="medium"),
                "Description": st.column_config.TextColumn("Description", width="medium"),
                "Endpoints": st.column_config.TextColumn("Endpoints", width="large"),
                "Status": st.column_config.TextColumn("Status", width="small"),
            }
        )
    
    with col2:
        st.write("")  # Add some spacing
        st.write("")  # Add some spacing
        for idx, row in df_services.iterrows():
            is_configured = row["Status"] == "Configured"
            if not is_configured:
                if st.button(f"Add", key=f"add_{row['Action']}"):
                    service = row['Action']
                    if services[service]['endpoints']:
                        result = set_metadata(f"set_standard_endpoints {service}")
                        if result and result[0].get('Success'):
                            st.success(f'{service} endpoints configured successfully!')
                            st.session_state["eai_generated"] = False
                            st.rerun()
                    else:
                        st.error(f"Please configure {service} settings first")
            else:
                if st.button(f"Remove", key=f"remove_{row['Action']}"):
                    service = row['Action']
                    result = set_metadata(f"remove_standard_endpoints {service}")
                    if result and result[0].get('Success'):
                        st.success(f'{service} endpoints removed successfully!')
                        st.session_state["eai_generated"] = False
                        st.rerun()

    # "Generate EAI" Button
    if st.button('Generate EAI'):
        st.session_state['eai_generated'] = True
        try:
            import snowflake.permissions as permissions
            permissions.request_reference(EAI_REFERENCE_NAME)  # Use direct string
        except Exception as e:
            st.error(f"Failed to request reference: {e}")

    # "Assign to Genesis" Button
    if st.session_state['eai_generated']:
        st.success('EAI generated successfully!')
        try:
            # Use direct string for check
            if check_eai_assigned(EAI_REFERENCE_NAME):
                st.session_state.disable_assign = True
            else:
                st.session_state.disable_assign = False
            
            if st.session_state.disable_assign == False:
                if st.button('Assign to Genesis'):
                    assign_eai_to_genesis()
                    st.success('Services updated successfully!')
        except Exception as e:
            st.error(f"Failed to check EAI assignment: {e}")

    # Get endpoint data before Delete Group section
    endpoint_data = get_metadata("get_endpoints ALL")
    df = pd.DataFrame(endpoint_data)

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

def handle_custom_endpoints():
    st.header('Add Custom Endpoint')
    
    with st.form(key='custom_endpoint_form'):
        group_name = st.text_input('Group Name').replace(' ', '_')
        endpoint = st.text_input('Endpoint').replace(' ', '')
        submit_button = st.form_submit_button(label='Add Endpoint')
    
    if submit_button:
        if group_name and endpoint:
            result = set_metadata(f"set_endpoint {group_name} {endpoint} CUSTOM")
            if result and result[0].get('Success'):
                st.success('Custom endpoint added successfully!')
                # Reset EAI generated flag to require regeneration
                st.session_state["eai_generated"] = False
        else:
            st.error('Please provide both Group Name and Endpoint.')

def handle_standard_endpoints():
    st.header('Standard Endpoints')
    
    # Initialize session state for EAI
    if "eai_generated" not in st.session_state:
        st.session_state["eai_generated"] = False
    if "disable_assign" not in st.session_state:
        st.session_state["disable_assign"] = False
    
    azure_endpoints = get_metadata("get_endpoints AZURE")
    custom_endpoints = get_metadata("get_endpoints CUSTOM")
    jira_endpoints = get_metadata("get_endpoints JIRA")
    dbtcloud_endpoints = get_metadata("get_endpoints DBTCLOUD")
    # Get currently configured endpoints
    configured_endpoints = get_metadata("get_configured_endpoints")
    
    # Display available standard endpoints by service
    services = {
        'AZURE': {
            'endpoints': azure_endpoints
        },
        'GOOGLE': {
            'endpoints': [
                'accounts.google.com',
                'oauth2.googleapis.com',
                'www.googleapis.com',
                'googleapis.com',
                'sheets.googleapis.com'
            ]
        },
        'SERPER': {
            'endpoints': ['google.serper.dev', 'scrape.serper.dev']
        },
        'CUSTOM': {
            'endpoints': custom_endpoints
        },
        'DBTCLOUD': {
            'endpoints': dbtcloud_endpoints
        },
        'JIRA': {
            'endpoints': jira_endpoints
        },
        'OPENAI': {
            'endpoints': [
                'api.openai.com',
                'oaidalleapiprodscus.blob.core.windows.net'
            ]
        },
        'SLACK': {
            'endpoints': [
                'slack.com',
                'www.slack.com',
                'wss-primary.slack.com',
                'wss-backup.slack.com',
                'wss-primary.slack.com',
                'wss-backup.slack.com',
                'slack-files.com',
                'downloads.slack-edge.com',
                'files-edge.slack.com',
                'files-origin.slack.com',
                'files.slack.com',
                'global-upload-edge.slack.com',
                'universal-upload-edge.slack.com'
            ]
        }
    }
    
    for service, config in services.items():
        with st.expander(f"{service} Endpoints"):
            # Check if service is already configured
            is_configured = any(endpoint['type'] == service for endpoint in configured_endpoints)
            
            if not is_configured:
                if st.button(f"Add {service} Endpoints", key=f"add_{service}"):
                    if not config['endpoints']:
                        st.error("Please go to the configuration page for this service to enter the required data")
                    else:
                        result = set_metadata(f"set_standard_endpoints {service}")
                        if result and result[0].get('Success'):
                            st.success(f'{service} endpoints configured successfully!')
                            st.session_state["eai_generated"] = False
                            st.rerun()
            else:
                if st.button(f"Remove {service} Endpoints", key=f"remove_{service}"):
                    result = set_metadata(f"remove_standard_endpoints {service}")
                    if result and result[0].get('Success'):
                        st.success(f'{service} endpoints removed successfully!')
                        st.session_state["eai_generated"] = False
                        st.rerun()

def handle_eai_generation():
    st.header('EAI Configuration')
    
    # Display current endpoints
    st.subheader('Configured Endpoints')
    endpoint_data = get_metadata("get_endpoints")
    if endpoint_data:
        df = pd.DataFrame(endpoint_data)
        column_mapping = {
            "group_name": "Group",
            "endpoints": "Endpoints",
            "type": "Type"
        }
        st.dataframe(df.rename(columns=column_mapping), hide_index=True)
    
    # Generate EAI button
    if not st.session_state.get("eai_generated", False):
        if st.button('Generate EAI'):
            try:
                import snowflake.permissions as permissions
                permissions.request_reference(EAI_REFERENCE_NAME)
                st.session_state["eai_generated"] = True
                st.success('EAI generated successfully!')
                st.rerun()
            except Exception as e:
                st.error(f"Failed to generate EAI: {e}")
    
    # Assign to Genesis button (only shown once and if EAI is generated)
    if st.session_state.get("eai_generated", False):
        try:
            if not check_eai_assigned(EAI_REFERENCE_NAME):
                if st.button('Assign to Genesis'):
                    upgrade_result = upgrade_services('CUSTOM', EAI_REFERENCE_NAME)
                    if upgrade_result:
                        st.success('Services updated successfully!')
                        st.session_state["disable_assign"] = True
                        st.rerun()
                    else:
                        st.error("Upgrade services failed to return a valid response.")
            else:
                st.session_state["disable_assign"] = True
                st.info("EAI is already assigned to Genesis")
        except Exception as e:
            st.error(f"Failed to check EAI assignment: {e}")