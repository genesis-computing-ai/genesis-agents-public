import streamlit as st
import time 
from utils import get_bot_details, get_metadata, configure_llm, check_eai_status, get_references, upgrade_services, get_bot_details


def llm_config():
    if "openai_eai_available" not in st.session_state:
        if st.session_state.NativeMode:
            st.session_state.openai_eai_available = False
        else:
            st.session_state.openai_eai_available = True
    if "azure_openai_eai_available" not in st.session_state:
        if st.session_state.NativeMode:
            st.session_state.azure_openai_eai_available = False
        else:
            st.session_state.azure_openai_eai_available = True
    if "set_endpoint" not in st.session_state:
        st.session_state.set_endpoint = False   
    if "eai_reference_name" not in st.session_state:
        st.session_state.eai_reference_name = None 
    if "assign_disabled" not in st.session_state:
        st.session_state.assign_disabled = True
    if "disable_create" not in st.session_state:
        st.session_state.disable_create = False
    if "disable_submit" not in st.session_state:
        st.session_state.disable_submit = True

    if st.session_state.openai_eai_available == False:
        # openai_ref = get_references("openai_external_access")
        openai_eai_status = False
        openai_eai_status = check_eai_status('openai')

        if openai_eai_status == True:
            st.session_state.openai_eai_available = openai_eai_status
            st.write(f"OpenAI External Access Integration available")
            st.session_state.eai_reference_name = 'openai_external_access'
            st.session_state.disable_submit = False

    if st.session_state.azure_openai_eai_available == False:
        azure_openai_eai_status = False
        azure_ref = get_references("azure_openai_external_access")
        if not azure_ref:
            st.session_state.azure_openai_eai_available = False
        # azure_openai_eai_status = check_eai_status('azureopenai')
        if azure_openai_eai_status == False:
            st.session_state.set_endpoint = False 
        else:
            st.session_state.set_endpoint = True
            st.session_state.azure_openai_eai_available = azure_openai_eai_status
            st.write(f"Azure OpenAI External Access Integration available")
            st.session_state.eai_reference_name = 'azure_openai_external_access'
            st.session_state.disable_submit = False


    get_bot_details().clear()
    bot_details = get_bot_details()
    
    llm_info = get_metadata("llm_info")
    llm_types = []
    active_llm_type = None

    # Create a dictionary for find-and-replace mapping to create user friendly names
    replace_map = {"openai": "OpenAI", "cortex": "Cortex"}
    # Iterate over llm_types and replace the values in the 'LLM Type' key
    for llm in llm_info:
        llm["llm_type"] = replace_map.get(llm["llm_type"], llm["llm_type"])

    if len(llm_info) > 0:
        # Check which llm_type has active = true
        active_llm_type = [llm["llm_type"] for llm in llm_info if llm["active"]]
        for llm in llm_info:
            active_marker = chr(10003) if llm["active"] else ""
            llm_types.append({"LLM Type": llm["llm_type"], "Active": active_marker})

    cur_key = ""

    if bot_details == {"Success": False, "Message": "Needs LLM Type and Key"}:
        st.success("Welcome! Before you chat with bots, please setup your LLM Keys")
        time.sleep(0.3)
        cur_key = ""

    st.header("LLM Model & API Key Setup")
    if cur_key == "" and active_llm_type is not None and len(active_llm_type) > 0:
        st.success(
            f"You already have an LLM active: **{active_llm_type[0]}**. If you want to change it, you can do so below."
        )

    st.write(
        "Genesis Bots can optionally use OpenAI LLMs, in addition to Snowflake Cortex. To add or update a key for these models, enter it below. If you have not yet assigned the External Access Integration to Genesis, click the Assign EAI to Genesis button and then you can enter your LLM Key."
    )
    if cur_key == "" and active_llm_type is not None and len(active_llm_type) > 0:
        st.markdown("**Currently Stored LLMs**")
        st.markdown(
            """
            <style>
            .dataframe {
                width: auto !important;
            }
            </style>
            """,
            unsafe_allow_html=True
        )
        st.dataframe(llm_types, use_container_width=False) 
    

    llm_model = st.selectbox("Choose LLM Model:", ["Cortex", "OpenAI", "Azure OpenAI"])
    llm_model_selected = llm_model
    st.session_state.llm_model_value = llm_model_selected.lower().replace(' ','')
    llm_api_endpoint = ""






    if st.session_state.llm_model_value == "openai":
        if st.session_state.NativeMode:
            if st.session_state.openai_eai_available == False:
                if st.button("Create External Access Integration", key="createeai", disabled=st.session_state.disable_create):
                    st.session_state.eai_reference_name = 'openai_external_access'
                    st.session_state.assign_disabled = False
                    st.session_state.disable_create = True
                    import snowflake.permissions as permissions
                    permissions.request_reference("openai_external_access")

                if st.session_state.assign_disabled == False: 
                    if st.button("Assign EAI to Genesis", key="assigneai", disabled=st.session_state.assign_disabled):
                        if st.session_state.eai_reference_name:
                            eai_type = st.session_state.eai_reference_name.split('_')[0].upper()
                            upgrade_result = upgrade_services(eai_type,st.session_state.eai_reference_name) #["reference('CONSUMER_EXTERNAL_ACCESS')"]
                            st.success(f"Genesis Bots upgrade result: {upgrade_result}")
                            # st.session_state.clear()
                            st.session_state.assign_disabled = True
                            st.session_state.openai_eai_available = True
                            st.session_state.disable_submit = False
                            st.rerun()
                        else:
                            st.error("No EAI reference set")

        if st.session_state.openai_eai_available == True or st.session_state.NativeMode == False: 
            selected_key = [llm["llm_key"] for llm in llm_info if llm["llm_type"] == st.session_state.llm_model_value]
            if selected_key:
                cur_key = selected_key[0][:3] + "*" * (len(selected_key[0]) - 7) + selected_key[0][-4:] if len(selected_key[0]) > 10 else selected_key[0]
            llm_api_key = st.text_input("Enter OpenAI API Key:", value=cur_key, key="oaikey")


    elif st.session_state.llm_model_value == "azureopenai":
        if st.session_state.azure_openai_eai_available == False or st.session_state.NativeMode == False:
            endpoint = st.text_input("Enter Azure API endpoint for your organziation (e.g. genesis-azureopenai-1):")
            azure_openai_model = st.text_input("Enter Azure OpenAI Model Deployment Name (e.g. gpt-4o)")
            azure_openai_embed_model = st.text_input("Enter Azure OpenAI Embedding Model Deployment Name (e.g. text-embedding-3-large)")
            # llm_api_endpoint = 'https://' + endpoint + '.openai.azure.com'

            if st.session_state.NativeMode:

                if st.button("Create External Access Integration", key="createaeai", disabled=st.session_state.disable_create):
                    set_endpoint = get_metadata(f"set_endpoint {endpoint}")
                    if isinstance(set_endpoint, list) and len(set_endpoint) > 0: 
                        if 'Success' in set_endpoint[0] and set_endpoint[0]['Success']==True:
                            set_model_names = get_metadata(f"set_model_name {azure_openai_model} {azure_openai_embed_model}")
                            if isinstance(set_model_names, list) and len(set_model_names) > 0: 
                                if 'Success' in set_model_names[0] and set_model_names[0]['Success']==True:                            
                                    st.session_state.assign_disabled = False
                                    st.session_state.eai_reference_name = 'azure_openai_external_access'
                                    st.session_state.disable_create = True
                                    import snowflake.permissions as permissions
                                    permissions.request_reference("azure_openai_external_access")


                if st.session_state.assign_disabled == False: 
                    if st.button("Assign EAI to Genesis", key="assignaeai", disabled=st.session_state.assign_disabled):
                        #TODO make a function
                        if st.session_state.eai_reference_name:
                            eai_type = st.session_state.eai_reference_name.split('_')[0].upper()
                            upgrade_result = upgrade_services(eai_type,st.session_state.eai_reference_name) #["reference('CONSUMER_EXTERNAL_ACCESS')"]
                            st.success(f"Genesis Bots upgrade result: {upgrade_result}")
                            # st.session_state.clear()
                            st.session_state.assign_disabled = True
                            st.session_state.azure_openai_eai_available = True
                            st.session_state.disable_submit = False
                            st.rerun()
                        else:
                            st.error("No EAI reference set")

        if st.session_state.azure_openai_eai_available == True or st.session_state.NativeMode == False:
            llm_api_key = st.text_input("Enter Azure OpenAI API Key:", value=cur_key, key="aoaikey")
            llm_api_endpoint = st.text_input("Enter Azure OpenAI API Endpoint (e.g. https://genesis-azureopenai-1.openai.azure.com):")          

    else:
        llm_api_key = 'cortex_no_key_needed'



    if st.button("Submit Model Selection", key="sendllm", disabled=st.session_state.disable_submit):
        # if llm_model != 'cortex':
        #     openai_eai_status = check_openai_eai_status('openai')
        # if openai_eai_status == True or llm_model == 'cortex':

        st.write("One moment while I validate the key and launch the bots...")
        if "***" in llm_api_key: # if it was hidden replace with real key
            llm_api_key = selected_key[0]
        if st.session_state.llm_model_value.lower() == 'azureopenai':
            st.session_state.llm_model_value = 'openai'
        config_response = configure_llm(st.session_state.llm_model_value.lower(), llm_api_key, llm_api_endpoint)
        if config_response["Success"] is False:
            resp = config_response["Message"]
            st.error(f"Failed to set LLM token: {resp}")
            config_response = configure_llm('cortex', 'cortex_no_key_needed', '')

            cur_key = ""
        else:
            st.session_state.disable_submit = True
            st.success(f"{llm_model} LLM validated!")

        if config_response["Success"]:
            with st.spinner("Getting active bot details..."):
                bot_details = get_bot_details()
            if bot_details:
                st.success("Bot details validated.")
                time.sleep(0.5)
                st.success(
                    "-> Please refresh this browser page to chat with your bots!"
                )
                # st.session_state.clear()
                st.cache_data.clear()
                st.cache_resource.clear()
                get_bot_details.clear()
                if llm_model in ["OpenAI"]:
                    st.session_state.show_openai_config = False

                
        if cur_key == "<existing key present on server>":
            st.write("Reload this page to chat with your apps.")

        # else:
        #     st.error(f"EAI status: {openai_eai_status}. Set up your EAI.")


            