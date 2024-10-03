import streamlit as st
import time 
from utils import get_bot_details, get_metadata, configure_llm, check_eai_status, get_references, upgrade_services, get_bot_details


def llm_config():
    if "eai_available" not in st.session_state:
        st.session_state.eai_available = False
        
    if st.session_state.eai_available == False:
        ref = get_references("consumer_external_access")
        # check for custom EAI
        eai_status = check_eai_status('openai')
        if not ref and eai_status == False:
            if st.session_state.NativeMode:
                import snowflake.permissions as permissions
                permissions.request_reference("consumer_external_access")
        else:
            # eai_status = check_eai_status('openai')
            st.session_state.eai_available = eai_status
            if eai_status == True:
                st.write(f"External Access Integration available")

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
    if st.session_state.eai_available == False and st.session_state.NativeMode:


        if st.button("Assign EAI to Genesis", key="assigneai"):
            upgrade_result = upgrade_services(True)
            st.success(f"Genesis Bots upgrade result: {upgrade_result}")
            # st.session_state.clear()
            st.rerun()
    else:

        llm_model = st.selectbox("Choose LLM Model:", ["OpenAI", "Cortex"])
        llm_model_value = llm_model.lower()
        llm_api_endpoint = ""

        if llm_model_value == "openai":
            selected_key = [llm["llm_key"] for llm in llm_info if llm["llm_type"] == llm_model_value]
            if selected_key:
                cur_key = selected_key[0][:3] + "*" * (len(selected_key[0]) - 7) + selected_key[0][-4:] if len(selected_key[0]) > 10 else selected_key[0]
            llm_api_key = st.text_input("Enter OpenAI or Azure OpenAI API Key:", value=cur_key)
            selected_endpoint = [llm["llm_endpoint"] for llm in llm_info if llm["llm_type"] == llm_model_value]
            if selected_endpoint:
                cur_endpoint = selected_endpoint[0]
            else:
                cur_endpoint = ""
            llm_api_endpoint = st.text_input("Enter Azure API Endpoint (if applicable):", value=cur_endpoint)          
        else:
            llm_api_key = 'cortex_no_key_needed'

        if "disable_submit" not in st.session_state:
            st.session_state.disable_submit = False

        if st.button("Submit Model Selection", key="sendllm", disabled=st.session_state.disable_submit):
            # if llm_model != 'cortex':
            #     eai_status = check_eai_status('openai')
            # if eai_status == True or llm_model == 'cortex':

            st.write("One moment while I validate the key and launch the bots...")
            if "***" in llm_api_key: # if it was hidden replace with real key
                llm_api_key = selected_key[0]
            config_response = configure_llm(llm_model.lower(), llm_api_key, llm_api_endpoint)
            if config_response["Success"] is False:
                resp = config_response["Message"]
                st.error(f"Failed to set LLM token: {resp}")
                config_response = configure_llm('cortex', 'cortex_no_key_needed', '')
                # if config_response["Success"] is False:
                # if "Connection" in resp:
                #     #TODO go to EAI page
                #     1=1
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
        #     st.error(f"EAI status: {eai_status}. Set up your EAI.")


            