import streamlit as st
from utils import get_bot_details, get_metadata, configure_llm

def llm_config():
    bot_details = get_bot_details()
    
    llm_info = get_metadata("llm_info")
    llm_types = []
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
    if cur_key == "" and active_llm_type is not None:
        st.success(
            f"You already have an LLM active: **{active_llm_type[0]}**. If you want to change it, you can do so below."
        )

    st.write(
        "Genesis Bots can optionally use OpenAI or Gemini LLMs, in addition to Snowflake Cortex. To add or update a key for these models, enter it below:"
    )
    if cur_key == "" and active_llm_type is not None:
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

    llm_model = st.selectbox("Choose LLM Model:", ["OpenAI", "gemini", "cortex"])

    if llm_model in ["OpenAI", "gemini"]:
        selected_key = [llm["llm_key"] for llm in llm_info if llm["llm_type"] == llm_model]
        if selected_key:
            cur_key = selected_key[0][:3] + "*" * (len(selected_key[0]) - 7) + selected_key[0][-4:] if len(selected_key[0]) > 10 else selected_key[0]
        llm_api_key = st.text_input("Enter API Key:", value=cur_key)
    else:
        llm_api_key = 'cortex_no_key_needed'

    if "disable_submit" not in st.session_state:
        st.session_state.disable_submit = False

    if st.button("Submit Model Selection", key="sendllm", disabled=st.session_state.disable_submit):
        st.write("One moment while I validate the key and launch the bots...")
        with st.spinner("Validating API key and launching bots..."):
            if cur_key:
                llm_api_key = selected_key[0]
            config_response = configure_llm(llm_model, llm_api_key)

            if config_response["Success"] is False:
                resp = config_response["Message"]
                st.error(f"Failed to set LLM token: {resp}")
                cur_key = ""
            else:
                st.session_state.disable_submit = True
                st.success("API key validated!")

            if config_response["Success"]:
                with st.spinner("Getting active bot details..."):
                    bot_details = get_bot_details()
                if bot_details:
                    st.success("Bot details validated.")
                    time.sleep(0.5)
                    st.success(
                        "-> Please refresh this browser page to chat with your bots!"
                    )
                    st.session_state.clear()

        if cur_key == "<existing key present on server>":
            st.write("Reload this page to chat with your apps.")