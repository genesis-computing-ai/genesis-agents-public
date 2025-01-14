import json
import streamlit as st
from apps.streamlit_gui.utils import ( get_metadata, set_metadata)

def config_web_access():
    # Page Title
    st.title("Configure WebAccess API settings")

    # Custom styles
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


    st.markdown('<p class="big-font">Google Serper Search API</p>',unsafe_allow_html=True )
    st.markdown("""Follow the insruction <a href="https://serper.dev/" target="_blank">here</a> to get an API key""", unsafe_allow_html=True)
    serper_api_key = st.text_input("Serper API Key")

    # Handle submission of Jira parameters
    if st.button("Add Serper API Key"):
        if not serper_api_key:
            st.error("Serper API Key is required.")
        else:
            try:
                key_pairs   = {"api_key": serper_api_key}
                # Send data to metadata
                api_config_result = set_metadata(f"api_config_params serper {json.dumps(key_pairs)}")
                # Check if the result indicates success
                if (isinstance(api_config_result, list) and api_config_result and
                    api_config_result[0].get('Success') is True):
                    st.success("Serper API parameters configured successfully!")
                else:
                    st.error(f"Failed to configure Serper API parameters: {api_config_result}")

            except Exception as e:
                st.error(f"Error configuring Serper params: {e}")


    st.markdown('<p class="big-font">Spider Cloud API</p>',unsafe_allow_html=True )
    st.markdown("""Spider is the fastest open source scraper and crawler that returns LLM-ready data.
                It converts any website into pure HTML, markdown, metadata or text while enabling you to
                crawl with custom actions using AI. Follow the insruction <a href="https://spider.cloud/" target="_blank">here</a> to get an API key""", unsafe_allow_html=True)
    spider_api_key = st.text_input("Spider API Key")

    # Handle submission of Jira parameters
    if st.button("Add Spider API Key"):
        if not spider_api_key:
            st.error("Spider API Key is required.")
        else:
            try:
                key_pairs   = {"api_key": spider_api_key}
                # Send data to metadata
                api_config_result = set_metadata(f"api_config_params spider {json.dumps(key_pairs)}")
                # Check if the result indicates success
                if (isinstance(api_config_result, list) and api_config_result and
                    api_config_result[0].get('Success') is True):
                    st.success("Spider API parameters configured successfully!")
                else:
                    st.error(f"Failed to configure Spider API parameters: {api_config_result}")

            except Exception as e:
                st.error(f"Error configuring Spider params: {e}")
