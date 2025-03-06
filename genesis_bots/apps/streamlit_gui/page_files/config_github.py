import json
import streamlit as st
from utils import (check_eai_assigned, get_references, get_session, set_metadata, upgrade_services)
from .components import config_page_header

def config_github():
    config_page_header("Setup GitHub API Params")
    # Initialize session state variables - use direct assignment
    st.session_state["github_eai_available"] = st.session_state.get("github_eai_available", False)
    st.session_state["eai_reference_name"] = "github_external_access"  # Always set correctly for this page
    st.session_state["NativeMode"] = st.session_state.get("NativeMode", False)

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

    # Instructions
    st.markdown(
        '<p class="big-font">Add your GitHub Personal Access Token (PAT) below</p>',
        unsafe_allow_html=True
    )

    # Add info about how to create a PAT
    with st.expander("How to create a GitHub Personal Access Token"):
        st.markdown("""
        1. Go to GitHub.com → Settings → Developer Settings → Personal Access Tokens → Tokens (classic)
        2. Click "Generate new token" → "Generate new token (classic)"
        3. Give your token a descriptive name
        4. Select the following scopes:
           - `repo` (Full control of private repositories)
           - `user` (Read and write user information)
        5. Click "Generate token"
        6. Copy the token immediately (you won't be able to see it again!)
        """)

    # User input fields
    github_token = st.text_input("Your GitHub Personal Access Token:", type="password")

    # Handle submission of GitHub parameters
    if st.button("Add GitHub API parameters to access GitHub from Genesis"):
        if not github_token:
            st.error("GitHub Personal Access Token is required.")
        else:
            try:
                # Prepare key-value pairs for metadata
                key_pairs = {
                    "github_token": github_token
                }
                # Send data to metadata
                github_api_config_result = set_metadata(f"api_config_params github {json.dumps(key_pairs)}")
                # Check if the result indicates success
                if (isinstance(github_api_config_result, list) and github_api_config_result and
                    github_api_config_result[0].get('Success') is True):
                    st.success("GitHub API parameters configured successfully!")
                else:
                    st.error(f"Failed to configure GitHub API parameters: {github_api_config_result}")

            except Exception as e:
                st.error(f"Error configuring GitHub params: {e}")

    # Check if GitHub EAI is available and we're in Native Mode
    if not st.session_state.github_eai_available and st.session_state.get("NativeMode", False) == True:
        try:
            eai_status = check_eai_assigned("github_external_access")  # Use direct string
            if eai_status:
                st.session_state.github_eai_available = True
                st.success("GitHub External Access Integration is available.")
            else:
                # If EAI is not available offer options
                try:
                    ref = get_references("github_external_access")  # Use direct string
                    if not ref:
                        # If no reference found, allow creating a new one
                        if st.button("Create External Access Integration", key="create_eai"):
                            import snowflake.permissions as permissions
                            permissions.request_reference("github_external_access")  # Use direct string
                            st.info("Request sent. Please rerun the app or try again to see updates.")
                    else:
                        # Reference exists but not assigned, allow assigning now
                        if st.button("Assign EAI to Genesis", key="assign_eai"):
                            # Use direct string literals
                            eai_type = "GITHUB"
                            upgrade_result = upgrade_services(eai_type, "github_external_access")
                            st.success(f"Genesis Bots upgrade result: {upgrade_result}")
                            st.session_state.github_eai_available = True
                            st.rerun()
                except Exception as e:
                    st.error(f"Failed to process references: {e}")
        except Exception as e:
            st.error(f"Failed to check EAI status: {e}") 