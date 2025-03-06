import streamlit as st

def config_page_header(title: str):
    """
    Renders a consistent header for configuration pages with a back button.
    
    Args:
        title (str): The title of the configuration page
    """
    # Hide sidebar on configuration pages
    st.markdown("""
        <style>
        [data-testid="stSidebar"][aria-expanded="true"]{
            display: none;
        }
        [data-testid="stSidebar"][aria-expanded="false"]{
            display: none;
        }
        </style>
    """, unsafe_allow_html=True)

    # Back button using Streamlit's button
    if st.button("← Back to Configuration", use_container_width=True):
        st.session_state["selected_page_id"] = "configuration"
        st.session_state["radio"] = "Configuration"
        st.rerun()
    
    st.title(title) 