import streamlit as st

def config_page_header(title: str):
    """
    Renders a consistent header for configuration pages with a back button.
    
    Args:
        title (str): The title of the configuration page
    """
    st.markdown("""
        <style>
        .config-header {
            display: flex;
            align-items: center;
            margin-bottom: 2rem;
        }
        
        /* Back button styling */
        [data-testid="baseButton-secondary"] {
            text-align: left !important;
            justify-content: flex-start !important;
            padding-left: 0.5rem !important;
            color: #FF4B4B !important;
            font-weight: 600 !important;
            background-color: transparent !important;
            border: none !important;
        }
        
        [data-testid="baseButton-secondary"]:hover {
            background-color: rgba(255, 75, 75, 0.1) !important;
            border-color: transparent !important;
        }
        </style>
    """, unsafe_allow_html=True)

    # Back button using Streamlit's button
    if st.button("← Back to Configuration", key="back_to_config", type="secondary"):
        st.session_state["selected_page_id"] = "configuration"
        st.session_state["radio"] = "Configuration"
        st.rerun()
    
    st.title(title) 