import streamlit as st
from streamlit_modal import Modal

def show_modal():
    # Initialize modal state
    if 'show_modal' not in st.session_state:
        st.session_state.show_modal = True

    # Function to handle "never show again" checkbox
    def never_show_again():
        st.session_state.show_modal = False
        st.session_state.never_show_modal = True

    # Show modal if not marked to never show again
    if st.session_state.get('show_modal', True) and not st.session_state.get('never_show_modal', False):
        modal = Modal("Additional settings", key="modal")
        with modal.container():
            st.write("I'm here to stay!")
            if st.checkbox("Never show again"):
                never_show_again()
            if st.button("Close"):
                st.session_state.show_modal = False
            if st.button("Configure Warehouse"):
                __import__('page_files.config_wh').config_wh.config_wh()  # Added button to open config_wh.py                