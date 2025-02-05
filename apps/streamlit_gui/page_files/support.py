import streamlit as st

def support():
    # Custom CSS for styling
    st.markdown("""
        <style>
        /* Link styling */
        .support-link {
            font-size: 1.1em;
            margin-bottom: 1rem;
            display: block;
            text-decoration: none;
        }
        
        /* Back button styling */
        .back-button {
            margin-bottom: 1.5rem;
        }
        
        .back-button .stButton > button {
            text-align: left !important;
            justify-content: flex-start !important;
            background-color: transparent !important;
            border: none !important;
            color: #FF4B4B !important;
            margin: 0 !important;
            font-weight: 600 !important;
            box-shadow: none !important;
            font-size: 1em !important;
            padding: 0.5rem 1rem !important;
        }
        
        .back-button .stButton > button:hover {
            background-color: rgba(255, 75, 75, 0.1) !important;
            box-shadow: none !important;
            transform: none !important;
        }
        </style>
    """, unsafe_allow_html=True)

    # Back button
    st.markdown('<div class="back-button">', unsafe_allow_html=True)
    if st.button("← Back to Chat", key="back_to_chat", use_container_width=True):
        st.session_state["selected_page_id"] = "chat_page"
        st.session_state["radio"] = "Chat with Bots"
        st.session_state['hide_chat_elements'] = False  # Clear the flag when going back
        st.rerun()
    st.markdown('</div>', unsafe_allow_html=True)

    # Support links with custom styling
    st.markdown('<a href="https://genesiscomputing.ai/docs/" class="support-link">📚 Genesis Documentation</a>', unsafe_allow_html=True)
    st.markdown('<a href="https://communityinviter.com/apps/genesisbotscommunity/genesis-bots-community" class="support-link">💬 Join our Slack Community</a>', unsafe_allow_html=True)

if __name__ == "__main__":
    support()