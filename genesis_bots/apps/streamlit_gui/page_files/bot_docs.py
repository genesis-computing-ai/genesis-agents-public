import streamlit as st
from utils import get_metadata
import pandas as pd
import os
import random

def bot_docs():
    # Custom CSS for back button styling
    uploader_key = random.randint(0, 1 << 32)
    st.markdown("""
        <style>
        .back-button {
            margin-bottom: 1.5rem;
        }
        
        .back-button .stButton > button {
            width: 500px !important;    
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
        st.session_state['hide_chat_elements'] = False
        st.rerun()
    st.markdown('</div>', unsafe_allow_html=True)

    st.title("Bot Documents Index Manager")
    
    try:
        # Get list of indices
        indices = get_metadata("index_manager LIST_INDICES")
        if 'index_upload_key' not in st.session_state:
            st.session_state['index_upload_key'] = random.randint(0, 1 << 32)

        selected_index = st.selectbox("Select Index:", indices)
        
        # Convert connections list directly to DataFrame
        if selected_index:
            documents = get_metadata(f"index_manager LIST_DOCUMENTS_IN_INDEX {selected_index}")

            df = pd.DataFrame.from_records(documents['documents'], columns=['file_name', 'file_type', 'file_size', 'path', 'index'])

            # Create the dataframe display
            st.dataframe(
                df.drop(columns=['path']),
                use_container_width=True,
                hide_index=True
            )

            selected_document = st.selectbox("Select Document to delete:", df['file_name'].tolist())

            if st.button("Delete Document") and selected_document:
                selected_path = df[df['file_name'] == selected_document]['path'].values[0]
                res = get_metadata(f"index_manager DELETE_DOCUMENT {selected_index} {selected_path}")
                if res:
                    st.info(f'{selected_document} deleted from the index.')

            uploaded_file = st.file_uploader("Add a new document to the index", type=['pdf', 'doc', 'docx', 'txt', 'csv'], key=st.session_state['index_upload_key'])
            if uploaded_file:
                print(uploaded_file.name)
                filepath = os.path.join('tmp', uploaded_file.name)
                with open(filepath, "wb") as f:
                    f.write(uploaded_file.getvalue())
                res = get_metadata(f"index_manager ADD_DOCUMENT {selected_index} {filepath}")                
                st.info(f'{uploaded_file.name} added to the index.')
                st.session_state['index_upload_key'] = random.randint(0, 1 << 32)
        else:
            st.info("No Document Index found.")

        query = st.text_input("Search in available documents:")
    
        # Handle submission of Jira parameters
        if st.button("Search") and selected_index and query:
            res = get_metadata(f"index_manager SEARCH {selected_index} {query}")
            st.session_state['search_results'] = res

        if st.button('Ask') and query:
            res = get_metadata(f"index_manager ASK {query}")
            st.session_state['search_results'] = res

        
        # fill the text area with the search results
        if 'search_results' in st.session_state:
            st.text_area("Search Results", value=st.session_state['search_results'], height=200)


    
    except Exception as e:
        st.error(f"An error occurred while managing database connections: {str(e)}")
    
