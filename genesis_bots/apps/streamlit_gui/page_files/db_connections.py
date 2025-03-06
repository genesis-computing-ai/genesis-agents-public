import streamlit as st
from utils import get_metadata
import pandas as pd

def db_connections():
    # Custom CSS for back button styling
    st.markdown("""
        <style>
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
        st.session_state['hide_chat_elements'] = False
        st.rerun()
    st.markdown('</div>', unsafe_allow_html=True)

    st.title("Database Connections")
    
    try:
        # Get database connections metadata
        connections = get_metadata("db_connections")
        
        # Convert connections list directly to DataFrame
        if isinstance(connections, list) and len(connections) > 0:
            # Convert connections to DataFrame
            df = pd.DataFrame.from_records(connections)
            
            # Create the dataframe display
            st.dataframe(
                df,
                column_config={
                    "connection_id": "Connection ID",
                    "db_type": "Database Type",
                    "owner_bot_id": "Owner Bot",
                    "allowed_bot_ids": "Allowed Bots",
                    "created_at": "Created",
                    "updated_at": "Updated",
                    "description": "Description"
                },
                use_container_width=True,
                hide_index=True
            )
        else:
            st.info("No database connections found.")
    
    except Exception as e:
        st.error(f"An error occurred while managing database connections: {str(e)}")
    
    # Add help text at the bottom
    st.markdown("---")
    st.markdown("""
    ### Managing Database Connections
    
    To add, change, or remove a database connection, please talk to Eve and tell her what type of database you want to connect to. 
    She can help you with:
    - Setting up new database connections
    - Modifying existing connections
    - Removing unused connections
    - Configuring access permissions
    """) 