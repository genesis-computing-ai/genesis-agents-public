import streamlit as st
from utils import get_bot_details, get_metadata

def bot_projects():
    # Custom CSS for back button
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
    # Get bot details
    try:
        bot_details = get_bot_details()
        if bot_details == {"Success": False, "Message": "Needs LLM Type and Key"}:
            st.session_state["radio"] = "LLM Model & Key"
            st.rerun()
        
        # Sort to make sure a bot with 'Eve' in the name is first if exists
        bot_details.sort(key=lambda bot: (not "Eve" in bot["bot_name"], bot["bot_name"]))
        
        # Get list of bot names
        bot_names = [bot["bot_name"] for bot in bot_details]
        
        # Display dropdown
        if bot_names:
            selected_bot = st.selectbox("Select a bot:", bot_names, key="bot_selector")
            if "previous_bot" not in st.session_state:
                st.session_state.previous_bot = selected_bot
            if st.session_state.previous_bot != selected_bot:
                st.session_state.previous_bot = selected_bot
                st.rerun()
        else:
            st.info("No bots available.")
            
        # Get and display selected bot's projects
        st.title(f"{selected_bot}'s Projects")
        # Get bot_id for selected bot
        selected_bot_id = next((bot["bot_id"] for bot in bot_details if bot["bot_name"] == selected_bot), None)
        projects = get_metadata(f"list_projects {selected_bot_id}")
        if projects:
            for project in projects['projects']:
                st.markdown(f"""
                **Project Name:** {project.get('project_name', 'N/A')}  
                **Status:** {project.get('current_status', 'N/A')}  
                **Created:** {project.get('created_at', 'N/A')}  
                **Todo Count:** {project.get('todo_count', 0)}
                """)
                
                # Get and display todos for this project
                project_id = project.get('project_id')
                if project_id:
                    todos = get_metadata(f"list_todos {project_id}")
                    if todos and todos.get('todos'):
                        st.markdown("**Tasks:**")
                        for todo in todos['todos']:
                            status_emoji = "✅" if todo.get('current_status') == 'COMPLETED' else "⏳"
                            st.markdown(f"""
                            {status_emoji} {todo.get('todo_name', 'No name')}  
                            - Status: {todo.get('current_status', 'N/A')}  
                            - Created: {todo.get('created_at', 'N/A')}
                            - Assigned to: {todo.get('assigned_to_bot_id', 'N/A')}
                            - Details: {todo.get('what_to_do', 'No details')}
                            """)
                            # Get todo history
                            if todo.get('history'):
                                with st.expander("View History"):
                                    for entry in todo['history']:
                                        st.markdown(
                                            f"<small>"
                                            f"Action: {entry.get('action_taken', 'N/A')}<br>"
                                            f"Time: {entry.get('action_timestamp', 'N/A')}<br>"
                                            f"By: {entry.get('action_by_bot_id', 'N/A')}<br>"
                                            f"Details: {entry.get('action_details', 'N/A')}<br>"
                                            f"Work Description: {entry.get('work_description', 'N/A')}<br>"
                                            f"Previous Status: {entry.get('previous_status', 'N/A')}<br>"
                                            f"Current Status: {entry.get('current_status', 'N/A')}<br>"
                                            f"Status Changed: {'Yes' if entry.get('status_changed_flag') else 'No'}<br>"
                                            #f"Work Results: {entry.get('work_results', 'N/A')}<br>"
                                            f"---"
                                            f"</small>", 
                                            unsafe_allow_html=True
                                        )
                st.markdown("---")
        else:
            st.info("No projects available.")
    except Exception as e:
        st.error(f"Error getting bot details: {e}")
        return