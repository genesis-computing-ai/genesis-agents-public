import streamlit as st
from utils import get_bot_details, get_metadata, set_metadata
from urllib.parse import quote

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
        
        # Display dropdowns side by side
        if bot_names:
            col1, col2 = st.columns(2)
            with col1:
                selected_bot = st.selectbox("Select a bot:", bot_names, key="bot_selector")
                if "previous_bot" not in st.session_state:
                    st.session_state.previous_bot = selected_bot
                if st.session_state.previous_bot != selected_bot:
                    st.session_state.previous_bot = selected_bot
                    st.rerun()
            
            # Get bot_id for selected bot
            selected_bot_id = next((bot["bot_id"] for bot in bot_details if bot["bot_name"] == selected_bot), None)
            projects = get_metadata(f"list_projects {selected_bot_id}")
            
            # Add project filter dropdown in second column
            with col2:
                if projects and projects['projects']:
                    project_names = [project['project_name'] for project in projects['projects']]
                    selected_project = st.selectbox("Filter by project:", project_names, key="project_filter")
            
            # Filter and display only the selected project
            selected_project_data = next((project for project in projects['projects'] 
                                        if project['project_name'] == selected_project), None)
        else:
            st.info("No projects yet - create your first project!")
            selected_project_data = None

        # Place expanders side by side - always show these
        col1, col2 = st.columns(2)
        
        # Create New Project expander in first column - always visible
        with col1:
            with st.expander("➕ Create New Project"):
                with st.form("new_project_form"):
                    project_name = st.text_input("Project Name*")
                    project_description = st.text_area("Project Description*")
                    submit_project = st.form_submit_button("Add Project")
                    
                    if submit_project:
                        if not project_name or not project_description:
                            st.error("Both project name and description are required.")
                        else:
                            try:
                                encoded_project_name = quote(project_name)
                                result = set_metadata(f"create_project {selected_bot_id} {encoded_project_name} {project_description}")
                                if result.get("success", False):
                                    st.success("Project created successfully!")
                                    st.rerun()
                                else:
                                    st.error(f"Failed to create project: {result.get('Message', 'Unknown error')}")
                            except Exception as e:
                                st.error(f"Error creating project: {e}")
        
        # Create New Todo expander in second column - only show if there's a selected project
        with col2:
            if selected_project_data:
                with st.expander("➕ Create New Todo"):
                    with st.form("new_todo_form"):
                        todo_title = st.text_input("Todo Title*")
                        todo_description = st.text_area("Todo Description*")
                        submit_todo = st.form_submit_button("Add Todo")
                        
                        if submit_todo:
                            if not todo_title or not todo_description:
                                st.error("Both todo title and description are required.")
                            else:
                                try:
                                    project_id = selected_project_data['project_id']
                                    encoded_title = quote(todo_title)
                                    result = set_metadata(f"add_todo {project_id} {selected_bot_id} {encoded_title} {todo_description}")
                                    if result.get("success", False):
                                        st.success("Todo added successfully!")
                                        st.rerun()
                                    else:
                                        st.error(f"Failed to add todo: {result.get('Message', 'Unknown error')}")
                                except Exception as e:
                                    st.error(f"Error adding todo: {e}")

        # Only show todos if we have a selected project
        if selected_project_data:
            # Get and display todos for this project
            project_id = selected_project_data.get('project_id')
            if project_id:
                todos = get_metadata(f"list_todos {project_id}")
                if todos and todos.get('todos'):
                    st.markdown("**Project Todo Status:**")
                    
                    # Create rows of 3 todos each
                    todos_list = todos['todos']
                    for i in range(0, len(todos_list), 3):
                        cols = st.columns(3)
                        for j in range(3):
                            if i + j < len(todos_list):
                                todo = todos_list[i + j]
                                with cols[j]:
                                    status_emoji = "✅" if todo.get('current_status') == 'COMPLETED' else "⏳"
                                    st.markdown(f"""
                                    <div style="border: 1px solid #ddd; padding: 10px; border-radius: 5px;">
                                        <h4>{status_emoji} {todo.get('todo_name', 'No name')}</h4>
                                        <p><i>Status: {todo.get('current_status', 'N/A')} | Created: {todo.get('created_at', 'N/A')} | Assigned: {todo.get('assigned_to_bot_id', 'N/A')}</i></p>
                                    </div>
                                    """, unsafe_allow_html=True)

                                    # Details with expansion option
                                    details = todo.get('what_to_do', 'No details')
                                    with st.expander("Show Details"):
                                        st.markdown(f"<p>{details}</p>", unsafe_allow_html=True)
                                    
                                    # History expander
                                    if todo.get('history'):
                                        with st.expander("View History"):
                                            for entry in todo['history']:
                                                st.markdown(
                                                    f"<small>"
                                                    f"Action: {entry.get('action_taken', 'N/A')}<br>"
                                                    f"Time: {entry.get('action_timestamp', 'N/A')}<br>"
                                                    f"Details: {entry.get('action_details', 'N/A')}<br>"
                                                    f"Work Description: {entry.get('work_description', 'N/A')}<br>"
                                                    f"Current Status: {entry.get('current_status', 'N/A')}<br>"
                                                    f"</small>", 
                                                    unsafe_allow_html=True
                                                )
                    st.markdown("---")
        else:
            st.info("No projects available.")
    except Exception as e:
        st.error(f"Error getting bot details: {e}")
        return