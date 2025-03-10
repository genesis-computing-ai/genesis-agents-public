import streamlit as st
import uuid
from utils import get_bot_details, get_metadata, set_metadata
from urllib.parse import quote
from page_files.chat_page import ChatMessage, set_initial_chat_sesssion_data
import os

def bot_projects():
    # Custom CSS for back button
    st.markdown("""
        <style>
        .back-button {
            margin-bottom: 1.5rem;
        }
        
        .delete-button > button {
            color: orange !important;
            background: none !important;
            border: none !important;
            padding: 2px 6px !important;
            line-height: 1 !important;
            min-height: 0 !important;
            transition: color 0.2s ease !important;
            margin: 10px 0px 0px -40px !important;
        }
        
        .delete-button > button:hover {
            color: #FF0000 !important;
            background: none !important;
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
        
        /* Delete button styles */
        [data-testid="column"]:has(button[key^="delete_"]) {
            margin-left: -16px;
        }
        
        [data-testid="column"]:has(button[key^="delete_"]) button {
            color: #FF4B4B !important;
            background: none !important;
            border: none !important;
            padding: 2px 6px !important;
            line-height: 1 !important;
            min-height: 0 !important;
            transition: color 0.2s ease !important;
        }
        
        [data-testid="column"]:has(button[key^="delete_"]) button:hover {
            color: #FF0000 !important;
            background: none !important;
        }
        
        /* Expander styles */
        .streamlit-expander {
            border: none !important;
            box-shadow: none !important;
        }
        
        .streamlit-expander .streamlit-expanderHeader {
            font-size: 0.9em !important;
            color: #666 !important;
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
                # Get currently selected bot from session state, or default to first bot
                default_index = 0
                if "current_bot" in st.session_state:
                    try:
                        default_index = bot_names.index(st.session_state.current_bot)
                    except ValueError:
                        default_index = 0

                selected_bot = st.selectbox("Select a bot:", bot_names, index=default_index, key="bot_selector")
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
                                    status_emoji = "✅" if todo.get('current_status') == 'COMPLETED' else ("🏃" if todo.get('current_status') == 'IN_PROGRESS' else ("🛑" if todo.get('current_status') == 'ERROR' else "⏳"))
                                    st.markdown(f"""
                                    <div style="border: 1px solid #ddd; padding: 10px; border-radius: 5px;">
                                        <h4>{status_emoji} {todo.get('todo_name', 'No name')}</h4>
                                        <p><i>Status: {todo.get('current_status', 'N/A')} | Created: {todo.get('created_at', 'N/A')} | Assigned: {todo.get('assigned_to_bot_id', 'N/A')}</i></p>
                                    </div>
                                    """, unsafe_allow_html=True)

                                    # Create three columns for the buttons
                                    btn_col1, btn_col2, btn_col3 = st.columns(3)

                                    # Work on this now button
                                    with btn_col1:
                                        if st.button("🔨 Work!", key=f"work_button_{todo.get('todo_id')}", use_container_width=True):
                                            try:
                                                new_thread_id = str(uuid.uuid4())
                                                st.session_state.current_bot = selected_bot
                                                st.session_state.current_thread_id = new_thread_id
                                                new_session = f"🤖 {st.session_state.current_bot} ({new_thread_id[:8]})"
                                                st.session_state.current_session = new_session

                                                if "active_sessions" not in st.session_state:
                                                    st.session_state.active_sessions = []
                                                if new_session not in st.session_state.active_sessions:
                                                    st.session_state.active_sessions.append(new_session)

                                                st.session_state[f"messages_{new_thread_id}"] = []

                                                from page_files.chat_page import set_initial_chat_sesssion_data
                                                initial_message = f"Perform work on the following todo:\ntodo id: {todo.get('todo_id')}\nWhat to do: {todo.get('what_to_do')}\n\nOnce you have performed the work, log your work on the todo with record_todo_work (include ALL the work you performed), and update the status of the todo to completed if applicable. The user is watching you do this work, so explain what you are doing and what tool calls you are making."
                                                set_initial_chat_sesssion_data(
                                                    bot_name=selected_bot,
                                                    initial_prompt=initial_message,
                                                    initial_message=None
                                                )

                                                st.session_state.active_chat_started = True
                                                st.session_state["radio"] = "Chat with Bots"

                                                st.rerun()
                                            except Exception as e:
                                                st.error(f"Failed to create chat session: {str(e)}")

                                    # Add Hint button
                                    with btn_col2:
                                        if st.button("💡 Hint", key=f"hint_button_{todo.get('todo_id')}", use_container_width=True):
                                            try:
                                                project_id = selected_project_data['project_id']
                                                todo_id = todo.get('todo_id')
                                                with st.form(key=f"hint_form_{todo_id}"):
                                                    hint = st.text_area("Enter hint:", key=f"hint_text_{todo_id}")
                                                    if st.form_submit_button("Submit Hint"):
                                                        result = set_metadata(f"add_todo_hint {project_id} {todo_id} {hint}")
                                                        if result.get("success", False):
                                                            st.success("Hint added successfully!")
                                                            st.rerun()
                                                        else:
                                                            st.error(f"Failed to add hint: {result.get('Message', 'Unknown error')}")
                                            except Exception as e:
                                                st.error(f"Error adding hint: {e}")

                                    # Delete button
                                    with btn_col3:
                                        if st.button("❌", key=f"delete_button_{todo.get('todo_id')}", use_container_width=True):
                                            try:
                                                todo_id = todo.get('todo_id')
                                                selected_bot_id = next((bot["bot_id"] for bot in bot_details if bot["bot_name"] == selected_bot), None)
                                                result = get_metadata(f"delete_todo {selected_bot_id} {todo_id}")
                                                if result.get("Success", False) or result.get("success", False) == True:
                                                    st.success("Todo deleted successfully!")
                                                    st.rerun()
                                                else:
                                                    st.error(f"Failed to delete todo: {result.get('Message', 'Unknown error')}")
                                            except Exception as e:
                                                st.error(f"Error deleting todo: {e}")

                                    # Details with expansion option
                                    details = todo.get('what_to_do', 'No details')
                                    with st.expander("Show Details"):
                                        st.markdown(f"<p>Todo ID: {todo.get('todo_id', 'N/A')}</p>", unsafe_allow_html=True)
                                        st.markdown(f"<p>{details}</p>", unsafe_allow_html=True)

                                    # History expander
                                    if todo.get('history'):
                                        with st.expander("View History"):
                                            history_entries = todo.get('history', [])
                                            if not history_entries:
                                                st.info("No history entries available.")
                                            else:
                                                for idx, entry in enumerate(history_entries):
                                                    if not isinstance(entry, dict):
                                                        continue

                                                    # Process all text fields for file paths
                                                    history_text = {
                                                        'action_taken': entry.get('action_taken', 'N/A'),
                                                        'action_details': entry.get('action_details', 'N/A'),
                                                        'work_description': entry.get('work_description', 'N/A'),
                                                        'thread_id': entry.get('thread_id', 'N/A')
                                                    }

                                                    # Convert file paths to links in all fields
                                                    for key, text in history_text.items():

                                                        if isinstance(text, str) and 'tmp/' in text:
                                                            import re
                                                            pattern = r'tmp/[^\s)]*\.txt'
                                                            matches = re.finditer(pattern, text)
                                                            for match in matches:
                                                                file_path = match.group(0)
                                                                # Button to navigate to file viewer
                                                                if st.button(f"💻 See Detailed Work Log", key=f"view_file_{file_path}"):
                                                                    # Store current state
                                                                    st.session_state["previous_bot"] = selected_bot
                                                                    st.session_state["previous_project"] = selected_project
                                                                    st.session_state["previous_todo_id"] = todo.get('todo_id')
                                                                    st.session_state["previous_history_open"] = True
                                                                    st.session_state[f"history_{todo.get('todo_id')}"] = True  # Store history expander state
                                                                    # Navigate to file viewer
                                                                    st.session_state["selected_page_id"] = "file_viewer"
                                                                    st.session_state["radio"] = "File Viewer"
                                                                    st.session_state["file_path_to_view"] = file_path
                                                                    st.session_state['hide_chat_elements'] = True
                                                                    st.rerun()

                                                                history_text[key] = text.replace(
                                                                    file_path,
                                                                    f"[View work log above]"
                                                                )
                                                    # Add button for thread ID if it exists and isn't 'N/A'
                                                    

                                                    st.markdown(
                                                        f"<small>"
                                                        f"Action: {history_text['action_taken']}<br>"
                                                        f"Time: {entry.get('action_timestamp', 'N/A')}<br>"
                                                        f"Current Status: {entry.get('current_status', 'N/A')}<br>"
                                                        f"Thread ID: {entry.get('thread_id', 'N/A')}<br>"
                                                        f"Details: {history_text['action_details']}<br>"
                                                        f"Work Description: {history_text['work_description']}<br>"
                                                      
                                                        f"</small>",
                                                        unsafe_allow_html=True
                                                    )
                                                    if history_text['thread_id'] and history_text['thread_id'] != 'N/A':
                                                        if st.button(f"🧵 View Thread", key=f"view_thread_{history_text['thread_id']}_{idx}_{todo.get('todo_id')}"):
                                                            # Store current state
                                                            st.session_state["previous_bot"] = selected_bot
                                                            st.session_state["previous_project"] = selected_project
                                                            st.session_state["previous_todo_id"] = todo.get('todo_id')
                                                            st.session_state["previous_history_open"] = True
                                                            st.session_state[f"history_{todo.get('todo_id')}"] = True  # Store history expander state
                                                            # Navigate to file viewer
                                                            st.session_state["selected_page_id"] = "file_viewer"
                                                            st.session_state["radio"] = "File Viewer"
                                                            st.session_state["file_path_to_view"] = f"Thread:{history_text['thread_id']}"
                                                            st.session_state['hide_chat_elements'] = True
                                                            st.rerun()
                    st.markdown("---")
        else:
            st.info("No projects available.")
    except Exception as e:
        st.error(f"Error getting bot details: {e}")
        return