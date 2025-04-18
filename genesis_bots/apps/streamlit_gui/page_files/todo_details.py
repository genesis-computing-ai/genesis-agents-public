import streamlit as st
from utils import get_metadata, set_metadata, get_bot_details
import re
import uuid
from page_files.chat_page import set_initial_chat_sesssion_data

def todo_details():
    # Debug available commands

    # Back button
    st.markdown('<div class="back-button">', unsafe_allow_html=True)
    if st.button("← Back to Projects", key="back_to_projects", use_container_width=True):
        st.session_state["selected_page_id"] = "bot_projects"
        st.session_state["radio"] = "Bot Projects"
        st.rerun()
    st.markdown('</div>', unsafe_allow_html=True)

    if "selected_todo_id" not in st.session_state:
        st.error("No todo selected")
        return

    todo_id = st.session_state["selected_todo_id"]

    todo_details = get_metadata(f"get_todo_details {todo_id}")
    
    if not todo_details:
        st.error("Could not fetch todo details - no response")
        return
    
    if isinstance(todo_details, dict) and todo_details.get('Success') is False:
        st.error(f"Error fetching todo details: {todo_details.get('Message', 'Unknown error')}")
        return

    # Simplified format handling
    try:
        # If todo_details is already a dictionary with the expected fields, use it directly
        todo = todo_details if isinstance(todo_details, dict) else todo_details[0][1]
    except Exception as e:
        st.error(f"Unexpected todo details format: {todo_details}")
        return

    # Display todo details
    st.title(todo.get("todo_name", "Unnamed Todo"))

    # Create two columns for the main content
    col1, col2 = st.columns([3, 2])

    with col1:
        # Details Section
        st.markdown("### Details")
        
        # Status indicator
        status = todo.get("current_status", "UNKNOWN")
        status_emoji = {
            "COMPLETED": "✅",
            "IN_PROGRESS": "🏃",
            "ERROR": "🛑"
        }.get(status, "⏳")
        
        st.markdown(f"**Status:** {status_emoji} {status}")
        
        # Basic information
        st.markdown(f"**Created:** {todo.get('created_at', 'N/A')}")
        st.markdown(f"**Assigned To:** {todo.get('assigned_to_bot_id', 'N/A')}")
        st.markdown(f"**Todo ID:** {todo.get('todo_id', 'N/A')}")
        st.markdown(f"**Project ID:** {todo.get('project_id', 'N/A')}")

        # Description
        st.markdown("### Description")
        st.markdown(todo.get("what_to_do", "No description available"))

        # Action buttons in a row
        button_cols = st.columns(2)
        with button_cols[0]:
            if st.button("🔨 Work on This", use_container_width=True):
                try:
                    bot_details = get_bot_details()
                    selected_bot = next((bot["bot_name"] for bot in bot_details if bot["bot_id"] == todo.get("assigned_to_bot_id")), None)
                    
                    if selected_bot:
                        new_thread_id = str(uuid.uuid4())
                        st.session_state.current_bot = selected_bot
                        st.session_state.current_thread_id = new_thread_id
                        new_session = f"🤖 {selected_bot} ({new_thread_id[:8]})"
                        st.session_state.current_session = new_session

                        if "active_sessions" not in st.session_state:
                            st.session_state.active_sessions = []
                        if new_session not in st.session_state.active_sessions:
                            st.session_state.active_sessions.append(new_session)

                        st.session_state[f"messages_{new_thread_id}"] = []

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

        with button_cols[1]:
            if st.button("💡 Add Hint", use_container_width=True):
                st.session_state["show_hint_form"] = True

        # Hint form
        if st.session_state.get("show_hint_form", False):
            with st.form("hint_form"):
                hint = st.text_area("Enter hint:")
                if st.form_submit_button("Submit Hint"):
                    result = set_metadata(f"add_todo_hint {todo.get('project_id')} {todo.get('todo_id')} {hint}")
                    if result.get("success", False):
                        st.success("Hint added successfully!")
                        st.session_state["show_hint_form"] = False
                        st.rerun()
                    else:
                        st.error(f"Failed to add hint: {result.get('Message', 'Unknown error')}")

    # History Section in the second column
    with col2:
        st.markdown("### History")
        history = get_metadata(f"get_todo_history {todo.get('todo_id')}")
        
        if not history or "history" not in history:
            st.info("No history available")
            return

        for entry in history["history"]:
            if not isinstance(entry, dict):
                continue

            with st.container():
                # Status emoji and timestamp
                status = entry.get('current_status', 'N/A')
                status_emoji = {
                    "COMPLETED": "✅",
                    "IN_PROGRESS": "🏃",
                    "ERROR": "🛑"
                }.get(status, "⏳")
                st.markdown(f"**{status_emoji} {entry.get('action_timestamp', 'N/A')}**")
                
                # Action taken
                st.markdown(f"_{entry.get('action_taken', 'N/A')}_")
                
                # Work description (if exists)
                if entry.get('work_description'):
                    st.markdown(entry.get('work_description'))

                # Check for work log files
                work_description = entry.get('work_description', '')
                if isinstance(work_description, str) and 'tmp/' in work_description:
                    pattern = r'tmp/[^\s)]*\.txt'
                    matches = re.finditer(pattern, work_description)
                    for match in matches:
                        file_path = match.group(0)
                        # Create a stable, unique key using file path and timestamp
                        view_log_key = f"view_file_{file_path}_{entry.get('action_timestamp')}"
                        
                        if not st.session_state.get("NativeMode", False):
                            if st.button(f"💻 View Log", key=view_log_key):
                                st.session_state["selected_page_id"] = "file_viewer"
                                st.session_state["radio"] = "File Viewer"
                                st.session_state["file_path_to_view"] = file_path
                                st.session_state['hide_chat_elements'] = True
                                st.rerun()

                # Thread ID link
                thread_id = entry.get('thread_id')
                if thread_id and thread_id != 'N/A':
                    # Create a stable, unique key using thread_id and timestamp
                    view_thread_key = f"view_thread_{thread_id}_{entry.get('action_timestamp')}"
                    if st.button(f"🧵 View Thread", key=view_thread_key):
                        st.session_state["selected_page_id"] = "file_viewer"
                        st.session_state["radio"] = "File Viewer"
                        st.session_state["file_path_to_view"] = f"Thread:{thread_id}"
                        st.session_state['hide_chat_elements'] = True
                        st.rerun()
                
                st.markdown("---") 