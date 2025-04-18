import streamlit as st
import uuid
from utils import get_bot_details, get_metadata, set_metadata
from urllib.parse import quote
from page_files.chat_page import ChatMessage, set_initial_chat_sesssion_data
import os
import pandas as pd

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

                    # Create a dataframe for the grid display
                    todos_list = todos['todos']
                    todo_data = []
                    for todo in todos_list:
                        status_emoji = "✅" if todo.get('current_status') == 'COMPLETED' else ("🏃" if todo.get('current_status') == 'IN_PROGRESS' else ("🛑" if todo.get('current_status') == 'ERROR' else "⏳"))
                        todo_data.append({
                            'Status': status_emoji,
                            'Title': todo.get('todo_name', 'No name'),
                            'Current Status': todo.get('current_status', 'N/A'),
                            'Created': todo.get('created_at', 'N/A'),
                            'Assigned To': todo.get('assigned_to_bot_id', 'N/A'),
                            'Actions': todo.get('todo_id')  # We'll use this to create action buttons
                        })
                    
                    df = pd.DataFrame(todo_data)
                    
                    # Display the grid
                    for idx, row in df.iterrows():
                        col1, col2, col3, col4, col5 = st.columns([0.5, 2, 1, 1, 2])
                        
                        with col1:
                            st.markdown(f"<h3 style='margin: 0; padding: 0;'>{row['Status']}</h3>", unsafe_allow_html=True)
                        with col2:
                            st.markdown(f"<p style='margin: 0; padding: 0;'><b>{row['Title']}</b></p>", unsafe_allow_html=True)
                        with col3:
                            st.markdown(f"<p style='margin: 0; padding: 0;'>{row['Current Status']}</p>", unsafe_allow_html=True)
                        with col4:
                            # Find the corresponding todo from the original todos list to get the full todo details
                            current_todo = next((todo for todo in todos['todos'] if todo['todo_id'] == row['Actions']), None)
                            if st.button("🔨 Work!", key=f"work_button_{row['Actions']}", use_container_width=True):
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

                                    initial_message = f"Perform work on the following todo:\ntodo id: {row['Actions']}\nWhat to do: {current_todo.get('what_to_do')}\n\nOnce you have performed the work, log your work on the todo with record_todo_work (include ALL the work you performed), and update the status of the todo to completed if applicable. The user is watching you do this work, so explain what you are doing and what tool calls you are making."
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
                        with col5:
                            btn_col1, btn_col2, btn_col3, btn_col4 = st.columns(4)
                            with btn_col1:
                                if st.button("📋", key=f"details_{row['Actions']}", help="View Details"):
                                    st.session_state["selected_page_id"] = "todo_details"
                                    st.session_state["selected_todo_id"] = row['Actions']
                                    st.session_state["radio"] = "Todo Details"
                                    st.rerun()
                            with btn_col2:
                                if st.button("💡", key=f"hint_{row['Actions']}", help="Add Hint"):
                                    # Add hint functionality here
                                    pass
                            with btn_col3:
                                if st.button("❌", key=f"delete_{row['Actions']}", help="Delete Todo"):
                                    try:
                                        todo_id = row['Actions']
                                        selected_bot_id = next((bot["bot_id"] for bot in bot_details if bot["bot_name"] == selected_bot), None)
                                        result = get_metadata(f"delete_todo {selected_bot_id} {todo_id}")
                                        if result.get("Success", False) or result.get("success", False) == True:
                                            st.success("Todo deleted successfully!")
                                            st.rerun()
                                        else:
                                            st.error(f"Failed to delete todo: {result.get('Message', 'Unknown error')}")
                                    except Exception as e:
                                        st.error(f"Error deleting todo: {e}")
                        
                        st.markdown("<hr style='margin: 5px 0;'>", unsafe_allow_html=True)
        else:
            st.info("No projects available.")
    except Exception as e:
        st.error(f"Error getting bot details: {e}")
        return

async def perform_source_research_v2(todo_id, field_name, requirements):
    messages = []
    
    # Step 1: State requirements and get confirmation
    messages.append({
        "role": "user",
        "content": f"I need you to help research the source data for mapping field: {field_name}. Here are the requirements:\n{requirements}\n\nPlease repeat back the requirements and confirm you understand what needs to be mapped."
    })
    
    # Step 2: Get search term proposals
    messages.append({
        "role": "user",
        "content": "Based on these requirements, please propose 3 or more relevant search terms that could help find source data for this mapping. Just list the terms - we'll use them in the next steps."
    })
    
    # Steps 3-5: Run data_explorer searches one at a time
    messages.append({
        "role": "user",
        "content": "Let's explore the first search term you proposed using data_explorer(). Run the search and briefly describe what you found."
    })
    
    messages.append({
        "role": "user",
        "content": "Now let's try the second search term with data_explorer(). Run it and give me a brief summary of the results."
    })
    
    messages.append({
        "role": "user",
        "content": "Let's explore your third search term (or more if you think additional searches would be valuable) with data_explorer(). Run the search and summarize what you found."
    })
    
    # Step 6: Get document search terms
    messages.append({
        "role": "user",
        "content": "Now, propose three search terms we should use with document_index() to find relevant documentation about this field. Just list the terms - don't run the searches yet."
    })
    
    # Steps 7-9: Run document searches one at a time
    messages.append({
        "role": "user",
        "content": "Let's run your first document search term using document_index(Action='Search'). What did you find?"
    })
    
    messages.append({
        "role": "user",
        "content": "Now run your second document search term using document_index(Action='Search'). What did you find?"
    })
    
    messages.append({
        "role": "user",
        "content": "Let's try your third document search term using document_index(Action='Search'). What did you find?"
    })
    
    # Step 10: Get questions for document_index
    messages.append({
        "role": "user",
        "content": "Based on what we've found so far, please write 3 specific questions we should ask using document_index(Action='Ask'). Just list the questions - don't ask them yet."
    })
    
    # Steps 11-13: Ask the questions one at a time
    messages.append({
        "role": "user",
        "content": "Let's ask your first question using document_index(Action='Ask'). What response did you get?"
    })
    
    messages.append({
        "role": "user",
        "content": "Now let's ask your second question using document_index(Action='Ask'). What did you learn?"
    })
    
    messages.append({
        "role": "user",
        "content": "Let's ask your third question using document_index(Action='Ask'). What was the response?"
    })
    
    # Step 14: Request final report
    messages.append({
        "role": "user",
        "content": f"""Please write a detailed report summarizing all the research we've done for mapping {field_name}. Include:
        1. Full DDL of potential source tables
        2. Relevant examples from the documentation
        3. Key findings from our data exploration
        4. Any important context from the document Q&A
        
        Save this report to Git in the path: 'mapping_research/{field_name}_source_research.md'
        Make sure to use proper markdown formatting."""
    })
    
    # Step 15: Verify git save and retry if needed
    messages.append({
        "role": "user",
        "content": "Please verify that the report was saved to Git. If it wasn't saved successfully, please try saving it again."
    })

    # Send all messages in sequence
    for message in messages:
        await record_todo_work(todo_id, f"Sending message: {message['content'][:100]}...")
        # Here you would add your actual message sending logic
        # For example:
        # response = await send_message_to_bot(message)
        # await process_response(response)
        
    return "Research completed"