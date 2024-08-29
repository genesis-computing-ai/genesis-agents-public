import streamlit as st
import time
import uuid
from utils import get_bot_details, get_slack_tokens, get_metadata, submit_to_udf_proxy, get_response_from_udf_proxy

def chat_page():
    def get_chat_history(thread_id):
        return st.session_state.get(f"messages_{thread_id}", [])

    def save_chat_history(thread_id, messages):
        st.session_state[f"messages_{thread_id}"] = messages

    def submit_button(prompt, chatmessage, intro_prompt=False):
        current_thread_id = st.session_state["current_thread_id"]
        messages = get_chat_history(current_thread_id)

        if not intro_prompt:
            # Display user message in chat message container
            with chatmessage:
                st.markdown(prompt)
            # Add user message to chat history
            messages.append({"role": "user", "content": prompt})

        request_id = submit_to_udf_proxy(
            input_text=prompt,
            thread_id=current_thread_id,
            bot_id=selected_bot_id,
        )
        response = ""
        with st.spinner("Thinking..."):
            i = 0
            while (
                response == ""
                or response == "not found"
                or (response == "!!EXCEPTION_NEEDS_RETRY!!" and i < 6)
            ):
                response = get_response_from_udf_proxy(
                    uu=request_id, bot_id=selected_bot_id
                )
                if response == "" or response == "not found":
                    time.sleep(0.5)
                if response == "!!EXCEPTION_NEEDS_RETRY!!":
                    i = i + 1
                    st.write(f"waiting 2 seconds after exception for retry #{i} of 5")
                    time.sleep(2)

        if i >= 5:
            st.error("Error reading the UDF response... reloading in 2 seconds...")
            time.sleep(2)
            st.rerun()

        in_resp = response

        # Initialize stop flag in session state
        if "stop_streaming" not in st.session_state:
            st.session_state.stop_streaming = False

        # Display assistant response in chat message container
        def response_generator(in_resp=None):
            previous_response = ""
            while True:
                if st.session_state.stop_streaming:
                    st.session_state.stop_streaming = False
                    break
                if in_resp is None:
                    response = get_response_from_udf_proxy(
                        uu=request_id, bot_id=selected_bot_id
                    )
                else:
                    response = in_resp
                    in_resp = None
                if response != previous_response:
                    if response != "not found":
                        if ( 
                            len(previous_response) > 10
                            and '...' in previous_response[-6:]
                            and chr(129520) in previous_response[-50:]
                            and (
                                len(response) > len(previous_response)
                            )
                        ):
                            offset = 0
                            new_increment = "\n\n"  + response[
                                max(len(previous_response) - 2, 0) : len(response) - offset
                            ]
                        else:
                            if len(response) >= 2 and ord(response[-1]) == 128172:
                                offset = -2
                            else:
                                offset = 0
                            new_increment = response[
                                max(len(previous_response) - 2, 0) : len(response) - offset
                            ]
                        previous_response = response
                        try:
                            if ord(new_increment[-1]) == 128172:
                                new_increment = new_increment[:-2]
                        except:
                            new_increment = ''
                        yield new_increment

                if len(response) < 3 or ord(response[-1]) != 128172:
                    break

                if len(response)>=1 and ord(response[-1]) == 128172:
                    time.sleep(0.5)

        if bot_avatar_image_url:
            with st.chat_message("assistant", avatar=bot_avatar_image_url):
                response = st.write_stream(response_generator(in_resp))
        else:
            with st.chat_message("assistant"):
                response = st.write_stream(response_generator(in_resp))
        st.session_state.stop_streaming = False

        # Initialize last_response if it doesn't exist
        if "last_response" not in st.session_state:
            st.session_state["last_response"] = ""

        if st.session_state["last_response"] == "":
            st.session_state["last_response"] = response

        if bot_avatar_image_url:
            messages.append({
                "role": "assistant",
                "content": response,
                "avatar": bot_avatar_image_url,
            })
        else:
            messages.append({"role": "assistant", "content": response})

        save_chat_history(current_thread_id, messages)

    try:
        # Initialize last_response if it doesn't exist
        if "last_response" not in st.session_state:
            st.session_state["last_response"] = ""

        bot_details = get_bot_details()
    except Exception as e:
        bot_details = {"Success": False, "Message": "Genesis Server Offline"}
        return

    if bot_details == {"Success": False, "Message": "Needs LLM Type and Key"}:
        from pages.llm_config import llm_config
        llm_config()
    else:
        try:
            # get bot details
            bot_details.sort(key=lambda x: (not "Eve" in x["bot_name"], x["bot_name"]))
            bot_names = [bot["bot_name"] for bot in bot_details]
            bot_ids = [bot["bot_id"] for bot in bot_details]
            bot_intro_prompts = [bot["bot_intro_prompt"] for bot in bot_details]

            tokens = get_slack_tokens()
            slack_active = tokens.get("SlackActiveFlag", False)
            if not slack_active:
                col1, col2 = st.columns([3, 4])
                with col1:
                    st.markdown("##### Genesis is best used on Slack!")
                with col2:
                    if "radio" in st.session_state:
                        if st.session_state["radio"] != "Setup Slack Connection":
                            if st.button("Activate Slack Keys Here"):
                                st.session_state["radio"] = "Setup Slack Connection"
                                st.rerun()
                    else:
                        if st.button("Activate Slack Keys Here"):
                            st.session_state["radio"] = "Setup Slack Connection"
                            st.rerun()
            if len(bot_names) > 0:
                col1, col2 = st.columns([3, 1])
                with col1:
                    selected_bot_name = st.selectbox("Active Bots", bot_names, key="bot_selector")
                with col2:
                    if st.button("New Chat", key="new_chat_button"):
                        # Create a new chat session for the selected bot
                        new_thread_id = str(uuid.uuid4())
                        new_session = f"Chat with {selected_bot_name} ({new_thread_id[:8]})"
                        
                        # Add the new session to active_sessions
                        if 'active_sessions' not in st.session_state:
                            st.session_state.active_sessions = []
                        if new_session not in st.session_state.active_sessions:
                            st.session_state.active_sessions.append(new_session)
                            st.session_state.new_session_added = True
                        
                        # Update the current thread ID
                        st.session_state["current_thread_id"] = new_thread_id
                        
                        # Initialize chat history for the new thread
                        st.session_state[f"messages_{new_thread_id}"] = []
                        
                        # Update the previous bot name
                        st.session_state["previous_bot_name"] = selected_bot_name
                        
                        # Set the flag to trigger a rerun in main.py
                        st.session_state.new_session_added = True
                        
                        # Trigger a rerun to update the UI
                        st.rerun()

                # Check if a session is selected from the sidebar
                loading_existing_session = False
                if 'selected_session' in st.session_state:
                    selected_session = st.session_state.selected_session
                    selected_bot_name = selected_session['bot_name']
                    selected_thread_id = selected_session['thread_id']
                    loading_existing_session = True
                    del st.session_state.selected_session
                else:
                    previous_bot_name = st.session_state.get("previous_bot_name")
                    selected_bot_index = bot_names.index(selected_bot_name)
                    
                    # Check if a new bot has been selected or if there's no current thread
                    if selected_bot_name != previous_bot_name or "current_thread_id" not in st.session_state:
                        # Create a new chat session for the newly selected bot
                        selected_thread_id = str(uuid.uuid4())
                        new_session = f"Chat with {selected_bot_name} ({selected_thread_id[:8]})"
                        
                        # Add the new session to active_sessions
                        if 'active_sessions' not in st.session_state:
                            st.session_state.active_sessions = []
                        if new_session not in st.session_state.active_sessions:
                            st.session_state.active_sessions.append(new_session)
                            st.session_state.new_session_added = True
                    else:
                        # Use the existing thread ID
                        selected_thread_id = st.session_state.get("current_thread_id")

                # Update the session state
                st.session_state["current_thread_id"] = selected_thread_id
                st.session_state["previous_bot_name"] = selected_bot_name

                selected_bot_index = bot_names.index(selected_bot_name)
                selected_bot_id = bot_ids[selected_bot_index]
                selected_bot_intro_prompt = bot_intro_prompts[selected_bot_index]

                # get avatar images
                bot_images = get_metadata("bot_images")
                bot_avatar_image_url = ""
                if len(bot_images) > 0:
                    bot_avatar_images = [bot["bot_avatar_image"] for bot in bot_images]
                    bot_names = [bot["bot_name"] for bot in bot_images]
                    selected_bot_image_index = bot_names.index(selected_bot_name) if selected_bot_name in bot_names else -1
                    if selected_bot_image_index < 0:
                        bot_avatar_image_url = ""
                    else:
                        encoded_bot_avatar_image = bot_avatar_images[
                            selected_bot_image_index
                        ]
                        if not encoded_bot_avatar_image:
                            bot_avatar_image_url = ""
                        else:
                            # Create data URL for the avatar image
                            bot_avatar_image_url = (
                                f"data:image/png;base64,{encoded_bot_avatar_image}"
                            )

                # Initialize chat history if it doesn't exist for the current thread
                if f"messages_{selected_thread_id}" not in st.session_state:
                    st.session_state[f"messages_{selected_thread_id}"] = []

                # Generate initial message and bot introduction only for new sessions
                if not loading_existing_session and not st.session_state[f"messages_{selected_thread_id}"]:
                    # Generate initial message
                    initial_message = f"Hi, I'm {selected_bot_name}! How can I help you today?"
                    if bot_avatar_image_url:
                        st.session_state[f"messages_{selected_thread_id}"].append({
                            "role": "assistant",
                            "content": initial_message,
                            "avatar": bot_avatar_image_url,
                        })
                    else:
                        st.session_state[f"messages_{selected_thread_id}"].append({
                            "role": "assistant",
                            "content": initial_message,
                        })
                    # Generate bot's introduction
                    submit_button(selected_bot_intro_prompt, st.empty, True)

                # Display chat messages from history
                for message in st.session_state[f"messages_{selected_thread_id}"]:
                    if message["role"] == "assistant" and bot_avatar_image_url:
                        with st.chat_message(message["role"], avatar=bot_avatar_image_url):
                            st.markdown(message["content"])
                    else:
                        with st.chat_message(message["role"]):
                            st.markdown(message["content"])

                # React to user input
                if prompt := st.chat_input("What is up?", key=f"chat_input_{selected_thread_id}"):
                    submit_button(prompt, st.chat_message("user"), False)
        except Exception as e:
            st.subheader(f"Error running Genesis GUI {e}")

    # Add this at the end of the chat_page function to update the sidebar
    st.session_state.active_sessions = list(set(st.session_state.active_sessions))  # Remove duplicates

    # Set the flag to trigger a rerun in main.py if a new session was added
    if st.session_state.get('new_session_added', False):
        st.rerun()

    st.sidebar.info(f"Active sessions: {st.session_state.active_sessions}")
    st.sidebar.info(f"Selected bot: {selected_bot_name}")
    st.sidebar.info(f"Thread ID: {st.session_state.get('current_thread_id', 'Not set')}")
    
    # Debug information
    st.sidebar.info(f"All session state keys: {list(st.session_state.keys())}")
    for key in st.session_state.keys():
        if key.startswith("messages_"):
            st.sidebar.info(f"Messages for {key}: {len(st.session_state[key])}")
