import streamlit as st
import time
import uuid
from utils import get_bot_details, get_slack_tokens, get_slack_tokens_cached, get_metadata, get_metadata2, submit_to_udf_proxy, get_response_from_udf_proxy
import re
import os


def file_to_html(bot_id, thread_id, file_path):    
    file_name = os.path.basename(file_path)    
    file_byte = get_metadata2('|'.join(('sandbox',bot_id, thread_id, file_name)))
    if 'png' in file_name or not os.path.splitext(file_path)[1]:
        href = f'<img src="data:image/png;base64,{file_byte}" style="max-width: 50%;display: block;">'    
    else:
        href = f'<a href="data:application/octet-stream;base64,{file_byte}" download="{file_name}">{file_name}</a>'
    return href

bot_images = get_metadata("bot_images")
bot_avatar_images = [bot["bot_avatar_image"] for bot in bot_images]


def chat_page():
    # Add custom CSS to reduce whitespace even further

    if 'session_message_uuids' not in st.session_state:
        st.session_state.session_message_uuids = {}
    if 'stream_files' not in st.session_state:
        st.session_state.stream_files = set()

    def get_chat_history(thread_id):
        return st.session_state.get(f"messages_{thread_id}", [])

    def save_chat_history(thread_id, messages):
        st.session_state[f"messages_{thread_id}"] = messages
        

        # Display assistant response in chat message container
    def response_generator(in_resp=None, request_id=None, selected_bot_id=None):
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
                found_partial = False
                full_files = re.findall(r"\n*\[.*\]\(sandbox:/mnt/data(?:/downloads)?/.*?\)", response) if 'sandbox' in response else []
                partial_files = re.findall('\n*\[', response)                      
                if full_files:     
                    for file in full_files:     
                        file_path = re.findall('\((.+)\)', file)
                        if not file_path: 
                            found_partial = True
                            break
                        response = response.replace(file, '')  
                        st.session_state.stream_files.add(file_path[0])                  
                elif partial_files:
                    found_partial = True
                
                if found_partial:
                    continue
                
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

    def handle_pending_request(thread_id, request_id ):
        messages = get_chat_history(thread_id)
        
        response = ""
        with st.spinner("Fetching pending response..."):
            i = 0
            while (
             (   response == ""
                or response == "not found"
                or (response == "!!EXCEPTION_NEEDS_RETRY!!" and i < 6))
                and i < 16
            ):
                response = get_response_from_udf_proxy(
                    uu=request_id, bot_id=selected_bot_id
                )
                if response == "" or response == "not found":
                    time.sleep(0.5)
                    i += 1
                if response == "!!EXCEPTION_NEEDS_RETRY!!":
                    i += 1
                    st.write(f"waiting 2 seconds after exception for retry #{i} of 5")
                    time.sleep(2)

        if i >= 5:
            # st.error("Error reading the UDF response... reloading in 2 seconds...")
            time.sleep(2)
            st.rerun()
        
        # Initialize stop flag in session state
        if "stop_streaming" not in st.session_state:
            st.session_state.stop_streaming = False

        with st.chat_message("assistant", avatar=bot_avatar_image_url):
            response = st.write_stream(response_generator(None,request_id=request_id, selected_bot_id=selected_bot_id))
        
        st.session_state.stop_streaming = False

        # Add the response to the chat history
        messages.append({"role": "assistant", "content": response,  "avatar": bot_avatar_image_url})

        while st.session_state.stream_files:
            file_path = st.session_state.stream_files.pop()
            file = file_to_html(selected_bot_id, thread_id, file_path)
            with st.chat_message("assistant", avatar=bot_avatar_image_url):
                st.markdown(file , unsafe_allow_html=True)
            messages.append({"role": "assistant", "content": file,  "avatar": bot_avatar_image_url})

        save_chat_history(thread_id, messages)

        # Clear the UUID for this session
        del st.session_state.session_message_uuids[thread_id]


    @st.cache_data(ttl=3000) 
    def get_llm_configuration(selected_bot_id):

        current_llm = 'unknown'
        bot_llms = get_metadata("bot_llms")
        if len(bot_llms) > 0:
            for bot_id, llm_info in bot_llms.items():
                if bot_id == selected_bot_id:
                    current_llm = llm_info.get('current_llm')

            return (current_llm)
        else:
            st.error(f"No LLM configuration found for bot with ID: {selected_bot_id}")
            return {}
        

    def submit_button(prompt, chatmessage, intro_prompt=False, fast_mode_override=False):
        current_thread_id = st.session_state["current_thread_id"]
        messages = get_chat_history(current_thread_id)

        if not intro_prompt:
            # Display user message in chat message container
            with chatmessage:
                st.markdown(prompt,unsafe_allow_html=True)
            # Add user message to chat history
            messages.append({"role": "user", "content": prompt})

        # Check if fast mode is selected in the sidebar

        # Get the LLM configuration for the active bot
        llm_configuration = get_llm_configuration(selected_bot_id)

        if intro_prompt or (('fast_mode' in st.session_state and st.session_state.fast_mode) and llm_configuration != 'openai'):
            #st.success("fast mode")
            prompt += "<<!!FAST_MODE!!>>"

        request_id = submit_to_udf_proxy(
            input_text=prompt,
            thread_id=current_thread_id,
            bot_id=selected_bot_id,
        )

       # Store the request_id for this session
        st.session_state.session_message_uuids[current_thread_id] = request_id
        # Display success message with the request_id
 
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

        if i >= 5 and response :
            # st.error("Error reading the UDF response... reloading in 2 seconds...")
            time.sleep(2)
            st.rerun()

        in_resp = response

        # Initialize stop flag in session state
        if "stop_streaming" not in st.session_state:
            st.session_state.stop_streaming = False

        with st.chat_message("assistant", avatar=bot_avatar_image_url):
            response = st.write_stream(response_generator(in_resp,request_id=request_id, selected_bot_id=selected_bot_id))
        st.session_state.stop_streaming = False

        # Initialize last_response if it doesn't exist
        if "last_response" not in st.session_state:
            st.session_state["last_response"] = ""

        if st.session_state["last_response"] == "":
            st.session_state["last_response"] = response

        messages.append({"role": "assistant","content": response,"avatar": bot_avatar_image_url})

        while st.session_state.stream_files:
            file_path = st.session_state.stream_files.pop()
            file = file_to_html(selected_bot_id, current_thread_id, file_path)
            with st.chat_message("assistant", avatar=bot_avatar_image_url):
                st.markdown(file , unsafe_allow_html=True)
            messages.append({"role": "assistant", "content": file,  "avatar": bot_avatar_image_url})

        save_chat_history(current_thread_id, messages)

        if current_thread_id in st.session_state.session_message_uuids:
            del st.session_state.session_message_uuids[current_thread_id]

    try:
        # Initialize last_response if it doesn't exist
        if "last_response" not in st.session_state:
            st.session_state["last_response"] = ""

        bot_details = get_bot_details()
    except Exception as e:
        bot_details = {"Success": False, "Message": "Genesis Server Offline"}
        return

    if bot_details == {"Success": False, "Message": "Needs LLM Type and Key"}:
        st.session_state["radio"] = "LLM Model & Key"
        st.rerun()
    else:
        try:
            # get bot details
            bot_details.sort(key=lambda x: (not "Janice" in x["bot_name"], x["bot_name"]))
            bot_names = [bot["bot_name"] for bot in bot_details]
            bot_ids = [bot["bot_id"] for bot in bot_details]
            bot_intro_prompts = [bot["bot_intro_prompt"] for bot in bot_details]

            # Fetch available bots
            available_bots = bot_names

            # Set Eve as the default bot if it exists
            default_bot = "Janice" if "Janice" in available_bots else available_bots[0] if available_bots else None

            # Initialize current_bot and current_thread_id if they don't exist
            if 'current_bot' not in st.session_state or 'current_thread_id' not in st.session_state:
                st.session_state.current_bot = default_bot if default_bot else (bot_names[1] if len(bot_names) > 1 else bot_names[0])
                new_thread_id = str(uuid.uuid4())
                st.session_state.current_thread_id = new_thread_id
                new_session = f"🤖 {st.session_state.current_bot} ({new_thread_id[:8]})"
                
                # Initialize active_sessions if it doesn't exist
                if 'active_sessions' not in st.session_state:
                    st.session_state.active_sessions = []
                # Initialize current_session
                st.session_state.current_session = new_session
                # Add the new session to active_sessions
                if new_session not in st.session_state.active_sessions:
                    st.session_state.active_sessions.append(new_session)

                
                # Initialize chat history for the new thread
                st.session_state[f"messages_{new_thread_id}"] = []

            # Sidebar content
            with st.sidebar:
                if len(bot_names) > 0:
              #      st.markdown("### Start a New Chat")
                    with st.form(key='new_chat_form'):
                        selected_bot = st.selectbox("Start new chat with:", available_bots)
                        st.write('   ')
                        col1, col2 = st.columns([2, 1])
                        with col1:
                            st.markdown('<span id="button-after"></span>', unsafe_allow_html=True)
                            start_chat = st.form_submit_button(" ⚡ Start New Chat")
                        with col2:
                            st.markdown('<span id="button-after"></span>', unsafe_allow_html=True)
                            refresh = st.form_submit_button("🔄 Bots")

                    if refresh:
                        get_bot_details.clear()
                        get_llm_configuration.clear()
                        st.rerun()
                    if start_chat:
                        # Create a new chat session for the selected bot
                        new_thread_id = str(uuid.uuid4())
                        new_session = f"🤖 {selected_bot} ({new_thread_id[:8]})"
                            
                        # Add the new session to active_sessions
                        if 'active_sessions' not in st.session_state:
                            st.session_state.active_sessions = []
                        if new_session not in st.session_state.active_sessions:
                            st.session_state.active_sessions.append(new_session)
                            st.session_state.new_session_added = True
                        
                        # Update the current thread ID and bot
                        st.session_state["current_thread_id"] = new_thread_id
                        st.session_state["current_bot"] = selected_bot

                        st.session_state.current_session = new_session
                        
                        # Initialize chat history for the new thread
                        st.session_state[f"messages_{new_thread_id}"] = []
                        
                        # Set the flag to trigger a rerun in main.py
                        st.session_state.new_session_added = True
                        
                        # Trigger a rerun to update the UI
                        st.rerun()

                st.markdown("#### Active Chat Sessions:")
                
                # Initialize active_sessions in session state if it doesn't exist
                if 'active_sessions' not in st.session_state:
                    st.session_state.active_sessions = []

                # Display active sessions as clickable links
                if st.session_state.active_sessions:

                    st.markdown(
                        """
                        <style>
                        .element-container:has(style){
                            display: none;
                        }
                        #button-after {
                            display: none;
                        }
                        .element-container:has(#button-after) {
                            display: none;
                        }
                        .element-container:has(#button-after) + div button {
                            background: none;
                            border: none;
                            padding: 0;
                            font: inherit;
                            cursor: pointer;
                            outline: inherit;
                            color: inherit;
                            text-align: left;
                            margin: 0;
                            font-weight: normal;
                            font-size: 0.8em;
                            }
                        .element-container:has(#button-after) + div button {
                            line-height: 0.5;
                            margin-top: -30px;
                            margin-bottom: 0px;
                        }

                        </style>

                        """,
                        unsafe_allow_html=True,
                    )

                    
                    for session in st.session_state.active_sessions:
                        bot_name, thread_id = session.split(' (')
                        bot_name = bot_name.split('🤖 ')[1]
                        thread_id = thread_id[:-1]  # Remove the closing parenthesis
                        full_thread_id = next((key.split('_')[1] for key in st.session_state.keys() if key.startswith(f"messages_{thread_id}")), thread_id)
                        col1, col2 = st.columns([4, 1])
                        with col1:
                        #    st.write("session ", session, ' current session ',  st.session_state.get('current_session'))
                            session_display = f"&nbsp;&nbsp;&nbsp;⚡ {session[2:]}" if session == st.session_state.get('current_session') else f"&nbsp;&nbsp;&nbsp;{session}"
                            st.markdown('<span id="button-after"></span>', unsafe_allow_html=True)
                            if st.button(session_display, key=f"btn_{thread_id}"):
                                st.session_state.current_bot = bot_name
                                st.session_state.selected_session = {
                                    'bot_name': bot_name,
                                    'thread_id': full_thread_id
                                }
                                st.session_state.current_session = session
                                st.session_state.load_history = True
                                st.rerun()
                        with col2:
                            st.markdown('<span id="button-after"></span>', unsafe_allow_html=True)
                            if st.button("⨂", key=f"remove_{thread_id}"):
                                st.session_state.active_sessions.remove(session)
                                if f"messages_{full_thread_id}" in st.session_state:
                                    del st.session_state[f"messages_{full_thread_id}"]
                                if st.session_state.get('current_session') == session:
                                    st.session_state.pop('current_session', None)
                                st.rerun()
                else:
                    st.info("No active chat sessions.")

                # Ensure only one mode is active at a time
                # Add toggle for fast mode
                # Initialize fast_mode in session state if it doesn't exist
  
                # Check if a session is selected from the sidebar
                if 'selected_session' in st.session_state:
                    selected_session = st.session_state.selected_session
                    selected_bot_name = selected_session['bot_name']
                    selected_thread_id = selected_session['thread_id']
                    st.session_state.current_bot = selected_bot_name
                    st.session_state.current_thread_id = selected_thread_id
                    del st.session_state.selected_session
                else:
                    selected_bot_name = st.session_state.current_bot
                    selected_thread_id = st.session_state.get("current_thread_id")

                selected_bot_index = bot_names.index(selected_bot_name)
                selected_bot_id = bot_ids[selected_bot_index]
                selected_bot_intro_prompt = bot_intro_prompts[selected_bot_index]
                if selected_bot_intro_prompt is None:
                    selected_bot_intro_prompt = 'Briefly introduce yourself and suggest a next step to the user.'

                selected_bot_id = bot_ids[selected_bot_index]
                llm_configuration = get_llm_configuration(selected_bot_id)

                if llm_configuration != 'openai':
                # Create the toggle and update session state when changed
                    fast_mode = st.toggle("Fast Mode", value=True, key='fast_mode')

                    if fast_mode:
                        st.info("Using faster LLM: Llama3.1-70b")

                tokens = get_slack_tokens_cached()
                slack_active = tokens.get("SlackActiveFlag", False)
                if not slack_active:
                    st.markdown("#### Genesis is best used on Slack!")
                    st.markdown("  ")
     
                    if "radio" in st.session_state:
                        if st.session_state["radio"] != "Setup Slack Connection":
                            st.markdown('<span id="button-after"></span>', unsafe_allow_html=True)
                            if st.button("&nbsp;&nbsp;&nbsp;⚡ Activate Slack keys here"):
                                st.session_state["radio"] = "Setup Slack Connection"
                                st.rerun()
                    else:
                        st.markdown('<span id="button-after"></span>', unsafe_allow_html=True)
                        if st.button("&nbsp;&nbsp;&nbsp;⚡ Activate Slack keys here"):
                            st.session_state["radio"] = "Setup Slack Connection"
                            st.rerun()


            # Main content area       

            if len(bot_names) > 0:

                # get avatar images
                bot_avatar_image_url = None
                if len(bot_images) > 0:                    
                    selected_bot_image_index = bot_names.index(selected_bot_name) if selected_bot_name in bot_names else -1
                    if selected_bot_image_index >= 0:
                        encoded_bot_avatar_image = bot_avatar_images[selected_bot_image_index]
                        if encoded_bot_avatar_image:
                            bot_avatar_image_url = f"data:image/png;base64,{encoded_bot_avatar_image}"

                # Initialize chat history if it doesn't exist for the current thread
                if selected_thread_id and f"messages_{selected_thread_id}" not in st.session_state:
                    st.session_state[f"messages_{selected_thread_id}"] = []

                # Display chat messages from history
                if selected_thread_id:
                    for message in st.session_state[f"messages_{selected_thread_id}"]:
                        if message["role"] == "assistant" and bot_avatar_image_url:
                            with st.chat_message(message["role"], avatar=bot_avatar_image_url):
                                st.markdown(message["content"],unsafe_allow_html=True)
                        else:
                            with st.chat_message(message["role"]):
                                st.markdown(message["content"],unsafe_allow_html=True)

                # Check if there's a pending request for the current session
                if selected_thread_id and selected_thread_id in st.session_state.session_message_uuids:
                    pending_request_id = st.session_state.session_message_uuids[selected_thread_id]
                    handle_pending_request(selected_thread_id, pending_request_id)

                # React to user input
                if selected_thread_id:
                    if prompt := st.chat_input("What is up?", key=f"chat_input_{selected_thread_id}"):
                        submit_button(prompt, st.chat_message("user"), False)

                # Generate initial message and bot introduction only for new sessions
                if not st.session_state[f"messages_{selected_thread_id}"]:
                    submit_button(selected_bot_intro_prompt, st.empty(), True)
                
      #          email_popup()

                # Check if 'popup' exists in session state, if not, initialize it to False
  
        except Exception as e:
            st.error(f"Error running Genesis GUI: {e}")

    # Add this at the end of the chat_page function to update the sidebar
    st.session_state.active_sessions = list(set(st.session_state.active_sessions))  # Remove duplicates

    # Set the flag to trigger a rerun in main.py if a new session was added
    if st.session_state.get('new_session_added', False):
        st.session_state.new_session_added = False
        st.success("new session added??")
        st.rerun()
