import streamlit as st
import time
import uuid
import re
import os
import base64
import random
import requests
import io
from pathlib import Path
from urllib.parse import urlsplit
from PIL import Image
import html
from textwrap import dedent
from utils import (
    get_bot_details,
    get_slack_tokens,
    get_metadata_cached,
    get_slack_tokens_cached,
    get_metadata,
    get_metadata2,
    get_artifact,
    submit_to_udf_proxy,
    get_response_from_udf_proxy,
)


def locate_url_markup(txt, replace_with=None):
    """
    Locate and optionally replace URL or pseudo-URL markup in a given text.

    This function searches for patterns in the text that match the url or image markdown format
    '[description](url)' or '![description](url)]', where 'url' can be an HTTP, HTTPS, file, or 'sandbox'/'artifact' pseudo-URLs
    (used for special-case handling of file rendering).
    It returns a list of triplets containing the description, URL, and the
    original markup. Optionally, it can replace the found patterns with a specified
    replacement string.

    Args:
        txt (str): The input text containing potential URL markup.
        replace_with (str, optional): A string to replace the found URL markup.
                                      Defaults to None, which means skip replacement.

    Returns:
        tuple: A 3-tuple containing
            (a) a list of triplets and the modified text.
                The triplets are in the form (description, URL, original (full) markup).
            (b) the input text modified by replacing with 'replace_with' is provided.
            (c) a boolean flag 'has_partial' indicating if the end of the text
                *may* potentially look like a prefix of an incomplete URL markup
    """
    pattern = r'(!?\[([^\]]+)\]\(((file|sandbox|artifact):/+[^\)]+)\))'  # regex for strings of the form '[description](url)' and '![description](url)'
                                                                                  # TOOD: support other standard URL schemas (use urllib?)
    matches = re.findall(pattern, txt)
    triplets = [(match[1], match[2], match[0])
                for match in matches]

    # has_partial is true if the last line of the txt may potentially be a partial URL
    i = txt.rfind('\n')
    line = txt[i:] if i > 0 else txt
    i = line.rfind('[')
    if i < 0:
        has_partial = False
    else:
        line = line[i:]
        opened_sqr = line.count('[') > line.count(']')
        opened_rnd = line.count('(') > line.count(')')
        has_partial = opened_sqr or (line.count('(')) == 0 or opened_rnd

    if matches and replace_with is not None:
        txt = re.sub(pattern, replace_with, txt)
    return triplets, txt, has_partial


bot_images = get_metadata_cached("bot_images")
bot_avatar_images = [bot["bot_avatar_image"] for bot in bot_images]
from PIL import Image
import io

@st.cache_data(ttl=10800)  # Cache the result for 3 hours
def resize_image(image_bytes, size=(64, 64)):
    img = Image.open(io.BytesIO(image_bytes))
    img = img.resize(size, Image.LANCZOS)
    buffered = io.BytesIO()
    img.save(buffered, format="PNG")
    return base64.b64encode(buffered.getvalue()).decode()

# Resize bot avatar images
bot_avatar_images = [
    resize_image(base64.b64decode(img)) if img else None
    for img in bot_avatar_images
]

class ChatMessage:
    def __init__(self, role: str, content: str, is_intro_prompt:bool=False, avatar=None):
        assert role in ("assistant", "user")
        self.role = role
        self.content = content
        self.is_intro_prompt = is_intro_prompt
        self.avatar=avatar


def set_initial_chat_sesssion_data(bot_name, initial_prompt, initial_message):
    """
    Sets the initial chat session data in the session state.

    This function is used to initialize the chat session with a specific bot,
    an initial prompt, and an initial bot message.
    """
    if initial_message:
        # Mark the initial welcome message as an intro prompt so
        # that later the system knows an introductory message is already present.
        initial_message = ChatMessage(role="assistant", content=initial_message, is_intro_prompt=False)
    st.session_state.initial_chat_session_data = dict(
        bot_name=bot_name,
        initial_prompt=initial_prompt,
        initial_message=initial_message
    )


SPEECH_BALLOON_ORD =  128172 # "speech balloon" emoji (💬).

def chat_page():

    # Add custom CSS to reduce whitespace even further
    if 'session_message_uuids' not in st.session_state:
        st.session_state.session_message_uuids = {}
    if 'stream_files' not in st.session_state:
        st.session_state.stream_files = set()
    if 'uploader_key' not in st.session_state:
        st.session_state['uploader_key'] = random.randint(0, 1 << 32)

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
                file_markups, response, has_partial = locate_url_markup(response, replace_with="")
                for file_markups in file_markups:
                    # push any file URLs or pseudo URLs (like 'sandbox', 'artifact' to session. It will be handled later.
                    desc, url, _ = file_markups # locate_url_markup returns triplets
                    st.session_state.stream_files.add(url) # for later rendering
                if has_partial:
                    # We are (very) likely seeing the prefix of a URL, so fetch the next part
                    # and parse it out (if any) before applying any parsing other logic
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
                        new_increment = ""  + response[
                            max(len(previous_response) - 4, 0) : len(response) - offset
                        ]
                    else:
                        if len(response) >= 2 and ord(response[-1]) == SPEECH_BALLOON_ORD:
                            offset = 0
                        else:
                            offset = 0
                        new_increment = response[
                            max(len(previous_response) - 2, 0) : len(response) - offset
                        ]
                    previous_response = response
                    try:
                        if ord(new_increment[-1]) == SPEECH_BALLOON_ORD:
                            new_increment = new_increment[:-2]
                    except:
                        new_increment = ''
                    yield new_increment

            if len(response) < 3 or ord(response[-1]) != SPEECH_BALLOON_ORD:
                break

            if len(response)>=1 and ord(response[-1]) == SPEECH_BALLOON_ORD:
                time.sleep(0.5)

    def emulate_write_stream(text_generator):
        '''
        Emulate the behavior of st.write_stream, with the ability to automatically render objects and HTML-formatted text

        st.write_stream will not automatically render html-formatted text and arbitrary objects in the same way that st.write will.
        This means that if e.g. the bot sends back HTML-formatted text, st.write_stream will render the raw text (e.g. "<html>..").
        So as a workaround we use container.write, which auto-formats HTML and other objects in the same way that st.write does.
        '''
        result = ""
        container = st.empty()
        for chunk in text_generator:
            result += chunk
            container.write(result, unsafe_allow_html=True)
        return result


    def render_url_markup(bot_id, thread_id, bot_avatar_image_url, message_avatar='🤖'):
        '''
        Special handling for url/file rendering: we want to make best effort to render the content of the file
        itself inline in the chat container, if we know how to handle it. Otherwise we wrap it in a generic downloadable link.
        '''
        # Try to render any files pushed into  st.session_state.stream_files

        messages = get_chat_history(thread_id)
        while st.session_state.stream_files:
            url = st.session_state.stream_files.pop()
            url_parts = urlsplit(url)
            file_content64 = None
            known_img_types = {'png', 'jpg', 'jpeg', 'gif', 'svg'}
            known_txt_types = {'plain', 'html', 'txt', 'json', 'text', 'csv'}

            file_path = Path(url_parts.path)
            image_format = None # set if files is a known image type
            text_format = None # set of file content is a known text format (plain or html)
            suffix = file_path.suffix
            # Guess the file format. We first look at the extension of the file and later fallback to the Content-Type header or mime type if available.
            # Note that snowflake-signed URLs (used as external links to artifacts) seems to always return type octet-stream (at least for AWS stages)
            if suffix and suffix.startswith("."):
                subtype = suffix[1:]
                if subtype in known_img_types:
                    image_format = subtype
                elif subtype in known_txt_types:
                    text_format = subtype

            if url_parts.scheme == 'sandbox':
                # special handling for the 'sandbox' URLs which are used for our internal app storage (./runtime/downloaded_files)
                try:
                    file_content64 = get_metadata2('|'.join(('sandbox',bot_id, thread_id, file_path.name)))
                except:
                    pass # best effort failed. fallback to a generic link (which is likely broken)
            elif url_parts.scheme == 'artifact':
                # special handling for the 'artifact' URLs which are used for our account-global permament artifact storage
                try:
                    metadata, file_content64 = get_artifact(file_path.name)
                except Exception as e:
                    # Show an error but leave the url as is.
                    st.error(f"Error fetching artifact {file_path.name}: {e}")
                else:
                    _, msubtype = metadata['mime_type'].split("/")
                    if msubtype in known_img_types:
                        image_format = msubtype
                    elif msubtype in known_txt_types:
                        text_format = msubtype
                    # TODO: handle more mime types, not just images or text. We fallback to a generic href with octet-stream)
            elif url_parts.scheme == 'https' or url_parts.scheme == 'http':
                file_content64 = None
            else:
                # get the content using GET.
                response = requests.get(url)
                if response.status_code == 200:
                    file_data = response.content
                    # Encode to Base64
                    file_content64 = base64.b64encode(file_data).decode('utf-8')
                    if not image_format:
                        # fallback to guessing the content type from the header
                        ctype, csubtype = response.headers['Content-Type'].split('/')
                        #if ctype == "image" and csubtype in known_img_suffixes: # FIXME: this is not working
                        #    image_format = csubtype
                        if ctype == "text" and csubtype in known_txt_types:
                            image_format = csubtype

            # If format not determined from extension or mime type, try to detect from content
            if file_content64 and not image_format and not text_format:
                try:
                    # Try to decode as text first
                    decoded = base64.b64decode(file_content64)
                    try:
                        # Try to decode as UTF-8 text
                        decoded.decode('utf-8')
                        text_format = 'plain'
                    except UnicodeDecodeError:
                        # If not text, check for common image headers
                        if decoded.startswith(b'\x89PNG\r\n'):
                            image_format = 'png'
                        elif decoded.startswith(b'\xff\xd8\xff'):
                            image_format = 'jpeg'
                        elif decoded.startswith(b'GIF87a') or decoded.startswith(b'GIF89a'):
                            image_format = 'gif'
                        elif decoded.startswith(b'<?xml') and b'svg' in decoded[:100]:
                            image_format = 'svg'
                except:
                    pass # If detection fails, will fall back to generic link

            allow_html = False
            markdown = None
            if file_content64 is not None:
                # we have the file content
                assert (image_format is None) or (text_format is None) # they can't both be set
                if image_format:
                    # For common image types, use <img>
                    markdown = f'<img src="data:image/{image_format};base64,{file_content64}" style="max-width: 50%;display: block;">'
                elif text_format:
                    text_content = base64.b64decode(file_content64).decode('utf-8')
                    if text_format in ('plain', 'txt', 'text', 'json', 'csv'):
                        markdown = f'<pre>{text_content}</pre>'
                    else:
                        #text_content = html.escape(text_content)
                        html_content = dedent(f"""
                            <div style="
                                border: 3px solid #888888; /* Medium gray */;
                                padding: 10px;
                                border-radius: 5px;
                                box-shadow: 0px 4px 12px rgba(0, 0, 0, 0.1);
                                padding: 15px;
                                white-space: pre-wrap;
                            ">
                            <p>
                            {text_content}
                            </p>
                            </div>
                            """)
                        markdown = html_content
                else:
                    # for all others, use a generic href
                    markdown = f'<a href="data:application/octet-stream;base64,{file_content64}" download="{file_path.name}">{file_path.name}</a>'
                allow_html = True
            else:
                # not able to download the content. Leave it as a file markdown.
                allow_html = True
                markdown = f'[{url}]({url})'

            # render
            assert markdown is not None
            with st.chat_message("assistant", avatar=bot_avatar_image_url):
                st.markdown(markdown , unsafe_allow_html=allow_html)
            messages.append(ChatMessage(role="assistant", content=markdown, avatar=message_avatar))


    def handle_pending_request(thread_id, request_id):
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

        # Don't create a new chat message if the last message was from the assistant
        # Instead, reuse the existing message container
        should_append = True
        if messages and messages[-1].role == "assistant":
            should_append = False
            # Remove the last message since we'll update it
            messages.pop()

        with st.chat_message("assistant", avatar=bot_avatar_image_url):
            response = emulate_write_stream(response_generator(None, request_id=request_id, selected_bot_id=selected_bot_id))

        st.session_state.stop_streaming = False

        # Only append if we didn't find an existing assistant message to update
        if should_append:
            messages.append(ChatMessage(role="assistant", content=response, avatar='🤖'))

        # render any file/url markups that were pushed into the session
        render_url_markup(selected_bot_id, thread_id, bot_avatar_image_url, '🤖')

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

            return current_llm
        else:
            st.error(f"No LLM configuration found for bot with ID: {selected_bot_id}")
            return current_llm


    def submit_button(prompt,
                      chatmessage,
                      intro_prompt=False,
                      fast_mode_override=False,
                      file=None, # or provide a dict {'filename' : file_name}
                      ):
        """
        Submits a prompt into the current chat session
        """

        current_thread_id = st.session_state["current_thread_id"]
        messages = get_chat_history(current_thread_id)
        if not intro_prompt:
            # Display user message in chat message container
            # Skipped for the intial intro prompt.
            with chatmessage:
                if file:
                    st.write(file['filename'])
                st.markdown(prompt,unsafe_allow_html=True)

        # Add user message to chat history
        if file:
            messages.append(ChatMessage(role="user", content=file['filename'], is_intro_prompt=intro_prompt))
        messages.append(ChatMessage(role="user", content=prompt, is_intro_prompt=intro_prompt))

        # Check if fast mode is selected in the sidebar

        # Get the LLM configuration for the active bot
        llm_configuration = get_llm_configuration(selected_bot_id)

        if (intro_prompt or ('fast_mode' in st.session_state and st.session_state.fast_mode)) and llm_configuration.lower() == 'cortex':
            #st.success("fast mode")
            if prompt is not None:
                prompt += "<<!!FAST_MODE!!>>"

        if prompt is None:
            prompt = 'hello'

        request_id = submit_to_udf_proxy(
            input_text=prompt,
            thread_id=current_thread_id,
            bot_id=selected_bot_id,
            file= file
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
            response = emulate_write_stream(response_generator(in_resp,request_id=request_id, selected_bot_id=selected_bot_id))
        st.session_state.stop_streaming = False

        # Initialize last_response if it doesn't exist
        if "last_response" not in st.session_state:
            st.session_state["last_response"] = ""

        if st.session_state["last_response"] == "":
            st.session_state["last_response"] = response

        messages.append(ChatMessage(role="assistant", content=response, avatar=bot_avatar_image_url))

        # render any file/url markups that were pushed into the session
        render_url_markup(selected_bot_id, current_thread_id, bot_avatar_image_url, bot_avatar_image_url)

        save_chat_history(current_thread_id, messages)

        if current_thread_id in st.session_state.session_message_uuids:
            del st.session_state.session_message_uuids[current_thread_id]


    # --- Page rendering logic starts here -----
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
            # Get bot details. Sort to make sure a bot with 'Eve' in the name is first if exists
            bot_details.sort(key=lambda bot: (not "Eve" in bot["bot_name"], bot["bot_name"]))
            bot_names = [bot["bot_name"] for bot in bot_details]
            bot_ids = [bot["bot_id"] for bot in bot_details]
            bot_intro_prompts_map = {bot["bot_name"]: bot["bot_intro_prompt"]
                                     for bot in bot_details}

            # Fetch available bots
            available_bots = bot_names

            # if we have initial chat session data (set externally, for this bot), use it to set the initial bot name and intro prompt
            initial_chat_session_data = st.session_state.get('initial_chat_session_data')
            if initial_chat_session_data:
                # use this initial data if it matches the current bot name
                initial_bot_name = initial_chat_session_data.get('bot_name')
                if initial_bot_name == st.session_state.get('current_bot'):
                    # override the initial prompt if provided
                    intro_prompt = initial_chat_session_data.get('initial_prompt')
                    if intro_prompt:
                        bot_intro_prompts_map[initial_bot_name] = intro_prompt
                    # set initial bot message, if provided (it is marked as an intro_prompt)
                    initial_bot_message = initial_chat_session_data.get('initial_message')
                    selected_thread_id = st.session_state.get("current_thread_id")
                    if selected_thread_id and initial_bot_message:
                        st.session_state[f"messages_{selected_thread_id}"] = [initial_bot_message]
                # we clear the initial chat session after the first rendering for this page for any bot.
                # This mechanism is used for injecting initial 'action' from the URL and the logic in Genesis.py to set initial chat session data should
                # be in-sync with the initial_chat_session_data for this mechanism to work. Otherwise we silently ignore it.
                st.session_state.initial_chat_session_data = None

            # (File uploader and active chat session UI are provided by Genesis.py.)
            # Retrieve the file uploader value from the main sidebar (set in Genesis.py)
            uploaded_file = st.session_state.get("uploaded_file_main", None)

            # Ensure selected_bot_name and selected_thread_id are defined from session state
            selected_bot_name = st.session_state.get("current_bot", "")
            selected_thread_id = st.session_state.get("current_thread_id", None)

            # Define selected_bot_id based on the available bot details.
            try:
                # Assume bot_names and bot_ids are defined earlier from get_bot_details()
                if selected_bot_name in bot_names:
                    selected_bot_index = bot_names.index(selected_bot_name)
                    selected_bot_id = bot_ids[selected_bot_index]
                else:
                    selected_bot_index = 0
                    selected_bot_id = bot_ids[0] if bot_ids and len(bot_ids) > 0 else None
            except Exception as e:
                selected_bot_id = None

            # Define selected_bot_intro_prompt using the bot_intro_prompts_map if available,
            # otherwise use a default prompt.
            try:
                selected_bot_intro_prompt = bot_intro_prompts_map.get(selected_bot_name,
                    'Briefly introduce yourself and suggest a next step to the user.')
            except Exception:
                selected_bot_intro_prompt = 'Briefly introduce yourself and suggest a next step to the user.'

            # Main chat content area
            # --------------------------------------------
            encoded_bot_avatar_image_array = None
            bot_avatar_image_url = None
            if len(bot_names) > 0:
                # get avatar images
                bot_avatar_image_url = None
                if len(bot_images) > 0:
                    selected_bot_image_index = bot_names.index(selected_bot_name) if selected_bot_name in bot_names else -1
                    if selected_bot_image_index >= 0:
                        # Use the default G logo image for all bots
                        encoded_bot_avatar_image = bot_avatar_images[0]
                        if encoded_bot_avatar_image:
                            encoded_bot_avatar_image_bytes = base64.b64decode(encoded_bot_avatar_image)
                            bot_avatar_image_url = f"data:image/png;base64,{encoded_bot_avatar_image}"

            if selected_thread_id:
                # Initialize chat history if it doesn't exist for the current thread
                if f"messages_{selected_thread_id}" not in st.session_state:
                    st.session_state[f"messages_{selected_thread_id}"] = []

                # Display chat messages from history
                messages = st.session_state[f"messages_{selected_thread_id}"]
                for i, message in enumerate(messages):
                    # Skip intro prompts
                    if message.is_intro_prompt:
                        continue

                    # Skip the last message if there's a pending request OR if it's a duplicate intro message
                    if (i == len(messages)-1 and
                        (selected_thread_id in st.session_state.session_message_uuids or
                         (i > 0 and message.content == messages[i-1].content))):  # Check for duplicate content
                        continue

                    if message.role == "assistant" and bot_avatar_image_url is not None:
                        with st.chat_message(message.role, avatar=bot_avatar_image_url):
                            st.markdown(message.content, unsafe_allow_html=True)
                    else:
                        with st.chat_message(message.role):
                            st.markdown(message.content, unsafe_allow_html=True)

                # Check if there's a pending request for the current session
                if selected_thread_id in st.session_state.session_message_uuids:
                    pending_request_id = st.session_state.session_message_uuids[selected_thread_id]
                    handle_pending_request(selected_thread_id, pending_request_id)

                # (File uploader has been moved to the sidebar)

                # React to user input (this will append to `messages`)
                if prompt := st.chat_input("What is up?", key=f"chat_input_{selected_thread_id}"):
                    file = {}
                    if uploaded_file:
                        bytes_data = base64.b64encode(uploaded_file.read()).decode()
                        st.session_state['uploader_key'] = random.randint(0, 1 << 32)
                        file = {'filename': uploaded_file.name, 'content': bytes_data}
                    submit_button(prompt,
                                  st.chat_message("user"),
                                  intro_prompt=False,
                                  file=file)

                # After rendering any existing messages
                messages = get_chat_history(selected_thread_id)
                # Only auto-submit the intro prompt if:
                #   1. No user message exists ‒ meaning the user hasn't spoken yet.
                #   2. No intro prompt (assistant message marked as intro) exists.
                if not any(m.role == "user" for m in messages) and not any(m.is_intro_prompt for m in messages):
                    # Use a thread-specific flag to ensure we only send it once per thread
                    intro_flag_key = f"intro_prompt_sent_{selected_thread_id}"
                    if intro_flag_key not in st.session_state:
                        st.session_state[intro_flag_key] = False
                    if not st.session_state[intro_flag_key]:
                        submit_button(selected_bot_intro_prompt, st.empty(), intro_prompt=True)
                        st.session_state[intro_flag_key] = True

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
