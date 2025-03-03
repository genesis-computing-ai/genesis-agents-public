from __future__ import annotations  # for python 9 support of | type operator
from collections import deque
import json

import requests
import functools
from pathlib import Path
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
import os, time
from slack_bolt.adapter.flask import SlackRequestHandler
from flask import jsonify, request
from genesis_bots.core.bot_os_input import BotOsInputAdapter, BotOsInputMessage, BotOsOutputMessage
from genesis_bots.core.bot_os_artifacts import ARTIFACT_ID_REGEX, get_artifacts_store
from genesis_bots.connectors import get_global_db_connector

from genesis_bots.core.logging_config import logger, logging, LogSupressor
import threading
import random
import re
import datetime


# module level
meta_lock = threading.Lock()
thread_ts_dict = {}
uniq = random.randint(100000, 999999)

logger.info("     ┌───────┐     ")
logger.info("    ╔═════════╗    ")
logger.info("   ║  ◉   ◉  ║   ")
logger.info("  ║    ───    ║  ")
logger.info(" ╚═══════════╝ ")
logger.info("     ╱     ╲     ")
logger.info("    ╱│  ◯  │╲    ")
logger.info("   ╱ │_____│ ╲   ")
logger.info("      │   │      ")
logger.info("      │   │      ")
logger.info("     ╱     ╲     ")
logger.info("    ╱       ╲    ")
logger.info("   ╱         ╲   ")
logger.info("  G E N E S I S ")
logger.info("    B o t O S")
logger.info("")
logger.info(f"Instantiation Code--->{uniq}")


class SlackBotAdapter(BotOsInputAdapter):

    def __init__(
        self,
        token: str,
        signing_secret: str,
        channel_id: str | None,
        bot_user_id: str,
        bot_name: str = "Unknown",
        slack_app_level_token=None,
        bolt_app_active=True,
        legacy_sessions=[]
    ) -> None:
        logger.info(f"Initializing SlackBotAdapter for {bot_name}")
        try:
            logger.debug("Calling parent class constructor")
            super().__init__()

            logger.debug("Initializing Slack App")
            self.slack_app = App(token=token, signing_secret=signing_secret)

            logger.debug("Setting basic attributes")
            self.channel_id = channel_id
            self.handler = SlackRequestHandler(self.slack_app)
            self.events = deque()
            self.bot_user_id = bot_user_id
            self.user_info_cache = {}
            self.bot_name = bot_name

            logger.debug("Initializing tracking dictionaries")
            self.last_message_id_dict = {}
            self.thinking_map = {}
            self.events_map = {}
            self.handled_events = {}
            self.chunk_start_map = {}
            self.chunk_last_100 = {}
            self.thinking_msg_overide_map = {}
            self.in_markdown_map = {}
            self.finalized_threads = {}
            self.split_at = 3700  # use 3700 normally
            self.legacy_sessions = legacy_sessions

            logger.debug("Setting thread-related attributes to None")
            self.slack_thread = None # initialized later
            self.slack_thread_shutdown_event = None # # initialized later
            self.slack_socket_mode_handler = None # initialized later

            logger.debug("Setting up log suppressors")
            for msg_re_to_suppress in [r"Failed to establish a connection", r"on_error invoked \(session id"]:
                LogSupressor.add_supressor(
                    self.slack_app.logger.name,
                    log_level=logging.ERROR,
                    regexp=msg_re_to_suppress,
                    n=100
                )

            logger.debug("Creating events lock")
            self.events_lock = threading.Lock()

            if slack_app_level_token and bolt_app_active:
                logger.info("Initializing Socket Mode with app-level token")
                try:
                    self.slack_app_level_token = slack_app_level_token

                    # Initialize Slack Bolt app
                    self.slack_socket = App(token=slack_app_level_token)

                    # Define Slack event handlers
                    @self.slack_socket.event("message")
                    def handle_message_events(ack, event, say):
                        ack()
                        # TODO, clear this after 30 min
                        if event.get("subtype", None) == "message_changed":
                            msg = event["message"].get("text", None)
                            thread_ts = event["message"].get("thread_ts", None)
                            user_id = event["message"].get("user", "NO_USER")
                            txt = msg[:30]
                        else:
                            #              if self.handled_events.get(event['ts'],False) == True:
                            #                  return
                            msg = event.get("text", "")
                            thread_ts = event.get("thread_ts", event.get("ts", ""))
                            user_id = event.get("user", "NO_USER")
                            txt = event.get("text", "no text")[:30]
                        #             self.handled_events[event['ts']]=True
                        if len(txt) == 50:
                            txt = txt + "..."
                        if (
                            msg != "no text"
                            and msg != "_thinking..._"
                            and msg[:10] != ":toolbox: "
                            and len(self.events) > 100    # change to 1 for testing
                        ):
                            logger.info(
                                f'{self.bot_name} slack_in {event.get("type","no type")[:50]}, queue len {len(self.events)+1}'
                            )
                        if self.bot_user_id == user_id:
                            self.last_message_id_dict[event.get("thread_ts", None)] = event.get(
                                "ts", None
                            )
                        # removed  event.get("subtype","none") != 'message_changed' to allow other bots to see streams from other bots
                        # may want to ignore messages that changed but have original timestamps more than 1 few minutes ago
                        if (
                            not msg.endswith("💬")
                            and not msg.endswith(":speech_balloon:")
                            and msg != "_thinking..._"
                            and msg[:10] != ":toolbox: "
                            and self.bot_user_id != user_id
                            and event.get("subtype", "none") != "message_deleted"
                        ):
                            with self.events_lock:
                                self.events.append(event)
                                self.events_map[event.get("ts", None)] = {
                                    "event": event,
                                    "datetime": datetime.datetime.now().isoformat(),
                                }
                                if random.randint(1, 100) == 1:
                                    current_time = datetime.datetime.now()
                                    thirty_minutes_ago = current_time - datetime.timedelta(
                                        minutes=30
                                    )
                                    for event_ts, event_info in list(self.events_map.items()):
                                        event_time = datetime.datetime.fromisoformat(
                                            event_info["datetime"]
                                        )
                                        if event_time < thirty_minutes_ago:
                                            del self.events_map[event_ts]
                                    for thinking_ts, thinking_info in list(
                                        self.thinking_map.items()
                                    ):
                                        thinking_time = datetime.datetime.fromisoformat(
                                            thinking_info["datetime"]
                                        )
                                        if thinking_time < thirty_minutes_ago:
                                            del self.thinking_map[thinking_ts]

                    @self.slack_socket.event("app_mention")
                    def mention_handler(event, say):
                        pass

                    @self.slack_socket.action({"action_id": re.compile(".*")})
                    def handle_all_block_actions(ack, body, client):
                        ack()
                        logger.info(f"Block action received: {body}")
                        event = {}
                        event["text"] = f"Block action received: {body['actions']}"
                        event["user"] = body["user"]["id"]
                        event["thread_ts"] = body["message"]["thread_ts"]
                        event["ts"] = body["message"]["ts"]
                        event["channel_type"] = body["channel"]["name"]
                        event["channel"] = body["channel"]["id"]
                        with self.events_lock:
                            self.events.append(event)

                    def run_slack_app():
                        # runs the event loop and blocks on an event. This emulates the .start() method
                        self.slack_socket_mode_handler.connect()
                        self.slack_thread_shutdown_event.wait()

                    # Run Slack app in a separate thread
                    self.slack_socket_mode_handler = SocketModeHandler(self.slack_socket, slack_app_level_token)
                    self.slack_thread = threading.Thread(target=run_slack_app)
                    self.slack_thread_shutdown_event = threading.Event() # used for signalling the thread to terminate
                    self.slack_thread.start()

                except Exception as e:
                    logger.error(f"Failed to initialize socket mode: {str(e)}", exc_info=True)
                    raise

            logger.info(f"Successfully initialized SlackBotAdapter for {bot_name}")

        except Exception as e:
            logger.error(f"Failed to initialize SlackBotAdapter: {str(e)}", exc_info=True)
            raise


    def shutdown(self):
        """
        Attempts to shut down the SlackBotAdapter by closing the Slack socket mode handler
        and terminating the dedicated thread
        """
        if self.slack_socket_mode_handler:
            self.slack_socket_mode_handler.close()
        self.slack_thread_shutdown_event.set() # thread should exit now.
        if self.slack_thread and self.slack_thread.is_alive():
            timeout = 1 # 1sec
            self.slack_thread.join(timeout=timeout)  # Wait for the thread to finish gracefully
            if self.slack_thread.is_alive():
                logger.warn(f"Unable to gracefully shut down Slack handler thread for {self.__class__.__name__} for bot name {self.bot_name} ({timeout=})")


    @functools.cached_property
    def db_connector(self):
        return get_global_db_connector()


    def add_event(self, event):
        self.events.append(event)

    def add_back_event(self, metadata=None):
        event_ts = metadata["event_ts"],
        event = self.events_map.get(event_ts, {}).get("event", None)
        if event is not None:
            self.events.append(event)

    def handle_message_events_old(self, event, context, say, logger):
        logger.info(event)  # Log the event data (optional)
        text = event.get("text", "")
        # logger.info('AT HANDLE MESSAGE EVENTS???')
        logger.debug(f"SlackBotAdapter:handle_message_events - {text}")
        thread_ts = event.get("thread_ts", event.get("ts", ""))
        channel_type = event.get("channel_type", "")
        if (
            f"<@{self.bot_user_id}>" in text
            or (self.bot_user_id, thread_ts) in thread_ts_dict
            or (channel_type == "im" and text != "")
        ):
            with self.events_lock:
                self.events.append(event)
            if (self.bot_user_id, thread_ts) not in thread_ts_dict:
                with meta_lock:
                    thread_ts_dict[self.bot_user_id, thread_ts] = {
                        "event": event,
                        "thread_id": None,
                    }
        else:
            logger.debug(f"SlackBotAdapter:handle_message_events - no mention skipped")

    # callback from Flask
    def slack_events(self):
        data = request.json
        # Check if this is a challenge request
        if data is not None and data["type"] == "url_verification":
            # Respond with the challenge value
            return jsonify({"challenge": data["challenge"]})
        #        return self.handler.handle(request)
        return self.handler.handle(request)

    def _download_slack_files(self, event, thread_id="no_thread") -> list:
        files = []
        for file_info in event["files"]:
            # logger.info('... download_slack_files ',file_info)
            url_private = file_info.get("url_private")
            file_name = file_info.get("name")
            if url_private and file_name:
                local_path = f"./runtime/downloaded_files/{thread_id}/{file_name}"
                #  logger.info('... downloading slack file ',file_name,' from ',url_private,' to ',local_path)
                os.makedirs(os.path.dirname(local_path), exist_ok=True)
                try:
                    with requests.get(
                        url_private,
                        headers={"Authorization": "Bearer %s" % self.slack_app._token},
                        stream=True,
                    ) as r:
                        # Raise an exception for bad responses
                        r.raise_for_status()
                        # Open a local file with write-binary mode
                        #      logger.info('... saving locally to ',local_path)
                        with open(local_path, "wb") as f:
                            # Write the content to the local file
                            for chunk in r.iter_content(chunk_size=32768):
                                f.write(chunk)  # Raise an exception for bad responses
                        #      f.write(r.content)

                        files.append(local_path)
                #        logger.info('... download_slack_files downloaded ',local_path)
                except Exception as e:
                    logger.info(f"Error downloading file from {url_private}: {e}")
        return files

    # abstract method from BotOsInputAdapter

    def get_user_info(self, user_id):
        user_info = self.slack_app.client.users_info(user=user_id)
        if user_id in self.user_info_cache:
            return self.user_info_cache[user_id]
        user_email     = user_info["user"].get('profile', {}).get('email', 'unknown_email')
        user_full_name = user_info["user"].get('profile', {}).get('real_name', 'unknown_name')
        self.user_info_cache[user_id] = (user_full_name, user_email)
        return self.user_info_cache[user_id]


    def get_input(
        self, thread_map=None, active=None, processing=None, done_map=None
    ) -> BotOsInputMessage | None:
        # logger.info(f"SlackBotAdapter:get_input")
        files = []

     #    logger.info(self.bot_name)
        with self.events_lock:
            if len(self.events) == 0:
                return None
            try:
                event = self.events.popleft()
            except IndexError:
                return None

        if event.get("subtype", None) == "message_changed":
            msg = event["message"]["text"]
            thread_ts = event["message"].get("thread_ts", None)
            if event["previous_message"].get("text", None) == msg:
                done_map[event["ts"]] = True
                return None
        else:
            msg = event.get("text", "")
            thread_ts = event.get("thread_ts", event.get("ts", ""))


        if thread_map is not None:
            openai_thread = thread_map.get(thread_ts, None)

        if done_map.get(event.get("ts", "")) == True:
            logger.info(f"*****!!! Resubmission zapped")
            return

        if thread_map is not None and processing is not None and active is not None:
            if (openai_thread in active or openai_thread in processing) and msg.strip().lower() not in ["!stop", "stop"]:
                self.events.append(event)
                return None

        if event["ts"] in self.thinking_map:
            input_message = self.thinking_map[event["ts"]]["input_message"]
            logger.info(f"***** Resubmission {input_message.msg}")
            return input_message

        if msg.strip().lower() == "!delete":
            last_message_id = self.last_message_id_dict.get(thread_ts)
            if last_message_id:
                try:
                    # Attempt to delete the last message
                    self.slack_app.client.chat_delete(
                        channel=event.get("channel"), ts=last_message_id
                    )
                    # Remove the message ID from the dictionary after deletion
                    del self.last_message_id_dict[thread_ts]
                except Exception as e:
                    logger.error(
                        f"Error deleting message with ts={last_message_id}: {e}"
                    )
            return None  # Do not process further if it's a delete command

        was_indic = False
        if msg.strip().lower() == "stop":
            # Remove the thread from the followed thread map if it exists
            was_indic = ((self.bot_user_id, thread_ts) in thread_ts_dict)
            if was_indic:
                msg = "!stop"
            else:
                return

        if msg == "_thinking..._" or msg[:10] == ":toolbox: " or msg == '!NO_RESPONSE_REQUIRED':
            return None

        if msg.endswith("💬") or msg.endswith(":speech_balloon:"):
            return None

        if msg.startswith("_still running..._"):
            return None

        active_thread = False
        channel_type = event.get("channel_type", "")

        tag = (f"<@{self.bot_user_id}>" in msg)
        indic = (self.bot_user_id, thread_ts) in thread_ts_dict
        dmcheck = channel_type == "im" and msg != ""
        legacy = thread_ts in self.legacy_sessions
        txt = msg[:50]
        if len(txt) == 50:
            txt += "..."
        if tag or indic or dmcheck or was_indic or legacy:
            active_thread = True
            if legacy:
                self.legacy_sessions.remove(thread_ts)
            if (self.bot_user_id, thread_ts) not in thread_ts_dict and not was_indic:
                with meta_lock:
                    thread_ts_dict[self.bot_user_id, thread_ts] = {
                        "event": event,
                        "thread_id": None,
                    }
        else:
            return None

        if active_thread is False:
            return None

        thread_id = thread_ts
        channel = event.get("channel", "")

        if False:
            pass
        else:
            if os.getenv("THINKING_TOGGLE", "true").lower() != "false":
                if msg.strip().lower() in ["stop", "!stop"]:
                    m = '_stopping..._'
                    logger.info(f"**** Stopping {self.bot_name} {thread_ts} msg len={len(msg)}")
                    stopping_message = self.slack_app.client.chat_postMessage(
                        channel=channel, thread_ts=thread_ts, text=m
                    )
                    thinking_ts = stopping_message["ts"]
                else:
                    logger.info(f"**** Thinking {self.bot_name} {thread_ts} msg len={len(msg)}")
                    thinking_message = self.slack_app.client.chat_postMessage(
                        channel=channel, thread_ts=thread_ts, text="_thinking..._"
                    )
                    thinking_ts = thinking_message["ts"]
            else:
                thinking_ts = None

        if "files" in event:
            files = self._download_slack_files(event, thread_id=thread_id)
        else:
            pass

        user_id = "unknown_id"
        try:
            if event.get("subtype", None) == "message_changed":
                user_id = event["message"]["user"]
            else:
                user_id = event["user"]
            user_full_name, user_email = self.get_user_info(user_id)

            user_ids_in_message = re.findall(r"<@(\w+)>", msg)
            for uid in user_ids_in_message:
                uid_full_name, _ = self.get_user_info(uid)
                msg = msg.replace(f"<@{uid}>", f"<@{uid}({uid_full_name})>")

            msg_with_user_and_id = f"<@{user_id}>({user_full_name}) says: {msg}"
        except Exception as e:
            logger.info(f"    --NOT A USER MESSAGE, SKIPPING {e} ")
            # not a user message
            return None

        if tag:
            tagged_flag = "TRUE"
        else:
            tagged_flag = "FALSE"
        if dmcheck:
            dmcheck_flag = "TRUE"
        else:
            dmcheck_flag = "FALSE"
        if event.get("message", {}).get("bot_id", None) is not None:
            is_bot = "TRUE"
        else:
            is_bot = "FALSE"
        metadata = {
                "thread_ts": thread_ts,
                "channel": channel,
                "channel_type": event.get("channel_type", ""),
                "user_id": user_id,
                "user_name": user_full_name,
                "user_email": user_email,
                "tagged_flag": tagged_flag,
                "dm_flag": dmcheck_flag,
                "is_bot": is_bot,
                "event_ts": event["ts"],
            }
        if thinking_ts:
            metadata['thinking_ts'] = thinking_ts

        if dmcheck:
            # Check if this was the first message in the DM channel with the user
            conversation_history = self.slack_app.client.conversations_history(
                channel=channel, limit=2
            ).data

            # If the conversation history is empty or the first message's user is not the current user, it's the first message
            if conversation_history and len(conversation_history.get("messages")) < 2:
                first_dm_message = True
            else:
                first_dm_message = False

            # If it's the first DM, add an introductory message
            if first_dm_message:
                system_message = "\nSYSTEM MESSAGE: This is your first message with this user.  Please answer their message, if any, but also by the way introduce yourself and explain your role and capabilities, then suggest something you can do for the user.\n"
                msg_with_user_and_id = f"{msg_with_user_and_id}\n{system_message}"

            # add here the summary of whats been going on recenly

        if (
            (event["ts"] != thread_ts)
            and (not indic and tag and not dmcheck)
            or (dmcheck and not indic)
            or legacy
        ):
            # Retrieve the first and the last up to 20 messages from the thread
            conversation_history = self.slack_app.client.conversations_replies(
                channel=channel, ts=thread_ts
            ).data

            # Check if the conversation history retrieval was successful
            if not conversation_history.get("ok", False):
                pass
            else:
                original_user = (
                    conversation_history["messages"][0]["user"]
                    if conversation_history["messages"]
                    else None
                )
                from_you = original_user == self.bot_user_id if original_user else False
                if from_you:
                    system_message = "\nSYSTEM MESSAGE: You were the initiator of this thread, likely from an automated task that caused you to post the first message.\n"
                    msg_with_user_and_id = f"{system_message}\n{msg_with_user_and_id}"

                messages = conversation_history.get("messages", [])

                # If there are more than 40 messages, slice the list to get the last 50 messages
                if len(messages) > 50:
                    messages = messages[-50:]

                # Always include the first message if it's not already in the last 50 messages
                first_message = conversation_history["messages"][0]
                if first_message not in messages:
                    messages.insert(0, first_message)

                # Construct the object with messages including who said what
                thread_messages = []
                for message in messages:
                    user_id = message.get("user")
                    user_name = self.user_info_cache.get(user_id, "unknown_name")
                    text = message.get("text", "")
                    thread_messages.append({"user": user_name, "message": text})

                # Construct the thread history message
                if len(thread_messages) > 2:
                    thread_history_msg = "YOU WERE JUST ADDED TO THIS SLACK THREAD IN PROGRESS, HERE IS THE HISTORY:\n"
                    for message in thread_messages:
                        thread_history_msg += (
                            f"{message['user']}: {message['message']}\n"
                        )
                    thread_history_msg += "\nTHE MESSAGE THAT YOU WERE JUST TAGGED ON AND SHOULD RESPOND TO IS:\n"
                    msg_with_user_and_id = f"{thread_history_msg}{msg_with_user_and_id}"

        if is_bot == "TRUE":
            msg_with_user_and_id += "\n\nRESPONSE GUIDANCE: This message is from another Bot. RESPOND WITH !NO_RESPONSE_REQUIRED UNLESS 1) the message is directed to you by the other bot or a human has asked you to work with the other bot, 2) you have not already answered a similar message, and 3) the thread does not seem to be in a loop.  Do NOT proactively suggest other things for the bot to do like you would with a human user."

        bot_input_message = BotOsInputMessage(
            thread_id=thread_id,
            msg=msg_with_user_and_id,   # + '<<!!FAST_MODE!!>>',
            files=files,
            metadata=metadata,
        )

        self.thinking_map[event.get("ts", None)] = {
            "input_message": bot_input_message,
            "datetime": datetime.datetime.now().isoformat(),
        }

        return bot_input_message


    def _upload_files(self, files: list[str], thread_ts: str, channel: str = None):
        """
        Uploads a list of files to Slack and returns their URLs.

        Args:
            files (list[str]): A list of file paths to be uploaded.
            thread_ts (str): The timestamp of the thread where the files are to be uploaded.
            channel (str, optional): The Slack channel ID where the files are to be uploaded. Defaults to None.

        Returns:
            list: A list of "Slack URLs" for the uploaded files. If no files are provided, returns an empty list.
        """
        if files:
            file_urls = []
            for file_path in files:
                try:
                    new_file = self.slack_app.client.files_upload_v2(
                        title=os.path.basename(file_path),
                        filename=os.path.basename(file_path),
                        file=file_path,
                    )
                    #    logger.info(f"Result of files_upload_v2: {new_file}")
                    file_url = new_file.get("file").get("permalink")
                    file_urls.append(file_url)
                except Exception as e:
                    logger.info(f"Error uploading file {file_path} to Slack: {e}")
            return file_urls
        else:
            return []


    def _extract_slack_blocks(self, msg: str) -> list | None:
        extract_pattern = re.compile(r"```(?:json|slack)(.*?)```", re.DOTALL)

        json_matches = extract_pattern.findall(msg)
        blocks = []
        for json_match in json_matches:
            try:
                # Ensure to strip any leading/trailing whitespace or newlines that may affect json loading
                msg_json = json.loads(json_match.strip())
                if "blocks" in msg_json:
                    blocks += msg_json["blocks"]
            except Exception as e:
                logger.info("Failed to decode JSON:", e)
        return blocks if blocks else None


    def fix_fn_calls(self, resp):
        postfix = ""
        pattern = re.compile(r'<\|python_tag\|>\{.*?\}')
        match = pattern.search(resp)

        if match and resp.endswith('}'):
            postfix = "💬"

        pattern_function = re.compile(r'<function>(.*?)</function>(\{.*?\})$')
        match_function = pattern_function.search(resp)

        if match_function and resp.endswith(match_function.group(2)):
            function_name = match_function.group(1)
            params = match_function.group(2)
            newcall = f"<function={function_name}>{params}</function>"
            resp = resp.replace(match_function.group(0), newcall)
            postfix = "💬"

        pattern_function_call = re.compile(r'<function=(.*?)>\{.*?\}</function>')
        match_function_call = pattern_function_call.search(resp)

        if not match_function_call:
            # look for the other way of calling functions
            pattern_function_call = re.compile(r'<\|python_tag\|>\{"type": "function", "name": "(.*?)", "parameters": \{.*?\}\}')
            match_function_call = pattern_function_call.search(resp)

        if not match_function_call:
            # look for the other way of calling functions
            pattern_function_call = re.compile(r'<function=(.*?)>\{.*?\}')
            match_function_call = pattern_function_call.search(resp)
        # make the tool calls prettier
        if match_function_call:
            function_name = match_function_call.group(1)
            function_name_pretty = re.sub(r'(_|^)([a-z])', lambda m: m.group(2).upper(), function_name).replace('_', ' ')
            new_resp = f"🧰 Using tool _{function_name_pretty}_..."
            # replace for display purposes only
            resp = resp.replace(match_function_call.group(0), new_resp)
            resp = re.sub(r'(?<!\n)(🧰)', r'\n\1', resp)
            # Remove trailing function call syntax if present
            if resp.endswith('...} </function>'):
                resp = resp[:resp.rfind('...') + 3]  # Keep the '...' but remove everything after
            # Remove trailing '...}' if present
            if resp.endswith('...}'):
                resp = resp[:-1]  # Remove the last character ('}')  # add newlines before toolboxes as needed
            # Handle case where response ends with }></function>
            if resp.endswith('}></function>'):
                resp = resp[:resp.rfind('...') + 3]  # Keep the '...' but remove everything after

        return resp


    def _extract_local_file_markdowns(self, msg, message_thread_id) -> list(str):
        """
        Extracts file paths from non-Slack-compatible markdown links like [name](url) in the msg, which
        contain local path placeholders, and transforms them into actual local paths.

        This function searches for specific patterns in the message that represent local file links
        like 'sandbox:", "./runtime/downloaded_files", etc. and converts these links into local file paths
        based on the message thread ID.

        Args:
            msg (str): The message containing markdown links to files.
            message_thread_id (str): The ID of the message thread, used to construct local paths.

        Returns:
            list: A list of unique local file paths extracted from the message.
        """
        local_paths = set()

        # Define patterns and their corresponding local path transformations
        patterns = [
            # typical image path using sandbox:
            (r"\[.*?\]\((sandbox:/mnt/data(?:/downloads)?/.*?)\)",
                lambda match: match.replace("sandbox:/mnt/data/downloads", f"./runtime/downloaded_files/{message_thread_id}").replace("sandbox:/mnt/data", f"./runtime/downloaded_files/{message_thread_id}")),
            # 'task' path
            (r"\[(.*?)\]\(./runtime/downloaded_files/thread_(.*?)/(.*?)\)",
                lambda match: f"./runtime/downloaded_files/thread_{match[1]}/{match[2]}"),
            # paths that use /mnt/data
            (r"\[.*?\]\((sandbox:/mnt/data/runtime/downloaded_files/.*?)\)",
                lambda match: match.replace("sandbox:/mnt/data", ".")),
            # 'chart' patterns
            (r"\(sandbox:/mnt/data/(.*?)\)\n2\. \[(.*?)\]",
                lambda match: f"./runtime/downloaded_files/{message_thread_id}/{match}"),
            # when using attachment:
            (r"!\[.*?\]\(attachment://\.(.*?)\)",
                lambda match: match),
            # using thread id + file ## duplicate??
            (r"!\[.*?\]\(\./runtime/downloaded_files/thread_(.*?)/(.+?)\)",
                lambda match: f"./runtime/downloaded_files/thread_{match[0]}/{match[1]}")
        ]

        # match patterns and apply transformations
        for pattern, path_transform in patterns:
            compiled_pattern = re.compile(pattern)
            matches = compiled_pattern.findall(msg)
            for match in matches:
                local_path = path_transform(match)
                local_paths.add(local_path)
        return sorted(local_paths)


    def _fix_external_url_markdowns(self, msg) -> str:
        """
        Transform urls markdown in non-Slack-compatible format like [name](url) into the URL format that
        Slack recognizes like <url|name>. Return transformed msg.
        Args:
            msg (str): The message containing markdown links to be transformed.

        Returns:
            str: The message with all applicable markdown links converted to Slack's format.
        """
        pattern = r'(!?\[([^\]]+)\]\(((http[s]?|file|mailto):/+[^\)]+)\))'  # regex for strings of the form '[description](url)' and '![description](url)'
                                                                            # for well known urls schemes (exclude 'sandbox:' etc)
                                                                            # TOOD: support other standard URL schemas (use urllib?)
        new_msg = re.sub(pattern, r"<\3|\2>", msg)
        return new_msg


    def _handle_artifacts_markdown(self, msg: str, message_thread_id: str) -> str:
        """
        Locate all markdown in the message of the format [description][artifact:/uuid], donwload thier data into
        local sandbox (under runtime/downloaded_files/{message_thread_id}/{downloaded file name}) and update the markdown to the "sandbox convention" (e.g.
        '[description](sandbox:/mnt/data/{filename})'.)

        Args:
            msg (str): The message containing artifact markdown links to be transformed.

        Returns:
            str: The message with all applicable artifact markdown links converted.
        """
        # TODO: refactor to use core.bot_os_artifact.lookup_artifact_markdown
        artifact_pattern = re.compile(r'(\[([^\]]+)\]\(artifact:/(' + ARTIFACT_ID_REGEX + r')\))')
        matches = artifact_pattern.findall(msg)

        af = None
        for full_match, description, uuid in matches:
            af = af or get_artifacts_store(self.db_connector)
            try:
                # Download the artifact data into a local file
                local_dir = f"runtime/downloaded_files/{message_thread_id}" # follow the conventions used by sandbox URLs.
                Path(local_dir).mkdir(parents=True, exist_ok=True)
                downloaded_filename = af.read_artifact(uuid, local_dir)
            except Exception as e:
                logger.info(f"{self.__class__.__name__}: Failed to fetch data for artifact {uuid}. Error: {e}")
            else:
                # Update the markdown in the message to look like a sandbox URL
                msg = msg.replace(full_match, f"[{description}](sandbox:/mnt/data/{downloaded_filename})")

        return msg


    # abstract method from BotOsInputAdapter
    def handle_response(
        self,
        session_id: str,
        message: BotOsOutputMessage,
        in_thread=None,
        in_uuid=None,
        task_meta=None,
    ):
   #     if "!NO_RESPONSE_REQUIRED" in message.output:
   #         pass
        logger.debug(f"SlackBotAdapter:handle_response - {session_id} {message}")
        thinking_ts = None
        try:
            thinking_ts = message.input_metadata.get("thinking_ts", None)
            orig_thinking = thinking_ts
            if orig_thinking in self.thinking_msg_overide_map:
                thinking_ts = self.thinking_msg_overide_map[orig_thinking]
            if thinking_ts:
             #   logger.info('0-0-0-0-0-0-0-0 SLACK RESPONSE HANDLER -0-0-0-0-0-0-0-0-0')
                current_chunk_start =  self.chunk_start_map.get(orig_thinking,None)
         #       if current_chunk_start:
         #           logger.info('     Current chunk start: ', current_chunk_start)
                msg = message.output.replace("\n 💬", " 💬")
                full_msg = msg
         #       if current_chunk_start:
         #           logger.info(f"    Length of message: {len(msg)}")
                inmarkdown = False
                if current_chunk_start is not None:
                    trimmed = False
                    if orig_thinking in self.chunk_last_100:
                        last100 = self.chunk_last_100[orig_thinking]
                        l100 = last100.replace(" \n\n", "\n")
          #              logger.info(f"    Length of last 100: {len(last100)}")
                        if last100 in msg:
           #                 logger.info(f"    Last 100 is in msg")
                            last_index = msg.rfind(last100, 0, current_chunk_start)
            #                logger.info(f"    Last index: {last_index}")
                            if last_index != -1:
                                msg = msg[last_index + len(last100):]
                                trimmed=True
                        if l100 in msg:
           #                 logger.info(f"    Last 100 is in msg")
                            last_index = msg.rfind(l100, 0, current_chunk_start)
            #                logger.info(f"    Last index: {last_index}")
                            if last_index != -1:
                                msg = msg[last_index + len(l100):]
                                trimmed=True
                        if not trimmed and l100[:100] in msg:
                            last_index = msg.rfind(l100[:100], 0, current_chunk_start)
            #                logger.info(f"    Last index: {last_index}")
                            if last_index != -1:
                                msg = msg[last_index + len(l100):]
                                trimmed=True
             #                   logger.info(f"    Length of new trimmed msg: {len(msg)}")
                    if not trimmed:
                 #       logger.info("     Not trimmed based on last100, going to trim on current chunk start: ",current_chunk_start)
                        msg = msg[current_chunk_start:]
                  #      logger.info(f"    Length of new trimmed msg: {len(msg)}")
                    if orig_thinking in self.in_markdown_map:
                        if self.in_markdown_map[orig_thinking] == True:
                            msg = '```' + msg
                 #           logger.info('     Added markedown start to start of msg')
                            inmarkdown = True

                if (
                    message.status == "in_progress"
                    or message.status == "requires_action"
                    or msg.endswith("💬")
                ):
         #           logger.info('processing in_progress message: ', message.status,
         #               " updating ",
         #               thinking_ts,
         #               " len ",
          #              len(message.output),
          #          )
                    # show knowledge incorporated
                    knowledge_parts = [f"({k}): {v}" for k, v in message.input_metadata.items() if k.endswith("_knowledge")]
                    if knowledge_parts:
                        logger.info(f"Length of msg before knowledge add: {len(msg)}")
                        msg = "\n\n".join(knowledge_parts) + "\n\n" + msg
                        logger.info(f"Length of msg after knowledge add: {len(msg)}")

                    split_at = self.split_at
                    if len(msg) > split_at:
                        logger.info('     Splitting message')
                        duplicato = False
                        split_index = msg[max(0, split_at-300):split_at].rfind("\n")
                        if split_index != -1:
                            split_index += (split_at-300)
                        if split_index == -1:
                            # Find the last space character within the range of 3400 to 3700 to split the message cleanly
                            split_index = msg[max(0, split_at-300):split_at].rfind(" ")
                            if split_index != -1:
                                split_index += split_at
                        if split_index != -1:
                            msg_part1 = msg[:split_index]
                            msg_part2 = msg[split_index:]
                            chunk_start = split_index
                        else:
                            msg_part1 = msg[:split_at]
                            msg_part2 = msg[split_at:]
                            chunk_start = split_at
                        last300 = self.fix_fn_calls(msg_part1[-300:])
                        self.chunk_last_100[orig_thinking] = last300
                        if msg_part1.count("```") % 2 != 0:
                            msg_part1 += "```"
                            msg_part2 = "```" + msg_part2
                            self.in_markdown_map[orig_thinking] = True
                        else:
                            self.in_markdown_map[orig_thinking] = False
                        if orig_thinking in self.chunk_start_map:
                           if self.chunk_start_map[orig_thinking] + chunk_start < len(full_msg):
                                self.chunk_start_map[orig_thinking] += chunk_start
                           else:
                                logger.info('*** avoiding double add to the chunk_start ')
                                duplicato = True
                        else:
                            self.chunk_start_map[orig_thinking] = chunk_start
                        if inmarkdown:
                            self.chunk_start_map[orig_thinking] -= 3
                       # logger.info('chunkstart: ', self.chunk_start_map[orig_thinking])
                        chunk_start = self.chunk_start_map[orig_thinking]
                        #logger.info('Breakpoint context: ', message.output.replace("\n 💬", " 💬")[max(0, chunk_start-20):chunk_start] + '<>' + message.output.replace("\n 💬", " 💬")[chunk_start:chunk_start+20])
                        # Store the first 100 characters of the first part of the message in the chunk_last_100 dictionary
                        # Store the substring of msg_part1 starting from the 100th character in the chunk_last_100 dictionary
                        # Store the last 100 characters of msg_part1 in the chunk_last_100 dictionary
                        if not duplicato:
                            try:
                                self.slack_app.client.chat_update(
                                    channel=message.input_metadata.get("channel", self.channel_id),
                                    ts=thinking_ts,
                                    text=self._convert_to_slack_format(self.fix_fn_calls(msg_part1)),
                                )
                                thread_ts = message.input_metadata.get("thread_ts", None)
                            except Exception as e:
                                pass
                            if msg_part2.count("```") % 2 != 0:
                                msg_part2 += "```"
                            posted_message = self.slack_app.client.chat_postMessage(
                                channel=message.input_metadata.get("channel", self.channel_id),
                                thread_ts=thread_ts,
                                text=self._convert_to_slack_format(msg_part2),
                            )
                            thinking_ts = posted_message["ts"]
                            if orig_thinking is not None:
                                self.thinking_msg_overide_map[orig_thinking] = thinking_ts
                        return
                    else:
                        if msg.count("```") % 2 != 0:
                            msg += "```"

                        if message.input_metadata.get("response_authorized", "TRUE") == "FALSE":
                                message.output = "!NO_RESPONSE_REQUIRED"

                        if "!NO_RESPONSE_REQUIRED" in message.output:
                            if not message.output.startswith("!NO_RESPONSE_REQUIRED"):
                                message.output = message.output.replace(
                                    "!NO_RESPONSE_REQUIRED", ""
                                ).strip()
                            else:
                                logger.info(
                                    "Bot has indicated that no response will be posted to this thread."
                                )
                                if thinking_ts is not None:
                                    self.slack_app.client.chat_delete(
                                        channel=message.input_metadata.get("channel", self.channel_id),
                                        ts=thinking_ts,
                                    )
                            return

                        self.slack_app.client.chat_update(
                            channel=message.input_metadata.get("channel", self.channel_id),
                            ts=thinking_ts,
                            text=self._convert_to_slack_format(msg),
                        )
                        return
                else:
                    pass
                    # self.slack_app.client.chat_delete(channel= message.input_metadata.get("channel",self.channel_id),ts = thinking_ts)
        except Exception as e:
            logger.debug(
                "thinking already deleted"
            )  # FixMe: need to keep track when thinking is deleted
        message.output = message.output.strip()
      #  logger.info('...in the completion handler now...')
        if message.output.startswith("<Assistant>"):
            message.output = message.output[len("<Assistant>") :].strip()

        if message.input_metadata.get("response_authorized", "TRUE") == "FALSE":
            message.output = "!NO_RESPONSE_REQUIRED"

        if "!NO_RESPONSE_REQUIRED" in message.output:
            if not message.output.startswith("!NO_RESPONSE_REQUIRED"):
                message.output = message.output.replace(
                    "!NO_RESPONSE_REQUIRED", ""
                ).strip()
            else:
                logger.info(
                    "Bot has indicated that no response will be posted to this thread."
                )
                if thinking_ts is not None:
                    try:
                        self.slack_app.client.chat_delete(
                            channel=message.input_metadata.get("channel", self.channel_id),
                            ts=thinking_ts,
                        )
                    except:
                        pass

        else:
            try:

                thinking_ts = message.input_metadata.get("thinking_ts", None)
                orig_thinking = thinking_ts

                # only finalize threads with this function once even if called multiple times
                if orig_thinking in self.finalized_threads:
                    return
                else:
                    self.finalized_threads[orig_thinking] = True

                if orig_thinking in self.thinking_msg_overide_map:
                    thinking_ts = self.thinking_msg_overide_map[orig_thinking]


                current_chunk_start =  self.chunk_start_map.get(orig_thinking,None)
                msg = message.output.replace("\n 💬", " 💬")

                if current_chunk_start is not None:

                 #   print (' -0-0-0-0-0- IN the completion handler ready to trim if needed -0-0-0-0-0')

                    trimmed = False
                    if orig_thinking in self.chunk_last_100:
                        last100 = self.chunk_last_100[orig_thinking]
                        logger.info(f"    Length of last 100: {len(last100)}")
                        if last100 in msg:
                            logger.info(f"    Last 100 is in msg")
                            last_index = msg.rfind(last100, 0, current_chunk_start)
                            logger.info(f"    Last index: {last_index}")
                            if last_index != -1:
                                msg = msg[last_index + len(last100):]
                                trimmed=True
                                logger.info(f"    Length of new trimmed msg: {len(msg)}")
                        else:
                            last50 = last100[-50:]
                            if last50 in msg:
                                logger.info(f"    Last 50 is in msg")
                                last_index = msg.rfind(last50, 0, current_chunk_start)
                                logger.info(f"    Last index: {last_index}")
                                if last_index != -1:
                                    msg = msg[last_index + len(last50):]
                                    trimmed=True
                                    logger.info(f"    Length of new trimmed msg based on last50: {len(msg)}")
                    if not trimmed:
                        logger.info("     Not trimmed based on last100, going to trim on current chunk start: ",current_chunk_start)
                        msg = msg[current_chunk_start:]
                        logger.info(f"    Length of new trimmed msg: {len(msg)}")
                    if orig_thinking in self.in_markdown_map:
                        if self.in_markdown_map[orig_thinking] == True:
                            msg = '```' + msg
                            logger.info('     Added markdown start to start of msg')
                            inmarkdown = True

                msg_trimmed = msg

                # fix non-Slack-compatible markdown for external URLs
                msg = self._fix_external_url_markdowns(msg)

                # Locate all artifact markdowns, save those artifacts to the local 'sandbox' and replace with sandbox markdown
                # so that they will be handled with other local files below.
                msg = self._handle_artifacts_markdown(msg, message.thread_id)

                # Extract local paths from the msg
                local_paths = self._extract_local_file_markdowns(msg, message.thread_id)

                files_in = list(set(message.files + local_paths)) # combine with message.files and remove duplicates

                #          logger.info("Uploading files:", files_in)
                thread_ts = message.input_metadata.get("thread_ts", None)
                msg_files = self._upload_files(
                    files_in,
                    thread_ts=thread_ts,
                    channel=message.input_metadata.get("channel", self.channel_id),
                )

                # Replace the markdown of the uploaded files with the Slack-compatible markdown for links to the
                # uploaded files
                for msg_url in msg_files:
                    # TODO: align the URL subsitution logic with _extract_local_file_markdowns
                    filename = msg_url.split("/")[-1]
                    msg_prime = msg

                    msg = re.sub(rf"(?i)\(sandbox:/mnt/data/{filename}\)", f"<{{msg_url}}>",msg)
                    alt_pattern = re.compile(r"\[(.*?)\]\(\./runtime/downloaded_files/thread_(.*?)/(.+?)\)" )
                    msg = re.sub(alt_pattern, f"<{{msg_url}}|\\1>", msg)

                    # Catch the pattern with thread ID and replace it with the correct URL
                    thread_file_pattern = re.compile(r"\[(.*?)\]\(sandbox:/mnt/data/runtime/downloaded_files/thread_(.*?)/(.+?)\)")
                    msg = re.sub(thread_file_pattern, f"<{{msg_url}}|\\1>", msg)

                    msg = msg.replace("{msg_url}", msg_url)
                    msg = msg.replace("[Download ", "[")
                    msg = re.sub(r"!\s*<", "<", msg)
                    if msg == msg_prime: # hans't changed?
                        msg += " {" + msg_url + "}"

                # Reformat the message if it contains a link in brackets followed by a URL in angle brackets
                link_pattern = re.compile(r"\[(.*?)\]<(.+?)>")
                msg = re.sub(link_pattern, r"<\2|\1>", msg)

                # just moved this back up here before the chat_update
                pattern = re.compile(
                    r"\[(.*?)\]\(sandbox:/mnt/data/runtime/downloaded_files/(.*?)/(.+?)\)"
                )
                msg = re.sub(pattern, r"<\2|\1>", msg)

                # Check for link blocks with a ! immediately before it and remove the !
                link_block_pattern = re.compile(r"!\s*(<https?://[^|]+?\|[^>]+?>)")
                msg = re.sub(link_block_pattern, r"\1", msg)


                #      logger.info("sending message to slack post url fixes:", msg)
                blocks = self._extract_slack_blocks(msg)
#                if blocks is not None or len(msg) > 2000:
#                    logger.info('blocks / long: ',len(msg))

                split_at = self.split_at
                if len(msg) > split_at:
                    split_index = msg[max(0, split_at-300):split_at].rfind("\n")
                    if split_index != -1:
                        split_index += split_at
                    if split_index == -1:
                        # Find the last space character within the range of 3400 to 3700 to split the message cleanly
                        split_index = msg[max(0, split_at-300):split_at].rfind(" ")
                        if split_index != -1:
                            split_index += split_at
                    if split_index != -1:
                        msg_part1 = msg[:split_index]
                        msg_part2 = msg[split_index:]
                        chunk_start = split_index
                    else:
                        msg_part1 = msg[:split_at]
                        msg_part2 = msg[split_at:]
                        chunk_start = split_at
                    self.chunk_last_100[orig_thinking] = self.fix_fn_calls(msg_part1[-300:])
                    if msg_part1.count("```") % 2 != 0:
                        msg_part1 += "```"
                        msg_part2 = "```" + msg_part2
                        self.in_markdown_map[orig_thinking] = True
                    else:
                        self.in_markdown_map[orig_thinking] = False
                    if orig_thinking in self.chunk_start_map:
                        self.chunk_start_map[orig_thinking] += chunk_start
                    else:
                        self.chunk_start_map[orig_thinking] = chunk_start
                    if inmarkdown:
                        self.chunk_start_map[orig_thinking] -= 3
                    # logger.info('chunkstart: ', self.chunk_start_map[orig_thinking])
                    chunk_start = self.chunk_start_map[orig_thinking]


                    try:
                        self.slack_app.client.chat_update(
                            channel=message.input_metadata.get("channel", self.channel_id),
                            ts=thinking_ts,
                            text=self._convert_to_slack_format(self.fix_fn_calls(msg_part1)),
                        )
                        thread_ts = message.input_metadata.get("thread_ts", None)
                    except Exception as e:
                        pass

                    if msg_part2 == None or len(msg_part2) == 0:
                        channel = message.input_metadata.get("channel", self.channel_id)
                        logger.error(f'bot={self.bot_name}: adjusting msg_part2 from "{msg_part2}" to " " {channel=} {thread_ts=}')
                        msg_part2 = " "

                    posted_message = self.slack_app.client.chat_postMessage(
                        channel=message.input_metadata.get("channel", self.channel_id),
                        thread_ts=thread_ts,
                        text=self._convert_to_slack_format(msg_part2),
                    )
                    thinking_ts = posted_message["ts"]
                    if orig_thinking is not None:
                        self.thinking_msg_overide_map[orig_thinking] = thinking_ts

                else:

                    if msg_trimmed == msg and thinking_ts is not None:
                        if msg.count("```") % 2 != 0:
                            msg += "```"
                        self.slack_app.client.chat_update(
                            channel=message.input_metadata.get("channel", self.channel_id),
                            ts=thinking_ts,
                            text=self._convert_to_slack_format(msg),
                            blocks=blocks,
                        )
                    else:

                        thinking_ts = message.input_metadata.get("thinking_ts", None)
                        orig_thinking = thinking_ts
                        if orig_thinking in self.thinking_msg_overide_map:
                            thinking_ts = self.thinking_msg_overide_map[orig_thinking]
                        if thinking_ts is not None:
                            self.slack_app.client.chat_delete(
                                channel=message.input_metadata.get(
                                    "channel", self.channel_id
                                ),
                                ts=thinking_ts,
                            )
                        if msg.count("```") % 2 != 0:
                            msg += "```"

                        if msg == None or len(msg) == 0:
                            channel = message.input_metadata.get("channel", self.channel_id)
                            logger.error(f'bot={self.bot_name}: adjusting msg from "{msg}" to " " {channel=} {thread_ts=}')
                            msg = " "

                        result = self.slack_app.client.chat_postMessage(
                            channel=message.input_metadata.get("channel", self.channel_id),
                            thread_ts=thread_ts,
                            text=self._convert_to_slack_format(msg),
                        )
                        if message.input_metadata.get("thinking_ts", None) is None:
                            message.input_metadata.thinking_ts = result.ts

                #    logger.info("Result of sending message to Slack:", result)
                # Replace patterns in msg with the appropriate format

                # Utility function handles file uploads and logs errors internally
                if thread_ts is not None:
                    with meta_lock:
                        thread_ts_dict[self.bot_user_id, thread_ts][
                            "thread_id"
                        ] = (
                            message.thread_id
                        )  # store thread id so we can map responses to the same assistant thread
            except Exception as e:
                logger.error(
                    f"SlackBotAdapter:handle_response - Error posting message: {e}"
                )

    def process_attachments(self, msg, attachments, files_in = None):
        files_to_attach = []
        for attachment in attachments:
            if "image_url" in attachment:
                image_path = attachment["image_url"]
                if image_path.startswith("./runtime/downloaded_files/"):
                    files_to_attach.append(image_path)

        # Extract file paths from the message and add them to files_in array
        image_pattern = re.compile(
            r"\[.*?\]\((sandbox:/mnt/data/runtime/downloaded_files/.*?)\)"
        )
        matches = image_pattern.findall(msg)
        for match in matches:
            local_path = match.replace("sandbox:/mnt/data", ".")
            if local_path not in files_to_attach:
                files_to_attach.append(local_path)

        pineapple_pattern = re.compile(
            r"\[(.*?)\]\(sandbox:/mnt/data/thread_(.*?)/(.+?)\)"
        )
        pineapple_matches = pineapple_pattern.findall(msg)
        for pineapple_match in pineapple_matches:
            local_pineapple_path = (
                f"./runtime/runtime/downloaded_files/thread_{pineapple_match[1]}/{pineapple_match[2]}"
            )
            if local_pineapple_path not in files_to_attach:
                files_to_attach.append(local_pineapple_path)

        # Extract file paths from the message and add them to files_in array
        chart_pattern = re.compile(r"\(sandbox:/mnt/data/(.*?)\)\n2\. \[(.*?)\]")
        chart_matches = chart_pattern.findall(msg)
        for chart_match in chart_matches:
            local_chart_path = f"./runtime/downloaded_files/{chart_match}"
            if local_chart_path not in files_to_attach:
                files_to_attach.append(local_chart_path)

        # Parse the message for the provided pattern and add to files_in
        file_pattern = re.compile(r"!\[.*?\]\(attachment://\.(.*?)\)")
        file_matches = file_pattern.findall(msg)
        for file_match in file_matches:
            local_file_path = file_match
            if local_file_path not in files_to_attach:
                files_to_attach.append(local_file_path)

        local_pattern = re.compile(
            r"!\[.*?\]\(\./runtime/downloaded_files/thread_(.*?)/(.+?)\)"
        )
        local_pattern_matches = local_pattern.findall(msg)
        for local_match in local_pattern_matches:
            local_path = f"./runtime/downloaded_files/thread_{local_match[0]}/{local_match[1]}"
            if local_path not in files_to_attach:
                files_to_attach.append(local_path)

        # Append files_in to files_to_attach if it's not None
        if files_in is not None:
            files_to_attach.extend(files_in)

        if files_to_attach:
            uploaded_files = self._upload_files(
                files_to_attach, thread_ts=None, channel=self.channel_id
            )
            return uploaded_files
        else:
            return []

    def replace_urls(self, msg=None, msg_files=[]):
        """
        Replaces URLs in the message with the correct format for Slack.

        Args:
            msg (str): The message containing URLs to be replaced.

        Returns:
            str: The message with URLs replaced.
        """
        for msg_url in msg_files:
            filename = msg_url.split("/")[-1]
            msg_prime = msg

            msg = re.sub(rf"(?i)\(sandbox:/mnt/data/{filename}\)", f"<{{msg_url}}>", msg)
            alt_pattern = re.compile(
                r"\[(.*?)\]\(\./runtime/downloaded_files/thread_(.*?)/(.+?)\)"
            )
            msg = re.sub(alt_pattern, f"<{{msg_url}}|\\1>", msg)
            # Catch the pattern with thread ID and replace it with the correct URL

            thread_file_pattern = re.compile(
                r"\[(.*?)\]\(sandbox:/mnt/data/runtime/downloaded_files/thread_(.*?)/(.+?)\)"
            )
            msg = re.sub(thread_file_pattern, f"<{{msg_url}}|\\1>", msg)
            # Catch the external URL pattern and replace it with the correct URL
            external_url_pattern = re.compile(r"\[(.*?)\]\((https?://.*?)\)")
            msg = re.sub(external_url_pattern, f"<{{msg_url}}|\\1>", msg)
            msg = msg.replace("{msg_url}", msg_url)
            msg = msg.replace("[Download ", "[")
            msg = re.sub(r"!\s*<", "<", msg)
            if msg == msg_prime:
                msg += " {" + msg_url + "}"
        # Reformat the message if it contains a link in brackets followed by a URL in angle brackets
        link_pattern = re.compile(r"\[(.*?)\]<(.+?)>")
        msg = re.sub(link_pattern, r"<\2|\1>", msg)
        return msg

    def send_slack_direct_message(
        self, slack_user_id: str, message: str, attachments=[], thread_id: str = None
    ):

        # Remove angle brackets from slack_user_id if present
        if slack_user_id.startswith('<') and slack_user_id.endswith('>'):
            slack_user_id = slack_user_id[1:-1]

        if slack_user_id.startswith('#'):
            return {
                "success": False,
                "message": "Invalid user ID. Use send_slack_channel_message to send to a channel.",
                "suggestion": "To send a message to a channel, use the _send_slack_channel_message function instead."
            }
        try:
            # Start a conversation with the user
            response = self.slack_app.client.conversations_open(users=slack_user_id)

        # Extract file paths from the message and add them to files_in array
            files_in = []
            image_pattern = re.compile(
                r"\[.*?\]\((sandbox:/mnt/data(?:/downloads)?/.*?)\)"
            )
            matches = image_pattern.findall(message)
            for match in matches:
                local_path = match.replace("sandbox:/mnt/data/downloads", f"./runtime/downloaded_files/{thread_id}").replace("sandbox:/mnt/data", f"./runtime/downloaded_files/{thread_id}")

                if local_path not in files_in:
                    #      logger.info(f"Pattern 0 found, attaching {local_path}")
                    files_in.append(local_path)

            file_list = self.process_attachments(message, attachments, files_in=files_in)

            message = self.replace_urls(msg=message, msg_files=file_list)
            if response["ok"]:
                channel_id = response["channel"]["id"]
                # Post a message to the new conversation
                res = self.slack_app.client.chat_postMessage(
                    channel=channel_id,
                    text=message,
                    attachments=file_list if file_list else None,
                )
                thread_ts = res["ts"]
                if (self.bot_user_id, thread_ts) not in thread_ts_dict:
                    with meta_lock:
                        thread_ts_dict[self.bot_user_id, thread_ts] = {
                            "event": None,
                            "thread_id": thread_id,
                        }

                try:
                    if res.data['ok']:
                        return {
                            "success": True,
                            "message": f"Message sent to {slack_user_id} successfully."
                        }
                    else:
                        if res.data:
                            return {
                                "success": False,
                                "message": f"Response from slack: {res.data}.",
                                "suggestion": f"Did you perhaps intend to sent a message to a channel with send_slack_channel_message?"
                            }
                        else:
                            return {
                                "success": False,
                                "message": f"Response from slack: {res}"
                            }
                except:
                    return {
                        "success": False,
                        "message": f"Response from slack: {res}"
                    }

        except Exception as e:
            return f"Error sending message: {str(e)}"

    def send_slack_channel_message(
        self, channel: str = None, message: str = None, attachments=[], thread_id: str = None, channel_id=None, channel_name=None
    ):
        if channel_name is None:
            channel_name = channel
        if channel_name is None and channel_id is not None:
            channel_name = channel_id

        if channel_name is None or not isinstance(channel_name, str):
            return "Error: channel is None or not a string. Please provide a valid channel name like #example or channel ID like C07FBCHFZ26."
        # Remove angle brackets from channel_name if present

        if channel_name and channel_name.startswith('<') and channel_name.endswith('|>'):
            channel_name = channel_name[1:-2]

        if channel_name and channel_name.startswith('<') and channel_name.endswith('>'):
            channel_name = channel_name[1:-1]

        if channel_name.endswith('|'):
            channel_name = channel_name[:-1]

        # Remove '#' from the beginning of the channel name if present
        if channel_name and channel_name.startswith('#'):
            channel_name = channel_name[1:]

        if channel_name.lower() == 'general':
            return "Bots are not allowed to post to #general. Reconfirm what channel you are supposed to post to."

        # Extract file paths from the message and add them to files_in array
        files_in = []
        image_pattern = re.compile(
            r"\[.*?\]\((sandbox:/mnt/data(?:/downloads)?/.*?)\)"
        )
        matches = image_pattern.findall(message)
        for match in matches:
            local_path = match.replace("sandbox:/mnt/data/downloads", f"./runtime/downloaded_files/{thread_id}").replace("sandbox:/mnt/data", f"./runtime/downloaded_files/{thread_id}")

            if local_path not in files_in:
                #      logger.info(f"Pattern 0 found, attaching {local_path}")
                files_in.append(local_path)


        try:
            file_list = self.process_attachments(message, attachments, files_in=files_in)
            message = self.replace_urls(msg=message, msg_files=file_list)
            res = self.slack_app.client.chat_postMessage(
                channel=channel_name,
                text=message,
                attachments=file_list if file_list else None,
            )
            if res["ok"]:
                thread_ts = res["ts"]
                if (self.bot_user_id, thread_ts) not in thread_ts_dict:
                    with meta_lock:
                        thread_ts_dict[self.bot_user_id, thread_ts] = {
                            "event": None,
                            "thread_id": thread_id,
                        }

                return {

                    "success": True,
                    "message": f"Message sent to channel {channel_name} successfully (note, this is not #general)."
                }
            else:
                return {
                    "success": False,
                    "message": f"Failed to send message to channel {channel_name}."
                }
        except Exception as e:
            if channel_name.upper.startswith('C') and not channel_name.isupper():
                return f"Error: The channel '{channel_name}' appears to be a channel ID, but it's not in the correct format. If you're using a channel ID, please make sure it's in all uppercase letters. Try again with '{channel_name.upper()}'."
            return f"Error sending message to channel {channel_name}: {str(e)}.  Double-check that you are sending the message to the correct Slack channel.  If you are running a process, be double-sure that you have the right channel name and are not making one up."

    def lookup_slack_user_id_real(self, user_name: str, thread_id: str):
        """
        Looks up the Slack user ID based on the provided user name by querying the Slack API.

        Args:
            user_name (str): The name of the user to look up.
            thread_id (str): The thread ID associated with the user (unused in this function).

        Returns:
            str: The Slack user ID if found, otherwise an error message.
        """
        try:
            # Normalize the user name to handle different capitalizations
            user_name = user_name.lower()
            # Call the Slack API users.list method to retrieve all users
            response = self.slack_app.client.users_list()
            if response["ok"]:
                # Iterate through the users to find a matching display name or real name
                for member in response["members"]:
                    if "id" in member and member["id"].lower() == user_name:
                        return member["id"]
                    if "name" in member and member["name"].lower() == user_name:
                        return member["id"]
                    if (
                        "profile" in member
                        and "display_name" in member["profile"]
                        and member["profile"]["display_name"].lower() == user_name
                    ):
                        return member["id"]
                    if (
                        "profile" in member
                        and "real_name" in member["profile"]
                        and member["profile"]["real_name"].lower() == user_name
                    ):
                        return member["id"]
                return "Error: Slack user not found."
            else:
                return f"Error: Slack API users.list call was not successful. Response: {response}"
        except Exception as e:
            return f"Error: Exception occurred while looking up Slack user ID: {str(e)}"

    def lookup_slack_user_id(
        self, user_name: str, thread_id: str
    ):  # FixMe: replace with real implementation querying slack

        user_id = self.lookup_slack_user_id_real(user_name, thread_id)
        if not user_id.startswith("Error:"):
            return {"success": True, "User_id:": user_id}
        else:
            return "Error: unknown slack user.  Maybe use the list_all_bots function to see if its a bot?"

    def _convert_to_slack_format(self, msg: str) -> str:
        import re
        # Convert Markdown headers (e.g. "# Header") into bold text
        msg = re.sub(r'^(#{1,6})\s*(.*)', lambda m: f"*{m.group(2).strip()}*", msg, flags=re.MULTILINE)
        # Convert Markdown bold (e.g. **text**) to Slack bold (*text*)
        msg = re.sub(r'\*\*(.*?)\*\*', r'*\1*', msg)
        # Remove language specifiers from triple backticks (e.g. ```python becomes ``` )
        msg = re.sub(r'```[\w+-]*\n', '```\n', msg)
        
        # Special handling for bold headers: format any standalone bold text so that it appears as a clearly delineated header with a trailing colon.
        msg = re.sub(r'(?:^|\n)(\*[^\*\n]+\*)(?!:)', lambda m: f"\n{m.group(1)}:\n", msg, flags=re.MULTILINE)
        
        # Detect and reformat Markdown tables to be monospaced and aligned.
        table_pattern = re.compile(r'((?:^\|.*\n){3,})', flags=re.MULTILINE)
        def block_table(match):
            table_text = match.group(1)
            lines = [line for line in table_text.splitlines() if line.strip()]
            rows = []
            for line in lines:
                if line.lstrip().startswith("|"):
                    # Split the row into cells by the '|' delimiter.
                    cells = [cell.strip() for cell in line.strip().strip('|').split('|')]
                    rows.append(cells)
            if not rows:
                return table_text
            # Determine the maximum number of columns.
            num_cols = max(len(row) for row in rows)
            # Pad rows with missing cells.
            for row in rows:
                if len(row) < num_cols:
                    row.extend([""] * (num_cols - len(row)))
            # Compute maximum width for each column.
            col_widths = [0] * num_cols
            for row in rows:
                for i, cell in enumerate(row):
                    col_widths[i] = max(col_widths[i], len(cell))
            # Helper to check if a row is a separator row (dividing header from data)
            def is_separator_row(row):
                return all(re.fullmatch(r'[-:\s]+', cell) for cell in row)
            formatted_lines = []
            for row in rows:
                if is_separator_row(row):
                    formatted_cells = []
                    for i, cell in enumerate(row):
                        cell = cell.strip()
                        if cell.endswith(':'):
                            formatted_cells.append('-' * (col_widths[i] - 1) + ':')
                        else:
                            formatted_cells.append('-' * col_widths[i])
                    formatted_line = "| " + " | ".join(formatted_cells) + " |"
                else:
                    formatted_cells = [cell.ljust(col_widths[i]) for i, cell in enumerate(row)]
                    formatted_line = "| " + " | ".join(formatted_cells) + " |"
                formatted_lines.append(formatted_line)
            new_table = "\n".join(formatted_lines)
            return f"```\n{new_table}\n```"
        msg = table_pattern.sub(block_table, msg)
        
        # Convert Markdown unordered lists (e.g. "- item" or "* item") to Slack bullet lists.
        msg = re.sub(r'^\s*[-*]\s+', '• ', msg, flags=re.MULTILINE)
        return msg
