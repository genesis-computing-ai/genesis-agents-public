from __future__ import annotations  # for python 9 support of | type operator
from collections import deque
import json
import logging
import requests
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
import os, time
from slack_bolt.adapter.flask import SlackRequestHandler
from flask import jsonify, request
from core.bot_os_input import BotOsInputAdapter, BotOsInputMessage, BotOsOutputMessage

logger = logging.getLogger(__name__)
import threading
import random
import re
import datetime


# module level
meta_lock = threading.Lock()
thread_ts_dict = {}
uniq = random.randint(100000, 999999)

print("     [-------]     ")
print("    [         ]    ")
print("   [  0   0  ]   ")
print("  [    ---    ]  ")
print(" [______] ")
print("     /     \\     ")
print("    /|  o  |\\    ")
print("   / |____| \\   ")
print("      |  |       ")
print("      |  |       ")
print("     /    \\      ")
print("    /      \\     ")
print("   /        \\    ")
print("  G E N E S I S ")
print("    B o t O S")
print("")
print(f"Instantiation Code--->{uniq}")


class SlackBotAdapter(BotOsInputAdapter):

    def __init__(
        self,
        token: str,
        signing_secret: str,
        channel_id: str,
        bot_user_id: str,
        bot_name: str = "Unknown",
        slack_app_level_token=None,
        bolt_app_active=True,
    ) -> None:
        logger.debug("SlackBotAdapter")
        super().__init__()
        self.slack_app = App(token=token, signing_secret=signing_secret)
        self.channel_id = channel_id
        # self.slack_app.event("message")(self.handle_message_events_old)
        self.handler = SlackRequestHandler(self.slack_app)
        self.events = deque()
        self.bot_user_id = bot_user_id
        self.user_info_cache = {}
        self.bot_name = bot_name
        self.last_message_id_dict = {}
        self.thinking_map = {}
        self.events_map = {}
        self.handled_events = {}
        self.chunk_start_map = {}
        self.thinking_msg_overide_map = {}

        self.events_lock = threading.Lock()

        if slack_app_level_token and bolt_app_active:
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
                    and len(self.events) > 1
                ):
                    print(
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

            #        print(f'event type: {event.get("type","no type")}, text: {event.get("text","no text")}')
            #        if event.get("text","no text") != '_thinking..._' and self.bot_user_id != event.get("user","NO_USER") and event.get("subtype","none") != 'message_changed':
            #            self.events.append(event)

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
                handler = SocketModeHandler(self.slack_socket, slack_app_level_token)
                handler.start()

            # Run Slack app in a separate thread
            slack_thread = threading.Thread(target=run_slack_app)
            slack_thread.start()

    def add_event(self, event):
        self.events.append(event)

    def add_back_event(self, event_ts):
        event = self.events_map.get(event_ts, {}).get("event", None)
        if event is not None:
            self.events.append(event)

    def handle_message_events_old(self, event, context, say, logger):
        logger.info(event)  # Log the event data (optional)
        text = event.get("text", "")
        # print('AT HANDLE MESSAGE EVENTS???')
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
            # print('... download_slack_files ',file_info,flush=True)
            url_private = file_info.get("url_private")
            file_name = file_info.get("name")
            if url_private and file_name:
                local_path = f"./downloaded_files/{thread_id}/{file_name}"
                #  print('... downloading slack file ',file_name,' from ',url_private,' to ',local_path,flush=True)
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
                        #      print('... saving locally to ',local_path)
                        with open(local_path, "wb") as f:
                            # Write the content to the local file
                            for chunk in r.iter_content(chunk_size=32768):
                                f.write(chunk)  # Raise an exception for bad responses
                        #      f.write(r.content)

                        files.append(local_path)
                #        print('... download_slack_files downloaded ',local_path)
                except Exception as e:
                    print(f"Error downloading file from {url_private}: {e}")
        return files

    # abstract method from BotOsInputAdapter

    def get_input(
        self, thread_map=None, active=None, processing=None, done_map=None
    ) -> BotOsInputMessage | None:
        # logger.info(f"SlackBotAdapter:get_input")
        files = []

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
            print(f"*****!!! Resubmission zapped")
            return

        if thread_map is not None and processing is not None and active is not None:
            if openai_thread in active or openai_thread in processing:
                self.events.append(event)
                return None

        if event["ts"] in self.thinking_map:
            input_message = self.thinking_map[event["ts"]]["input_message"]
            print(f"***** Resubmission {input_message.msg}")
            return input_message

        #     if active is not None and processing is not None:
        #         if thread_ts in active or thread_ts in processing:
        #             self.events.append(event)
        #             return None

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

        if msg.strip().lower() == "stop":
            # Remove the thread from the followed thread map if it exists
            if (self.bot_user_id, thread_ts) in thread_ts_dict:
                with meta_lock:
                    del thread_ts_dict[(self.bot_user_id, thread_ts)]
            return None  # Ignore the message and do not process further

        if msg == "_thinking..._" or msg[:10] == ":toolbox: ":
            return None

        if msg.endswith("💬") or msg.endswith(":speech_balloon:"):
            return None

        if msg.startswith("_still running..._"):
            return None

        active_thread = False
        channel_type = event.get("channel_type", "")

        # print(f"{uniq} {self.bot_name}-Looking for {(self.bot_user_id, thread_ts)}-Is in? {(self.bot_user_id, thread_ts) in thread_ts_dict}-Current keys in thread_ts_dict:", thread_ts_dict.keys())
        tag = f"<@{self.bot_user_id}>" in msg
        indic = (self.bot_user_id, thread_ts) in thread_ts_dict
        dmcheck = channel_type == "im" and msg != ""
        txt = msg[:50]
        if len(txt) == 50:
            txt += "..."
        if tag or indic or dmcheck:
            print(
                f"{self.bot_name} bot_os get_input for {self.bot_user_id} {tag},{indic},{dmcheck}",
                flush=True,
            )
            active_thread = True
            if (self.bot_user_id, thread_ts) not in thread_ts_dict:
                #     print(f'{uniq}     --ENGAGE/ADD>  Adding {thread_ts} to dict', flush=True)
                with meta_lock:
                    thread_ts_dict[self.bot_user_id, thread_ts] = {
                        "event": event,
                        "thread_id": None,
                    }
            #    print(f"{uniq} {self.bot_name}-ADDED-Now is {(self.bot_user_id,thread_ts)} in??-Is in? {(self.bot_user_id,thread_ts) in thread_ts_dict}-Current keys in thread_ts_dict:", thread_ts_dict.keys(),flush=True)

        #   else:
        #   print(f'{uniq}     --ENGAGE/EXISTING>  {thread_ts} already in dict', flush=True)

        if active_thread is False:
            # public channel, not flagged yet in thread
            return None

        thread_id = thread_ts
        #
        channel = event.get("channel", "")

        # I dont think this works, perhaps remove it (its trying to map openai to slack thread ids thats why..)
        #        if active is not None and processing is not None:
        #            if thread_ts in active or thread_ts in processing:
        #                self.events.append(event)
        #                return None
        if False:
            pass
        else:
            if os.getenv("THINKING_TOGGLE", "true").lower() != "false":
                print(f"**** Thinking {self.bot_name} {thread_ts} msg={msg}")
                thinking_message = self.slack_app.client.chat_postMessage(
                    channel=channel, thread_ts=thread_ts, text="_thinking..._"
                )
                thinking_ts = thinking_message["ts"]
            else:
                thinking_ts = None

        if "files" in event:
            #    print(f"    --/DOWNLOAD> downloading files for ({self.bot_name}) ")
            files = self._download_slack_files(event, thread_id=thread_id)
        #    print(f"    --/DOWNLOADED> downloaded files for ({self.bot_name}), files={files} ")
        else:
            pass
        #   print('...*-*-*-* Files not in event', flush=True)

        user_full_name = "Unknown User"
        user_id = "Unknown User ID"
        try:
            if event.get("subtype", None) == "message_changed":
                user_id = event["message"]["user"]
            else:
                user_id = event["user"]
            if user_id not in self.user_info_cache:
                try:
                    user_info = self.slack_app.client.users_info(user=user_id)
                    self.user_info_cache[user_id] = user_info["user"]["real_name"]
                    user_full_name = self.user_info_cache[user_id]
                except:
                    try:
                        self.user_info_cache[user_id] = user_info["user"]["profile"][
                            "real_name"
                        ]
                        user_full_name = self.user_info_cache[user_id]
                    except:
                        user_full_name = user_id
            else:
                user_full_name = self.user_info_cache[user_id]

            user_ids_in_message = re.findall(r"<@(\w+)>", msg)
            for uid in user_ids_in_message:
                if uid not in self.user_info_cache:
                    try:
                        user_info = self.slack_app.client.users_info(user=uid)
                        self.user_info_cache[uid] = user_info["user"]["real_name"]
                    except:
                        try:
                            self.user_info_cache[uid] = user_info["user"]["profile"][
                                "real_name"
                            ]
                        except:
                            self.user_info_cache[uid] = uid
                msg = msg.replace(f"<@{uid}>", f"<@{uid}({self.user_info_cache[uid]})>")

            msg_with_user_and_id = f"<@{user_id}({user_full_name})> says: {msg}"
        except Exception as e:
            print(f"    --NOT A USER MESSAGE, SKIPPING {e} ")
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
        if thinking_ts:
            metadata = {
                "thread_ts": thread_ts,
                "channel": channel,
                "thinking_ts": thinking_ts,
                "channel_type": event.get("channel_type", ""),
                "user_id": user_id,
                "user_name": user_full_name,
                "tagged_flag": tagged_flag,
                "dm_flag": dmcheck_flag,
                "is_bot": is_bot,
                "event_ts": event["ts"],
            }
        else:
            metadata = {
                "thread_ts": thread_ts,
                "channel": channel,
                "channel_type": event.get("channel_type", ""),
                "user_id": user_id,
                "user_name": user_full_name,
                "tagged_flag": tagged_flag,
                "dm_flag": dmcheck_flag,
                "is_bot": is_bot,
                "event_ts": event["ts"],
            }

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
        ):
            # Retrieve the first and the last up to 20 messages from the thread
            conversation_history = self.slack_app.client.conversations_replies(
                channel=channel, ts=thread_ts
            ).data

            # Check if the conversation history retrieval was successful
            if not conversation_history.get("ok", False):
                pass
            #       print("Failed to retrieve conversation history.")
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
                    user_name = self.user_info_cache.get(user_id, "Unknown User")
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

        #  if active is not None and processing is not None:
        #      if thread_ts in active or thread_ts in processing:
        #          self.events.append(event)
        #          return None

        if is_bot == "TRUE":
            msg_with_user_and_id += "\n\nRESPONSE GUIDANCE: WARNING! THIS MESSAGE IS FROM ANOTHER BOT, YOU SHOULD PROBABLY NOT RESPOND. ONLY RESPOND TO IT IF IT IS SPECIFICALLY DIRECTED TO YOU, AND IF YOU HAVE NOT ALREADY PROVIDED A RESPONSE TO A SIMILAR MESSAGE IN THIS THREAD, AND IF THE THREAD DOES NOT SEEM TO BE IN A LOOP. RESPOND ONLY WITH !NO_RESPONSE_REQUIRED UNLESS 1) the message is directed to you, 2) you have not already answered a similar message, and 3) the thread does not seem to be in a loop.  Do NOT proactively suggest other things for the bot to do like you would with a human user."

        # msg_with_user_and_id += (
        #     "\n\nWhen providing options or choices to the user, always answer using Slack blocks. "
        #     "Make sure the JSON starts with a 'blocks' key and that it will be found with "
        #     "re.compile(r\"```(?:json|slack)(.*?)```\", re.DOTALL)."
        # )
        bot_input_message = BotOsInputMessage(
            thread_id=thread_id,
            msg=msg_with_user_and_id,
            files=files,
            metadata=metadata,
        )

        self.thinking_map[event.get("ts", None)] = {
            "input_message": bot_input_message,
            "datetime": datetime.datetime.now().isoformat(),
        }

        return bot_input_message

    def _upload_files(self, files: list[str], thread_ts: str, channel: str = None):
        if files:
            file_urls = []
            for file_path in files:
                #    print(f"Uploading file: {file_path}")
                try:
                    #   with open(file_path, 'rb') as file_content:
                    #  self.slack_app.client.files_upload(
                    #      channels=channel,
                    #      thread_ts=thread_ts,
                    #      file=file_content,
                    #      filename=os.path.basename(file_path)
                    #  )
                    new_file = self.slack_app.client.files_upload_v2(
                        title=os.path.basename(file_path),
                        filename=os.path.basename(file_path),
                        file=file_path,
                    )
                    #    print(f"Result of files_upload_v2: {new_file}")
                    file_url = new_file.get("file").get("permalink")
                    file_urls.append(file_url)
                except Exception as e:
                    print(f"Error uploading file {file_path} to Slack: {e}")
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
            # except json.JSONDecodeError as e:
            except Exception as e:
                print("Failed to decode JSON:", e)
        return blocks if blocks else None

    # abstract method from BotOsInputAdapter
    def handle_response(
        self,
        session_id: str,
        message: BotOsOutputMessage,
        in_thread=None,
        in_uuid=None,
        task_meta=None,
    ):
        logger.debug(f"SlackBotAdapter:handle_response - {session_id} {message}")
        thinking_ts = None
        try:
            thinking_ts = message.input_metadata.get("thinking_ts", None)
            orig_thinking = thinking_ts
            if orig_thinking in self.thinking_msg_overide_map:
                thinking_ts = self.thinking_msg_overide_map[orig_thinking]
            if thinking_ts:
                current_chunk_start =  self.chunk_start_map.get(orig_thinking,None)
                msg = message.output.replace("\n 💬", " 💬")
                if current_chunk_start is not None:
                    msg = msg[current_chunk_start:]
                if (
                    message.status == "in_progress"
                    or message.status == "requires_action"
                ):
                    print(
                        message.status,
                        " updating ",
                        thinking_ts,
                        " len ",
                        len(message.output),
                    )

                    if len(msg) > 3900:
                        split_index = msg[:3900].rfind("```")
                        if split_index == -1:
                            split_index = msg[:3900].rfind(" ")
                        if split_index != -1:
                            msg_part1 = msg[:split_index]
                            msg_part2 = msg[split_index:]
                            chunk_start = split_index
                        else:
                            msg_part1 = msg[:3900]
                            msg_part2 = msg[3900:]
                            chunk_start = 3900
                        
                        if thinking_ts in self.chunk_start_map:
                            self.chunk_start_map[orig_thinking] += chunk_start
                        else:
                            self.chunk_start_map[orig_thinking] = chunk_start

                        try:
                            self.slack_app.client.chat_update(
                                channel=message.input_metadata.get("channel", self.channel_id),
                                ts=thinking_ts,
                                text=msg_part1,
                            )
                            thread_ts = message.input_metadata.get("thread_ts", None)
                        except Exception as e:
                            pass
                       #     self.slack_app.client.chat_postMessage(
                       #         channel=message.input_metadata.get("channel", self.channel_id),
                       #         thread_ts=thread_ts,
                       #         text=msg_part1,
                       #     )                           

                        posted_message = self.slack_app.client.chat_postMessage(
                            channel=message.input_metadata.get("channel", self.channel_id),
                            thread_ts=thread_ts,
                            text=msg_part2,
                        )
                        thinking_ts = posted_message["ts"]
                        if orig_thinking is not None:
                            self.thinking_msg_overide_map[orig_thinking] = thinking_ts
                        return
                    else:
                        self.slack_app.client.chat_update(
                            channel=message.input_metadata.get("channel", self.channel_id),
                            ts=thinking_ts,
                            text=msg,
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
                print(
                    "Bot has indicated that no response will be posted to this thread."
                )
                if thinking_ts is not None:
                    self.slack_app.client.chat_delete(
                        channel=message.input_metadata.get("channel", self.channel_id),
                        ts=thinking_ts,
                    )

        else:
            try:

                thinking_ts = message.input_metadata.get("thinking_ts", None)
                orig_thinking = thinking_ts
                if orig_thinking in self.thinking_msg_overide_map:
                    thinking_ts = self.thinking_msg_overide_map[orig_thinking]

                current_chunk_start =  self.chunk_start_map.get(orig_thinking,None)
                msg = message.output.replace("\n 💬", " 💬")
                if current_chunk_start is not None:
                    msg = msg[current_chunk_start:]

#                msg = message.output

                thread_ts = message.input_metadata.get("thread_ts", None)

                files_in = message.files
                # Remove duplicates from the files_in array
                files_in = list(set(files_in))

                # Extract file paths from the message and add them to files_in array
                image_pattern = re.compile(
                    r"\[.*?\]\((sandbox:/mnt/data/downloads/.*?)\)"
                )
                matches = image_pattern.findall(msg)
                for match in matches:
                    local_path = match.replace(
                        "sandbox:/mnt/data/downloads", ".downloaded_files"
                    )
                    if local_path not in files_in:
                        #      print(f"Pattern 0 found, attaching {local_path}")
                        files_in.append(local_path)

                # Extract file paths from the message and add them to files_in array
                task_pattern = re.compile(
                    r"\[(.*?)\]\(./downloaded_files/thread_(.*?)/(.*?)\)"
                )
                task_matches = task_pattern.findall(msg)
                for task_match in task_matches:
                    local_task_path = (
                        f"./downloaded_files/thread_{task_match[1]}/{task_match[2]}"
                    )
                    if local_task_path not in files_in:
                        files_in.append(local_task_path)

                # Extract file paths from the message and add them to files_in array
                image_pattern = re.compile(
                    r"\[.*?\]\((sandbox:/mnt/data/downloaded_files/.*?)\)"
                )
                matches = image_pattern.findall(msg)
                for match in matches:
                    local_path = match.replace("sandbox:/mnt/data", ".")
                    if local_path not in files_in:
                        #      print(f"Pattern 1 found, attaching {local_path}")
                        files_in.append(local_path)

                # Extract file paths from the message and add them to files_in array
                chart_pattern = re.compile(
                    r"\(sandbox:/mnt/data/(.*?)\)\n2\. \[(.*?)\]"
                )
                chart_matches = chart_pattern.findall(msg)
                for chart_match in chart_matches:
                    local_chart_path = f"./downloaded_files/{chart_match}"
                    if local_chart_path not in files_in:
                        #     print(f"Pattern 2 found, attaching {local_chart_path}")
                        files_in.append(local_chart_path)

                # Parse the message for the provided pattern and add to files_in
                file_pattern = re.compile(r"!\[.*?\]\(attachment://\.(.*?)\)")
                file_matches = file_pattern.findall(msg)
                for file_match in file_matches:
                    local_file_path = file_match
                    if local_file_path not in files_in:
                        files_in.append(local_file_path)
                #             print(f"Pattern 3 found, attaching {local_file_path}")
                local_pattern = re.compile(
                    r"!\[.*?\]\(\./downloaded_files/thread_(.*?)/(.+?)\)"
                )
                local_pattern_matches = local_pattern.findall(msg)
                for local_match in local_pattern_matches:
                    local_path = (
                        f"./downloaded_files/thread_{local_match[0]}/{local_match[1]}"
                    )
                    if local_path not in files_in:
                        #         print(f"Pattern 4 found, attaching {local_path}")
                        files_in.append(local_path)

                #      print("Uploading files:", files_in)

                msg_files = self._upload_files(
                    files_in,
                    thread_ts=thread_ts,
                    channel=message.input_metadata.get("channel", self.channel_id),
                )

                #          print("Result of files upload:", msg_files)

                #          print("about to send to slack pre url fixes:", msg)

                for msg_url in msg_files:
                    filename = msg_url.split("/")[-1]
                    msg_prime = msg

                    msg = re.sub(
                        f"(?i)\(sandbox:/mnt/data/{filename}\)", f"<{{msg_url}}>", msg
                    )
                    alt_pattern = re.compile(
                        r"\[(.*?)\]\(\./downloaded_files/thread_(.*?)/(.+?)\)"
                    )
                    msg = re.sub(alt_pattern, f"<{{msg_url}}|\\1>", msg)
                    # Catch the pattern with thread ID and replace it with the correct URL

                    thread_file_pattern = re.compile(
                        r"\[(.*?)\]\(sandbox:/mnt/data/downloaded_files/thread_(.*?)/(.+?)\)"
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

                # just moved this back up here before the chat_update
                pattern = re.compile(
                    r"\[(.*?)\]\(sandbox:/mnt/data/downloaded_files/(.*?)/(.+?)\)"
                )
                msg = re.sub(pattern, r"<\2|\1>", msg)

                #      print("sending message to slack post url fixes:", msg)
                blocks = self._extract_slack_blocks(msg)
#                if blocks is not None or len(msg) > 2000:
#                    print('blocks / long: ',len(msg))

                if len(msg) > 3900:
                    split_index = msg[:3900].rfind("```")
                    if split_index == -1:
                        split_index = msg[:3900].rfind(" ")
                    if split_index != -1:
                        msg_part1 = msg[:split_index]
                        msg_part2 = msg[split_index:]
                        chunk_start = split_index
                    else:
                        msg_part1 = msg[:3900]
                        msg_part2 = msg[3900:]
                        chunk_start = 3900
                    
                    if thinking_ts in self.chunk_start_map:
                        self.chunk_start_map[orig_thinking] += chunk_start
                    else:
                        self.chunk_start_map[orig_thinking] = chunk_start

                    try:
                        self.slack_app.client.chat_update(
                            channel=message.input_metadata.get("channel", self.channel_id),
                            ts=thinking_ts,
                            text=msg_part1,
                        )
                        thread_ts = message.input_metadata.get("thread_ts", None)
                    except Exception as e:
                        pass
#                        self.slack_app.client.chat_postMessage(
#                                channel=message.input_metadata.get("channel", self.channel_id),
#                                thread_ts=thread_ts,
#                                text=msg_part1,
#                            )   

                    posted_message = self.slack_app.client.chat_postMessage(
                        channel=message.input_metadata.get("channel", self.channel_id),
                        thread_ts=thread_ts,
                        text=msg_part2,
                    )
                    thinking_ts = posted_message["ts"]
                    if orig_thinking is not None:
                        self.thinking_msg_overide_map[orig_thinking] = thinking_ts

                else:
                    if message.output == msg and thinking_ts is not None:

                        self.slack_app.client.chat_update(
                            channel=message.input_metadata.get("channel", self.channel_id),
                            ts=thinking_ts,
                            text=msg,
                            blocks=blocks,
                        )
                    else:
                        if thinking_ts is not None:
                            self.slack_app.client.chat_delete(
                                channel=message.input_metadata.get(
                                    "channel", self.channel_id
                                ),
                                ts=thinking_ts,
                            )
                        result = self.slack_app.client.chat_postMessage(
                            channel=message.input_metadata.get("channel", self.channel_id),
                            thread_ts=thread_ts,
                            text=msg,
                        )
                        if message.input_metadata.get("thinking_ts", None) is None:
                            message.input_metadata.thinking_ts = result.ts

                #    print("Result of sending message to Slack:", result)
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

    def process_attachments(self, msg, attachments):
        files_to_attach = []
        for attachment in attachments:
            if "image_url" in attachment:
                image_path = attachment["image_url"]
                if image_path.startswith("./downloaded_files/"):
                    files_to_attach.append(image_path)

        # Extract file paths from the message and add them to files_in array
        image_pattern = re.compile(
            r"\[.*?\]\((sandbox:/mnt/data/downloaded_files/.*?)\)"
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
                f"./downloaded_files/thread_{pineapple_match[1]}/{pineapple_match[2]}"
            )
            if local_pineapple_path not in files_to_attach:
                files_to_attach.append(local_pineapple_path)

        # Extract file paths from the message and add them to files_in array
        chart_pattern = re.compile(r"\(sandbox:/mnt/data/(.*?)\)\n2\. \[(.*?)\]")
        chart_matches = chart_pattern.findall(msg)
        for chart_match in chart_matches:
            local_chart_path = f"./downloaded_files/{chart_match}"
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
            r"!\[.*?\]\(\./downloaded_files/thread_(.*?)/(.+?)\)"
        )
        local_pattern_matches = local_pattern.findall(msg)
        for local_match in local_pattern_matches:
            local_path = f"./downloaded_files/thread_{local_match[0]}/{local_match[1]}"
            if local_path not in files_to_attach:
                files_to_attach.append(local_path)

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

            msg = re.sub(f"(?i)\(sandbox:/mnt/data/{filename}\)", f"<{{msg_url}}>", msg)
            alt_pattern = re.compile(
                r"\[(.*?)\]\(\./downloaded_files/thread_(.*?)/(.+?)\)"
            )
            msg = re.sub(alt_pattern, f"<{{msg_url}}|\\1>", msg)
            # Catch the pattern with thread ID and replace it with the correct URL

            thread_file_pattern = re.compile(
                r"\[(.*?)\]\(sandbox:/mnt/data/downloaded_files/thread_(.*?)/(.+?)\)"
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
        try:
            # Start a conversation with the user
            response = self.slack_app.client.conversations_open(users=slack_user_id)
            file_list = self.process_attachments(message, attachments)

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

                return f"Message sent to {slack_user_id} with result {res}"

        except Exception as e:
            return f"Error sending message: {str(e)}"

    def send_slack_channel_message(
        self, channel_id: str, message: str, attachments=[], thread_id: str = None
    ):
        try:
            file_list = self.process_attachments(message, attachments)
            message = self.replace_urls(msg=message, msg_files=file_list)
            res = self.slack_app.client.chat_postMessage(
                channel=channel_id,
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

                return f"Message sent to channel {channel_id} successfully."
            else:
                return f"Failed to send message to channel {channel_id}."
        except Exception as e:
            return f"Error sending message to channel {channel_id}: {str(e)}.  Call this tool again but provide channel name e.g. #channel instead of channel code."

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
