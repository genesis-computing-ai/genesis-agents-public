from __future__ import annotations  # for python 9 support of | type operator
from collections import deque
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

# module level 
meta_lock =  threading.Lock()
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

    def __init__(self, token:str, signing_secret:str, channel_id:str, bot_user_id:str, bot_name:str='Unknown', slack_app_level_token=None) -> None:
        logger.debug("SlackBotAdapter")
        super().__init__()
        self.slack_app = App(
            token=token,
            signing_secret=signing_secret
        )
        self.channel_id = channel_id
        self.slack_app.event("message")(self.handle_message_events_old)
        self.handler = SlackRequestHandler(self.slack_app)
        self.events = deque()
        self.bot_user_id = bot_user_id
        self.user_info_cache = {}
        self.bot_name = bot_name
        self.last_message_id_dict = {} 
        
        self.events_lock =  threading.Lock()

        if slack_app_level_token:
            self.slack_app_level_token = slack_app_level_token      

            # Initialize Slack Bolt app
            self.slack_socket = App(token=slack_app_level_token)

            # Define Slack event handlers
            @self.slack_socket.event("message")
            def handle_message_events(event, say):
                print(f'event type: {event.get("type","no type")}, text: {event.get("text","no text")}')
                if self.bot_user_id == event.get("user","NO_USER"):
                    self.last_message_id_dict[event.get("thread_ts",None)] = event.get("ts",None)
                if event.get("text","no text") != '_thinking..._' and self.bot_user_id != event.get("user","NO_USER") and event.get("subtype","none") != 'message_changed' and event.get("subtype","none") != 'message_deleted':
                    with self.events_lock:
                        self.events.append(event)

            @self.slack_socket.event("app_mention")
            def mention_handler(event, say):
                pass
        #        print(f'event type: {event.get("type","no type")}, text: {event.get("text","no text")}')
        #        if event.get("text","no text") != '_thinking..._' and self.bot_user_id != event.get("user","NO_USER") and event.get("subtype","none") != 'message_changed':
        #            self.events.append(event)

            def run_slack_app():
                handler = SocketModeHandler(self.slack_socket, slack_app_level_token)
                handler.start()

            # Run Slack app in a separate thread
            slack_thread = threading.Thread(target=run_slack_app)
            slack_thread.start()



    def add_event(self, event):
        self.events.append(event)

    def handle_message_events_old(self, event, context, say, logger):
        logger.info(event)  # Log the event data (optional)
        text = event.get('text', '')
        print('AT HANDLE MESSAGE EVENTS???')
        logger.debug(f"SlackBotAdapter:handle_message_events - {text}")
        thread_ts = event.get('thread_ts', event.get('ts', ''))
        channel_type = event.get('channel_type', '')
        if f"<@{self.bot_user_id}>" in text or (self.bot_user_id,thread_ts) in thread_ts_dict or (channel_type == 'im' and text != ''):
            with self.events_lock:
                self.events.append(event)
            if (self.bot_user_id, thread_ts) not in thread_ts_dict:
                with meta_lock:
                    thread_ts_dict[self.bot_user_id,thread_ts] = {"event": event, "thread_id": None}
        else:
            logger.debug(f"SlackBotAdapter:handle_message_events - no mention skipped")

    # callback from Flask
    def slack_events(self):
        data = request.json
        # Check if this is a challenge request
        if data is not None and data['type'] == 'url_verification':
            # Respond with the challenge value
            return jsonify({'challenge': data['challenge']})  
#        return self.handler.handle(request)
        return self.handler.handle(request)

    def _download_slack_files(self, event, thread_id='no_thread') -> list:
        files = []
        for file_info in event['files']:
            url_private = file_info.get('url_private')
            file_name = file_info.get('name')
            if url_private and file_name:
                local_path = f"./downloaded_files/{thread_id}/{file_name}"
                os.makedirs(os.path.dirname(local_path), exist_ok=True)

                try:
                    with requests.get(url_private, headers={'Authorization': 'Bearer %s' % self.slack_app._token}, stream=True) as r:
                        # Raise an exception for bad responses
                        r.raise_for_status()
                        # Open a local file with write-binary mode
                        with open(local_path, 'wb') as f:
                            # Write the content to the local file
                            for chunk in r.iter_content(chunk_size=32768):
                                f.write(chunk)                            # Raise an exception for bad responses
                      #      f.write(r.content)
                        
                        files.append(local_path)
                except Exception as e:
                    logger.error(f"Error downloading file from {url_private}: {e}")
        return files

    # abstract method from BotOsInputAdapter
    def get_input(self) -> BotOsInputMessage|None:
        #logger.info(f"SlackBotAdapter:get_input")
        files=[]
        
        with self.events_lock:
            if len(self.events) == 0:
                return None        
            try:
                event = self.events.popleft()
            except IndexError:
                return None

        msg = event.get('text', '')

        if msg.strip().lower() == "!delete":
            thread_ts = event.get('thread_ts', event.get('ts', ''))
            last_message_id = self.last_message_id_dict.get(thread_ts)
            if last_message_id:
                try:
                    # Attempt to delete the last message
                    self.slack_app.client.chat_delete(channel=event.get('channel'), ts=last_message_id)
                    # Remove the message ID from the dictionary after deletion
                    del self.last_message_id_dict[thread_ts]
                except Exception as e:
                    logger.error(f"Error deleting message with ts={last_message_id}: {e}")
            return None  # Do not process further if it's a delete command


        if msg == '_thinking..._':
            return None

        active_thread = False
        thread_ts = event.get('thread_ts', event.get('ts', ''))
        channel_type = event.get('channel_type', '')
        
        #print(f"{uniq} {self.bot_name}-Looking for {(self.bot_user_id, thread_ts)}-Is in? {(self.bot_user_id, thread_ts) in thread_ts_dict}-Current keys in thread_ts_dict:", thread_ts_dict.keys())
        tag = f"<@{self.bot_user_id}>" in msg
        indic = (self.bot_user_id, thread_ts) in thread_ts_dict
        dmcheck = (channel_type == 'im' and msg != '')
        print(f"{uniq} --> get_input for {self.bot_user_id}/{self.bot_name} msg30: {msg[:30]}... {tag},{indic},{dmcheck}", flush=True)
        if tag or indic or dmcheck:
#        if f"<@{self.bot_user_id}>" in msg or thread_ts in self.thread_ts_dict or (channel_type == 'im' and msg != ''):
            active_thread = True
            if (self.bot_user_id,thread_ts) not in thread_ts_dict:
           #     print(f'{uniq}     --ENGAGE/ADD>  Adding {thread_ts} to dict', flush=True)
                with meta_lock:
                    thread_ts_dict[self.bot_user_id,thread_ts] = {"event": event, "thread_id": None}
            #    print(f"{uniq} {self.bot_name}-ADDED-Now is {(self.bot_user_id,thread_ts)} in??-Is in? {(self.bot_user_id,thread_ts) in thread_ts_dict}-Current keys in thread_ts_dict:", thread_ts_dict.keys(),flush=True)

         #   else:
             #   print(f'{uniq}     --ENGAGE/EXISTING>  {thread_ts} already in dict', flush=True)

        if active_thread is False:
            # public channel, not flagged yet in thread
            return None

        thread_id = thread_ts
#     
        channel = event.get('channel', '')

        if os.getenv('THINKING_TOGGLE', 'true').lower() != 'false':
            thinking_message = self.slack_app.client.chat_postMessage(
                    channel   = channel, 
                    thread_ts = thread_ts,
                    text      = "_thinking..._")
            thinking_ts = thinking_message["ts"]
        else:
            thinking_ts = None

        if 'files' in event:
            print(f"    --/DOWNLOAD> downloading files for ({self.bot_name}) ")
            files = self._download_slack_files(event, thread_id = thread_id)
            print(f"    --/DOWNLOADED> downloaded files for ({self.bot_name}) ")

        try:
            user_id = event["user"]
            if user_id not in self.user_info_cache:
                user_info = self.slack_app.client.users_info(user=user_id)
                self.user_info_cache[user_id] = user_info['user']['real_name']
            user_full_name = self.user_info_cache[user_id]
            msg_with_user_and_id = f"{user_full_name} ({user_id}): {msg}"
        except Exception as e:
            print(f"    --NOT A USER MESSAGE, SKIPPING {e} ")
            # not a user message
            return None

        if thinking_ts:
            metadata={"thread_ts": thread_ts,
                        "channel": channel,
                        "thinking_ts": thinking_ts,
                        "channel_type": event.get('channel_type', '')}
        else:
            metadata={"thread_ts": thread_ts,
                        "channel": channel,
                        "channel_type": event.get('channel_type', '')}
 
        return BotOsInputMessage(thread_id=thread_id, msg=msg_with_user_and_id, files=files, 
                                metadata=metadata) 
    
    def _upload_files(self, files:list[str], thread_ts:str, channel:str):
        if files:
            file_urls = []
            for file_path in files:
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
                    file_url = new_file.get("file").get("permalink")
                    file_urls.append(file_url)
                except Exception as e:
                    print(f"Error uploading file {file_path} to Slack: {e}")
            return file_urls
        else:
            return []


    # abstract method from BotOsInputAdapter
    def handle_response(self, session_id:str, message:BotOsOutputMessage, in_thread=None, in_uuid=None):
        logger.debug(f"SlackBotAdapter:handle_response - {session_id} {message}")
        try:
            thinking_ts = message.input_metadata.get("thinking_ts", None)
            if thinking_ts:
                self.slack_app.client.chat_delete(channel= message.input_metadata.get("channel",self.channel_id),ts = thinking_ts)
        except Exception as e:
            logger.debug("thinking already deleted") # FixMe: need to keep track when thinking is deleted
        message.output = message.output.strip()

        if message.output.startswith("<Assistant>"):
            message.output = message.output[len("<Assistant>"):].strip()

        if message.output == "!NO_RESPONSE_REQUIRED":
            print("Bot has indicated that no response will be posted to this thread.")
        else:
            try:

                thread_ts = message.input_metadata.get("thread_ts", None)
                msg_files = self._upload_files(message.files, thread_ts=thread_ts, channel=message.input_metadata.get("channel", self.channel_id))

                msg = message.output
                for msg_url in msg_files:
                    filename = msg_url.split('/')[-1]
                    msg_prime = msg
                    msg = re.sub(f"(?i)\(sandbox:/mnt/data/{filename}\)", f"<{{msg_url}}>", msg)
                    msg = msg.replace('{msg_url}',msg_url)
                    msg = msg.replace('[Download ','[')
                    if msg == msg_prime:
                        msg += ' {'+msg_url+'}'
                # Reformat the message if it contains a link in brackets followed by a URL in angle brackets
                link_pattern = re.compile(r'\[(.*?)\]<(.+?)>')
                msg = re.sub(link_pattern, r'<\2|\1>', msg)
                result = self.slack_app.client.chat_postMessage(
                    channel=message.input_metadata.get("channel", self.channel_id),
                    thread_ts=thread_ts,
                    text=msg 
                )
                # Utility function handles file uploads and logs errors internally
                if thread_ts is not None:
                    with meta_lock:
                        thread_ts_dict[self.bot_user_id,thread_ts]["thread_id"] = message.thread_id # store thread id so we can map responses to the same assistant thread
            except Exception as e:
                logger.error(f"SlackBotAdapter:handle_response - Error posting message: {e}")

    def send_slack_direct_message(self, slack_user_id:str, message:str, thread_id:str):
        try:
            # Start a conversation with the user
            response = self.slack_app.client.conversations_open(users=slack_user_id)

            if response['ok']:
                channel_id = response['channel']['id']
                # Post a message to the new conversation
                res = self.slack_app.client.chat_postMessage(channel=channel_id, text=message)
                thread_ts = res["ts"]
                if (self.bot_user_id,thread_ts) not in thread_ts_dict:
                    with meta_lock:
                        thread_ts_dict[self.bot_user_id,thread_ts] = {"event": None, "thread_id": thread_id}

                return f"Message sent to {slack_user_id} with result {res}"
            
        except Exception as e:
            return f"Error sending message: {str(e)}"
        
    def send_slack_channel_message(self, channel_id:str, message:str, thread_id:str):
        try:
            res = self.slack_app.client.chat_postMessage(channel=channel_id, text=message)
            if res['ok']:
                thread_ts = res["ts"]
                if (self.bot_user_id,thread_ts) not in thread_ts_dict:
                    with meta_lock:
                        thread_ts_dict[self.bot_user_id,thread_ts] = {"event": None, "thread_id": thread_id}

                return f"Message sent to channel {channel_id} successfully."
            else:
                return f"Failed to send message to channel {channel_id}."
        except Exception as e:
            return f"Error sending message to channel {channel_id}: {str(e)}"


    def lookup_slack_user_id_real(self, user_name:str, thread_id:str):
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
            if response['ok']:
                # Iterate through the users to find a matching display name or real name
                for member in response['members']:
                    if 'name' in member and member['name'].lower() == user_name:
                        return member['id']
                    if 'profile' in member and 'display_name' in member['profile'] and member['profile']['display_name'].lower() == user_name:
                        return member['id']
                    if 'profile' in member and 'real_name' in member['profile'] and member['profile']['real_name'].lower() == user_name:
                        return member['id']
                return "Error: Slack user not found."
            else:
                return f"Error: Slack API users.list call was not successful. Response: {response}"
        except Exception as e:
            return f"Error: Exception occurred while looking up Slack user ID: {str(e)}"


    def lookup_slack_user_id(self, user_name:str, thread_id:str): # FixMe: replace with real implementation querying slack
   
        user_id = self.lookup_slack_user_id_real(user_name, thread_id)
        if not user_id.startswith("Error:"):
            return user_id
        else:
            return "Error: unknown slack user.  Maybe use the list_all_bots function to see if its a bot?"
   
 