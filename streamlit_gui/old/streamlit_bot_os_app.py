from __future__ import annotations  # for python 9 support of | type operator
from collections import deque
import logging
from slack_bolt import App
import os, time
from slack_bolt.adapter.flask import SlackRequestHandler
from flask import jsonify, request
from bot_os_input import BotOsInputAdapter, BotOsInputMessage, BotOsOutputMessage
logger = logging.getLogger(__name__)
from openai import OpenAI
import os
from typing import Callable
import streamlit as st

class BotInputStreamlit(BotOsInputAdapter):
    def __init__(self, initial_message:str, prompt_on_response=True) -> None:
        super().__init__()

        self.next_message = initial_message
        self.prompt_on_response = prompt_on_response

        #self.slack_app.event("message")(self.handle_message_events)
        #self.handler = SlackRequestHandler(self.slack_app)
        self.events = deque()
        self.responses = deque()

    def submit_chat_line(self, prompt):

        self.events.append(prompt)
        print("appended ",prompt, " ", len(self.events) )
    

    def get_input(self, thread_map=None,  active=None, processing=None, done_map=None) -> BotOsInputMessage|None:

        print("len input=",len(self.events)," len responsese=",len(self.responses) )
        
        if len(self.events) == 0:
            return None

        files = []
        event = self.events.popleft()
        msg   = event

#        print("MESSAGE ", msg)
#        files = event.get('files','') #FixMe dowload files so they can be uploaded to slack
#        thread_ts = event.get('ts', '')
#        channel = event.get('channel', '')

        #thinking_ts = thinking_message.data["ts"] 
        #self.slack_app.client.chat_update(channel = channel, ts = thinking_ts, text = "_still thinking...")
        #self.slack_app.client.chat_delete(channel= channel,ts = thinking_ts)

        return BotOsInputMessage(thread_id=self.thread_id, msg=msg, files=files, 
                                 metadata={}) 


    def handle_response(self, session_id:str, message:BotOsOutputMessage): 
       # print("RESPONSE HANDLE!!!")

       # self.messages.append({"role": "assistant", "content": message})

        self.responses.append(message.output)

       # print(f"{session_id} - {message.thread_id} - {message.status} - {message.output}")
       # if self.prompt_on_response:
       #     self.next_message = input(f"[{self.thread_id}]> ") #FixMe do we need self.thread_id 
    


#######

