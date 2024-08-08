from flask import Blueprint, request, render_template, make_response, jsonify
import uuid
import os
#from connectors.snowflake_connector import SnowflakeConnector
import logging
from core.bot_os_input import BotOsInputAdapter, BotOsInputMessage, BotOsOutputMessage
from collections import deque
import asyncio
from botbuilder.core import ActivityHandler, MessageFactory, TurnContext

import json


logger = logging.getLogger(__name__)


class TeamsBotOsInputAdapter(BotOsInputAdapter):
    #teams_snow_connector = SnowflakeConnector(connection_name='Snowflake')

    def __init__(self, bot):
        # from bots.echo_bot import EchoBot
        super().__init__()
        
        self.response_map = {}
        self.proxy_messages_in = []
        self.events = deque()
        self.genbot_internal_project_and_schema = os.getenv('GENESIS_INTERNAL_DB_SCHEMA','None')
        self.bot = bot
        self.bot_id= {}
        #self.echo_bot_instance = EchoBot()
        #self.teams_bot_handler = TeamsBotOsInputAdapter(self.echo_bot_instance)


    def add_event(self, event):
        self.events.append(event)

    def get_input(self, thread_map=None, active=None, processing=None, done_map=None) -> BotOsInputMessage | None:
        if len(self.events) == 0:
            return None        
        try:
            event = self.events.popleft()
        except IndexError:
            return None
        uu = event.get('uuid',None)
        bot_id = event.get('bot_id', {})
        metadata = {}
        if uu:
            metadata["input_uuid"] = uu
        metadata["channel_type"] = "msteams"
        metadata["channel_name"] = ""
        metadata["user_id"] = bot_id.get('user_id', 'Unknown User ID')
        metadata["user_name"] = bot_id.get('user_name', 'Unknown User')
        return BotOsInputMessage(thread_id=event.get('thread_id'), msg=event.get('msg'), metadata=metadata)
    
    async def return_result(self, turn_context: TurnContext, message: BotOsOutputMessage):
        logger.info(f"return_result called with turn_context: {turn_context} and message: {message}")
        await turn_context.send_activity(
            MessageFactory.text(f"Response: {message.output}")
        )
    
    def handle_response(self, session_id: str, message: BotOsOutputMessage, in_thread=None, in_uuid=None, task_meta=None):
        if in_uuid is not None:
            if message.output == '!NO_RESPONSE_REQUIRED':
                self.response_map[in_uuid] = "(no response needed)"
            else:
                self.response_map[in_uuid] = message.output
        print("Message output: {message.output}")
        self.return_result(TurnContext, message)
       #MessageFactory.text(f"Response: {message.output}")
   


    def submit(self, input, thread_id, bot_id):
        if type(bot_id) == str:
            bot_id = json.loads(bot_id)
        
        uu = str(uuid.uuid4())
        self.proxy_messages_in.append({"msg": input, "uuid": uu, "thread_id": thread_id, "bot_id": bot_id})
       
        self.add_event({"msg": input, "thread_id": thread_id, "uuid": uu, "bot_id": bot_id})
      
        #TeamsBotOsInputAdapter.teams_snow_connector.db_insert_llm_results(uu, "")
        return uu 
    
    