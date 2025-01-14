from flask import Blueprint, request, render_template, make_response
import uuid
from genesis_bots.core.bot_os_input import BotOsInputAdapter, BotOsInputMessage, BotOsOutputMessage
from collections import deque

from genesis_bots.core.logging_config import logger

class TaskBotOsInputAdapter(BotOsInputAdapter):
    def __init__(self):
        super().__init__()
        self.response_map = {}
        self.proxy_messages_in = []
        self.events = deque()

    def add_event(self, event):
        self.events.append(event)

    def get_input(self, thread_map=None,  active=None, processing=None, done_map=None):
        if len(self.events) == 0:
            return None
        try:
            event = self.events.popleft()
        except IndexError:
            return None
        uu = event.get('task_meta',None)
        metadata = {}
        if uu:
            metadata["task_meta"]=uu
        return BotOsInputMessage(thread_id=event.get('thread_id'), msg=event.get('msg'), metadata=metadata)

    def handle_response(self, session_id: str, message: BotOsOutputMessage, in_thread=None, in_uuid=None, task_meta=None):
        # Here you would implement how the Flask app should handle the response.
        # For example, you might send the response back to the client via a WebSocket
        # or store it in a database for later retrieval.
        #logger.info("UDF output: ",message.output, ' in_uuid ', in_uuid)
        logger.info(f'TASK RESPONSE FOR {message.input_metadata}')
        # save the response to a map, then process it from the main task loop
        if 'task_meta' in message.input_metadata:
            self.response_map[message.input_metadata['task_meta']['task_id']] = message
        else:
            self.response_map[message.input_metadata['task_id']] = message
        return

