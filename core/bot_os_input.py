from __future__ import annotations
from abc import abstractmethod
from typing import Optional
from core.logging_config import logger

class BotOsInputMessage:
    def __init__(self, thread_id:str, msg:str, files:Optional(list) = None, metadata:Optional(dict) = None) -> None: # type: ignore
        self.thread_id = thread_id
        self.msg = msg
        self.files = files or list()
        self.metadata = metadata or dict()

class BotOsOutputMessage:
    def __init__(self, thread_id:str, status:str, output, messages, files:Optional(list) = None,
                 input_metadata:Optional(dict) = None) -> None:
        self.thread_id = thread_id
        self.status = status
        self.output = output
        self.messages = messages
        self.files = files or list()
        self.input_metadata = input_metadata or dict()

class BotOsInputAdapter:
    def __init__(self) -> None:
        self.thread_id = None

    # allows for polling from source
    @abstractmethod
    def add_event(self, event):
        pass

    # allows for polling from source
    @abstractmethod
    def get_input(self, thread_map=None,  active=None, processing=None, done_map=None) -> BotOsInputMessage|None:
        pass

    # allows response to be sent back with optional reply
    @abstractmethod
    def handle_response(self, session_id:str, message:BotOsOutputMessage, in_thread=None, in_uuid=None, task_meta=None):
        pass

class BotInputAdapterCLI(BotOsInputAdapter):
    def __init__(self, initial_message:str, prompt_on_response=True) -> None:
        super().__init__()
        self.next_message = initial_message
        self.prompt_on_response = prompt_on_response

    def get_input(self, thread_map=None,  active=None, processing=None, done_map=None) -> BotOsInputMessage|None:
        if self.next_message is None or self.thread_id is None:
            return None

        prompt = self.next_message
        self.next_message = None
        files=[]
        return BotOsInputMessage(thread_id=self.thread_id, msg=prompt, files=files)

    def handle_response(self, session_id:str, message:BotOsOutputMessage):
        logger.info(f"{session_id} - {message.thread_id} - {message.status} - {message.output}")
        if self.prompt_on_response:
            self.next_message = input(f"[{self.thread_id}]> ") #FixMe do we need self.thread_id
