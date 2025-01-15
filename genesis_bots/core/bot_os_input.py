from __future__ import annotations
from   abc                      import abstractmethod
from   genesis_bots.core.bot_os_tools2       import ToolFuncDescriptor
from   genesis_bots.core.bot_os_utils        import truncate_string
from   genesis_bots.core.logging_config      import logger
import time
from   typing                   import Optional
import uuid
from enum import Enum



class BotOsInputMessage:

    ALLOWED_MSG_TYPES = ("chat_input", "client_function_definition")

    def __init__(self,
                 thread_id:str,
                 msg:str,
                 files:Optional(list) = None,
                 metadata:Optional(dict) = None,
                 msg_type: str = "chat_input"
                 ) -> None: # type: ignore
        self.thread_id = thread_id
        self.msg = msg
        self.files = files or list()
        self.metadata = metadata or dict()
        msg_type = msg_type.lower()
        assert msg_type in self.ALLOWED_MSG_TYPES, f"Unrecognized {self.__class__.__name__} msg_type value: {msg_type}. Expected one of: {self.ALLOWED_MSG_TYPES}"
        self.msg_type = msg_type


class BotOsOutputMessage:

    # list of allowed status values, designed to match a subset of the ApenAI run and Message status values
    ALLOWED_STATUS_VALUES = ("in_progress",
                             "completed",
                             "incomplete",
                             "requires_action", # an information status indicating that the LLM has requested an action.
                             "user_invocation_required" # indicating this message is a request for a user to invoke a function
                             )

    def __init__(self,
                 thread_id:str,
                 status:str,
                 output,
                 messages,
                 files: Optional[list] = None,
                 input_metadata:Optional[dict] = None) -> None:
        status = status.lower()
        if status not in self.ALLOWED_STATUS_VALUES:
            logger.warning(f"Unrecognized {self.__class__.__name__} status value: {status}. Expected one of: {self.ALLOWED_STATUS_VALUES}")
        self.thread_id = thread_id
        self.status = status
        self.output = output
        self.messages = messages
        self.files = files or list()
        self.input_metadata = input_metadata or dict()


class BosOsClientAsyncToolInvocationHandle(BotOsOutputMessage):


    class ResultStatusEnum(Enum):
        UNSET = "UNSET"
        PENDING_CONSUMPTION = "PENDING_CONSUMPTION"
        CONSUMED_SUCCESS = "CONSUMED_SUCCESS"
        CONSUMED_TIMEOUT = "CONSUMED_TIMEOUT"

    # the status code for a user_invocation_required message
    """
    Represents an asynchronous tool invocation handle for a client.

    This class is used to manage the state and result of an asynchronous tool function invocation
    initiated by a client. It extends the BotOsOutputMessage class and provides additional
    properties and methods specific to tool function invocations.

    """

    STATUS_CODE = "user_invocation_required" # the status code for a user_invocation_required message

    def __init__(self,
                 thread_id:str,
                 tool_func_descriptor:ToolFuncDescriptor,
                 invocation_kwargs:dict,
                 input_metadata:Optional[dict] = None
                 ) -> None:
        assert self.STATUS_CODE in BotOsOutputMessage.ALLOWED_STATUS_VALUES
        super().__init__(thread_id=thread_id,
                         status=self.STATUS_CODE,
                         output="",
                         messages=None,
                         files=None,
                         input_metadata=input_metadata)
        self._invocation_id = str(uuid.uuid4())
        self._invocation_kwargs = invocation_kwargs
        self._tool_func_descriptor = tool_func_descriptor
        self._result_obj = None
        self._result_status = self.ResultStatusEnum.UNSET
        self._result_poll_count = 0


    @property
    def invocation_id(self):
        return self._invocation_id


    @property
    def invocation_kwargs(self):
        return self._invocation_kwargs


    @property
    def tool_func_descriptor(self):
        return self._tool_func_descriptor


    def submit_func_result(self, invocation_id, result_obj):
        assert invocation_id == self.invocation_id, f"Invocation id mismatch: got: {invocation_id}, expected: {self._invocation_id}"
        assert self._result_obj is None, f"Result already submitted for invocation {self._invocation_id}"
        self._result_status = self.ResultStatusEnum.PENDING_CONSUMPTION
        self._result_obj = result_obj


    def get_func_result(self, timeout:float=None) -> str | None:
        assert self._result_status not in (self.ResultStatusEnum.CONSUMED_SUCCESS, self.ResultStatusEnum.CONSUMED_TIMEOUT), (
            f"Result for invocation_id: {self._invocation_id} has already been consumed with status: {self._result_status}"
        )

        start_time = time.time()
        # poll the result status until we have
        while self._result_status != self.ResultStatusEnum.PENDING_CONSUMPTION:
            #logger.debug(f"get_func_result: polling (#{self._result_poll_count}) on result for invocation_id: {self._invocation_id}")
            self._result_poll_count += 1
            if timeout is not None and (time.time() - start_time) > timeout:
                err_msg = f"Timeout expired without receiving a result for invocation_id: {self._invocation_id} of function {self._tool_func_descriptor.name}. "
                self._result_status = self.ResultStatusEnum.CONSUMED_TIMEOUT
                logger.warning(err_msg)
                raise TimeoutError(err_msg)
            time.sleep(0.1)  # Polling interval

        self._result_status = self.ResultStatusEnum.CONSUMED_SUCCESS
        logger.debug(f"get_func_result: returning result for invocation_id: {self._invocation_id} after {self._result_poll_count} polls ({time.time() - start_time} seconds)")
        return self._result_obj


    def __repr__(self):
        tr = lambda s: truncate_string(str(s), 12)
        return (f"BosOsClientAsyncToolInvocationHandle(thread_id={tr(self.thread_id)}, "
                f"tool_func_descriptor={tr(self._tool_func_descriptor)}, "
                f"invocation_kwargs={tr(self._invocation_kwargs)}, "
                f"input_metadata={tr(self.input_metadata)}, "
                f"invocation_id={tr(self._invocation_id)}, "
                f"result_obj={tr(self._result_obj)})")


class BotOsInputAdapter:
    def __init__(self, bot_id: Optional[str] = None) -> None:
        self.bot_id = bot_id
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
