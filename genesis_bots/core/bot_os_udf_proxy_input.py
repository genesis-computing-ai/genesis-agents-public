from   genesis_bots.connectors               import get_global_db_connector
from   flask                    import make_response, request
import os
import uuid

import base64
from   collections              import deque
from   genesis_bots.core.bot_os_input        import (BosOsClientAsyncToolInvocationHandle,
                                        BotOsInputAdapter, BotOsInputMessage,
                                        BotOsOutputMessage)
from   genesis_bots.core.bot_os_utils        import truncate_string
import functools
import json
import types

from   genesis_bots.core.logging_config      import logger


class UDFInputEvent:
    """
    Represents an input event for the UDF Bot OS Adapter.

    Attributes:
        uuid (str): The unique identifier for the event.
        input_msg (str): The input message.
        thread_id (str): The thread ID associated with the event.
        bot_id (dict): The bot ID information.
        file (Optional[dict]): The file information associated with the event.
        event_type (str): The type of event, must match BotOsInputMessage.ALLOWED_MSG_TYPES.
    """
    def __init__(self,
                 uuid: str,
                 input_msg: str,
                 thread_id: str,
                 bot_id: dict,
                 file: dict = None,
                 event_type: str = "chat_input"):
        self.uuid = uuid
        self.input_msg = input_msg
        self.thread_id = thread_id
        self.bot_id = bot_id
        self.file = file
        event_type = event_type.lower()
        assert event_type in BotOsInputMessage.ALLOWED_MSG_TYPES, f"Unrecognized UDFInputEvent event_type value: {event_type}. Expected one of: {BotOsInputMessage.ALLOWED_MSG_TYPES}"
        self.event_type = event_type


class UDFBotOsInputAdapter(BotOsInputAdapter):
    """
    UDFBotOsInputAdapter is an implementation of the BotOsInputAdapter class for handling input/output messages routed through the Flask end points.
    (when Genesis Sever runs as a native app in Snowflake, teh endpoints are called with a UDF in Snowflake which calls the Flask server.)
    "input" means information flowing from the client to server.
    """

    # Workflow for chat messages:
    # * Submitting a Message:
    #   - When a user submits a message using the submit() method, a new UDFInputEvent is created with a unique UUID.
    #   - This event is added to the events queue and also stored in events_map for quick access.
    #   - The UUID is added to pending_map to indicate that the event is awaiting processing.
    #
    # * Processing an Event:
    #   - The get_input() method is called by a worker thread in the BotOsServer to retrieve the next event from the events queue for processing.
    #   - The event is converted into a BotOsInputMessage and sent to the bot (LLM) for handling.
    #
    # * Handling a Response:
    #   - When a response is received from the bot (LLM), a BosOsServer worker thread calls handle_response().
    #   - The response is stored in response_map using the UUID of the input event.
    #     The response is also written to the LLM_RESULTS table.
    #   - If the event's UUID is in pending_map, it is removed, indicating that the event has been processed.
    #   - The response can then be retrieved by the client (via endpoint) using the UUID using the lookup* methods
    #     or by reading from the LLM_RESULTS table (which is what the streamlit app does)
    #
    # Workflow for client tool invocations:
    # * BosOsServer invokes a tool function that is a proxy to the client tool function.
    #     * the proxy function adds an BosOsClientAsyncToolInvocationHandle to the user_actions_tacker.unprocessed_q.
    #     * the proxy function waits on the result of the invocation to be available in the handle
    #
    # * When the client polls for an available response msg:
    #      * we process any new pending actions first. For any pending action we send a special message to the client to
    #        request it to call that tool function, along with a unique invocation_id.
    #      * client receives the special message, calls the tool function submits back a special message that contains the
    #        result of the tool function, along with the invocation_id.
    #      * we locate the BosOsClientAsyncToolInvocationHandle associated with the invocation_id in the pending_result_q
    #        and attach the result to the handle.
    #        Note that we do not append a new InputEvent to the events queue since the client tool invocation is
    #        not associated  of the chat request-response cycle.
    #      * The proxy function can now unblock and return the result to the LLM.

    # Shared class-level maps for all instances
    _shared_in_to_out_thread_map = {}
    _shared_response_map = {}
    _shared_events = {}
    _shared_events_map = {}
    _shared_pending_map = {}
    _shared_user_actions = {}

    ACTION_MSG_DELIM = "<!!-ACTION_MSG-!!>"
    '''prefix/suffix delimeter for special 'action' messages that distinguish them from normal chat messages'''

    ACTION_MSG_TYPES = ("action_required", # we are requesting the client to run some action (e.g. a tool function)
                        "action_result")   # the client has run the action and is submitting the result


    @classmethod
    def format_action_msg(cls, action_type, **action_params):
        '''
        Builds a special 'action' message that distinguishes it from normal chat messages.
        '''
        assert action_type in cls.ACTION_MSG_TYPES, f"Unrecognized action_type value: {action_type}. Expected one of: {cls.ACTION_MSG_TYPES}"
        d = dict(action_type=action_type, **action_params)
        dj = json.dumps(d)
        return f"{cls.ACTION_MSG_DELIM}{dj}{cls.ACTION_MSG_DELIM}"


    @classmethod
    def parse_action_msg(cls, msg):
        '''
        Parses a special 'action' message that distinguishes it from normal chat messages.
        Will raise a ValueError if the message is not a valid action message.
        '''
        if not msg.startswith(cls.ACTION_MSG_DELIM) or not msg.endswith(cls.ACTION_MSG_DELIM):
            raise ValueError(f"Expected action message to start with {cls.ACTION_MSG_DELIM} and end with {cls.ACTION_MSG_DELIM}, got: {msg}")
        dj = msg[len(cls.ACTION_MSG_DELIM):-len(cls.ACTION_MSG_DELIM)]
        try:
            d = json.loads(dj)
        except json.JSONDecodeError as e:
            raise ValueError(f"Failed to parse JSON from action message: {e}")
        if 'action_type' not in d:
            raise ValueError(f"Expected action message to contain an action_type field, got: {d}")
        return d


    def __init__(self, bot_id = None):
        super().__init__(bot_id=bot_id)
        self.genbot_internal_project_and_schema = os.getenv('GENESIS_INTERNAL_DB_SCHEMA','None')

        # Generate a unique name for this adapter instance if needed
        if self.bot_id is None:
            self.adapter_name = str(uuid.uuid4())
        else:
            self.adapter_name = self.bot_id

        # Initialize shared maps for this adapter
        if self.adapter_name not in self.__class__._shared_in_to_out_thread_map:
            self.__class__._shared_in_to_out_thread_map[self.adapter_name] = {}
        if self.adapter_name not in self.__class__._shared_response_map:
            self.__class__._shared_response_map[self.adapter_name] = {}
        if self.adapter_name not in self.__class__._shared_events:
            self.__class__._shared_events[self.adapter_name] = deque()
        if self.adapter_name not in self.__class__._shared_events_map:
            self.__class__._shared_events_map[self.adapter_name] = {}
        if self.adapter_name not in self.__class__._shared_pending_map:
            self.__class__._shared_pending_map[self.adapter_name] = {}
        if self.adapter_name not in self.__class__._shared_user_actions:
            self.__class__._shared_user_actions[self.adapter_name] = types.SimpleNamespace()
            self.__class__._shared_user_actions[self.adapter_name].unprocessed_q = deque()
            self.__class__._shared_user_actions[self.adapter_name].pending_result_q = deque()

        # Set instance variables to reference the shared collections
        self.in_to_out_thread_map = self.__class__._shared_in_to_out_thread_map[self.adapter_name]
        self.response_map = self.__class__._shared_response_map[self.adapter_name]
        self.events = self.__class__._shared_events[self.adapter_name]
        self.events_map = self.__class__._shared_events_map[self.adapter_name]
        self.pending_map = self.__class__._shared_pending_map[self.adapter_name]
        self.user_actions_tacker = self.__class__._shared_user_actions[self.adapter_name]


    @functools.cached_property
    def db_connector(self):
        return get_global_db_connector()


    def add_event(self, event: UDFInputEvent):
        """Adds an event to the events queue."""
        self.events.append(event)
        if event and event.uuid:
            self.events_map[event.uuid] = event


    def add_back_event(self, metadata: dict = None):
        """Adds an event back to the queue based on metadata."""
        event = self.events_map.get(metadata['input_uuid'], None)
        if event is not None:
            self.events.append(event)


    def get_input(self, thread_map=None, active=None, processing=None, done_map=None) -> BotOsInputMessage:
        """
        Retrieves the next event from the queue (if not empty) and convert it to a BotOsInputMessage object.

        Returns None if the queue is empty.
        """
        if len(self.events) == 0:
            return None
        try:
            event = self.events.popleft()
        except IndexError:
            return None

        uu = event.uuid
        bot_id = event.bot_id
        metadata = {
            "input_uuid": uu,
            "thread_id": event.thread_id,
            "channel_type": "Streamlit",
            "channel_name": "",
            "is_bot": 'FALSE',
            "user_id": bot_id.get('user_id', 'unknown_id'),
            "user_name": bot_id.get('user_name', 'unknown_name'),
            "user_email": bot_id.get('user_email', 'unknown_email')
        }

        file = event.file
        if isinstance(file, str):
            file = json.loads(file)
        files = []
        if file:
            file_path = f"./runtime/downloaded_files/{event.thread_id}/{file['filename']}"
            os.makedirs(f"./runtime/downloaded_files/{event.thread_id}", exist_ok=True)
            with open(file_path, 'wb') as f:
                f.write(base64.b64decode(file['content']))
            files.append(file_path)

        return BotOsInputMessage(thread_id=event.thread_id,
                                 msg=event.input_msg,
                                 metadata=metadata,
                                 files=files)


    def handle_response(self,
                        session_id: str,  # unused
                        message: BotOsOutputMessage,
                        in_thread=None,  # unused
                        in_uuid=None,
                        task_meta=None  # unused
                        ):
        # Handle the response from the LLM to the client
        # Here you would implement how the Flask app should handle the response from the LLM.
        # For example, you might send the response back to the client via a WebSocket
        # or store it in a database for later retrieval.
        #logger.info("UDF output: ",message.output, ' in_uuid ', in_uuid)

        do_update_db = True # whether to update the hybrid table
        # in_uuid is required except for async client tool invocation which are currentlu not linked to the request-response cycle
        assert in_uuid is not None or message.status == BosOsClientAsyncToolInvocationHandle.STATUS_CODE

        if message.output == '!NO_RESPONSE_REQUIRED':
            self.response_map[in_uuid] = "(no response needed)"
        elif message.status == 'completed' and message.files:
            output = message.output
            for file in message.files:
                filename = os.path.basename(file)
                output += f'\n ![Link](sandbox:/mnt/data/{filename})'
            self.response_map[in_uuid] = output
        elif message.status == BosOsClientAsyncToolInvocationHandle.STATUS_CODE:
            assert isinstance(message, BosOsClientAsyncToolInvocationHandle), f"Expected a BosOsClientAsyncToolInvocationHandle, got: {type(message)}"
            # Note: the in_uuid of this request is unique to the action_required message and is unrealted to the
            #        original input event that caused the LLM to reques this action
            # mark this action as pending by adding it to the pending_actions_map (not self.response_map)
            self.user_actions_tacker.unprocessed_q.append(message)
            logger.info(f"UDFBotOsInputAdapter: New tool invocation requested with invocation_id: {message.invocation_id} for {message.tool_func_descriptor.name}({message.invocation_kwargs})")
            do_update_db = False # we don't log this to the DB since action_required is only expected to be
                                 # triggered by the GenesisAPI client, which does not use the DB
        else:
            self.response_map[in_uuid] = message.output

        # write or update the message corresponding to in_uuid in the hybrid table. Update pending map.
        if in_uuid in self.pending_map:
            do_update_db = do_update_db and not message.input_metadata["thread_id"].startswith('delegate_')
            if do_update_db:
                self.db_connector.db_insert_llm_results(in_uuid, message.output)
            del self.pending_map[in_uuid]
        else:
            if do_update_db:
                self.db_connector.db_update_llm_results(in_uuid, message.output)

    # Commented out - not used?
    #
    # def lookup_fn(self):
    #     '''
    #     Main handler for providing a web UI.
    #     '''
    #     if request.method == "POST":
    #         # getting input in HTML form
    #         input_text = request.form.get("input") # the UUID of the input event
    #         # display input and output
    #         #logger.info("lookup input: ", input_text )
    #         resp = "not found"
    #         #logger.info(response_map)
    #         if input_text in self.response_map.keys():
    #             resp = self.response_map[input_text]
    #         #logger.info("lookup resp: ", resp )
    #         return render_template("lookup_ui.html",
    #             uuid_input=input_text,
    #             response=resp)
    #     return render_template("lookup_ui.html")
    #
    #
    # def submit_fn(self):
    #     '''
    #     Main handler for providing a web UI.
    #     '''
    #     if request.method == "POST":
    #         # getting input in HTML form
    #         input_text = request.form.get("input")
    #         thread_text = request.form.get("thread_text")
    #         # display input and output
    #         return render_template("submit_ui.html",
    #             echo_input=input_text,
    #             thread_id=thread_text,
    #             echo_reponse=self.submit(input_text, thread_text),
    #             thread_output=thread_text)
    #     return render_template("submit_ui.html")


    def submit(self, input: str, thread_id: str, bot_id: dict | str, file: dict = None) -> str:
        """
        Submits a new event to the queue. Returns the UUID of the event.
        """
        if isinstance(bot_id, str):
            bot_id = json.loads(bot_id)

        # if this is a tool response then lookup the invocaiton id in the pending_user_actions_queue and attach the result on that object.
        # this should notify the waiting LLM-initiated invocation that the the result is ready.
        try:
            action_msg = self.parse_action_msg(input)
        except ValueError as e:
            pass # not an action message - regular chat response message
        else:
            if action_msg['action_type'] == "action_result":
                invocation_id = action_msg['invocation_id']
                result = action_msg['func_result']
                logger.info(f"UDFBotOsInputAdapter: Received action_result for invocation_id: {invocation_id}")
                invocation_handle = None
                # rotate through the pending_result_q until we find the matching invocation_id
                for _ in range(len(self.user_actions_tacker.pending_result_q)):
                    item = self.user_actions_tacker.pending_result_q.popleft()
                    if item.invocation_id == invocation_id:
                        invocation_handle = item
                        break
                    self.user_actions_tacker.pending_result_q.append(item)
                if invocation_handle is None:
                    logger.warning(f"No matching invocation handle found for user-submitted invocation_id: {invocation_id}. Message ignored.")
                    return None
                logger.info(f"UDFBotOsInputAdapter: Submitting action_result for invocation_id: {invocation_id} : {truncate_string(str(result), 20)}")
                invocation_handle.submit_func_result(invocation_id, result)
                return invocation_id
            else:
                raise ValueError(f"Unrecognized action message: {action_msg}")

        # If we got here, treat as a normal chat message
        file = file or {}
        uu = str(uuid.uuid4())
        event = UDFInputEvent(uuid=uu, input_msg=input, thread_id=thread_id, bot_id=bot_id, file=file)
        self.add_event(event)
        self.pending_map[uu] = True
        return uu


    def healthcheck_fn(self):
        return "I'm ready!"


    def submit_udf_fn(self):
        '''
        Main handler for input data from user to the bot sent through the "UDF Proxy" mechanism.

        This is called from our Flask end point (POST on /udf_proxy/submit_udf endpoint).

        Internally calls self.submit() to handle each row of input data in the request message.

        The response is a JSON message with a UUIDs assigned to each of the rows in the input message.
        Eeach input row is considered a separate input event.
        '''
        message = request.json
        #   logger.debug(f'Received request: {message}')

        if message is None or not message['data']:
            logger.info('Received empty message')
            return {}

        # input format:
        #   {"data": [
        #     [row_index, column_1_value, column_2_value, ...],
        #     ...
        #   ]}
        input_rows = message['data']
        #  logger.info(f'Received {len(input_rows)} rows')

        # output format:
        #   {"data": [
        #     [row_index, column_1_value, column_2_value, ...}],
        #     ...
        #   ]}

        output_rows = []
        for row in input_rows:
            if len(row) > 4:
                arg = json.loads(row[4]) if isinstance(row[4], str) else row[4]
            else:
                arg = None
            output_rows.append([row[0], self.submit(row[1], row[2], row[3], arg)])

        response = make_response({"data": output_rows})
        response.headers['Content-type'] = 'application/json'
        # logger.debug(f'Sending response: {response.json}')
        return response


    def test_udf_strings(self):
        test_submit = """
        curl -X POST http://127.0.0.1:8080/udf_proxy/jl-local-elsa-test-1/submit_udf \
            -H "Content-Type: application/json" \
            -d '{"data": [[1, "whats the secret word?", "111"]]}'
        """
        test_response_udf = """
        curl -X POST http://127.0.0.1:8080/udf_proxy/lookup_udf \
            -H "Content-Type: application/json" \
            -d '{"data": [[1, "4764a0e9-ee8f-4605-b3f4-e72897ba7347"]]}'
        """

    def _lookup_response(self, in_uuid:str):
        response = None
        # First, handle any pending action invocations.
        # A request for actions (e.g. client tool invocations) takes precedence since they are part of a pending request context.
        log_ctxt = self.__class__.__name__ + "::_lookup_response"

        # handle any new requested user actions (e.g. tool invocations) that are waiting in the self.pending_user_actions.unprocessed_q.
        # This takes precedence over chat reponses as long as there are pending actions.
        # We handle action requests one at a time and rely on the client to keep polling until all pending actions are processed.
        if self.user_actions_tacker.unprocessed_q:
            # construct a special output message for each pending action request and move the request to the the pending_result_q
            ihandle: BosOsClientAsyncToolInvocationHandle = self.user_actions_tacker.unprocessed_q.popleft() # type: ignore
            assert isinstance(ihandle, BosOsClientAsyncToolInvocationHandle)
            logger.info(f"{log_ctxt}: Queing pending action: {ihandle}")
            output = self.format_action_msg("action_required",
                                            invocation_id=ihandle.invocation_id,
                                            tool_func_name=ihandle.tool_func_descriptor.name,
                                            invocation_kwargs=ihandle.invocation_kwargs)
            self.user_actions_tacker.pending_result_q.append(ihandle)
            response = output
        elif in_uuid in self.response_map.keys():
            # otherwise, lookup the response corresponding to in_uuid in the response_map
            response =  self.response_map[in_uuid]
            logger.debug(f"{log_ctxt}: found response for request id {in_uuid}")
        else:
            logger.debug(f"{log_ctxt}: ! no response found for request id {in_uuid} !")

        return response


    def lookup_udf_fn(self):
        '''
        Main handler for retreiving output data from the the BotOsServer to the user.

        This is be called from our Flask end point to handle messages from the BotOsServer to the user when the user polls the BotOsServer via a Flask endpoint.
        Not that when the client (e.g. the streamlit app) runs as a native app by itself, its interface with the Flask servers is via a
        UDF in Snowflake which calles the Flask server running as an SPCS service - hence the "udf" in the name.

        '''
        # TODO: use lookup_udf() instead of replacing its logic

        message = request.json
        #logger.debug(f'Received request: {message}')

        if message is None or not message['data']:
            #logger.info('Received empty message')
            return {}

        # input format:
        #   {"data": [
        #     [row_index, column_1_value, column_2_value, ...],
        #     ...
        #   ]}

        input_rows = message['data']
        #logger.info(f'Received {len(input_rows)} rows')

        # output format:
        #   {"data": [
        #     [row_index, column_1_value, column_2_value, ...}],
        #     ...
        #   ]}
        request_uuid = input_rows[0][1]
        #logger.info("lookup input: ", input_text )
        resp = self._lookup_response(request_uuid) or "not found"
        output_rows = [[row[0], resp] for row in input_rows]

        response = make_response({"data": output_rows})
        response.headers['Content-type'] = 'application/json'
        logger.debug(f'Sending response: {response.json}')
        return response


    def lookup_udf(self, in_uuid:str):
        return self._lookup_response(in_uuid)
