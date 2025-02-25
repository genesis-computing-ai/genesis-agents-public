import asyncio
from collections import deque
import datetime
import json
import os
import time
import uuid
from typing_extensions import override
import google.generativeai as genai
from google.generativeai import caching
from genesis_bots.connectors import snowflake_tools
from genesis_bots.core.bot_os_assistant_base import BotOsAssistantInterface, execute_function
from genesis_bots.core.bot_os_input import BotOsInputMessage, BotOsOutputMessage
from genesis_bots.core.logging_config import logger

class BotOsAssistantGemini(BotOsAssistantInterface):
    def __init__(self, name:str, instructions:str,
                tools:list[dict] = [], available_functions={}, files=[],
                update_existing=False, log_db_connector=None, bot_id='default_bot_id', bot_name='default_bot_name',
                all_tools:list[dict]=[], all_functions={},all_function_to_tool_map={},skip_vectors=False) -> None:
        super().__init__(name, instructions, tools, available_functions, files, update_existing, skip_vectors=False)
        #self.active_runs = deque()
        self.llm_engine = 'gemini-1.5-pro-001' # 'gemini-1.5-pro' #'models/gemini-1.5-pro-001'
        self.instructions = instructions
        self.tools = tools
        self.available_functions = available_functions
        #self.done_map = {}
        self.thread_run_map = {}
        #self.active_runs = deque()
        #self.processing_runs = deque()

        #self.my_tools = tools
        self.log_db_connector = log_db_connector
        #self.callback_closures = {}
        #self.user_allow_cache = {}
        #self.clear_access_cache = False
        #self.run_meta_map = {}
        self.requests  = deque()
        #self.responses = deque()

        # genai.configure(api_key=os.environ['GEMINI_API_KEY'])
        genai.configure(api_key=os.getenv('GEMINI_API_KEY',''))

        if files:
            files_uploaded = self._upload_files(files.urls)

        tools = self._convert_tool_format(tools)
        tools = genai.protos.Tool(tools)

        if False:
            # Create a cache with a 5 minute TTL
            cache = caching.CachedContent.create(
                model=self.llm_engine,
                display_name=bot_id, # used to identify the cache
                system_instruction=instructions,
                contents=files_uploaded,
                ttl=datetime.timedelta(minutes=5),
                tools='code_execution',
            )
            # Construct a GenerativeModel which uses the created cache.
            self.model = genai.GenerativeModel.from_cached_content(cached_content=cache)
        self.model = genai.GenerativeModel(
            model_name=self.llm_engine,
            tools=tools, #+ 'code_execution',
            system_instruction=instructions,
        )

        logger.debug("BotOsAssistantGemini:__init__ - SnowflakeConnector initialized")

    def _convert_tool_format(self, tools:list[dict]):
        def replace_type_key(d):
            if isinstance(d, dict):
                return {('type_' if k == 'type' else k): (v.upper() if k == 'type' else replace_type_key(v)) for k, v in d.items() if k != 'default'}
            elif isinstance(d, list):
                return [replace_type_key(i) for i in d]
            else:
                return d

        return {
            'function_declarations': [replace_type_key(tool['function']) for tool in tools if 'function' in tool]
        }

    def _upload_files(self, files):
        files_to_upload = [genai.upload_file(path=f) for f in files]
        # Wait for all files to finish processing
        for file in files_to_upload:
            while file.state.name == 'PROCESSING':
                logger.info(f'Waiting for file {file.name} to be processed.')
                time.sleep(2)
                file = genai.get_file(file.name)
        return files_to_upload

    @override
    def is_active(self) -> deque:
       return self.requests

    @override
    def is_processing_runs(self) -> deque:
       return self.requests

    @override
    def get_done_map(self) -> dict:
       return {} #self.done_map

    def create_thread(self) -> str:
        thread_id = f"Gemini_thread_{uuid.uuid4()}"
        chat  = self.model.start_chat(enable_automatic_function_calling=True)
        self.thread_run_map[thread_id] = {"chat": chat}
        return thread_id

    def add_message(self, input_message:BotOsInputMessage):
        #async def handle_response():
        def handle_response():
            chat = self.thread_run_map[input_message.thread_id]["chat"]
            #response = await chat.send_message_async(input_message.msg)
            response = chat.send_message(input_message.msg)
            #if response:
            #    self.responses.append({"response": response, "input_message": input_message})  # Store the response in the messages deque
            #return {"response": response, "input_message": input_message}
            return {"response": response, "thread_id": input_message.thread_id, "timestamp": datetime.datetime.now().strftime("%Y%m%d%H%M%S%f"),
                    "metadata": input_message.metadata}

        # asyncio.run(handle_response)
        self.requests.append(handle_response)
        #asyncio.create_task(handle_response())

    def check_runs(self, event_callback):
        try:
            next_request  = self.requests.popleft()
            # try:
            #     loop = asyncio.get_running_loop()
            # except RuntimeError:
            #     loop = asyncio.new_event_loop()
            #     asyncio.set_event_loop(loop)
            # next_response = loop.run_until_complete(next_request())
            next_response = next_request()
        except IndexError:
        #    logger.info(f"BotOsAssistantGemini:check_runs - no active runs for: {self.bot_id}")
            return

        response = next_response["response"]
        thread_id= next_response["thread_id"]
        input_metadata = next_response["metadata"]

        for part in response.parts:
            if fn := part.function_call:
                args = {key: val for key, val in fn.args.items()}
                logger.info(f'{{"function_name":"{fn.name}","arguments":{json.dumps(args)}}}')
                timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S%f")
                self._process_tool_call(thread_id, timestamp, fn.name, args, input_metadata)
            else:
                event_callback(str(self.model), BotOsOutputMessage(thread_id=thread_id,
                                                                    status="completed",
                                                                    output=part.text,
                                                                    messages=None,
                                                                    input_metadata=input_metadata))

    def _process_tool_call(self, thread_id, timestamp, function_to_call, arguments, message_metadata):
        try:
            cb_closure = self._generate_callback_closure(thread_id, func_name=function_to_call, timestamp=timestamp, message_metadata=message_metadata)
            logger.warning(f"BotAssistantGemini:_process_tool_call - {thread_id} {function_to_call} {arguments}")
            execute_function(function_to_call, json.dumps(arguments), self.available_functions, cb_closure, thread_id, self.bot_id)
        except Exception as e:
            logger.error(f"Error processing tool call: {e}")
            cb_closure(f"Error processing tool call: {e}")

    def _submit_tool_outputs(self, thread_id, timestamp, func_name, results, message_metadata):
        #async def handle_response():
        def handle_response():
            logger.warning(f"BotAssistantGemini:_submit_tool_outputs - {thread_id} {func_name} {results}")

            chat = self.thread_run_map[thread_id]["chat"]
            #response = await chat.send_message_async(
            response = chat.send_message(
                genai.protos.Content(
                    parts=[genai.protos.Part(
                        function_response=genai.protos.FunctionResponse(
                            name=func_name,
                            response={'result': results}))]))
            #self.messages.append({"response": response, "thread_id": thread_id, "timestamp": timestamp, "metadata": message_metadata})
            return {"response": response, "thread_id": thread_id, "timestamp": timestamp, "metadata": message_metadata}
        #handle_response()
        self.requests.append(handle_response)
        #asyncio.run(handle_response())

    def _generate_callback_closure(self, thread_id, func_name, timestamp, message_metadata):
      def callback_closure(func_response):  # FixMe: need to break out as a generate closure so tool_call_id isn't copied
        #  try:
        #     del self.running_tools[tool_call_id]
        #  except Exception as e:
        #     logger.error(f"callback_closure - tool call already deleted - caught exception: {e}")
         try:
            self._submit_tool_outputs(thread_id, timestamp, func_name, func_response, message_metadata)
         except Exception as e:
            error_string = f"callback_closure - _submit_tool_outputs - caught exception: {e}"
            logger.error(error_string)
            return error_string
            #self._submit_tool_outputs(thread_id, timestamp, error_string, message_metadata)
      return callback_closure

    # def update_threads(self, thread_id, timestamp):
    #     pass

def gemini_test():
    import datetime
    import json

    from genesis_bots.connectors.database_tools import database_tool_functions

    # Mock event callback
    def event_callback(model, message:BotOsOutputMessage):
        logger.info(f"Event callback triggered with model: {model} and message: {message.output}")

    # Create an instance of the class (assuming the class name is BotOsGemini)
    bot = BotOsAssistantGemini(name="test_gemini",
                               instructions="you are a Genesis AI agent",
                               tools=database_tool_functions,
                               available_functions=snowflake_tools)

    thread_id = bot.create_thread()

    # Add a message and check runs instead
    bot.add_message(BotOsInputMessage(
        thread_id=thread_id,
        metadata={"user": "test_user"},
        msg="This is a test message"
    ))
    bot.check_runs(event_callback=event_callback)

    bot.add_message(BotOsInputMessage(
        thread_id=thread_id,
        metadata={"user": "test_user"},
        msg="run a sql query to get the current time"
    ))
    bot.check_runs(event_callback=event_callback)

#if __name__ == "__main__":
#    gemini_test()