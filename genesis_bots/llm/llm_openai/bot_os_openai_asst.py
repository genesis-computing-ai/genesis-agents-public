'''
  Implements OpenAI interface based on Assistants API
'''
import json
import os, uuid, re
from typing import TypedDict

from genesis_bots.demo.app import genesis_app
from genesis_bots.core.bot_os_assistant_base import BotOsAssistantInterface, execute_function
from collections import deque
import datetime
import time
import random
import types

import threading
from genesis_bots.core import global_flags
from genesis_bots.core.bot_os_input import BotOsInputMessage, BotOsOutputMessage
from genesis_bots.core.bot_os_defaults import _BOT_OS_BUILTIN_TOOLS, BASE_BOT_INSTRUCTIONS_ADDENDUM, BASE_BOT_DB_CONDUCT_INSTRUCTIONS,BASE_BOT_PROCESS_TOOLS_INSTRUCTIONS,BASE_BOT_SLACK_TOOLS_INSTRUCTIONS,BASE_BOT_OPENAI_INSTRUCTIONS
# For Streaming
from typing_extensions import override
from openai import AssistantEventHandler
from openai.types.beta.threads import Text, TextDelta
from openai.types.beta.threads.runs import ToolCall, ToolCallDelta
from openai.types.beta.threads import Message, MessageDelta
from openai.types.beta.threads.runs import ToolCall, RunStep
from openai.types.beta import AssistantStreamEvent
from collections import defaultdict
import traceback
from genesis_bots.bot_genesis.make_baby_bot import (  get_bot_details )
from genesis_bots.llm.llm_openai.openai_utils import get_openai_client

from genesis_bots.core.logging_config import logger
def _get_function_details(run):
      function_details = []
      if run.required_action and run.required_action.submit_tool_outputs:
         for tool_call in run.required_action.submit_tool_outputs.tool_calls:
            function_details.append(
               (tool_call.function.name, tool_call.function.arguments, tool_call.id)
            )
      else:
         logger.info("run.required_action.submit_tool_outputs is None")
        # raise AttributeError("'NoneType' object has no attribute 'submit_tool_outputs'")
      return function_details


class StreamingEventHandler(AssistantEventHandler):

   run_id_to_output_stream = {}
   run_id_to_messages = {}
   run_id_to_metadata = {}
   run_id_to_bot_assist = {}

   def __init__(self, client, thread_id, assistant_id, metadata, bot_assist):
       super().__init__()
       self.output = None
       self.tool_id = None
       self.thread_id = thread_id
       self.assistant_id = assistant_id
       self.run_id = None
       self.run_step = None
       self.function_name = ""
       self.arguments = ""
       self.client = client
       self.metadata = metadata
       self.bot_assist = bot_assist

   @override
   def on_text_created(self, text) -> None:
       pass

   @override
   def on_text_delta(self, delta, snapshot):
       # logger.info(f"\nassistant on_text_delta > {delta.value}")
#       logger.info(f"{delta.value}")
      if self.run_id not in StreamingEventHandler.run_id_to_output_stream:
          StreamingEventHandler.run_id_to_output_stream[self.run_id] = ""
      if delta is not None and isinstance(delta.value, str):
         StreamingEventHandler.run_id_to_output_stream[self.run_id] += delta.value


   @override
   def on_end(self, ):
       pass

   @override
   def on_exception(self, exception: Exception) -> None:
       """Fired whenever an exception happens during streaming"""
       pass

   @override
   def on_message_created(self, message: Message) -> None:
      self.run_id = message.run_id
      if self.run_id in StreamingEventHandler.run_id_to_messages:
         messages = StreamingEventHandler.run_id_to_messages[self.run_id]
         if messages and messages[-1]["type"] == "tool_call":
               if self.run_id in StreamingEventHandler.run_id_to_output_stream:
                  if not StreamingEventHandler.run_id_to_output_stream[self.run_id].endswith('\n'):
                     StreamingEventHandler.run_id_to_output_stream[self.run_id] += '\n'
    #   logger.info(f"\nassistant on_message_created > {message}\n")
   @override
   def on_message_done(self, message: Message) -> None:
      if self.run_id not in StreamingEventHandler.run_id_to_messages:
          StreamingEventHandler.run_id_to_messages[self.run_id] = []

   #   try:
   #       message_text = message.content[0].text.value if message.content else ""
   #   except:
   #       message_text = ""

      try:
          message_id = message.id if message.id else ""
      except:
          message_text = ""
      message_obj = {
          "type": "message",
       #   "text": message_text,
          "id": message_id
      }

      StreamingEventHandler.run_id_to_messages[self.run_id].append(message_obj)

      if self.run_id in StreamingEventHandler.run_id_to_output_stream:
         if not StreamingEventHandler.run_id_to_output_stream[self.run_id].endswith('\n'):
            StreamingEventHandler.run_id_to_output_stream[self.run_id] += ' '

      return


   @override
   def on_message_delta(self, delta: MessageDelta, snapshot: Message) -> None:
       # logger.info(f"\nassistant on_message_delta > {delta}")
       pass

   def on_tool_call_created(self, tool_call):
       # 4
       logger.info(f"\nassistant tool_call > {tool_call}")
       return

   @override
   def on_tool_call_done(self, tool_call: ToolCall) -> None:
       return

   @override
   def on_run_step_created(self, run_step: RunStep) -> None:
       # 2
       return

   @override
   def on_run_step_done(self, run_step: RunStep) -> None:
       return

   def on_tool_call_delta(self, delta, snapshot):
       return

   @override
   def on_event(self, event: AssistantStreamEvent) -> None:
       # logger.info("In on_event of event is ", event.event)
       #event.data.id
       try:
          if event.event == 'thread.run.created':
            self.run_id = event.data.id
            StreamingEventHandler.run_id_to_metadata[self.run_id] = self.metadata
            StreamingEventHandler.run_id_to_bot_assist[self.run_id] = self.bot_assist
            if 'parent_run' in self.metadata:
                parent_run_id = self.metadata['parent_run']
                if parent_run_id in StreamingEventHandler.run_id_to_output_stream:
                    StreamingEventHandler.run_id_to_output_stream[self.run_id] = StreamingEventHandler.run_id_to_output_stream[parent_run_id]
            self.bot_assist.thread_run_map[self.thread_id] = {"run": self.run_id, "completed_at": None}
            logger.info(f"----> run is {self.run_id}")
            if self.thread_id not in self.bot_assist.active_runs:
               self.bot_assist.active_runs.append(self.thread_id)
       except:
          pass
       return
       if event.event == "thread.run.requires_action":
           logger.info("\nthread.run.requires_action > submit tool call")
           logger.info(f"ARGS: {self.arguments}")


class BotOsAssistantOpenAIAsst(BotOsAssistantInterface):

   stream_mode = True
   all_functions_backup = None
   _shared_completion_threads = {}  # Maps bot names to their completion threads
   _shared_thread_working_set = {}  # Maps bot names to their thread working sets
   _thread_io_map = {}  # Maps input thread IDs to output thread IDs
   _shared_thread_run_map = {}  # Maps thread IDs to their run information
   _shared_active_runs = {}  # Maps bot names to their active runs deque
   _shared_processing_runs = {}  # Maps bot names to their processing runs deque
   _shared_tool_completion_status = {}  # Maps run IDs to tool completion status
   _shared_failed_retry_run_count_map = {}  # Maps thread IDs to retry counts
   _shared_run_id_to_output_stream = {}  # Maps run IDs to their output streams
   _shared_done_map = {}  # Maps bot names to their completed runs
   _shared_thread_stop_map = {}  # Maps thread IDs to stop timestamps
   _shared_stop_result_map = {}  # Maps thread IDs to stop results
   _shared_first_message_map = {}  # Maps thread IDs to first message flags
   _shared_thread_fast_mode_map = {}  # Maps thread IDs to fast mode settings
   _shared_tool_failure_map = {}  # Maps run hashes to failure counts and timestamps
   _shared_invalid_threads = set()  # Set of thread IDs that are known to be invalid
   _shared_thread_creation_map = {}  # Maps thread IDs to creation timestamps
   _shared_completions_runs = {}  # Maps bot names to their completion runs

   def __init__(self, name:str, instructions:str,
                tools:list[dict] = [], available_functions={}, files=[],
                update_existing=False, log_db_connector=None, bot_id='default_bot_id', bot_name='default_bot_name', all_tools:list[dict]=[], all_functions={},all_function_to_tool_map={}, skip_vectors=False, assistant_id = None) -> None:
      logger.debug("BotOsAssistantOpenAIAsst:__init__")
      super().__init__(name, instructions, tools, available_functions, files, update_existing, skip_vectors=False, bot_id=bot_id, bot_name=bot_name)

      model_name = os.getenv("OPENAI_MODEL_NAME", default="gpt-4o-2024-11-20")
      self.client = get_openai_client()

      name = bot_id
      logger.info(f"-> OpenAI Model == {model_name}")
      self.use_assistants = os.getenv("OPENAI_USE_ASSISTANTS", "False").lower() == "true"
      self.thread_run_map = {}
      self.active_runs = deque()
      self.processing_runs = deque()
      self.done_map = {}
      self.file_storage = {}
      self.available_functions = available_functions
      self.all_tools = all_tools
      self.all_functions = all_functions
      if BotOsAssistantOpenAIAsst.all_functions_backup == None and all_functions is not None:
         BotOsAssistantOpenAIAsst.all_functions_backup = all_functions
      self.all_function_to_tool_map = all_function_to_tool_map
      self.running_tools = {}
      self.tool_completion_status = {}
      self.log_db_connector = log_db_connector
      if self.use_assistants:
         if (files is None or files == []):
            my_tools = tools + [{"type": "code_interpreter"}]
         else:
            my_tools = tools + [{"type": "file_search"}]  + [{"type": "code_interpreter"}]
      else:
         my_tools = tools
      #my_tools = tools
      #logger.info(f'yoyo mytools {my_tools}')
      #logger.warn(f'yoyo mytools {my_tools}')
      self.my_tools = my_tools
      self.callback_closures = {}
      self.clear_access_cache = False
      self.first_tool_call = defaultdict(lambda: True)
      self.first_data_call = defaultdict(lambda: True)

      self.allowed_types_search = [".c", ".cs", ".cpp", ".doc", ".docx", ".html", ".java", ".json", ".md", ".pdf", ".php", ".pptx", ".py", ".rb", ".tex", ".txt", ".css", ".js", ".sh", ".ts"]
      self.allowed_types_code_i = [".c", ".cs", ".cpp", ".doc", ".docx", ".html", ".java", ".json", ".md", ".pdf", ".php", ".pptx", ".py", ".rb", ".tex", ".txt", ".css", ".js", ".sh", ".ts", ".csv", ".jpeg", ".jpg", ".gif", ".png", ".tar", ".xlsx", ".xml", ".zip"]
      self.run_meta_map = {}
      self.threads_in_recovery = deque()
      self.unposted_run_ids = {}
      self.thread_stop_map = {}
      self.stop_result_map = {}
      self.run_tools_message_map = {}
      self.thread_fast_mode_map = {}
      self.first_message_map = {}
      self.failed_retry_run_count_map = {}
      self.tool_failure_map = {}
     # self.last_stop_time_map = {}
      self.thread_working_set = {}
      self.completions_runs = {}
      self.instructions = instructions
      self.tools = my_tools
      self.completion_threads = {}

      genbot_internal_project_and_schema = os.getenv('GENESIS_INTERNAL_DB_SCHEMA','None')
      if genbot_internal_project_and_schema is not None:
         genbot_internal_project_and_schema = genbot_internal_project_and_schema.upper()
      self.genbot_internal_project_and_schema = genbot_internal_project_and_schema
      if genbot_internal_project_and_schema == 'None':
         logger.info("ENV Variable GENESIS_INTERNAL_DB_SCHEMA is not set.")

      self.db_schema = genbot_internal_project_and_schema.split('.')
      self.internal_db_name = self.db_schema[0]
      self.internal_schema_name = self.db_schema[1]

      my_assistant = None

      if self.use_assistants:
         # Try loading assistant ID from cache if not provided
         if assistant_id is None:
            try:
               map_file = f'./tmp/bot_maps/{self.bot_id}.map'
               if os.path.exists(map_file):
                     with open(map_file, 'r') as f:
                        assistant_id = f.read().strip()
                        logger.info(f'Loaded assistant ID {assistant_id} from cache for bot {name}')
            except Exception as e:
               logger.warning(f'Failed to load bot-assistant mapping from cache: {str(e)}')

         if assistant_id is not None:
            try:
               logger.info(f'loading assistant {assistant_id} for bot {name}...')
               my_assistant = self.client.beta.assistants.retrieve(assistant_id=assistant_id)
               self.assistant = my_assistant
               my_assistants = [my_assistant]
            except Exception as e:
               my_assistant = None

         if my_assistant is None:
            logger.info('finding assistant...')
            my_assistants = self.client.beta.assistants.list(order="desc", limit=100)
            my_assistants = [a for a in my_assistants if a.name == name]

         if True:
            if len(my_assistants) == 0 and update_existing:
               instructions += "\n" + BASE_BOT_OPENAI_INSTRUCTIONS
               vector_store_name = self.bot_id + '_vectorstore'
               self.vector_store = self.create_vector_store(vector_store_name=vector_store_name, files=files)
               if self.vector_store is not None:
                  self.tool_resources = {"file_search": {"vector_store_ids": [self.vector_store]}}
               else:
                  self.tool_resources = {}
               try:
                  self.assistant = self.client.beta.assistants.create(
                  name=name,
                  instructions=instructions,
                  tools=my_tools, # type: ignore
                  model=model_name,
                  # file_ids=self._upload_files(files) #FixMe: what if the file contents change?
                  tool_resources=self.tool_resources,
                  temperature=0.0
                  )
               except:
                  my_tools = [tool for tool in my_tools if tool.get('type') != 'file_search']
                  self.assistant = self.client.beta.assistants.create(
                     name=name,
                     instructions=instructions,
                     tools=my_tools, # type: ignore
                     model=model_name,
                     temperature=0.0
                  # file_ids=self._upload_files(files) #FixMe: what if the file contents change?
                  )

            elif len(my_assistants) > 0:
               self.assistant = my_assistants[0]
               logger.info('assistant found for bot ',name,': ',self.assistant.id,'.')
               # Save mapping between bot_id and assistant_id
               try:
                  os.makedirs('./tmp/bot_maps', exist_ok=True)
                  map_file = f'./tmp/bot_maps/{self.bot_id}.map'
                  with open(map_file, 'w') as f:
                     f.write(self.assistant.id)
               except Exception as e:
                  logger.warning(f'Failed to save bot-assistant mapping: {str(e)}')

         #  if os.getenv("API_MODE", "false").lower() == "true" and self.assistant is not None:

            if os.getenv("TASK_MODE", "false").lower() == "true":
               # dont do this for the TASK SERVER, just have it use the existing assistant being managed by the MultiBot Runner Process
               pass
            else:
               try:
                  vector_store_id = self.assistant.tool_resources.file_search.vector_store_ids[0]
               except:
                  vector_store_id = None
               if vector_store_id is not None and skip_vectors == False:
                  try:
                     self.client.beta.vector_stores.delete( vector_store_id=vector_store_id )
                  except:
                     pass
               vector_store_name = self.bot_id + '_vectorstore'
               if skip_vectors == False:
                  self.vector_store = self.create_vector_store(vector_store_name=vector_store_name, files=files)
                  if self.vector_store is not None:
                     self.tool_resources = {"file_search": {"vector_store_ids": [self.vector_store]}}
                  else:
                     self.tool_resources = {}
               else:
                  self.tool_resources = {"file_search": {"vector_store_ids": [vector_store_id]}}
               # Ensure my_tools has unique function names
               seen_names = set()
               duplicate_names = set()
               unique_tools = []

               for tool in my_tools:
                   if 'function' in tool:
                       name = tool['function']['name']
                       if name not in seen_names:
                           seen_names.add(name)
                           unique_tools.append(tool)
                       else:
                           duplicate_names.add(name)
                   else:
                       unique_tools.append(tool)

               if duplicate_names:
                   logger.info(f"Found duplicate tool names, combining them: {', '.join(duplicate_names)}")

               my_tools = unique_tools
             #  logger.debug(f"Number of unique tools after deduplication: {len(my_tools)}")


               if True or hasattr(files, 'urls') and files.urls is not None:
                  try:
                     self.client.beta.assistants.update(self.assistant.id,
                                                instructions=instructions,
                                                tools=my_tools, # type: ignore
                                                model=model_name,
                                                tool_resources=self.tool_resources
                     )
                  except Exception as e:
                     self.client.beta.assistants.update(self.assistant.id,
                                                instructions=instructions,
                                                tools=my_tools, # type: ignore
                                                model=model_name,
                                             #   tool_resources=self.tool_resources
                     )
               else:
                  my_tools = [tool for tool in my_tools if tool.get('type') != 'file_search']
                  self.client.beta.assistants.update(self.assistant.id,
                                                instructions=instructions,
                                                tools=my_tools, # type: ignore
                                                model=model_name,
                     )
               self.first_message = True

         logger.debug(f"BotOsAssistantOpenAIAsst:__init__: assistant.id={self.assistant.id}")
      else:
         self.assistant = types.SimpleNamespace()
         self.assistant.id = "no_assistant"


      if self.use_assistants == False:
         # Initialize shared thread working set for this bot name if needed
         if name not in self.__class__._shared_thread_working_set:
               self.__class__._shared_thread_working_set[name] = {}
         self.thread_working_set = self.__class__._shared_thread_working_set[name]

         # Initialize shared completion threads for this bot name if needed
         if name not in self.__class__._shared_completion_threads:
            self.__class__._shared_completion_threads[name] = {}
         self.completion_threads = self.__class__._shared_completion_threads[name]

         # Initialize other shared resources for this bot name
         if name not in self.__class__._shared_thread_run_map:
            self.__class__._shared_thread_run_map[name] = {}
         self.thread_run_map = self.__class__._shared_thread_run_map[name]

         if name not in self.__class__._shared_active_runs:
            self.__class__._shared_active_runs[name] = deque()
         self.active_runs = self.__class__._shared_active_runs[name]

         if name not in self.__class__._shared_processing_runs:
            self.__class__._shared_processing_runs[name] = deque()
         self.processing_runs = self.__class__._shared_processing_runs[name]

         if name not in self.__class__._shared_tool_completion_status:
            self.__class__._shared_tool_completion_status[name] = {}
         self.tool_completion_status = self.__class__._shared_tool_completion_status[name]

         if name not in self.__class__._shared_failed_retry_run_count_map:
            self.__class__._shared_failed_retry_run_count_map[name] = {}
         self.failed_retry_run_count_map = self.__class__._shared_failed_retry_run_count_map[name]

         # Initialize shared done_map for this bot name
         if name not in self.__class__._shared_done_map:
            self.__class__._shared_done_map[name] = {}
         self.done_map = self.__class__._shared_done_map[name]

         # Initialize the new shared variables
         if name not in self.__class__._shared_thread_stop_map:
            self.__class__._shared_thread_stop_map[name] = {}
         self.thread_stop_map = self.__class__._shared_thread_stop_map[name]

         if name not in self.__class__._shared_stop_result_map:
            self.__class__._shared_stop_result_map[name] = {}
         self.stop_result_map = self.__class__._shared_stop_result_map[name]

         if name not in self.__class__._shared_first_message_map:
            self.__class__._shared_first_message_map[name] = {}
         self.first_message_map = self.__class__._shared_first_message_map[name]

         if name not in self.__class__._shared_thread_fast_mode_map:
            self.__class__._shared_thread_fast_mode_map[name] = {}
         self.thread_fast_mode_map = self.__class__._shared_thread_fast_mode_map[name]

         if name not in self.__class__._shared_tool_failure_map:
            self.__class__._shared_tool_failure_map[name] = {}
         self.tool_failure_map = self.__class__._shared_tool_failure_map[name]

         # Note: run_id_to_output_stream is handled by StreamingEventHandler class

         # Initialize shared completions runs for this bot name
         if name not in self.__class__._shared_completions_runs:
            self.__class__._shared_completions_runs[name] = {}
         self.completions_runs = self.__class__._shared_completions_runs[name]
      else:
         self.active_runs = deque()
         self.processing_runs = deque()
         self.tool_completion_status = {}
         self.failed_retry_run_count_map = {}
         self.done_map = {}
         self.thread_stop_map = {}
         self.stop_result_map = {}
         self.first_message_map = {}
         self.thread_fast_mode_map = {}
         self.tool_failure_map = {}
         self.completions_runs = {}

   @override
   def is_active(self) -> bool:
      return self.active_runs

   @override
   def is_processing_runs(self) -> bool:
      return self.processing_runs

   @override
   def get_done_map(self) -> dict:
      return self.done_map

   #@staticmethod
   #def load_by_name(name: str):
   #   return BotOsAssistantOpenAIAsst(name, update_existing=False)

   def update_vector_store(self, vector_store_id: str, files: list=None, plain_files: list=None, for_bot = None):

      #internal_stage =  f"{self.internal_db_name}.{self.internal_schema_name}.BOT_FILES_STAGE"

      if for_bot == None:
         for_bot = self.bot_id
      file_path = "./uploads/"
      # Ready the files for upload to OpenAI
      if files is None and plain_files is None:
         return vector_store_id

      if files is not None:
         files = files.urls
      if plain_files is not None:
         files = plain_files

      try:
         files = files.urls
      except:
         files = files

      if files is None:
         files = []

      local_files = [file for file in files if file.startswith('serverlocal:')]
      stage_files = [file for file in files if not file.startswith('serverlocal:')]
      files_from_stage = []

      # Expand wildcard expressions in stage_files
      expanded_stage_files = []
      for file in stage_files:
          if '*' in file:
              # Assuming 'self' has an attribute 'snowflake_connector' which is an instance of the SnowflakeConnector class
              matching_files = self.log_db_connector.list_stage_contents(
                  database=self.internal_db_name,
                  schema=self.internal_schema_name,
                  stage='BOT_FILES_STAGE',
                  pattern=file
              )
              matching_files_names = [file_info['name'] for file_info in matching_files]
              matching_files_names = [file_info['name'].split('/', 1)[-1] for file_info in matching_files]
              expanded_stage_files.extend(matching_files_names)
          else:
              expanded_stage_files.append(file)
      stage_files = expanded_stage_files
      # Deduplicate stage_files
      stage_files = list(set(stage_files))

      valid_extensions = {
               '.c': 'text/x-c',
               '.cs': 'text/x-csharp',
               '.cpp': 'text/x-c++',
               '.doc': 'application/msword',
               '.docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
               '.html': 'text/html',
               '.java': 'text/x-java',
               '.json': 'application/json',
               '.md': 'text/markdown',
               '.pdf': 'application/pdf',
               '.php': 'text/x-php',
               '.pptx': 'application/vnd.openxmlformats-officedocument.presentationml.presentation',
               '.py': 'text/x-python',
               '.rb': 'text/x-ruby',
               '.tex': 'text/x-tex',
               '.txt': 'text/plain',
               '.css': 'text/css',
               '.js': 'text/javascript',
               '.sh': 'application/x-sh',
               '.ts': 'application/typescript',
         }

      # Filter out files from stage_files that don't have a valid extension
      excluded_files = [file for file in stage_files if not any(file.endswith(ext) for ext in valid_extensions)]
      stage_files = [file for file in stage_files if any(file.endswith(ext) for ext in valid_extensions)]

      if excluded_files:
          logger.info(f"{self.bot_name} for bot {for_bot} update_vector_store excluded files with invalid extensions: {', '.join(excluded_files)}")
      for file in stage_files:
          # Read each file from the stage and save it to a local location
          try:
              # Assuming 'self' has an attribute 'snowflake_connector' which is an instance of the SnowflakeConnector class
            new_file_location = f"./runtime/downloaded_files/{for_bot}_BOT_DOCS/{file}"
            os.makedirs(f"./runtime/downloaded_files/{for_bot}_BOT_DOCS", exist_ok=True)
            contents = self.log_db_connector.read_file_from_stage(
                  database=self.internal_db_name,
                  schema=self.internal_schema_name,
                  stage='BOT_FILES_STAGE',
                  file_name=file,
                  for_bot=f'{for_bot}_BOT_DOCS',
                  thread_id=f'{for_bot}_BOT_DOCS',
                  return_contents=False,
                  is_binary=False
                  )
            if contents==file:
               local_file_path = new_file_location
               files_from_stage.append(local_file_path)
               logger.info(f"{self.bot_name} for bot {for_bot} update_vector_store successfully retrieved {file} from stage and saved to {new_file_location}")
          except Exception as e:
               logger.info(f"{self.bot_name} for bot {for_bot} update_vector_store failed to retrieve {file} from stage: {e}")

      local_files = [file.replace('serverlocal:', '') for file in local_files]

      for file in local_files:
          if not os.path.isfile(file_path + file):
              logger.error(f"Vector indexer: Can't find file: {file_path+file}")

      file_streams = [open(file_path + file_id, "rb") for file_id in local_files]
      stage_streams = [open(file_id, "rb") for file_id in files_from_stage]

      # Use the upload and poll SDK helper to upload the files, add them to the vector store,
      # and poll the status of the file batch for completion.

      try:
         if len(file_streams) > 0:
            file_batch = self.client.beta.vector_stores.file_batches.upload_and_poll(
            vector_store_id=vector_store_id, files=file_streams )
            logger.info(f"File counts added to the vector store '{vector_store_id}': local: {file_batch.file_counts}")
         if len(stage_streams) > 0:
            stage_batch = self.client.beta.vector_stores.file_batches.upload_and_poll(
            vector_store_id=vector_store_id, files=stage_streams )
            logger.info(f"File counts added to the vector store '{vector_store_id}': local: {stage_batch.file_counts}")

      except Exception as e:
         logger.error(f"Failed to add files to the vector store '{vector_store_id}' for the bot with error: {e}")
         return vector_store_id

               # Close the file streams after uploading
      for file_stream in file_streams:
         file_stream.close()
            # Close the file streams after uploading
      for file_stream in stage_streams:
         file_stream.close()
      # Log the status and the file counts of the batch to see the result of this operation
         import time

         logger.info(f"Vector store '{vector_store_id}' creation status: {stage_batch.status}")
         return vector_store_id

      else:
         logger.info(f"No files provided to add to '{vector_store_id}'")
         return vector_store_id


   def create_vector_store(self, vector_store_name: str, files: list=None, plain_files: list=None, for_bot= None):
      # Create a vector store with the given name

      try:
         vector_store = self.client.beta.vector_stores.create(name=vector_store_name)
      except Exception as e:
         logger.error(f"Error creating vector store '{vector_store_name}': {e}")
         return None

      return self.update_vector_store(vector_store.id, files, plain_files, for_bot=for_bot)



   def create_thread(self) -> str:
        if self.use_assistants == False:
            thread_id = "completion_thread_" + str(uuid.uuid4())
            thread = thread_id
            logger.info(f"{self.bot_name} openai completion new_thread -> {thread_id}")
        else:
            thread = self.client.beta.threads.create()
            thread_id = thread.id
            logger.info(f"{self.bot_name} openai assistant new_thread -> {thread_id}")

        self.thread_working_set[thread_id] = thread
        self.first_message_map[thread_id] = True

        return thread_id

   def _upload_files(self, files, thread_id=None):
      file_ids = []
      file_map = []
      for f in files:


         original_file_location = f
         file_name = original_file_location.split('/')[-1]
         new_file_location = f"./runtime/downloaded_files/{thread_id}/{file_name}"

         # Convert .JPEG extension to .JPG if present
         if file_name.upper().endswith('.JPEG'):
             new_file_location = new_file_location[:-5] + '.jpg'
             file_name = file_name[:-5] + '.jpg'

         os.makedirs(f"./runtime/downloaded_files/{thread_id}", exist_ok=True)
         with open(original_file_location, 'rb') as source_file:
             with open(f"./runtime/downloaded_files/{thread_id}/{file_name}", 'wb') as dest_file:
                 dest_file.write(source_file.read())

         # Validate file type is allowed
         file_ext = os.path.splitext(new_file_location)[1].lower()
         allowed_types = self.allowed_types_search + self.allowed_types_code_i
         if not any(file_ext.endswith(ext) for ext in allowed_types):
             logger.warning(f"Skipping file {f} - extension {file_ext} not in allowed types")
             continue

    #     logger.info("loading files")
         fo = open(new_file_location,"rb")
         file = self.client.files.create(file=(file_name, fo), purpose="assistants")

         file_ids.append(file.id)

         # make a copy based on the new openai file.id as well in case the bot needs it by this reference later
         new_file_location_file_id = f"./runtime/downloaded_files/{thread_id}/{file.id}"
         with open(original_file_location, 'rb') as source_file:
             with open(new_file_location_file_id, 'wb') as dest_file:
                 dest_file.write(source_file.read())


         self.file_storage[file.id] = new_file_location
         file_map.append({'file_id': file.id, 'file_name': file_name})

      logger.debug(f"BotOsAssistantOpenAIAsst:_upload_files - uploaded {len(file_ids)} files")
      return file_ids, file_map





   def add_message(self, input_message:BotOsInputMessage, reuse_run_id=None, *, bot_os_thread=None, event_callback=None):
      thread_id = input_message.thread_id
      #logger.debug("BotOsA ssistantOpenAI:add_message")

      if input_message.metadata and 'thread_id' in input_message.metadata:
          if input_message.thread_id.startswith('delegate_'):
             pass
          # thread_id = input_message.thread_id


      stop_flag = False
      fast_mode = False
      attachments = []

      if input_message.msg.endswith('<<!!FAST_MODE!!>>') or thread_id in self.thread_fast_mode_map:
         # fast_mode = True
         # logger.info('openai fast mode = true')
          input_message.msg = input_message.msg.rstrip('<<!!FAST_MODE!!>>').rstrip()

      if thread_id in self.first_message_map:
         del self.first_message_map[thread_id]
         if input_message.metadata and 'thread_ts' in input_message.metadata:
       #     fast_mode = True
        #    self.thread_fast_mode_map[thread_id] = True
            logger.info('openai fast mode = false (set by default for a new slack-based thread)')
         if input_message.metadata and 'channel' in input_message.metadata:
            channel = input_message.metadata['channel']
          #  input_message.msg += f" [FYI Current Slack channel id is: {channel}]"


      if input_message.msg.endswith(') says: !model') or input_message.msg=='!model':
         if fast_mode:
            input_message.msg = input_message.msg.replace ('!model',f'SYSTEM MESSAGE: The User has requested to know what LLM model is running.  Respond by telling them that the system is running in fast mode and that the current model is: { os.getenv("OPENAI_FAST_MODEL_NAME", default="gpt-4o-mini")}')
         else:
            input_message.msg = input_message.msg.replace ('!model',f'SYSTEM MESSAGE: The User has requested to know what LLM model is running.  Respond by telling them that the system is running in smart mode and that current model is: { os.getenv("OPENAI_MODEL_NAME", default="gpt-4o-2024-11-20")}')

      if input_message.msg.endswith(') says: !fast on') or input_message.msg == '!fast on':
            self.thread_fast_mode_map[thread_id] = True
            fast_mode = True
            input_message.msg = input_message.msg.replace('!fast on', f"SYSTEM MESSAGE: Tell the user that Fast mode activated for this thread. Model is now {os.getenv('OPENAI_FAST_MODEL_NAME', 'gpt-4o-mini')}")
      elif input_message.msg.endswith(') says: !fast off') or input_message.msg == '!fast off':
            if thread_id in self.thread_fast_mode_map:
               del self.thread_fast_mode_map[thread_id]
            fast_mode = False
            input_message.msg = input_message.msg.replace('!fast off', f"SYSTEM MESSAGE:Tell the user that Fast mode deactivated for this thread. Model is now {os.getenv('OPENAI_MODEL_NAME', 'gpt-4o-2024-11-20')}")

      if input_message.msg.endswith(') says: !stop') or input_message.msg=='!stop':
            stopped = False
            try:
               thread_run = self.thread_run_map[thread_id]
               run = self.client.beta.threads.runs.retrieve(thread_id = thread_id, run_id = thread_run["run"])
               output = StreamingEventHandler.run_id_to_output_stream[run.id]+" 💬"
               output = output[:-2]
               output += ' `Stopped`'
               try:
                  self.client.beta.threads.runs.cancel(thread_id=thread_id, run_id=run.id)
                  logger.info(f"Cancelled run_id: {run.id} for thread_id: {thread_id}")
                  resp = "Streaming stopped for previous request"
                  stopped = True
               except Exception as e:
                     pass
            except:
               pass
            if not stopped:
               if thread_id in self.active_runs or thread_id in self.processing_runs:
                  future_timestamp = datetime.datetime.now() + datetime.timedelta(seconds=60)
                  self.thread_stop_map[thread_id] = future_timestamp
               #  self.last_stop_time_map[thread_id] = datetime.datetime.now()
                  # TODO FIND AND CANCEL RUNS DIRECTLY HERE ?

                  i = 0
                  for _ in range(15):
                     if self.stop_result_map.get(thread_id) == 'stopped':
                        break
                     time.sleep(1)
                     i = i + 1
                     logger.info("stop ",i)

                  if self.stop_result_map.get(thread_id) == 'stopped':
                     self.thread_stop_map.pop(thread_id, None)
                     self.stop_result_map.pop(thread_id, None)
                     resp = "Streaming stopped for previous request"
                  else:
                     self.thread_stop_map.pop(thread_id, None)
                     self.stop_result_map.pop(thread_id, None)
                     resp = "No streaming response found to stop"
               return True

   #   if thread_id in self.thread_stop_map:
   #         self.thread_stop_map.pop(thread_id)

      if thread_id in self.active_runs or thread_id in self.processing_runs:
         return False
#      print ("&#&$&$&$&$&$&$&$ TEMP: ",thread_id in self.active_runs or thread_id in self.processing_runs)
      if thread_id is None:
         raise(Exception("thread_id is None"))

      if self.use_assistants:
         thread = self.thread_working_set.get(thread_id)
         if thread is None:
            thread = self.client.beta.threads.retrieve(thread_id)
            self.thread_working_set[thread_id] = thread
      else:
         # get tread from local thread storage
         pass

      #logger.warn(f"ADDING MESSAGE -- input thread_id: {thread_id} -> openai thread: {thread}")
      if self.use_assistants and input_message.files is not None and len(input_message.files) > 0:
         try:
            #logger.error("REMINDER: Update for message new files line 117 on botosopenai.py")
            #logger.info('... openai add_message before upload_files, input_message.files = ', input_message.files)
            file_ids, file_map = self._upload_files(input_message.files, thread_id=thread_id)
            #logger.info('... openai add_message file_id, file_map: ', file_ids, file_map)
            attachments = []
            for file_id in file_ids:
               tools = []
               # Retrieve the file name from the file_map using the file_id
               file_name = next((item['file_name'] for item in file_map if item['file_id'] == file_id), None)
               # Only include the file_search tool if the file_name does not have a PNG extension
               if file_name and any(file_name.lower().endswith(ext) for ext in self.allowed_types_search):
                  tools.insert(0, {"type": "file_search"})
               if file_name and any(file_name.lower().endswith(ext) for ext in self.allowed_types_code_i):
                  tools.append({"type": "code_interpreter"})
               attachments.append({"file_id": file_id, "tools": tools})
            if input_message.metadata and input_message.metadata.get("response_authorized", 'TRUE') == 'FALSE':
                  input_message.msg = "THIS IS AN INFORMATIONAL MESSAGE ONLY ABOUT ACTIVITY IN THIS THREAD BETWEEN OTHER USERS.  RESPOND ONLY WITH '!NO_RESPONSE_REQUIRED'\nHere is the rest of the message so you know whats going on: \n\n"+ input_message.msg  + "\n REMINDER: RESPOND ONLY WITH '!NO_RESPONSE_REQUIRED'."
                  # don't add a run if there is no response needed do to an unauthorized user, but do make the bot aware of the thread message
            content = input_message.msg
            if file_map:
               content += "\n\nFile Name to Id Mappings:\n"
               for mapping in file_map:
                  content += f"- {mapping['file_name']}: {mapping['file_id']}\n"
         # logger.info('... openai add_message attachments: ', attachments)
            thread_message = self.client.beta.threads.messages.create(
               thread_id=thread_id, attachments=attachments, content=content,
               role="user",
            )
         except Exception as e:
            fixed = False
            try:
               if 'while a run' in e.body.get('message') and self.first_message == True:
                  run_id_match = re.search(r'run_([a-zA-Z0-9]+)', e.body.get('message'))
                  if run_id_match:
                     run_id = "run_" + run_id_match.group(1)
                     logger.info(f"Extracted run_id: {run_id}")
                     self.client.beta.threads.runs.cancel(run_id=run_id, thread_id=thread_id)
                     logger.info(f"Cancelled run_id: {run_id}")
                     thread_message = self.client.beta.threads.messages.create(
                        thread_id=thread_id, attachments=attachments, content=content,
                        role="user",
                     )
                  fixed = True
            except Exception as e:
               pass
      else:
            if self.use_assistants:
               thread_message = self.client.beta.threads.messages.create(
                  thread_id=thread_id, content=input_message.msg,
                  role="user")
            attachments=None
       # removed some stuff here 6/15/24
       #logger.debug(f"add_message - created {thread_message}")
      self.first_message = False
      task_meta = input_message.metadata.pop('task_meta', None)

      if fast_mode == True:
          input_message.metadata['fast_mode'] = 'TRUE'

      if self.use_assistants:
         if BotOsAssistantOpenAIAsst.stream_mode == True:
            try:
            #   logger.info('MINI override')
               if fast_mode:
                  with self.client.beta.threads.runs.stream(
                     thread_id=thread.id,
                     assistant_id=self.assistant.id,
                     event_handler=StreamingEventHandler(self.client, thread.id, self.assistant.id, input_message.metadata, self),
                     metadata=input_message.metadata,
                     model=os.getenv('OPENAI_FAST_MODEL_NAME', 'gpt-4o-mini')
                  ) as stream:
                     stream.until_done()
               else:
                  with self.client.beta.threads.runs.stream(
                     thread_id=thread.id,
                     assistant_id=self.assistant.id,
                     event_handler=StreamingEventHandler(self.client, thread.id, self.assistant.id, input_message.metadata, self),
                     metadata=input_message.metadata,
                  ) as stream:
                     stream.until_done()
            except Exception as e:
               try:
                  if e.status_code == 400 and 'already has an active run' in e.message:
                     logger.info('bot_os_openai add_message thread already has an active run, putting event back on queue...')
                     return False
               except:
                  pass
               logger.info('bot_os_openai add_message Error from OpenAI on run.streams: ',e)
               return False
      else:
         # handle non-stream mode?
         # get a stream handler first...

         try:
           # event_handler=StreamingEventHandler(self.client, thread.id, self.assistant.id, input_message.metadata, self)
            model_name = (
               os.getenv("OPENAI_FAST_MODEL_NAME", "gpt-4o-mini")
               if fast_mode
               else os.getenv("OPENAI_MODEL_NAME", "gpt-4o-2024-11-20")
            )
            if '!o1!' in input_message.msg or input_message.metadata.get('o1_override', False)==True:
               model_name = 'o1'
               input_message.msg = input_message.msg.replace('!o1!', '').strip()
               input_message.metadata['o1_override'] = True
            elif '!o3-mini!' in input_message.msg or input_message.metadata.get('o3_override', False)==True:
               model_name = 'o3-mini'
               input_message.msg = input_message.msg.replace('!o3!', '').strip() 
               input_message.metadata['o3_override'] = True
               input_message.metadata['reasoning_effort'] = 'low'
            elif '!o3-mini-low!' in input_message.msg or input_message.metadata.get('o3_mini_low_override', False)==True:
               model_name = 'o3-mini'
               input_message.msg = input_message.msg.replace('!o3-mini-low!', '').strip()
               input_message.metadata['o3_mini_low_override'] = True
               input_message.metadata['reasoning_effort'] = 'low'
            elif '!o3-mini-medium!' in input_message.msg or input_message.metadata.get('o3_mini_medium_override', False)==True:
               model_name = 'o3-mini'
               input_message.msg = input_message.msg.replace('!o3-mini-medium!', '').strip()
               input_message.metadata['o3_mini_medium_override'] = True
               input_message.metadata['reasoning_effort'] = 'medium'
            elif '!o3-mini-high!' in input_message.msg or input_message.metadata.get('o3_mini_high_override', False)==True:
               model_name = 'o3-mini'
               input_message.msg = input_message.msg.replace('!o3-mini-high!', '').strip()
               input_message.metadata['o3_mini_high_override'] = True
               input_message.metadata['reasoning_effort'] = 'high'
            # Create event for thread run created
            if reuse_run_id:
               run_id = reuse_run_id
            else:
               run_id = thread_id + "_" + str(datetime.datetime.now().timestamp())
            self.completions_runs[run_id] = types.SimpleNamespace(
                status="in_progress",
                last_error=None,
                required_action=None,
                id=run_id,
                metadata=input_message.metadata,
                created_at=datetime.datetime.now(),
                completed_at=None,
                tool_calls=None
            )
            StreamingEventHandler.run_id_to_metadata[run_id] = input_message.metadata
            StreamingEventHandler.run_id_to_bot_assist[run_id] = self
            if 'parent_run' in input_message.metadata:
               parent_run_id = input_message.metadata['parent_run']
               if not reuse_run_id and parent_run_id in StreamingEventHandler.run_id_to_output_stream:
                  StreamingEventHandler.run_id_to_output_stream[run_id] = StreamingEventHandler.run_id_to_output_stream[parent_run_id]
               if not reuse_run_id or run_id not in StreamingEventHandler.run_id_to_output_stream:
                  StreamingEventHandler.run_id_to_output_stream[run_id] = ""
            else:
               if not reuse_run_id or run_id not in StreamingEventHandler.run_id_to_output_stream:
                  StreamingEventHandler.run_id_to_output_stream[run_id] = ""
            self.thread_run_map[thread_id] = {"run": run_id, "completed_at": None}
          #  logger.info(f"----> {self.jl_id} completions-based run is {run_id} for thread_id: {thread_id}")
            if thread_id not in self.active_runs:
               self.active_runs.append(thread_id)

            if input_message.files is not None and len(input_message.files) > 0:
                # Add note about uploaded files to message
                files_info = "\n[User has attached the following files:\n"
                for file in input_message.files:
                    files_info += f"- {file}\n"
                files_info += "You can reference these files in future calls using their full paths as shown above.]"
                input_message.msg += files_info
                attachments = input_message.files

            # Check if we have existing messages for this thread
            if thread_id in self.completion_threads:
                # Get existing messages and append new user message
                openai_messages = self.completion_threads[thread_id]
                if input_message.msg != "Tool call completed, results":
                    openai_messages.append({
                        "role": "user",
                        "content": input_message.msg
                    })
            else:
                # Initialize new message thread
                openai_messages = [
                    {
                        "role": "user" if model_name in ['o1', 'o1-mini'] else "system",
                        "content": self.instructions
                    },
                    {
                        "role": "user",
                        "content": input_message.msg
                    }
                ]
                self.completion_threads[thread_id] = openai_messages

            # Update first message content to current instructions
            openai_messages[0]["content"] = self.instructions

            stream = self.client.chat.completions.create(
               model=model_name if model_name else 'gpt-4o',
               **({'tools': self.tools} if self.tools and len(self.tools) > 0 else {}),
               #tools=[{"type": "code_interpreter"}],
               messages=openai_messages,
               stream=True,
               stream_options={"include_usage": True},
               **({'reasoning_effort': input_message.metadata.get('reasoning_effort', 'low')} if model_name == 'o3-mini' else {})
            )

            # Collect streaming chunks f
            usage = None
            collected_chunks = []
            collected_messages = []
            tool_calls = []
            for chunk in stream:
               if chunk.usage != None and chunk.choices == []:
                  usage = chunk.usage
                  continue
               if len(chunk.choices) > 0 and hasattr(chunk.choices[0].delta, 'tool_calls') and chunk.choices[0].delta.tool_calls is not None:
                  tc_chunk_list = chunk.choices[0].delta.tool_calls
                  for tc_chunk in tc_chunk_list:
                     if len(tool_calls) <= tc_chunk.index:
                        tool_calls.append({"id": "", "type": "function", "function": {"name": "", "arguments": ""}})
                     tc = tool_calls[tc_chunk.index]
                     if tc_chunk.id is not None:
                           tc['id'] += tc_chunk.id
                     if tc_chunk.function is not None and tc_chunk.function.name is not None:
                           tc['function']['name'] += tc_chunk.function.name
                     if tc_chunk.function is not None and tc_chunk.function.arguments is not None:
                           tc['function']['arguments'] += tc_chunk.function.arguments
               if len(chunk.choices) > 0 and hasattr(chunk.choices[0].delta, 'content') and chunk.choices[0].delta.content:
                  delta_content = chunk.choices[0].delta.content
                  if delta_content is not None and isinstance(delta_content, str):
                     StreamingEventHandler.run_id_to_output_stream[run_id] += delta_content

            if tool_calls != []:
               self.completions_runs[run_id].tool_calls = tool_calls
               self.completions_runs[run_id].required_action = "tool_calls"
               self.completions_runs[run_id].status = "requires_action"
               # Add tool calls as assistant message to completion_threads
               if thread_id in self.completion_threads:
                  self.completion_threads[thread_id].append({
                        "role": "assistant",
                        "content": None,
                        "tool_calls": tool_calls
                  })
            else:
               self.completions_runs[run_id].tool_calls = None
               self.completions_runs[run_id].required_action = None
               self.completions_runs[run_id].status = "completed"
               self.completions_runs[run_id].completed_at = datetime.datetime.now()
               self.completions_runs[run_id].usage = usage
            # Add the assistant's response to the completions run object
               if run_id in StreamingEventHandler.run_id_to_output_stream:
                  self.completions_runs[run_id].response = StreamingEventHandler.run_id_to_output_stream[run_id]
                  # Add assistant's response as a chat message to completion_threads
                  if thread_id in self.completion_threads:
                     self.completion_threads[thread_id].append({
                           "role": "assistant",
                           "content": StreamingEventHandler.run_id_to_output_stream[run_id]
                     })

         except Exception as e:
               # Replace your old error handling
               logger.error(f"Error during OpenAI streaming call: {e}")
               # Add error info to input message metadata
               if hasattr(input_message, 'metadata'):
                  try:
                     input_message.metadata['openai_error_info'] = str(e)
                     if run_id in StreamingEventHandler.run_id_to_output_stream:
                        StreamingEventHandler.run_id_to_output_stream[run_id] = str(e)
                        # Set run to completed status when there's an error
                        self.completions_runs[run_id].status = "completed"
                        self.completions_runs[run_id].completed_at = datetime.datetime.now()
                        self.completions_runs[run_id].response = str(e)
                        self.completions_runs[run_id].tool_calls = None
                        self.completions_runs[run_id].required_action = None
                        return True
                  except:
                     pass
               return False


      primary_user = json.dumps({'user_id': input_message.metadata.get('user_id', 'unknown_id'),
                                 'user_name': input_message.metadata.get('user_name', 'unknown_name'),
                                 'user_email': input_message.metadata.get('user_email', 'unknown_email')})

      self.log_db_connector.insert_chat_history_row(datetime.datetime.now(), bot_id=self.bot_id, bot_name=self.bot_name, thread_id=thread_id,
                                                   message_type='User Prompt', message_payload=input_message.msg, message_metadata=input_message.metadata, files=attachments,
                                                   channel_type=input_message.metadata.get("channel_type", None), channel_name=input_message.metadata.get("channel", None),
                                                   primary_user=primary_user)
      return True

   def is_bot_openai(self,bot_id):
       if self.use_assistants == False:
          return False
       bot_details = get_bot_details(bot_id)
       bot_is_openai = False
       if bot_details.get("bot_implementation") == "openai":
           bot_is_openai = True
       elif bot_details.get("bot_implementation") is None:
           default_engine = os.getenv("BOT_OS_DEFAULT_LLM_ENGINE")
           if default_engine == "openai":
               bot_is_openai = True
           else:
               bot_is_openai = False
       else:
           bot_is_openai = False
       return bot_is_openai

   def reset_bot_if_not_openai(self,bot_id):

       if not self.is_bot_openai(bot_id):
           os.environ[f'RESET_BOT_SESSION_{bot_id}'] = 'True'
           return True
       else:
            return False


   def _submit_tool_outputs(self, run_id, thread_id, tool_call_id, function_call_details, func_response, metadata=None):
# ncl, obB, dvj
     # logger.debug(f"_submit_tool_outputs - {thread_id} {run_id} {tool_call_id} - {function_call_details} - {func_response}")
# 9ef, xjf
      new_response = func_response



# aj, od, 9p
#song.txt
#add this file to bot Janice
  #    if function_call_details[0][0] == '_lookup_slack_user_id' and isinstance(func_response, str):
  #          new_response = {"response": func_response}
  #          func_response = new_response
      if isinstance(func_response, dict) and len(func_response) == 1 and 'error' in func_response:
          new_response = {"success": False, "error": func_response['error']}
          func_response = new_response
          logger.info(f'openai submit_tool_outputs list with error converted to: {func_response}')

      if isinstance(func_response, str):
         try:
            new_response = {"success": False, "error": func_response}
            func_response = new_response
            logger.info(f'openai submit_tool_outputs string response converted call: {function_call_details}, response: {func_response}')
         except:
            logger.info(f'openai submit_tool_outputs string response converted call to JSON.')

      if isinstance(func_response, dict) and func_response.get('success') == False and 'error' in func_response:

         # Create object with run details kw,tg,add bot files. song.txt
         # run_details = {
         #    "run_id": run_id,
         #    "thread_id": thread_id,
         #    "function_name": function_call_details[0][0],
         #    "function_args": function_call_details[0][1],
         #    "function_response": func_response
         # }
         # Create a string hash of the run details
         run_details_str = f"{run_id}_{thread_id}_{function_call_details[0][0]}_{function_call_details[0][1]}_{func_response}"
         run_hash = str(hash(run_details_str))

         if run_hash in self.tool_failure_map:
            self.tool_failure_map[run_hash]["fail_count"] += 1
            if self.tool_failure_map[run_hash]["fail_count"] == 2:
                func_response['warning'] = "Note: Please do not retry this failed operation more than twice to avoid getting stuck in a retry loop. "
            if self.tool_failure_map[run_hash]["fail_count"] >= 3:
               func_response = {
                  "success": False,
                  "message": "The tool call has repeatedly failed, please inform the user that this tool call has failed and cannot be immediately retried."
               }
            if self.tool_failure_map[run_hash]["fail_count"] > 3:
               logger.warning(f"Tool call has failed {self.tool_failure_map[run_hash]['fail_count']} times, attempting to cancel run. Details: function={function_call_details[0][0]}, thread_id={thread_id}, run_id={run_id}")
               try:
                  self.client.beta.threads.runs.cancel(
                        thread_id=thread_id,
                        run_id=run_id
                  )
                  # Add thread back to active runs so check_runs can properly handle the failure
                  if thread_id not in self.active_runs:
                     if thread_id in self.processing_runs:
                         self.processing_runs.remove(thread_id)
                     self.active_runs.append(thread_id)
               except Exception as e:
                  logger.error(f"Failed to cancel run after repeated tool failures: {str(e)}")
               if run_id in StreamingEventHandler.run_id_to_output_stream:
                   StreamingEventHandler.run_id_to_output_stream[run_id] += "\n\nTool call has failed too many times, cancelling request."
               return
         else:
            self.tool_failure_map[run_hash] = {
               "first_fail_timestamp": datetime.datetime.now(),
               "fail_count": 1
            }
         # Randomly clean old failures from map (1% chance)

         if random.randint(0,100) >= 0:
             current_time = datetime.datetime.now()
             # Create list of keys to remove to avoid modifying dict during iteration
             keys_to_remove = []
             for run_hash, failure_data in self.tool_failure_map.items():
                 time_diff = current_time - failure_data["first_fail_timestamp"]
                 if time_diff.total_seconds() > 3600: # 60 minutes
                     keys_to_remove.append(run_hash)

             # Remove old entries
             for key in keys_to_remove:
                 del self.tool_failure_map[key]



      try:
         if function_call_details[0][0] == '_modify_slack_allow_list' and (func_response.get('success',False)==True or func_response.get('Success',False)==True):
            self.clear_access_cache = True

         if (function_call_details[0][0] == 'remove_tools_from_bot' or function_call_details[0][0] == 'add_new_tools_to_bot') and (func_response.get('success',False)==True or func_response.get('Success',False)==True):
            target_bot = json.loads(function_call_details[0][1]).get('bot_id',None)
            if target_bot is not None:

               if self.is_bot_openai(target_bot):
                  bot_tools = None
                  all_tools_for_bot = func_response.get('all_bot_tools', None)
                  if all_tools_for_bot is not None:
                     #logger.info(all_tools_for_bot)
                     #logger.info(self.all_function_to_tool_map)
                     bot_tools_array = []
                     for tool in all_tools_for_bot:
                     #  logger.warn(f'--> Calling validate_or_add_function on {tool} <---- ')
                        self.validate_or_add_function(tool)
                        tool_name = tool
                        if tool_name in self.all_function_to_tool_map:
                           for t in self.all_function_to_tool_map[tool_name]:
                              bot_tools_array.append(t)


                  # Check for cached assistant ID first
                  assistant_id_file = f"/tmp/assistant_id_{target_bot}.txt"
                  my_assistants = []

                  if os.path.exists(assistant_id_file):
                      with open(assistant_id_file, 'r') as f:
                          assistant_id = f.read().strip()
                          try:
                              assistant = self.client.beta.assistants.retrieve(assistant_id)
                              if assistant.name == target_bot:
                                  my_assistants = [assistant]
                          except:
                              pass

                  if not my_assistants:
                      my_assistants = self.client.beta.assistants.list(order="desc", limit=100)
                      my_assistants = [a for a in my_assistants if a.name == target_bot]

                      # Cache the assistant ID if found
                      if my_assistants:
                          with open(assistant_id_file, 'w') as f:
                              f.write(my_assistants[0].id)

                  for assistant in my_assistants:

                     new_instructions = assistant.instructions
                     if "snowflake_tools" in all_tools_for_bot and 'make_baby_bot' in all_tools_for_bot:
                           new_instructions += f"\nYour Internal Files Stage for bots is at snowflake stage: {self.genbot_internal_project_and_schema}.BOT_FILES_STAGE"
                           logger.info("Instruction for target bot updated with Internal Files Stage location.")
                     bot_tools_array = bot_tools_array + _BOT_OS_BUILTIN_TOOLS + [{"type": "code_interpreter"}, {"type": "file_search"}]

                     # TODO JD - Do we need this for database_tools?
                     if "snowflake_tools" in all_tools_for_bot:
                        workspace_schema_name = f"{global_flags.project_id}.{target_bot.replace(r'[^a-zA-Z0-9]', '_').replace('-', '_').replace('.', '_')}_WORKSPACE".upper()
                        new_instructions += f"\nYou have a workspace schema created specifically for you named {workspace_schema_name} that the user can also access. You may use this schema for creating tables, views, and stages that are required when generating answers to data analysis questions. Only use this schema if asked to create an object. Always return the full location of the object.\nYour default stage is {workspace_schema_name}.MY_STAGE."

                     self.client.beta.assistants.update(assistant.id,tools=bot_tools_array, instructions=new_instructions)

               else: # target bot is not openai assistant-backed
                  # this will start a new session with the updated tools and proper instructions

                   # this is no needed here anymore since we are doing it inside the function directly 
                  # self.reset_bot_if_not_openai(bot_id=target_bot)
                  pass

               logger.info(f"Bot tools for {target_bot} updated.")

         if function_call_details[0][0] == 'update_bot_instructions' and (func_response.get('success',False)==True or func_response.get('Success',False)==True):
            new_instructions = func_response.get("new_instructions",None)
            if new_instructions:

               target_bot = json.loads(function_call_details[0][1]).get('bot_id',None)
               bot_details = func_response.get('new_bot_details',None)
               if bot_details is not None:
                   func_response.pop("new_bot_details", None)

               if target_bot is not None:

                  instructions = new_instructions + "\n" + BASE_BOT_INSTRUCTIONS_ADDENDUM
                  instructions += f'\nNote current settings:\nData source: {global_flags.source}\nYour bot_id: {bot_details["bot_id"]}.\n'
                  if global_flags.runner_id is not None:
                     instructions += f'Runner_id: {global_flags.runner_id}\n'
                  if bot_details["slack_active"]=='Y':
                     instructions += "\nYour slack user_id: "+bot_details["bot_slack_user_id"]

                  # if "snowflake_tools" in bot_details["available_tools"] and 'make_baby_bot' in bot_details["available_tools"]:
                  #    instructions += f"\nYour Internal Files Stage for bots is at snowflake stage: {global_flags.genbot_internal_project_and_schema}.BOT_FILES_STAGE"

                  if "snowflake_tools" in bot_details["available_tools"]:

                     workspace_schema_name = f"{global_flags.project_id}.{target_bot.replace(r'[^a-zA-Z0-9]', '_').replace('-', '_').replace('.', '_')}_WORKSPACE".upper()
                     instructions += f"\nYou have a workspace schema created specifically for you named {workspace_schema_name} that the user can also access. You may use this schema for creating tables, views, and stages that are required when generating answers to data analysis questions. Only use this schema if asked to create an object. Always return the full location of the object."
                     instructions += "\n" + BASE_BOT_DB_CONDUCT_INSTRUCTIONS

                  #add process mgr tools instructions
                  if "process_manager_tools" in bot_details["available_tools"] or "notebook_manager_tools" in bot_details["available_tools"]:
                     instructions += "\n" + BASE_BOT_PROCESS_TOOLS_INSTRUCTIONS

                  if "slack_tools" in bot_details["available_tools"]:
                     instructions += "\n" + BASE_BOT_SLACK_TOOLS_INSTRUCTIONS

                  genesis_app.server.sessions

                  if not self.reset_bot_if_not_openai(bot_id=target_bot):

                     my_assistants = self.client.beta.assistants.list(order="desc",limit=100)
                     my_assistants = [a for a in my_assistants if a.name == target_bot]

                     for assistant in my_assistants:
                        self.client.beta.assistants.update(assistant.id,instructions=instructions)

                  logger.info(f"Bot instructions for {target_bot} updated, len={len(instructions)}")

                  #new_response.pop("new_instructions", None)
         if (function_call_details[0][0] == 'add_bot_files' or function_call_details[0][0] == 'remove_bot_files' ) and (func_response.get('success',False)==True or func_response.get('Success',False)==True):
         #  raise ('need to update bot_os_openai.py line 215 for new files structure with v2')
            try:
               updated_files_list = func_response.get("current_files_list",None)
            except:
               updated_files_list = None

            target_bot = json.loads(function_call_details[0][1]).get('bot_id',None)
            if target_bot is not None:
               my_assistants = self.client.beta.assistants.list(order="desc",limit=100)
               my_assistants = [a for a in my_assistants if a.name == target_bot]
               assistant_zero = my_assistants[0]

               try:
                  vector_store_id = assistant_zero.tool_resources.file_search.vector_store_ids[0]
               except:
                  vector_store_id = None
               if vector_store_id is not None:
                     try:
                        self.client.beta.vector_stores.delete( vector_store_id=vector_store_id )
                     except:
                        pass
                  #  self.update_vector_store(vector_store_id=vector_store_id, files=None, plain_files=updated_files_list)
                  #  tool_resources = assistant_zero.tool_resources
               bot_tools = assistant_zero.tools
               if updated_files_list:
             #     file_search_exists = any(tool['type'] == 'file_search' for tool in bot_tools)
   #               if not file_search_exists:
   #                  bot_tools.insert(0, {"type": "file_search"})
                  vector_store_name = json.loads(function_call_details[0][1]).get('bot_id',None) + '_vectorstore'
                  vector_store = self.create_vector_store(vector_store_name=vector_store_name, files=None, plain_files=updated_files_list, for_bot = target_bot)
                  if vector_store is not None:
                     tool_resources = {"file_search": {"vector_store_ids": [vector_store]}}
                  else:
                     tool_resources = {}
               else:
                #  bot_tools = [tool for tool in bot_tools if tool.get('type') != 'file_search']
                  tool_resources = {}
               self.client.beta.assistants.update(assistant_zero.id, tool_resources=tool_resources)

               logger.info(f"{self.bot_name} open_ai submit_tool_outputs Bot files for {target_bot} updated.")
      except Exception as e:
         logger.info(f'openai submit_tool_outputs error to tool checking, func_response: {func_response} e: {e}')

      if tool_call_id is not None: # in case this is a resubmit
         self.tool_completion_status[run_id][tool_call_id] = new_response

      # check if all parallel tool calls are complete

      if self.use_assistants == False:
         # Add tool response to completion_threads as user message

         # Check if all parallel tool calls for this run are complete
         if run_id not in self.tool_completion_status:
            self.tool_completion_status[run_id] = {}

         # Add the current tool call response to the completion status map
         self.tool_completion_status[run_id][tool_call_id] = func_response

         # Get all tool calls for this run
   #      all_tool_calls = self.completions_runs[run_id].tool_calls if run_id in self.completions_runs else []
   #      if not all_tool_calls:
   #         return

         all_tool_calls = self.completions_runs[run_id].tool_calls
         if not all_tool_calls:
            return

         # Get list of all tool call IDs that need to complete
         pending_tool_call_ids = [tc['id'] for tc in all_tool_calls]

         # Check if any tool calls are still pending completion
         for tc_id in pending_tool_call_ids:
            if tc_id not in self.tool_completion_status[run_id] or self.tool_completion_status[run_id][tc_id] is None:
               logger.info(f"Waiting for parallel tool call {tc_id} to complete")
               return


         if thread_id in self.completion_threads:
            tool_outputs = []
            for tc_id in self.tool_completion_status[run_id]:
               self.completion_threads[thread_id].append({
                  "role": "tool",
                  "tool_call_id": tc_id,
                  "content": str(self.tool_completion_status[run_id][tc_id])
               })
               tool_outputs.append({
                  'tool_call_id': tc_id,
                  'output': str(self.tool_completion_status[run_id][tc_id])
               })
         # Set run status to completed in completions_runs map
         if run_id in self.completions_runs:
         #   self.completions_runs[run_id].status = ""
         #   self.completions_runs[run_id].completed_at = datetime.datetime.now()
            self.completions_runs[run_id].tool_calls = []
            self.completions_runs[run_id].status = 'in_progress'
            self.completions_runs[run_id].required_action = None
           # Add run to active pending list
            if thread_id in self.processing_runs:
                self.processing_runs.remove(thread_id)
       #     if thread_id not in self.active_runs:
       #        self.active_runs.append(thread_id)
            self.add_message(BotOsInputMessage(thread_id=thread_id, msg='Tool call completed, results', metadata=metadata), reuse_run_id=run_id)   # self.completions_runs[run_id].usage = usage

            # tool_outputs and meta are needed for adding row to message_log table
          #  tool_outputs = [{'tool_call_id': key, 'output': str(self.tool_completion_status[run_id][key])} for key in pending_tool_call_ids]
            meta = metadata
            
      else: # self.use_assistants is True
         run = self.client.beta.threads.runs.retrieve(thread_id = thread_id, run_id = run_id)
         function_details = _get_function_details(run)

         if run.status == 'requires_action':
            parallel_tool_call_ids = [f[2] for f in function_details]

            #  check to see if any expected tool calls are missing from completion_status
            missing_tool_call_ids = [tool_call_id for tool_call_id in parallel_tool_call_ids if tool_call_id not in self.tool_completion_status[run.id]]
            if missing_tool_call_ids:
               logger.info('Error: a parallel tool call is missing form the completion status map.  Probably need to fail the run.')
               return

            if all(self.tool_completion_status[run.id][key] is not None for key in parallel_tool_call_ids):
               tool_outputs = [{'tool_call_id': key, 'output': str(self.tool_completion_status[run.id][key])} for key in parallel_tool_call_ids]
            else:
               logger.info(f"_submit_tool_outputs - {thread_id} {run_id} {tool_call_id}, not submitted, waiting for parallel tool calls")
               return
         else:
            logger.info('No tool response needed for this run, status is now ',run.status)
            return

    #  if any(value is None for value in self.tool_completion_status[run_id].values()):
    #     return

      # now package up the responses together

         tool_outputs = [{'tool_call_id': k, 'output': str(v)} for k, v in self.tool_completion_status[run_id].items()]

         # Limit the output of each tool to length 800000
         tool_outputs_limited = []
         for tool_output in tool_outputs:
            output_limited = tool_output['output'][:400000]
            if len(output_limited) == 400000:
               output_limited = output_limited + '\n!!WARNING!! LONG TOOL OUTPUT TRUNCATED.  CONSIDER CALLING WITH TOOL PARAMATERS THAT PRODUCE LESS RAW DATA.' # Truncate the output if it exceeds 400000 characters
            tool_outputs_limited.append({'tool_call_id': tool_output['tool_call_id'], 'output': output_limited})
         tool_outputs = tool_outputs_limited
         # Check if the total size of tool_outputs exceeds the limit
         total_size = sum(len(output['output']) for output in tool_outputs)
         if total_size > 510000:
            # If it does, alter all the tool_outputs to the error message
            tool_outputs = [{'tool_call_id': output['tool_call_id'], 'output': 'Error! Total size of tool outputs too large to return to OpenAI, consider using tool paramaters that produce less raw data.'} for output in tool_outputs]
         try:
            if BotOsAssistantOpenAIAsst.stream_mode == True:

               meta = StreamingEventHandler.run_id_to_metadata.get(run_id,None)
               logger.info(f'{self.bot_name} openai submit_tool_outputs submitting tool outputs len={len(tool_outputs)} ')
               run_id_to_update = run_id

               with self.client.beta.threads.runs.submit_tool_outputs_stream(
                      thread_id=thread_id,
                      run_id=run_id_to_update,
                      tool_outputs=tool_outputs,
                           #                model=model,
                      event_handler=StreamingEventHandler(self.client, thread_id,   StreamingEventHandler.run_id_to_bot_assist[run_id],  meta, self)
               ) as stream:
             #     logger.info('.. (not) sleeping 0.0 seconds before requeing run after submit_tool_outputs...')
                #  time.sleep(0.2)
                  if thread_id in self.processing_runs:
                     self.processing_runs.remove(thread_id)
                  if thread_id not in self.active_runs:
                     self.active_runs.append(thread_id)
                  stream.until_done()
            else:
               updated_run = self.client.beta.threads.runs.submit_tool_outputs(
                  thread_id=thread_id,
                  run_id=run_id,
                  tool_outputs=tool_outputs # type: ignore
               )
               logger.debug(f"_submit_tool_outputs - {updated_run}")
               meta = updated_run.metadata
               logger.info('...sleeping 0.1 seconds before requeing run after submit_tool_outputs...')
               time.sleep(0.1)
               if thread_id in self.processing_runs:
                  self.processing_runs.remove(thread_id)
               if thread_id not in self.active_runs:
                  self.active_runs.append(thread_id)
       #  if thread_id in self.processing_runs:
       #     self.processing_runs.remove(thread_id)

         except Exception as e:
            logger.error(f"submit_tool_outputs - caught exception: {e}")
            return

      primary_user = json.dumps({'user_id': meta.get('user_id', 'unknown_id'),
                     'user_name': meta.get('user_name', 'unknown_name'),
                     'user_email': meta.get('user_email', 'unknown_email')})
      for tool_output in tool_outputs:
        self.log_db_connector.insert_chat_history_row(datetime.datetime.now(), bot_id=self.bot_id, bot_name=self.bot_name, thread_id=thread_id,
                                                      message_type='Tool Output', message_payload=tool_output['output'],
                                                      message_metadata={'tool_call_id':tool_output['tool_call_id']},
                                                      channel_type=meta.get("channel_type", None), channel_name=meta.get("channel", None),
                                                      primary_user=primary_user)

   def _generate_callback_closure(self, run, thread, tool_call_id, function_details, metadata=None):
      def callback_closure(func_response):  # FixMe: need to break out as a generate closure so tool_call_id isn't copied
         try:
            del self.running_tools[tool_call_id]
         except Exception as e:
            error_string = f"callback_closure - tool call already deleted - caught exception: {e}"
            logger.info(error_string)
         try:
            self._submit_tool_outputs(run.id, thread.id, tool_call_id, function_details, func_response, metadata)
         except Exception as e:
            error_string = f"callback_closure - _submit_tool_outputs - caught exception: {e}"
            logger.info(error_string)
            logger.info(traceback.format_exc())
            try:
               self._submit_tool_outputs(run.id, thread.id, tool_call_id, function_details, error_string, metadata)
            except Exception as e:
               error_string = f"callback_closure - _submit_tool_outputs - caught exception: {e} submitting error_string {error_string}"
               logger.info(error_string)

      return callback_closure

   def _download_openai_file(self, file_id, thread_id):
      logger.debug(f"BotOsAssistantOpenAIAsst:download_openai_file - {file_id}")
      # Use the retrieve file contents API to get the file directly

      try:
        # logger.info(f"{self.bot_name} open_ai download_file file_id: {file_id}")

         try:
            file_id = file_id.get('file_id',None)
         except:
            try:
               file_id = file_id.file_id
            except:
               pass

         file_info = self.client.files.retrieve(file_id=file_id)
      #   logger.info(f"{self.bot_name} open_ai download_file id: {file_info.id} name: {file_info.filename}")
         file_contents = self.client.files.content(file_id=file_id)

     #    try:
     #       logger.info(f"{self.bot_name} open_ai download_file file_id: {file_id} contents_len: {len(file_contents.content)}")
     #    except Exception as e:
     #        logger.info(f"{self.bot_name} open_ai download_file file_id: {file_id} ERROR couldn't get file length: {e}")

         local_file_path = os.path.join(f"./runtime/downloaded_files/{thread_id}/", os.path.basename(file_info.filename))
      #   logger.info(f"{self.bot_name} open_ai download_file file_id: {file_id} localpath: {local_file_path}")

         # Ensure the directory exists
         os.makedirs(os.path.dirname(local_file_path), exist_ok=True)


         # Save the file contents locally
         file_contents.write_to_file(local_file_path)

  #       logger.info(f"{self.bot_name} open_ai download_file wrote file: {file_id} to localpath: {local_file_path}")

         # Save a copy of the file with the file_id as the file name
         try:
            file_id_based_path = f"./runtime/downloaded_files/{thread_id}/{file_id}"
            file_contents.write_to_file(file_id_based_path)
         except Exception as e:
            logger.info(f"{self.bot_name} open_ai download_file - error - couldnt write to {file_id_based_path} err: {e}")
            pass
      except Exception as e:
         logger.info(f"{self.bot_name} open_ai download_file ERROR: {e}")


      logger.debug(f"File downloaded: {local_file_path}")
      return local_file_path

   def _store_files_locally(self, file_ids, thread_id):
    #  logger.info(f"{self.bot_name} open_ai store_files_locally, file_ids: {file_ids}")
      return [self._download_openai_file(file_id, thread_id) for file_id in file_ids]

   def validate_or_add_function(self, function_name):
      """
      Validates if the given function_name is in self.all_functions. If not, it adds the function.

      Args:
          function_name (str): The name of the function to validate or add.

      Returns:
          bool: True if the function is valid or successfully added, False otherwise.
      """
      if function_name in self.all_functions:
         #logger.info(f"Function '{function_name}' is already in all_functions.")
         return True
      else:

         # make sure this works when adding tools
         logger.info(f'validate_or_add_function, fn name={function_name}')
         try:
            available_functions_load = {}
         #   logger.warn(f"validate_or_add_function - function_name: {function_name}")
            fn_name = function_name.split('.')[-1] if '.' in function_name else function_name
            #logger.warn(f"validate_or_add_function - fn_name: {fn_name}")
            module_path = "generated_modules."+fn_name
            #logger.warn(f"validate_or_add_function - module_path: {module_path}")
            desc_func = "TOOL_FUNCTION_DESCRIPTION_"+fn_name.upper()
            #logger.warn(f"validate_or_add_function - desc_func: {desc_func}")
            functs_func = fn_name.lower()+'_action_function_mapping'
            #logger.warn(f"validate_or_add_function - functs_func: {functs_func}")
            try:
               module = __import__(module_path, fromlist=[desc_func, functs_func])
            except:
            #   logger.warn(f"validate_or_add_function - module {module_path} does not need to be imported, proceeding...")
               return True
           # logger.warn(f"validate_or_add_function - module: {module}")
            # here's how to get the function for generated things even new ones...
            func = [getattr(module, desc_func)]
    #        logger.warn(f"validate_or_add_function - func: {func}")
            self.all_tools.extend(func)
            self.all_function_to_tool_map[fn_name]=func
           # logger.warn(f"validate_or_add_function - all_function_to_tool_map[{fn_name}]: {func}")
            #self.function_to_tool_map[function_name]=func
            func_af = getattr(module, functs_func)
         #   logger.warn(f"validate_or_add_function - func_af: {func_af}")
            available_functions_load.update(func_af)
        #    logger.warn(f"validate_or_add_function - available_functions_load: {available_functions_load}")

            for name, full_func_name in available_functions_load.items():
            #   logger.warn(f"validate_or_add_function - Looping through available_functions_load - name: {name}, full_func_name: {full_func_name}")
               module2 = __import__(module_path, fromlist=[fn_name])
             #  logger.warn(f"validate_or_add_function - module2: {module2}")
               func = getattr(module2, fn_name)
            #   logger.warn(f"validate_or_add_function - Imported function: {func}")
               self.all_functions[name] = func
            #   logger.warn(f"validate_or_add_function - all_functions[{name}]: {func}")
         except:
            logger.warning(f"Function '{function_name}' is not in all_functions. Please add it before proceeding.")

         logger.info(f"Likely newly generated function '{function_name}' added all_functions.")
         return False


   def check_runs(self, event_callback):
      logger.debug("BotOsAssistantOpenAIAsst:check_runs")

      threads_completed = {}
      threads_still_pending = []
#      for thread_id in self.thread_run_map:

      try:
         thread_id = self.active_runs.popleft()
         if thread_id is None:
            return
       #  logger.info(f"0-0-0-0-0-0-0->>>> thread_id: {thread_id}, in self.processing_runs: {thread_id in self.processing_runs}")
         if thread_id in self.processing_runs:
        #    logger.info('.... outta here ...')
            return
         if thread_id not in self.processing_runs:
            self.processing_runs.append(thread_id)

      except IndexError:
         thread_id = None
         return

      for _ in range(1):
         thread_run = self.thread_run_map[thread_id]
         restarting_flag = False
         failed_but_restarting_flag = False
         if thread_run["completed_at"] is None:

            try:
               if self.use_assistants:
                  run = self.client.beta.threads.runs.retrieve(thread_id = thread_id, run_id = thread_run["run"])
               else:
                  # Create a SimpleNamespace object instead of dict to allow attribute access
                  run = self.completions_runs[thread_run["run"]]
            # Handle non-assistant runs

            except:
               retry_count = 0
               max_retries = 3
               while retry_count < max_retries:
                   try:
                       run = self.client.beta.threads.runs.retrieve(thread_id=thread_id, run_id=thread_run["run"])
                       break  # If successful, exit the retry loop
                   except Exception as e:
                       logger.info(f"Error retrieving run (attempt {retry_count + 1}): {e}")
                       retry_count += 1
                       if retry_count < max_retries:
                           time.sleep(5)  # Wait for 5 seconds before retrying
                       else:
                           logger.info(f"Failed to retrieve run after {max_retries} attempts")
                           try:
                              self.thread_run_map[thread_id]["completed_at"] = 'Thread Error'
                           except:
                              pass
                           raise  # Re-raise the last exception if all retries fail


           # logger.info(run.status)

            if run.status == "failed" or run.status == "cancelled":
               if run.status == "cancelled":
                  logger.info(f"!!!!!!!!!! {run.status} JOB, TOO MANY REPEATED FAILED TOOL CALLS  !!!!!!!")
               else:
                  logger.info(f"!!!!!!!!!! {run.status} JOB, run.lasterror {run.last_error} !!!!!!!")
               # resubmit tool output if throttled
               #tools_to_rerun = {k: v for k, v in self.tool_completion_status[run.id].items() if v is not None}
               #self._run_tools(thread_id, run, tools_to_rerun) # type: ignore
               #self._submit_tool_outputs(run.id, thread_id, tool_call_id=None, function_call_details=self.tool_completion_status[run.id],
               #                          func_response=None)
               # Todo add more handling here to tell the user the thread failed

               if thread_id in self.failed_retry_run_count_map:
                   self.failed_retry_run_count_map[thread_id] += 1
               else:
                   self.failed_retry_run_count_map[thread_id] = 1

               if run.status == "failed"  and self.failed_retry_run_count_map[thread_id] <=2 :
                  output = StreamingEventHandler.run_id_to_output_stream.get(run.id,'') + f"\n\n!! Error from OpenAI, run.lasterror {run.last_error} on run {run.id} for thread {thread_id}, attempting retry #{self.failed_retry_run_count_map[thread_id]} of 2\n 💬"
                  restarting_flag = True
                  failed_but_restarting_flag = True
               else:
                  if run.status != "cancelled":
                     output = StreamingEventHandler.run_id_to_output_stream.get(run.id,'') + f"\n\n!! Error from OpenAI, run.lasterror {run.last_error} on run {run.id} for thread {thread_id}, 2 retrys have failed, stopping attempts to retry."
                  else:
                     output = StreamingEventHandler.run_id_to_output_stream.get(run.id,'') + f"\n\n!! Too many repeated identical tool calls from LLM on run {run.id} for thread {thread_id}. Stopping run."
                  restarting_flag = False
                  # Clear the failed retry count map for this run
                  if thread_id in self.failed_retry_run_count_map:
                     del self.failed_retry_run_count_map[thread_id]

               event_callback(self.assistant.id, BotOsOutputMessage(thread_id=thread_id,
                                                                     status=run.status,
                                                                     output=output,
                                                                     messages=None,
                                                                     input_metadata=run.metadata))
               self.log_db_connector.insert_chat_history_row(datetime.datetime.now(), bot_id=self.bot_id, bot_name=self.bot_name, thread_id=thread_id,
                                                              message_type='Assistant Response', message_payload=output, message_metadata=None,
                                                              tokens_in=0, tokens_out=0)
               if not restarting_flag:
                  threads_completed[thread_id] = run.completed_at
                  continue

            if thread_id in self.failed_retry_run_count_map and not restarting_flag:
               del self.failed_retry_run_count_map[thread_id]

            if (run.status == "in_progress" or run.status == 'requires_action') and BotOsAssistantOpenAIAsst.stream_mode == True and run.id in StreamingEventHandler.run_id_to_output_stream:
                #logger.info(StreamingEventHandler.run_id_to_output_stream[run.id])

               output = StreamingEventHandler.run_id_to_output_stream[run.id]+" 💬"
               if thread_id in self.thread_stop_map:
                  stop_timestamp = self.thread_stop_map[thread_id]
                  if isinstance(stop_timestamp, datetime.datetime) and (time.time() - stop_timestamp.timestamp()) <= 0:
                     self.stop_result_map[thread_id] = 'stopped'
                     self.thread_stop_map[thread_id] = time.time()
                     output = output[:-2]
                     output += ' `Stopped`'
                     try:
                        self.client.beta.threads.runs.cancel(thread_id=thread_id, run_id=run.id)
                     except:
                        # thread already completed
                        pass
                     logger.info(f"Cancelled run_id: {run.id} for thread_id: {thread_id}")
               if output != " 💬":
                  event_callback(self.assistant.id, BotOsOutputMessage(thread_id=thread_id,
                                                      status=run.status,
                                                      output=output,
                                                      messages=None,
                                                      input_metadata=run.metadata))
           #    continue

            #logger.info(f"run.status {run.status} Thread: {thread_id}")
            logger.info(f"{self.bot_name} open_ai check_runs {run.status} thread: {thread_id} runid: {run.id}")

            current_time = datetime.datetime.now()
            if self.use_assistants:
               run_duration = (current_time - datetime.datetime.fromtimestamp(run.created_at)).total_seconds()
            else:
               run_duration = (current_time - run.created_at).total_seconds()
            if run.status == "in_progress":
               threads_still_pending.append(thread_id)
               try:
                  # Corrected to ensure it only calls after each minute beyond the first 60 seconds
                  if run_duration > 60 and run_duration % 60 < 2 and run.id not in StreamingEventHandler.run_id_to_output_stream:  # Check if run duration is beyond 60 seconds and within the first 5 seconds of each subsequent minute # Check if run duration is beyond 60 seconds and within the first 5 seconds of each subsequent minute
                     event_callback(self.assistant.id, BotOsOutputMessage(thread_id=thread_id,
                                                                           status=run.status,
                                                                           output=f"_still running..._ {run.id} has been waiting on OpenAI for {int(run_duration // 60)} minute(s)...",
                                                                           messages=None,
                                                                           input_metadata=run.metadata))
               except Exception as e:
                  logger.info("requires action exception: ",e)
                  pass

            if run.status == "queued":
               threads_still_pending.append(thread_id)
               try:
                  if run_duration > 60 and run_duration % 60 < 2 and run.id not in StreamingEventHandler.run_id_to_output_stream:  # Check if run duration is beyond 60 seconds and within the first 5 seconds of each subsequent minute # Check if run duration is beyond 60 seconds and within the first 5 seconds of each subsequent minute
                     event_callback(self.assistant.id, BotOsOutputMessage(thread_id=thread_id,
                                                                           status=run.status,
                                                                           output=f"_still running..._ {run.id} has been queued by OpenAI for {int(run_duration // 60)} minute(s)...",
                                                                           messages=None,
                                                                           input_metadata=run.metadata))
               except:
                  pass
               continue

            if run.status == "expired":
               logger.info(f"!!!!!!!!!! EXPIRED JOB, run.lasterror {run.last_error} !!!!!!!")
               output = StreamingEventHandler.run_id_to_output_stream.get(run.id,'') + "\n\n!!! OpenAI run expired !!!"
               event_callback(self.assistant.id, BotOsOutputMessage(thread_id=thread_id,
                                                                     status=run.status,
                                                                     output=output,
                                                                     messages=None,
                                                                     input_metadata=run.metadata))
               #del threads_completed[thread_id]
               # Todo add more handling here to tell the user the thread failed
               continue

            if run.status == "requires_action":
               # FUNCTION CALL request
               # ----------------------
               # LLM requesting us to call a function and send back the result so it can run the next Step in the current Run.

               if self.use_assistants:
                  try:
                     function_details = _get_function_details(run)
                     parallel_tool_call_ids = [f[2] for f in function_details]
                  except Exception as e:
                     logger.info('!! no function details')
                     continue
               else:
                  function_details = []
                  for tool_call in run.tool_calls:
                     function_details.append((tool_call['function']['name'], tool_call['function']['arguments'], tool_call['id']))
                  parallel_tool_call_ids = [f[2] for f in function_details]

           #    if self.tool_completion_status.get(run.id,None) is not None:
           #       function_details = [f for f in function_details if f[2] not in self.tool_completion_status[run.id]]
               try:
                  if not all(key in self.tool_completion_status[run.id] for key in parallel_tool_call_ids):
                     self.tool_completion_status[run.id] = {key: None for key in parallel_tool_call_ids}
               except:
                  self.tool_completion_status[run.id] = {key: None for key in parallel_tool_call_ids} # need to submit completed parallel calls together

               if all(self.tool_completion_status[run.id][key] is not None for key in parallel_tool_call_ids):
                  if run.id not in self.threads_in_recovery:
                     self.threads_in_recovery.append(run.id)
                     logger.info(f"*** Run {run.id} is now in recovery mode. *** ")
                     time.sleep(3)
                     try:
                        run = self.client.beta.threads.runs.retrieve(thread_id = thread_id, run_id = thread_run["run"])
                        function_details = _get_function_details(run)
                        if run.status == 'requires_action':
                           parallel_tool_call_ids = [f[2] for f in function_details]
                           if all(self.tool_completion_status[run.id][key] is not None for key in parallel_tool_call_ids):
                              logger.info('All tool call results are ready for this run, and its still pending after a 3 second delay')
                              logger.info("############################################################")
                              logger.info("############################################################")
                              logger.info("##                                                        ##")
                              logger.info("##              Resubmitting tool outputs !!              ##")
                              logger.info("##                                                        ##")
                              logger.info("############################################################")
                              logger.info("############################################################")
                              try:
                                 if parallel_tool_call_ids:
                                    tool_call_id = parallel_tool_call_ids[0]
                                    tool_output = self.tool_completion_status[run.id][tool_call_id]
                                    if tool_output is not None:
                                       self._submit_tool_outputs(
                                          run_id=run.id,
                                          thread_id=thread_id,
                                          tool_call_id=tool_call_id,
                                          function_call_details=function_details,
                                          func_response=tool_output
                                       )
                                 time.sleep(2)
                                 if run.id in self.threads_in_recovery:
                                    self.threads_in_recovery.remove(run.id)
                                 logger.info('* Recovery complete')
                              except Exception as e:
                                 logger.info(f"Failed to resubmit tool outputs for run {run.id} with error: {e}")
                                 if run.id in self.threads_in_recovery:
                                    self.threads_in_recovery.remove(run.id)
                           else:
                              logger.info('* Recovery no longer needed, all calls not yet complete now')
                              if run.id in self.threads_in_recovery:
                                 self.threads_in_recovery.remove(run.id)
                        else:
                           logger.info('* Recovery no longer needed, status is no longer requires_action')
                           if run.id in self.threads_in_recovery:
                              self.threads_in_recovery.remove(run.id)

                     except Exception as e:
                        logger.info(f"Recovery attempted, errored with exception: {e}")
                        if run.id in self.threads_in_recovery:
                            self.threads_in_recovery.remove(run.id)
                        pass


               # need to submit tool runs, but first check how long the run has been going, consider starting a new run

               current_time = time.time()
               if self.use_assistants:
                  try:
                     seconds_left = run.expires_at - current_time
                  except:
                     # run is gone
                     continue
                  logger.info(f"Seconds left before the run {run.id} expires: {seconds_left}")
                  if seconds_left < 120:
                     try:
                        # Cancel the current run
                        restarting_flag = True
                     except Exception as e:
                        logger.info(f"Failed to handle thread expiration for run {run.id} with error: {e}")

               if restarting_flag == False:
                  if self.use_assistants:
                     thread = self.client.beta.threads.retrieve(thread_id)
                  else:
                     thread = types.SimpleNamespace()
                     thread.id = thread_id
                  try:
                     for func_name, func_args, tool_call_id in function_details:
                        if tool_call_id in self.running_tools: # already running in a parallel thread
                           continue
                        log_readable_payload = func_name+"("+func_args+")"
                        try:
                           callback_closure = self._generate_callback_closure(run, thread, tool_call_id, function_details, run.metadata)
                           self.callback_closures[tool_call_id] = callback_closure
                        except Exception as e:
                           logger.error(f"Failed to generate callback closure for run {run.id}, thread {thread.id}, tool_call_id {tool_call_id} with error: {e}")
                        self.running_tools[tool_call_id] = {"run_id": run.id, "thread_id": thread.id }

                        meta = run.metadata
                        primary_user = json.dumps({'user_id': meta.get('user_id', 'unknown_id'),
                                    'user_name': meta.get('user_name', 'unknown_name'),
                                     'user_email': meta.get('user_email', 'unknown_email')})
                        self.log_db_connector.insert_chat_history_row(datetime.datetime.now(), bot_id=self.bot_id, bot_name=self.bot_name, thread_id=thread_id,
                                                                     message_type='Tool Call', message_payload=log_readable_payload,
                                                                     message_metadata={'tool_call_id':tool_call_id, 'func_name':func_name, 'func_args':func_args},
                                                                     channel_type=meta.get("channel_type", None), channel_name=meta.get("channel", None),
                                                                     primary_user=primary_user)
                        logger.telemetry('execute_function:', thread_id, self.bot_id, meta.get('user_email', 'unknown_email'),
                                         os.getenv("BOT_OS_DEFAULT_LLM_ENGINE", ""), func_name, 'arg len:'+str(len(func_args)))
                        func_args_dict = json.loads(func_args)
                        if "image_data" in func_args_dict: # FixMe: find a better way to convert file_id back to stored file
                           func_args_dict["image_data"] = self.file_storage.get(func_args_dict["image_data"].removeprefix('/mnt/data/'))
                           func_args = json.dumps(func_args_dict)
                     # if "files" in func_args_dict: # FixMe: find a better way to convert file_id back to stored file
                     #    files = json.loads(func_args_dict["files"])
                     #    from urllib.parse import quote
                     #    func_args_dict["files"] = json.dumps([f"file://{quote(self.file_storage.get(f['file_id']))}" for f in files])
                     #    func_args = json.dumps(func_args_dict)

                        if 'file_name' in func_args_dict:

                           try:
                              if func_args_dict['file_name'].startswith('/mnt/data/'):
                                 file_id = func_args_dict['file_name'].split('/')[-1]
                                 new_file_location = self.file_storage[file_id]
                                 if '/' in new_file_location:
                                    new_file_location = new_file_location.split('/')[-1]
                                 func_args_dict['file_name'] = new_file_location
                                 func_args = json.dumps(func_args_dict)
                           except Exception as e:
                              logger.warn(f"Failed to update file_name in func_args_dict with error: {e}")

                        if 'openai_file_id' in func_args_dict:

                           try:
                              file_id = func_args_dict['openai_file_id'].split('/')[-1]
                              existing_location = f"./runtime/downloaded_files/{thread_id}/{file_id}"
                              if not os.path.exists(existing_location):
                                 # If the file does not exist at the existing location, download it from OpenAI
                                 try:
                                    os.makedirs(os.path.dirname(existing_location), exist_ok=True)
                                    self._download_openai_file(file_id, thread_id)
                                 except:
                                    pass
                           except:
                              pass

                        self.validate_or_add_function(func_name)


                        if BotOsAssistantOpenAIAsst.stream_mode == True and run.id in StreamingEventHandler.run_id_to_bot_assist:
                           function_name_pretty = re.sub(r'(_|^)([a-z])', lambda m: m.group(2).upper(), func_name).replace('_', '')
                           msg = f"🧰 Using tool: _{function_name_pretty}_...\n\n"
#                           msg = f':toolbox: _Using {func_name}_...\n'


                           if run.id not in StreamingEventHandler.run_id_to_messages:
                              StreamingEventHandler.run_id_to_messages[run.id] = []

                           message_obj = {
                              "type": "tool_call",
                              "text": msg
                           }

                           StreamingEventHandler.run_id_to_messages[run.id].append(message_obj)

                           # Initialize the array for this run_id if it doesn't exist
                           if  StreamingEventHandler.run_id_to_output_stream.get(run.id,None) is not None:
                              if StreamingEventHandler.run_id_to_output_stream.get(run.id,"").endswith('\n'):
                                 StreamingEventHandler.run_id_to_output_stream[run.id] += "\n"
                              else:
                                 StreamingEventHandler.run_id_to_output_stream[run.id] += "\n\n"
                              StreamingEventHandler.run_id_to_output_stream[run.id] += msg
                              msg = StreamingEventHandler.run_id_to_output_stream[run.id]
                           else:
                              StreamingEventHandler.run_id_to_output_stream[run.id] = msg
                           event_callback(self.assistant.id, BotOsOutputMessage(thread_id=thread_id,
                                                                              status=run.status,
                                                                              output=msg+" 💬",
                                                                              messages=None,
                                                                              input_metadata=run.metadata))

                        if func_name not in self.all_functions:
                           self.all_functions = BotOsAssistantOpenAIAsst.all_functions_backup
                           if func_name in self.all_functions:
                              logger.info('!! function was missing from self.all_functions, restored from backup, now its ok')
                           else:
                              logger.info(f'!! function was missing from self.all_functions, restored from backup, still missing func: {func_name}, len of backup={len(BotOsAssistantOpenAIAsst.all_functions_backup)}')

                        # Execute the tool function
                        execute_function(func_name, func_args, self.all_functions, callback_closure,
                                       thread_id = thread_id, bot_id=self.bot_id, status_update_callback=event_callback if event_callback else None, session_id=self.assistant.id if self.assistant.id is not None else None, input_metadata=run.metadata if run.metadata is not None else None, run_id =run.id )#, dispatch_task_callback=dispatch_task_callback)

                     continue
                  except Exception as e:
                     logger.info(f"check_runs - requires action - exception:{e}")
                     try:
                        output = f"!!! Error making tool call, exception:{str(e)}"
                        event_callback(self.assistant.id, BotOsOutputMessage(thread_id=thread_id,
                                                                           status=run.status,
                                                                           output=output,
                                                                           messages=None,
                                                                           input_metadata=run.metadata))
                     except:
                        pass
                     try:
                        self.client.beta.threads.runs.cancel(run_id=run.id, thread_id=thread_id)
                     except:
                        pass

            if restarting_flag == False and ( run.status == "completed" and run.completed_at != thread_run["completed_at"]):

               try:
                  self.done_map[run.metadata['event_ts']] = True
               except:
                  pass

               if self.use_assistants:
                  messages = self.client.beta.threads.messages.list(thread_id=thread_id)
               else:
                  messages = types.SimpleNamespace()
                  messages.data = []
                  message = types.SimpleNamespace()
                  message.content = [
                        types.SimpleNamespace(
                           type='text',
                           text=types.SimpleNamespace(
                                 value=StreamingEventHandler.run_id_to_output_stream[run.id]
                           )
                        )
                     ]
                  message.run_id = run.id
                  message.attachments = []
                  message.id = f"msg_{run.id}"
                  messages.data.append(message)
                  
               output_array = []
               latest_attachments = []
               input_tokens = 0
               output_tokens = 0

               for message in messages.data:

                  if message.run_id is None:
                     continue
                  if (message.run_id != run.id and message.run_id not in self.unposted_run_ids.get(thread_id, [])):
                     break
                  latest_attachments.extend( message.attachments)

                  # Find tool calls that occurred before this message but after the previous message
                  tool_calls = []
                  if run.id in StreamingEventHandler.run_id_to_messages:
                      messages_and_tool_calls = StreamingEventHandler.run_id_to_messages[run.id]
                      found_message = False
                      for item in reversed(messages_and_tool_calls):
                          if not found_message:
                              if item["type"] == "message" and item["id"] == message.id:
                                  found_message = True
                          else:
                              if item["type"] == "tool_call":
                                  tool_calls.insert(0, item)
                              elif item["type"] == "message":
                                  break

                  # If there are tool calls, add them to the output

                  output = ""

                  if tool_calls:
                      for tool_call in tool_calls:
                          output += "\n"+tool_call['text']+"\n"

                  for content in message.content:
                     if content.type == 'image_file':
                        try:
                           file_id = content.image_file.file_id if hasattr(content.image_file, 'file_id') else None
                        #   logger.info('openai image_file tag present, fileid: ',file_id)
                           if file_id is not None and file_id not in latest_attachments:
                              latest_attachments.append({"file_id": file_id})
                        except Exception as e:
                           logger.info('openai error parsing image attachment ',e)
                     if content.type == 'text':
                        try:
                           if output != '' and content.text.value == '!NO_RESPONSE_REQUIRED':
                               pass
                           else:
                              output += (content.text.value + "\n") if output else content.text.value
                        except:
                           pass
                  #output = output.strip()  # Remove the trailing newline if it exists
                  #if output != '!NO_RESPONSE_REQUIRED':
            #      if  StreamingEventHandler.run_id_to_output_stream.get(run.id,None) is not None:
            #         output = StreamingEventHandler.run_id_to_output_stream.get(run.id)
                  try:
                     output_array.append(output)
                  except:
                     pass
               meta_prime = self.run_meta_map.get(run.id, None)
               if meta_prime is not None:
                  meta = meta_prime
               else:
                  meta = run.metadata

                  #  logger.info(f"{self.bot_name} open_ai attachment info going into store files locally: {latest_attachments}")
               files_in = self._store_files_locally(latest_attachments, thread_id)
               output = '\n'.join(reversed(output_array))
               if os.getenv('SHOW_COST', 'false').lower() == 'true':
                  # Check if the thread is in fast mode
                  if thread_id in self.thread_fast_mode_map:
                      model_name = os.getenv("OPENAI_FAST_MODEL_NAME", default="gpt-4o-mini")
                  else:
                      model_name = os.getenv("OPENAI_MODEL_NAME", default="gpt-4o-2024-11-20")
                  if model_name.startswith("gpt-4o"):
                     input_cost = 5.000 / 1000000
                     output_cost = 15.000 / 1000000
                  elif model_name == "gpt-4o-2024-08-06":
                     input_cost = 2.500 / 1000000
                     output_cost = 10.000 / 1000000
                  elif model_name in ["gpt-4o-mini", "gpt-4o-mini-2024-07-18"]:
                     input_cost = 0.150 / 1000000
                     output_cost = 0.600 / 1000000
                  else:
                     # Default to gpt-4o prices if model is unknown
                     input_cost = 5.000 / 1000000
                     output_cost = 15.000 / 1000000
                  if self.use_assistants:
                     total_cost = (run.usage.prompt_tokens * input_cost) + (run.usage.completion_tokens * output_cost)
                     output += f'  `${total_cost:.4f}`'
                     input_tokens = run.usage.prompt_tokens
                     output_tokens = run.usage.completion_tokens
                  else:
                     if hasattr(run, 'usage'):
                        total_cost = (run.usage.prompt_tokens * input_cost) + (run.usage.completion_tokens * output_cost)
                        output += f'  `${total_cost:.4f}`'
                        input_tokens = run.usage.prompt_tokens
                        output_tokens = run.usage.completion_tokens

         #
         #   StreamingEventHandler.run_id_to_messages[run.id]
               event_callback(self.assistant.id, BotOsOutputMessage(thread_id=thread_id,
                                                                     status=run.status,
                                                                     output=output,
                                                                     messages=messages,
                                                                     # UPDATE THIS FOR LOCAL FILE DOWNLOAD
                                                                     files=files_in,
                                                                     input_metadata=meta))
               self.unposted_run_ids[thread_id] = []
               if run.id in StreamingEventHandler.run_id_to_output_stream:
                   del StreamingEventHandler.run_id_to_output_stream[run.id]
               if run.id in StreamingEventHandler.run_id_to_messages:
                   del StreamingEventHandler.run_id_to_messages[run.id]
               try:
                  message_metadata = str(message.content)
               except:
                  message_metadata = "!error converting content to string"
               primary_user = json.dumps({'user_id': meta.get('user_id', 'unknown_id'),
                              'user_name': meta.get('user_name', 'unknown_name'),
                              'user_email': meta.get('user_email', 'unknown_email')})
               try:
                  self.log_db_connector.insert_chat_history_row(datetime.datetime.now(), bot_id=self.bot_id, bot_name=self.bot_name, thread_id=thread_id,
                                                                  message_type='Assistant Response', message_payload=output, message_metadata=message_metadata,
                                                                  tokens_in=input_tokens, tokens_out=output_tokens, files=files_in,
                                                                  channel_type=meta.get("channel_type", None), channel_name=meta.get("channel", None),
                                                                  primary_user=primary_user)
               except:
                  pass
               threads_completed[thread_id] = run.completed_at
               logger.telemetry('add_answer:', thread_id, self.bot_id, meta.get('user_email', 'unknown_email'),
                                os.getenv("BOT_OS_DEFAULT_LLM_ENGINE", ""), input_tokens, output_tokens)
         else:
            logger.debug(f"check_runs - {thread_id} - {run.status} - {run.completed_at} - {thread_run['completed_at']}")


            # record completed runs.  FixMe: maybe we should rmeove from the map at some point?
            for thread_id in threads_completed:
               self.thread_run_map[thread_id]["completed_at"] = threads_completed[thread_id]

         # get next run to check
       #  try:
       #     thread_id = self.active_runs.popleft()
       #  except IndexError:
         #if restarting_flag == False and run.status != "requires_action" and run.status != 'cancelled':
           # if thread_id in self.processing_runs:
           #    if run.status == 'cancelled':
       #           self.processing_runs.remove(thread_id)
       #       return
        # else:
        #    if restarting_flag == False:
        #       self.active_runs.append(thread_id)

         if restarting_flag == True:
               try:
                  self.client.beta.threads.runs.cancel(thread_id=thread_id, run_id=run.id)
               except Exception as e:
                  pass
               # Add a user message to the thread
               if run.id in self.threads_in_recovery:
                  self.threads_in_recovery.remove(run.id)
               meta_prime = self.run_meta_map.get(run.id, None)
               if meta_prime is not None:
                  meta = meta_prime
               else:
                  meta = run.metadata
     #          if 'thinking_ts' in meta:
     #             del meta['thinking_ts']
               meta['parent_run'] = run.id
               if thread_id in self.processing_runs:
                  self.processing_runs.remove(thread_id)
               if thread_id in threads_still_pending:
                  del threads_still_pending[thread_id]

               if thread_id not in self.unposted_run_ids:
                   self.unposted_run_ids[thread_id] = []
               if thread_id not in self.active_runs:
                   self.active_runs.append(thread_id)
               self.unposted_run_ids[thread_id].append(run.id)
               # check here to make sure correct thread_id is getting put on this...should be the input thread id
               if failed_but_restarting_flag:
                  self.add_message(BotOsInputMessage(thread_id=thread_id, msg='The previous run failed, please try again where you left off.', metadata=meta))
               else:
                  self.add_message(BotOsInputMessage(thread_id=thread_id, msg='The run has expired, please resubmit the tool call(s).', metadata=meta))
               # Remove the current thread/run from the processing queue

               # Add the new thread/run to the active runs queue
               # self.active_runs_queue.append((thread_id, run.id))
               logger.info(f"Run {run.id} restarted in new message")
               return


         #thread_id = None

      if thread_id in self.processing_runs:
         self.processing_runs.remove(thread_id)
      # put pending threads back on queue
      for thread_id in threads_still_pending:
         if thread_id not in self.active_runs:
             self.active_runs.append(thread_id)
