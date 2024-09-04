import json
import os, uuid, re
from typing import TypedDict
from core.bot_os_assistant_base import BotOsAssistantInterface, execute_function
from openai import OpenAI
from collections import deque
import datetime
import time 
import logging
import threading
import core.global_flags as global_flags
from core.bot_os_input import BotOsInputMessage, BotOsOutputMessage
from core.bot_os_defaults import _BOT_OS_BUILTIN_TOOLS, BASE_BOT_INSTRUCTIONS_ADDENDUM
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
from bot_genesis.make_baby_bot import (  get_bot_details ) 

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.WARN, format='%(asctime)s - %(levelname)s - %(message)s')

def _get_function_details(run):
      function_details = []
      if run.required_action and run.required_action.submit_tool_outputs:
         for tool_call in run.required_action.submit_tool_outputs.tool_calls:
            function_details.append(
               (tool_call.function.name, tool_call.function.arguments, tool_call.id)
            )
      else:
         print("run.required_action.submit_tool_outputs is None")
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
       # print(f"\nassistant on_text_delta > {delta.value}", end="", flush=True)
#       print(f"{delta.value}")
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
    #   print(f"\nassistant on_message_created > {message}\n", end="", flush=True)
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
   
      try:
         txt = message.text
      except:
         try:
            txt = message.content[0].text.value
         except:
            txt = None
            pass
      if self.run_id  in StreamingEventHandler.run_id_to_output_stream and txt != None:
         StreamingEventHandler.run_id_to_output_stream[self.run_id] = txt


   @override
   def on_message_delta(self, delta: MessageDelta, snapshot: Message) -> None:
       # print(f"\nassistant on_message_delta > {delta}\n", end="", flush=True)
       pass

   def on_tool_call_created(self, tool_call):
       # 4
       print(f"\nassistant tool_call > {tool_call}\n", end="", flush=True)
       return
       print(f"\nassistant on_tool_call_created > {tool_call}")
       self.function_name = tool_call.function.name       
       self.tool_id = tool_call.id
       print(f"\n_tool_call_created > run_step.status > {self.run_step.status}")
      
       print(f"\nassistant > {tool_call.type} {self.function_name}\n", flush=True)

       keep_retrieving_run = self.client.beta.threads.runs.retrieve(
           thread_id=self.thread_id,
           run_id=self.run_id
       )

       while keep_retrieving_run.status in ["queued", "in_progress"]: 
           keep_retrieving_run = self.client.beta.threads.runs.retrieve(
               thread_id=self.thread_id,
               run_id=self.run_id
           )
          
           print(f"\nSTATUS: {keep_retrieving_run.status}")      
      
   @override
   def on_tool_call_done(self, tool_call: ToolCall) -> None: 
       return      
       keep_retrieving_run = self.client.beta.threads.runs.retrieve(
           thread_id=self.thread_id,
           run_id=self.run_id
       )
      
       print(f"\nDONE STATUS: {keep_retrieving_run.status}")
      
       if keep_retrieving_run.status == "completed":
           all_messages = self.client.beta.threads.messages.list(
               thread_id=self.thread_id
           )

           print(all_messages.data[0].content[0].text.value, "", "")
           return
      
       elif keep_retrieving_run.status == "requires_action":
           print("here you would call your function")

           if self.function_name == "example_blog_post_function":
               function_data = my_example_funtion()
  
               self.output=function_data
              
               with self.client.beta.threads.runs.submit_tool_outputs_stream(
                   thread_id=self.thread_id,
                   run_id=self.run_id,
                   tool_outputs=[{
                       "tool_call_id": self.tool_id,
                       "output": self.output,
                   }],
                   event_handler=StreamingEventHandler(self.client, self.thread_id, self.assistant_id)
               ) as stream:
                 stream.until_done()                       
           else:
               print("unknown function")
               return
      
   @override
   def on_run_step_created(self, run_step: RunStep) -> None:
       # 2       
       return
       print(f"on_run_step_created")
       self.run_id = run_step.run_id
       self.run_step = run_step
       print("The type ofrun_step run step is ", type(run_step), flush=True)
       print(f"\n run step created assistant > {run_step}\n", flush=True)

   @override
   def on_run_step_done(self, run_step: RunStep) -> None:
       return
       print(f"\n run step done assistant > {run_step}\n", flush=True)

   def on_tool_call_delta(self, delta, snapshot): 
       return
       if delta.type == 'function':
           # the arguments stream through here and then you get the requires action event
           print(delta.function.arguments, end="", flush=True)
           self.arguments += delta.function.arguments
       elif delta.type == 'code_interpreter':
           print(f"on_tool_call_delta > code_interpreter")
           if delta.code_interpreter.input:
               print(delta.code_interpreter.input, end="", flush=True)
           if delta.code_interpreter.outputs:
               print(f"\n\noutput >", flush=True)
               for output in delta.code_interpreter.outputs:
                   if output.type == "logs":
                       print(f"\n{output.logs}", flush=True)
       else:
           print("ELSE")
           print(delta, end="", flush=True)

   @override
   def on_event(self, event: AssistantStreamEvent) -> None:
       # print("In on_event of event is ", event.event, flush=True)
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
            print(f"----> run is {self.run_id}")
            if self.thread_id not in self.bot_assist.active_runs:
               self.bot_assist.active_runs.append(self.thread_id)
       except:
          pass
       return 
       if event.event == "thread.run.requires_action":
           print("\nthread.run.requires_action > submit tool call")
           print(f"ARGS: {self.arguments}")


class BotOsAssistantOpenAI(BotOsAssistantInterface):

   stream_mode = False
   all_functions_backup = None

   def __init__(self, name:str, instructions:str, 
                tools:list[dict] = {}, available_functions={}, files=[], 
                update_existing=False, log_db_connector=None, bot_id='default_bot_id', bot_name='default_bot_name', all_tools:list[dict]={}, all_functions={},all_function_to_tool_map={}, skip_vectors=False) -> None:
      logger.debug("BotOsAssistantOpenAI:__init__") 
      super().__init__(name, instructions, tools, available_functions, files, update_existing, skip_vectors=False, bot_id=bot_id, bot_name=bot_name)

      self.client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
      model_name = os.getenv("OPENAI_MODEL_NAME", default="gpt-4o")
    
      name = bot_id
      print("-> OpenAI Model == ",model_name)
      self.thread_run_map = {}
      self.active_runs = deque()
      self.processing_runs = deque()
      self.done_map = {}
      self.file_storage = {}
      self.available_functions = available_functions
      self.all_tools = all_tools
      self.all_functions = all_functions
      if BotOsAssistantOpenAI.all_functions_backup == None and all_functions is not None:
         BotOsAssistantOpenAI.all_functions_backup = all_functions
      self.all_function_to_tool_map = all_function_to_tool_map
      self.running_tools = {}
      self.tool_completion_status = {}
      self.log_db_connector = log_db_connector
      my_tools = tools + [{"type": "code_interpreter"}, {"type": "file_search"}]
      #my_tools = tools 
      #print(f'yoyo mytools {my_tools}')
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
     # self.last_stop_time_map = {}

      genbot_internal_project_and_schema = os.getenv('GENESIS_INTERNAL_DB_SCHEMA','None')
      if genbot_internal_project_and_schema is not None:
         genbot_internal_project_and_schema = genbot_internal_project_and_schema.upper()
      self.genbot_internal_project_and_schema = genbot_internal_project_and_schema
      if genbot_internal_project_and_schema == 'None':
         print("ENV Variable GENESIS_INTERNAL_DB_SCHEMA is not set.")

      self.db_schema = genbot_internal_project_and_schema.split('.')
      self.internal_db_name = self.db_schema[0]
      self.internal_schema_name = self.db_schema[1]

      my_assistants = self.client.beta.assistants.list(
         order="desc", limit=100
      )
      print('finding assistant...')
      my_assistants = [a for a in my_assistants if a.name == name]
      print('assistant found')

      if len(my_assistants) == 0 and update_existing:
         vector_store_name = self.bot_id + '_vectorstore'
         self.vector_store = self.create_vector_store(vector_store_name=vector_store_name, files=files)
         self.tool_resources = {"file_search": {"vector_store_ids": [self.vector_store]}}
         if True or hasattr(files, 'urls') and files.urls is not None:
            self.assistant = self.client.beta.assistants.create(
            name=name,
            instructions=instructions,
            tools=my_tools, # type: ignore
            model=model_name,
            # file_ids=self._upload_files(files) #FixMe: what if the file contents change?
            tool_resources=self.tool_resources,
            temperature=0.0
            )
         else:
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
               self.tool_resources = {"file_search": {"vector_store_ids": [self.vector_store]}}
            else:
               self.tool_resources = {"file_search": {"vector_store_ids": [vector_store_id]}}
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
         
      logger.debug(f"BotOsAssistantOpenAI:__init__: assistant.id={self.assistant.id}")

   @override
   def is_active(self) -> bool:
      return self.active_runs
   
   @override
   def is_processing_runs(self) -> bool:
      return self.processing_runs

   @override
   def get_done_map(self) -> dict:
      return self.done_map

   @staticmethod
   def load_by_name(name: str):
      return BotOsAssistantOpenAI(name, update_existing=False)

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
          print(f"{self.bot_name} for bot {for_bot} update_vector_store excluded files with invalid extensions: {', '.join(excluded_files)}")
      for file in stage_files:
          # Read each file from the stage and save it to a local location
          try:
              # Assuming 'self' has an attribute 'snowflake_connector' which is an instance of the SnowflakeConnector class
            new_file_location = f"./downloaded_files/{for_bot}_BOT_DOCS/{file}"
            os.makedirs(f"./downloaded_files/{for_bot}_BOT_DOCS", exist_ok=True)
            contents = self.log_db_connector.read_file_from_stage(
                  database=self.internal_db_name,
                  schema=self.internal_schema_name,
                  stage='BOT_FILES_STAGE',
                  file_name=file,
                  for_bot=f'{for_bot}_BOT_DOCS',
                  thread_id=f'{for_bot}_BOT_DOCS',
                  return_contents=False
                  )
            if contents==file:
               local_file_path = new_file_location
               files_from_stage.append(local_file_path)
               print(f"{self.bot_name} for bot {for_bot} update_vector_store successfully retrieved {file} from stage and saved to {new_file_location}")
          except Exception as e:
               print(f"{self.bot_name} for bot {for_bot} update_vector_store failed to retrieve {file} from stage: {e}")
          
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
      vector_store = self.client.beta.vector_stores.create(name=vector_store_name)
      
      return self.update_vector_store(vector_store.id, files, plain_files, for_bot=for_bot)



   def create_thread(self) -> str:
      logger.debug("BotOsAssistantOpenAI:create_thread") 
      thread_id = self.client.beta.threads.create().id
      print(f"{self.bot_name} openai new_thread -> {thread_id}")
      return thread_id

   def _upload_files(self, files, thread_id=None):
      file_ids = []
      file_map = []
      for f in files:

         original_file_location = f
         file_name = original_file_location.split('/')[-1]
         new_file_location = f"./downloaded_files/{thread_id}/{file_name}"
         os.makedirs(f"./downloaded_files/{thread_id}", exist_ok=True)
         with open(original_file_location, 'rb') as source_file:
             with open(f"./downloaded_files/{thread_id}/{file_name}", 'wb') as dest_file:
                 dest_file.write(source_file.read())
   
    #     print("loading files")
         fo = open(new_file_location,"rb")
         file = self.client.files.create(file=(file_name, fo), purpose="assistants")

         file_ids.append(file.id)
         
         # make a copy based on the new openai file.id as well in case the bot needs it by this reference later
         new_file_location_file_id = f"./downloaded_files/{thread_id}/{file.id}"
         with open(original_file_location, 'rb') as source_file:
             with open(new_file_location_file_id, 'wb') as dest_file:
                 dest_file.write(source_file.read())
         

         self.file_storage[file.id] = new_file_location
         file_map.append({'file_id': file.id, 'file_name': file_name})
         
      logger.debug(f"BotOsAssistantOpenAI:_upload_files - uploaded {len(file_ids)} files") 
      return file_ids, file_map



   def add_message(self, input_message:BotOsInputMessage):#thread_id:str, message:str, files):
      #logger.debug("BotOsA ssistantOpenAI:add_message") 

      thread_id = input_message.thread_id

      stop_flag = False
      if input_message.msg.endswith(') says: !model') or input_message.msg=='!model':
         input_message.msg = input_message.msg.replace ('!model',f'SYSTEM MESSAGE: The User has requested to know what LLM model is running.  Respond by telling them that the current model is: { os.getenv("OPENAI_MODEL_NAME", default="gpt-4o")}')
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
                  print(f"Cancelled run_id: {run.id} for thread_id: {thread_id}")
                  resp = "Streaming stopped for previous request"
                  stopped = True
               except:
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
                     print("stop ",i)
                     
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

      thread = self.client.beta.threads.retrieve(thread_id)
      #logger.warn(f"ADDING MESSAGE -- input thread_id: {thread_id} -> openai thread: {thread}")
      try:
         #logger.error("REMINDER: Update for message new files line 117 on botosopenai.py")
         #print('... openai add_message before upload_files, input_message.files = ', input_message.files)
         file_ids, file_map = self._upload_files(input_message.files, thread_id=thread_id)
         #print('... openai add_message file_id, file_map: ', file_ids, file_map)
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
        # print('... openai add_message attachments: ', attachments)
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
                  print(f"Extracted run_id: {run_id}")
                  self.client.beta.threads.runs.cancel(run_id=run_id, thread_id=thread_id)
                  print(f"Cancelled run_id: {run_id}")
                  thread_message = self.client.beta.threads.messages.create(
                     thread_id=thread_id, attachments=attachments, content=content, 
                     role="user", 
                  )
               fixed = True
         except Exception as e:
            pass
       # removed some stuff here 6/15/24
       #logger.debug(f"add_message - created {thread_message}")
      self.first_message = False 
      task_meta = input_message.metadata.pop('task_meta', None)

      if BotOsAssistantOpenAI.stream_mode == True:
         try:
         
            with self.client.beta.threads.runs.stream(
               thread_id=thread.id,
               assistant_id=self.assistant.id,
               event_handler=StreamingEventHandler(self.client, thread.id, self.assistant.id, input_message.metadata, self),
               metadata=input_message.metadata
            ) as stream:
             #  print('here')
               stream.until_done()
         except Exception as e:
            try:
               if e.status_code == 400 and 'already has an active run' in e.message:
                  print('bot_os_openai add_message thread already has an active run, putting event back on queue...')
                  return False
            except:
               pass
            print('bot_os_openai add_message Error from OpenAI on run.streams: ',e)
            return False
      else:
         run = self.client.beta.threads.runs.create(
            thread_id=thread.id, assistant_id=self.assistant.id, metadata=input_message.metadata)
         if task_meta is not None:
            self.run_meta_map[run.id]=task_meta
         self.thread_run_map[thread_id] = {"run": run.id, "completed_at": None}
         self.active_runs.append(thread_id)

      primary_user = json.dumps({'user_id': input_message.metadata.get('user_id', 'Unknown User ID'), 
                                 'user_name': input_message.metadata.get('user_name', 'Unknown User')})
      self.log_db_connector.insert_chat_history_row(datetime.datetime.now(), bot_id=self.bot_id, bot_name=self.bot_name, thread_id=thread_id, 
                                                    message_type='User Prompt', message_payload=input_message.msg, message_metadata=input_message.metadata, files=attachments,
                                                    channel_type=input_message.metadata.get("channel_type", None), channel_name=input_message.metadata.get("channel", None),
                                                    primary_user=primary_user)
      return True

   def is_bot_openai(self,bot_id):
       bot_details = get_bot_details(bot_id)
       return bot_details.get("bot_implementation") == 'openai'

   def reset_bot_if_not_openai(self,bot_id):
       bot_details = get_bot_details(bot_id)
       if bot_details.get("bot_implementation") != "openai":
           os.environ[f'RESET_BOT_SESSION_{bot_id}'] = 'True'
           return True
       else:
            return False


   def _submit_tool_outputs(self, run_id, thread_id, tool_call_id, function_call_details, func_response, metadata=None):
     
     # logger.debug(f"_submit_tool_outputs - {thread_id} {run_id} {tool_call_id} - {function_call_details} - {func_response}")

      new_response = func_response
 
  #    if function_call_details[0][0] == '_lookup_slack_user_id' and isinstance(func_response, str):
  #          new_response = {"response": func_response}
  #          func_response = new_response

      if isinstance(func_response, str):
         try:
            new_response = {"success": False, "message": func_response}
            func_response = new_response
            print(f'openai submit_tool_outputs string response converted call: {function_call_details}, response: {func_response}')
         except:
            print(f'openai submit_tool_outputs string response converted call to JSON.')

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
                     #print(all_tools_for_bot)
                     #print(self.all_function_to_tool_map)
                     bot_tools_array = []
                     for tool in all_tools_for_bot:
                     #  logger.warn(f'--> Calling validate_or_add_function on {tool} <---- ')
                        self.validate_or_add_function(tool)
                        tool_name = tool
                        if tool_name in self.all_function_to_tool_map:
                           for t in self.all_function_to_tool_map[tool_name]:
                              bot_tools_array.append(t)

                  my_assistants = self.client.beta.assistants.list(order="desc", limit=100)
                  my_assistants = [a for a in my_assistants if a.name == target_bot]

                  for assistant in my_assistants:

                     new_instructions = assistant.instructions 
                     if "snowflake_stage_tools" in all_tools_for_bot and 'make_baby_bot' in all_tools_for_bot:        
                           new_instructions += f"\nYour Internal Files Stage for bots is at snowflake stage: {self.genbot_internal_project_and_schema}.BOT_FILES_STAGE"
                           print("Instruction for target bot updated with Internal Files Stage location.")
                     bot_tools_array = bot_tools_array + _BOT_OS_BUILTIN_TOOLS + [{"type": "code_interpreter"}, {"type": "file_search"}]

                     if "database_tools" in all_tools_for_bot:
                        workspace_schema_name = f"{global_flags.project_id}.{target_bot.replace(r'[^a-zA-Z0-9]', '_').replace('-', '_').replace('.', '_')}_WORKSPACE".upper()
                        new_instructions += f"\nYou have a workspace schema created specifically for you named {workspace_schema_name} that the user can also access. You may use this schema for creating tables, views, and stages that are required when generating answers to data analysis questions. Only use this schema if asked to create an object. Always return the full location of the object."

                     self.client.beta.assistants.update(assistant.id,tools=bot_tools_array, instructions=new_instructions)

               else: # target bot is not openai
                  # this will start a new session with the updated tools and proper instructions
                  self.reset_bot_if_not_openai(bot_id=target_bot) 
                  
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

                  if "snowflake_stage_tools" in bot_details["available_tools"] and 'make_baby_bot' in bot_details["available_tools"]:        
                     instructions += f"\nYour Internal Files Stage for bots is at snowflake stage: {global_flags.genbot_internal_project_and_schema}.BOT_FILES_STAGE"

                  if "database_tools" in bot_details["available_tools"]:
                     
                     workspace_schema_name = f"{global_flags.project_id}.{target_bot.replace(r'[^a-zA-Z0-9]', '_').replace('-', '_').replace('.', '_')}_WORKSPACE".upper()
                     instructions += f"\nYou have a workspace schema created specifically for you named {workspace_schema_name} that the user can also access. You may use this schema for creating tables, views, and stages that are required when generating answers to data analysis questions. Only use this schema if asked to create an object. Always return the full location of the object."

                  if not self.reset_bot_if_not_openai(bot_id=target_bot):

                     my_assistants = self.client.beta.assistants.list(order="desc",limit=100)
                     my_assistants = [a for a in my_assistants if a.name == target_bot]

                     for assistant in my_assistants:
                        self.client.beta.assistants.update(assistant.id,instructions=instructions)
                     
                  print(f"Bot instructions for {target_bot} updated: {instructions}")
                        
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
                  tool_resources = {"file_search": {"vector_store_ids": [vector_store]}}
               else:
                #  bot_tools = [tool for tool in bot_tools if tool.get('type') != 'file_search']
                  tool_resources = {}
               self.client.beta.assistants.update(assistant_zero.id, tool_resources=tool_resources)

               print(f"{self.bot_name} open_ai submit_tool_outputs Bot files for {target_bot} updated.")
      except Exception as e:
         print(f'openai submit_tool_outputs error to tool checking, func_response: {func_response} e: {e}')    

      if tool_call_id is not None: # in case this is a resubmit
         self.tool_completion_status[run_id][tool_call_id] = new_response

      # check if all parallel tool calls are complete

      run = self.client.beta.threads.runs.retrieve(thread_id = thread_id, run_id = run_id)
      function_details = _get_function_details(run)
      
      if run.status == 'requires_action':
         parallel_tool_call_ids = [f[2] for f in function_details]

         #  check to see if any expected tool calls are missing from completion_status 
         missing_tool_call_ids = [tool_call_id for tool_call_id in parallel_tool_call_ids if tool_call_id not in self.tool_completion_status[run.id]]
         if missing_tool_call_ids: 
            print('Error: a parallel tool call is missing form the completion status map.  Probably need to fail the run.')
            return

         if all(self.tool_completion_status[run.id][key] is not None for key in parallel_tool_call_ids):
            tool_outputs = [{'tool_call_id': key, 'output': str(self.tool_completion_status[run.id][key])} for key in parallel_tool_call_ids]
         else:
            logger.info(f"_submit_tool_outputs - {thread_id} {run_id} {tool_call_id}, not submitted, waiting for parallel tool calls")
            return
      else:
         print('No tool response needed for this run, status is now ',run.status)
         return

    #  if any(value is None for value in self.tool_completion_status[run_id].values()):
    #     return
      
      # now package up the responses together

      tool_outputs = [{'tool_call_id': k, 'output': str(v)} for k, v in self.tool_completion_status[run_id].items()]

      # if os.getenv("USE_KNOWLEDGE", "false").lower() == 'true' and metadata is not None:
      #    primary_user = json.dumps({'user_id': metadata.get('user_id', 'Unknown User ID'), 
      #                   'user_name': metadata.get('user_name', 'Unknown User')})
      #    knowledge = self.log_db_connector.extract_knowledge(primary_user, self.bot_name, bot_id=self.bot_id)
      #    if knowledge:
      #          if function_call_details[0][0] == 'search_metadata' and self.first_tool_call[thread_id]:
      #             tool_outputs[0]['output'] += f'''\n\nNOTE--Here are some things you know about this user and the data they used from previous interactions, that may be helpful to this conversation:
      #                            {knowledge['DATA_LEARNING']}''' 
      #             metadata["data_knowledge"] = knowledge['DATA_LEARNING']
      #             self.first_tool_call[thread_id] = False
      #          elif self.first_data_call[thread_id]:
      #             tool_outputs[0]['output'] += f'''\n\nNOTE--Here are some things you know about this user and the tools they called from previous interactions, that may be helpful to this conversation:
      #                            {knowledge['TOOL_LEARNING']}'''
      #             metadata["tool_knowledge"] = knowledge['TOOL_LEARNING'] 
      #             self.first_data_call[thread_id] = False


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
         if BotOsAssistantOpenAI.stream_mode == True:
 
            meta = StreamingEventHandler.run_id_to_metadata.get(run_id,None)
            print(f'{self.bot_name} openai submit_tool_outputs submitting tool outputs len={len(tool_outputs)} ')
            run_id_to_update = run_id
    #        import random
    #        if random.random() < 0.33:
    #            run_id_to_update = "Zowzers!"
    #        else:
    #            run_id_to_update = run_id
            with self.client.beta.threads.runs.submit_tool_outputs_stream(
                   thread_id=thread_id,
                   run_id=run_id_to_update,
                   tool_outputs=tool_outputs,
                   event_handler=StreamingEventHandler(self.client, thread_id,   StreamingEventHandler.run_id_to_bot_assist[run_id],  meta, self)
               ) as stream:
                  print('.. (not) sleeping 0.0 seconds before requeing run after submit_tool_outputs...')
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
            print('...sleeping 0.2 seconds before requeing run after submit_tool_outputs...')
            time.sleep(0.2)
            if thread_id in self.processing_runs:
               self.processing_runs.remove(thread_id)
            if thread_id not in self.active_runs:
               self.active_runs.append(thread_id)
       #  if thread_id in self.processing_runs:
       #     self.processing_runs.remove(thread_id)
         primary_user = json.dumps({'user_id': meta.get('user_id', 'Unknown User ID'), 
                     'user_name': meta.get('user_name', 'Unknown User')})
         for tool_output in tool_outputs:
            self.log_db_connector.insert_chat_history_row(datetime.datetime.now(), bot_id=self.bot_id, bot_name=self.bot_name, thread_id=thread_id, 
                                                          message_type='Tool Output', message_payload=tool_output['output'], 
                                                          message_metadata={'tool_call_id':tool_output['tool_call_id']},
                                                          channel_type=meta.get("channel_type", None), channel_name=meta.get("channel", None),
                                                          primary_user=primary_user)

      except Exception as e:
         logger.error(f"submit_tool_outputs - caught exception: {e}")

   def _generate_callback_closure(self, run, thread, tool_call_id, function_details, metadata=None):
      def callback_closure(func_response):  # FixMe: need to break out as a generate closure so tool_call_id isn't copied
         try:                     
            del self.running_tools[tool_call_id]
         except Exception as e:
            error_string = f"callback_closure - tool call already deleted - caught exception: {e}"
            print(error_string)
         try:
            self._submit_tool_outputs(run.id, thread.id, tool_call_id, function_details, func_response, metadata)
         except Exception as e:
            error_string = f"callback_closure - _submit_tool_outputs - caught exception: {e}"
            print(error_string)
            print(traceback.format_exc())
            try:
               self._submit_tool_outputs(run.id, thread.id, tool_call_id, function_details, error_string, metadata)
            except Exception as e:
               error_string = f"callback_closure - _submit_tool_outputs - caught exception: {e} submitting error_string {error_string}"
               print(error_string)
 
      return callback_closure

   def _download_openai_file(self, file_id, thread_id):
      logger.debug(f"BotOsAssistantOpenAI:download_openai_file - {file_id}")
      # Use the retrieve file contents API to get the file directly

      try:
        # print(f"{self.bot_name} open_ai download_file file_id: {file_id}", flush=True)

         try:
            file_id = file_id.get('file_id',None)
         except:
            try:
               file_id = file_id.file_id
            except: 
               pass

         file_info = self.client.files.retrieve(file_id=file_id)
      #   print(f"{self.bot_name} open_ai download_file id: {file_info.id} name: {file_info.filename}", flush=True)
         file_contents = self.client.files.content(file_id=file_id)

     #    try:         
     #       print(f"{self.bot_name} open_ai download_file file_id: {file_id} contents_len: {len(file_contents.content)}", flush=True)
     #    except Exception as e:
     #        print(f"{self.bot_name} open_ai download_file file_id: {file_id} ERROR couldn't get file length: {e}", flush=True)
  
         local_file_path = os.path.join(f"./downloaded_files/{thread_id}/", os.path.basename(file_info.filename))
      #   print(f"{self.bot_name} open_ai download_file file_id: {file_id} localpath: {local_file_path}", flush=True)
         
         # Ensure the directory exists
         os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
         
         
         # Save the file contents locally
         file_contents.write_to_file(local_file_path)
       
  #       print(f"{self.bot_name} open_ai download_file wrote file: {file_id} to localpath: {local_file_path}", flush=True)
       
         # Save a copy of the file with the file_id as the file name
         try:
            file_id_based_path = f"./downloaded_files/{thread_id}/{file_id}"
            file_contents.write_to_file(file_id_based_path)
         except Exception as e:
            print(f"{self.bot_name} open_ai download_file - error - couldnt write to {file_id_based_path} err: {e}", flush=True)
            pass
      except Exception as e:
         print(f"{self.bot_name} open_ai download_file ERROR: {e}", flush=True)
 

      logger.debug(f"File downloaded: {local_file_path}")
      return local_file_path

   def _store_files_locally(self, file_ids, thread_id):
    #  print(f"{self.bot_name} open_ai store_files_locally, file_ids: {file_ids}", flush=True)
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
         print(f'validate_or_add_function, fn name={function_name}')
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

         print(f"Likely newly generated function '{function_name}' added all_functions.")
         return False

   
   def check_runs(self, event_callback):
      logger.debug("BotOsAssistantOpenAI:check_runs") 

      threads_completed = {}
      threads_still_pending = []
#      for thread_id in self.thread_run_map:
      try:
         thread_id = self.active_runs.popleft()
         if thread_id is None:
            return
       #  print(f"0-0-0-0-0-0-0->>>> thread_id: {thread_id}, in self.processing_runs: {thread_id in self.processing_runs}")
         if thread_id in self.processing_runs:
        #    print('.... outta here ...')
            return
         if thread_id not in self.processing_runs:
            self.processing_runs.append(thread_id)

      except IndexError:
         thread_id = None
         return

      for _ in range(1):
         thread_run = self.thread_run_map[thread_id]
         restarting_flag = False
         if thread_run["completed_at"] is None:

            run = self.client.beta.threads.runs.retrieve(thread_id = thread_id, run_id = thread_run["run"])
           # print(run.status)
            if (run.status == "in_progress" or run.status == 'requires_action') and BotOsAssistantOpenAI.stream_mode == True and run.id in StreamingEventHandler.run_id_to_output_stream:
                #print(StreamingEventHandler.run_id_to_output_stream[run.id])

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
                     print(f"Cancelled run_id: {run.id} for thread_id: {thread_id}")
               event_callback(self.assistant.id, BotOsOutputMessage(thread_id=thread_id, 
                                                      status=run.status, 
                                                      output=output,
                                                      messages=None, 
                                                      input_metadata=run.metadata))
           #    continue
                                         
            #logger.info(f"run.status {run.status} Thread: {thread_id}")
            print(f"{self.bot_name} open_ai check_runs ",run.status," thread: ", thread_id, ' runid: ', run.id, flush=True)

            current_time = datetime.datetime.now()
            run_duration = (current_time - datetime.datetime.fromtimestamp(run.created_at)).total_seconds()
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
                  print("requires action exception: ",e)
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


            if run.status == "failed":
               print(f"!!!!!!!!!! FAILED JOB, run.lasterror {run.last_error} !!!!!!!")
               # resubmit tool output if throttled
               #tools_to_rerun = {k: v for k, v in self.tool_completion_status[run.id].items() if v is not None}
               #self._run_tools(thread_id, run, tools_to_rerun) # type: ignore
               #self._submit_tool_outputs(run.id, thread_id, tool_call_id=None, function_call_details=self.tool_completion_status[run.id],
               #                          func_response=None)
               # Todo add more handling here to tell the user the thread failed
               output = StreamingEventHandler.run_id_to_output_stream.get(run.id,'') + f"\n\n!!! Error from OpenAI, run.lasterror {run.last_error} on run {run.id} for thread {thread_id}!!!"
               
               event_callback(self.assistant.id, BotOsOutputMessage(thread_id=thread_id, 
                                                                     status=run.status, 
                                                                     output=output, 
                                                                     messages=None, 
                                                                     input_metadata=run.metadata))
               self.log_db_connector.insert_chat_history_row(datetime.datetime.now(), bot_id=self.bot_id, bot_name=self.bot_name, thread_id=thread_id,
                                                              message_type='Assistant Response', message_payload=output, message_metadata=None, 
                                                              tokens_in=0, tokens_out=0)
               threads_completed[thread_id] = run.completed_at
               continue


            if run.status == "expired":
               print(f"!!!!!!!!!! EXPIRED JOB, run.lasterror {run.last_error} !!!!!!!")
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
               try:
                  function_details = _get_function_details(run)
               except Exception as e:
                  print('!! no function details')
                  continue 

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
                     print(f"*** Run {run.id} is now in recovery mode. *** ")
                     time.sleep(3)
                     try:
                        run = self.client.beta.threads.runs.retrieve(thread_id = thread_id, run_id = thread_run["run"])
                        function_details = _get_function_details(run)
                        if run.status == 'requires_action':
                           parallel_tool_call_ids = [f[2] for f in function_details]
                           if all(self.tool_completion_status[run.id][key] is not None for key in parallel_tool_call_ids):
                              print('All tool call results are ready for this run, and its still pending after a 3 second delay')
                              print("############################################################")
                              print("############################################################")
                              print("##                                                        ##")
                              print("##              Resubmitting tool outputs !!              ##")
                              print("##                                                        ##")
                              print("############################################################")
                              print("############################################################")
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
                                 print('* Recovery complete')
                              except Exception as e:
                                 print(f"Failed to resubmit tool outputs for run {run.id} with error: {e}")
                                 if run.id in self.threads_in_recovery:
                                    self.threads_in_recovery.remove(run.id)
                           else: 
                              print('* Recovery no longer needed, all calls not yet complete now')
                              if run.id in self.threads_in_recovery:
                                 self.threads_in_recovery.remove(run.id)
                        else:
                           print('* Recovery no longer needed, status is no longer requires_action')
                           if run.id in self.threads_in_recovery:
                              self.threads_in_recovery.remove(run.id)

                     except Exception as e:
                        print("Recovery attempted, errored with exception: ",e)
                        if run.id in self.threads_in_recovery:
                            self.threads_in_recovery.remove(run.id)
                        pass
               

               # need to submit tool runs, but first check how long the run has been going, consider starting a new run

               current_time = time.time()
               try:
                  seconds_left = run.expires_at - current_time
               except:
                  # run is gone
                  continue
               print(f"Seconds left before the run {run.id} expires: {seconds_left}")
               if seconds_left < 120:
                  try:
                     # Cancel the current run
                     restarting_flag = True
                  except Exception as e:
                     print(f"Failed to handle thread expiration for run {run.id} with error: {e}")



               
               if restarting_flag == False:
                  thread = self.client.beta.threads.retrieve(thread_id)
                  try:
                     for func_name, func_args, tool_call_id in function_details:
                        if tool_call_id in self.running_tools: # already running in a parallel thread
                           continue
                        log_readable_payload = func_name+"("+func_args+")"
                        try:
                           callback_closure = self._generate_callback_closure(run, thread, tool_call_id, function_details, run.metadata)
                           self.callback_closures[tool_call_id] = callback_closure
                        except Exception as e:
                           print(f"Failed to generate callback closure for run {run.id}, thread {thread.id}, tool_call_id {tool_call_id} with error: {e}")
                        self.running_tools[tool_call_id] = {"run_id": run.id, "thread_id": thread.id }

                        meta = run.metadata
                        primary_user = json.dumps({'user_id': meta.get('user_id', 'Unknown User ID'), 
                                    'user_name': meta.get('user_name', 'Unknown User')})
                        self.log_db_connector.insert_chat_history_row(datetime.datetime.now(), bot_id=self.bot_id, bot_name=self.bot_name, thread_id=thread_id,
                                                                     message_type='Tool Call', message_payload=log_readable_payload, 
                                                                     message_metadata={'tool_call_id':tool_call_id, 'func_name':func_name, 'func_args':func_args},
                                                                     channel_type=meta.get("channel_type", None), channel_name=meta.get("channel", None),
                                                                     primary_user=primary_user)
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
                              existing_location = f"./downloaded_files/{thread_id}/{file_id}"
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


                        if BotOsAssistantOpenAI.stream_mode == True and run.id in StreamingEventHandler.run_id_to_bot_assist:
                           function_name_pretty = re.sub(r'(_|^)([a-z])', lambda m: m.group(2).upper(), func_name).replace('_', '')
                           msg = f"🧰 Using tool: _{function_name_pretty}_..."
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
                           self.all_functions = BotOsAssistantOpenAI.all_functions_backup
                           if func_name in self.all_functions:
                              print('!! function was missing from self.all_functions, restored from backup, now its ok')
                           else:
                              print(f'!! function was missing from self.all_functions, restored from backup, still missing func: {func_name}, len of backup={len(BotOsAssistantOpenAI.all_functions_backup)}')
         
                        execute_function(func_name, func_args, self.all_functions, callback_closure,
                                       thread_id = thread_id, bot_id=self.bot_id)#, dispatch_task_callback=dispatch_task_callback)

                     continue
                  except Exception as e:
                     print(f"check_runs - requires action - exception:{e}")
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

               messages = self.client.beta.threads.messages.list(thread_id=thread_id)

               output_array = []
               latest_attachments = []

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
                        #   print('openai image_file tag present, fileid: ',file_id)
                           if file_id is not None and file_id not in latest_attachments:
                              latest_attachments.append({"file_id": file_id})
                        except Exception as e:
                           print('openai error parsing image attachment ',e)
                     if content.type == 'text':
                        try:
                           if output != '' and content.text.value == '!NO_RESPONSE_REQUIRED':
                               pass
                           else:
                              output += (content.text.value + "\n") if output else content.text.value
                        except:
                           pass
                  output = output.strip()  # Remove the trailing newline if it exists
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
                  
                  #  print(f"{self.bot_name} open_ai attachment info going into store files locally: {latest_attachments}", flush=True)
               files_in = self._store_files_locally(latest_attachments, thread_id)
               output = '\n'.join(reversed(output_array))
               if os.getenv('SHOW_COST', 'false').lower() == 'true':
                  model_name = os.getenv("OPENAI_MODEL_NAME", default="gpt-4o")
                  if model_name == "gpt-4o":
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
                  total_cost = (run.usage.prompt_tokens * input_cost) + (run.usage.completion_tokens * output_cost)
                  output += f'  `${total_cost:.4f}`'
          
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
               primary_user = json.dumps({'user_id': meta.get('user_id', 'Unknown User ID'), 
                              'user_name': meta.get('user_name', 'Unknown User')})
               try:
                  self.log_db_connector.insert_chat_history_row(datetime.datetime.now(), bot_id=self.bot_id, bot_name=self.bot_name, thread_id=thread_id, 
                                                                  message_type='Assistant Response', message_payload=output, message_metadata=message_metadata,
                                                                  tokens_in=run.usage.prompt_tokens, tokens_out=run.usage.completion_tokens, files=files_in,
                                                                  channel_type=meta.get("channel_type", None), channel_name=meta.get("channel", None),
                                                                  primary_user=primary_user)
               except:
                  pass
               threads_completed[thread_id] = run.completed_at

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
               self.client.beta.threads.runs.cancel(thread_id=thread_id, run_id=run.id)
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
               self.unposted_run_ids[thread_id].append(run.id)
               # check here to make sure correct thread_id is getting put on this...should be the input thread id
               self.add_message(BotOsInputMessage(thread_id=thread_id, msg='The run has expired, please resubmit the tool call(s).', metadata=meta))
               # Remove the current thread/run from the processing queue

               # Add the new thread/run to the active runs queue
               # self.active_runs_queue.append((thread_id, run.id))
               print(f"Run {run.id} cancelled before tool call, and agent notified to resubmit the tool call due to upcoming thread expiration.")
               return 


         #thread_id = None
      
      if thread_id in self.processing_runs:
         self.processing_runs.remove(thread_id)
      # put pending threads back on queue
      for thread_id in threads_still_pending:
         if thread_id not in self.active_runs:
             self.active_runs.append(thread_id)