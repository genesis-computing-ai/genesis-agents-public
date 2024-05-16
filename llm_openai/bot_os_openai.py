import json
import os, uuid, re
from typing import TypedDict
from core.bot_os_assistant_base import BotOsAssistantInterface, execute_function
from openai import OpenAI
from collections import deque
import datetime
import logging
from core.bot_os_input import BotOsInputMessage, BotOsOutputMessage
from core.bot_os_defaults import _BOT_OS_BUILTIN_TOOLS
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.WARN, format='%(asctime)s - %(levelname)s - %(message)s')

def _get_function_details(run):
      function_details = []
      for tool_call in run.required_action.submit_tool_outputs.tool_calls:
         function_details.append(
            (tool_call.function.name, tool_call.function.arguments, tool_call.id)
         )
      return function_details





class BotOsAssistantOpenAI(BotOsAssistantInterface):
   def __init__(self, name:str, instructions:str, 
                tools:list[dict] = {}, available_functions={}, files=[], 
                update_existing=False, log_db_connector=None, bot_id='default_bot_id', bot_name='default_bot_name', all_tools:list[dict]={}, all_functions={},all_function_to_tool_map={}) -> None:
      logger.debug("BotOsAssistantOpenAI:__init__") 
      self.client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
      model_name = os.getenv("OPENAI_MODEL_NAME", default="gpt-4o")
    
      name = bot_id
      print("-> OpenAI Model == ",model_name)
      self.thread_run_map = {}
      self.active_runs = deque()
      self.bot_id = bot_id
      self.bot_name = bot_name
      self.file_storage = {}
      self.available_functions = available_functions
      self.all_tools = all_tools
      self.all_functions = all_functions
      self.all_function_to_tool_map = all_function_to_tool_map
      self.running_tools = {}
      self.tool_completion_status = {}
      self.log_db_connector = log_db_connector
      my_tools = tools + [{"type": "code_interpreter"}, {"type": "file_search"}]
      #my_tools = tools 
      #print(f'yoyo mytools {my_tools}')
      #logger.warn(f'yoyo mytools {my_tools}')
      self.my_tools = my_tools
         
      self.allowed_types_search = [".c", ".cs", ".cpp", ".doc", ".docx", ".html", ".java", ".json", ".md", ".pdf", ".php", ".pptx", ".py", ".rb", ".tex", ".txt", ".css", ".js", ".sh", ".ts"]
      self.allowed_types_code_i = [".c", ".cs", ".cpp", ".doc", ".docx", ".html", ".java", ".json", ".md", ".pdf", ".php", ".pptx", ".py", ".rb", ".tex", ".txt", ".css", ".js", ".sh", ".ts", ".csv", ".jpeg", ".jpg", ".gif", ".png", ".tar", ".xlsx", ".xml", ".zip"]
      self.run_meta_map = {}

      genbot_internal_project_and_schema = os.getenv('GENESIS_INTERNAL_DB_SCHEMA','None')
      self.genbot_internal_project_and_schema = genbot_internal_project_and_schema
      if genbot_internal_project_and_schema == 'None':
         print("ENV Variable GENESIS_INTERNAL_DB_SCHEMA is not set.")
      self.db_schema = genbot_internal_project_and_schema.split('.')
      self.internal_db_name = self.db_schema[0]
      self.internal_schema_name = self.db_schema[1]

      my_assistants = self.client.beta.assistants.list(
         order="desc"
      )
      my_assistants = [a for a in my_assistants if a.name == name]

      if len(my_assistants) == 0 and update_existing:
         vector_store_name = name + '_vectorstore'
         self.vector_store = self.create_vector_store(vector_store_name=vector_store_name, files=files)
         self.tool_resources = {"file_search": {"vector_store_ids": [self.vector_store]}}
         self.assistant = self.client.beta.assistants.create(
            name=name,
            instructions=instructions,
            tools=my_tools, # type: ignore
            model=model_name,
           # file_ids=self._upload_files(files) #FixMe: what if the file contents change?
            tool_resources=self.tool_resources
         )
      elif len(my_assistants) > 0:
         self.assistant = my_assistants[0]
         try:
            vector_store_id = self.assistant.tool_resources.file_search.vector_store_ids[0]
         except:
            vector_store_id = None
         if vector_store_id is not None:
            self.update_vector_store(vector_store_id=vector_store_id, files=files)
            self.tool_resources = self.assistant.tool_resources
         else:
            vector_store_name = name + '_vectorstore'
            self.vector_store = self.create_vector_store(vector_store_name=vector_store_name, files=files)
            self.tool_resources = {"file_search": {"vector_store_ids": [self.vector_store.id]}}

         self.client.beta.assistants.update(self.assistant.id,
                                          instructions=instructions,
                                          tools=my_tools, # type: ignore
                                          model=model_name,
                                          tool_resources=self.tool_resources
                )
         self.first_message = True
         
      logger.debug(f"BotOsAssistantOpenAI:__init__: assistant.id={self.assistant.id}")

   @staticmethod
   def load_by_name(name: str):
      return BotOsAssistantOpenAI(name, update_existing=False)

   def update_vector_store(self, vector_store_id: str, files: list=None, plain_files: list=None):

      #internal_stage =  f"{self.internal_db_name}.{self.internal_schema_name}.BOT_FILES_STAGE"

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

      for file in stage_files:
          # Read each file from the stage and save it to a local location
          try:
              # Assuming 'self' has an attribute 'snowflake_connector' which is an instance of the SnowflakeConnector class
            new_file_location = f"./downloaded_files/{self.bot_id}_BOT_DOCS/{file}"
            os.makedirs(f"./downloaded_files/{self.bot_id}_BOT_DOCS", exist_ok=True)
            contents = self.log_db_connector.read_file_from_stage(
                  database=self.internal_db_name,
                  schema=self.internal_schema_name,
                  stage='BOT_FILES_STAGE',
                  file_name=file,
                  thread_id=f'{self.bot_id}_BOT_DOCS',
                  return_contents=False
                  )
            if contents==file:
               local_file_path = new_file_location
               files_from_stage.append(local_file_path)
               logger.info(f"Successfully retrieved {file} from stage and saved to {new_file_location}")
          except Exception as e:
              logger.error(f"Failed to retrieve {file} from stage: {e}")
          
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


   def create_vector_store(self, vector_store_name: str, files: list=None, plain_files: list=None):
      # Create a vector store with the given name
      vector_store = self.client.beta.vector_stores.create(name=vector_store_name)
      
      return self.update_vector_store(vector_store.id, files, plain_files)

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
   
         print("loading files")
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
      logger.debug("BotOsAssistantOpenAI:add_message") 
      
      thread_id = input_message.thread_id
      if thread_id is None:
         raise(Exception("thread_id is None"))
      thread = self.client.beta.threads.retrieve(thread_id)
      #logger.warn(f"ADDING MESSAGE -- input thread_id: {thread_id} -> openai thread: {thread}")
      try:
         #logger.error("REMINDER: Update for message new files line 117 on botosopenai.py")
         print('... openai add_message before upload_files, input_message.files = ', input_message.files)
         file_ids, file_map = self._upload_files(input_message.files, thread_id=thread_id)
         print('... openai add_message file_id, file_map: ', file_ids, file_map)
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

         content = input_message.msg
         if file_map:
             content += "\n\nFile Name to Id Mappings:\n"
             for mapping in file_map:
                 content += f"- {mapping['file_name']}: {mapping['file_id']}\n"
         print('... openai add_message attachments: ', attachments)
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
                  logger.error(f"Extracted run_id: {run_id}")
                  self.client.beta.threads.runs.cancel(run_id=run_id, thread_id=thread_id)
                  logger.info(f"Cancelled run_id: {run_id}")
                  thread_message = self.client.beta.threads.messages.create(
                     thread_id=thread_id, attachments=attachments, content=content, 
                     role="user", 
                  )
               fixed = True
         except Exception as e:
            pass
         if not fixed:
            logger.error(f"Thread message for {thread_id} creation failed or already running: {e}")
            try:
               thread_message = self.client.beta.threads.messages.create(
                  thread_id=thread_id, content="Error on input submission: "+str(e), 
                  role="user", 
               )
            except:
               return
      #logger.debug(f"add_message - created {thread_message}")
      self.first_message = False 
      task_meta = input_message.metadata.pop('task_meta', None)
      run = self.client.beta.threads.runs.create(
         thread_id=thread.id, assistant_id=self.assistant.id, metadata=input_message.metadata)
      if task_meta is not None:
         self.run_meta_map[run.id]=task_meta
      self.thread_run_map[thread_id] = {"run": run.id, "completed_at": None}
      self.active_runs.append(thread_id)

      self.log_db_connector.insert_chat_history_row(datetime.datetime.now(), bot_id=self.bot_id, bot_name=self.bot_name, thread_id=thread_id, message_type='User Prompt', message_payload=input_message.msg, message_metadata=None, files=attachments)


   def _submit_tool_outputs(self, run_id, thread_id, tool_call_id, function_call_details, func_response):
     
      logger.debug(f"_submit_tool_outputs - {thread_id} {run_id} {tool_call_id} - {function_call_details} - {func_response}")

      new_response = func_response
 
      try:
         if function_call_details[0][0] == 'add_new_tools_to_bot' and func_response.get('success',False)==True:
            target_bot = json.loads(function_call_details[0][1]).get('bot_id',None)
            if target_bot is not None:
               my_assistants = self.client.beta.assistants.list(order="desc")
               my_assistants = [a for a in my_assistants if a.name == target_bot]
               for assistant in my_assistants:
                  bot_tools = None
                  #print(self.all_tools)
                  all_tools_for_bot = func_response.get('all_bot_tools', None)
                  if all_tools_for_bot is not None:
                     #print(all_tools_for_bot)
                     #print(self.all_function_to_tool_map)
                     bot_tools_array = []
                     for tool in all_tools_for_bot:
                        logger.warn(f'--> Calling validate_or_add_function on {tool} <---- ')
                        self.validate_or_add_function(tool)
                        tool_name = tool
                        if tool_name in self.all_function_to_tool_map:
                           for t in self.all_function_to_tool_map[tool_name]:
                              bot_tools_array.append(t)

                  new_instructions = assistant.instructions 
                  if "snowflake_stage_tools" in all_tools_for_bot and 'make_baby_bot' in all_tools_for_bot:        
                        new_instructions += f"\nYour Internal Files Stage for bots is at snowflake stage: {self.genbot_internal_project_and_schema}.BOT_FILES_STAGE"
                        print("Instruction for target bot updated with Internal Files Stage location.")
                  bot_tools_array = bot_tools_array + _BOT_OS_BUILTIN_TOOLS + [{"type": "code_interpreter"}, {"type": "file_search"}]

                  self.client.beta.assistants.update(assistant.id,tools=bot_tools_array, instructions=new_instructions)
                  
                  # handle looking for newly created tools, import them and add on the fly, also do that in execute function if not already there 

                  ### PLAN HERE: TODO ##
                  ## have available_functions be in multibot main, and have all available functions pre-mapped, and make sure new ones somehow are added to it, or add them here somehow
                  ## have a function here that assembles the tools object (but no links to the actual functions)
                  #self.client.beta.assistants.update(assistant.id,tools=bot_tools)
                  
               logger.info(f"Bot tools for {target_bot} updated.")
         if function_call_details[0][0] == 'update_bot_instructions':
            new_instructions = func_response.get("new_instructions",None)
            if new_instructions:
               target_bot = json.loads(function_call_details[0][1]).get('bot_id',None)
               if target_bot is not None:
                  my_assistants = self.client.beta.assistants.list(order="desc")
                  my_assistants = [a for a in my_assistants if a.name == target_bot]
                  for assistant in my_assistants:
                     self.client.beta.assistants.update(assistant.id,instructions=new_instructions)
                  logger.info(f"Bot instructions for {target_bot} updated: {new_instructions}")
                  new_response.pop("new_instructions", None)

         if function_call_details[0][0] == 'add_bot_files':
         #  raise ('need to update bot_os_openai.py line 215 for new files structure with v2')
            try:
               updated_files_list = func_response.get("current_files_list",None)
            except:
               updated_files_list = None
            if updated_files_list:
               target_bot = json.loads(function_call_details[0][1]).get('bot_id',None)
               if target_bot is not None:
                  my_assistants = self.client.beta.assistants.list(order="desc")
                  my_assistants = [a for a in my_assistants if a.name == target_bot]
                  assistant_zero = my_assistants[0]

                  try:
                     vector_store_id = assistant_zero.tool_resources.file_search.vector_store_ids[0]
                  except:
                     vector_store_id = None
                  if vector_store_id is not None:
                     self.update_vector_store(vector_store_id=vector_store_id, files=None, plain_files=updated_files_list)
                     tool_resources = assistant_zero.tool_resources
                  else:
                     vector_store_name = json.loads(function_call_details[0][1]).get('bot_name',None) + '_vectorstore'
                     vector_store = self.create_vector_store(vector_store_name=vector_store_name, files=None, plain_files=updated_files_list)
                     tool_resources = {"file_search": {"vector_store_ids": [vector_store.id]}}
                  self.client.beta.assistants.update(assistant_zero.id, tool_resources=tool_resources)

                  logger.info(f"Bot files for {target_bot} updated.")
      except Exception as e:
         print('openai submit_tool_outputs error to tool checking: ', e)    

      if tool_call_id is not None: # in case this is a resubmit
         self.tool_completion_status[run_id][tool_call_id] = new_response

      # check if all parallel tool calls are complete
      if any(value is None for value in self.tool_completion_status[run_id].values()):
         logger.info(f"_submit_tool_outputs - {thread_id} {run_id} {tool_call_id} complete. waiting for {sum(value is None for value in self.tool_completion_status[run_id].values())}")
         return
      
      # now pacakge up the responses together
      tool_outputs = [{'tool_call_id': k, 'output': str(v)} for k, v in self.tool_completion_status[run_id].items()]
      try:
         updated_run = self.client.beta.threads.runs.submit_tool_outputs(
            thread_id=thread_id,
            run_id=run_id,
            tool_outputs=tool_outputs # type: ignore
         )
         logger.debug(f"_submit_tool_outputs - {updated_run}")
         for tool_output in tool_outputs:
            self.log_db_connector.insert_chat_history_row(datetime.datetime.now(), bot_id=self.bot_id, bot_name=self.bot_name, thread_id=thread_id, message_type='Tool Output', message_payload=tool_output['output'], message_metadata={'tool_call_id':tool_output['tool_call_id']})

      except Exception as e:
         logger.error(f"submit_tool_outputs - caught exception: {e}")

   def _generate_callback_closure(self, run, thread, tool_call_id, function_details):
      def callback_closure(func_response):  # FixMe: need to break out as a generate closure so tool_call_id isn't copied
         try:                     
            del self.running_tools[tool_call_id]
         except Exception as e:
            logger.error(f"callback_closure - tool call already deleted - caught exception: {e}")
         try:
            self._submit_tool_outputs(run.id, thread.id, tool_call_id, function_details, func_response)
         except Exception as e:
            error_string = f"callback_closure - _submit_tool_outputs - caught exception: {e}"
            logger.error(error_string)
            self._submit_tool_outputs(run.id, thread.id, tool_call_id, function_details, error_string)
      return callback_closure

   def _download_openai_file(self, file_id, thread_id):
      logger.debug(f"BotOsAssistantOpenAI:download_openai_file - {file_id}")
      # Use the retrieve file contents API to get the file directly

      try:
         print(f"{self.bot_name} open_ai download_file file_id: {file_id}", flush=True)

         try:
            file_id = file_id.get('file_id',None)
         except:
            try:
               file_id = file_id.file_id
            except: 
               pass

         file_info = self.client.files.retrieve(file_id=file_id)
         print(f"{self.bot_name} open_ai download_file id: {file_info.id} name: {file_info.filename}", flush=True)
         file_contents = self.client.files.content(file_id=file_id)

         try:         
            print(f"{self.bot_name} open_ai download_file file_id: {file_id} contents_len: {len(file_contents.content)}", flush=True)
         except Exception as e:
             print(f"{self.bot_name} open_ai download_file file_id: {file_id} ERROR couldn't get file length: {e}", flush=True)
  
         local_file_path = os.path.join(f"./downloaded_files/{thread_id}/", os.path.basename(file_info.filename))
         print(f"{self.bot_name} open_ai download_file file_id: {file_id} localpath: {local_file_path}", flush=True)
         
         # Ensure the directory exists
         os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
         
         
         # Save the file contents locally
         file_contents.write_to_file(local_file_path)
       
         print(f"{self.bot_name} open_ai download_file wrote file: {file_id} to localpath: {local_file_path}", flush=True)
       
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
      print(f"{self.bot_name} open_ai store_files_locally, file_ids: {file_ids}", flush=True)
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
         logger.warning(f'validate_or_add_function, fn name={function_name}')
         try:
            available_functions_load = {}
            logger.warn(f"validate_or_add_function - function_name: {function_name}")
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
               logger.warn(f"validate_or_add_function - module {module_path} does not need to be imported, proceeding...")
               return True
            logger.warn(f"validate_or_add_function - module: {module}")
            # here's how to get the function for generated things even new ones... 
            func = [getattr(module, desc_func)]
            logger.warn(f"validate_or_add_function - func: {func}")
            self.all_tools.extend(func)
            self.all_function_to_tool_map[fn_name]=func
            logger.warn(f"validate_or_add_function - all_function_to_tool_map[{fn_name}]: {func}")
            #self.function_to_tool_map[function_name]=func
            func_af = getattr(module, functs_func)
            logger.warn(f"validate_or_add_function - func_af: {func_af}")
            available_functions_load.update(func_af)
            logger.warn(f"validate_or_add_function - available_functions_load: {available_functions_load}")

            for name, full_func_name in available_functions_load.items():
               logger.warn(f"validate_or_add_function - Looping through available_functions_load - name: {name}, full_func_name: {full_func_name}")
               module2 = __import__(module_path, fromlist=[fn_name])
               logger.warn(f"validate_or_add_function - module2: {module2}")
               func = getattr(module2, fn_name)
               logger.warn(f"validate_or_add_function - Imported function: {func}")
               self.all_functions[name] = func
               logger.warn(f"validate_or_add_function - all_functions[{name}]: {func}")
         except:
            logger.warning(f"Function '{function_name}' is not in all_functions. Please add it before proceeding.")

         logger.info(f"Likely newly generated function '{function_name}' added all_functions.")
         return False

   
   def check_runs(self, event_callback):
      logger.debug("BotOsAssistantOpenAI:check_runs") 

      threads_completed = {}
      threads_still_pending = []

#      for thread_id in self.thread_run_map:
      try:
         thread_id = self.active_runs.popleft()
      except IndexError:
         thread_id = None
      while thread_id is not None:
         thread_run = self.thread_run_map[thread_id]
         if thread_run["completed_at"] is None:

            run = self.client.beta.threads.runs.retrieve(thread_id = thread_id, 
                                                      run_id = thread_run["run"])
            #logger.info(f"run.status {run.status} Thread: {thread_id}")
            print(f"{self.bot_name} open_ai check_runs ",run.status," thread: ", thread_id, flush=True)

            current_time = datetime.datetime.now()
            run_duration = (current_time - datetime.datetime.fromtimestamp(run.created_at)).total_seconds()
            if run.status == "in_progress":
               threads_still_pending.append(thread_id)
               try:
                  # Corrected to ensure it only calls after each minute beyond the first 60 seconds
                  if run_duration > 60 and run_duration % 60 < 2:  # Check if run duration is beyond 60 seconds and within the first 5 seconds of each subsequent minute
                     event_callback(self.assistant.id, BotOsOutputMessage(thread_id=thread_id, 
                                                                           status=run.status, 
                                                                           output=f"_still running..._ {run.id} has been active for {int(run_duration // 60)} minute(s)...", 
                                                                           messages=None, 
                                                                           input_metadata=run.metadata))
               except:
                  pass

            if run.status == "queued":
               threads_still_pending.append(thread_id)
               try:
                  if run_duration > 60 and run_duration % 60 < 2:  # Check if run duration is beyond 60 seconds and within the first 5 seconds of each subsequent minute
                     event_callback(self.assistant.id, BotOsOutputMessage(thread_id=thread_id, 
                                                                           status=run.status, 
                                                                           output=f"_still running..._ {run.id} has been active for {int(run_duration // 60)} minute(s)...", 
                                                                           messages=None, 
                                                                           input_metadata=run.metadata))
               except:
                  pass


            if run.status == "failed":
               logger.error(f"!!!!!!!!!! FAILED JOB, run.lasterror {run.last_error} !!!!!!!")
               # resubmit tool output if throttled
               #tools_to_rerun = {k: v for k, v in self.tool_completion_status[run.id].items() if v is not None}
               #self._run_tools(thread_id, run, tools_to_rerun) # type: ignore
               #self._submit_tool_outputs(run.id, thread_id, tool_call_id=None, function_call_details=self.tool_completion_status[run.id],
               #                          func_response=None)
               # Todo add more handling here to tell the user the thread failed
               output = f"!!! Error from OpenAI, run.lasterror {run.last_error} !!!"
               event_callback(self.assistant.id, BotOsOutputMessage(thread_id=thread_id, 
                                                                     status=run.status, 
                                                                     output=output, 
                                                                     messages=None, 
                                                                     input_metadata=run.metadata))
               self.log_db_connector.insert_chat_history_row(datetime.datetime.now(), bot_id=self.bot_id, bot_name=self.bot_name, thread_id=thread_id, message_type='Assistant Response', message_payload=output, message_metadata=None, tokens_in=0, tokens_out=0)
               threads_completed[thread_id] = run.completed_at


            if run.status == "expired":
               logger.error(f"!!!!!!!!!! EXPIRED JOB, run.lasterror {run.last_error} !!!!!!!")
               output = "!!! OpenAI run expired !!!"
               event_callback(self.assistant.id, BotOsOutputMessage(thread_id=thread_id, 
                                                                     status=run.status, 
                                                                     output=output, 
                                                                     messages=None, 
                                                                     input_metadata=run.metadata))
               #del threads_completed[thread_id] 
               # Todo add more handling here to tell the user the thread failed

            if run.status == "requires_action":
               function_details = _get_function_details(run)
               parallel_tool_call_ids = [f[2] for f in function_details]
               self.tool_completion_status[run.id] = {key: None for key in parallel_tool_call_ids} # need to submit completed parallel calls together
               thread = self.client.beta.threads.retrieve(thread_id)
               try:
                  for func_name, func_args, tool_call_id in function_details:
                     if tool_call_id in self.running_tools: # already running in a parallel thread
                        continue
                     log_readable_payload = func_name+"("+func_args+")"
                     callback_closure = self._generate_callback_closure(run, thread, tool_call_id, function_details)
                     self.running_tools[tool_call_id] = {"run_id": run.id, "thread_id": thread.id }
                     self.log_db_connector.insert_chat_history_row(datetime.datetime.now(), bot_id=self.bot_id, bot_name=self.bot_name, thread_id=thread_id, message_type='Tool Call', message_payload=log_readable_payload, message_metadata={'tool_call_id':tool_call_id, 'func_name':func_name, 'func_args':func_args})
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

                     execute_function(func_name, func_args, self.all_functions, callback_closure,
                                      thread_id = thread_id, bot_id=self.bot_id)#, dispatch_task_callback=dispatch_task_callback)

                  continue
               except Exception as e:
                  logger.error(f"check_runs - exception:{str(e)}")

            elif run.status == "completed" and run.completed_at != thread_run["completed_at"]:
               messages = self.client.beta.threads.messages.list(thread_id=thread_id)
               latest_message = messages.data[0]
               latest_attachments = latest_message.attachments

               print(f"{self.bot_name} open_ai response full content: {latest_message.content}", flush=True)

               print(f"{self.bot_name} open_ai response attachment info: {latest_message.attachments}", flush=True)
               output = ""
               for content in latest_message.content:
                   if content.type == 'image_file':
                     try:
                        file_id = content.image_file.file_id if hasattr(content.image_file, 'file_id') else None
                        print('openai image_file tag present, fileid: ',file_id)
                        if file_id is not None and file_id not in latest_attachments:
                           latest_attachments.append({"file_id": file_id})
                     except Exception as e:
                        print('openai error parsing image attachment ',e)
                   if content.type == 'text':
                       output += (content.text.value + "\n") if output else content.text.value
               output = output.strip()  # Remove the trailing newline if it exists
                #if output != '!NO_RESPONSE_REQUIRED':
               if True:
                  if os.getenv('SHOW_COST', 'false').lower() == 'true':
                     output += '  `'+"$"+str(round(run.usage.prompt_tokens/1000000*10+run.usage.completion_tokens/1000000*30,4))+'`'
                  meta_prime = self.run_meta_map.get(run.id, None)
                  if meta_prime is not None:
                     meta = meta_prime
                  else:
                     meta = run.metadata
                  print(f"{self.bot_name} open_ai attachment info going into store files locally: {latest_attachments}", flush=True)
                  files_in = self._store_files_locally(latest_attachments, thread_id)
                  print(f"{self.bot_name} open_ai output of store files locally {files_in}")
                  event_callback(self.assistant.id, BotOsOutputMessage(thread_id=thread_id, 
                                                                     status=run.status, 
                                                                     output=output, 
                                                                     messages=messages, 
                                                                     # UPDATE THIS FOR LOCAL FILE DOWNLOAD 
                                                                     files=files_in,
                                                                     input_metadata=meta))
                  try:
                     message_metadata = str(latest_message.content)
                  except:
                     message_metadata = "!error converting content to string"
                  self.log_db_connector.insert_chat_history_row(datetime.datetime.now(), bot_id=self.bot_id, bot_name=self.bot_name, thread_id=thread_id, message_type='Assistant Response', message_payload=output, message_metadata=message_metadata, tokens_in=run.usage.prompt_tokens, tokens_out=run.usage.completion_tokens, files=files_in)
               threads_completed[thread_id] = run.completed_at

            else:
               logger.debug(f"check_runs - {thread_id} - {run.status} - {run.completed_at} - {thread_run['completed_at']}")


            # record completed runs.  FixMe: maybe we should rmeove from the map at some point?
            for thread_id in threads_completed:
               self.thread_run_map[thread_id]["completed_at"] = threads_completed[thread_id]

         # get next run to check       
         try:
            thread_id = self.active_runs.popleft()
         except IndexError:
            thread_id = None
            
      # put pending threads back on queue
      for thread_id in threads_still_pending:
         self.active_runs.append(thread_id)
            
