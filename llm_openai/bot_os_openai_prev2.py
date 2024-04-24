import json
import os, uuid
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
      model_name = os.getenv("OPENAI_MODEL_NAME", default="gpt-4-1106-preview")
      print("-> OpenAI Model = ",model_name)
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
      my_tools = tools + [{"type": "code_interpreter"}, {"type": "retrieval"}]
      #my_tools = tools 
      #print(f'yoyo mytools {my_tools}')
      #logger.warn(f'yoyo mytools {my_tools}')
      self.my_tools = my_tools

      my_assistants = self.client.beta.assistants.list(
         order="desc",
         limit=20,
      )
      my_assistants = [a for a in my_assistants if a.name == name]

      # Check if files are available in OpenAI and remove the ones that aren't
      file_list = self.client.files.list()
      existing_file_ids = [file.id for file in file_list]
      valid_files = [file_id for file_id in files if file_id in existing_file_ids]
      missing_files = list(set(files) - set(valid_files))
      if missing_files:
         logger.warning(f"Missing files in OpenAI, they will be removed from the list: {missing_files}")
      files = valid_files  # Update the files list to only include valid files
      
      if len(my_assistants) == 0 and update_existing:
         self.assistant = self.client.beta.assistants.create(
            name=name,
            instructions=instructions,
            tools=my_tools, # type: ignore
            model=model_name,
           # file_ids=self._upload_files(files) #FixMe: what if the file contents change?
            file_ids=files
         )
      elif len(my_assistants) > 0:
         self.assistant = my_assistants[0]
         logger.warning(f'files {files}')
         if not isinstance(files, list) or not all(isinstance(file, str) for file in files):
            logger.warning("The 'files' parameter is expected to be a list of strings (List[str]), but the provided argument does not meet this criterion.")
         if update_existing and (
            self.assistant.instructions != instructions or \
            self.assistant.tools        != my_tools or \
            self.assistant.file_ids     != files or \
            self.assistant.model        != model_name):
            self.client.beta.assistants.update(self.assistant.id,
                                          instructions=instructions,
                                          tools=my_tools, # type: ignore
                                          model=model_name,
                                         file_ids=files 
                )
         
      logger.debug(f"BotOsAssistantOpenAI:__init__: assistant.id={self.assistant.id}")

   @staticmethod
   def load_by_name(name: str):
      return BotOsAssistantOpenAI(name, update_existing=False)

   def create_thread(self) -> str:
      logger.debug("BotOsAssistantOpenAI:create_thread") 
      return self.client.beta.threads.create().id

   def _upload_files(self, files):
      file_ids = []
      for f in files:
         # add handler to download from URL and save to temp file for upload then cleanup
         print("loading files")
         fo = open(f,"rb")
         file = self.client.files.create(file=fo, purpose="assistants")
         file_ids.append(file.id)
         self.file_storage[file.id] = f
      logger.debug(f"BotOsAssistantOpenAI:_upload_files - uploaded {len(file_ids)} files") 
      return file_ids
   
   def add_message(self, input_message:BotOsInputMessage):#thread_id:str, message:str, files):
      logger.debug("BotOsAssistantOpenAI:add_message") 
      thread_id = input_message.thread_id
      if thread_id is None:
         raise(Exception("thread_id is None"))
      thread = self.client.beta.threads.retrieve(thread_id)
      try:
         thread_message = self.client.beta.threads.messages.create(
            thread_id=thread_id, file_ids=self._upload_files(input_message.files), content=input_message.msg, 
            role="user", 
         )
      except Exception as e:
         logger.error(f"Thread message for {thread_id} creation failed or already running: {e}")
         try:
            thread_message = self.client.beta.threads.messages.create(
               thread_id=thread_id, content="Error on input submission: "+str(e), 
               role="user", 
            )
         except:
            return
      logger.debug(f"add_message - created {thread_message}")
      run = self.client.beta.threads.runs.create(
         thread_id=thread.id, assistant_id=self.assistant.id, metadata=input_message.metadata)
      self.thread_run_map[thread_id] = {"run": run.id, "completed_at": None}
      self.active_runs.append(thread_id)
      self.log_db_connector.insert_chat_history_row(datetime.datetime.now(), bot_id=self.bot_id, bot_name=self.bot_name, thread_id=thread_id, message_type='User Prompt', message_payload=input_message.msg, message_metadata=None)

   def _submit_tool_outputs(self, run_id, thread_id, tool_call_id, function_call_details, func_response):
      logger.debug(f"_submit_tool_outputs - {thread_id} {run_id} {tool_call_id} - {function_call_details} - {func_response}")

      new_response = func_response
      if function_call_details[0][0] == 'add_new_tools_to_bot':
         target_bot = json.loads(function_call_details[0][1]).get('bot_id',None)
         if target_bot is not None:
            my_assistants = self.client.beta.assistants.list(order="desc",limit=50)
            my_assistants = [a for a in my_assistants if a.name == target_bot]
            for assistant in my_assistants:
               bot_tools = None
               print(self.all_tools)
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

               bot_tools_array = bot_tools_array + _BOT_OS_BUILTIN_TOOLS + [{"type": "code_interpreter"}, {"type": "retrieval"}]

               self.client.beta.assistants.update(assistant.id,tools=bot_tools_array)
               
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
               my_assistants = self.client.beta.assistants.list(order="desc",limit=50)
               my_assistants = [a for a in my_assistants if a.name == target_bot]
               for assistant in my_assistants:
                  self.client.beta.assistants.update(assistant.id,instructions=new_instructions)
               logger.info(f"Bot instructions for {target_bot} updated: {new_instructions}")
               new_response.pop("new_instructions", None)

      if function_call_details[0][0] == 'add_bot_files':
         try:
            updated_files_list = func_response.get("current_files_list",None)
         except:
            updated_files_list = None
         if updated_files_list:
            target_bot = json.loads(function_call_details[0][1]).get('bot_id',None)
            if target_bot is not None:
               my_assistants = self.client.beta.assistants.list(order="desc",limit=50)
               my_assistants = [a for a in my_assistants if a.name == target_bot]
               for assistant in my_assistants:
                  self.client.beta.assistants.update(assistant.id, file_ids=updated_files_list)
               logger.info(f"Bot files for {target_bot} updated.")
               

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

   def _download_openai_file(self, file_id):
      logger.debug(f"BotOsAssistantOpenAI:download_openai_file - {file_id}")
      # Use the retrieve file contents API to get the file directly
      file_info = self.client.files.retrieve(file_id=file_id)
      file_contents = self.client.files.content(file_id=file_id)
 
      local_file_path = os.path.join("./downloaded_files", os.path.basename(file_info.filename))
      
      # Ensure the directory exists
      os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
      
      # Save the file contents locally
      file_contents.write_to_file(local_file_path)

      logger.debug(f"File downloaded: {local_file_path}")
      return local_file_path

   def _store_files_locally(self, file_ids):
      return [self._download_openai_file(file_id) for file_id in file_ids]
   
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

         logger.warning(f'validate_or_add_function, fn name={function_name}')
         try:
            available_functions_load = {}
            logger.warn(f"validate_or_add_function - function_name: {function_name}")
            fn_name = function_name.split('.')[-1] if '.' in function_name else function_name
            logger.warn(f"validate_or_add_function - fn_name: {fn_name}")
            module_path = "generated_modules."+fn_name
            logger.warn(f"validate_or_add_function - module_path: {module_path}")
            desc_func = "TOOL_FUNCTION_DESCRIPTION_"+fn_name.upper()
            logger.warn(f"validate_or_add_function - desc_func: {desc_func}")
            functs_func = fn_name.lower()+'_action_function_mapping'
            logger.warn(f"validate_or_add_function - functs_func: {functs_func}")
            module = __import__(module_path, fromlist=[desc_func, functs_func])
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
            logger.info(f"run.status {run.status}")
            print("run.status ",run.status)

            if run.status == "in_progress":
               threads_still_pending.append(thread_id)

            if run.status == "queued":
               threads_still_pending.append(thread_id)


            if run.status == "failed":
               logger.error(f"!!!!!!!!!! FAILED JOB, run.lasterror {run.last_error} !!!!!!!")
               # resubmit tool output if throttled
               #tools_to_rerun = {k: v for k, v in self.tool_completion_status[run.id].items() if v is not None}
               #self._run_tools(thread_id, run, tools_to_rerun) # type: ignore
               #self._submit_tool_outputs(run.id, thread_id, tool_call_id=None, function_call_details=self.tool_completion_status[run.id],
               #                          func_response=None)
               # Todo add more handling here to tell the user the thread failed

            if run.status == "expired":
               logger.error(f"!!!!!!!!!! EXPIRED JOB, run.lasterror {run.last_error} !!!!!!!")
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
                     if "files" in func_args_dict: # FixMe: find a better way to convert file_id back to stored file
                        files = json.loads(func_args_dict["files"])
                        from urllib.parse import quote
                        func_args_dict["files"] = json.dumps([f"file://{quote(self.file_storage.get(f['file_id']))}" for f in files])
                        func_args = json.dumps(func_args_dict)

                     self.validate_or_add_function(func_name)

                     execute_function(func_name, func_args, self.all_functions, callback_closure,
                                      thread_id = thread_id, bot_id=self.bot_id)#, dispatch_task_callback=dispatch_task_callback)

                  continue
               except Exception as e:
                  logger.error(f"check_runs - exception:{str(e)}")

            elif run.status == "completed" and run.completed_at != thread_run["completed_at"]:
               messages = self.client.beta.threads.messages.list(thread_id=thread_id)
               latest_message = messages.data[0]
               if latest_message == "image_file":
                  output = latest_message.content[0].image_file #type: ignore
               elif latest_message.content[0].type == 'text':
                  output = latest_message.content[0].text.value #type: ignore
               else:
                  logger.warn(f"!!!!!!!!!! WARNING: received unexpected content type: {latest_message.content[0]}!!!!!!!")
                  output = str(latest_message.content[0])
               if output != '!NO_RESPONSE_REQUIRED':
                  output += '  `'+"$"+str(round(run.usage.prompt_tokens/1000000*10+run.usage.completion_tokens/1000000*30,4))+'`'
                  event_callback(self.assistant.id, BotOsOutputMessage(thread_id=thread_id, 
                                                                     status=run.status, 
                                                                     output=output, 
                                                                     messages=messages, 
                                                                     files=self._store_files_locally(latest_message.file_ids),
                                                                     input_metadata=run.metadata))
                  self.log_db_connector.insert_chat_history_row(datetime.datetime.now(), bot_id=self.bot_id, bot_name=self.bot_name, thread_id=thread_id, message_type='Assistant Response', message_payload=output, message_metadata=None, tokens_in=run.usage.prompt_tokens, tokens_out=run.usage.completion_tokens)
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
            
