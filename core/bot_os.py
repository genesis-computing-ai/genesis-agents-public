import datetime
import random
import re
import os
import threading
from core.bot_os_corpus import FileCorpus
from core.bot_os_input import BotOsInputAdapter, BotOsInputMessage, BotOsOutputMessage
from llm_openai.bot_os_openai import BotOsAssistantOpenAI
#from bot_os_reka import BotOsAssistantReka
from core.bot_os_reminders import RemindersTest
import schema_explorer.embeddings_index_handler as embeddings_handler
from core.bot_os_defaults import _BOT_OS_BUILTIN_TOOLS
import pickle

import logging
import json

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.WARN, format='%(asctime)s - %(levelname)s - %(message)s')


class BotOsThread:
    def __init__(self, asistant_implementaion, input_adapter, thread_id = None) -> None:
        self.assistant_impl = asistant_implementaion
        if thread_id == None:
            self.thread_id      = asistant_implementaion.create_thread()
        else:
            self.thread_id = thread_id
        self.input_adapter  = input_adapter
        self.input_adapter.thread_id = self.thread_id
        self.validated      = False

    def add_message(self, message:BotOsInputMessage):
        #logger.debug("BotOsThread:add message")
        ret = self.assistant_impl.add_message(message)
        if ret == False:
            print('thread add_message: false return, run already going')
            return ret

    def handle_response(self, session_id:str, output_message:BotOsOutputMessage):
        logger.debug("BotOsThread:handle_response")
        in_thread = output_message.input_metadata.get('input_thread',None)
        in_uuid = output_message.input_metadata.get('input_uuid',None)
        task_meta = output_message.input_metadata.get('task_meta',None)
        self.input_adapter.handle_response(session_id, output_message,
         in_thread=in_thread, in_uuid=in_uuid, task_meta=task_meta)



def _get_future_datetime(delta_string:str) -> datetime.datetime:
        # Regular expression to extract number and time unit from the string
        match = re.match(r"(\d+)\s*(day|hour|minute|second)s?", delta_string, re.I)
        if not match:
            raise ValueError("Invalid time delta format")

        quantity, unit = match.groups()
        quantity = int(quantity)

        # Map unit to the corresponding keyword argument for timedelta
        unit_kwargs = {
            'day': {'days': quantity},
            'hour': {'hours': quantity},
            'minute': {'minutes': quantity},
            'second': {'seconds': quantity},
        }.get(unit.lower())

        if unit_kwargs is None:
            raise ValueError("Unsupported time unit")

        # Calculate the future datetime
        future_datetime = datetime.datetime.now() + datetime.timedelta(**unit_kwargs)
        return future_datetime

class BotOsSession:
    last_annoy_refresh = datetime.datetime.now() 
    refresh_lock = False
    clear_access_cache = False 
    def __init__(self, session_name: str, instructions=None, 
                 validation_instructions=None,
                 tools=None, available_functions=None, 
                 assistant_implementation=None,
                 reminder_implementation=RemindersTest,
                 file_corpus: FileCorpus = None,  # Updated to use FileCorpus type
                 knowledgebase_implementation=None,
                 log_db_connector=None,
                 input_adapters:list[BotOsInputAdapter]=[], 
                 update_existing=False,
                 bot_id="default_bot_id",
                 bot_name="default_bot_name",
                 all_tools=None, all_functions=None, all_function_to_tool_map=None,
                 stream_mode=False
                 ):
        
        BotOsAssistantOpenAI.stream_mode = stream_mode
        self.session_name = session_name
        
        self.task_test_mode = os.getenv('TEST_TASK_MODE', 'false').lower()=='true'

        if tools is None:
            self.tools = _BOT_OS_BUILTIN_TOOLS
        else:
            self.tools = tools + _BOT_OS_BUILTIN_TOOLS
        if available_functions is None:
            self.available_functions = {}
        else:
            self.available_functions = available_functions
        self.available_functions["_add_task"] = self.add_task
        self.available_functions["_mark_task_completed"] = self._mark_task_completed
        self.bot_name = bot_name
        if all_tools is None:
            all_tools = []
        all_tools = all_tools + _BOT_OS_BUILTIN_TOOLS

        if all_functions is None:
            all_functions = {}
        all_functions["_add_task"] = self.add_task
        all_functions["_mark_task_completed"] = self._mark_task_completed
        
        if all_function_to_tool_map is None:
            all_function_to_tool_map = {}
        all_function_to_tool_map["bot_os"] = _BOT_OS_BUILTIN_TOOLS

        if reminder_implementation is None:
            self.reminder_impl = None
        else:
            self.reminder_impl = reminder_implementation(self._reminder_callback)  #type: ignore
            self.available_functions["_add_reminder"] = self._add_reminder
            all_functions["_add_reminder"] = self._add_reminder

        self.input_adapters = input_adapters
        self.threads = {}
        self.instructions = instructions
        self.validation_instructions = validation_instructions
        if assistant_implementation is None:
            assistant_implementation = BotOsAssistantOpenAI
      #  logger.warn(f"Files: {file_corpus}")
        self.assistant_impl = assistant_implementation(session_name, instructions, self.tools, available_functions=self.available_functions, files=file_corpus,
                                                     update_existing=update_existing, log_db_connector=log_db_connector, bot_id=bot_id, bot_name=bot_name, all_tools=all_tools, all_functions=all_functions, all_function_to_tool_map=all_function_to_tool_map)
        self.runs = {}
        self.log_db_connector = log_db_connector
        self.knowledge_impl = knowledgebase_implementation
        self.available_functions["_store_memory"] = self.knowledge_impl.store_memory #type: ignore
        self.lock = threading.Lock()
        self.tasks = []
        self.current_task_index = 0
        self.in_to_out_thread_map = {}
        self.out_to_in_thread_map = {}

        self.next_messages = []
        self.bot_id = bot_id

        sanitized_bot_id = re.sub(r'[^a-zA-Z0-9]', '', self.bot_id)
        thread_maps_filename = f'./thread_maps_{sanitized_bot_id}.pickle'
        if os.path.exists(thread_maps_filename):
            with open(thread_maps_filename, 'rb') as handle:
                maps = pickle.load(handle)
                self.in_to_out_thread_map = maps.get('in_to_out', {})
                self.out_to_in_thread_map = maps.get('out_to_in', {})

    def create_thread(self, input_adapter) -> str:

        logger.debug("create thread")
        thread = BotOsThread(self.assistant_impl, input_adapter)
        self.threads[thread.thread_id] = thread
        return thread.thread_id
 
    def _retrieve_memories(self, msg:str) -> str:
        user_memories = self.knowledge_impl.find_memory(msg, scope="user_preferences")
        gen_memories  = self.knowledge_impl.find_memory(msg, scope="general")

        mem = ""
        if len(user_memories) > 0:
            mem += f". Here are a few user preferences from your knowledge base to consider: {'. '.join(user_memories[:3])}. Do not store these in your knowledge base."
        if len(gen_memories) > 0:
            mem += f". Here are a few general memories from your knowledge base to consider: {'. '.join(gen_memories[:3])}. Do not store these in your knowledge base."
        return mem

    def add_message(self, input_message:BotOsInputMessage):# thread_id:str, message:str, files=[]):
 
        if input_message.thread_id not in self.threads:
            print(f"{self.bot_name} bot_os add_message new_thread for {input_message.thread_id} not found in existing threads.")
            thread = BotOsThread(self.assistant_impl, self.input_adapters[0], thread_id=input_message.thread_id)
            self.threads[input_message.thread_id] = thread
        else:
            thread = self.threads[input_message.thread_id]
        print(f"{self.bot_name} bot_os add_message, len={len(input_message.msg)}", flush=True)
        #print(f"add_message: {self.bot_id} - {input_message.msg} size:{len(input_message.msg)}")
        if '!reflect' in input_message.msg.lower():
            input_message.metadata["genesis_reflect"] = "True"

        # input_message - get slack User id
        # self.log_db_connector - snowflake access?
        # add 1 minute cycle to check 
        user_id = input_message.metadata.get('user_id',None)
        if user_id is not None: 
            print(f"{self.bot_name} bot_os add_message access check for {self.bot_name} slack user: {user_id}", flush=True)
            input_message.metadata["user_authorized"] = 'TRUE'
            input_message.metadata["response_authorized"] = 'TRUE'
            if self.assistant_impl.user_allow_cache.get(user_id,False)==False:
                slack_user_access = self.log_db_connector.db_get_bot_access(self.bot_id).get('slack_user_allow')
                if slack_user_access is not None:
                    allow_list = json.loads(slack_user_access)
                    if user_id not in allow_list:
                        input_message.metadata["user_authorized"] = 'FALSE'
                        if input_message.metadata.get("dm_flag",'FALSE')=='TRUE' or input_message.metadata.get("tagged_flag",'FALSE')=='TRUE':
                            # add check for tagged or DMed otherwise process the message but tell it not to respond, and set input metadata to not respond and check that later at response time
                            user_name = input_message.metadata.get('user_name',None)
                            if len(allow_list) == 1 and allow_list[0] == "!BLOCK_ALL":
                                input_message.msg = f'ERROR -- Access to this bot denied. User {user_name} can not interact with this bot because it is set to not allow ANY users on Slack to interact with it. Please politely tell the user to contact their Genesis Bots Administrator and ask them to ask the Eve bot to add their slack ID which is {user_id} added to the list of users this bot, bot_id {self.bot_id} can talk to.'    
                            else:
                                input_message.msg = f'ERROR -- Access to this bot denied. User {user_name} is not on the list of users allowed to interact with this bot.  Please politely tell the user to contact their Genesis Bots Administrator and ask them to ask the Eve bot to add their slack ID which is {user_id} added to the list of users this bot, bot_id {self.bot_id} can talk to.'
                        else:
                            input_message.metadata["response_authorized"] = 'FALSE'
                if input_message.metadata["user_authorized"] == 'TRUE':
                    self.assistant_impl.user_allow_cache[user_id] = True

        ret = thread.add_message(input_message)
        if ret == False:
            print('bot os session add false - thread already running')
            return False
        #logger.debug(f'added message {input_message.msg}')

    def _validate_response(self, session_id:str, output_message:BotOsOutputMessage): #thread_id:str, status:str, output:str, messages:str, attachments:list):
        thread = self.threads[output_message.thread_id]
        if output_message.status == "completed" and "genesis_reflect" in output_message.input_metadata and output_message.output.find("!COMPLETE") == -1 and output_message.output.find("!NEED_INPUT") == -1 and \
            output_message.output != '!COMPLETE' and output_message.output != '!NEED_INPUT' :
          #  print(f'{self.bot_id} ****needs review: ',output_message.output)
            self.next_messages.append(BotOsInputMessage(thread_id=output_message.thread_id, 
                                                        msg=self.validation_instructions + self._retrieve_memories(output_message.output), 
                                                        metadata=output_message.input_metadata))
        else:
#            if not self.task_test_mode:
#//                txt = output_message.output[:50]
#            else:
#                txt = output_message.output
#            if len(txt) == 50:
#                txt += '...'
            try:
                print(f'{self.bot_name} bot_os response, len={len(output_message.output)}', flush=True)
                if len(output_message.output) == 0:
                    pass
            except:
                pass
        thread.handle_response(session_id, output_message )

    def execute(self):
        #self._health_check()

        #print('execute ',self.session_name)
      #  if random.randint(0, 20) == 0:
      #      self._check_reminders()
      #      self._check_task_list()

        # Execute validating messages

        if hasattr(self.assistant_impl, 'clear_access_cache') and self.assistant_impl.clear_access_cache:
            BotOsSession.clear_access_cache = True
            self.assistant_impl.clear_access_cache = False

        if self.next_messages:
            for message in self.next_messages:
                ret = self.add_message(message)
            self.next_messages.clear()

        self.assistant_impl.check_runs(self._validate_response)

        for a in self.input_adapters:

            input_message = a.get_input(thread_map=self.in_to_out_thread_map,active=self.assistant_impl.is_active(), processing= self.assistant_impl.is_processing_runs(),done_map=self.assistant_impl.get_done_map())
            if input_message is None or input_message.msg == "":
                continue
            # populate map
            #out_thread = self.in_to_out_thread_map.get(input_message.thread_id,None)
            
            out_thread = self.in_to_out_thread_map.get(input_message.thread_id,None)
         
            if out_thread is None:
               # logger.error(f"NO Map to Out thread ... making new one for ->> In Thead {input_message.thread_id}")
                out_thread = self.create_thread(a)
                if input_message.thread_id is None:
                    input_message.thread_id = out_thread
                self.in_to_out_thread_map[input_message.thread_id] = out_thread
                self.out_to_in_thread_map[out_thread] = input_message.thread_id
                # Save the out_to_in_thread_map to a file
                sanitized_bot_id = re.sub(r'[^a-zA-Z0-9]', '', self.bot_id)
                with open(f'./thread_maps_{sanitized_bot_id}.pickle', 'wb') as handle:
                    pickle.dump({'out_to_in': self.out_to_in_thread_map, 'in_to_out': self.in_to_out_thread_map}, handle, protocol=pickle.HIGHEST_PROTOCOL)

                if os.getenv("USE_KNOWLEDGE", "false").lower() == 'true':
                    primary_user = json.dumps({'user_id': input_message.metadata.get('user_id', 'Unknown User ID'), 
                                               'user_name': input_message.metadata.get('user_name', 'Unknown User')})
                    knowledge = self.log_db_connector.extract_knowledge(primary_user, self.bot_id)
                    if knowledge:
                        input_message.msg = f'''NOTE--Here are some things you know about this user from previous interactions, that may be helpful to this conversation:
                                           {knowledge['THREAD_SUMMARY']}\n\n''' + input_message.msg

           # logger.error(f"Out Thread {out_thread} ->> In Thead {input_message.thread_id}")
           
           # input_message.metadata["input_thread"] = input_message.thread_id
           # input_message.metadata["input_uuid"] = input_message.input_uuid
            input_message.thread_id = out_thread
            
            if input_message is None or input_message.msg == "":
                continue
            ret = self.add_message(input_message)
            if ret == False and input_message is not None:
                is_bot = input_message.metadata.get('is_bot','TRUE')
                if is_bot == 'FALSE':
                    print('bot os message from human - thread already running - put back on queue..')
                    try:
                        print(input_message.metadata['event_ts'])
                        a.add_back_event(input_message.metadata['event_ts'])
                    except:
                        pass
                else:
                    print('bot os message from bot - thread already running - put back on queue..')
                    try:
                        print(input_message.metadata['event_ts'])
                        a.add_back_event(input_message.metadata['event_ts'])
                    except:
                        pass                   

            logger.debug("execute completed")

        current_time = datetime.datetime.now()
        if (current_time - BotOsSession.last_annoy_refresh).total_seconds() > 120 and not BotOsSession.refresh_lock:
            BotOsSession.refresh_lock = True
            BotOsSession.last_annoy_refresh = current_time 
            if current_time == BotOsSession.last_annoy_refresh:
                self._refresh_cached_annoy()
            BotOsSession.last_annoy_refresh = current_time 
            BotOsSession.refresh_lock = False
      
    def _refresh_cached_annoy(self):
        table = self.knowledge_impl.meta_database_connector.metadata_table_name   
        embeddings_handler.load_or_create_embeddings_index(table, refresh=True)

    def _reminder_callback(self, message:str):
        logger.info(f"reminder_callback - {message}")

    def _check_reminders(self):
        if self.reminder_impl is None:
            return
        
        with self.lock:
            reminders = self.reminder_impl.check_reminders(current_time=datetime.datetime.now())
            logger.info(f"_check_reminders - {len(reminders)} reminders")
            logger.info(f"_check_reminders - {reminders}")

            for r in reminders:
                self.reminder_impl.mark_reminder_completed(reminder_id=r["id"]) #FixMe: this should be done later instead when AI is confirmed completion
                self.add_message(BotOsInputMessage(thread_id=r["thread_id"], msg=f'THIS IS AN AUTOMATED MESSAGE FROM THE REMINDER MONITORING SYSTEM -- A reminder just came due. Please take the needed action, or inform the user. id:{r["id"]}, message:{r["text"]}'))
    
    def _add_reminder(self, task_to_remember:str, due_date_delta:str, is_recurring:bool=False, frequency = None,
                      thread_id = "") -> dict:
        logger.warn(f"_add_reminder - {thread_id} - {task_to_remember} - {due_date_delta}")

        if self.reminder_impl is None:
            raise(Exception("no reminder system defined"))
        due_date = _get_future_datetime(due_date_delta)
        return self.reminder_impl.add_reminder(task_to_remember, due_date=due_date, is_recurring=is_recurring, frequency=frequency,
                                               thread_id = thread_id)
                                        #completion_action={}
        
    # this will need to be exposed as a tool
    def _mark_reminder_completed(self, reminder_id:str):
        if self.reminder_impl is None:
            raise(Exception("reminder system not defined"))
        self.reminder_impl.mark_reminder_completed(reminder_id)
    
    # FixMe: breakout to a pluggable, persistent task module
    def add_task(self, task:str, input_adapter:BotOsInputAdapter): #thread_id=None): 
        thread_id = self.create_thread(input_adapter)
        logger.warn(f"add_task - {thread_id} - {task}")
        with self.lock:
            self.tasks.append({"task_id": str(self.current_task_index), "msg":task, "thread_id": thread_id})
            self.current_task_index += 1
        return thread_id

    def _mark_task_completed(self, task_id:str, thread_id:str):
        logger.warn(f"_mark_task_completed - task_id:{task_id}, thread_id:{thread_id}")
        self.tasks = [t for t in self.tasks if t["task_id"] != task_id]

    def _check_task_list(self):
        with self.lock:
            logger.info(f"_check_task_list - {len(self.tasks)} tasks")
            logger.info(f"_check_task_list - {self.tasks}")

            if len(self.tasks) == 0:
                return
            task = self.tasks[0] #.pop(0)
            self.add_message(BotOsInputMessage(thread_id=task["thread_id"], 
                                               msg=f'THIS IS AN AUTOMATED MESSAGE FROM YOUR TASK MANAGEMENT SYSTEM: Please continue this task id:{task["task_id"]} or mark it complete silently once its completed: {task["msg"]}'))

    #def _store_memory(self, memory:str, scope:str="user_preferences"):
    #    self.knowledge_impl.store_memory(memory, scope=scope)
 
    def _health_check(self):
        pass
