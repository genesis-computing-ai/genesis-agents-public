from collections import deque
import datetime
from decimal import Decimal
import html
import json
import os
import requests
import sseclient
import time
import uuid
import threading
from typing_extensions import override
from decimal import Decimal

from connectors.snowflake_connector import SnowflakeConnector
from core.bot_os_assistant_base import BotOsAssistantInterface, execute_function

import logging

from core.bot_os_input import BotOsInputMessage, BotOsOutputMessage
logger = logging.getLogger(__name__)

class BotOsAssistantSnowflakeCortex(BotOsAssistantInterface):
    def __init__(self, name:str, instructions:str, 
                tools:list[dict] = {}, available_functions={}, files=[], 
                update_existing=False, log_db_connector=None, bot_id='default_bot_id', bot_name='default_bot_name', all_tools:list[dict]={}, all_functions={},all_function_to_tool_map={},skip_vectors=False) -> None:
        super().__init__(name, instructions, tools, available_functions, files, update_existing, skip_vectors=False)
        self.active_runs = deque()
#        self.llm_engine = 'mistral-large'

        #self.llm_engine = 'llama3.1-70b'
        if os.getenv("CORTEX_MODEL", None) is not None:
            self.llm_engine =  os.getenv("CORTEX_MODEL", None)
        else:
            self.llm_engine = 'llama3.1-405b'

        self.instructions = instructions 
        self.bot_name = bot_name
        self.bot_id = bot_id
        self.tools = tools
        self.available_functions = available_functions
        self.done_map = {}
        self.thread_run_map = {}
        self.active_runs = deque()
        self.processing_runs = deque()
        self.cortex_threads_schema_input_table  = os.getenv("GENESIS_INTERNAL_DB_SCHEMA") + ".CORTEX_THREADS_INPUT"
        self.cortex_threads_schema_output_table = os.getenv("GENESIS_INTERNAL_DB_SCHEMA") + ".CORTEX_THREADS_OUTPUT"
        self.client = SnowflakeConnector(connection_name='Snowflake')
        logger.debug("BotOsAssistantSnowflakeCortex:__init__ - SnowflakeConnector initialized")
        self.my_tools = tools
        self.log_db_connector = log_db_connector
        self.callback_closures = {}
        self.user_allow_cache = {}
        self.clear_access_cache = False 
        
        self.allowed_types_search = [".c", ".cs", ".cpp", ".doc", ".docx", ".html", ".java", ".json", ".md", ".pdf", ".php", ".pptx", ".py", ".rb", ".tex", ".txt", ".css", ".js", ".sh", ".ts"]
        self.allowed_types_code_i = [".c", ".cs", ".cpp", ".doc", ".docx", ".html", ".java", ".json", ".md", ".pdf", ".php", ".pptx", ".py", ".rb", ".tex", ".txt", ".css", ".js", ".sh", ".ts", ".csv", ".jpeg", ".jpg", ".gif", ".png", ".tar", ".xlsx", ".xml", ".zip"]
        self.run_meta_map = {}

    # Create a map to store thread history
        self.thread_history = {}

        self.thread_full_response = {}


   
    def cortex_complete(self,thread_id):
        
        if os.getenv("CORTEX_VIA_REST", "False").lower() == "true":
            return self.cortex_rest_api(thread_id)

        newarray = [{"role": message["message_type"], "content": message["content"]} for message in self.thread_history[thread_id]]
        new_array_str = json.dumps(newarray)


        print(self.bot_name, f"bot_os_cortex calling cortex {self.llm_engine} via SQL, content est tok len=",len(new_array_str)/4)

        context_limit = 128000 * 4 #32000 * 4
        cortex_query = f"""
                        select SNOWFLAKE.CORTEX.COMPLETE('{self.llm_engine}', %s) as completion;
        """
        try:
            cursor = self.client.connection.cursor()
            start_time = time.time()
            cursor.execute(cortex_query, (new_array_str,))
            self.client.connection.commit()
            elapsed_time = time.time() - start_time
            result = cursor.fetchone()
            completion = result[0] if result else None

            print(completion)

            return(completion)
        except Exception as e:
            print('query error: ',e)
            self.client.connection.rollback()
 

    def cortex_rest_api(self,thread_id):

        if os.getenv("CORTEX_FIREWORKS_OVERRIDE", "False").lower() == "true":
            fireworks = True
        else:
            fireworks = False

        newarray = [{"role": message["message_type"], "content": message["content"]} for message in self.thread_history[thread_id]]
        
        if fireworks:
 
            if self.llm_engine == 'llama3.1-405b':
                fireworks_model = 'accounts/fireworks/models/llama-v3p1-405b-instruct'
            else:
                fireworks_model = 'accounts/fireworks/models/llama-v3p1-70b-instruct'
            fireworks_api_key = os.getenv('FIREWORKSAI_API_KEY',None)
            if fireworks_api_key == None:
                print('No Fireworks API key set in FIREWORKSAI_API_KEY ENV VAR')
                raise('No Fireworks API key set in FIREWORKSAI_API_KEY ENV VAR')
            url = "https://api.fireworks.ai/inference/v1/chat/completions"
            payload = {
            "model": fireworks_model,
            "max_tokens": 16384,
            "top_p": 1,
            "top_k": 40,
            "presence_penalty": 0,
            "frequency_penalty": 0,
            "temperature": 0.6,
            "messages": newarray
            }
            headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": "Bearer "+fireworks_api_key+""
            }
            print(self.bot_name, f" bot_os_cortex calling {fireworks_model.split('/')[-1]} via FIREWORKS API, content est tok len=",len(str(newarray))/4)

            response = requests.request("POST", url, headers=headers, data=json.dumps(payload))
        else:

            SNOWFLAKE_HOST = self.client.client.host
            REST_TOKEN = self.client.client.rest.token
            url=f"https://{SNOWFLAKE_HOST}/api/v2/cortex/inference/complete"
            headers = {
                "Accept": "text/event-stream",
                "Content-Type": "application/json",
                "Authorization": f'Snowflake Token="{REST_TOKEN}"',
            }
        
            request_data = {
                "model": self.llm_engine,
    #            "messages": [{"content": "Hi there"}],
                "messages": newarray,
                "stream": False,
            }
            print(self.bot_name, f" bot_os_cortex calling cortex {self.llm_engine} via REST API, content est tok len=",len(str(newarray)/4))

            response = requests.post(url, json=request_data, stream=False, headers=headers)
        try:
            if fireworks:
                s = 'Fireworks'
            else:
                s = 'Cortex'
            print(f"{s} response: ", json.loads(response.content)["usage"])
        except:
            pass

        return(response.content)



    def stream_data(self,response, thread_id):
        
        client = sseclient.SSEClient(response)
        for event in client.events():
            d = json.loads(event.data)
            try:
                response = d['choices'][0]['delta']['content']
                self.thread_full_response[thread_id] += response
                yield response
            except Exception:
                pass
        

    @override
    def is_active(self) -> bool:
       return self.active_runs
   
    @override
    def is_processing_runs(self) -> bool:
       return self.processing_runs

    @override
    def get_done_map(self) -> dict:
       return self.done_map

    def create_thread(self) -> str:
        thread_id = f"Cortex_thread_{uuid.uuid4()}"
        timestamp = datetime.datetime.now()
        message_type = 'System Prompt'
    
        insert_query = f"""
        INSERT INTO {self.cortex_threads_schema_input_table} (
            TIMESTAMP, BOT_ID, BOT_NAME, THREAD_ID, MESSAGE_TYPE, MESSAGE_PAYLOAD, MESSAGE_METADATA
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

        try:
        #    cursor = self.client.connection.cursor()
        #    cursor.execute(insert_query, (
        #        timestamp, self.bot_id, self.bot_name, thread_id, message_type, self.instructions, "",
        #    ))
        #    self.client.connection.commit()
        #    cursor.execute(insert_query, (
        #        timestamp, self.bot_id, self.bot_name, thread_id, message_type, TOOLS_PREFIX+str(self.tools), "",
        #    ))
        #    self.client.connection.commit()
          #  thread_name = f"Cortex_{thread_id}"
          #  threading.Thread(target=self.update_threads, args=(thread_id, thread_name, None)).start()

            message_object = {
                "message_type": "system",
                "content": self.instructions,
                "timestamp": timestamp.isoformat()
            }

            if thread_id not in self.thread_history:
                self.thread_history[thread_id] = []
            self.thread_history[thread_id].append(message_object)

            logger.info(f"Successfully inserted system prompt for thread_id: {thread_id}")
        except Exception as e:
            logger.error(f"Failed to insert system prompt for thread_id: {thread_id} with error: {e}")
        return thread_id
    
    def add_message(self, input_message:BotOsInputMessage):
        timestamp = datetime.datetime.now()
        thread_id = input_message.thread_id  # Assuming input_message has a thread_id attribute
        message_payload = input_message.msg 
        message_type = 'user'
        message_metadata = json.dumps(input_message.metadata)  # Assuming BotOsInputMessage has a metadata attribute that needs to be converted to string
    
        insert_query = f"""
        INSERT INTO {self.cortex_threads_schema_input_table} (
            TIMESTAMP, BOT_ID, BOT_NAME, THREAD_ID, MESSAGE_TYPE, MESSAGE_PAYLOAD, MESSAGE_METADATA
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

        message_object = {
            "message_type": message_type,
            "content": message_payload,
            "timestamp": timestamp.isoformat(),
            "metadata": message_metadata
        }

        if thread_id not in self.thread_history:
            self.thread_history[thread_id] = []
            system_message_object = {
                    "message_type": "system",
                   "content": self.instructions,
                   "timestamp": timestamp.isoformat()
               }
            self.thread_history[thread_id].append(system_message_object)
        self.thread_history[thread_id].append(message_object)

        try:
         #   cursor = self.client.connection.cursor()
         #   cursor.execute(insert_query, (
         #       timestamp, self.bot_id, self.bot_name, thread_id, message_type, message_payload, message_metadata,
         #   ))
         #   self.client.connection.commit()
            thread_name = f"Cortex_{self.bot_name}_{thread_id}"
            threading.Thread(target=self.update_threads, name=thread_name, args=(thread_id, timestamp)).start()

            logger.info(f"Successfully inserted message log for bot_id: {self.bot_id}")
            #self.active_runs.append({"thread_id": thread_id, "timestamp": timestamp})
        except Exception as e:
            logger.error(f"Failed to insert message log for bot_id: {self.bot_id} with error: {e}")

    def check_runs(self, event_callback):
        try:
            thread_to_check = self.active_runs.popleft()
        except IndexError:
            logger.info(f"BotOsAssistantSnowflakeCortex:check_runs - no active runs for: {self.bot_id}")
            return
        thread_id = thread_to_check["thread_id"]
        timestamp = thread_to_check["timestamp"]
        if True:
            print(f"BotOsAssistantSnowflakeCortex:check_runs - running now, thread {thread_id} ts {timestamp} ")

            thread = self.thread_history.get(thread_id, [])
            user_message = next((msg for msg in thread if msg.get("message_type") == "user" and msg.get("timestamp") == timestamp.isoformat()), None)
            assistant_message = next((msg for msg in thread if msg.get("message_type") == "assistant" and msg.get("timestamp") == timestamp.isoformat()), None)
            if assistant_message:
                message_payload = assistant_message.get("content")
                print(f"Assistant message found: {message_payload}")
            else:
                print("No assistant message found in the thread with the specified timestamp.")
                message_payload = None
            if user_message:
                message_metadata = user_message.get('metadata')
            else:
                message_metadata = None

           # query = f"""
           # SELECT message_payload, message_metadata FROM {self.cortex_threads_schema_output_table}
           # WHERE thread_id = %s AND model_name = %s AND message_type = 'Assistant Response' AND timestamp = %s
           # """
            try:
            #    cursor = self.client.connection.cursor()
            #    cursor.execute(query, (thread_id, self.llm_engine, timestamp))
               # responses = cursor.fetchall()
                if message_payload:

                       # if thread_id not in self.thread_history:
                       #     self.thread_history[thread_id] = []
                       # self.thread_history[thread_id].append(message_object)

                    print(f"Response for Thread ID {thread_id}, {timestamp} with model {self.llm_engine}: {message_payload}")
                    decoded_payload = html.unescape(message_payload)
                    if "<TOOL_CALL>" in decoded_payload:
                        self.process_tool_call(thread_id, timestamp, decoded_payload, message_metadata)
                        #self.active_runs.append(thread_to_check)
                    elif "<function=" in decoded_payload and "</function>" in decoded_payload:
                        self.process_tool_call(thread_id, timestamp, decoded_payload, message_metadata)
                    #  elif '{\n "type": "function",' in decoded_payload:
                    #      self.process_tool_call(thread_id, timestamp, decoded_payload, message_metadata)
                    elif '<|python_tag|>{"type": "function"' in decoded_payload:
                        self.process_tool_call(thread_id, timestamp, decoded_payload, message_metadata)
                    else:
                        if message_metadata == '':
                            message_metadata = '{}'                  
                        event_callback(self.bot_id, BotOsOutputMessage(thread_id=thread_id, 
                                                                    status="completed", 
                                                                    output=message_payload, 
                                                                    messages="", 
                                                                    input_metadata=json.loads(message_metadata)))
                else:
                    logger.error(f"No Assistant Response found for Thread ID {thread_id} {timestamp} and model {self.llm_engine}")
                    #self.active_runs.append(thread_to_check)
                logger.warn("BotOsAssistantSnowflakeCortex:check_runs - run complete")
            except Exception as e:
                logger.error(f"Error retrieving Assistant Response for Thread ID {thread_id} and model {self.llm_engine}: {e}")

    def process_tool_call(self, thread_id, timestamp, message_payload, message_metadata):
        import json
        start_tag = '<function='
        end_tag = '</function>'
        start_index = message_payload.find(start_tag) 
        end_index = message_payload.find(end_tag, start_index)
        if start_index == -1:
            start_tag = "<|python_tag|>"
            end_tag = "<|eom_id|>"
            start_index = message_payload.find(start_tag)
            if start_index != -1:
                start_index += len(start_tag)
            end_index = message_payload.find(end_tag)
            tool_type = 'json'
        else:
            tool_type = 'markup'
              
        tool_call_str = message_payload[start_index:end_index].strip()
        try:
            if tool_type == 'markup':
                cb_closure = self._generate_callback_closure(thread_id, timestamp, message_metadata)
                function_call_str = message_payload[start_index:end_index].strip()
                function_name_start = function_call_str.find('<function=') + len('<function=')
                function_name_end = function_call_str.find('>', function_name_start)
                function_name = function_call_str[function_name_start:function_name_end]

                arguments_start = function_name_end + 1
                arguments_str = function_call_str[arguments_start:].strip()
                arguments_str = arguments_str.replace('\\\\"', '\\"')
                if arguments_str.endswith('>'):
                    arguments_str = arguments_str[:-1]
                if arguments_str == '':
                    arguments_str = '{}'
                arguments_json = json.loads(arguments_str)
            if tool_type == 'json':
                cb_closure = self._generate_callback_closure(thread_id, timestamp, message_metadata)
                function_call_str = message_payload[start_index:end_index].strip()
                try:
                    function_call_json = json.loads(function_call_str)
                    function_name = function_call_json.get("name")
                    arguments_json = function_call_json.get("parameters", {})
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to decode function call JSON {function_call_str}: {e}")
                    cb_closure(f"Failed to decode function call JSON {function_call_str}: {e}")
                    return
            function_to_call = function_name
            arguments = arguments_json
            print(f"Function to call: {function_to_call}")
            print(f"Arguments: {json.dumps(arguments, indent=2)}")
            execute_function(function_to_call, json.dumps(arguments), self.available_functions, cb_closure, thread_id, self.bot_id)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode tool call JSON {tool_call_str}: {e}")
            cb_closure(f"Failed to decode tool call JSON {tool_call_str}: {e}")
        except Exception as e:
            logger.error(f"Error processing tool call: {e}")
            cb_closure(f"Error processing tool call: {e}")

    def _submit_tool_outputs(self, thread_id, timestamp, results, message_metadata):
        """
        Inserts tool call results back into the genesis_test.public.genesis_threads table.
        """

        def custom_serializer(obj):
            if isinstance(obj, Decimal):
                return float(obj)
            raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")

        new_ts = datetime.datetime.now()
        if isinstance(results, (dict, list)):
            results = json.dumps(results, default=custom_serializer)

        message_object = {
            "message_type": "user",
            "content": results,
            "timestamp": new_ts.isoformat(),
            "metadata": message_metadata
        }

        if thread_id not in self.thread_history:
            self.thread_history[thread_id] = []
        self.thread_history[thread_id].append(message_object)

        self.update_threads(thread_id, new_ts)
   #     self.active_runs.append({"thread_id": thread_id, "timestamp": new_ts})
        return 

        insert_query = f"""
        INSERT INTO {self.cortex_threads_schema_input_table} (
            TIMESTAMP, BOT_ID, BOT_NAME, THREAD_ID, MESSAGE_TYPE, MESSAGE_PAYLOAD, MESSAGE_METADATA
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        try:
            cursor = self.client.connection.cursor()
            new_timestamp = datetime.datetime.now()
            try:
                def default_converter(o):
                    if isinstance(o, Decimal):
                        return float(o)  # Convert Decimal to float for JSON serialization
                    elif isinstance(o, datetime):
                        return o.isoformat()
                    raise TypeError(f"Object of type {o.__class__.__name__} is not JSON serializable")
                results_str = json.dumps(results, default=default_converter, indent=2)
                cursor.execute(insert_query, (new_timestamp, self.bot_id, self.bot_name, thread_id, "Tool Response", results_str, message_metadata))
                self.client.connection.commit()
            except Exception as e:
                print('Cortex submit tool output, default_converter or insert error: {e}')
                try:
                    results_str = json.dumps(results, indent=2)
                    cursor.execute(insert_query, (new_timestamp, self.bot_id, self.bot_name, thread_id, "Tool Response", results_str, message_metadata))
                    self.client.connection.commit()
                except Exception as e:
                    print('Cortex submit tool output, simple insert error: {e}')
 
            thread_name = f"Cortex_ToolSub_{thread_id}"
            #threading.Thread(target=self.update_threads, name=thread_name, args=(thread_id, new_timestamp)).start()
            self.update_threads(thread_id, new_timestamp)
            self.active_runs.append({"thread_id": thread_id, "timestamp": new_timestamp})

            logger.info(f"Successfully inserted tool call results for Thread ID {thread_id} and Tool Call ID {new_timestamp} old: {timestamp}")
        except Exception as e:
            logger.error(f"Failed to insert tool call results for Thread ID {thread_id} and Tool Call ID {timestamp}: {e}")
           # JL - I think this may be causing thread locking issues
           # self.client.connection.rollback()

    def _generate_callback_closure(self, thread_id, timestamp, message_metadata):
      def callback_closure(func_response):  # FixMe: need to break out as a generate closure so tool_call_id isn't copied
        #  try:                     
        #     del self.running_tools[tool_call_id]
        #  except Exception as e:
        #     logger.error(f"callback_closure - tool call already deleted - caught exception: {e}")
         try:
            self._submit_tool_outputs(thread_id, timestamp, func_response, message_metadata)
         except Exception as e:
            error_string = f"callback_closure - _submit_tool_outputs - caught exception: {e}"
            logger.error(error_string)
            return error_string
            #self._submit_tool_outputs(thread_id, timestamp, error_string, message_metadata)
      return callback_closure

    def update_threads(self, thread_id, timestamp):
        """
        Executes the SQL query to update threads based on the provided SQL, incorporating self.cortex... tables.
        """

  #      resp = self.cortex_rest_api(thread_id)
        resp = self.cortex_complete(thread_id=thread_id)

        try:
            if isinstance(resp, (list, tuple, bytes)):
                resp = json.loads(resp)['choices'][0]['message']['content']
            if "<|eom_id|>" in resp:
                resp = resp.split("<|eom_id|>")[0] + "<|eom_id|>"
        except:
            resp = 'Cortex error -- nothing returned.'
        
        message_object = {
            "message_type": "assistant",
            "content": resp,
            "timestamp": timestamp.isoformat(),
        }

        if thread_id not in self.thread_history:
            self.thread_history[thread_id] = []
        self.thread_history[thread_id].append(message_object)

        self.active_runs.append({"thread_id": thread_id, "timestamp": timestamp})

        return


