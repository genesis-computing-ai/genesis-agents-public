from collections import deque
import datetime
from decimal import Decimal
import html
import json
import os
import re
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

    stream_mode = False
    
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


        # TODO Make this dy
        self.event_callback = None
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
        self.thread_stop_map = {}
        self.last_stop_time_map = {}
        self.stop_result_map = {}

    # Create a map to store thread history
        self.thread_history = {}
        self.thread_busy_list = deque()

        self.thread_full_response = {}


   
    def cortex_complete(self,thread_id, message_metadata = None, event_callback = None, temperature=None ):
        
        if os.getenv("CORTEX_VIA_COMPLETE", "false").lower() == "false":
            return self.cortex_rest_api(thread_id, message_metadata=message_metadata, event_callback=event_callback, temperature=temperature)

        newarray = [{"role": message["message_type"], "content": message["content"]} for message in self.thread_history[thread_id]]
        new_array_str = json.dumps(newarray) 

        resp = ''
        curr_resp = ''

        last_user_message = next((message for message in reversed(newarray) if message["role"] == "user"), None)
        if last_user_message is not None:

            if ') says: !model' in last_user_message["content"] or last_user_message["content"]=='!model':
                resp= f"The model is set to: {self.llm_engine}. Currently running via Cortext via REST. You can say !model llama3.1-405b, !model llama3.1-70b, or !model llama3.1-8b to change model size."
                curr_resp = resp
            if ') says: !model llama3.1-405b' in last_user_message["content"] or last_user_message["content"]=='!model llama3.1-405b':
                self.llm_engine = 'llama3.1-405b'
                resp= f"The model is changed to: {self.llm_engine}"
                curr_resp = resp
            if ') says: !model llama3.1-70b' in last_user_message["content"] or last_user_message["content"]=='!model llama3.1-70b':
                self.llm_engine = 'llama3.1-70b'
                resp= f"The model is changed to: {self.llm_engine}"
                curr_resp = resp
            if ') says: !model llama3.1-8b' in last_user_message["content"] or last_user_message["content"]=='!model llama3.1-8b':
                self.llm_engine = 'llama3.1-8b'
                resp= f"The model is changed to: {self.llm_engine}"
                curr_resp = resp
        if resp != '':
            self.thread_history[thread_id] = [message for message in self.thread_history[thread_id] if not (message.get("role","") == "user" and message == last_user_message)]
            if BotOsAssistantSnowflakeCortex.stream_mode == True:
                if self.event_callback:
                    self.event_callback(self.bot_id, BotOsOutputMessage(thread_id=thread_id, 
                                                                        status='in_progress', 
                                                                        output=resp, 
                                                                        messages=None, 
                                                                        input_metadata=json.loads(message_metadata)))
            return None 

        print(self.bot_name, f"bot_os_cortex calling cortex {self.llm_engine} via SQL, content est tok len=",len(new_array_str)/4, flush=True)

        context_limit = 128000 * 4 #32000 * 4
        cortex_query = f"""
                        select SNOWFLAKE.CORTEX.COMPLETE('{self.llm_engine}', %s) as completion;
        """
        try:
            cursor = self.client.connection.cursor()
            start_time = time.time()
            start_exec_time = time.time()
            cursor.execute(cortex_query, (new_array_str,))
            end_exec_time = time.time()
            etime = end_exec_time - start_exec_time
            self.client.connection.commit()
            elapsed_time = time.time() - start_time
            result = cursor.fetchone()
            completion = result[0] if result else None

            print(f"{completion} ({elapsed_time:.2f} seconds)")
            resp = completion
            curr_resp = completion

            if resp != '' and BotOsAssistantSnowflakeCortex.stream_mode == True:
                if self.event_callback:
                    self.event_callback(self.bot_id, BotOsOutputMessage(thread_id=thread_id, 
                                                                        status='in_progress', 
                                                                        output=resp, 
                                                                        messages=None, 
                                                                        input_metadata=json.loads(message_metadata)))

            self.thread_full_response[thread_id] = resp + '\n'
            return(curr_resp)

        except Exception as e:
            print('query error: ',e)
            self.client.connection.rollback()
 
    def fix_tool_calls(self, resp):

        while True:
            orig_resp = resp 
        
            pattern_function_call = re.compile(r'<function=(.*?)>\{.*?\}</function>')
            match_function_call = pattern_function_call.search(resp)
            
            if not match_function_call:
                # look for the other way of calling functions 
                pattern_function_call = re.compile(r'<\|python_tag\|>\{"type": "function", "name": "(.*?)", "parameters": \{.*?\}\}')
                match_function_call = pattern_function_call.search(resp)

            if not match_function_call:
                # look for the other way of calling functions 
                pattern_function_call = re.compile(r'<function=(.*?)>\{.*?\}')
                match_function_call = pattern_function_call.search(resp)

            # make the tool calls prettier 
            if match_function_call:
                function_name = match_function_call.group(1)
                function_name_pretty = re.sub(r'(_|^)([a-z])', lambda m: m.group(2).upper(), function_name).replace('_', ' ')
                new_resp = f"🧰 Using tool: _{function_name_pretty}_..."
                # replace for display purposes only
                resp = resp.replace(match_function_call.group(0), new_resp)
                resp = re.sub(r'(?<!\n)(🧰)', r'\n\1', resp)  # add newlines before toolboxes as needed

            if resp == orig_resp:
                break
            else: 
                orig_resp = resp

        return resp 


    def cortex_rest_api(self,thread_id,message_metadata=None, event_callback=None, temperature=None):

        if os.getenv("CORTEX_FIREWORKS_OVERRIDE", "False").lower() == "true":
            fireworks = True
        else:
            fireworks = False

        newarray = [{"role": message["message_type"], "content": message["content"]} for message in self.thread_history[thread_id]]
        resp = ''
        curr_resp = ''

        last_user_message = next((message for message in reversed(newarray) if message["role"] == "user"), None)
        if last_user_message is not None:
            if last_user_message["content"].endswith(') says: !stop') or last_user_message["content"]=='!stop':
                future_timestamp = datetime.datetime.now() + datetime.timedelta(seconds=10)
                self.thread_stop_map[thread_id] = future_timestamp
                self.last_stop_time_map[thread_id] = datetime.datetime.now()
 
                i = 0
                for _ in range(15):
                    if self.stop_result_map.get(thread_id) == 'stopped':
                        break
                    time.sleep(1)
                    i = i + 1
                    
                if self.stop_result_map.get(thread_id) == 'stopped':
                    time_to_wait = max(0, 15 - i)
                    time.sleep(time_to_wait)
                    self.thread_stop_map.pop(thread_id, None)
                    self.stop_result_map.pop(thread_id, None)
                    resp = "Streaming stopped for previous request"
                else:
                    time_to_wait = max(0, 15 - i)
                    time.sleep(time_to_wait)
                    self.thread_stop_map.pop(thread_id, None)
                    self.stop_result_map.pop(thread_id, None)
                    resp = "No streaming response found to stop"  
                curr_resp = resp
            if thread_id in self.thread_stop_map:
                self.thread_stop_map.pop(thread_id)
            if ') says: !model' in last_user_message["content"] or last_user_message["content"]=='!model':
                resp= f"The model is set to: {self.llm_engine}. Currently running via Cortext via REST. You can say !model llama3.1-405b, !model llama3.1-70b, or !model llama3.1-8b to change model size."
                curr_resp = resp
            if ') says: !model llama3.1-405b' in last_user_message["content"] or last_user_message["content"]=='!model llama3.1-405b':
                self.llm_engine = 'llama3.1-405b'
                resp= f"The model is changed to: {self.llm_engine}"
                curr_resp = resp
            if ') says: !model llama3.1-70b' in last_user_message["content"] or last_user_message["content"]=='!model llama3.1-70b':
                self.llm_engine = 'llama3.1-70b'
                resp= f"The model is changed to: {self.llm_engine}"
                curr_resp = resp
            if ') says: !model llama3.1-8b' in last_user_message["content"] or last_user_message["content"]=='!model llama3.1-8b':
                self.llm_engine = 'llama3.1-8b'
                resp= f"The model is changed to: {self.llm_engine}"
                curr_resp = resp
        if resp != '':
            self.thread_history[thread_id] = [message for message in self.thread_history[thread_id] if not (message.get("role","") == "user" and message == last_user_message)]
            if BotOsAssistantSnowflakeCortex.stream_mode == True:
                if self.event_callback:
                    self.event_callback(self.bot_id, BotOsOutputMessage(thread_id=thread_id, 
                                                                        status='in_progress', 
                                                                        output=resp, 
                                                                        messages=None, 
                                                                        input_metadata=json.loads(message_metadata)))
            return None 
                
        if resp == '':
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

                resp = requests.request("POST", url, headers=headers, data=json.dumps(payload))
                curr_resp = resp 
            else:

                SNOWFLAKE_HOST = self.client.client.host
                REST_TOKEN = self.client.client.rest.token
                url=f"https://{SNOWFLAKE_HOST}/api/v2/cortex/inference/complete"
                headers = {
                    "Accept": "text/event-stream",
                    "Content-Type": "application/json",
                    "Authorization": f'Snowflake Token="{REST_TOKEN}"',
                }
            
                if temperature is None:
                    temperature = 0.2
                request_data = {
                    "model": self.llm_engine,
                    "messages": newarray,
                    "stream": True,
                    "max_tokens": 3000,
                    "temperature": temperature,
                    "top_p": 1,
                    "top_k": 40,
                    "presence_penalty": 0,
                    "frequency_penalty": 0,
                    "stop": '</function>',
                }

                print(self.bot_name, f" bot_os_cortex calling cortex {self.llm_engine} via REST API, content est tok len=",len(str(newarray))/4, flush=True)


            #    response = requests.post(url, json=request_data, stream=False, headers=headers)

                start_time = time.time()

                resp = self.thread_full_response.get(thread_id,None)
                curr_resp = ''
                usage = None
                gen_start_time = None
                last_update = None
                if resp is None:
                    resp = ''
                response = requests.post(url, json=request_data, stream=True, headers=headers)
                
                if response.status_code != 200:
                    print(f"Failed to connect to Cortex API. Status code: {response.status_code}")
                    print(f"Response: {response.text}")
                    return False
                else:
                    for line in response.iter_lines():
                        if thread_id in self.thread_stop_map:
                            stop_timestamp = self.thread_stop_map[thread_id]
                            # if isinstance(stop_timestamp, str) and stop_timestamp == 'stopped':
                            #     del self.thread_stop_map[thread_id]
                            if isinstance(stop_timestamp, datetime.datetime) and (time.time() - stop_timestamp.timestamp()) <= 10:
                                self.stop_result_map[thread_id] = 'stopped'
                                if 'curr_resp' not in locals():
                                    curr_resp = ''
                                resp += ' `stopped`'
                                print('cortex thread stopped by user request')
                                gen_start_time = time.time()
                                break
                            if isinstance(stop_timestamp, datetime.datetime) and (time.time() - stop_timestamp.timestamp()) > 30:
                                self.thread_stop_map.pop(thread_id,None)
                                self.stop_result_map.pop(thread_id,None)
                        if line:
                            try:
                                decoded_line = line.decode('utf-8')
                                if not decoded_line.strip():
                                    print("Received an empty line.")
                                    continue
                                if decoded_line.startswith("data: "):
                                    decoded_line = decoded_line[len("data: "):]
                                d = ''
                                event_data = json.loads(decoded_line)
                                break_after_update = False
                                if 'choices' in event_data:
                                    if gen_start_time is None:
                                        gen_start_time = time.time()
                                    d = event_data['choices'][0]['delta'].get('content','')
                                    curr_resp += d
                                    resp += d
                                    if "<|eom_id|>" in curr_resp[-100:]:
                                        curr_resp = curr_resp[:curr_resp.rfind("<|eom_id|>")].strip()
                                        resp = resp[:resp.rfind("<|eom_id|>")].strip()
                                        break_after_update = True
                                    if "}</function>" in curr_resp[-100:]:
                                        curr_resp = curr_resp[:curr_resp.rfind("}</function>") + len("}</function>")].strip()
                                        resp = resp[:resp.rfind("}</function>") + len("}</function>")].strip()
                                        break_after_update = True
                                    u = event_data.get('usage')
                                    if u:
                                        usage = u
                                if d != '' and BotOsAssistantSnowflakeCortex.stream_mode == True and (last_update is None and len(resp) >= 15) or (last_update and (time.time() - last_update > 2)):
                                    last_update = time.time()
                                    if self.event_callback:
                                        self.event_callback(self.bot_id, BotOsOutputMessage(thread_id=thread_id, 
                                                                                            status='in_progress', 
                                                                                            output=resp+" 💬", 
                                                                                            messages=None, 
                                                                                            input_metadata=json.loads(message_metadata)))
                                if break_after_update:
                                    break
                               
                            except json.JSONDecodeError as e:
                                print(f"Error decoding JSON: {e}")
                                continue
                            
                if gen_start_time is not None:
                    elapsed_time = time.time() - start_time
                    gen_time = time.time() - gen_start_time
                    print(f"\nRequest to Cortex REST API completed in {elapsed_time:.2f} seconds total, {gen_time:.2f} seconds generating, time to gen start: {gen_start_time - start_time:.2f} seconds")

                else:
                    try:
                        resp = f"Error calling Cortex: Received status code {response.status_code} with message: {response.reason}"
                        curr_resp = resp
                    except:
                        resp = 'Error calling Cortex'
                        curr_resp = resp 

        try:
            print(json.dumps(usage))
            response_tokens = usage['completion_tokens']
            tokens_per_second = response_tokens / elapsed_time
            tokens_per_second_gen = response_tokens / gen_time
            print(f"Tokens per second overall: {tokens_per_second:.2f}, Tokens per second generating: {tokens_per_second_gen:.2f}")
        except:
            pass

        postfix = ""
        if "</function>" in resp[-30:]:
            postfix = "💬"
   
        pattern = re.compile(r'<\|python_tag\|>\{.*?\}')
        match = pattern.search(resp)
        
        if match and resp.endswith('}'):
            postfix = "💬"

   #     pattern_function = re.compile(r'<function=.*?>\{.*?\}')
   #     match_function = pattern_function.search(resp)
   #     pattern_function2 = re.compile(r'<function>.*?</function>\{.*?\}')
   #     match_function2 = pattern_function2.search(resp)

    #    if (match_function or match_function2) and resp.endswith('}'):
    #        if match_function2:
    #            resp = resp.replace("</function>", "")
    #        postfix = "💬"

        # fix things like this: <function>function_name</function>{"param1": "param1value", "param2": "param2value, etc."} 
        # to make it : <function=_manage_processes>{"action": "LIST", "bot_id": "MrsEliza-3348b2"}</function>
        pattern_function = re.compile(r'<function>(.*?)</function>(\{.*?\})$')
        match_function = pattern_function.search(resp)
        
        if match_function and resp.endswith(match_function.group(2)):
            function_name = match_function.group(1)
            params = match_function.group(2)
            newcall = f"<function={function_name}>{params}</function>"
            resp = resp.replace(match_function.group(0), newcall)
            curr_resp = resp
            postfix = "💬"

        resp = self.fix_tool_calls(resp)

        if resp != '' and BotOsAssistantSnowflakeCortex.stream_mode == True:
            if self.event_callback:
                self.event_callback(self.bot_id, BotOsOutputMessage(thread_id=thread_id, 
                                                                    status='in_progress', 
                                                                    output=resp+postfix, 
                                                                    messages=None, 
                                                                    input_metadata=json.loads(message_metadata)))
        try:
            if fireworks:
                s = 'Fireworks'
            else:
                s = 'Cortex'
            print(f"{s} response: ", json.loads(response.content)["usage"])
        except:
            pass

        self.thread_full_response[thread_id] = resp if resp.endswith('\n') else resp + '\n'
        return(curr_resp)



    #def stream_data(self,response, thread_id):
        
#        client = sseclient.SSEClient(response)
#        for event in client.events():
#            d = json.loads(event.data)
#            try:
#                response = d['choices'][0]['delta']['content']
#                self.thread_full_response[thread_id] += response
#                yield response
#            except Exception:
#                pass
        

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
    
     #   insert_query = f"""
     #   INSERT INTO {self.cortex_threads_schema_input_table} (
     #       TIMESTAMP, BOT_ID, BOT_NAME, THREAD_ID, MESSAGE_TYPE, MESSAGE_PAYLOAD, MESSAGE_METADATA
     #   ) VALUES (%s, %s, %s, %s, %s, %s, %s)
     #   """

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
    
    def add_message(self, input_message:BotOsInputMessage, event_callback=None):
        timestamp = datetime.datetime.now()
        if self.event_callback is None and event_callback is not None:
            self.event_callback = event_callback
        thread_id = input_message.thread_id  # Assuming input_message has a thread_id attribute
        message_payload = input_message.msg 

        if thread_id in self.thread_stop_map:
            stop_timestamp = self.thread_stop_map[thread_id]
            if isinstance(stop_timestamp, datetime.datetime) and (time.time() - stop_timestamp.timestamp()) <= 10:
                message_payload = '!NO_RESPONSE_REQUIRED'

        if thread_id in self.thread_busy_list:
            if not(message_payload.endswith(') says: !stop') or message_payload =='!stop'):
                print('bot_os_cortex add_message thread is busy, returning new message to queue')
                return False
        
        message_type = 'user'
        message_metadata = json.dumps(input_message.metadata)  # Assuming BotOsInputMessage has a metadata attribute that needs to be converted to string

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

            thread_name = f"Cortex_{self.bot_name}_{thread_id}"
            threading.Thread(target=self.update_threads, name=thread_name, args=(thread_id, timestamp, message_metadata, event_callback)).start()

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
        if thread_id in self.thread_busy_list:
            print(f"BotOsAssistantSnowflakeCortex:check_runs - skipping thread {thread_to_check['thread_id']} as its busy in another run")
            return
        if True:
            if thread_id not in self.thread_busy_list:
                self.thread_busy_list.append(thread_id)
            else:
                print(f"BotOsAssistantSnowflakeCortex:check_runs - skipping thread {thread_to_check['thread_id']} as its busy in another run")
                return
            print(f"BotOsAssistantSnowflakeCortex:check_runs - running now, thread {thread_id} ts {timestamp} ")

            thread = self.thread_history.get(thread_id, [])
            user_message = next((msg for msg in thread if (msg.get("message_type") == "user" or msg.get("message_type") == "ipython") and msg.get("timestamp") == timestamp.isoformat()), None)
            # This line searches for the last message in the thread that is of type "assistant" and has a timestamp matching the given timestamp.
            assistant_message = next((msg for msg in reversed(thread) if msg.get("message_type") == "assistant" and msg.get("timestamp") == timestamp.isoformat()), None)
            if assistant_message:
                message_payload = assistant_message.get("content")
              #  print(f"Assistant message found: {message_payload}")
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

                    # handle this pattern: <function>_manage_processes</function>{"action": "LIST", "bot_id": "MrsEliza-3348b2"} (

                    print(f"Response for Thread ID {thread_id}, {timestamp} with model {self.llm_engine}: {message_payload}")
                    decoded_payload = html.unescape(message_payload)

                    # fix tool calls with a missing / in the close block
                    pattern_function_call = re.compile(r'<function=(.*?)>\{.*?\}<function>')
                    match_function_call = pattern_function_call.search(decoded_payload)

                    if match_function_call:
                        function_name = match_function_call.group(1)
                        decoded_payload = re.sub(pattern_function_call, f'<function={function_name}>\\g<0></function>', decoded_payload)
                        decoded_payload = decoded_payload.replace('<function></function>', '</function>')

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
                        pattern_any_function = re.compile(r'<function=.*?>\{.*?\}')
                        match_any_function = pattern_any_function.search(decoded_payload)
                        if match_any_function and decoded_payload.endswith('}'):
                            self.process_tool_call(thread_id, timestamp, decoded_payload, message_metadata)
                        else:
                            if message_metadata == '' or message_metadata == None:
                                message_metadata = '{}'   
                            output = self.thread_full_response[thread_id]
                            self.thread_full_response[thread_id] = "" 
                             
                      #  event_callback(self.bot_id, BotOsOutputMessage(thread_id=thread_id, 
                      #                                              status="completed", 
                      #                                              output=output, 
                      #                                              messages="", 
                      #                                              input_metadata=json.loads(message_metadata)))
                else:
                    logger.error(f"No Assistant Response found for Thread ID {thread_id} {timestamp} and model {self.llm_engine}")
                    #self.active_runs.append(thread_to_check)
                logger.warn("BotOsAssistantSnowflakeCortex:check_runs - run complete")
            except Exception as e:
                print(f"Error retrieving Assistant Response for Thread ID {thread_id} and model {self.llm_engine}: {e}")
        if thread_id in self.thread_busy_list:
            self.thread_busy_list.remove(thread_id)

    def process_tool_call(self, thread_id, timestamp, message_payload, message_metadata):
        import json

        start_tag = '<function='
        end_tag = '</function>'
        start_index = message_payload.find(start_tag) 
        end_index = message_payload.find(end_tag, start_index)
        if end_index == -1 and message_payload.endswith('}'):
            end_index = len(message_payload)
        if start_index == -1:
            start_tag = "<|python_tag|>"
            end_tag = "<|eom_id|>"
            start_index = message_payload.find(start_tag)
            if start_index != -1:
                start_index += len(start_tag)
            end_index = message_payload.find(end_tag)
            if end_index == -1:
                end_index = len(message_payload)
            tool_type = 'json'
        else:
            tool_type = 'markup'
              
        tool_call_str = message_payload[start_index:end_index].strip()
        try:
            if tool_type == 'markup':
                function_call_str = message_payload[start_index:end_index].strip()
                #function_call_str = function_call_str.encode("utf-8").decode("unicode_escape")
                function_name_start = function_call_str.find('<function=') + len('<function=')
                function_name_end = function_call_str.find('>', function_name_start)
                function_name = function_call_str[function_name_start:function_name_end]

                arguments_start = function_name_end + 1
                arguments_str = function_call_str[arguments_start:].strip()
               # arguments_str = arguments_str.encode("utf-8").decode("unicode_escape")
                arguments_str = arguments_str.replace('\\\\"', '\\"')
                if arguments_str.endswith('>'):
                    arguments_str = arguments_str[:-1]
                if arguments_str == '':
                    arguments_str = '{}'
                arguments_json = json.loads(arguments_str)
                func_call_details = {
                        "function_name": function_name,
                        "arguments": arguments_json
                    }
                cb_closure = self._generate_callback_closure(thread_id, timestamp, message_metadata, func_call_details=func_call_details)
            if tool_type == 'json':
                function_call_str = message_payload[start_index:end_index].strip()
                function_call_str = bytes(function_call_str, "utf-8").decode("unicode_escape")
                try:
                    function_call_json = json.loads(function_call_str)
                    function_name = function_call_json.get("name")
                    arguments_json = function_call_json.get("parameters", {})
                    func_call_details = {
                        "function_name": function_name,
                        "arguments": arguments_json
                    }
                    cb_closure = self._generate_callback_closure(thread_id, timestamp, message_metadata, func_call_details=func_call_details)
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to decode function call JSON {function_call_str}: {e}")
                    cb_closure = self._generate_callback_closure(thread_id, timestamp, message_metadata)
                    cb_closure(f"Failed to decode function call JSON {function_call_str}: {e}")
                    return
            function_to_call = function_name
            arguments = arguments_json
            print(f"Function to call: {function_to_call}")
            print(f"Arguments: {json.dumps(arguments, indent=2)}", flush=True)
            execute_function(function_to_call, json.dumps(arguments), self.available_functions, cb_closure, thread_id, self.bot_id)
        except json.JSONDecodeError as e:
            print(f"Failed to decode tool call JSON {tool_call_str}: {e}")
            cb_closure = self._generate_callback_closure(thread_id, timestamp, message_metadata)
            cb_closure(f"Failed to decode tool call JSON {tool_call_str}: {e}.  Did you make sure to escape any double quotes that are inside another")
        except Exception as e:
            logger.error(f"Error processing tool call: {e}")
            cb_closure(f"Error processing tool call: {e}")

    def _submit_tool_outputs(self, thread_id, timestamp, results, message_metadata, func_call_details=None):
        """
        Inserts tool call results back into the genesis_test.public.genesis_threads table.
        """

        def custom_serializer(obj):
            if isinstance(obj, Decimal):
                return float(obj)
            elif isinstance(obj, datetime.datetime):
                return obj.isoformat()
            elif isinstance(obj, datetime.date):
                return obj.isoformat()
            elif isinstance(obj, datetime.time):
                return obj.isoformat()
            elif isinstance(obj, set):
                return list(obj)
            elif isinstance(obj, bytes):
                return obj.decode('utf-8')
            raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")

        new_ts = datetime.datetime.now()
        if isinstance(results, (dict, list)):
            results = json.dumps(results, default=custom_serializer)


        prefix = ""
        if os.getenv("CORTEX_VIA_COMPLETE", "false").lower() == "true":
            prefix = 'Here are the results of the tool call: '

        message_object = {
            "message_type": "user",
            "content": prefix+results,
            "timestamp": new_ts.isoformat(),
            "metadata": message_metadata
        }

        try:
            results_json = json.loads(results)
        except json.JSONDecodeError as e:
           # logger.error(f"Failed to decode results JSON: {e}")
            results_json = results  # Fallback to original results if JSON decoding fails
        if isinstance(results, dict) and 'success' in results and results['success']:
            logger.info(f"Tool call was successful for Thread ID {thread_id}")        
        if func_call_details is not None:
            function_name = func_call_details.get('function_name')
            if function_name in ['remove_tools_from_bot','add_new_tools_to_bot', 'add_bot_files', 'update_bot_instructions', 'remove_bot_files']:
                try:
                    results_json = json.loads(results)
                    if ('success' in results_json and results_json['success']) or ('Success' in results_json and results_json['Success']):
                        if func_call_details and 'arguments' in func_call_details:
                            arguments = func_call_details['arguments']
                            if 'bot_id' in arguments:
                                bot_id = arguments['bot_id']
                                os.environ[f'RESET_BOT_SESSION_{bot_id}'] = 'True'
                except:
                    pass

        if thread_id not in self.thread_history:
            self.thread_history[thread_id] = []
        self.thread_history[thread_id].append(message_object)

        if isinstance(results_json, str) and results_json.strip() == "Error, your query was cut off.  Query must be complete and end with a semicolon.  Include the full query text, with an ; on the end and RUN THIS TOOL AGAIN NOW!":
            hightemp = 0.6
        else:
            hightemp = None
        if thread_id in self.last_stop_time_map and timestamp < self.last_stop_time_map[thread_id]:
            print('bot_os_cortex _submit_tool_outputs stop message received, not rerunning thread with outputs')
            self.stop_result_map[thread_id] = 'stopped'
        else:
            self.update_threads(thread_id, new_ts, message_metadata=message_metadata, temperature=hightemp)
   #     self.active_runs.append({"thread_id": thread_id, "timestamp": new_ts})
        return 


    def _generate_callback_closure(self, thread_id, timestamp, message_metadata, func_call_details = None):
      def callback_closure(func_response):  # FixMe: need to break out as a generate closure so tool_call_id isn't copied
        #  try:                     
        #     del self.running_tools[tool_call_id]
        #  except Exception as e:
        #     logger.error(f"callback_closure - tool call already deleted - caught exception: {e}")
         try:
            self._submit_tool_outputs(thread_id, timestamp, func_response, message_metadata, func_call_details=func_call_details)
         except Exception as e:
            error_string = f"callback_closure - _submit_tool_outputs - caught exception: {e}"
            logger.error(error_string)
            return error_string
            #self._submit_tool_outputs(thread_id, timestamp, error_string, message_metadata)
      return callback_closure

    def update_threads(self, thread_id, timestamp, message_metadata = None, event_callback = None, temperature = None):
        """
        Executes the SQL query to update threads based on the provided SQL, incorporating self.cortex... tables.
        """

        if thread_id not in self.thread_busy_list:
            self.thread_busy_list.append(thread_id)

  #      resp = self.cortex_rest_api(thread_id)

        resp = self.cortex_complete(thread_id=thread_id, message_metadata=message_metadata, event_callback=event_callback, temperature=temperature)
        if resp is None:
            if thread_id in self.thread_busy_list:
                self.thread_busy_list.remove(thread_id)
            return 
        
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

        if thread_id in self.thread_busy_list:
            self.thread_busy_list.remove(thread_id)
        self.active_runs.append({"thread_id": thread_id, "timestamp": timestamp})

        return


