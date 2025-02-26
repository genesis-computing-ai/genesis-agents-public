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
import shutil
import uuid
import threading
import math
from typing_extensions import override
from decimal import Decimal

from openai import OpenAI

from genesis_bots.connectors.snowflake_connector.snowflake_connector import SnowflakeConnector
from genesis_bots.core.bot_os_assistant_base import BotOsAssistantInterface, execute_function, get_tgt_pcnt

from genesis_bots.core.logging_config import logger

from genesis_bots.core.bot_os_input import BotOsInputMessage, BotOsOutputMessage
from genesis_bots.llm.llm_openai.openai_utils import get_openai_client


class BotOsAssistantSnowflakeCortex(BotOsAssistantInterface):

    stream_mode = False
    _shared_thread_history = {}  # Maps bot names to their thread histories

    def __init__(self, name:str, instructions:str,
                tools:list[dict] = [], available_functions={}, files=[],
                update_existing=False, log_db_connector=None, bot_id='default_bot_id', bot_name='default_bot_name', all_tools:list[dict]=[], all_functions={},all_function_to_tool_map={},skip_vectors=False, assistant_id=None) -> None:
        super().__init__(name, instructions, tools, available_functions, files, update_existing, skip_vectors=False)
        self.active_runs = deque()
#        self.llm_engine = 'mistral-large'

        if os.getenv("CORTEX_PREMIERE_MODEL", None) is not None:
            self.llm_engine =  os.getenv("CORTEX_PREMIERE_MODEL", None)
        else:
            if os.getenv("CORTEX_MODEL", None) is not None:
                self.llm_engine =  os.getenv("CORTEX_MODEL", None)
            else:
                self.llm_engine = 'claude-3-5-sonnet'

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
       # self.cortex_threads_schema_input_table  = os.getenv("GENESIS_INTERNAL_DB_SCHEMA") + ".CORTEX_THREADS_INPUT"
       # self.cortex_threads_schema_output_table = os.getenv("GENESIS_INTERNAL_DB_SCHEMA") + ".CORTEX_THREADS_OUTPUT"
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
        self.thread_fast_mode_map = {}
        self.first_message_map = {}
        self.thread_tool_call_counter = {}
        self.thread_model_map = {}
        self.thread_tool_call_counter_failsafe = {}

        # Initialize shared thread history for this bot name if needed
        if name not in self.__class__._shared_thread_history:
            self.__class__._shared_thread_history[name] = {}
        self.thread_history = self.__class__._shared_thread_history[name]

        self.thread_busy_list = deque()

        self.thread_full_response = {}



    def cortex_complete(self,thread_id, message_metadata = None, event_callback = None, temperature=None , fast_mode=False):

        return self.cortex_rest_api(thread_id, message_metadata=message_metadata, event_callback=event_callback, temperature=temperature, fast_mode = fast_mode)


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

            # New pattern to match <function=list_all_bots></function>
            if not match_function_call:
                pattern_function_call = re.compile(r'<function=(.*?)></function>')
                match_function_call = pattern_function_call.search(resp)

            # make the tool calls prettier
            if match_function_call:
                function_name = match_function_call.group(1)
                function_name_pretty = re.sub(r'(_|^)([a-z])', lambda m: m.group(2).upper(), function_name).replace('_', ' ')
                new_resp = f"🧰 Using tool: _{function_name_pretty}_..."
                # replace for display purposes only
                resp = resp.replace(match_function_call.group(0), new_resp)
                resp = re.sub(r'(?<!\n)(🧰)', r'\n\1', resp)  # add newlines before toolboxes as needed
            # Remove trailing function call syntax if present
            # Remove trailing function call syntax if present
            if resp.endswith('...} </function>'):
                resp = resp[:resp.rfind('...') + 3]  # Keep the '...' but remove everything after
            # Remove trailing '...}' if present
            if resp.endswith('...}'):
                resp = resp[:-1]  # Remove the last character ('}')
            # Handle case where response ends with }></function>
            if resp.endswith('}></function>'):
                resp = resp[:resp.rfind('...') + 3]  # Keep the '...' but remove everything after

            if resp == orig_resp:
                break
            else:
                orig_resp = resp

        return resp

    def trim_messages(self, thread_id):
        '''
        Trim messages list by deleting entries
        to keep it under the LLM context window limmit.
        We use rolling window strategy: eliminate messages starting with oldest until we reduce
        overall byte size to target percentage (configured).
        Do not delete current run messages nor instructions.
        '''

        tgt_pcnt = get_tgt_pcnt()
        if tgt_pcnt == None:
            return False

        orig_messages = self.thread_history[thread_id]

        messg_bytes = [len(json.dumps({"role": message["message_type"], "content": message["content"]})) for message in orig_messages]
        total_bytes = sum(messg_bytes)
        tgt_bytes = math.ceil((total_bytes * tgt_pcnt) / 100)
        logger.info(f'bot={self.bot_id}, thread={thread_id}, {len(orig_messages)} messages, {total_bytes} bytes, {tgt_bytes=}')

        messages = orig_messages[:1]
        count = 0
        first_messg_after_deletes = True

        # don't delete instruction and current run messages
        for messg, bytes in zip(orig_messages[1:-1], messg_bytes[1:-1]):
            if total_bytes > tgt_bytes:
                total_bytes -= bytes
                count += 1
                continue

            if first_messg_after_deletes:
                first_messg_after_deletes = False
                # delete assisstant message without preceding user messages
                if messg['message_type'] == 'assistant':
                    total_bytes -= bytes
                    count += 1
                    continue

            messages.append(messg)

        self.thread_history[thread_id] = messages + orig_messages[-1:]
        logger.info(f'bot={self.bot_id}, thread={thread_id}, deleted {count} messages, {total_bytes} bytes in messages now')
        return True

    def make_messages(self, thread_id):
        '''convert thread_history list to messages suitable for Cortex API'''

        newarray = [{"role": message["message_type"], "content": message["content"]} for message in self.thread_history[thread_id]]
        consolidated_array = []
        current_user_content = []

        for message in newarray:
            if message["role"] == "user":
                # Accumulate content from consecutive 'user' messages
                current_user_content.append(message["content"])
            else:
                # If there are accumulated 'user' messages, consolidate them
                if current_user_content:
                    consolidated_array.append({
                        "role": "user",
                        "content": "\n".join(current_user_content)
                    })
                    current_user_content = []  # Reset after consolidation
                # Append the non-'user' message as is
                consolidated_array.append(message)

        # After the loop, check if there are any remaining 'user' messages to consolidate
        if current_user_content:
            consolidated_array.append({
                "role": "user",
                "content": "\n".join(current_user_content)
            })

        return consolidated_array

    def cortex_rest_api(self,thread_id,message_metadata=None, event_callback=None, temperature=None, fast_mode=False):

        newarray = self.make_messages(thread_id)

        process_flag = False
        if self.thread_history[thread_id]:
            last_message = self.thread_history[thread_id][-1]
            if isinstance(last_message, dict) and 'process_flag' in last_message:
                process_flag = last_message['process_flag'] == "TRUE"

        if process_flag == True and fast_mode == True:
#            logger.info(f"Process flag is set to TRUE for thread {thread_id}, forcing Smart model instead of Fast Mode")
            fast_mode = False

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

                if (self.bot_id in ['eva-x1y2z3','Armen2-ps73td', os.getenv("O1_OVERRIDE_BOT","")]) or (self.bot_id is not None and self.bot_id.endswith('-o1or')):
                    resp += f'\nThis bot is running on {os.getenv("OPENAI_O1_OVERRIDE_MODEL",os.getenv("OPENAI_MODEL_NAME","gpt-4o-2024-11-20"))} in override mode.'
                else:
                    if thread_id in self.thread_fast_mode_map or fast_mode:
                        resp += f"\nFast mode activated for this thread. Model is now {os.getenv('CORTEX_FAST_MODEL_NAME', 'llama3.1-70b')}."
                    else:
                        resp += f"\nSmart mode is active for this thread. Model is now {self.llm_engine}."
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
            if ') says: !model claude-3-5-sonnet' in last_user_message["content"] or last_user_message["content"]=='!model claude-3-5-sonnet':
                self.llm_engine = 'claude-3-5-sonnet'
                resp= f"The model is changed to: {self.llm_engine}"
                curr_resp = resp
            if ') says: !fast on' in last_user_message["content"] or last_user_message["content"] == '!fast on':
                self.thread_fast_mode_map[thread_id] = True
                resp = f"Fast mode activated for this thread. Model is now {os.getenv('CORTEX_FAST_MODEL_NAME', 'llama3.1-70b')}"
                curr_resp = resp
            elif ') says: !fast off' in last_user_message["content"] or last_user_message["content"] == '!fast off':
                if thread_id in self.thread_fast_mode_map:
                    del self.thread_fast_mode_map[thread_id]
                resp = f"Fast mode deactivated for this thread. Model is now {self.llm_engine}"
                curr_resp = resp
        if resp != '':
            #self.thread_history[thread_id] = [message for message in self.thread_history[thread_id] if not (message.get("role","") == "user" and message['content'] == last_user_message['content'])]
            if self.thread_history[thread_id]:
                self.thread_history[thread_id].pop()
            if thread_id in self.thread_model_map and self.thread_model_map[thread_id] is not None:
                self.thread_model_map.pop(thread_id, None)

            if BotOsAssistantSnowflakeCortex.stream_mode == True:
                if self.event_callback:
                    self.event_callback(self.bot_id, BotOsOutputMessage(thread_id=thread_id,
                                                                        status='completed',
                                                                        output=resp,
                                                                        messages=None,
                                                                        input_metadata=json.loads(message_metadata)))
            return None

        if resp == '':

            if (self.bot_id in ['eva-x1y2z3', 'Armen2-ps73td', os.getenv("O1_OVERRIDE_BOT","")]) or (self.bot_id is not None and self.bot_id.endswith('-o1or')):

                if os.getenv("BOT_OS_DEFAULT_LLM_ENGINE",'').lower() == 'openai':
                    api_key = os.getenv("OPENAI_API_KEY")
                    if not api_key:
                        logger.info("OpenAI API key is not set in the environment variables.")
                        return None

                    openai_model = os.getenv("OPENAI_O1_OVERRIDE_MODEL",os.getenv("OPENAI_MODEL_NAME","gpt-4o-2024-11-20"))
                    newarray[0]['role'] = 'user'
                    logger.info(f'**** OpenaAI o1 override for bot {self.bot_id} using model: {openai_model}')
                    try:
                        client = get_openai_client(use_external=True)
                        response = client.chat.completions.create(
                            model=openai_model,
                            messages=newarray,
                        )
                    except Exception as e:
                        logger.info(f"Error occurred while calling OpenAI API with snowpark escallation model {openai_model}: {e}")
                        return None

                    resp = self.thread_full_response.get(thread_id,None)
                    if resp is None:
                        resp = ''
                    curr_resp += response.choices[0].message.content
                    resp += curr_resp
                else:
                    curr_resp = 'Openai not set as default but this is an override bot for openai o1'

            else:

                SNOWFLAKE_HOST = self.client.client.host
                REST_TOKEN = self.client.client.rest.token
                url=f"https://{SNOWFLAKE_HOST}/api/v2/cortex/inference:complete"
                headers = {
                    "Accept": "text/event-stream",
                    "Content-Type": "application/json",
                    "Authorization": f'Snowflake Token="{REST_TOKEN}"',
                }

                if fast_mode:
                    model = os.getenv("CORTEX_FAST_MODEL_NAME", "llama3.1-70b")
                else:
                    model = self.llm_engine

                if not fast_mode and thread_id in self.thread_fast_mode_map and self.thread_fast_mode_map[thread_id]:
                    model = os.getenv("CORTEX_FAST_MODEL_NAME", "llama3.1-70b")

                if temperature is None:
                    temperature = 0.2
                request_data = {
                    "model": model,
                    "messages": newarray,
                    "stream": True,
                    "max_tokens": 4000,
                    "temperature": temperature,
                    "top_p": 1,
                    "top_k": 40,
                    "presence_penalty": 0,
                    "frequency_penalty": 0,
                    "stop": '</function>',
                }
                self.thread_model_map[thread_id] = model

                logger.info(self.bot_name, f" bot_os_cortex calling cortex {model} via REST API, content est tok len=",len(str(newarray))/4)

                start_time = time.time()

                resp = self.thread_full_response.get(thread_id,None)
                curr_resp = ''
                usage = None
                gen_start_time = None
                last_update = None
                if resp is None:
                    resp = ''
                response = requests.post(url, json=request_data, stream=True, headers=headers)

                if response.status_code in (200, 400) and response.text.startswith('{"message":"unknown model'):
                        # Try models in order until one works
                        models_to_try = [
                            os.getenv("CORTEX_PREMIERE_MODEL", "claude-3-5-sonnet"),
                            os.getenv("CORTEX_MODEL", "llama3.1-405b"),
                            os.getenv("CORTEX_FAST_MODEL_NAME", "llama3.1-70b")
                        ]
                        logger.info(f"Model not {self.llm_engine} active. Trying all models in priority order.")
                        for model in models_to_try:

                            request_data["model"] = model
                            self.thread_model_map[thread_id] = model
                            response = requests.post(url, json=request_data, stream=True, headers=headers)

                            if response.status_code == 200 and not response.text.startswith('{"message":"unknown model '):
                                # Found working model
                                self.llm_engine = model
                                os.environ["CORTEX_MODEL"] = model
                                os.environ["CORTEX_PREMIERE_MODEL"] = model
                                logger.info(f"Found working model {model}")
                                break
                            else:
                                logger.info(f"Model {model} not working, trying next model.")
                        else:
                            # No models worked
                            logger.info(f'No available Cortex models found after trying: {models_to_try}')
                            self.thread_model_map[thread_id] = None

                if response.status_code == 400 and re.search("max tokens of [0-9]+ exceeded", str(vars(response))):
                    if self.trim_messages(thread_id):
                        request_data['messages'] = self.make_messages(thread_id)
                        logger.info(self.bot_name, f" second attempt to call cortex {model} via REST API, content est tok len=", len(str(request_data['messages']))/4)
                        response = requests.post(url, json=request_data, stream=True, headers=headers)

                if response.status_code != 200:
                    msg = f"Cortex REST API Error. The Cortex REST API returned an error message. Status code: {response.status_code}."
                    # Dump the entire response object to a string
                    response_string = str(vars(response))
                    msg += f"\nFull response dump:\n{response_string}"
                    logger.info(f"Cortex Error: {msg}")
                    self.thread_history[thread_id] = [message for message in self.thread_history[thread_id] if not (message.get("role","") == "user" and message == last_user_message)]
                    if True or BotOsAssistantSnowflakeCortex.stream_mode == True:
                        if self.event_callback:
                            chunk_size = 3000
                            for i in range(0, len(msg), chunk_size):
                                chunk = msg[i:i+chunk_size]
                                status = 'in_progress' if i + chunk_size < len(msg) else 'completed'
                                self.event_callback(self.bot_id, BotOsOutputMessage(thread_id=thread_id,
                                                                                    status=status,
                                                                                    output=chunk,
                                                                                    messages=None,
                                                                                    input_metadata=json.loads(message_metadata)))
                    return msg
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
                                logger.info('cortex thread stopped by user request')
                                gen_start_time = time.time()
                                break
                            if isinstance(stop_timestamp, datetime.datetime) and (time.time() - stop_timestamp.timestamp()) > 30:
                                self.thread_stop_map.pop(thread_id,None)
                                self.stop_result_map.pop(thread_id,None)
                        curr_line = ''
                        if line:
                            try:
                                decoded_line = line.decode('utf-8')
                                if not decoded_line.strip():
                                    logger.info("Received an empty line.")
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
                                    curr_line += d
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
                                fn_call = False
                                if d != '' and BotOsAssistantSnowflakeCortex.stream_mode == True and (last_update is None and len(resp) >= 15) or (last_update and (time.time() - last_update > 2)):
                                    last_update = time.time()
                                    if self.event_callback:
                                        fn_call = False

                                        if any(partial in curr_resp for partial in ['<fu', '<fun', '<func', '<funct', '<functi', '<functio', '<function', '<function=', '<function>']):
                                            fn_call = True
                                        elif '<function=' in curr_resp[-100:]:
                                            last_function_start = curr_resp.rfind('<function=')
                                            if '</function>' not in curr_resp[last_function_start:]:
                                                fn_call = True
                                        # Check for incomplete <|python_tag|> at the end
                                        # Check for incomplete <|python_tag|> at the end
                                        elif any(partial in curr_resp for partial in [ '<|p', '<|py', '<|pyt', '<|pyth', '<|pytho', '<|python', '<|python_', '<|python_t', '<|python_ta', '<|python_tag', '<|python_tag|']):
                                            fn_call = True
                                        elif '<|python_tag|>' in curr_resp[-100:]:
                                            last_python_tag_start = curr_resp.rfind('<|python_tag|>')
                                            if curr_resp[last_python_tag_start:].strip()[-1] not in ['}', '>']:
                                                fn_call = True
                                        if not fn_call and len(resp)>50:
                                            self.event_callback(self.bot_id, BotOsOutputMessage(thread_id=thread_id,
                                                                                            status='in_progress',
                                                                                            output=resp+" 💬",
                                                                                            messages=None,
                                                                                            input_metadata=json.loads(message_metadata)))
                                if break_after_update:
                                    break

                            except json.JSONDecodeError as e:
                                logger.info(f"Error decoding JSON: {e}")
                                continue

                if gen_start_time is not None:
                    elapsed_time = time.time() - start_time
                    gen_time = time.time() - gen_start_time
                #  logger.info(f"\nRequest to Cortex REST API completed in {elapsed_time:.2f} seconds total, {gen_time:.2f} seconds generating, time to gen start: {gen_start_time - start_time:.2f} seconds")

        else:
            try:
                resp = f"Error calling Cortex: Received status code UNKNOWN" # {response.status_code} with message: {response.reason}"
                curr_resp = resp
            except:
                resp = 'Error calling Cortex'
                curr_resp = resp

        try:
         #   logger.info(json.dumps(usage))
            response_tokens = usage['completion_tokens']
            tokens_per_second_gen = response_tokens / gen_time
            logger.info(f"Cortex {model} warmup: {gen_start_time - start_time:.2f} sec, tok/sec: {tokens_per_second_gen:.2f}")
        except:
            pass

        postfix = ""
        status = 'completed'
        if "</function>" in resp[-30:]:
            postfix = " 💬"

        pattern = re.compile(r'<\|python_tag\|>\{.*?\}')
        match = pattern.search(resp)

        if match and resp.endswith('}'):
            postfix = " 💬"

        pattern_function = re.compile(r'<function>(.*?)</function>(\{.*?\})$')
        match_function = pattern_function.search(resp)

        if match_function and resp.endswith(match_function.group(2)):
            function_name = match_function.group(1)
            params = match_function.group(2)
            newcall = f"<function={function_name}>{params}</function>"
            resp = resp.replace(match_function.group(0), newcall)
            curr_resp = resp
            postfix = " 💬"

        # Handle case where there are no parameters between > and <
        pattern_function_no_params = re.compile(r'<function=(.*?)></function>$')
        match_function_no_params = pattern_function_no_params.search(resp)

        if match_function_no_params and resp.endswith('</function>'):
            function_name = match_function_no_params.group(1)
            newcall = f"<function={function_name}></function>"
            resp = resp.replace(match_function_no_params.group(0), newcall)
            curr_resp = resp
            postfix = " 💬"

        # Handle case where function call is in the format <function=function_name(params)>
        pattern_function_params = re.compile(r'<function=(.*?)\((.*?)\)></function>$')
        match_function_params = pattern_function_params.search(resp)

        if match_function_params and resp.endswith('</function>'):
            function_name = match_function_params.group(1)
            params = match_function_params.group(2)
            try:
                params_dict = json.loads(params.replace("'", '"'))
                newcall = f"<function={function_name}>{json.dumps(params_dict)}</function>"
            except json.JSONDecodeError:
                # If parsing as JSON fails, keep the original format
                newcall = f"<function={function_name}>{{{params}}}</function>"
            resp = resp.replace(match_function_params.group(0), newcall)
            curr_resp = resp
            postfix = " 💬"

        resp = self.fix_tool_calls(resp)

        # Remove trailing 💬 if present
        if resp.endswith('💬'):
            resp = resp[:-1]
            curr_resp = resp
            status = 'in_progress'
        else:
            if '🧰' in resp[-40:]:
                status = 'in_progress'
        if postfix.endswith('💬'):
            status = 'in_progress'

        if resp != '' and ((BotOsAssistantSnowflakeCortex.stream_mode == True) or (
            message_metadata is not None and 'task_meta' in message_metadata and status == 'completed' and not postfix.endswith('💬')
        )):
            if ( BotOsAssistantSnowflakeCortex.stream_mode == True ):
                output = resp + postfix
            else:
                output = curr_resp
         #   if self.thread_model_map[thread_id] is not None:
         #       self.thread_model_map.pop(thread_id, None)
            if self.event_callback:
                self.event_callback(self.bot_id, BotOsOutputMessage(thread_id=thread_id,
                                                                    status=status,
                                                                    output=output,
                                                                    messages=None,
                                                                    input_metadata=json.loads(message_metadata)))
        try:
            logger.info(f"Cortex response: ", json.loads(response.content)["usage"])
        except:
            pass

        self.thread_full_response[thread_id] = resp if resp.endswith('\n') else resp + '\n'
        return(curr_resp)


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
        self.first_message_map[thread_id] = True
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

        fast_mode = False

        thread_id = input_message.thread_id  # Assuming input_message has a thread_id attribute

        if input_message.msg.endswith('<<!!FAST_MODE!!>>'):
          fast_mode = True
          input_message.msg = input_message.msg.rstrip('<<!!FAST_MODE!!>>').rstrip()

        # Check if files are present in input_message
        if hasattr(input_message, 'files') and isinstance(input_message.files, list):
            for file in input_message.files:
                if os.path.isfile(file):
                    # Get the filename from the full path
                    file_name = os.path.basename(file)
                    target_path = f"./runtime/downloaded_files/{thread_id}/" + file_name
                    if file != target_path:
                    # Ensure the target directory exists
                        os.makedirs(os.path.dirname(target_path), exist_ok=True)
                        # Copy the file to the target path
                        shutil.copy2(file, target_path)
                        logger.info(f"File copied from {file} to {target_path}")
                    input_message.msg += f"\n<FILE: Note that the user attached a file {file_name}. It's stored on the server. If you need to use it, add it to stage and use snowpark to read it>"
                else:
                    input_message.msg += f"\n<FILE_NOT_FOUND: Note that the user attached a file {file} but it was not found on the server>"

        if thread_id in self.first_message_map:
            del self.first_message_map[thread_id]
            if input_message.metadata and 'thread_ts' in input_message.metadata:
                fast_mode = False
              #  self.thread_fast_mode_map[thread_id] = False
                #self.llm_engine = os.getenv("CORTEX_FAST_MODEL_NAME")
                if os.getenv("CORTEX_MODEL",False) and os.getenv("CORTEX_FAST_MODEL_NAME",False) and os.getenv("CORTEX_MODEL") != os.getenv("CORTEX_FAST_MODEL_NAME"):
                    logger.info('cortex fast mode = false (set by default for a new slack-based thread)')
                 #  logger.info(f'Switching from {os.getenv("CORTEX_MODEL")} to {os.getenv("CORTEX_FAST_MODEL_NAME")}')
            #        input_message.msg  += ' [NOTE: Also in your response YOU MUST mention in passing that fast mode is active and remind me that I can send !fast off to switch to smart mode.]'
            # Check if channel is in input_message.metadata
            if input_message.metadata and 'channel' in input_message.metadata:
                channel = input_message.metadata['channel']
              #  input_message.msg += f" [FYI Current Slack channel id is: {channel}, but do NOT send _slack_channel_messages to it unless specifically directed to by a user. Just respond normally with regular chat messages.]"

        timestamp = datetime.datetime.now()
        if self.event_callback is None and event_callback is not None:
            self.event_callback = event_callback

        message_payload = input_message.msg

        if thread_id in self.thread_stop_map:
            stop_timestamp = self.thread_stop_map[thread_id]
            if isinstance(stop_timestamp, datetime.datetime) and (time.time() - stop_timestamp.timestamp()) <= 10:
                message_payload = '!NO_RESPONSE_REQUIRED'

        if thread_id in self.thread_busy_list:
            logger.info('Cortex thread busy but putting message anyway')
        else:
            if thread_id in self.thread_tool_call_counter:
                del self.thread_tool_call_counter[thread_id]
            if thread_id in self.thread_tool_call_counter_failsafe:
                del self.thread_tool_call_counter_failsafe[thread_id]


#            if not(message_payload.endswith(') says: !stop') or message_payload =='!stop'):
#                logger.info('bot_os_cortex add_message thread is busy, returning new message to queue')
#                return False

        message_type = 'user'

        # Add fast_mode to metadata if it's True
        if fast_mode:
            if isinstance(input_message.metadata, dict):
                input_message.metadata['fast_mode'] = True
            else:
                input_message.metadata = {'fast_mode': True}

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
        else:
            # Update the first (system) message with current instructions
            if self.thread_history[thread_id][0]["message_type"] == "system":
                self.thread_history[thread_id][0]["content"] = self.instructions

        self.thread_history[thread_id].append(message_object)

        try:

            thread_name = f"Cortex_{self.bot_name}_{thread_id}"
            threading.Thread(target=self.update_threads, name=thread_name, args=(thread_id, timestamp, message_metadata, event_callback, None, fast_mode)).start()

            logger.info(f"Successfully inserted message log for bot_id: {self.bot_id}")
            #self.active_runs.append({"thread_id": thread_id, "timestamp": timestamp})
        except Exception as e:
            logger.error(f"Failed to insert message log for bot_id: {self.bot_id} with error: {e}")

        primary_user = json.dumps({'user_id': input_message.metadata.get('user_id', 'unknown_id'),
                                 'user_name': input_message.metadata.get('user_name', 'unknown_name'),
                                 'user_email': input_message.metadata.get('user_email', 'unknown_email')})
        attachments = []
        self.log_db_connector.insert_chat_history_row(datetime.datetime.now(), bot_id=self.bot_id, bot_name=self.bot_name, thread_id=thread_id,
                                                    message_type='User Prompt', message_payload=input_message.msg, message_metadata=input_message.metadata, files=attachments,
                                                    channel_type=input_message.metadata.get("channel_type", None), channel_name=input_message.metadata.get("channel", None),
                                                    primary_user=primary_user)

    def check_runs(self, event_callback):
        try:
            thread_to_check = self.active_runs.popleft()
        except IndexError:
       #     logger.info(f"BotOsAssistantSnowflakeCortex:check_runs - no active runs for: {self.bot_id}")
            return
        thread_id = thread_to_check["thread_id"]
        timestamp = thread_to_check["timestamp"]
        if thread_id in self.thread_busy_list:
            logger.info(f"BotOsAssistantSnowflakeCortex:check_runs - skipping thread {thread_to_check['thread_id']} as its busy in another run")
            return
        output = None
        if True:
            if thread_id not in self.thread_busy_list:
                self.thread_busy_list.append(thread_id)
            else:
                logger.info(f"BotOsAssistantSnowflakeCortex:check_runs - skipping thread {thread_to_check['thread_id']} as its busy in another run")
                return
            logger.info(f"BotOsAssistantSnowflakeCortex:check_runs - running now, thread {thread_id} ts {timestamp} ")

            thread = self.thread_history.get(thread_id, [])
            user_message = next((msg for msg in thread if (msg.get("message_type") == "user" or msg.get("message_type") == "ipython") and msg.get("timestamp") == timestamp.isoformat()), None)
            # This line searches for the last message in the thread that is of type "assistant" and has a timestamp matching the given timestamp.
            assistant_message = next((msg for msg in reversed(thread) if msg.get("message_type") == "assistant" and msg.get("timestamp") == timestamp.isoformat()), None)
            if assistant_message:
                message_payload = assistant_message.get("content")
              #  logger.info(f"Assistant message found: {message_payload}")
            else:
                logger.info("No assistant message found in the thread with the specified timestamp.")
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

                    decoded_payload = html.unescape(message_payload)

                    # fix tool calls with a missing / in the close block
                    pattern_function_call = re.compile(r'<function=(.*?)>\{.*?\}<function>')
                    match_function_call = pattern_function_call.search(decoded_payload)

                    if match_function_call:
                        function_name = match_function_call.group(1)
                        decoded_payload = re.sub(pattern_function_call, f'<function={function_name}>\\g<0></function>', decoded_payload)
                        decoded_payload = decoded_payload.replace('<function></function>', '</function>')

                    # Fix tool calls with missing > and extra parentheses


                    pattern_function_call = re.compile(r'<function=([^>]+)\((.*?)\)</function>')

                    def fix_function_call(match):
                        function_name = match.group(1)
                        function_args = match.group(2)
                        return f'<function={function_name}>{function_args}</function>'

                    decoded_payload = pattern_function_call.sub(fix_function_call, decoded_payload)

                    if "<TOOL_CALL>" in decoded_payload:
                        self.process_tool_call(thread_id, timestamp, decoded_payload, message_metadata)
                    elif "<function=" in decoded_payload and "</function>" in decoded_payload:
                        self.process_tool_call(thread_id, timestamp, decoded_payload, message_metadata)
                    elif "<function>" in decoded_payload and "</function>" in decoded_payload:
                        self.process_tool_call(thread_id, timestamp, decoded_payload, message_metadata)
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
            except Exception as e:
                logger.info(f"Error retrieving Assistant Response for Thread ID {thread_id} and model {self.llm_engine}: {e}")

        message_metadata_json = json.loads(message_metadata)
        primary_user = json.dumps({'user_id': message_metadata_json.get('user_id', 'unknown_id'),
                                    'user_name': message_metadata_json.get('user_name', 'unknown_name'),
                                    'user_email': message_metadata_json.get('user_email', 'unknown_email')})
        if output is not None:
            self.log_db_connector.insert_chat_history_row(datetime.datetime.now(), bot_id=self.bot_id, bot_name=self.bot_name, thread_id=thread_id,
                                                                    message_type='Assistant Response', message_payload=output, message_metadata=message_metadata,
                                                                    tokens_in=len(user_message['content'].split()), tokens_out=len(output.split()), files=[],
                                                                    channel_type=message_metadata_json.get("channel_type", None), channel_name=message_metadata_json.get("channel", None),
                                                                    primary_user=primary_user)

        if thread_id in self.thread_busy_list:
            self.thread_busy_list.remove(thread_id)

    def process_tool_call(self, thread_id, timestamp, message_payload, message_metadata):
        import json

# <|python_tag|><function>_run_query>{"query": "SELECT COUNT(ID) FROM "SPIDER_DATA"."BASEBALL"."HOME_GAME"", "connection": "Snowflake", "max_rows": "100"}</function>

        # Check if thread_id is in self.thread_tool_call_counter

        # TODO add the counter reset on add_message

        if thread_id not in self.thread_tool_call_counter:
            self.thread_tool_call_counter[thread_id] = 1
        else:
            self.thread_tool_call_counter[thread_id] += 1
        if thread_id not in self.thread_tool_call_counter_failsafe:
            self.thread_tool_call_counter_failsafe[thread_id] = 1
        else:
            self.thread_tool_call_counter_failsafe[thread_id] += 1


        if self.thread_tool_call_counter_failsafe[thread_id] > 102:
            logger.info("bot_os_cortex runaway_error_102")
            return

        model = self.thread_model_map.get(thread_id, None)
        claude_model = False
        if model is not None and model.startswith('claude'):
            claude_model = True

        if self.thread_tool_call_counter[thread_id] > 22 and not claude_model:
            logger.info("bot_os_cortex runaway_error_22")
            return

        if self.thread_tool_call_counter[thread_id] > 20 and not claude_model:
            error_message = "Error: more than 20 successive tool calls have occurred on this thread. The user needs to send a new message before any more tool calls will be processed."
            cb_closure = self._generate_callback_closure(thread_id, timestamp, message_metadata)
            logger.info("bot_os_cortex runaway_error_20 ",error_message)
            cb_closure(error_message)
            return
        if self.thread_tool_call_counter_failsafe[thread_id] > 100:
            error_message = "Error: more than 100 successive tool calls have occurred on this thread during a process run without input from a user. The user needs to send a new message before any more tool calls will be processed.  This is a failsafe against looping or runaway processes."
            cb_closure = self._generate_callback_closure(thread_id, timestamp, message_metadata)
            logger.info("bot_os_cortex runaway_error_100 ",error_message)
            cb_closure(error_message)
            return

        # If the counter is > 10, take no action
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
                   # logger.error(f"Failed to decode function call JSON {function_call_str}: {e}")
                    cb_closure = self._generate_callback_closure(thread_id, timestamp, message_metadata)
                    cb_closure(f"Failed to decode function call JSON {function_call_str}: {e}")
                    return
            function_to_call = function_name
            arguments = arguments_json
            if 'arguments_str' not in locals():
                arguments_str = json.dumps(arguments)
            logger.info(f"Function to call: {function_to_call}")
            logger.info(f"Argument keys: {', '.join(arguments.keys())}")
            meta = json.loads(message_metadata)
            primary_user = json.dumps({'user_id': meta.get('user_id', 'unknown_id'),
                                    'user_name': meta.get('user_name', 'unknown_name'),
                                    'user_email': meta.get('user_email', 'unknown_email')})
            log_readable_payload = function_name+"("+arguments_str+")"
            self.log_db_connector.insert_chat_history_row(datetime.datetime.now(), bot_id=self.bot_id, bot_name=self.bot_name, thread_id=thread_id,
                                                        message_type='Tool Call', message_payload=log_readable_payload,
                                                        message_metadata={'func_name':function_to_call, 'func_args':arguments},
                                                        channel_type=meta.get("channel_type", None), channel_name=meta.get("channel", None),
                                                        primary_user=primary_user)
#              execute_function(func_name, func_args, self.all_functions, callback_closure,
#                                       thread_id = thread_id, bot_id=self.bot_id, status_update_callback=event_callback if event_callback else None, session_id=self.assistant.id if self.assistant.id is not None else None, input_metadata=run.metadata if run.metadata is not None else None )#, dispatch_task_callback=dispatch_task_callback)

            execute_function(function_to_call, json.dumps(arguments), self.available_functions, cb_closure, thread_id, self.bot_id)
        except json.JSONDecodeError as e:
            logger.info(f"Failed to decode tool call JSON: {e}")
            cb_closure = self._generate_callback_closure(thread_id, timestamp, message_metadata)
            cb_closure(f"Failed to decode tool call JSON {tool_call_str}: {e}.  Did you make sure to escape any double quotes that are inside another")
        except Exception as e:
            logger.error(f"Error processing tool call: {e}")
            cb_closure(f"Error processing tool call: {e}")


    def _submit_tool_outputs(self, thread_id, timestamp, results, message_metadata, func_call_details=None):
        """
        Inserts tool call results back into the genesis_test.public.genesis_threads table.
        """

        if results is None:
            results = ''

        # Check if fast_mode is True in message_metadata
        fast_mode = False
        if message_metadata:
            try:
                metadata_dict = json.loads(message_metadata)
                fast_mode = metadata_dict.get('fast_mode', False)
            except json.JSONDecodeError:
                logger.warning("Failed to parse message_metadata as JSON")

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
            # Check if this is an image generation result
            if isinstance(results, dict) and 'local_file_name' in results and results.get('success', True):
                file_path = results['local_file_name']
                # Add the image reference to the results
                results_copy = results.copy()
                results_copy['image_markdown'] = f"![Generated Image]({file_path})"
                results = json.dumps(results_copy, default=custom_serializer)
            else:
                results = json.dumps(results, default=custom_serializer)
        else:
            results = str(results)


        prefix = ""
        prefix = 'SYSTEM MESSAGE: Here are the results of the tool call. Note that the end user has not seen these details:\n\n'

        # For image generation results, add the image markdown to the thread_full_response
        if isinstance(results, str) and '"local_file_name"' in results and '"success": true' in results:
            try:
                results_dict = json.loads(results)
                if 'local_file_name' in results_dict:
                    file_path = results_dict['local_file_name']
                    if thread_id not in self.thread_full_response:
                        self.thread_full_response[thread_id] = ""
                    self.thread_full_response[thread_id] += f"\n\n![Generated Image]({file_path})"
            except:
                pass

        message_object = {
            "message_type": "user",
            "content": str(prefix) + results, # Convert both to strings explicitly
            "timestamp": new_ts.isoformat(),
            "metadata": message_metadata,
        }

        function_name = ''
        arguments = ''

        try:
            results_json = json.loads(results)
        except json.JSONDecodeError as e:
           # logger.error(f"Failed to decode results JSON: {e}")
            results_json = results  # Fallback to original results if JSON decoding fails
        if isinstance(results, dict) and 'success' in results and results['success']:
            logger.info(f"Tool call was successful for Thread ID {thread_id}")
        if func_call_details is not None:
            function_name = func_call_details.get('function_name')
            if function_name == '_run_process':
                message_object['process_flag'] = 'TRUE'
            if function_name == '_run_process':
                if isinstance(results_json, dict) and ('success' in results_json and results_json['success']) or ('Success' in results_json and results_json['Success']):
                    if thread_id in self.thread_tool_call_counter:
                        del self.thread_tool_call_counter[thread_id]
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

        if isinstance(results_json, str) and results_json.strip() == "Error, your query was cut off.  Query must be complete and end with a semicolon.  Include the full query text, with an ; on the end and RUN THIS TOOL AGAIN NOW! Also replace all ' (single quotes) in the query with <!Q!>":
            hightemp = 0.6
            logger.info('Cortex query cut off, calling update threads with Hightemp')
        else:
            hightemp = None
        if thread_id in self.last_stop_time_map and timestamp < self.last_stop_time_map[thread_id]:
            logger.info('bot_os_cortex _submit_tool_outputs stop message received, not rerunning thread with outputs')
            self.stop_result_map[thread_id] = 'stopped'
        else:
            self.update_threads(thread_id, new_ts, message_metadata=message_metadata, temperature=hightemp, fast_mode=fast_mode)
   #     self.active_runs.append({"thread_id": thread_id, "timestamp": new_ts})
        meta = json.loads(message_metadata)
        primary_user = json.dumps({'user_id': meta.get('user_id', 'unknown_id'),
                     'user_name': meta.get('user_name', 'unknown_name'),
                     'user_email': meta.get('user_email', 'unknown_email')})
        self.log_db_connector.insert_chat_history_row(datetime.datetime.now(), bot_id=self.bot_id, bot_name=self.bot_name, thread_id=thread_id,
                                                          message_type='Tool Output', message_payload=results,
                                                          message_metadata={'function_name':function_name, 'arguments': arguments},
                                                          channel_type=meta.get("channel_type", None), channel_name=meta.get("channel", None),
                                                          primary_user=primary_user)
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

    def update_threads(self, thread_id, timestamp, message_metadata = None, event_callback = None, temperature = None, fast_mode = False):
        """
        Executes the SQL query to update threads based on the provided SQL, incorporating self.cortex... tables.
        """

        if thread_id not in self.thread_busy_list:
            self.thread_busy_list.append(thread_id)

  #      resp = self.cortex_rest_api(thread_id)

        resp = self.cortex_complete(thread_id=thread_id, message_metadata=message_metadata, event_callback=event_callback, temperature=temperature, fast_mode = fast_mode)
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


