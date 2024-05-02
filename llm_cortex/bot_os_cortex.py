from collections import deque
import datetime
from decimal import Decimal
import html
import json
import uuid
import urllib.parse

from connectors.snowflake_connector import SnowflakeConnector
from core.bot_os_assistant_base import BotOsAssistantInterface, execute_function

import logging

from core.bot_os_input import BotOsInputMessage, BotOsOutputMessage
logger = logging.getLogger(__name__)

class BotOsAssistantSnowflakeCortex(BotOsAssistantInterface):
    def __init__(self, name:str, instructions:str, 
                tools:list[dict] = {}, available_functions={}, files=[], 
                update_existing=False, log_db_connector=None, bot_id='default_bot_id', bot_name='default_bot_name', all_tools:list[dict]={}, all_functions={},all_function_to_tool_map={}) -> None:
        super().__init__(name, instructions, tools, available_functions, files, update_existing)
        self.active_runs = deque()
        self.llm_engine = "mistral-large"
        self.instructions = instructions + '. Wrap calls to tools in a <TOOL_CALL>...<TOOL_CALL/> block with no other text.'
        self.tools = tools
        self.available_functions = available_functions
        self.bot_id = bot_id
        self.bot_name = bot_name
        self.client = SnowflakeConnector(connection_name='Snowflake')
        logger.debug("BotOsAssistantSnowflakeCortex:__init__ - SnowflakeConnector initialized")

    def create_thread(self) -> str:
        thread_id = f"Cortex_thread_{uuid.uuid4()}"
        timestamp = datetime.datetime.now()
        message_type = 'System Prompt'
    
        insert_query = """
        INSERT INTO genesis_test.public.genesis_threads (
            TIMESTAMP, BOT_ID, BOT_NAME, THREAD_ID, MESSAGE_TYPE, MESSAGE_PAYLOAD, MESSAGE_METADATA
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

        try:
            cursor = self.client.connection.cursor()
            cursor.execute(insert_query, (
                timestamp, self.bot_id, self.bot_name, thread_id, message_type, self.instructions, "",
            ))
            self.client.connection.commit()
            cursor.execute(insert_query, (
                timestamp, self.bot_id, self.bot_name, thread_id, message_type, str(self.tools), "",
            ))
            self.client.connection.commit()

            logger.info(f"Successfully inserted system prompt for thread_id: {thread_id}")
        except Exception as e:
            logger.error(f"Failed to insert system prompt for thread_id: {thread_id} with error: {e}")
        return thread_id
    
    def add_message(self, input_message:BotOsInputMessage):
        timestamp = datetime.datetime.now()
        thread_id = input_message.thread_id  # Assuming input_message has a thread_id attribute
        message_payload = input_message.msg 
        message_type = 'User Prompt'
        message_metadata = json.dumps(input_message.metadata)  # Assuming BotOsInputMessage has a metadata attribute that needs to be converted to string
    
        insert_query = """
        INSERT INTO genesis_test.public.genesis_threads (
            TIMESTAMP, BOT_ID, BOT_NAME, THREAD_ID, MESSAGE_TYPE, MESSAGE_PAYLOAD, MESSAGE_METADATA
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

        try:
            cursor = self.client.connection.cursor()
            cursor.execute(insert_query, (
                timestamp, self.bot_id, self.bot_name, thread_id, message_type, message_payload, message_metadata,
            ))
            self.client.connection.commit()
            logger.info(f"Successfully inserted message log for bot_id: {self.bot_id}")
            self.active_runs.append({"thread_id": thread_id, "timestamp": timestamp})
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
            query = """
            SELECT message_payload, message_metadata FROM genesis_test.public.genesis_threads_dynamic
            WHERE thread_id = %s AND model_name = %s AND message_type = 'Assistant Response' AND timestamp = %s
            """
            try:
                cursor = self.client.connection.cursor()
                cursor.execute(query, (thread_id, self.llm_engine, timestamp))
                responses = cursor.fetchall()
                if responses:
                    for message_payload, message_metadata in responses:
                        logger.info(f"Response for Thread ID {thread_id}, {timestamp} with model {self.llm_engine}: {message_payload}")
                        decoded_payload = html.unescape(message_payload)
                        if "<TOOL_CALL>" in decoded_payload:
                            self.process_tool_call(thread_id, timestamp, decoded_payload, message_metadata)
                            #self.active_runs.append(thread_to_check)
                        else:
                            if message_metadata == '':
                                message_metadata = '{}'                  
                            event_callback(self.bot_id, BotOsOutputMessage(thread_id=thread_id, 
                                                                     status="completed", 
                                                                     output=message_payload, 
                                                                     messages="", 
                                                                     input_metadata=json.loads(message_metadata)))
                else:
                    logger.info(f"No Assistant Response found for Thread ID {thread_id} {timestamp} and model {self.llm_engine}")
                    self.active_runs.append(thread_to_check)
            except Exception as e:
                logger.error(f"Error retrieving Assistant Response for Thread ID {thread_id} and model {self.llm_engine}: {e}")

    def process_tool_call(self, thread_id, timestamp, message_payload, message_metadata):
        import json
        start_tag = "<TOOL_CALL>"
        end_tag = "</TOOL_CALL>"
        start_index = message_payload.find(start_tag) + len(start_tag)
        end_index = message_payload.find(end_tag)
        tool_call_str = message_payload[start_index:end_index].strip()
        try:
            tool_call_data = json.loads(tool_call_str)
            function_to_call = tool_call_data.get("function")
            arguments = tool_call_data.get("arguments", {})
            cb_closure = self._generate_callback_closure(thread_id, timestamp, message_metadata)
            execute_function(function_to_call, json.dumps(arguments), self.available_functions, cb_closure, thread_id, self.bot_id)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode tool call JSON: {e}")
        except Exception as e:
            logger.error(f"Error processing tool call: {e}")

    def _submit_tool_outputs(self, thread_id, timestamp, results, message_metadata):
        """
        Inserts tool call results back into the genesis_test.public.genesis_threads table.
        """
        insert_query = """
        INSERT INTO genesis_test.public.genesis_threads (
            TIMESTAMP, BOT_ID, BOT_NAME, THREAD_ID, MESSAGE_TYPE, MESSAGE_PAYLOAD, MESSAGE_METADATA
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        try:
            cursor = self.client.connection.cursor()
            new_timestamp = datetime.datetime.now()
            def default_converter(o):
                if isinstance(o, Decimal):
                    return float(o)  # Convert Decimal to float for JSON serialization
                elif isinstance(o, datetime):
                    return o.isoformat()
                raise TypeError(f"Object of type {o.__class__.__name__} is not JSON serializable")
            results_str = json.dumps(results, default=default_converter, indent=2)
            cursor.execute(insert_query, (new_timestamp, self.bot_id, self.bot_name, thread_id, "Tool Response", results_str,
                                          message_metadata))
            self.client.connection.commit()
            self.active_runs.append({"thread_id": thread_id, "timestamp": new_timestamp})

            logger.info(f"Successfully inserted tool call results for Thread ID {thread_id} and Tool Call ID {new_timestamp} old: {timestamp}")
        except Exception as e:
            logger.error(f"Failed to insert tool call results for Thread ID {thread_id} and Tool Call ID {timestamp}: {e}")
            self.client.connection.rollback()

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
            self._submit_tool_outputs(thread_id, timestamp, error_string, message_metadata)
      return callback_closure

   