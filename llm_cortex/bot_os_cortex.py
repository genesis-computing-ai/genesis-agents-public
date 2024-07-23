from collections import deque
import datetime
from decimal import Decimal
import html
import json
import os
import time
import uuid
import threading
from typing_extensions import override

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

        self.llm_engine = 'llama3.1-405b'
        self.instructions = instructions + '. To call a tool, return only the unescaped tool call JSON in a <TOOL_CALL></TOOL_CALL> block with no other text. DO NOT HALUCINATE RESULTS OF TOOL CALLS, actually call the tools!'
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
            cursor = self.client.connection.cursor()
            cursor.execute(insert_query, (
                timestamp, self.bot_id, self.bot_name, thread_id, message_type, self.instructions, "",
            ))
            self.client.connection.commit()
            cursor.execute(insert_query, (
                timestamp, self.bot_id, self.bot_name, thread_id, message_type, str(self.tools), "",
            ))
            self.client.connection.commit()
          #  thread_name = f"Cortex_{thread_id}"
          #  threading.Thread(target=self.update_threads, args=(thread_id, thread_name, None)).start()

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
    
        insert_query = f"""
        INSERT INTO {self.cortex_threads_schema_input_table} (
            TIMESTAMP, BOT_ID, BOT_NAME, THREAD_ID, MESSAGE_TYPE, MESSAGE_PAYLOAD, MESSAGE_METADATA
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

        try:
            cursor = self.client.connection.cursor()
            cursor.execute(insert_query, (
                timestamp, self.bot_id, self.bot_name, thread_id, message_type, message_payload, message_metadata,
            ))
            self.client.connection.commit()
            thread_name = f"Cortex_{thread_id}"
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
            logger.warn("BotOsAssistantSnowflakeCortex:check_runs - running now")
            query = f"""
            SELECT message_payload, message_metadata FROM {self.cortex_threads_schema_output_table}
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
                        elif "<|python_tag|>" in decoded_payload:
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
        start_tag = "<TOOL_CALL>"
        end_tag = "</TOOL_CALL>"
        start_index = message_payload.find(start_tag) + len(start_tag)
        end_index = message_payload.find(end_tag)
        if start_index == -1:
            start_tag = "<|python_tag|>"
            end_tag = "<|eom_id|>"
            start_index = message_payload.find(start_tag) + len(start_tag)
            end_index = message_payload.find(end_tag)        
        tool_call_str = message_payload[start_index:end_index].strip()
        try:
            cb_closure = self._generate_callback_closure(thread_id, timestamp, message_metadata)
            tool_call_data = json.loads(tool_call_str)
            function_to_call = tool_call_data.get("name")
            arguments = tool_call_data.get("parameters", {})
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
            threading.Thread(target=self.update_threads, name=thread_name, args=(thread_id, new_timestamp)).start()
            #self.update_threads(thread_id, new_timestamp)
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
        context_limit = 128000 * 4 #32000 * 4
        update_query = f"""
        insert into {self.cortex_threads_schema_output_table}
                        with input as 
                        (
                        select 
                            i.* from {self.cortex_threads_schema_input_table} i
                            LEFT JOIN {self.cortex_threads_schema_output_table} o ON i.thread_id = o.thread_id and i.timestamp = o.timestamp
                            WHERE o.thread_id IS NULL
                            AND i.thread_id = '{thread_id}'
                        ),

                        prior_in_thread as
                        (
                        select 
                            i.* from {self.cortex_threads_schema_input_table} i
                            where i.thread_id = '{thread_id}'
                        ),

                        system_prompts as 
                        (
                        SELECT
                        i.thread_id,
                        ARRAY_TO_STRING(ARRAY_AGG(CASE WHEN i.message_type = 'System Prompt' THEN CONCAT('<System Prompt>', i.message_payload, '</System Prompt>') ELSE NULL END) WITHIN GROUP (ORDER BY i.timestamp ASC), ' ') AS system_prompts_payload
                        FROM
                        prior_in_thread i
                        GROUP BY
                        i.thread_id
                        ),

                        latest_tool_response as 
                        (
                        SELECT
                        p.thread_id,
                        '<Tool Response>' || p.message_payload || '</Tool Response>' as latest_tool_response_payload,
                        p.timestamp as latest_tool_response_timestamp
                        FROM
                        prior_in_thread p
                        WHERE p.timestamp = (SELECT MAX(i.timestamp) FROM prior_in_thread i WHERE i.thread_id = p.thread_id AND i.message_type = 'Tool Response')
                        AND p.message_type = 'Tool Response'
                        ),

                        latest_assistant_response as 
                        (
                        SELECT
                        p.thread_id,
                        '<Assistant Response>' || p.message_payload || '</Assistant Response>' as latest_assistant_response_payload,
                        p.timestamp as latest_assistant_response_timestamp
                        FROM
                        prior_in_thread p
                        WHERE p.timestamp = (SELECT MAX(i.timestamp) FROM prior_in_thread i WHERE i.thread_id = p.thread_id AND i.message_type = 'Assistant Response')
                        AND p.message_type = 'Assistant Response'
                        ),

                        latest_user_prompt as 
                        (
                        SELECT
                        p.thread_id,
                        '<User Prompt>' || p.message_payload || '</User Prompt>' as latest_user_prompt_payload,
                        p.timestamp as latest_user_prompt_timestamp
                        FROM
                        prior_in_thread p
                        WHERE p.timestamp = (SELECT MAX(i.timestamp) FROM prior_in_thread i WHERE i.thread_id = p.thread_id AND i.message_type = 'User Prompt')
                        AND p.message_type = 'User Prompt'
                        ),

                        summarized_messages as 
                        (
                        SELECT
                        p.thread_id,
                        '<Prior Thread History>'  || ARRAY_TO_STRING(ARRAY_AGG(CASE WHEN message_type NOT IN ('System Prompt') AND timestamp < COALESCE(lup.latest_user_prompt_timestamp, '9999-12-31') THEN CONCAT('<', message_type, '>', message_payload, '</', message_type, '>') ELSE NULL END) WITHIN GROUP (ORDER BY timestamp ASC), ' ') || '</Prior Prompt Summary>' AS summarized_payload
                        FROM
                        prior_in_thread p
                        LEFT JOIN latest_user_prompt lup ON p.thread_id = lup.thread_id
                        GROUP BY
                        p.thread_id
                        ),

                        threads as 
                        (
                        SELECT
                        sp.thread_id,
                        SUBSTRING(CONCAT(sp.system_prompts_payload, ' ', COALESCE(sm.summarized_payload, ''), ' ', COALESCE(lup.latest_user_prompt_payload, ''), ' ', COALESCE(lar.latest_assistant_response_payload, ''), ' ', COALESCE(ltr.latest_tool_response_payload, '')), 1, {context_limit}) AS concatenated_payload
                        FROM
                        system_prompts sp
                        LEFT JOIN latest_user_prompt lup ON sp.thread_id = lup.thread_id
                        LEFT JOIN latest_assistant_response lar ON sp.thread_id = lar.thread_id
                        LEFT JOIN latest_tool_response ltr ON sp.thread_id = ltr.thread_id
                        LEFT JOIN summarized_messages sm ON sp.thread_id = sm.thread_id
                        )

                        select 
                            *, 'user', '' from input
                        union all 
                        select 
                            i.timestamp,
                            i.bot_id,
                            i.bot_name,
                            i.thread_id,
                            'Assistant Response',
                            SNOWFLAKE.CORTEX.COMPLETE('{self.llm_engine}', LEFT(concatenated_payload, {context_limit})) as message_payload,
                            i.message_metadata, 
                            0 as tokens_in,
                            0 as tokens_out,
                            '{self.llm_engine}',
                            LEFT(concatenated_payload, 16000)
                        from input as i
                        join threads on i.thread_id = threads.thread_id
                        where i.message_type = 'User Prompt' or i.message_type = 'Tool Response';
        """
# '<Prior Prompt Summary>' || SNOWFLAKE.CORTEX.COMPLETE('{self.llm_engine}', 'Summarize this:' || ARRAY_TO_STRING(ARRAY_AGG(CASE WHEN message_type NOT IN ('System Prompt') AND timestamp < COALESCE(lup.latest_user_prompt_timestamp, '9999-12-31') THEN CONCAT('<', message_type, '>', message_payload, '</', message_type, '>') ELSE NULL END) WITHIN GROUP (ORDER BY timestamp ASC), ' ')) || '</Prior Prompt Summary>' AS summarized_payload
        try:
            cursor = self.client.connection.cursor()
            start_time = time.time()
            logger.info(f"BotOsAssistantSnowflakeCortex:update_threads -- update_query = {update_query}")
            cursor.execute(update_query)
            self.client.connection.commit()
            elapsed_time = time.time() - start_time
            logger.warn(f"BotOsAssistantSnowflakeCortex:update_threads -- took {elapsed_time} seconds.")
            if timestamp:
                self.active_runs.append({"thread_id": thread_id, "timestamp": timestamp})
            logger.info("Successfully updated threads.")
        except Exception as e:
            logger.error(f"Failed to update threads: {e}")
            self.client.connection.rollback()

