from flask import Blueprint, request, render_template, make_response
import uuid
import os
from connectors.snowflake_connector import SnowflakeConnector
from connectors.sqlite_connector import SqliteConnector
import logging
from core.bot_os_input import BotOsInputAdapter, BotOsInputMessage, BotOsOutputMessage
from collections import deque
import json
import time

logger = logging.getLogger(__name__)

genesis_source = os.getenv('GENESIS_SOURCE',default="BigQuery")
if genesis_source == 'Sqlite':
    db_connector = SqliteConnector(connection_name="Sqlite")
elif genesis_source == 'Snowflake':    
    db_connector = SnowflakeConnector(connection_name='Snowflake')
else:
    raise ValueError('Invalud Source')

class UDFBotOsInputAdapter(BotOsInputAdapter):

    db_connector = db_connector

    def __init__(self):
        super().__init__()
        self.response_map = {}
    #    self.proxy_messages_in = []
        self.events = deque()
        self.genbot_internal_project_and_schema = os.getenv('GENESIS_INTERNAL_DB_SCHEMA','None')
        self.bot_id = {}
        self.pending_map = {}
        self.events_map = {}
        self.in_to_out_thread_map = {}

    def add_event(self, event):
        self.events.append(event)
        if event and event.hasattr('uuid'):
            self.events_map[event['uuid']] = event

    def add_back_event(self, metadata=None):
        event = self.events_map.get(metadata['input_uuid'], None)
        if event is not None:
            self.events.append(event)

    def get_input(self, thread_map=None,  active=None, processing=None, done_map=None):
        if len(self.events) == 0:
            return None        
        try:
            event = self.events.popleft()
        except IndexError:
            return None
        uu = event.get('uuid',None)
        self.bot_id = event.get('bot_id', {})
        metadata = {}
        if uu:
            metadata["input_uuid"] = uu
        metadata["thread_id"] = event.get('thread_id')
        metadata["channel_type"] = "Streamlit"
        metadata["channel_name"] = ""
        metadata['is_bot'] = 'FALSE'
        metadata["user_id"] = self.bot_id.get('user_id', 'Unknown User ID')
        metadata["user_name"] = self.bot_id.get('user_name', 'Unknown User')
        metadata["user_email"] = self.bot_id.get('user_id', 'Unknown Email')
        return BotOsInputMessage(thread_id=event.get('thread_id'), msg=event.get('msg'), metadata=metadata)

 

    def handle_response(self, session_id: str, message: BotOsOutputMessage, in_thread=None, in_uuid=None, task_meta=None):
        # Here you would implement how the Flask app should handle the response.
        # For example, you might send the response back to the client via a WebSocket
        # or store it in a database for later retrieval.
        #print("UDF output: ",message.output, ' in_uuid ', in_uuid)
        
        self.in_to_out_thread_map[message.input_metadata['thread_id']] = message.thread_id
        if in_uuid is not None:
            if message.output == '!NO_RESPONSE_REQUIRED':
                self.response_map[in_uuid] = "(no response needed)"
            else:
                self.response_map[in_uuid] = message.output
        # write the value to the hybrid table
        if in_uuid in self.pending_map:
            UDFBotOsInputAdapter.db_connector.db_insert_llm_results(in_uuid, message.output)
            del self.pending_map[in_uuid]
        else:
            UDFBotOsInputAdapter.db_connector.db_update_llm_results(in_uuid, message.output)
      # pass

    def lookup_fn(self):
        '''
        Main handler for providing a web UI.
        '''
        if request.method == "POST":
            # getting input in HTML form
            input_text = request.form.get("input")
            # display input and output
            #print("lookup input: ", input_text )
            resp = "not found"
            #print(response_map)
            if input_text in self.response_map.keys():
                resp = self.response_map[input_text]
            #print("lookup resp: ", resp )
            return render_template("lookup_ui.html",
                uuid_input=input_text,
                response=resp)
        return render_template("lookup_ui.html")

    def submit_fn(self):
        '''
        Main handler for providing a web UI.
        '''
        if request.method == "POST":
            # getting input in HTML form
            input_text = request.form.get("input")
            thread_text = request.form.get("thread_text")
            # display input and output
            return render_template("submit_ui.html",
                echo_input=input_text,
                thread_id=thread_text,
                echo_reponse=self.submit(input_text, thread_text),
                thread_output=thread_text)
        return render_template("submit_ui.html")



    def submit(self, input, thread_id, bot_id):
        
        if type(bot_id) == str:
            bot_id = json.loads(bot_id)
        
        uu = str(uuid.uuid4())
        # self.proxy_messages_in.append({"msg": input, "uuid": uu, "thread_id": thread_id, "bot_id": bot_id})
        
        self.add_event({"msg": input, "thread_id": thread_id, "uuid": uu, "bot_id": bot_id})
        #UDFBotOsInputAdapter.db_connector.db_insert_llm_results(uu, "")
        self.pending_map[uu] = True
        return uu


    def healthcheck_fn(self):
        return "I'm ready!"


    def submit_udf_fn(self):
        '''
        Main handler for input data sent by Snowflake.
        '''
        message = request.json
     #   logger.debug(f'Received request: {message}')

        if message is None or not message['data']:
            logger.info('Received empty message')
            return {}

        # input format:
        #   {"data": [
        #     [row_index, column_1_value, column_2_value, ...],
        #     ...
        #   ]}
        input_rows = message['data']
      #  logger.info(f'Received {len(input_rows)} rows')

        # output format:
        #   {"data": [
        #     [row_index, column_1_value, column_2_value, ...}],
        #     ...
        #   ]}
        
        output_rows = [[row[0], self.submit(*row[1:])] for row in input_rows]
      
        response = make_response({"data": output_rows})
        response.headers['Content-type'] = 'application/json'
       # logger.debug(f'Sending response: {response.json}')
        return response

    def test_udf_strings(self):
        test_submit = """
        curl -X POST http://127.0.0.1:8080/udf_proxy/jl-local-elsa-test-1/submit_udf \
            -H "Content-Type: application/json" \
            -d '{"data": [[1, "whats the secret word?", "111"]]}'
        """
        test_response_udf = """
        curl -X POST http://127.0.0.1:8080/udf_proxy/lookup_udf \
            -H "Content-Type: application/json" \
            -d '{"data": [[1, "4764a0e9-ee8f-4605-b3f4-e72897ba7347"]]}'
        """

    def lookup_udf_fn(self):
        '''
        Main handler for input data sent by Snowflake.
        '''
        message = request.json
        #logger.debug(f'Received request: {message}')

        if message is None or not message['data']:
            #logger.info('Received empty message')
            return {}

        # input format:
        #   {"data": [
        #     [row_index, column_1_value, column_2_value, ...],
        #     ...
        #   ]}

        input_rows = message['data']
        #logger.info(f'Received {len(input_rows)} rows')

        # output format:
        #   {"data": [
        #     [row_index, column_1_value, column_2_value, ...}],
        #     ...
        #   ]}

        input_text = input_rows[0][1]
        #print("lookup input: ", input_text )
        resp = "not found"
        if input_text in self.response_map.keys():
            resp =self.response_map[input_text]
        #print("lookup resp: ", resp )
        
        output_rows = [[row[0], resp] for row in input_rows]
        #logger.info(f'Produced {len(output_rows)} rows')

        response = make_response({"data": output_rows})
        response.headers['Content-type'] = 'application/json'
        logger.debug(f'Sending response: {response.json}')
        return response

    def lookup_udf(self, input_text:str):
        if input_text in self.response_map.keys():
            return self.response_map[input_text]
        else:
            return None
