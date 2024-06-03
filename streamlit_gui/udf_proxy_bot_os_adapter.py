from flask import Blueprint, request, render_template, make_response
import uuid
import logging
from core.bot_os_input import BotOsInputAdapter, BotOsInputMessage, BotOsOutputMessage
from collections import deque
import json

logger = logging.getLogger(__name__)

class UDFBotOsInputAdapter(BotOsInputAdapter):
    def __init__(self):
        super().__init__()
        self.response_map = {}
        self.proxy_messages_in = []
        self.events = deque()

    def add_event(self, event):
        self.events.append(event)

    def get_input(self):
        if len(self.events) == 0:
            return None        
        try:
            event = self.events.popleft()
        except IndexError:
            return None
        uu = event.get('uuid',None)
        bot_id = event.get('bot_id', {})
        metadata = {}
        if uu:
            metadata["input_uuid"] = uu
        metadata["channel_type"] = "Streamlit"
        metadata["channel_name"] = ""
        metadata["user_id"] = bot_id.get('user_id', 'Unknown User ID')
        metadata["user_name"] = bot_id.get('user_name', 'Unknown User')
        return BotOsInputMessage(thread_id=event.get('thread_id'), msg=event.get('msg'), metadata=metadata)



    def handle_response(self, session_id: str, message: BotOsOutputMessage, in_thread=None, in_uuid=None, task_meta=None):
        # Here you would implement how the Flask app should handle the response.
        # For example, you might send the response back to the client via a WebSocket
        # or store it in a database for later retrieval.
        #print("UDF output: ",message.output, ' in_uuid ', in_uuid)
        if in_uuid is not None:
            if message.output == '!NO_RESPONSE_REQUIRED':
                self.response_map[in_uuid] = "(no response needed)"
            else:
                self.response_map[in_uuid] = message.output
        pass

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
        self.proxy_messages_in.append({"msg": input, "uuid": uu, "thread_id": thread_id, "bot_id": bot_id})
       
        self.add_event({"msg": input, "thread_id": thread_id, "uuid": uu, "bot_id": bot_id})
        return uu


    def healthcheck_fn(self):
        return "I'm ready!"


    def submit_udf_fn(self):
        '''
        Main handler for input data sent by Snowflake.
        '''
        message = request.json
        logger.debug(f'Received request: {message}')

        if message is None or not message['data']:
            logger.info('Received empty message')
            return {}

        # input format:
        #   {"data": [
        #     [row_index, column_1_value, column_2_value, ...],
        #     ...
        #   ]}
        input_rows = message['data']
        logger.info(f'Received {len(input_rows)} rows')

        # output format:
        #   {"data": [
        #     [row_index, column_1_value, column_2_value, ...}],
        #     ...
        #   ]}
        output_rows = [[row[0], self.submit(*row[1:])] for row in input_rows]
        logger.info(f'Produced {len(output_rows)} rows')

        response = make_response({"data": output_rows})
        response.headers['Content-type'] = 'application/json'
        logger.debug(f'Sending response: {response.json}')
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

