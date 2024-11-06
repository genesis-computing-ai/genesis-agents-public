from flask import Blueprint, request, render_template, make_response
import uuid


from core.logging_config import logger

lookup_ui = Blueprint('lookup_fn', __name__)
submit_ui = Blueprint('submit_fn', __name__)
healthcheck = Blueprint('healthcheck_fn', __name__)
submit_udf = Blueprint('submit_udf_fn', __name__)
lookup_udf = Blueprint('lookup_udf_fn', __name__)

response_map = {}
proxy_messages_in = []

@lookup_ui.route("/udf_proxy/lookup_ui", methods=["GET", "POST"])
def lookup_fn():
    '''
    Main handler for providing a web UI.
    '''
    if request.method == "POST":
        # getting input in HTML form
        input_text = request.form.get("input")
        # display input and output
        print("lookup input: ", input_text )
        resp = "not found"
        #print(response_map)
        if input_text in response_map.keys():
            resp = response_map[input_text]
        print("lookup resp: ", resp )
        return render_template("lookup_ui.html",
            uuid_input=input_text,
            response=resp)
    return render_template("lookup_ui.html")


@submit_ui.route("/udf_proxy/submit_ui", methods=["GET", "POST"])
def submit_fn():
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
            echo_reponse=submit(input_text, thread_text),
            thread_output=thread_text)
    return render_template("submit_ui.html")



def submit(input, thread_id):
    
    uu = str(uuid.uuid4())
    proxy_messages_in.append({"msg": input, "uuid": uu, "thread_id": thread_id})
    # temporary 
    response_map[uu] = input.upper()
    return uu


@healthcheck.get("/healthcheck")
def healthcheck_fn():
    return "I'm ready!"


@submit_udf.post("/udf_handler/submit_udf")
def submit_udf_fn():
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
    output_rows = [[row[0], submit(row[1],row[2])] for row in input_rows]
    logger.info(f'Produced {len(output_rows)} rows')

    response = make_response({"data": output_rows})
    response.headers['Content-type'] = 'application/json'
    logger.debug(f'Sending response: {response.json}')
    return response

def test_udf_strings():
    test_submit = """
    curl -X POST http://127.0.0.1:8080/udf_handler/submit_udf \
        -H "Content-Type: application/json" \
        -d '{"data": [[1, "hi there", "111"]]}'
    """
    test_response_udf = """
    curl -X POST http://127.0.0.1:8080/udf_handler/lookup_udf \
        -H "Content-Type: application/json" \
        -d '{"data": [[1, "94c9e3ab-e3f3-4dc9-8f63-9d60b625aa47"]]}'
    """


@lookup_udf.post("/udf_handler/lookup_udf")
def lookup_udf_fn():
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

    input_text = input_rows[0][1]
    print("lookup input: ", input_text )
    resp = "not found"
    if input_text in response_map.keys():
        resp =response_map[input_text]
    #print("lookup resp: ", resp )
    
    output_rows = [[row[0], resp] for row in input_rows]
    logger.info(f'Produced {len(output_rows)} rows')

    response = make_response({"data": output_rows})
    response.headers['Content-type'] = 'application/json'
    logger.debug(f'Sending response: {response.json}')
    return response

