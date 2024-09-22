
from flask import Flask
from flask import request
from flask import make_response
from flask import render_template
import logging
import os
import sys
from openai import OpenAI
import ngrok
import uuid
from collections import deque
import threading
import traceback
import pandas
import snowflake.connector

from llm_openai.openai_utils import get_openai_client
#from snowflake.snowpark import Session

def connection() -> snowflake.connector.SnowflakeConnection:
    if os.path.isfile("/snowflake/session/token"):
        creds = {
            'host': os.getenv('SNOWFLAKE_HOST'),
            'port': os.getenv('SNOWFLAKE_PORT'),
            'protocol': "https",
            'account': os.getenv('SNOWFLAKE_ACCOUNT'),
            'authenticator': "oauth",
            'token': open('/snowflake/session/token', 'r').read(),
            'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
            'database': os.getenv('SNOWFLAKE_DATABASE'),
            'schema': os.getenv('SNOWFLAKE_SCHEMA'),
            'client_session_keep_alive': True
        }
    else:
        creds = {
            'account': os.getenv('SNOWFLAKE_ACCOUNT'),
            'user': os.getenv('SNOWFLAKE_USER'),
            'password': os.getenv('SNOWFLAKE_PASSWORD'),
            'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
            'database': os.getenv('SNOWFLAKE_DATABASE'),
            'schema': os.getenv('SNOWFLAKE_SCHEMA'),
            'client_session_keep_alive': True
        }

    connection = snowflake.connector.connect(**creds)
    return connection

def test_snowflake_sql():
    sql = "show tables;"

    # Connector connection
    conn = connection()
    data = conn.cursor().execute("show tables;")
    
    # process results

    print("test query: ",data)
    res = ""
    for row in data:
        res = res + str(row) + "\n"
        print(row)

    # Snowpark Session
    #session = session()
    #data = session.sql(sql).to_pandas()
    # process results

# test database connection
#test_snowflake_sql()

SERVICE_HOST = os.getenv('SERVER_HOST', '0.0.0.0')
SERVICE_PORT = os.getenv('SERVER_PORT', 8080)
CHARACTER_NAME = os.getenv('CHARACTER_NAME', 'I')

n_key= os.getenv("NGROK_AUTHTOKEN")
print("Ngrok key: ",n_key)

listener = ngrok.connect(addr=f"localhost:{SERVICE_PORT}",
                         authtoken_from_env=True,)
                       #  proto="labeled",
                       #  labels="edge:edghts_2XrFea08KOrxjjMPI6w3GJ4OUlv")

client = get_openai_client()

message_map = {}

messages_in = deque()

response_map = {}

def get_logger(logger_name):
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(
        logging.Formatter(
            '%(name)s [%(asctime)s] [%(levelname)s] %(message)s'))
    logger.addHandler(handler)
    return logger

#conn = connection()

def handle_jobs():
  threading.Timer(1.0, handle_jobs).start()
  #print("responses: ", response_map)
  #print("Submit!")
  if len(messages_in) > 0:
        msg = messages_in.popleft()
        print("process: ",msg)

        messages = message_map.get(msg["thread_id"],[])
        messages.append({"role": "user", "content": msg["msg"]})

        if msg["msg"][0:4] == 'SQL ':
            query = msg["msg"][4:]

            try:
                data = conn.cursor().execute(query)
                # process results

                print("query: ",data)
                res = ""
                for row in data:
                    res = res + str(row) + "\n"
                    print("")
                    print(row)

            except Exception as e:
                res = traceback.format_exc()

            response_map[msg["uuid"]] = res

        elif msg["msg"][0:9] == 'DATABASES':
            query = "show databases"

            try:
                data = conn.cursor().execute(query)
                # process results

                print("query: ",data)
                res = []
                for row in data:
                    res.append(str(row[1]))
                print(res)

            except Exception as e:
                res = traceback.format_exc()

            response_map[msg["uuid"]] = res

        elif msg["msg"][0:11] == 'SCHEMAS IN ':
            db = msg["msg"][11:]

            try:
                query1 = "use database "+db
                data = conn.cursor().execute(query1)
                print("query: ",query1)

                query2 = "show schemas"
                data = conn.cursor().execute(query2)
                # process results

                print("query: ",query2)
                res = []
                for row in data:
                    res.append(str(row[1]))
                print(res)

            except Exception as e:
                res = traceback.format_exc()

        elif msg["msg"][0:10] == 'TABLES IN ':
            sch = msg["msg"][10:]

            try:
                query1 = "use schema "+sch
                data = conn.cursor().execute(query1)
                print("query: ",query1)

                query2 = "show tables"
                data = conn.cursor().execute(query2)

                print("query: ",query2)
                res = []
                for row in data:
                    res.append(str(row[1]))

                query3 = "show views"
                data = conn.cursor().execute(query3)
                # process results

                print("query: ",query3)
                for row in data:
                    res.append(str(row[1]))
                print(res)
            except Exception as e:
                res = traceback.format_exc()

        elif msg["msg"][0:10] == 'TABLE DDL ':
            TAB = msg["msg"][10:]

            try:
                query1 = "SELECT GET_DDL('TABLE','"+TAB+"')"
                data = conn.cursor().execute(query1)
                print("query: ",query1)

                res = []
                for row in data:
                    res.append(str(row[0]))

                print(res)
            except Exception as e:
                res = traceback.format_exc()

        elif msg["msg"][0:9] == 'VIEW DDL ':
            TAB = msg["msg"][9:]

            try:
                query1 = "SELECT GET_DDL('VIEW','"+TAB+"')"
                data = conn.cursor().execute(query1)
                print("query: ",query1)

                res = []
                for row in data:
                    res.append(str(row[0]))

                print(res)


            except Exception as e:
                res = traceback.format_exc()

            response_map[msg["uuid"]] = res

        elif msg["msg"][0:12] == 'SAMPLE DATA ':
            TAB = msg["msg"][12:]

            try:
                query1 = "SELECT * FROM "+TAB+" LIMIT 10"
                df = conn.cursor().execute(query1).fetch_pandas_all()
                print("query: ",query1)

                res = df.to_csv()

                print(res)
                

            except Exception as e:
                res = traceback.format_exc()

            response_map[msg["uuid"]] = res




        else:
            response = client.chat.completions.create(
                    model='gpt-4o',
                    messages=[
                        {"role": m["role"], "content": m["content"]}
                        for m in messages
                    ],
                    stream=False,
                )
            messages.append({"role": "assistant", "content": response.choices[0].message.content})
            response_map[msg["uuid"]] = response.choices[0].message.content
            message_map[msg["thread_id"]]=messages
            print(response)

        return


handle_jobs()

logger = get_logger('echo-service')

app = Flask(__name__)

@app.get("/healthcheck")
def readiness_probe():
    return "I'm ready!"


@app.post("/echo")
def echo():
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


@app.post("/lookup")
def lookup():
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


@app.route("/lookup_ui", methods=["GET", "POST"])
def lookup_ui():
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


@app.route("/ui", methods=["GET", "POST"])
def ui():
    '''
    Main handler for providing a web UI.
    '''
    if request.method == "POST":
        # getting input in HTML form
        input_text = request.form.get("input")
        thread_text = request.form.get("thread_text")
        # display input and output
        return render_template("basic_ui.html",
            echo_input=input_text,
            thread_id=thread_text,
            echo_reponse=submit(input_text, thread_text),
            thread_output=thread_text)
    return render_template("basic_ui.html")


def submit(input, thread_id):
    
    uu = str(uuid.uuid4())
    messages_in.append({"msg": input, "uuid": uu, "thread_id": thread_id})
    return uu

#test_snowflake_sql()


if __name__ == '__main__':
    app.run(host=SERVICE_HOST, port=SERVICE_PORT)
