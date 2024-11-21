#from connectors.snowflake_connector.snowflake_connector import SnowflakeConnector
from snowflake.connector import SnowflakeConnection
import os
import time
import uuid
from cryptography.hazmat.primitives import serialization
import requests, json, sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'core')))
from logging_config import logger
# from streamlit_gui.utils import submit_to_udf_proxy, get_response_from_udf_proxy

# import uuid
# Build a SnowflakeConnection from env variables

# Is Service Up?
# SELECT SYSTEM$GET_SERVICE_STATUS('GENESISAPP_SERVICE_SERVICE'); -- Should return 'RUNNING'

def wait_for_return(thread_id, start_time):
    cursor.execute(f"""
    select genesis_bots.app1.lookup_udf ('{thread_id}', 'Janice')
    """)

    max_attempts = 1000
    attempts = 0
    response_result = None

    i = 0
    while attempts < max_attempts and (response_result is None or response_result[0] == "not found" or response_result[0][-1] == '💬'):
        cursor.execute(f"""
        select genesis_bots.app1.lookup_udf ('{thread_id}', 'Janice')
        """)

        response_result = cursor.fetchone()
        response = response_result[0] if response_result else None
        elapsed_time = time.time() - start_time
        elapsed_time_str = time.strftime("%H:%M:%S", time.gmtime(elapsed_time))

        if response == "not found":
            print(f"{i} {elapsed_time_str} {response}", end='\r')
            i += 1
        else:
            print(f"Elapsed Time:  {elapsed_time_str} {response}")

        # Check if the response is valid
        if response_result is not None and response_result[0] != "not found" and response_result[0][-1] != '💬':
            print(f"{thread_id} - Received valid response: {response}")
            return response

        attempts += 1
        time.sleep(1)

    # Check if the responses are valid
    if thread_id is not None and response is not None:
        return response
    else:
        return {"error": "No response received after 1000 attempts"}

def wait_for_return_udf(thread_id, bot_id):
    not_found = True
    while not_found:
        try:
            url = f"http://127.0.0.1:8080/udf_proxy/lookup_udf"
            headers = {"Content-Type": "application/json"}
            data = json.dumps({"data": [[1, thread_id, bot_id]]})
            response = requests.post(url, headers=headers, data=data, timeout=5)
            if response.status_code == 200 and response.json()["data"][0][1] != "not found":
                return response.json()["data"][0][1]
            print(f"Response status code: {response.status_code} | Message: {response.json()["data"][0][1]}")
            continue
        except requests.exceptions.RequestException as e:
            logger.info(f"Error connecting to local server: {e}")
            print("Error")
            return "Error: Unable to connect to the bot server. Please try again later."

    return

def call_submit_udf(input_text, thread_id, primary_user = {
        "user_id": "Unknown User ID",
        "user_name": "Unknown User",
        "user_email": "test@example.com",
        "bot_id": 'Eve-37zAQo'
    }):
    LOCAL_SERVER_URL = "http://127.0.0.1:8080/"
    url = LOCAL_SERVER_URL + "udf_proxy/submit_udf"

    headers = {"Content-Type": "application/json"}
    data = json.dumps({"data": [[1, input_text, thread_id, primary_user,{}]]})
    response = requests.post(url, headers=headers, data=data)
    if response.status_code == 200:
        return response.json()["data"][0][1]
    else:
        raise Exception(f"Failed to submit to UDF proxy: {response.text}")

# Load the private key from environment variable
private_key = os.getenv('SNOWFLAKE_PRIVATE_KEY')

if private_key:
    # If your key is encrypted with a passphrase
    passphrase = os.getenv('PRIVATE_KEY_PASSPHRASE')

    # Convert the key to bytes
    private_key_bytes = private_key.encode('utf-8')

    # Load the private key using cryptography
    p_key = serialization.load_pem_private_key(
        private_key_bytes,
        password=passphrase.encode('utf-8') if passphrase else None,
    )

    # Snowflake connection using private key authentication
    conn = SnowflakeConnection(
        user=os.getenv('SNOWFLAKE_USER_OVERRIDE'),
        account=os.getenv('SNOWFLAKE_ACCOUNT_OVERRIDE'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE_OVERRIDE'),
        database=os.getenv('SNOWFLAKE_DATABASE_OVERRIDE'),
        role=os.getenv("SNOWFLAKE_ROLE_OVERRIDE"),
        private_key=p_key
    )
else:
    conn = SnowflakeConnection(
    account=os.getenv("SNOWFLAKE_ACCOUNT_OVERRIDE"),
    user=os.getenv("SNOWFLAKE_USER_OVERRIDE"),
    password=os.getenv("SNOWFLAKE_PASSWORD_OVERRIDE"),
    database=os.getenv("SNOWFLAKE_DATABASE_OVERRIDE"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE_OVERRIDE"),
    role=os.getenv("SNOWFLAKE_ROLE_OVERRIDE")
    )

# Execute the SQL code
#grant_usage_1 = conn.cursor().execute("call genesis_bots.core.run_arbitrary('grant usage on function genesis_bots.app1.submit_udf(varchar, varchar, varchar) to application role app_public')")
#grant_usage_2 = conn.cursor().execute("call genesis_bots.core.run_arbitrary('grant usage on function genesis_bots.app1.lookup_udf(varchar, varchar) to application role app_public')")

# cursor = conn.cursor()

start_time = time.time()
start_time_str = time.strftime("%A, %B %d, %Y %H:%M:%S", time.localtime(start_time))
print(f"Start time: {start_time_str}")

thread_id = str(uuid.uuid4())
print(f"Initializing thread_id: {thread_id} - Ask Janice to show her list of tools")

input_text = 'output the string "HELLO WORLD" to the #jeff-local-runner channel, then run process pi_100'
result = call_submit_udf(input_text, thread_id)
print(f"Response from submit_udf: {result}")

response = wait_for_return_udf(thread_id, start_time)
print(f"Janice's tool list: {response}")

# 'File Upload Test',"NY MLB Stadiums", 'Pascal Triangle Test',
# , 'second_test', 'third_test', 'fourth_test'

for test_name in ['first_test']:
    print(f'Sending request to Janice - thread id: {thread_id}')
    input_text = f"run process {test_name}"
    result = call_submit_udf(input_text, thread_id)

    time.sleep(10)

    response = wait_for_return_udf(thread_id, start_time)
    print(f"Response: {response}")

    elapsed_time = time.time() - start_time
    elapsed_time_str = time.strftime("%H:%M:%S", time.gmtime(elapsed_time))
    print(f"Total elapsed time: {elapsed_time_str}")

exit(0)

