#from connectors.snowflake_connector.snowflake_connector import SnowflakeConnector
from snowflake.connector import SnowflakeConnection
import os
import time
import uuid
import json
from cryptography.hazmat.primitives import serialization
# import uuid
# Build a SnowflakeConnection from env variables

def wait_for_return_direct(thread_id, cursor):
    flag = True
    while flag:
        query = f"select message from genesis_bots.app1.llm_results where UU = '{thread_id}'"
        cursor.execute(query)
        response_result = cursor.fetchone()
        response = response_result[0] if response_result else None
        if response and response != "not found" and response[-1] != '💬':
            return response
        print(response, end = '\r')
    return

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

        if response == "not found":
            print(f"{i} {response}", end='\r')
            i += 1
        else:
            print(f"{response}")

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
cursor = conn.cursor()

result =None
while result != 'READY':
    cursor.execute("USE DATABASE GENESIS_BOTS;")
    cursor.execute("USE SCHEMA APP1;")
    cursor.execute("SELECT SYSTEM$GET_SERVICE_STATUS('GENESISAPP_SERVICE_SERVICE');")
    result = cursor.fetchone()
    result = json.loads(result[0].replace("[" , "").replace("]", ""))['status']

# Grant access to LLM_RESULTS table
sql_command = """
CALL genesis_bots.core.run_arbitrary($$grant select on table app1.llm_results to application role app_public $$);
"""
cursor.execute(sql_command)
result = cursor.fetchone()
print(f"Result: {result}")

bot_id = 'Janice-dev'
start_time = time.strftime("%A, %B %d, %Y %H:%M:%S", time.localtime())
print(f"Start time: {start_time}")

thread_id = str(uuid.uuid4())
print(f"Initializing thread_id: {thread_id} - Ask {bot_id} to show her list of processes")
query = """
select genesis_bots.app1.submit_udf('say "Hi there, just testing tests" in channel #dev-genbots', '""" + thread_id + """', '{"bot_id": "Janice-dev"}')
"""
print(query)
cursor.execute(query)
thread_id = cursor.fetchone()
thread_id = thread_id[0]
print(f"Thread returned: {thread_id}")

response = wait_for_return_direct(thread_id, cursor)
print(f"Janice's tool list: {response}")

# Check for the existence of the table 'test_manager'
cursor.execute("SHOW TABLES LIKE 'test_manager'")
table_exists = cursor.fetchone()

if not table_exists:
    print("Table 'test_manager' does not exist. Exiting program.")
    exit(0)

# Read all rows where 'active' is true, ordered by 'priority'
cursor.execute(f"SELECT process_name FROM test_manager WHERE bot_id = {bot_id} ORDER BY order")
active_processes = cursor.fetchall()

# Store the process names in an array
process_names = [row[0] for row in active_processes]
print(f"Active process names: {process_names}")

for test in process_names:
    query = """
select genesis_bots.app1.submit_udf('use your tool SendSlackChannelMessage to send this string to channel #dev-genbots ''Thread: {}''; run process {}', '', '{{"bot_id": "Janice-dev"}}')
""".format(thread_id, test)
    print(query)
    cursor.execute(query)
    thread_id_result = cursor.fetchone()
    thread_id = thread_id_result[0] if thread_id_result else None

    print(f'Waiting for return from thread id: {thread_id}')
    response = wait_for_return_direct(thread_id, cursor)

exit(0)