#from connectors.snowflake_connector.snowflake_connector import SnowflakeConnector
from snowflake.connector import SnowflakeConnection
import os
import time
from cryptography.hazmat.primitives import serialization
# import uuid
# Build a SnowflakeConnection from env variables



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
cursor.execute("""
select genesis_bots.app1.submit_udf('show me all of your processes', '', '{"bot_id": "Janice"}')
""")
thread_id_result = cursor.fetchone()
thread_id = thread_id_result[0] if thread_id_result else None
print(f'Sending request to Janice - thread id: {thread_id}')

time.sleep(10)

cursor.execute(f"""
select genesis_bots.app1.lookup_udf ('{thread_id}', 'Janice')
""")

max_attempts = 1000
attempts = 0
response_result = None

while attempts < max_attempts and (response_result is None or response_result[0][-1] == '💬'):
    cursor.execute(f"""
    select genesis_bots.app1.lookup_udf ('{thread_id}', 'Janice')
    """)

    response_result = cursor.fetchone()
    response = response_result[0] if response_result else None

    # Check if the response is valid
    if response_result is not None and response_result[0][-1] != '💬':
        print("Response is valid")
        print(thread_id)
        print(response)
        break

    attempts += 1
    time.sleep(1)

# Check if the responses are valid
if thread_id is not None and response is not None:
    print("Responses are valid")
    exit(0)
else:
    print("Responses are not valid")
    exit(1)

