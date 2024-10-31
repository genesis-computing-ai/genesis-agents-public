#from connectors.snowflake_connector.snowflake_connector import SnowflakeConnector
# from snowflake.connector import SnowflakeConnection
# import os
import time
# from cryptography.hazmat.primitives import serialization
import subprocess

# import uuid
# Build a SnowflakeConnection from env variables



# # Load the private key from environment variable
# private_key = os.getenv('SNOWFLAKE_PRIVATE_KEY')

# if private_key:
#     # If your key is encrypted with a passphrase
#     passphrase = os.getenv('PRIVATE_KEY_PASSPHRASE')

#     # Convert the key to bytes
#     private_key_bytes = private_key.encode('utf-8')

#     # Load the private key using cryptography
#     p_key = serialization.load_pem_private_key(
#         private_key_bytes,
#         password=passphrase.encode('utf-8') if passphrase else None,
#     )

#     # Snowflake connection using private key authentication
#     conn = SnowflakeConnection(
#         user=os.getenv('SNOWFLAKE_USER_OVERRIDE'),
#         account=os.getenv('SNOWFLAKE_ACCOUNT_OVERRIDE'),
#         warehouse=os.getenv('SNOWFLAKE_WAREHOUSE_OVERRIDE'),
#         database=os.getenv('SNOWFLAKE_DATABASE_OVERRIDE'),
#         role=os.getenv("SNOWFLAKE_ROLE_OVERRIDE"),
#         private_key=p_key
#     )
# else:
#     conn = SnowflakeConnection(
#     account=os.getenv("SNOWFLAKE_ACCOUNT_OVERRIDE"),
#     user=os.getenv("SNOWFLAKE_USER_OVERRIDE"),
#     password=os.getenv("SNOWFLAKE_PASSWORD_OVERRIDE"),
#     database=os.getenv("SNOWFLAKE_DATABASE_OVERRIDE"),
#     warehouse=os.getenv("SNOWFLAKE_WAREHOUSE_OVERRIDE"),
#     role=os.getenv("SNOWFLAKE_ROLE_OVERRIDE")
#     )


sql_query = """select genesis_bots.app1.submit_udf('tell me more about your capabilities', '', '{"bot_id": "Janice"}')"""

command = [
    'snow',
    '--config-file', '/home/runner/.snowcli/config.toml',
    'sql',
    '-c', 'GENESIS-DEV-CONSUMER-2',
    '-q', sql_query
]


# Execute the command
result = subprocess.run(command, capture_output=True, text=True)

# Check if the command was successful
if result.returncode == 0:
    print("Command executed successfully.")
    print("Output:")
    print(result.stdout)
else:
    print("Command failed with return code:", result.returncode)
    print("Error:")
    print(result.stderr)


# Execute the SQL code
#grant_usage_1 = conn.cursor().execute("call genesis_bots.core.run_arbitrary('grant usage on function genesis_bots.app1.submit_udf(varchar, varchar, varchar) to application role app_public')")
#grant_usage_2 = conn.cursor().execute("call genesis_bots.core.run_arbitrary('grant usage on function genesis_bots.app1.lookup_udf(varchar, varchar) to application role app_public')")
# cursor = conn.cursor()
# cursor.execute("""
# select genesis_bots.app1.submit_udf('tell me more about your capabilities', '', '{"bot_id": "Janice"}')
# """)
thread_id = result.stdout   
# thread_id = thread_id_result[0] if thread_id_result else None

time.sleep(10)


sql_query = f"""select genesis_bots.app1.lookup_udf ('{thread_id}', 'Janice')"""

command = [
    'snow',
    '--config-file', '/home/runner/.snowcli/config.toml',
    'sql',
    '-c', 'GENESIS-DEV-CONSUMER-2',
    '-q', sql_query
]


# Execute the command
result = subprocess.run(command, capture_output=True, text=True)

# Check if the command was successful
if result.returncode == 0:
    print("Command executed successfully.")
    print("Output:")
    print(result.stdout)
else:
    print("Command failed with return code:", result.returncode)
    print("Error:")
    print(result.stderr)

# cursor.execute(f"""
# select genesis_bots.app1.lookup_udf ('{thread_id}', 'Janice')
# """)
response = result.stdout
# response = response_result[0] if response_result else None

print(thread_id)
print(response)

# Check if the responses are valid
if thread_id is not None and response is not None:
    print("Responses are valid")
    exit(0)
else:
    print("Responses are not valid")
    exit(1)

