from .generate_JWT import JWTGenerator
from datetime import timedelta
import argparse
import logging
import sys
import requests
import json
logger = logging.getLogger(__name__)

def main():
  args = _parse_args()
  token = _get_token(args)
  snowflake_jwt = token_exchange(token,endpoint=args.endpoint, role=args.role,
                  snowflake_account_url=args.snowflake_account_url,
                  snowflake_account=args.account)
  spcs_url=f'https://{args.endpoint}{args.endpoint_path}'
  connect_to_spcs(snowflake_jwt, spcs_url)
  test_chat(snowflake_jwt, spcs_url)

def _get_token(args):
  token = JWTGenerator(args.account, args.user, args.private_key_file_path, timedelta(minutes=args.lifetime),
            timedelta(minutes=args.renewal_delay)).get_token()
  logger.info("Key Pair JWT: %s" % token)
  return token

def token_exchange(token, role, endpoint, snowflake_account_url, snowflake_account):
    scope_role = f'session:role:{role}' if role is not None else None
    scope = f'{scope_role} {endpoint}' if scope_role is not None else endpoint
    data = {
        'grant_type': 'urn:ietf:params:oauth:grant-type:jwt-bearer',
        'scope': scope,
        'assertion': token,
    }
    logger.info(f"Request data: {data}")
    url = f'https://{snowflake_account}.snowflakecomputing.com/oauth/token'
    if snowflake_account_url:
        url = f'{snowflake_account_url}/oauth/token'
    logger.info(f"OAuth URL: {url}")
    
    response = requests.post(url, data=data)
    logger.info(f"Response status code: {response.status_code}")
    logger.info(f"Response headers: {response.headers}")
    logger.info(f"Response body: {response.text}")
    
    if response.status_code != 200:
        error_msg = f"Failed to get Snowflake token. Status: {response.status_code}, Response: {response.text}"
        logger.error(error_msg)
        raise Exception(error_msg)
        
    return response.text

def connect_to_spcs(token, url):
  # Create a request to the ingress endpoint with authz.
  headers = {'Authorization': f'Snowflake Token="{token}"'}
  data = {
    "data": [
      [0, "test_value"]  # Row index 0 with a test value
    ]
  }
  response = requests.post(f'{url}/echo', headers=headers, json=data)
  logger.info("return code %s" % response.status_code)
  logger.info(response.text)


def call_submit_udf(token, url, bot_id, row_data, thread_id=None, file=None):
    """
    Call the submit_udf endpoint with proper authentication
    
    Args:
        token: Snowflake JWT token
        url: Base SPCS URL
        bot_id: Bot ID to include in request
        row_data: Data for the row (input message)
        thread_id: Optional thread ID to associate with request
        file: Optional file data to include
    """
    headers = {'Authorization': f'Snowflake Token="{token}"'}
    
    # Format bot_id as JSON object
    bot_id_json = json.dumps({"bot_id": bot_id})
    

    data = {
        "data": [
            [0, row_data, thread_id, bot_id_json, file]  # Match input_rows structure
        ]
    }


    submit_url = f'{url}/udf_proxy/submit_udf'
    response = requests.post(submit_url, headers=headers, json=data)

    #logger.info(f"Submit UDF status code: {response.status_code}")
    #logger.info(f"Submit UDF response: {response.text}")
    return response

def call_lookup_udf(token, url, bot_id, uuid):
    """
    Call the lookup_udf endpoint with proper authentication
    
    Args:
        token: Snowflake JWT token
        url: Base SPCS URL
        bot_id: Bot ID to include in request
        uuid: UUID of the request to look up
    """
    headers = {
        'Authorization': f'Snowflake Token="{token}"',
        'Content-Type': 'application/json'
    }
    
    data = {
        "data": [[1, uuid, bot_id]]
    }
    
    lookup_url = f'{url}/udf_proxy/lookup_udf'
    response = requests.post(lookup_url, headers=headers, json=data)  # Use json parameter instead of data
    

    return response


def test_chat(token, url):
    """
    Interactive chat test function that sends messages to a bot and polls for responses
    
    Args:
        token: Snowflake JWT token
        url: Base SPCS URL
    """
    import uuid
    import time
    # Get bot ID from user
    bot_id = input("Enter bot ID (default: Eve): ") or "Eve"
    thread_id = str(uuid.uuid4())  # Generate thread ID for conversation

    while True:
        # Get message from user
        message = input("\nEnter message (or 'quit' to exit): ")
        if message.lower() == 'quit':
            break

        # Submit message
        submit_response = call_submit_udf(
            token=token,
            url=url,
            bot_id=bot_id,
            row_data=message,
            thread_id=thread_id
        )
        
        if submit_response.status_code != 200:
            logger.error("Failed to submit message")
            continue
            

        # Get UUID from response
        try:
            uuid = submit_response.json()['data'][0][1]
        except (KeyError, IndexError, json.JSONDecodeError) as e:
            logger.error(f"Failed to parse UUID from response: {e}")
            continue

        # Poll for response
        while True:
            lookup_response = call_lookup_udf(
                token=token,
                url=url,
                bot_id=bot_id,
                uuid=uuid
            )
            
            if lookup_response.status_code != 200:
                logger.error("Failed to lookup response")
                break
                

            try:
                response_data = lookup_response.json()['data'][0][1]
                if response_data != "not found":
                    print(f"\nBot: {response_data}")
                    break
            except (KeyError, IndexError, json.JSONDecodeError) as e:
                logger.error(f"Failed to parse response: {e}")
                break

            time.sleep(1)  # Wait before polling again


def _parse_args():
  logging.basicConfig(stream=sys.stdout, level=logging.INFO)
  cli_parser = argparse.ArgumentParser()
  cli_parser.add_argument('--account', required=True,
              help='The account identifier (for example, "myorganization-myaccount" for '
                '"myorganization-myaccount.snowflakecomputing.com").')
  cli_parser.add_argument('--user', required=True, help='The user name.')
  cli_parser.add_argument('--private_key_file_path', required=True,
              help='Path to the private key file used for signing the JWT.')
  cli_parser.add_argument('--lifetime', type=int, default=59,
              help='The number of minutes that the JWT should be valid for.')
  cli_parser.add_argument('--renewal_delay', type=int, default=54,
              help='The number of minutes before the JWT generator should produce a new JWT.')
  cli_parser.add_argument('--role',
              help='The role we want to use to create and maintain a session for. If a role is not provided, '
                'use the default role.')
  cli_parser.add_argument('--endpoint', required=True,
              help='The ingress endpoint of the service')
  cli_parser.add_argument('--endpoint-path', default='/',
              help='The url path for the ingress endpoint of the service')
  cli_parser.add_argument('--snowflake_account_url', default=None,
              help='The account url of the account for which we want to log in. Type of '
                'https://myorganization-myaccount.snowflakecomputing.com')
  args = cli_parser.parse_args()
  return args

if __name__ == "__main__":
  main()