# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.

from botbuilder.core import ActivityHandler, TurnContext
from botbuilder.schema import ChannelAccount


from datetime import timedelta
import logging
import requests
import json
from cryptography.hazmat.primitives.serialization import load_pem_private_key
from cryptography.hazmat.primitives.serialization import Encoding
from cryptography.hazmat.primitives.serialization import PublicFormat
from cryptography.hazmat.backends import default_backend
from datetime import timedelta, timezone, datetime
import base64
from getpass import getpass
import hashlib
import logging
import sys
from dotenv import load_dotenv
import os

logger = logging.getLogger(__name__)
script_dir = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(script_dir, '.env'))

#cryptography>=3.0
#PyJWT>=2.0.0
#requests>=2.0.0
#typing-extensions ; python_version < "3.5.2"

# This class relies on the PyJWT module (https://pypi.org/project/PyJWT/).
import jwt

try:
    from typing import Text
except ImportError:
    logger.debug('# Python 3.5.0 and 3.5.1 have incompatible typing modules.', exc_info=True)
    from typing_extensions import Text

ISSUER = "iss"
EXPIRE_TIME = "exp"
ISSUE_TIME = "iat"
SUBJECT = "sub"

# If you generated an encrypted private key, implement this method to return
# the passphrase for decrypting your private key. As an example, this function
# prompts the user for the passphrase.
def get_private_key_passphrase():
    return getpass('Passphrase for private key: ')

class JWTGenerator(object):
    """
    Creates and signs a JWT with the specified private key file, username, and account identifier. The JWTGenerator keeps the
    generated token and only regenerates the token if a specified period of time has passed.
    """
    LIFETIME = timedelta(minutes=59)  # The tokens will have a 59-minute lifetime
    RENEWAL_DELTA = timedelta(minutes=54)  # Tokens will be renewed after 54 minutes
    ALGORITHM = "RS256"  # Tokens will be generated using RSA with SHA256

    def __init__(self, account: Text, user: Text, private_key_path: Text,
                lifetime: timedelta = LIFETIME, renewal_delay: timedelta = RENEWAL_DELTA):
        """
        __init__ creates an object that generates JWTs for the specified user, account identifier, and private key.
        :param account: Your Snowflake account identifier.
        :param user: The Snowflake username.
        :param private_key_path: Path to private key string in PEM format
        :param lifetime: The number of minutes (as a timedelta) during which the key will be valid.
        :param renewal_delay: The number of minutes (as a timedelta) from now after which the JWT generator should renew the JWT.
        """

        logger.info(
            """Creating JWTGenerator with arguments
            account : %s, user : %s, lifetime : %s, renewal_delay : %s""",
            account, user, lifetime, renewal_delay)

        self.account = self.prepare_account_name_for_jwt(account)
        self.user = user.upper()
        self.qualified_username = self.account + "." + self.user
        self.private_key_path = private_key_path

        self.lifetime = lifetime
        self.renewal_delay = renewal_delay
        self.renew_time = datetime.now(timezone.utc)
        self.token = None

        # Load the private key from the specified file.
        with open(self.private_key_path, 'rb') as pem_in:
            pemlines = pem_in.read()
            try:
                # Try to access the private key without a passphrase.
                self.private_key = load_pem_private_key(pemlines, None, default_backend())
            except TypeError:
                # If that fails, provide the passphrase returned from get_private_key_passphrase().
                self.private_key = load_pem_private_key(pemlines, get_private_key_passphrase().encode(), default_backend())

    def prepare_account_name_for_jwt(self, raw_account: Text) -> Text:
        """
        Prepare the account identifier for use in the JWT.
        For the JWT, the account identifier must not include the subdomain or any region or cloud provider information.
        :param raw_account: The specified account identifier.
        :return: The account identifier in a form that can be used to generate the JWT.
        """
        account = raw_account
        if not '.global' in account:
            # Handle the general case.
            idx = account.find('.')
            if idx > 0:
                account = account[0:idx]
        else:
            # Handle the replication case.
            idx = account.find('-')
            if idx > 0:
                account = account[0:idx]
        # Use uppercase for the account identifier.
        return account.upper()

    def get_token(self) -> Text:
        """
        Generates a new JWT. If a JWT has already been generated earlier, return the previously generated token unless the
        specified renewal time has passed.
        :return: the new token
        """
        now = datetime.now(timezone.utc)  # Fetch the current time

        # If the token has expired or doesn't exist, regenerate the token.
        if self.token is None or self.renew_time <= now:
            logger.info("Generating a new token because the present time (%s) is later than the renewal time (%s)",
                        now, self.renew_time)
            # Calculate the next time we need to renew the token.
            self.renew_time = now + self.renewal_delay

            # Prepare the fields for the payload.
            # Generate the public key fingerprint for the issuer in the payload.
            public_key_fp = self.calculate_public_key_fingerprint(self.private_key)

            # Create our payload
            payload = {
                # Set the issuer to the fully qualified username concatenated with the public key fingerprint.
                ISSUER: self.qualified_username + '.' + public_key_fp,

                # Set the subject to the fully qualified username.
                SUBJECT: self.qualified_username,

                # Set the issue time to now.
                ISSUE_TIME: now,

                # Set the expiration time, based on the lifetime specified for this object.
                EXPIRE_TIME: now + self.lifetime
            }

            # Regenerate the actual token
            token = jwt.encode(payload, key=self.private_key, algorithm=JWTGenerator.ALGORITHM)
            # If you are using a version of PyJWT prior to 2.0, jwt.encode returns a byte string instead of a string.
            # If the token is a byte string, convert it to a string.
            if isinstance(token, bytes):
              token = token.decode('utf-8')
            self.token = token
            logger.info("Generated a JWT with the following payload: %s", jwt.decode(self.token, key=self.private_key.public_key(), algorithms=[JWTGenerator.ALGORITHM]))

        return self.token

    def calculate_public_key_fingerprint(self, private_key: Text) -> Text:
        """
        Given a private key in PEM format, return the public key fingerprint.
        :param private_key: private key string
        :return: public key fingerprint
        """
        # Get the raw bytes of public key.
        public_key_raw = private_key.public_key().public_bytes(Encoding.DER, PublicFormat.SubjectPublicKeyInfo)

        # Get the sha256 hash of the raw bytes.
        sha256hash = hashlib.sha256()
        sha256hash.update(public_key_raw)

        # Base64-encode the value and prepend the prefix 'SHA256:'.
        public_key_fp = 'SHA256:' + base64.b64encode(sha256hash.digest()).decode('utf-8')
        logger.info("Public key fingerprint is %s", public_key_fp)

        return public_key_fp

global_token = None
global_url = None

conversation_history = {}
thread_id = None

class Args:
    def __init__(self):
      self.account = os.getenv('SNOWFLAKE_ACCOUNT', 'eqb52188')
      self.user = os.getenv('SNOWFLAKE_USER', 'JUSTIN.LANGSETH@GENESISCOMPUTING.AI')
      self.role = os.getenv('SNOWFLAKE_ROLE', 'ACCOUNTADMIN')
      self.private_key_path = os.getenv('PRIVATE_KEY_PATH', 'rsa_key.p8')
      self.platform = os.getenv('PLATFORM', 'ALPHA')
      if self.platform == 'DEV':
        self.endpoint = os.getenv('SNOWFLAKE_ENDPOINT_DEV', 'blf4aam4-dshrnxx-genesis-dev-consumer.snowflakecomputing.app')
      elif self.platform == 'ALPHA':
        self.endpoint = os.getenv('SNOWFLAKE_ENDPOINT_ALPHA', 'fsc4ar3w-dshrnxx-cvb46967.snowflakecomputing.app')
      elif self.platform == 'NGROK':
        self.endpoint = os.getenv('NGROK_ENDPOINT', '')
      self.endpoint_path = os.getenv('ENDPOINT_PATH', '')
      self.lifetime = int(os.getenv('TOKEN_LIFETIME', '59'))
      self.renewal_delay = int(os.getenv('TOKEN_RENEWAL_DELAY', '54'))
      self.snowflake_account_url = os.getenv('SNOWFLAKE_ACCOUNT_URL', None)

def login():
    global global_token
    global global_url

    args = Args()
    print(f"LOGGING IN - Platform: {args.platform} | Endpoint: {args.endpoint}")

    global_url=f'https://{args.endpoint}{args.endpoint_path}'
    if args.platform != 'NGROK':
        token = _get_token(args)
        snowflake_jwt = token_exchange(token,endpoint=args.endpoint, role=args.role,
            snowflake_account_url=args.snowflake_account_url,
            snowflake_account=args.account)
        global_token = snowflake_jwt
        connect_to_spcs(global_token, global_url)
    else:
        print("Using NGROK platform, skipping login")

def _get_token(args):
  token = JWTGenerator(args.account, args.user, args.private_key_path, timedelta(minutes=args.lifetime),
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

def main():
  login()
#   resp = send_message('Hi')
#   print(f"Response from login 'hi': {resp}")

def call_submit_udf(token, url, bot_id, row_data, conversation_id, thread_id=None, file=None):
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

    print(f'Enter call submit udf - url: {url} bot_id: {bot_id} row_data: {row_data} thread_id: {thread_id} file: {file}', flush=True)

    headers = {'Authorization': f'Snowflake Token="{token}"'}

    # Format bot_id as JSON object
    bot_id_json = json.dumps({"bot_id": bot_id})

    data = {
        "data": [
            [0, row_data, thread_id, bot_id_json, file]  # Match input_rows structure
        ]
    }

    submit_url = f'{url}/udf_proxy/submit_udf'
    logger.info(f"Call Submit udf - data: {data} Url: {submit_url}")
    response = requests.post(submit_url, headers=headers, json=data)

    logger.info(f"Submit UDF status code: {response.status_code}")
    logger.info(f"Submit UDF response: {response.text}")
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


def send_message(message, conversation_id = None):
    """
    Interactive chat test function that sends messages to a bot and polls for responses
    
    Args:
        token: Snowflake JWT token
        url: Base SPCS URL
    """
    import uuid
    import time

    global thread_id

    print(f"send_messages conversation_id: {conversation_id}", flush=True)
    bot_id = os.getenv('BOT_ID', 'Eve')
    if thread_id is None:
        thread_id = str(uuid.uuid4())  # Generate thread ID for conversation
        print(f"Created new thread_id: {thread_id}", flush=True)

    print(f"Submitting uuid: {thread_id}", flush=True)

    # THIS STORES CONVERSATION HISTORY IN PROXY
    # conversation_context = ''
    # if conversation_id and conversation_id in conversation_history:
    #     conversation_context = "Here is the history of our conversation:\n"

    #     print(f"Conversation context 1: {conversation_context}", flush=True)

    #     for msg in conversation_history[conversation_id]:
    #         conversation_context += msg[0] + ": " + msg[2] + ", "

    #     conversation_context += f"And the latest message from the user is: {message}"

    #     print(f"Conversation context 2: {conversation_context}", flush=True)

    #     conversation_history[conversation_id].append(['user', thread_id, message])
    # elif conversation_id:
    #     conversation_history[conversation_id] = [['user', thread_id, message]]
    #     conversation_context = message
    # else:
    #     conversation_context = message

    conversation_context = message

    print(f"Submitting message: {conversation_context}", flush=True)

    # Submit message
    submit_response = call_submit_udf(
        token=global_token,
        url=global_url,
        bot_id=bot_id,
        conversation_id=conversation_id,
        row_data=conversation_context,
        thread_id=thread_id,
    )

    if submit_response.status_code != 200:
        logger.error("Failed to submit message")
        return

    # Get UUID from response
    try:
        print(f"Return from submit_response: {submit_response.json()}", flush=True)
        uuid = submit_response.json()['data'][0][1]
        print(f"UUID from response: {uuid}", flush=True)
    except (KeyError, IndexError, json.JSONDecodeError) as e:
        logger.error(f"Failed to parse UUID from response: {e}", flush=True)
        return

        # Poll for response
    while True:
        lookup_response = call_lookup_udf(
            token=global_token,
            url=global_url,
            bot_id=bot_id,
            uuid=uuid
        )

        if lookup_response.status_code != 200:
            logger.error("Failed to lookup response")
            break

        try:
            response_data = lookup_response.json()['data'][0][1]
            if response_data != "not found" and not response_data.endswith('💬'):
                print(f"\nBOT RESPONSE DATA: {response_data}\n")
                # THIS ADDS BOT RESPONSE TO CONVERSATION HISTORY
                # if conversation_id:
                #     if conversation_id in conversation_history:
                #         conversation_history[conversation_id].append(['bot', thread_id, response_data])
                #     else:
                #         conversation_history[conversation_id] = [['bot', thread_id, response_data]]

                # print(f"CONVERSATION CONTEXT: {conversation_history}")

                return response_data
        except (KeyError, IndexError, json.JSONDecodeError) as e:
            logger.error(f"Failed to parse response: {e}")
            break

        time.sleep(1)  # Wait before polling again


if __name__ == "__main__":
  main()

class EchoBot(ActivityHandler):
    # See https://aka.ms/about-bot-activity-message to learn more about the message and other activity types.

    def __init__(self):
        super().__init__()
        main()


    async def on_message_activity(self, turn_context: TurnContext):
        print(f'Turn Context Received: {turn_context.activity}\n')
        print(f"Conversation ID: {turn_context.activity.conversation.id}\n", flush=True)

        resp = send_message(turn_context.activity.text, turn_context.activity.conversation.id)
        await turn_context.send_activity(resp)

    async def on_members_added_activity(
        self,
        members_added: ChannelAccount,
        turn_context: TurnContext
    ):
        for member_added in members_added:
            if member_added.id != turn_context.activity.recipient.id:
                await turn_context.send_activity("Hello and welcome!##")


