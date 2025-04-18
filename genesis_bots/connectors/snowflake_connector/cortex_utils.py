import os
import json
import hashlib
import time
import requests
from datetime import datetime
import base64
import requests
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
import jwt
from genesis_bots.core.logging_config import logger
from genesis_bots.core.bot_os_llm import BotLlmEngineEnum

def check_cortex_available(self):
    if os.environ.get("CORTEX_AVAILABLE", 'False') in ['False', '']:
        os.environ["CORTEX_AVAILABLE"] = 'False'
    if os.getenv("CORTEX_VIA_COMPLETE",'False') in ['False', '']:
        os.environ["CORTEX_VIA_COMPLETE"] = 'False'

    if self.source_name == "Snowflake" and os.getenv("CORTEX_AVAILABLE", "False").lower() == 'false':
        try:
            cortex_test = self.test_cortex_via_rest()

            if cortex_test == True:
                os.environ["CORTEX_AVAILABLE"] = 'True'
                self.default_llm_engine = BotLlmEngineEnum.cortex
                self.llm_api_key = 'cortex_no_key_needed'
                logger.info('Cortex LLM is Available via REST and successfully tested')
                return True
            else:
                os.environ["CORTEX_MODE"] = "False"
                os.environ["CORTEX_AVAILABLE"] = 'False'
                logger.info('Cortex LLM is not available via REST ')
                return False
        except Exception as e:
            logger.info('Cortex LLM Not available via REST, exception on test: ',e)
            return False

    return self.source_name == "Snowflake" and os.getenv("CORTEX_AVAILABLE", "False").lower() == 'true'

def test_cortex(self):
    newarray = [{"role": "user", "content": "hi there"} ]
    new_array_str = json.dumps(newarray)

    logger.info(f"snowflake_connector test calling cortex {self.llm_engine} via SQL, content est tok len=",len(new_array_str)/4)

    context_limit = 128000 * 4 #32000 * 4
    cortex_query = f"""
        select SNOWFLAKE.CORTEX.COMPLETE('{self.llm_engine}', %s) as completion;
    """
    try:
        cursor = self.connection.cursor()
        start_time = time.time()
        try:
            cursor.execute(cortex_query, (new_array_str,))
        except Exception as e:
            if 'unknown model' in e.msg:
                logger.info(f'Model {self.llm_engine} not available in this region, trying llama3.1-70b')
                self.llm_engine = 'llama3.1-70b'
                cortex_query = f"""
                    select SNOWFLAKE.CORTEX.COMPLETE('{self.llm_engine}', %s) as completion; """
                cursor.execute(cortex_query, (new_array_str,))
                logger.info('Ok that worked, changing CORTEX_MODEL ENV VAR to llama3.1-70b')
                os.environ['CORTEX_MODEL'] = 'llama3.1-70b'
                os.environ['CORTEX_AVAILABLE'] = 'True'
            else:
                # TODO remove llmkey handler from this file
                os.environ['CORTEX_MODE'] = 'False'
                os.environ['CORTEX_AVAILABLE'] = 'False'
                raise(e)
        self.connection.commit()
        elapsed_time = time.time() - start_time
        result = cursor.fetchone()
        completion = result[0] if result else None

        if completion == True:
            logger.info(f"snowflake_connector test call result: ",completion)
            return True
        else:
            logger.info("Cortex complete failed to return a result")
            return False
    except Exception as e:
        logger.info('cortex not available, query error: ',e)
        self.connection.rollback()
        os.environ['CORTEX_MODE'] = 'False'
        os.environ['CORTEX_AVAILABLE'] = 'False'
        return False

def test_cortex_via_rest(self):
    if os.getenv("CORTEX_OFF", "").upper() == "TRUE":
        logger.info('CORTEX OFF ENV VAR SET -- SIMULATING NO CORTEX')
        return False
    response, status_code  = self.cortex_chat_completion("Hi there", test=True)
    if status_code != 200:
        # logger.info(f"Failed to connect to Cortex API. Status code: {status_code} RETRY 1")
        response, status_code  = self.cortex_chat_completion("Hi there", test=True)
        if status_code != 200:
            #   logger.info(f"Failed to connect to Cortex API. Status code: {status_code} RETRY 2")
            response, status_code  = self.cortex_chat_completion("Hi there",test=True)
            if status_code != 200:
                #      logger.info(f"Failed to connect to Cortex API. Status code: {status_code} FAILED AFTER 3 TRIES")
                return False

    if len(response) > 2:
        os.environ['CORTEX_AVAILABLE'] = 'True'
        return True
    else:
        os.environ['CORTEX_MODE'] = 'False'
        os.environ['CORTEX_AVAILABLE'] = 'False'
        return False

def cortex_chat_completion(self, prompt, system=None, test=False):
    if system:
        newarray = [{"role": "user", "content": system}, {"role": "user", "content": prompt} ]
    else:
        newarray = [{"role": "user", "content": prompt} ]

    try:
        SNOWFLAKE_HOST = self.client.host
        REST_TOKEN = self.client.rest.token
        url=f"https://{SNOWFLAKE_HOST}/api/v2/cortex/inference:complete"
        headers = {
            "Accept": "text/event-stream",
            "Content-Type": "application/json",
            "Authorization": f'Snowflake Token="{REST_TOKEN}"',
        }

        request_data = {
            "model": self.llm_engine,
            "messages": newarray,
            "stream": True,
        }

        if not test:
            logger.info(f"snowflake_connector calling cortex {self.llm_engine} via REST API, content est tok len=",len(str(newarray))/4)

        response = requests.post(url, json=request_data, stream=True, headers=headers)

        if response.status_code in (200, 400) and response.text.startswith('{"message":"unknown model '):
            # Try models in order until one works
            models_to_try = [
                os.getenv("CORTEX_PREMIERE_MODEL", "claude-3-5-sonnet"),
                os.getenv("CORTEX_MODEL", "llama3.1-405b"),
                os.getenv("CORTEX_FAST_MODEL_NAME", "llama3.1-70b")
            ]
            logger.info(f"Model not {self.llm_engine} active. Trying all models in priority order.")

            for model in models_to_try:
                request_data["model"] = model
                response = requests.post(url, json=request_data, stream=True, headers=headers)
                
                if response.status_code == 200 and not response.text.startswith('{"message":"unknown model'):
                    # Found working model
                    self.llm_engine = model
                    os.environ["CORTEX_MODEL"] = model
                    os.environ["CORTEX_PREMIERE_MODEL"] = model
                    logger.info(f"Found working model {model}")
                    break
                else:
                    logger.info(f"Model {model} not working, trying next model.")
            else:
                # No models worked
                logger.info(f'No available Cortex models found after trying: {models_to_try}')
                return False, False

        curr_resp = ''
        for line in response.iter_lines():
            if line:
                try:
                    decoded_line = line.decode('utf-8')
                    if not decoded_line.strip():
                        #       logger.info("Received an empty line.")
                        continue
                    if decoded_line.startswith("data: "):
                        decoded_line = decoded_line[len("data: "):]
                    event_data = json.loads(decoded_line)
                    if 'choices' in event_data:
                        d = event_data['choices'][0]['delta'].get('content','')
                        curr_resp += d
                        #          logger.info(d)
                except json.JSONDecodeError as e:
                    logger.info(f"Error decoding JSON: {e}")
                    continue

        return curr_resp, response.status_code

    except Exception as e:
        logger.info("Bottom of function -- Error calling Cortex Rest API, ",e)
        return False, False

def get_cortex_search_service(self):
    """
    Executes a query to retrieve a summary of the harvest results, including the source name, database name, schema name,
    role used for crawl, last crawled timestamp, and the count of objects crawled, grouped and ordered by the source name,
    database name, schema name, and role used for crawl.
    
    Returns:
        list: A list of dictionaries, each containing the harvest summary for a group.
    """
    query = f"""
        SHOW CORTEX SEARCH SERVICES;
    """
    try:
        cursor = self.client.cursor()
        cursor.execute(query)
        results = cursor.fetchall()

        # Convert the query results to a list of dictionaries
        summary = [
            dict(zip([column[0] for column in cursor.description], row))
            for row in results
        ]

        json_data = json.dumps(
            summary, default=str
        )  # default=str to handle datetime and other non-serializable types

        return {"Success": True, "Data": json_data}

    except Exception as e:
        err = f"An error occurred while retrieving the harvest summary: {e}"
        return {"Success": False, "Error": err}

def cortex_search(self,  query: str, service_name: str='service', top_n: int=1, thread_id=None):
    try:
        def generate_jwt_token(private_key_path, account, user, role="ACCOUNTADMIN"):
            # Uppercase account and user
            account = account.upper()
            user = user.upper()
            qualified_username = account + "." + user

            # Current time and token lifetime
            now = datetime.datetime.now(datetime.timezone.utc)
            lifetime = datetime.timedelta(minutes=59)

            # Load the private key
            password = os.getenv("PRIVATE_KEY_PASSWORD")
            if password:
                password = password.encode()
            with open(private_key_path, "rb") as key_file:
                private_key = serialization.load_pem_private_key(
                    key_file.read(),
                    password=password,
                    backend=default_backend()
                )

            public_key_raw = private_key.public_key().public_bytes(serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo)

            # Get the sha256 hash of the raw bytes.
            sha256hash = hashlib.sha256()
            sha256hash.update(public_key_raw)

            # Base64-encode the value and prepend the prefix 'SHA256:'.
            public_key_fp = 'SHA256:' + base64.b64encode(sha256hash.digest()).decode('utf-8')

            # Payload for the token
            payload = {
                "iss": qualified_username + '.' + public_key_fp,
                "sub": qualified_username,
                "iat": now,
                "exp": now + lifetime
            }

            logger.info(payload)

            # Generate the JWT token
            encoding_algorithm = "RS256"
            token = jwt.encode(payload, key=private_key, algorithm=encoding_algorithm)

            # Convert to string if necessary
            if isinstance(token, bytes):
                token = token.decode('utf-8')

            return token

        def make_api_request(jwt_token, api_endpoint, payload):
            # Define headers
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {jwt_token}",
                "Accept": "application/json",
                "User-Agent": "myApplicationName/1.0",
                "X-Snowflake-Authorization-Token-Type": "KEYPAIR_JWT"
            }

            # Make the POST request
            response = requests.post(api_endpoint, headers=headers, json=payload)

            print (response)
            # Print the response status and data
            logger.info(f"Status Code: {response.status_code}")
            logger.info(f"Response: {response.json()}")
            return response

        schema = os.getenv("GENESIS_INTERNAL_DB_SCHEMA").split('.')[-1]
        # service_name = 'HARVEST_SEARCH_SERVICE'.lower()
        api_endpoint = f'https://{self.client.host}/api/v2/databases/{self.database}/schemas/{schema}/cortex-search-services/{service_name}:query'

        payload = {"query": query, "limit": top_n}
        private_key_path = ".keys/rsa_key.p8"
        account = os.getenv("SNOWFLAKE_ACCOUNT_OVERRIDE")
        user = os.getenv("SNOWFLAKE_USER_OVERRIDE")

        jwt_token = generate_jwt_token(private_key_path, account, user)
        response = make_api_request(jwt_token, api_endpoint, payload)

        return response.text, response.status_code

    except Exception as e:
        print ("Bottom of function -- Error calling Cortex Search Rest API, ",e)
        return False, False

def _cortex_complete(self, model="llama3.1-405b", prompt=None):
    try:
        from snowflake.cortex import Complete

        result = Complete(model, str(prompt))
    except Exception as e:
        logger.info(f"Cortex not available: {e}")
        self.sp_session = None
        result = None
    return result
