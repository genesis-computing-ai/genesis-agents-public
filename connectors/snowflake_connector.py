from snowflake.connector import connect

import os
import json
import logging
from itertools import islice
from datetime import datetime
import uuid
import os
import time
import hashlib
import yaml, time, random, string
import snowflake.connector
import random, string
import requests
from openai import OpenAI

from .database_connector import DatabaseConnector
from core.bot_os_defaults import (
    BASE_EVE_BOT_INSTRUCTIONS,
    ELIZA_DATA_ANALYST_INSTRUCTIONS,
    STUART_DATA_STEWARD_INSTRUCTIONS,
    JANICE_JANITOR_INSTRUCTIONS,
    EVE_INTRO_PROMPT,
    ELIZA_INTRO_PROMPT,
    STUART_INTRO_PROMPT,
    JANICE_INTRO_PROMPT,
)

# from database_connector import DatabaseConnector
from threading import Lock
import base64
import requests
import re
from tqdm import tqdm

import core.bot_os_tool_descriptions

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.WARN, format="%(asctime)s - %(levelname)s - %(message)s"
)

_semantic_lock = Lock()


class SnowflakeConnector(DatabaseConnector):

    def __init__(self, connection_name, bot_database_creds=None):
        super().__init__(connection_name)
        # print('Snowflake connector entry...')

        account, database, user, password, warehouse, role = [None] * 6

        if bot_database_creds:
            account = bot_database_creds.get("account")
            database = bot_database_creds.get("database")
            user = bot_database_creds.get("user")
            password = bot_database_creds.get("pwd")
            warehouse = bot_database_creds.get("warehouse")
            role = bot_database_creds.get("role")

        # used to get the default value if not none, otherwise get env var. allows local mode to work with bot credentials
        def get_env_or_default(value, env_var):
            return value if value is not None else os.getenv(env_var)

        self.account = get_env_or_default(account, "SNOWFLAKE_ACCOUNT_OVERRIDE")
        self.user = get_env_or_default(user, "SNOWFLAKE_USER_OVERRIDE")
        self.password = get_env_or_default(password, "SNOWFLAKE_PASSWORD_OVERRIDE")
        self.database = get_env_or_default(database, "SNOWFLAKE_DATABASE_OVERRIDE")
        self.warehouse = get_env_or_default(warehouse, "SNOWFLAKE_WAREHOUSE_OVERRIDE")
        self.role = get_env_or_default(role, "SNOWFLAKE_ROLE_OVERRIDE")        
        self.source_name = "Snowflake"


        # print('Calling _create_connection...')
        self.token_connection = False
        self.connection = self._create_connection()
        self.semantic_models_map = {}

        self.client = self.connection
        self.schema = os.getenv("GENESIS_INTERNAL_DB_SCHEMA", "GENESIS_INTERNAL")

        if os.getenv("CORTEX_MODEL", None) is not None and os.getenv("CORTEX_MODEL", '') != '':
            self.llm_engine =  os.getenv("CORTEX_MODEL", None)
        else:
            self.llm_engine = 'llama3.1-405b'

        # self.client = self._create_client()
        self.genbot_internal_project_and_schema = os.getenv("GENESIS_INTERNAL_DB_SCHEMA", "None")
        if self.genbot_internal_project_and_schema == "None":
            # Todo remove, internal note
            print("ENV Variable GENESIS_INTERNAL_DB_SCHEMA is not set.")
        if self.genbot_internal_project_and_schema is not None:
            self.genbot_internal_project_and_schema = (self.genbot_internal_project_and_schema.upper() )

        if self.database:
            self.project_id = self.database
        else:
            db, sch = self.genbot_internal_project_and_schema.split('.')
            self.project_id = db

        self.genbot_internal_harvest_table = os.getenv("GENESIS_INTERNAL_HARVEST_RESULTS_TABLE", "harvest_results" )
        self.genbot_internal_harvest_control_table = os.getenv("GENESIS_INTERNAL_HARVEST_CONTROL_TABLE", "harvest_control")
        self.genbot_internal_message_log = os.getenv("GENESIS_INTERNAL_MESSAGE_LOG_TABLE", "MESSAGE_LOG")
        self.genbot_internal_knowledge_table = os.getenv("GENESIS_INTERNAL_KNOWLEDGE_TABLE", "KNOWLEDGE")
        self.genbot_internal_processes_table = os.getenv("GENESIS_INTERNAL_PROCESSES_TABLE", "PROCESSES" )
        self.genbot_internal_process_history_table = os.getenv("GENESIS_INTERNAL_PROCESS_HISTORY_TABLE", "PROCESS_HISTORY" )
        self.genbot_internal_user_bot_table = os.getenv("GENESIS_INTERNAL_USER_BOT_TABLE", "USER_BOT")
        self.app_share_schema = "APP_SHARE"

        # print("genbot_internal_project_and_schema: ", self.genbot_internal_project_and_schema)
        self.metadata_table_name = (
            self.genbot_internal_project_and_schema
            + "."
            + self.genbot_internal_harvest_table
        )
        self.harvest_control_table_name = (
            self.genbot_internal_project_and_schema
            + "."
            + self.genbot_internal_harvest_control_table
        )
        self.message_log_table_name = (
            self.genbot_internal_project_and_schema
            + "."
            + self.genbot_internal_message_log
        )
        self.knowledge_table_name = (
            self.genbot_internal_project_and_schema
            + "."
            + self.genbot_internal_knowledge_table
        )
        self.processes_table_name = (
            self.genbot_internal_project_and_schema
            + "."
            + self.genbot_internal_processes_table
        )
        self.process_history_table_name = (
            self.genbot_internal_project_and_schema
            + "."
            + self.genbot_internal_process_history_table
        )
        self.user_bot_table_name = (
            self.genbot_internal_project_and_schema
            + "."
            + self.genbot_internal_user_bot_table
        )
        self.slack_tokens_table_name = (
            self.genbot_internal_project_and_schema + "." + "SLACK_APP_CONFIG_TOKENS"
        )
        self.available_tools_table_name = (
            self.genbot_internal_project_and_schema + "." + "AVAILABLE_TOOLS"
        )
        self.bot_servicing_table_name = (
            self.genbot_internal_project_and_schema + "." + "BOT_SERVICING"
        )
        self.ngrok_tokens_table_name = (
            self.genbot_internal_project_and_schema + "." + "NGROK_TOKENS"
        )
        self.images_table_name = self.app_share_schema + "." + "IMAGES"  

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
                    self.default_llm_engine = 'cortex'
                    
                    self.llm_api_key = 'cortex_no_key_needed'
                    print('\nCortex LLM is Available via REST and successfully tested')
                    return True
                else:
                    os.environ["CORTEX_MODE"] = "False"
                    os.environ["CORTEX_AVAILABLE"] = 'False'
                    print('\nCortex LLM is not available via REST ')
                    return False
            except Exception as e:
                print('Cortex LLM Not available via REST, exception on test: ',e)
                return False 
        if self.source_name == "Snowflake" and os.getenv("CORTEX_AVAILABLE", "False").lower() == 'true':
            return True
        else:
            return False

            # if os.environ["CORTEX_AVAILABLE"] == 'False' or os.getenv("CORTEX_VIA_COMPLETE",'False').lower() == 'true':
            #     try:
            #         cortex_test = self.test_cortex()

            #         if cortex_test == True:
            #             os.environ["CORTEX_AVAILABLE"] = 'True'
            #             os.environ["CORTEX_VIA_COMPLETE"] = 'True'
            #             # os.environ["CORTEX_MODE"] = "True"
            #             self.default_llm_engine = 'cortex'
            #             self.llm_api_key = 'cortex_no_key_needed'
            #             print('Cortex LLM is Available via SQL COMPLETE() and successfully tested')
            #             return True
            #         else:
            #             os.environ["CORTEX_MODE"] = "False"
            #             os.environ["CORTEX_AVAILABLE"] = 'False'
            #             return False
            #     except Exception as e:
            #         print('Cortex LLM Not available via SQL COMPLETE(), exception on test: ',e)
            #         return False
            # else:
            #     return True


    def test_cortex(self):
        newarray = [{"role": "user", "content": "hi there"} ]
        new_array_str = json.dumps(newarray)

        print(f"snowflake_connector test calling cortex {self.llm_engine} via SQL, content est tok len=",len(new_array_str)/4)

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
                    print(f'Model {self.llm_engine} not available in this region, trying llama3.1-70b')
                    self.llm_engine = 'llama3.1-70b'        
                    cortex_query = f"""
                        select SNOWFLAKE.CORTEX.COMPLETE('{self.llm_engine}', %s) as completion; """
                    cursor.execute(cortex_query, (new_array_str,))
                    print('Ok that worked, changing CORTEX_MODEL ENV VAR to llama3.1-70b')
                    os.environ['CORTEX_MODEL'] = 'llama3.1-70b'
                    os.environ['CORTEX_AVAILABLE'] = 'True'
                else:
                    #TODO remove llmkey handler from this file
                    os.environ['CORTEX_MODE'] = 'False'
                    os.environ['CORTEX_AVAILABLE'] = 'False'
                    raise(e)
            self.connection.commit()
            elapsed_time = time.time() - start_time
            result = cursor.fetchone()
            completion = result[0] if result else None

            if completion == True:
                print(f"snowflake_connector test call result: ",completion)
                return True
            else:
                print("Cortex complete failed to return a result")
                return False
        except Exception as e:
            print('cortex not available, query error: ',e)
            self.connection.rollback()
            os.environ['CORTEX_MODE'] = 'False'
            os.environ['CORTEX_AVAILABLE'] = 'False'
            return False
 

    def test_cortex_via_rest(self):
        response, status_code  = self.cortex_chat_completion("Hi there")
        if status_code != 200:
            print(f"Failed to connect to Cortex API. Status code: {status_code}")
            return False

        if len(response) > 2:
            os.environ['CORTEX_AVAILABLE'] = 'True'
            return True
        else:
            os.environ['CORTEX_MODE'] = 'False'
            os.environ['CORTEX_AVAILABLE'] = 'False'
            return False

    def cortex_chat_completion(self, prompt):
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

            print(f"snowflake_connector test calling cortex {self.llm_engine} via REST API, content est tok len=",len(str(newarray))/4)

            response = requests.post(url, json=request_data, stream=True, headers=headers)

            curr_resp = ''
            for line in response.iter_lines():
                if line:
                    try:
                        decoded_line = line.decode('utf-8')
                        if not decoded_line.strip():
                            print("Received an empty line.")
                            continue
                        if decoded_line.startswith("data: "):
                            decoded_line = decoded_line[len("data: "):]
                        event_data = json.loads(decoded_line)
                        if 'choices' in event_data:
                            d = event_data['choices'][0]['delta'].get('content','')
                            curr_resp += d
                            print(d, end='', flush=True)
                    except json.JSONDecodeError as e:
                        print(f"Error decoding JSON: {e}")
                        continue

            return curr_resp, response.status_code
        
        except Exception as e:
            print ("Bottom of function -- Error calling Cortex Rest API, ",e, flush=True)
            return False, False

    def _create_snowpark_connection(self):
        try:
            from snowflake.snowpark import Session
            from snowflake.cortex import Complete

            connection_parameters = {
                "account": os.getenv("SNOWFLAKE_ACCOUNT_OVERRIDE"),
                "user": os.getenv("SNOWFLAKE_USER_OVERRIDE"),
                "password": os.getenv("SNOWFLAKE_PASSWORD_OVERRIDE"),
                "role": os.getenv("SNOWFLAKE_ROLE_OVERRIDE", "PUBLIC"),  # optional
                "warehouse": os.getenv(
                    "SNOWFLAKE_WAREHOUSE_OVERRIDE", "XSMALL"
                ),  # optional
                "database": os.getenv(
                    "SNOWFLAKE_DATABASE_OVERRIDE", "GENESIS_TEST"
                ),  # optional
                "schema": os.getenv(
                    "GENESIS_INTERNAL_DB_SCHEMA", "GENESIS_TEST.GENESIS_JL"
                ),  # optional
            }

            sp_session = Session.builder.configs(connection_parameters).create()

        except Exception as e:
            print(f"Cortex not available: {e}")
            sp_session = None
        return sp_session

    def _cortex_complete(self, model="reka-flash", prompt=None):
        try:
            from snowflake.cortex import Complete

            result = Complete(model, str(prompt))
        except Exception as e:
            print(f"Cortex not available: {e}")
            self.sp_session = None
            result = None
        return result

    def sha256_hash_hex_string(self, input_string):
        # Encode the input string to bytes, then create a SHA256 hash and convert it to a hexadecimal string
        return hashlib.sha256(input_string.encode()).hexdigest()

    def get_harvest_control_data_as_json(self, thread_id=None):
        """
        Retrieves all the data from the harvest control table and returns it as a JSON object.

        Returns:
            JSON object: All the data from the harvest control table.
        """

        try:
            query = f"SELECT * FROM {self.harvest_control_table_name}"
            cursor = self.connection.cursor()
            cursor.execute(query)
            columns = [col[0] for col in cursor.description]

            # Fetch all results
            data = cursor.fetchall()

            # Convert the query results to a list of dictionaries
            rows = [dict(zip(columns, row)) for row in data]

            # Convert the list of dictionaries to a JSON object
            json_data = json.dumps(
                rows, default=str
            )  # default=str to handle datetime and other non-serializable types

            cursor.close()
            return {"Success": True, "Data": json_data}

        except Exception as e:
            err = f"An error occurred while retrieving the harvest control data: {e}"
            return {"Success": False, "Error": err}

    # snowed
    # SEE IF THIS WAY OF DOING BIND VARS WORKS, if so do it everywhere
    def set_harvest_control_data(
        self,
        source_name,
        database_name,
        initial_crawl_complete=False,
        refresh_interval=1,
        schema_exclusions=None,
        schema_inclusions=None,
        status="Include",
        thread_id=None,
    ):
        """
        Inserts or updates a row in the harvest control table using MERGE statement with explicit parameters for Snowflake.

        Args:
            source_name (str): The source name for the harvest control data.
            database_name (str): The database name for the harvest control data.
            initial_crawl_complete (bool): Flag indicating if the initial crawl is complete. Defaults to False.
            refresh_interval (int): The interval at which the data is refreshed. Defaults to 1.
            schema_exclusions (list): A list of schema names to exclude. Defaults to an empty list.
            schema_inclusions (list): A list of schema names to include. Defaults to an empty list.
            status (str): The status of the harvest control. Defaults to 'Include'.
        """
        try:
            # Set default values for schema_exclusions and schema_inclusions if None
            if schema_exclusions is None:
                schema_exclusions = []
            if schema_inclusions is None:
                schema_inclusions = []
            # Confirm the database and schema names are correct and match the case
            # First, get the list of databases and check the case
            databases = self.get_visible_databases()
            if database_name not in databases:
                return {
                    "Success": False,
                    "Error": f"Database {database_name} does not exist.",
                }
            # Now, get the list of schemas in the database and check the case
            schemas = self.get_schemas(database_name)
            if schema_exclusions:
                for schema in schema_exclusions:
                    if schema.upper() not in (s.upper() for s in schemas):
                        return {
                            "Success": False,
                            "Error": f"Schema exclusion {schema} does not exist in database {database_name}.",
                        }
            if schema_inclusions:
                for schema in schema_inclusions:
                    if schema.upper() not in (s.upper() for s in schemas):
                        return {
                            "Success": False,
                            "Error": f"Schema inclusion {schema} does not exist in database {database_name}.",
                        }
            # Ensure the case of the database and schema names matches that returned by the get_databases and get_schemas functions
            database_name = next(
                (db for db in databases if db.upper() == database_name.upper()),
                database_name,
            )
            schema_exclusions = [
                next((sch for sch in schemas if sch.upper() == schema.upper()), schema)
                for schema in schema_exclusions
            ]
            schema_inclusions = [
                next((sch for sch in schemas if sch.upper() == schema.upper()), schema)
                for schema in schema_inclusions
            ]

            # Prepare the MERGE statement for Snowflake
            merge_statement = f"""
            MERGE INTO {self.harvest_control_table_name} T
            USING (SELECT %(source_name)s AS source_name, %(database_name)s AS database_name) S
            ON T.source_name = S.source_name AND T.database_name = S.database_name
            WHEN MATCHED THEN
              UPDATE SET
                initial_crawl_complete = %(initial_crawl_complete)s,
                refresh_interval = %(refresh_interval)s,
                schema_exclusions = %(schema_exclusions)s,
                schema_inclusions = %(schema_inclusions)s,
                status = %(status)s
            WHEN NOT MATCHED THEN
              INSERT (source_name, database_name, initial_crawl_complete, refresh_interval, schema_exclusions, schema_inclusions, status)
              VALUES (%(source_name)s, %(database_name)s, %(initial_crawl_complete)s, %(refresh_interval)s, %(schema_exclusions)s, %(schema_inclusions)s, %(status)s)
            """

            # Execute the MERGE statement
            self.client.cursor().execute(
                merge_statement,
                {
                    "source_name": source_name,
                    "database_name": database_name,
                    "initial_crawl_complete": initial_crawl_complete,
                    "refresh_interval": refresh_interval,
                    "schema_exclusions": str(schema_exclusions),
                    "schema_inclusions": str(schema_inclusions),
                    "status": status,
                },
            )

            return {
                "Success": True,
                "Message": "Harvest control data set successfully.",
            }

        except Exception as e:
            err = f"An error occurred while setting the harvest control data: {e}"
            return {"Success": False, "Error": err}

    def remove_harvest_control_data(self, source_name, database_name, thread_id=None):
        """
        Removes a row from the harvest control table based on the source_name and database_name.

        Args:
            source_name (str): The source name of the row to remove.
            database_name (str): The database name of the row to remove.
        """
        try:
            # TODO test!! Construct the query to exclude the row
            query = f"""
            UPDATE {self.harvest_control_table_name}
            SET STATUS = 'Exclude'
            WHERE UPPER(source_name) = UPPER(%s) AND UPPER(database_name) = UPPER(%s) AND STATUS = 'Include'
            """
            # Execute the query
            cursor = self.client.cursor()
            cursor.execute(query, (source_name, database_name))
            affected_rows = cursor.rowcount

            if affected_rows == 0:
                return {
                    "Success": False,
                    "Message": "No harvest records were found for that source and database. You should check the source_name and database_name with the get_harvest_control_data tool ?",
                }
            else:
                return {
                    "Success": True,
                    "Message": f"Harvest control data removed successfully. {affected_rows} rows affected.",
                }

        except Exception as e:
            err = f"An error occurred while removing the harvest control data: {e}"
            return {"Success": False, "Error": err}

    def remove_metadata_for_database(self, source_name, database_name, thread_id=None):
        """
        Removes rows from the metadata table based on the source_name and database_name.

        Args:
            source_name (str): The source name of the rows to remove.
            database_name (str): The database name of the rows to remove.
        """
        try:
            # Construct the query to delete the rows
            delete_query = f"""
            DELETE FROM {self.metadata_table_name}
            WHERE source_name = %s AND database_name = %s
            """
            # Execute the query
            cursor = self.client.cursor()
            cursor.execute(delete_query, (source_name, database_name))
            affected_rows = cursor.rowcount

            return {
                "Success": True,
                "Message": f"Metadata rows removed successfully. {affected_rows} rows affected.",
            }

        except Exception as e:
            err = f"An error occurred while removing the metadata rows: {e}"
            return {"Success": False, "Error": err}

    def get_available_databases(self, thread_id=None):
        """
        Retrieves a list of databases and their schemas that are not currently being harvested per the harvest_control table.

        Returns:
            dict: A dictionary with a success flag and either a list of available databases with their schemas or an error message.
        """
        try:
            # Get the list of visible databases
            visible_databases_result = self.get_visible_databases_json()
            if not visible_databases_result:
                return {
                    "Success": False,
                    "Message": "An error occurred while retrieving visible databases",
                }

            visible_databases = visible_databases_result
            # Filter out databases that are currently being harvested
            query = f"""
            SELECT DISTINCT database_name
            FROM {self.harvest_control_table_name}
            WHERE status = 'Include'
            """
            cursor = self.client.cursor()
            cursor.execute(query)
            harvesting_databases = {row[0] for row in cursor.fetchall()}

            available_databases = []
            for database in visible_databases:
                if database not in harvesting_databases:
                    # Get the list of schemas for the database
                    schemas_result = self.get_schemas(database)
                    if schemas_result:
                        available_databases.append(
                            {"DatabaseName": database, "Schemas": schemas_result}
                        )

            if not available_databases:
                return {
                    "Success": False,
                    "Message": "No available databases to display.",
                }

            return {"Success": True, "Data": json.dumps(available_databases)}

        except Exception as e:
            err = f"An error occurred while retrieving available databases: {e}"
            return {"Success": False, "Error": err}

    def get_visible_databases_json(self, thread_id=None):
        """
        Retrieves a list of all visible databases.

        Returns:
            list: A list of visible database names.
        """
        try:
            query = "SHOW DATABASES"
            cursor = self.client.cursor()
            cursor.execute(query)
            results = cursor.fetchall()

            databases = [
                row[1] for row in results
            ]  # Assuming the database name is in the second column

            return {"Success": True, "Databases": databases}

        except Exception as e:
            err = f"An error occurred while retrieving visible databases: {e}"
            return {"Success": False, "Error": err}

    def get_schemas(self, database_name, thread_id=None):
        """
        Retrieves a list of all schemas in a given database.
        Args:
            database_name (str): The name of the database to retrieve schemas from.

        Returns:
            list: A list of schema names in the given database.
        """
        try:
            query = f"SHOW SCHEMAS IN DATABASE {database_name}"
            cursor = self.client.cursor()
            cursor.execute(query)
            results = cursor.fetchall()

            schemas = [
                row[1] for row in results
            ]  # Assuming the schema name is in the second column

            return {"Success": True, "Schemas": schemas}

        except Exception as e:
            err = f"An error occurred while retrieving schemas from database {database_name}: {e}"
            return {"Success": False, "Error": err}

    def get_shared_schemas(self, database_name):
        try:
            query = f"SELECT DISTINCT SCHEMA_NAME FROM {self.metadata_table_name} where DATABASE_NAME = '{database_name}'"
            cursor = self.client.cursor()
            cursor.execute(query)
            schemas = cursor.fetchall()
            schema_list = [schema[0] for schema in schemas]
            # for schema in schema_list:
            #     print(f"can we see baseball and f1?? {schema}")
            return schema_list

        except Exception as e:
            err = f"An error occurred while retrieving shared schemas: {e}"
            return "Error: {err}"
        
    def get_bot_images(self, thread_id=None):
        """
        Retrieves a list of all bot avatar images.

        Returns:
            list: A list of bot names and bot avatar images.
        """
        try:
            query = f"SELECT BOT_NAME, BOT_AVATAR_IMAGE FROM {self.bot_servicing_table_name} "
            cursor = self.client.cursor()
            cursor.execute(query)
            bots = cursor.fetchall()
            columns = [col[0].lower() for col in cursor.description]
            bot_list = [dict(zip(columns, bot)) for bot in bots]
            # Check the total payload size
            payload_size = sum(len(str(bot).encode('utf-8')) for bot in bot_list)
            # If payload size exceeds 16MB (16 * 1024 * 1024 bytes) (with buffer for JSON) remove rows from the bottom
            while payload_size > 15.9 * 1000 * 1000 and len(bot_list) > 0:
                bot_list.pop()
                payload_size = sum(len(str(bot).encode('utf-8')) for bot in bot_list)
            json_data = json.dumps(
                bot_list, default=str
            )  # default=str to handle datetime and other non-serializable types

            return {"Success": True, "Data": json_data}

        except Exception as e:
            err = f"An error occurred while retrieving bot images: {e}"
            return {"Success": False, "Error": err}

    def get_llm_info(self, thread_id=None):
        """
        Retrieves a list of all llm types and keys.

        Returns:
            list: A list of llm keys, llm types, and the active switch.
        """
        try:
            query = f"SELECT LLM_TYPE, ACTIVE, LLM_KEY FROM {self.genbot_internal_project_and_schema}.LLM_TOKENS WHERE LLM_KEY is not NULL"
            cursor = self.client.cursor()
            cursor.execute(query)
            llm_info = cursor.fetchall()
            columns = [col[0].lower() for col in cursor.description]
            llm_list = [dict(zip(columns, llm)) for llm in llm_info]
            json_data = json.dumps(
                llm_list, default=str
            )  # default=str to handle datetime and other non-serializable types

            return {"Success": True, "Data": json_data}


        except Exception as e:
            err = f"An error occurred while retrieving bot images: {e}"
            return {"Success": False, "Error": err}


    def get_harvest_summary(self, thread_id=None):
        """
        Executes a query to retrieve a summary of the harvest results, including the source name, database name, schema name,
        role used for crawl, last crawled timestamp, and the count of objects crawled, grouped and ordered by the source name,
        database name, schema name, and role used for crawl.

        Returns:
            list: A list of dictionaries, each containing the harvest summary for a group.
        """
        query = f"""
        SELECT source_name, database_name, schema_name, role_used_for_crawl, 
               MAX(last_crawled_timestamp) AS last_change_ts, COUNT(*) AS objects_crawled 
        FROM {self.metadata_table_name}
        GROUP BY source_name, database_name, schema_name, role_used_for_crawl
        ORDER BY source_name, database_name, schema_name, role_used_for_crawl;
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

    def table_summary_exists(self, qualified_table_name):
        query = f"""
        SELECT COUNT(*)
        FROM {self.metadata_table_name}
        WHERE qualified_table_name = %s
        """
        try:
            cursor = self.client.cursor()
            cursor.execute(query, (qualified_table_name,))
            result = cursor.fetchone()

            return result[0] > 0  # Returns True if a row exists, False otherwise
        except Exception as e:
            print(f"An error occurred while checking if the table summary exists: {e}")
            return False

    def insert_chat_history_row(
        self,
        timestamp,
        bot_id=None,
        bot_name=None,
        thread_id=None,
        message_type=None,
        message_payload=None,
        message_metadata=None,
        tokens_in=None,
        tokens_out=None,
        files=None,
        channel_type=None,
        channel_name=None,
        primary_user=None,
        task_id=None,
    ):
        """
        Inserts a single row into the chat history table using Snowflake's streaming insert.

        :param timestamp: TIMESTAMP field, format should be compatible with Snowflake.
        :param bot_id: STRING field representing the bot's ID.
        :param bot_name: STRING field representing the bot's name.
        :param thread_id: STRING field representing the thread ID, can be NULL.
        :param message_type: STRING field representing the type of message.
        :param message_payload: STRING field representing the message payload, can be NULL.
        :param message_metadata: STRING field representing the message metadata, can be NULL.
        :param tokens_in: INTEGER field representing the number of tokens in, can be NULL.
        :param tokens_out: INTEGER field representing the number of tokens out, can be NULL.
        :param files: STRING field representing the list of files, can be NULL.
        :param channel_type: STRING field representing Slack_channel, Slack_DM, Streamlit, can be NULL.
        :param channel_name: STRING field representing Slack channel name, or the name of the user the DM, can be NULL.
        :param primary_user: STRING field representing the who sent the original message, can be NULL.
        :param task_id: STRING field representing the task, can be NULL.
        """
        cursor = None
        if files is None:
            files = []
        files_str = str(files)
        if files_str == "":
            files_str = "<no files>"
        try:
            # Ensure the timestamp is in the correct format for Snowflake
            formatted_timestamp = (
                timestamp.strftime("%Y-%m-%d %H:%M:%S")
                if isinstance(timestamp, datetime)
                else timestamp
            )
            if isinstance(message_metadata, dict):
                message_metadata = json.dumps(message_metadata)

            insert_query = f"""
            INSERT INTO {self.message_log_table_name} 
                (timestamp, bot_id, bot_name, thread_id, message_type, message_payload, message_metadata, tokens_in, tokens_out, files, channel_type, channel_name, primary_user, task_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor = self.client.cursor()
            cursor.execute(
                insert_query,
                (
                    formatted_timestamp,
                    bot_id,
                    bot_name,
                    thread_id,
                    message_type,
                    message_payload,
                    message_metadata,
                    tokens_in,
                    tokens_out,
                    files_str,
                    channel_type,
                    channel_name,
                    primary_user,
                    task_id,
                ),
            )
            self.client.commit()
        except Exception as e:
            print(
                f"Encountered errors while inserting into chat history table row: {e}"
            )
        finally:
            if cursor is not None:
                cursor.close()

    # ========================================================================================================

    def get_processes_list(self, bot_id="all"):
        cursor = self.client.cursor()
        try:
            if bot_id == "all":
                list_query = f"SELECT process_id, bot_id, process_name FROM {self.schema}.PROCESSES"
                cursor.execute(list_query)
            else:
                list_query = f"SELECT process_id, bot_id, process_name FROM {self.schema}.PROCESSES WHERE upper(bot_id) = upper(%s)"
                cursor.execute(list_query, (bot_id,))
            processs = cursor.fetchall()
            process_list = []
            for process in processs:
                process_dict = {
                    "process_id": process[0],
                    "bot_id": process[1],
                    "process_name": process[2],
#                    "process_details": process[4],
                  #  "process_instructions": process[3],
            #        "process_reporting_instructions": process[5],
                }
                process_list.append(process_dict)
            return {"Success": True, "processes": process_list}
        except Exception as e:
            return {
                "Success": False,
                "Error": f"Failed to list processs for bot {bot_id}: {e}",
            }
        finally:
            cursor.close()

    def get_process_info(self, bot_id, process_name):
        cursor = self.client.cursor()
        try:
            query = f"SELECT * FROM {self.schema}.PROCESSES WHERE bot_id like %s AND process_name LIKE %s"
            cursor.execute(query, (f"%{bot_id}%", f"%{process_name}%",))
            result = cursor.fetchone()
            if result:
                # Assuming the result is a tuple of values corresponding to the columns in the PROCESSES table
                # Convert the tuple to a dictionary with appropriate field names
                field_names = [desc[0] for desc in cursor.description]
                return dict(zip(field_names, result))
            else:
                return {}
        except Exception as e:
            return {}

    def OLDOLD__manage_processes(
        self, action, bot_id=None, process_id=None, process_details=None, thread_id=None, process_name=None
    ):
        """
        Manages processs in the PROCESSES table with actions to create, delete, or update a process.

        Args:
            action (str): The action to perform - 'CREATE', 'DELETE','UPDATE', 'LIST', 'SHOW'.
            bot_id (str): The bot ID associated with the process.
            process_id (str): The process ID for the process to manage.
            process_details (dict, optional): The details of the process for create or update actions.

        Returns:
            dict: A dictionary with the result of the operation.
        """

        # If process_name is specified but not in process_details, add it to process_details
        if process_name and process_details and 'process_name' not in process_details:
            process_details['process_name'] = process_name

        # If process_name is specified but not in process_details, add it to process_details
        if process_name and process_details==None:
            process_details = {}
            process_details['process_name'] = process_name

        required_fields_create = [
            "process_name",
      #      "process_details"_  
           "process_instructions",
       #     "process_reporting_instructions",
        ]

        required_fields_update = [
            "process_name",
 #           "process_details",
            "process_instructions",
     #       "process_reporting_instructions",
        ]

        if action == "TIME":
            return {
                "current_system_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S %Z")
            }
        action = action.upper()

        try:
            if action == "CREATE" or action == "UPDATE":
                # Send process_instructions to 2nd LLM to check it and format nicely
                tidy_process_instructions = f"""
                Below is a process that has been submitted by a user.  Please review it to insure it is something
                that will make sense to the run_process tool.  If not, make changes so it is organized into clear
                steps.  Make sure that it is tidy, legible and properly formatted. 
                Return the updated and tidy process.  If there is an issue with the process, return an error message.

                The process is as follows:\n {process_details['process_instructions']}
                """

                tidy_process_instructions = "\n".join(
                    line.lstrip() for line in tidy_process_instructions.splitlines()
                )

                # Check to see what LLM is currently available
                # os.environ["CORTEX_MODE"] = "False"
                # os.environ["CORTEX_AVAILABLE"] = 'False'
                # os.getenv("BOT_OS_DEFAULT_LLM_ENGINE") == 'openai | cortex'
                # os.getenv("CORTEX_FIREWORKS_OVERRIDE", "False").lower() 

                if os.getenv("BOT_OS_DEFAULT_LLM_ENGINE") == 'openai':
                    api_key = os.getenv("OPENAI_API_KEY")
                    if not api_key:
                        print("OpenAI API key is not set in the environment variables.")
                        return None

                    openai_api_key = os.getenv("OPENAI_API_KEY")
                    client = OpenAI(api_key=openai_api_key)
                    response = client.chat.completions.create(
                        model="gpt-4o",
                        messages=[
                            {
                                "role": "user",
                                "content": tidy_process_instructions,
                            },
                        ],
                    )

                    process_details['process_instructions'] = response.choices[0].message.content

                elif os.getenv("BOT_OS_DEFAULT_LLM_ENGINE") == 'cortex':
                    if not self.check_cortex_available():
                        print("Cortex is not available.")
                        return None
                    else:
                        response, status_code = self.cortex_chat_completion(tidy_process_instructions)
                        process_details['process_instructions'] = response


        except Exception as e:
            return {"Success": False, "Error": f"Error connecting to LLM: {e}"}

        if action == "CREATE":
            return {
                "Success": False,
                "Cleaned up instructions": process_details['process_instructions'],
                "Confirmation_Needed": "I've run the process instructions through a cleanup step.  Please reconfirm these instructions and all the other process details with the user, then call this function again with the action CREATE_CONFIRMED to actually create the process.  Remember that this function is used to create processes for existing bots, not to create bots themselves.",
            #    "Info": f"By the way the current system time is {datetime.now().strftime('%Y-%m-%d %H:%M:%S %Z')}",
            }

        if action == "CREATE":
            return {
                "Success": False,
                "Cleaned up instructions": process_details['process_instructions'],
                "Confirmation_Needed": "I've run the process instructions through a cleanup step.  Please reconfirm these instructions and all the other process details with the user, then call this function again with the action UPDATE_CONFIRMED to actually update the process.",
            #    "Info": f"By the way the current system time is {datetime.now().strftime('%Y-%m-%d %H:%M:%S %Z')}",
            }

        
        if action == "CREATE_CONFIRMED":
            action = "CREATE"
        if action == "UPDATE_CONFIRMED":
            action = "UPDATE"

        if action == "DELETE":
            return {
                "Success": False,
                "Confirmation_Needed": "Please reconfirm that you are deleting the correct process_ID, and double check with the user they want to delete this process, then call this function again with the action DELETE_CONFIRMED to actually delete the process.  Call with LIST to double-check the process_id if you aren't sure that its right.",
            }

        if action == "DELETE_CONFIRMED":
            action = "DELETE"

        if action not in ["CREATE", "DELETE", "UPDATE", "LIST", "SHOW"]:
            return {"Success": False, "Error": "Invalid action specified. Should be CREATE, DELETE, UPDATE, LIST, or SHOW."}

        cursor = self.client.cursor()

        if action == "LIST":
            print("Running get processes list")
            return self.get_processes_list(bot_id if bot_id is not None else "all")

        if action == "SHOW":
            print("Running show process info")
            if bot_id is None:
                return {"Success": False, "Error": "bot_id is required for SHOW action"}
            if process_details is None or 'process_name' not in process_details:
                return {"Success": False, "Error": "process_name is required in process_details for SHOW action"}
            process_name = process_details['process_name']
            return self.get_process_info(bot_id=bot_id, process_name=process_name)

        process_id_created = False
        if process_id is None:
            if action == "CREATE":
                process_id = f"{bot_id}_{''.join(random.choices(string.ascii_letters + string.digits, k=6))}"
                process_id_created = True
            else:
                return {"Success": False, "Error": f"Missing process_id field"}

        if action in ["CREATE", "UPDATE"] and not process_details:
            return {
                "Success": False,
                "Error": "Process details must be provided for CREATE or UPDATE action.",
            }

        if action in ["CREATE"] and any(
            field not in process_details for field in required_fields_create
        ):
            missing_fields = [
                field
                for field in required_fields_create
                if field not in process_details
            ]
            return {
                "Success": False,
                "Error": f"Missing required process details: {', '.join(missing_fields)}",
            }

        if action in ["UPDATE"] and any(
            field not in process_details for field in required_fields_update
        ):
            missing_fields = [
                field
                for field in required_fields_update
                if field not in process_details
            ]
            return {
                "Success": False,
                "Error": f"Missing required process details: {', '.join(missing_fields)}",
            }

    #    if action == "UPDATE" and process_details.get("process_active", False):
    #        if "next_check_ts" not in process_details:
    #            return {
    #                "Success": False,
    #                "Error": "The 'next_check_ts' field is required when updating an active process.",
    #            }

        # Convert timestamp from string in format 'YYYY-MM-DD HH:MM:SS' to a Snowflake-compatible timestamp
   #     if process_details is not None and process_details.get("process_active", False):
   #         try:
   #             formatted_next_check_ts = datetime.strptime(
   #                 process_details["next_check_ts"], "%Y-%m-%d %H:%M:%S"
   #             )
   #         except ValueError as ve:
   #             return {
   #                 "Success": False,
   #                 "Error": f"Invalid timestamp format for 'next_check_ts'. Required format: 'YYYY-MM-DD HH:MM:SS' in system timezone. Error details: {ve}",
   #                 "Info": f"Current system time in system timezone is {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}. The system timezone is {datetime.now().strftime('%Z')}. Please note that the timezone should not be included in the submitted timestamp.",
   #             }
   #         if formatted_next_check_ts < datetime.now():
   #             return {
   #                 "Success": False,
   #                 "Error": "The 'next_check_ts' is in the past.",
   #                 "Info": f"Current system time is {datetime.now().strftime('%Y-%m-%d %H:%M:%S %Z')}",
   #             }
        if bot_id is None:
            return {
                "Success": False,
                "Error": "The 'bot_id' field is required."
            }
    
        try:
            if action == "CREATE":
                insert_query = f"""
                    INSERT INTO {self.schema}.PROCESSES (
                        timestamp, process_id, bot_id, process_name, process_instructions
                    ) VALUES (
                        current_timestamp(), %(process_id)s, %(bot_id)s, %(process_name)s, %(process_instructions)s
                    )
                """

                # Generate 6 random alphanumeric characters
                if process_id_created == False:
                    random_suffix = "".join(
                    random.choices(string.ascii_letters + string.digits, k=6)
                     )
                    process_id_with_suffix = process_id + "_" + random_suffix
                else:
                    process_id_with_suffix = process_id
                cursor.execute(
                    insert_query,
                    {
                        **process_details,
                        "process_id": process_id_with_suffix,
                        "bot_id": bot_id,
                    },
                )
                self.client.commit()
                return {
                    "Success": True,
                    "Message": f"process successfully created.",
                    "process_id": process_id,
                    "Suggestion": "Now that the process is created, offer to test it using run_process, and if there are any issues you can later on UPDATE the process using manage_processes to clarify anything needed.  OFFER to test it, but don't just test it unless the user agrees.",
                    "Reminder": "If you are asked to test the process, use _run_process function to each step, don't skip ahead since you already know what the steps are, pretend you don't know what the process is and let run_process give you one step at a time!",
                }

            elif action == "DELETE":
                delete_query = f"""
                    DELETE FROM {self.schema}.PROCESSES
                    WHERE process_id = %s
                """
                cursor.execute(delete_query, (process_id))
                self.client.commit()

            elif action == "UPDATE":
                update_query = f"""
                    UPDATE {self.schema}.PROCESSES
                    SET {', '.join([f"{key} = %({key})s" for key in process_details.keys()])}
                    WHERE process_id = %(process_id)s
                """
                cursor.execute(
                    update_query,
                    {**process_details, "process_id": process_id},
                )
                self.client.commit()

            return {"Success": True, "Message": f"process update or delete confirmed."}
        except Exception as e:
            return {"Success": False, "Error": str(e)}

        finally:
            cursor.close()

    def insert_process_history(
        self,
        process_id,
        work_done_summary,
        process_status,
        updated_process_learnings,
        report_message="",
        done_flag=False,
        needs_help_flag="N",
        process_clarity_comments="",
    ):
        """
        Inserts a row into the PROCESS_HISTORY table.

        Args:
            process_id (str): The unique identifier for the process.
            work_done_summary (str): A summary of the work done.
            process_status (str): The status of the process.
            updated_process_learnings (str): Any new learnings from the process.
            report_message (str): The message to report about the process.
            done_flag (bool): Flag indicating if the process is done.
            needs_help_flag (bool): Flag indicating if help is needed.
            process_clarity_comments (str): Comments on the clarity of the process.
        """
        insert_query = f"""
            INSERT INTO {self.schema}.PROCESS_HISTORY (
                process_id, work_done_summary, process_status, updated_process_learnings, 
                report_message, done_flag, needs_help_flag, process_clarity_comments
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s
            )
        """
        try:
            cursor = self.client.cursor()
            cursor.execute(
                insert_query,
                (
                    process_id,
                    work_done_summary,
                    process_status,
                    updated_process_learnings,
                    report_message,
                    done_flag,
                    needs_help_flag,
                    process_clarity_comments,
                ),
            )
            self.client.commit()
            cursor.close()
            print(
                f"Process history row inserted successfully for process_id: {process_id}"
            )
        except Exception as e:
            print(f"An error occurred while inserting the process history row: {e}")
            if cursor is not None:
                cursor.close()

    # ========================================================================================================

    def process_scheduler(
        self, action, bot_id, task_id=None, task_details=None, thread_id=None
    ):
        import random
        import string

        """
        Manages tasks in the TASKS table with actions to create, delete, or update a task.

        Args:
            action (str): The action to perform - 'CREATE', 'DELETE', or 'UPDATE'.
            bot_id (str): The bot ID associated with the task.
            task_id (str): The task ID for the task to manage.
            task_details (dict, optional): The details of the task for create or update actions.

        Returns:
            dict: A dictionary with the result of the operation.
        """
        required_fields_create = [
            "task_name",
            "primary_report_to_type",
            "primary_report_to_id",
            "next_check_ts",
            "action_trigger_type",
            "action_trigger_details",
           # "task_instructions",
           # "reporting_instructions",
            "last_task_status",
            "task_learnings",
            "task_active",
        ]

        required_fields_update = ["last_task_status", "task_learnings", "task_active"]

        if action == "TIME":
            return {
                "current_system_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S %Z")
            }
        action = action.upper()

        if action == "CREATE":
            return {
                "Success": False,
                "Confirmation_Needed": "Please reconfirm all the scheduled process details with the user, then call this function again with the action CREATE_CONFIRMED to actually create the schedule for the process.   Make sure to be clear in the action_trigger_details field whether the process schedule is to be triggered one time, or if it is ongoing and recurring. Also make the next Next Check Timestamp is in the future, and aligns with when the user wants the task to run next.",
                "Info": f"By the way the current system time is {datetime.now().strftime('%Y-%m-%d %H:%M:%S %Z')}",
            }
        if action == "CREATE_CONFIRMED":
            action = "CREATE"

        if action == "UPDATE":

            return {
                "Success": False,
                "Confirmation_Needed": "Please reconfirm all the scheduled process details with the user, especially that you're altering the correct TASK_ID, then call this function again with the action UPDATE_CONFIRMED to actually update the scheduled process.  Call with LIST to double-check the task_id if you aren't sure.",
                "Info": f"By the way the current system time is {datetime.now().strftime('%Y-%m-%d %H:%M:%S %Z')}",
            }
        if action == "UPDATE_CONFIRMED":
            action = "UPDATE"

        if action == "DELETE":
            return {
                "Success": False,
                "Confirmation_Needed": "Please reconfirm that you are deleting the correct TASK_ID, and double check with the user they want to delete this schedule for the process, then call this function again with the action DELETE_CONFIRMED to actually delete the task.  Call with LIST to double-check the task_id if you aren't sure that its right.",
            }

        if action == "DELETE_CONFIRMED":
            action = "DELETE"

        if action not in ["CREATE", "DELETE", "UPDATE", "LIST"]:
            return {"Success": False, "Error": "Invalid action specified."}

        cursor = self.client.cursor()

        if action == "LIST":
            try:
                list_query = (
                    f"SELECT * FROM {self.schema}.TASKS WHERE upper(bot_id) = upper(%s)"
                )
                cursor.execute(list_query, (bot_id,))
                tasks = cursor.fetchall()
                task_list = []
                for task in tasks:
                    task_dict = {
                        "task_id": task[0],
                        "bot_id": task[1],
                        "task_name": task[2],
                        "primary_report_to_type": task[3],
                        "primary_report_to_id": task[4],
                        "next_check_ts": task[5].strftime("%Y-%m-%d %H:%M:%S"),
                        "action_trigger_type": task[6],
                        "action_trigger_details": task[7],
                        "task_instructions": task[8],
                        "reporting_instructions": task[9],
                        "last_task_status": task[10],
                        "task_learnings": task[11],
                        "task_active": task[12],
                    }
                    task_list.append(task_dict)
                return {"Success": True, "Tasks": task_list}
            except Exception as e:
                return {
                    "Success": False,
                    "Error": f"Failed to list tasks for bot {bot_id}: {e}",
                }

        if task_id is None:
            return {"Success": False, "Error": f"Missing task_id field"}

        if action in ["CREATE", "UPDATE"] and not task_details:
            return {
                "Success": False,
                "Error": "Task details must be provided for CREATE or UPDATE action.",
            }

        if action in ["CREATE"] and any(
            field not in task_details for field in required_fields_create
        ):
            missing_fields = [
                field for field in required_fields_create if field not in task_details
            ]
            return {
                "Success": False,
                "Error": f"Missing required task details: {', '.join(missing_fields)}",
            }

        if action in ["UPDATE"] and any(
            field not in task_details for field in required_fields_update
        ):
            missing_fields = [
                field for field in required_fields_update if field not in task_details
            ]
            return {
                "Success": False,
                "Error": f"Missing required task details: {', '.join(missing_fields)}",
            }

        if action == "UPDATE" and task_details.get("task_active", False):
            if "next_check_ts" not in task_details:
                return {
                    "Success": False,
                    "Error": "The 'next_check_ts' field is required when updating an active task.",
                }


        # Check if the action is CREATE or UPDATE
        if action in ["CREATE", "UPDATE"] and task_details and "task_name" in task_details:
            # Check if the task_name is a valid process for the bot
            valid_processes = self.manage_processes(action="LIST", bot_id=bot_id)
            if not valid_processes["Success"]:
                return {
                    "Success": False,
                    "Error": f"Failed to retrieve processes for bot {bot_id}: {valid_processes['Error']}",
                }

            if task_details["task_name"] not in [
                process["process_name"] for process in valid_processes["processes"]
            ]:
                return {
                    "Success": False,
                    "Error": f"Invalid task_name: {task_details.get('task_name')}. It must be one of the valid processes for this bot",
                    "Valid_Processes": [process["process_name"] for process in valid_processes["processes"]],
                }


        # Convert timestamp from string in format 'YYYY-MM-DD HH:MM:SS' to a Snowflake-compatible timestamp
        if task_details is not None and task_details.get("task_active", False):
            try:
                formatted_next_check_ts = datetime.strptime(
                    task_details["next_check_ts"], "%Y-%m-%d %H:%M:%S"
                )
            except ValueError as ve:
                return {
                    "Success": False,
                    "Error": f"Invalid timestamp format for 'next_check_ts'. Required format: 'YYYY-MM-DD HH:MM:SS' in system timezone. Error details: {ve}",
                    "Info": f"Current system time in system timezone is {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}. The system timezone is {datetime.now().strftime('%Z')}. Please note that the timezone should not be included in the submitted timestamp.",
                }
            if formatted_next_check_ts < datetime.now():
                return {
                    "Success": False,
                    "Error": "The 'next_check_ts' is in the past.",
                    "Info": f"Current system time is {datetime.now().strftime('%Y-%m-%d %H:%M:%S %Z')}",
                }

        try:
            if action == "CREATE":
                insert_query = f"""
                    INSERT INTO {self.schema}.TASKS (
                        task_id, bot_id, task_name, primary_report_to_type, primary_report_to_id,
                        next_check_ts, action_trigger_type, action_trigger_details, task_instructions,
                        reporting_instructions, last_task_status, task_learnings, task_active
                    ) VALUES (
                        %(task_id)s, %(bot_id)s, %(task_name)s, %(primary_report_to_type)s, %(primary_report_to_id)s,
                        %(next_check_ts)s, %(action_trigger_type)s, %(action_trigger_details)s, null,
                        null, %(last_task_status)s, %(task_learnings)s, %(task_active)s
                    )
                """

                # Generate 6 random alphanumeric characters
                random_suffix = "".join(
                    random.choices(string.ascii_letters + string.digits, k=6)
                )
                task_id_with_suffix = task_id + "_" + random_suffix
                cursor.execute(
                    insert_query,
                    {**task_details, "task_id": task_id_with_suffix, "bot_id": bot_id},
                )
                self.client.commit()
                return {
                    "Success": True,
                    "Message": f"Task successfully created, next check scheduled for {task_details['next_check_ts']}",
                }

            elif action == "DELETE":
                delete_query = f"""
                    DELETE FROM {self.schema}.TASKS
                    WHERE task_id = %s AND bot_id = %s
                """
                cursor.execute(delete_query, (task_id, bot_id))
                self.client.commit()

            elif action == "UPDATE":
                update_query = f"""
                    UPDATE {self.schema}.TASKS
                    SET {', '.join([f"{key} = %({key})s" for key in task_details.keys()])}
                    WHERE task_id = %(task_id)s AND bot_id = %(bot_id)s
                """
                cursor.execute(
                    update_query, {**task_details, "task_id": task_id, "bot_id": bot_id}
                )
                self.client.commit()

            return {"Success": True, "Message": f"Task update or delete confirmed."}
        except Exception as e:
            return {"Success": False, "Error": str(e)}

        finally:
            cursor.close()

    def insert_task_history(
        self,
        task_id,
        work_done_summary,
        task_status,
        updated_task_learnings,
        report_message="",
        done_flag=False,
        needs_help_flag="N",
        task_clarity_comments="",
    ):
        """
        Inserts a row into the TASK_HISTORY table.

        Args:
            task_id (str): The unique identifier for the task.
            work_done_summary (str): A summary of the work done.
            task_status (str): The status of the task.
            updated_task_learnings (str): Any new learnings from the task.
            report_message (str): The message to report about the task.
            done_flag (bool): Flag indicating if the task is done.
            needs_help_flag (bool): Flag indicating if help is needed.
            task_clarity_comments (str): Comments on the clarity of the task.
        """
        insert_query = f"""
            INSERT INTO {self.schema}.TASK_HISTORY (
                task_id, work_done_summary, task_status, updated_task_learnings, 
                report_message, done_flag, needs_help_flag, task_clarity_comments
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s
            )
        """
        try:
            cursor = self.client.cursor()
            cursor.execute(
                insert_query,
                (
                    task_id,
                    work_done_summary,
                    task_status,
                    updated_task_learnings,
                    report_message,
                    done_flag,
                    needs_help_flag,
                    task_clarity_comments,
                ),
            )
            self.client.commit()
            cursor.close()
            print(f"Task history row inserted successfully for task_id: {task_id}")
        except Exception as e:
            print(f"An error occurred while inserting the task history row: {e}")
            if cursor is not None:
                cursor.close()

    def db_insert_llm_results(self, uu, message):
        """
        Inserts a row into the LLM_RESULTS table.

        Args:
            uu (str): The unique identifier for the result.
            message (str): The message to store.
        """
        insert_query = f"""
            INSERT INTO {self.schema}.LLM_RESULTS (uu, message, created)
            VALUES (%s, %s, CURRENT_TIMESTAMP)
        """
        try:
            cursor = self.client.cursor()
            cursor.execute(insert_query, (uu, message))
            self.client.commit()
            cursor.close()
            print(f"LLM result row inserted successfully for uu: {uu}")
        except Exception as e:
            print(f"An error occurred while inserting the LLM result row: {e}")
            if cursor is not None:
                cursor.close()

    def db_update_llm_results(self, uu, message):
        """
        Inserts a row into the LLM_RESULTS table.

        Args:
            uu (str): The unique identifier for the result.
            message (str): The message to store.
        """
        update_query = f"""
            UPDATE {self.schema}.LLM_RESULTS
            SET message = %s
            WHERE uu = %s
        """
        try:
            cursor = self.client.cursor()
            cursor.execute(update_query, (message, uu))
            self.client.commit()
            cursor.close()
        #     print(f"LLM result row inserted successfully for uu: {uu}")
        except Exception as e:
            print(f"An error occurred while inserting the LLM result row: {e}")
            if cursor is not None:
                cursor.close()

    def db_get_llm_results(self, uu):
        """
        Retrieves a row from the LLM_RESULTS table using the uu.

        Args:
            uu (str): The unique identifier for the result.
        """
        select_query = f"""
            SELECT message
            FROM {self.schema}.LLM_RESULTS
            WHERE uu = %s
        """
        try:
            cursor = self.client.cursor()
            cursor.execute(select_query, (uu,))
            result = cursor.fetchone()
            cursor.close()
            if result is not None:
                return result[0]
            else:
                print(f"No LLM result found for uu: {uu}")
        except Exception as e:
            print(f"An error occurred while retrieving the LLM result: {e}")
            if cursor is not None:
                cursor.close()

    def db_clean_llm_results(self):
        """
        Removes rows from the LLM_RESULTS table that are over 10 minutes old.
        """
        delete_query = f"""
            DELETE FROM {self.schema}.LLM_RESULTS
            WHERE CURRENT_TIMESTAMP - created > INTERVAL '10 MINUTES'
        """
        try:
            cursor = self.client.cursor()
            cursor.execute(delete_query)
            self.client.commit()
            cursor.close()
            print(
                "LLM result rows older than 10 minutes have been successfully deleted."
            )
        except Exception as e:
            print(f"An error occurred while deleting old LLM result rows: {e}")
            if cursor is not None:
                cursor.close()

    def ensure_table_exists(self):
        import core.bot_os_tool_descriptions

        streamlitdc_url = os.getenv("DATA_CUBES_INGRESS_URL", None)
        print(f"streamlit data cubes ingress URL: {streamlitdc_url}")

        llm_results_table_check_query = (
            f"SHOW TABLES LIKE 'LLM_RESULTS' IN SCHEMA {self.schema};"
        )
        try:
            cursor = self.client.cursor()
            cursor.execute(llm_results_table_check_query)
            if not cursor.fetchone():
                create_llm_results_table_ddl = f"""
                CREATE OR REPLACE HYBRID TABLE {self.schema}.LLM_RESULTS (
                    uu VARCHAR(40) PRIMARY KEY,
                    message VARCHAR NOT NULL,
                    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    INDEX uu_idx (uu)
                );
                """
                cursor.execute(create_llm_results_table_ddl)
                self.client.commit()
                print(f"Table {self.schema}.LLM_RESULTS created successfully.")
            else:
                print(f"Table {self.schema}.LLM_RESULTS already exists.")
        except Exception as e:
            print(
                f"An error occurred while checking or creating the LLM_RESULTS table: {e}"
            )
        finally:
            if cursor is not None:
                cursor.close()

        tasks_table_check_query = f"SHOW TABLES LIKE 'TASKS' IN SCHEMA {self.schema};"
        try:
            cursor = self.client.cursor()
            cursor.execute(tasks_table_check_query)
            if not cursor.fetchone():
                create_tasks_table_ddl = f"""
                CREATE TABLE {self.schema}.TASKS (
                    task_id VARCHAR(255),
                    bot_id VARCHAR(255),
                    task_name VARCHAR(255),
                    primary_report_to_type VARCHAR(50),
                    primary_report_to_id VARCHAR(255),
                    next_check_ts TIMESTAMP,
                    action_trigger_type VARCHAR(50),
                    action_trigger_details VARCHAR(1000),
                    task_instructions TEXT,
                    reporting_instructions TEXT,
                    last_task_status VARCHAR(255),
                    task_learnings TEXT,
                    task_active BOOLEAN
                );
                """
                cursor.execute(create_tasks_table_ddl)
                self.client.commit()
                print(f"Table {self.schema}.TASKS created successfully.")
            else:
                print(f"Table {self.schema}.TASKS already exists.")
        except Exception as e:
            print(f"An error occurred while checking or creating the TASKS table: {e}")
        finally:
            if cursor is not None:
                cursor.close()

        task_history_check_query = (
            f"SHOW TABLES LIKE 'TASK_HISTORY' IN SCHEMA {self.schema};"
        )
        try:
            cursor = self.client.cursor()
            cursor.execute(task_history_check_query)
            if not cursor.fetchone():
                create_task_history_table_ddl = f"""
                CREATE TABLE {self.schema}.TASK_HISTORY (
                    task_id VARCHAR(255),
                    run_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
                    work_done_summary TEXT,
                    task_status TEXT,
                    updated_task_learnings TEXT,
                    report_message TEXT,
                    done_flag BOOLEAN,
                    needs_help_flag BOOLEAN,
                    task_clarity_comments TEXT
                );
                """
                cursor.execute(create_task_history_table_ddl)
                self.client.commit()
                print(f"Table {self.schema}.TASK_HISTORY created successfully.")
            else:
                print(f"Table {self.schema}.TASK_HISTORY already exists.")
        except Exception as e:
            print(
                f"An error occurred while checking or creating the TASK_HISTORY table: {e}"
            )
        finally:
            if cursor is not None:
                cursor.close()

        semantic_stage_check_query = (
            f"SHOW STAGES LIKE 'SEMANTIC_MODELS_DEV' IN SCHEMA {self.schema};"
        )
        try:
            cursor = self.client.cursor()
            cursor.execute(semantic_stage_check_query)
            if not cursor.fetchone():
                semantic_stage_ddl = f"""
                CREATE STAGE {self.schema}.SEMANTIC_MODELS_DEV
                ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');
                """
                cursor.execute(semantic_stage_ddl)
                self.client.commit()
                print(f"Stage {self.schema}.SEMANTIC_MODELS_DEV created.")
            else:
                print(f"Stage {self.schema}.SEMANTIC_MODELS_DEV already exists.")
        except Exception as e:
            print(
                f"An error occurred while checking or creating stage SEMANTIC_MODELS_DEV: {e}"
            )
        finally:
            if cursor is not None:
                cursor.close()

        semantic_stage_check_query = (
            f"SHOW STAGES LIKE 'SEMANTIC_MODELS' IN SCHEMA {self.schema};"
        )
        try:
            cursor = self.client.cursor()
            cursor.execute(semantic_stage_check_query)
            if not cursor.fetchone():
                semantic_stage_ddl = f"""
                CREATE STAGE {self.schema}.SEMANTIC_MODELS
                ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');
                """
                cursor.execute(semantic_stage_ddl)
                self.client.commit()
                print(f"Stage {self.schema}.SEMANTIC_MODELS created.")
            else:
                print(f"Stage {self.schema}.SEMANTIC_MODELS already exists.")
        except Exception as e:
            print(
                f"An error occurred while checking or creating stage SEMANTIC_MODELS: {e}"
            )
        finally:
            if cursor is not None:
                cursor.close()

        udf_check_query = (
            f"SHOW USER FUNCTIONS LIKE 'SET_BOT_APP_LEVEL_KEY' IN SCHEMA {self.schema};"
        )
        try:
            cursor = self.client.cursor()
            cursor.execute(udf_check_query)
            if not cursor.fetchone():
                udf_creation_ddl = f"""
                CREATE OR REPLACE FUNCTION {self.schema}.set_bot_app_level_key (bot_id VARCHAR, slack_app_level_key VARCHAR)
                RETURNS VARCHAR
                SERVICE={self.schema}.GENESISAPP_SERVICE_SERVICE
                ENDPOINT=udfendpoint AS '/udf_proxy/set_bot_app_level_key';
                """
                cursor.execute(udf_creation_ddl)
                self.client.commit()
                print(f"UDF set_bot_app_level_key created in schema {self.schema}.")
            else:
                print(
                    f"UDF set_bot_app_level_key already exists in schema {self.schema}."
                )
        except Exception as e:
            print(
                f"UDF not created in {self.schema} {e}.  This is expected in Local mode."
            )

        bot_files_stage_check_query = f"SHOW STAGES LIKE 'BOT_FILES_STAGE' IN SCHEMA {self.genbot_internal_project_and_schema};"
        try:
            cursor = self.client.cursor()
            cursor.execute(bot_files_stage_check_query)
            if not cursor.fetchone():
                bot_files_stage_ddl = f"""
                CREATE OR REPLACE STAGE {self.genbot_internal_project_and_schema}.BOT_FILES_STAGE
                ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');
                """
                cursor.execute(bot_files_stage_ddl)
                self.client.commit()
                print(
                    f"Stage {self.genbot_internal_project_and_schema}.BOT_FILES_STAGE created."
                )
            else:
                print(
                    f"Stage {self.genbot_internal_project_and_schema}.BOT_FILES_STAGE already exists."
                )
        except Exception as e:
            print(
                f"An error occurred while checking or creating stage BOT_FILES_STAGE: {e}"
            )
        finally:
            if cursor is not None:
                cursor.close()

        llm_config_table_check_query = (
            f"SHOW TABLES LIKE 'LLM_TOKENS' IN SCHEMA {self.schema};"
        )
        try:
            cursor = self.client.cursor()
            cursor.execute(llm_config_table_check_query)
            if not cursor.fetchone():
                llm_config_table_ddl = f"""
                CREATE OR REPLACE TABLE {self.genbot_internal_project_and_schema}.LLM_TOKENS (
                    RUNNER_ID VARCHAR(16777216),
                    LLM_KEY VARCHAR(16777216),
                    LLM_TYPE VARCHAR(16777216),
                    ACTIVE BOOLEAN
                );
                """
                cursor.execute(llm_config_table_ddl)
                self.client.commit()
                #      print(f"Table {self.genbot_internal_project_and_schema}.LLM_TOKENS created.")

                # Insert a row with the current runner_id and cortex as the active LLM key and type
                runner_id = os.getenv("RUNNER_ID", "jl-local-runner")
                insert_initial_row_query = f"""
                INSERT INTO {self.genbot_internal_project_and_schema}.LLM_TOKENS (RUNNER_ID, LLM_KEY, LLM_TYPE, ACTIVE)
                VALUES (%s, %s, %s, %s);
                """
                # if a new install, set cortex to default LLM if available
                test_cortex_available = self.check_cortex_available()
                if test_cortex_available == True:
                    cursor.execute(insert_initial_row_query, (runner_id,'cortex_no_key_needed', 'cortex', True,))
                else:
                    cursor.execute(insert_initial_row_query, (runner_id,None,None,False,))
                self.client.commit()
            #       print(f"Inserted initial row into {self.genbot_internal_project_and_schema}.LLM_TOKENS with runner_id: {runner_id}")
            else:
                check_query = f"DESCRIBE TABLE {self.genbot_internal_project_and_schema}.LLM_TOKENS;"
                try:
                    cursor.execute(check_query)
                    columns = [col[0] for col in cursor.fetchall()]
                    
                    if "ACTIVE" not in columns:
                        alter_table_query = f"ALTER TABLE {self.genbot_internal_project_and_schema}.LLM_TOKENS ADD COLUMN ACTIVE BOOLEAN;"
                        cursor.execute(alter_table_query)
                        self.client.commit()
                        logger.info(
                            f"Column 'ACTIVE' added to table {self.genbot_internal_project_and_schema}.LLM_TOKENS."
                        )
                        update_query = f"UPDATE {self.genbot_internal_project_and_schema}.LLM_TOKENS SET ACTIVE=TRUE WHERE LLM_TYPE='OpenAI'"
                        cursor.execute(update_query)
                        self.client.commit()
                except Exception as e:
                    print(
                        f"An error occurred while checking or altering table {self.genbot_internal_project_and_schema}.LLM_TOKENS to add ACTIVE column: {e}"
                    )
                #               print(f"Table {self.schema}.LLM_TOKENS already exists.")
        except Exception as e:
            print(f"An error occurred while checking or creating table LLM_TOKENS: {e}")
        finally:
            if cursor is not None:
                cursor.close()

        slack_tokens_table_check_query = (
            f"SHOW TABLES LIKE 'SLACK_APP_CONFIG_TOKENS' IN SCHEMA {self.schema};"
        )
        try:
            cursor = self.client.cursor()
            cursor.execute(slack_tokens_table_check_query)
            if not cursor.fetchone():
                slack_tokens_table_ddl = f"""
                CREATE OR REPLACE TABLE {self.slack_tokens_table_name} (
                    RUNNER_ID VARCHAR(16777216),
                    SLACK_APP_CONFIG_TOKEN VARCHAR(16777216),
                    SLACK_APP_CONFIG_REFRESH_TOKEN VARCHAR(16777216)
                );
                """
                cursor.execute(slack_tokens_table_ddl)
                self.client.commit()
                print(f"Table {self.slack_tokens_table_name} created.")

                # Insert a row with the current runner_id and NULL values for the tokens
                runner_id = os.getenv("RUNNER_ID", "jl-local-runner")
                insert_initial_row_query = f"""
                INSERT INTO {self.slack_tokens_table_name} (RUNNER_ID, SLACK_APP_CONFIG_TOKEN, SLACK_APP_CONFIG_REFRESH_TOKEN)
                VALUES (%s, NULL, NULL);
                """
                cursor.execute(insert_initial_row_query, (runner_id,))
                self.client.commit()
                print(
                    f"Inserted initial row into {self.slack_tokens_table_name} with runner_id: {runner_id}"
                )
            else:
                print(
                    f"Table {self.slack_tokens_table_name} already exists."
                )  # SLACK_APP_CONFIG_TOKENS
        except Exception as e:
            print(
                f"An error occurred while checking or creating table {self.slack_tokens_table_name}: {e}"
            )
        finally:
            if cursor is not None:
                cursor.close()

        bot_servicing_table_check_query = (
            f"SHOW TABLES LIKE 'BOT_SERVICING' IN SCHEMA {self.schema};"
        )
        try:
            cursor = self.client.cursor()
            cursor.execute(bot_servicing_table_check_query)
            if not cursor.fetchone():
                bot_servicing_table_ddl = f"""
                CREATE OR REPLACE TABLE {self.bot_servicing_table_name} (
                    API_APP_ID VARCHAR(16777216),
                    BOT_SLACK_USER_ID VARCHAR(16777216),
                    BOT_ID VARCHAR(16777216),
                    BOT_NAME VARCHAR(16777216),
                    BOT_INSTRUCTIONS VARCHAR(16777216),
                    AVAILABLE_TOOLS VARCHAR(16777216),
                    RUNNER_ID VARCHAR(16777216),
                    SLACK_APP_TOKEN VARCHAR(16777216),
                    SLACK_APP_LEVEL_KEY VARCHAR(16777216),
                    SLACK_SIGNING_SECRET VARCHAR(16777216),
                    SLACK_CHANNEL_ID VARCHAR(16777216),
                    AUTH_URL VARCHAR(16777216),
                    AUTH_STATE VARCHAR(16777216),
                    CLIENT_ID VARCHAR(16777216),
                    CLIENT_SECRET VARCHAR(16777216),
                    UDF_ACTIVE VARCHAR(16777216),
                    SLACK_ACTIVE VARCHAR(16777216),
                    FILES VARCHAR(16777216),
                    BOT_IMPLEMENTATION VARCHAR(16777216),
                    BOT_INTRO_PROMPT VARCHAR(16777216),
                    BOT_AVATAR_IMAGE VARCHAR(16777216),
                    SLACK_USER_ALLOW  ARRAY,
                    DATABASE_CREDENTIALS VARIANT
                );
                """
                cursor.execute(bot_servicing_table_ddl)
                self.client.commit()
                print(f"Table {self.bot_servicing_table_name} created.")

                # Insert a row with specified values and NULL for the rest
                runner_id = os.getenv("RUNNER_ID", "jl-local-runner")
                bot_id = "Eve-"
                bot_id += "".join(
                    random.choices(string.ascii_letters + string.digits, k=6)
                )
                bot_name = "Eve"
                bot_instructions = BASE_EVE_BOT_INSTRUCTIONS
                available_tools = '["slack_tools", "make_baby_bot", "snowflake_stage_tools", "image_tools"]'
                udf_active = "Y"
                slack_active = "N"
                bot_intro_prompt = EVE_INTRO_PROMPT

                insert_initial_row_query = f"""
                INSERT INTO {self.bot_servicing_table_name} (
                    RUNNER_ID, BOT_ID, BOT_NAME, BOT_INSTRUCTIONS, AVAILABLE_TOOLS, UDF_ACTIVE, SLACK_ACTIVE, BOT_INTRO_PROMPT
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
                """
                cursor.execute(
                    insert_initial_row_query,
                    (
                        runner_id,
                        bot_id,
                        bot_name,
                        bot_instructions,
                        available_tools,
                        udf_active,
                        slack_active,
                        bot_intro_prompt,
                    ),
                )
                self.client.commit()
                print(
                    f"Inserted initial Eve row into {self.bot_servicing_table_name} with runner_id: {runner_id}"
                )

                runner_id = os.getenv("RUNNER_ID", "jl-local-runner")
                bot_id = "Eliza-"
                bot_id += "".join(
                    random.choices(string.ascii_letters + string.digits, k=6)
                )
                bot_name = "Eliza"
                bot_instructions = ELIZA_DATA_ANALYST_INSTRUCTIONS
                available_tools = '["slack_tools", "database_tools", "snowflake_stage_tools",  "image_tools", "process_manager_tools", "process_runner_tools", "process_scheduler_tools"]'
                udf_active = "Y"
                slack_active = "N"
                bot_intro_prompt = ELIZA_INTRO_PROMPT

                insert_initial_row_query = f"""
                INSERT INTO {self.bot_servicing_table_name} (
                    RUNNER_ID, BOT_ID, BOT_NAME, BOT_INSTRUCTIONS, AVAILABLE_TOOLS, UDF_ACTIVE, SLACK_ACTIVE, BOT_INTRO_PROMPT
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
                """
                cursor.execute(
                    insert_initial_row_query,
                    (
                        runner_id,
                        bot_id,
                        bot_name,
                        bot_instructions,
                        available_tools,
                        udf_active,
                        slack_active,
                        bot_intro_prompt,
                    ),
                )
                self.client.commit()
                print(
                    f"Inserted initial Eliza row into {self.bot_servicing_table_name} with runner_id: {runner_id}"
                )

                runner_id = os.getenv("RUNNER_ID", "jl-local-runner")
                bot_id = "Janice-"
                bot_id += "".join(
                    random.choices(string.ascii_letters + string.digits, k=6)
                )
                bot_name = "Janice"
                bot_instructions = JANICE_JANITOR_INSTRUCTIONS
                available_tools = '["slack_tools", "database_tools", "snowflake_stage_tools", "image_tools", "process_manager_tools", "process_runner_tools", "process_scheduler_tools"]'
                udf_active = "Y"
                slack_active = "N"
                bot_intro_prompt = JANICE_INTRO_PROMPT

                insert_initial_row_query = f"""
                INSERT INTO {self.bot_servicing_table_name} (
                    RUNNER_ID, BOT_ID, BOT_NAME, BOT_INSTRUCTIONS, AVAILABLE_TOOLS, UDF_ACTIVE, SLACK_ACTIVE, BOT_INTRO_PROMPT
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
                """
                cursor.execute(
                    insert_initial_row_query,
                    (
                        runner_id,
                        bot_id,
                        bot_name,
                        bot_instructions,
                        available_tools,
                        udf_active,
                        slack_active,
                        bot_intro_prompt,
                    ),
                )
                self.client.commit()
                print(
                    f"Inserted initial Janice row into {self.bot_servicing_table_name} with runner_id: {runner_id}"
                )
                # add files to stage from local dir for Janice
                database, schema = self.genbot_internal_project_and_schema.split('.')
                result = self.add_file_to_stage(
                    database=database,
                    schema=schema,
                    stage="BOT_FILES_STAGE",
                    file_name="./default_files/janice/*",
                )
                print(result)

            #          runner_id = os.getenv('RUNNER_ID', 'jl-local-runner')
            #          bot_id = 'Stuart-'
            #          bot_id += ''.join(random.choices(string.ascii_letters + string.digits, k=6))
            #          bot_name = "Stuart"
            #          bot_instructions = STUART_DATA_STEWARD_INSTRUCTIONS
            #          available_tools = '["slack_tools", "database_tools", "snowflake_stage_tools", "snowflake_semantic_tools", "image_tools", "autonomous_tools"]'
            #          udf_active = "Y"
            #          slack_active = "N"
            #          bot_intro_prompt = STUART_INTRO_PROMPT

            #          insert_initial_row_query = f"""
            #         INSERT INTO {self.bot_servicing_table_name} (
            #              RUNNER_ID, BOT_ID, BOT_NAME, BOT_INSTRUCTIONS, AVAILABLE_TOOLS, UDF_ACTIVE, SLACK_ACTIVE, BOT_INTRO_PROMPT
            #          )
            #          VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
            #          """
            #          cursor.execute(insert_initial_row_query, (runner_id, bot_id, bot_name, bot_instructions, available_tools, udf_active, slack_active, bot_intro_prompt))
            #          self.client.commit()
            #          print(f"Inserted initial Stuart row into {self.bot_servicing_table_name} with runner_id: {runner_id}")

            else:
                # Check if the 'ddl_short' column exists in the metadata table

                update_query = f"""
                UPDATE {self.bot_servicing_table_name}
                SET AVAILABLE_TOOLS = REPLACE(REPLACE(AVAILABLE_TOOLS, 'vision_chat_analysis', 'image_tools'),'autonomous_functions','autonomous_tools')
                WHERE AVAILABLE_TOOLS LIKE '%vision_chat_analysis%' or AVAILABLE_TOOLS LIKE '%autonomous_functions%'
                """
                cursor.execute(update_query)
                self.client.commit()
                print(
                    f"Updated 'vision_chat_analysis' to 'image_analysis' in AVAILABLE_TOOLS where applicable in {self.bot_servicing_table_name}."
                )

                check_query = f"DESCRIBE TABLE {self.bot_servicing_table_name};"
                try:
                    cursor.execute(check_query)
                    columns = [col[0] for col in cursor.fetchall()]
                    if "SLACK_APP_LEVEL_KEY" not in columns:
                        alter_table_query = f"ALTER TABLE {self.bot_servicing_table_name} ADD COLUMN SLACK_APP_LEVEL_KEY STRING;"
                        cursor.execute(alter_table_query)
                        self.client.commit()
                        logger.info(
                            f"Column 'SLACK_APP_LEVEL_KEY' added to table {self.bot_servicing_table_name}."
                        )
                    if "BOT_IMPLEMENTATION" not in columns:
                        alter_table_query = f"ALTER TABLE {self.bot_servicing_table_name} ADD COLUMN BOT_IMPLEMENTATION STRING;"
                        cursor.execute(alter_table_query)
                        self.client.commit()
                        logger.info(
                            f"Column 'BOT_IMPLEMENTATION' added to table {self.bot_servicing_table_name}."
                        )
                    if "BOT_INTRO" in columns:
                        alter_table_query = f"ALTER TABLE {self.bot_servicing_table_name} DROP COLUMN BOT_INTRO;"
                        cursor.execute(alter_table_query)
                        self.client.commit()
                        logger.info(
                            f"Column 'BOT_INTRO' dropped from table {self.bot_servicing_table_name}."
                        )
                    if "BOT_INTRO_PROMPT" not in columns:
                        alter_table_query = f"ALTER TABLE {self.bot_servicing_table_name} ADD COLUMN BOT_INTRO_PROMPT STRING;"
                        cursor.execute(alter_table_query)
                        self.client.commit()
                        logger.info(
                            f"Column 'BOT_INTRO_PROMPT' added to table {self.bot_servicing_table_name}."
                        )
                        insert_initial_intros_query = f"""UPDATE {self.bot_servicing_table_name} b SET BOT_INTRO_PROMPT = a.BOT_INTRO_PROMPT
                        FROM (
                            SELECT BOT_NAME, BOT_INTRO_PROMPT
                            FROM (
                                SELECT 'EVE' BOT_NAME, $${EVE_INTRO_PROMPT}$$ BOT_INTRO_PROMPT
                                UNION
                                SELECT 'ELIZA' BOT_NAME, $${ELIZA_INTRO_PROMPT}$$ BOT_INTRO_PROMPT
                                UNION
                                SELECT 'JANICE' BOT_NAME, $${JANICE_INTRO_PROMPT}$$ BOT_INTRO_PROMPT
                                UNION
                                SELECT 'STUART' BOT_NAME, $${STUART_INTRO_PROMPT}$$ BOT_INTRO_PROMPT                                
                            ) ) a 
                        WHERE upper(a.BOT_NAME) = upper(b.BOT_NAME)"""
                        cursor.execute(insert_initial_intros_query)
                        self.client.commit()
                        logger.info(
                            f"Initial 'BOT_INTRO_PROMPT' data inserted into table {self.bot_servicing_table_name}."
                        )
                    if "BOT_AVATAR_IMAGE" not in columns:
                        alter_table_query = f"ALTER TABLE {self.bot_servicing_table_name} ADD COLUMN BOT_AVATAR_IMAGE VARCHAR(16777216);"
                        cursor.execute(alter_table_query)
                        self.client.commit()
                        logger.info(
                            f"Column 'BOT_AVATAR_IMAGE' added to table {self.bot_servicing_table_name}."
                        )
                    if "SLACK_USER_ALLOW" not in columns:
                        alter_table_query = f"ALTER TABLE {self.bot_servicing_table_name} ADD COLUMN SLACK_USER_ALLOW ARRAY;"
                        cursor.execute(alter_table_query)
                        self.client.commit()
                        logger.info(
                            f"Column 'SLACK_USER_ALLOW' added to table {self.bot_servicing_table_name}."
                        )
                    if "DATABASE_CREDENTIALS" not in columns:
                        alter_table_query = f"ALTER TABLE {self.bot_servicing_table_name} ADD COLUMN DATABASE_CREDENTIALS VARIANT;"
                        cursor.execute(alter_table_query)
                        self.client.commit()
                        logger.info(
                            f"Column 'DATABASE_CREDENTIALS' added to table {self.bot_servicing_table_name}."
                        )

                except Exception as e:
                    print(
                        f"An error occurred while checking or altering table {self.bot_servicing_table_name} to add BOT_IMPLEMENTATION column: {e}"
                    )
                except Exception as e:
                    print(
                        f"An error occurred while checking or altering table {metadata_table_id}: {e}"
                    )
                print(f"Table {self.bot_servicing_table_name} already exists.")
            # update bot servicing table bot avatars from shared images table
            insert_images_query = f"""UPDATE {self.bot_servicing_table_name} b SET BOT_AVATAR_IMAGE = a.ENCODED_IMAGE_DATA
            FROM (
                SELECT BOT_NAME, ENCODED_IMAGE_DATA FROM (
                    SELECT S.ENCODED_IMAGE_DATA, R.BOT_NAME
                    FROM {self.images_table_name} S, {self.bot_servicing_table_name} R
                    WHERE UPPER(S.BOT_NAME) = UPPER(R.BOT_NAME)
                    UNION
                    SELECT P.ENCODED_IMAGE_DATA, Q.BOT_NAME
                    FROM {self.images_table_name} P, {self.bot_servicing_table_name} Q
                    WHERE UPPER(P.BOT_NAME) = 'DEFAULT' AND
                        Q.BOT_NAME NOT IN (SELECT BOT_NAME FROM {self.images_table_name})
                    )
                ) a 
            WHERE upper(a.BOT_NAME) = upper(b.BOT_NAME)"""
            cursor.execute(insert_images_query)
            self.client.commit()
            logger.info(
                f"Initial 'BOT_AVATAR_IMAGE' data inserted into table {self.bot_servicing_table_name}."
            )
        except Exception as e:
            print(
                f"An error occurred while checking or creating table {self.bot_servicing_table_name}: {e}"
            )
        finally:
            if cursor is not None:
                cursor.close()

        ngrok_tokens_table_check_query = (
            f"SHOW TABLES LIKE 'NGROK_TOKENS' IN SCHEMA {self.schema};"
        )
        try:
            cursor = self.client.cursor()
            cursor.execute(ngrok_tokens_table_check_query)
            if not cursor.fetchone():
                ngrok_tokens_table_ddl = f"""
                CREATE OR REPLACE TABLE {self.ngrok_tokens_table_name} (
                    RUNNER_ID VARCHAR(16777216),
                    NGROK_AUTH_TOKEN VARCHAR(16777216),
                    NGROK_USE_DOMAIN VARCHAR(16777216),
                    NGROK_DOMAIN VARCHAR(16777216)
                );
                """
                cursor.execute(ngrok_tokens_table_ddl)
                self.client.commit()
                print(f"Table {self.ngrok_tokens_table_name} created.")

                # Insert a row with the current runner_id and NULL values for the tokens and domain, 'N' for use_domain
                runner_id = os.getenv("RUNNER_ID", "jl-local-runner")
                insert_initial_row_query = f"""
                INSERT INTO {self.ngrok_tokens_table_name} (RUNNER_ID, NGROK_AUTH_TOKEN, NGROK_USE_DOMAIN, NGROK_DOMAIN)
                VALUES (%s, NULL, 'N', NULL);
                """
                cursor.execute(insert_initial_row_query, (runner_id,))
                self.client.commit()
                print(
                    f"Inserted initial row into {self.ngrok_tokens_table_name} with runner_id: {runner_id}"
                )
            else:
                print(f"Table {self.ngrok_tokens_table_name} already exists.")
        except Exception as e:
            print(
                f"An error occurred while checking or creating table {self.ngrok_tokens_table_name}: {e}"
            )
        finally:
            if cursor is not None:
                cursor.close()

        available_tools_table_check_query = (
            f"SHOW TABLES LIKE 'AVAILABLE_TOOLS' IN SCHEMA {self.schema};"
        )
        try:
            cursor = self.client.cursor()
            # cursor.execute(available_tools_table_check_query)
            if True:
                available_tools_table_ddl = f"""
                CREATE OR REPLACE TABLE {self.available_tools_table_name} (
                    TOOL_NAME VARCHAR(16777216),
                    TOOL_DESCRIPTION VARCHAR(16777216)
                );
                """
                cursor.execute(available_tools_table_ddl)
                self.client.commit()
                print(
                    f"Table {self.available_tools_table_name} (re)created, this is expected on every run."
                )

                tools_data = core.bot_os_tool_descriptions.tools_data

                insert_tools_query = f"""
                INSERT INTO {self.available_tools_table_name} (TOOL_NAME, TOOL_DESCRIPTION)
                VALUES (%s, %s);
                """
                for tool_name, tool_description in tools_data:
                    cursor.execute(insert_tools_query, (tool_name, tool_description))
                self.client.commit()
                print(f"Inserted initial rows into {self.available_tools_table_name}")
            else:
                print(f"Table {self.available_tools_table_name} already exists.")
        except Exception as e:
            print(
                f"An error occurred while checking or creating table {self.available_tools_table_name}: {e}"
            )
        finally:
            if cursor is not None:
                cursor.close()

        # Check if the 'snowflake_semantic_tools' row exists in the available_tables and insert if not present
        check_snowflake_semantic_tools_query = f"SELECT COUNT(*) FROM {self.available_tools_table_name} WHERE TOOL_NAME = 'snowflake_semantic_tools';"
        try:
            cursor = self.client.cursor()
            cursor.execute(check_snowflake_semantic_tools_query)
            if cursor.fetchone()[0] == 0:
                insert_snowflake_semantic_tools_query = f"""
                INSERT INTO {self.available_tools_table_name} (TOOL_NAME, TOOL_DESCRIPTION)
                VALUES ('snowflake_semantic_tools', 'Create and modify Snowflake Semantic Models');
                """
                cursor.execute(insert_snowflake_semantic_tools_query)
                self.client.commit()
                print("Inserted 'snowflake_semantic_tools' into available_tools table.")
        except Exception as e:
            print(
                f"An error occurred while inserting 'snowflake_semantic_tools' into available_tools table: {e}"
            )
        finally:
            if cursor is not None:
                cursor.close()

        # CHAT HISTORY TABLE
        chat_history_table_id = self.message_log_table_name
        chat_history_table_check_query = (
            f"SHOW TABLES LIKE 'MESSAGE_LOG' IN SCHEMA {self.schema};"
        )

        # Check if the chat history table exists
        try:
            cursor = self.client.cursor()
            cursor.execute(chat_history_table_check_query)
            if not cursor.fetchone():
                chat_history_table_ddl = f"""
                CREATE TABLE {self.message_log_table_name} (
                    timestamp TIMESTAMP NOT NULL,
                    bot_id STRING NOT NULL,
                    bot_name STRING NOT NULL,
                    thread_id STRING,
                    message_type STRING NOT NULL,
                    message_payload STRING,
                    message_metadata STRING,
                    tokens_in INTEGER,
                    tokens_out INTEGER,
                    files STRING,
                    channel_type STRING,
                    channel_name STRING,
                    primary_user STRING,
                    task_id STRING
                );
                """
                cursor.execute(chat_history_table_ddl)
                self.client.commit()
                print(f"Table {self.message_log_table_name} created.")
            else:
                check_query = f"DESCRIBE TABLE {chat_history_table_id};"
                try:
                    cursor.execute(check_query)
                    columns = [col[0] for col in cursor.fetchall()]
                    for col in [
                        "FILES",
                        "CHANNEL_TYPE",
                        "CHANNEL_NAME",
                        "PRIMARY_USER",
                        "TASK_ID",
                    ]:
                        if col not in columns:
                            alter_table_query = f"ALTER TABLE {chat_history_table_id} ADD COLUMN {col} STRING;"
                            cursor.execute(alter_table_query)
                            self.client.commit()
                            logger.info(
                                f"Column '{col}' added to table {chat_history_table_id}."
                            )
                except Exception as e:
                    print("Error adding column FILES to MESSAGE_LOG: ", e)
                print(f"Table {self.message_log_table_name} already exists.")
        except Exception as e:
            print(
                f"An error occurred while checking or creating table {self.message_log_table_name}: {e}"
            )

        # KNOWLEDGE TABLE
        knowledge_table_check_query = (
            f"SHOW TABLES LIKE 'KNOWLEDGE' IN SCHEMA {self.schema};"
        )
        # Check if the chat knowledge table exists
        try:
            cursor = self.client.cursor()
            cursor.execute(knowledge_table_check_query)
            if not cursor.fetchone():
                knowledge_table_ddl = f"""
                CREATE TABLE {self.knowledge_table_name} (
                    timestamp TIMESTAMP NOT NULL,
                    thread_id STRING NOT NULL,
                    knowledge_thread_id STRING NOT NULL,
                    primary_user STRING,
                    bot_id STRING,
                    last_timestamp TIMESTAMP NOT NULL,
                    thread_summary STRING,
                    user_learning STRING,
                    tool_learning STRING,
                    data_learning STRING
                );
                """
                cursor.execute(knowledge_table_ddl)
                self.client.commit()
                print(f"Table {self.knowledge_table_name} created.")
            else:
                check_query = f"DESCRIBE TABLE {self.knowledge_table_name};"
                print(f"Table {self.knowledge_table_name} already exists.")
        except Exception as e:
            print(
                f"An error occurred while checking or creating table {self.knowledge_table_name}: {e}"
            )

        # PROCESSES TABLE
        processes_table_check_query = (
            f"SHOW TABLES LIKE 'PROCESSES' IN SCHEMA {self.schema};"
        )
        # Check if the processes table exists
        try:
            cursor = self.client.cursor()
            cursor.execute(processes_table_check_query)
            if not cursor.fetchone():
                processes_table_ddl = f"""
                CREATE TABLE {self.processes_table_name} (
                    timestamp TIMESTAMP NOT NULL,
                    process_id STRING NOT NULL,
                    bot_id STRING,
                    process_name STRING NOT NULL,
                    process_details STRING,
                    process_instructions STRING
                );
                """
                cursor.execute(processes_table_ddl)
                self.client.commit()
                print(f"Table {self.processes_table_name} created.")
            else:
                check_query = f"DESCRIBE TABLE {self.processes_table_name};"
                print(f"Table {self.processes_table_name} already exists.")
        except Exception as e:
            print(
                f"An error occurred while checking or creating table {self.processes_table_name}: {e}"
            )

        # PROCESS_HISTORY TABLE
        process_history_table_check_query = (
            f"SHOW TABLES LIKE 'PROCESS_HISTORY' IN SCHEMA {self.schema};"
        )
        # Check if the processes table exists
        try:
            cursor = self.client.cursor()
            cursor.execute(process_history_table_check_query)
            if not cursor.fetchone():
                process_history_table_ddl = f"""
                CREATE TABLE {self.process_history_table_name} (
                    timestamp TIMESTAMP NOT NULL,
                    process_id STRING NOT NULL,
                    work_done_summary STRING,
                    process_status STRING,
                    updated_process_learnings STRING,
                    report_message STRING,
                    done_flag BOOLEAN,
                    needs_help_flag BOOLEAN,
                    process_clarity_comments STRING
                );
                """
                cursor.execute(process_history_table_ddl)
                self.client.commit()
                print(f"Table {self.process_history_table_name} created.")
            else:
                check_query = f"DESCRIBE TABLE {self.process_history_table_name};"
                print(f"Table {self.process_history_table_name} already exists.")
        except Exception as e:
            print(
                f"An error occurred while checking or creating table {self.process_history_table_name}: {e}"
            )

        # USER BOT TABLE
        user_bot_table_check_query = (
            f"SHOW TABLES LIKE 'USER_BOT' IN SCHEMA {self.schema};"
        )
        # Check if the chat knowledge table exists
        try:
            cursor = self.client.cursor()
            cursor.execute(user_bot_table_check_query)
            if not cursor.fetchone():
                user_bot_table_ddl = f"""
                CREATE TABLE IF NOT EXISTS {self.user_bot_table_name} (
                    timestamp TIMESTAMP NOT NULL,
                    primary_user STRING,
                    bot_id STRING,                    
                    user_learning STRING,
                    tool_learning STRING,
                    data_learning STRING
                );
                """
                cursor.execute(user_bot_table_ddl)
                self.client.commit()
                print(f"Table {self.user_bot_table_name} created.")
            else:
                check_query = f"DESCRIBE TABLE {self.user_bot_table_name};"
                print(f"Table {self.user_bot_table_name} already exists.")
        except Exception as e:
            print(
                f"An error occurred while checking or creating table {self.user_bot_table_name}: {e}"
            )

        # HARVEST CONTROL TABLE
        hc_table_id = self.genbot_internal_harvest_control_table
        hc_table_check_query = (
            f"SHOW TABLES LIKE '{hc_table_id.upper()}' IN SCHEMA {self.schema};"
        )

        # Check if the harvest control table exists
        try:
            cursor.execute(hc_table_check_query)
            if not cursor.fetchone():
                hc_table_id = self.harvest_control_table_name
                hc_table_ddl = f"""
                CREATE TABLE {hc_table_id} (
                    source_name STRING NOT NULL,
                    database_name STRING NOT NULL,
                    schema_inclusions ARRAY,
                    schema_exclusions ARRAY,
                    status STRING NOT NULL,
                    refresh_interval INTEGER NOT NULL,
                    initial_crawl_complete BOOLEAN NOT NULL
                );
                """
                cursor.execute(hc_table_ddl)
                self.client.commit()
                print(f"Table {hc_table_id} created.")
            else:
                print(f"Table {hc_table_id} already exists.")
        except Exception as e:
            print(
                f"An error occurred while checking or creating table {hc_table_id}: {e}"
            )

        # METADATA TABLE FOR HARVESTER RESULTS
        metadata_table_id = self.genbot_internal_harvest_table
        metadata_table_check_query = (
            f"SHOW TABLES LIKE '{metadata_table_id.upper()}' IN SCHEMA {self.schema};"
        )

        # Check if the metadata table exists
        try:
            cursor.execute(metadata_table_check_query)
            if not cursor.fetchone():
                metadata_table_id = self.metadata_table_name
                metadata_table_ddl = f"""
                CREATE TABLE {metadata_table_id} (
                    source_name STRING NOT NULL,
                    qualified_table_name STRING NOT NULL,
                    database_name STRING NOT NULL,
                    memory_uuid STRING NOT NULL,
                    schema_name STRING NOT NULL,
                    table_name STRING NOT NULL,
                    complete_description STRING NOT NULL,
                    ddl STRING NOT NULL,
                    ddl_short STRING,
                    ddl_hash STRING NOT NULL,
                    summary STRING NOT NULL,
                    sample_data_text STRING NOT NULL,
                    last_crawled_timestamp TIMESTAMP NOT NULL,
                    crawl_status STRING NOT NULL,
                    role_used_for_crawl STRING NOT NULL,
                    embedding ARRAY,
                    embedding_native ARRAY
                );
                """
                cursor.execute(metadata_table_ddl)
                self.client.commit()
                print(f"Table {metadata_table_id} created.")

                try:
                    insert_initial_metadata_query = f"""
                    INSERT INTO {metadata_table_id} (SOURCE_NAME, QUALIFIED_TABLE_NAME, DATABASE_NAME, MEMORY_UUID, SCHEMA_NAME, TABLE_NAME, COMPLETE_DESCRIPTION, DDL, DDL_SHORT, DDL_HASH, SUMMARY, SAMPLE_DATA_TEXT, LAST_CRAWLED_TIMESTAMP, CRAWL_STATUS, ROLE_USED_FOR_CRAWL)
                    SELECT SOURCE_NAME, replace(QUALIFIED_TABLE_NAME,'APP_NAME', CURRENT_DATABASE()) QUALIFIED_TABLE_NAME,  CURRENT_DATABASE() DATABASE_NAME, MEMORY_UUID, SCHEMA_NAME, TABLE_NAME, REPLACE(COMPLETE_DESCRIPTION,'APP_NAME', CURRENT_DATABASE()) COMPLETE_DESCRIPTION, REPLACE(DDL,'APP_NAME', CURRENT_DATABASE()) DDL, REPLACE(DDL_SHORT,'APP_NAME', CURRENT_DATABASE()) DDL_SHORT, 'SHARED_VIEW' DDL_HASH, REPLACE(SUMMARY,'APP_NAME', CURRENT_DATABASE()) SUMMARY, SAMPLE_DATA_TEXT, LAST_CRAWLED_TIMESTAMP, CRAWL_STATUS, ROLE_USED_FOR_CRAWL 
                    FROM APP_SHARE.HARVEST_RESULTS WHERE SCHEMA_NAME IN ('BASEBALL','FORMULA_1') AND DATABASE_NAME = 'APP_NAME'
                    """
                    cursor.execute(insert_initial_metadata_query)
                    self.client.commit()
                    print(f"Inserted initial rows into {metadata_table_id}")
                except Exception as e:
                    print(
                        f"Initial rows from APP_SHARE.HARVEST_RESULTS NOT ADDED into {metadata_table_id} due to erorr {e}"
                    )

            else:
                # Check if the 'ddl_short' column exists in the metadata table
                metadata_col_check_query = f"DESCRIBE TABLE {self.metadata_table_name};"
                try:
                    cursor.execute(metadata_col_check_query)
                    columns = [col[0] for col in cursor.fetchall()]
                    if "DDL_SHORT" not in columns:
                        alter_table_query = f"ALTER TABLE {self.metadata_table_name} ADD COLUMN ddl_short STRING;"
                        cursor.execute(alter_table_query)
                        self.client.commit()
                        print(f"Column 'ddl_short' added to table {metadata_table_id}.")
                except Exception as e:
                    print(
                        f"An error occurred while checking or altering table {metadata_table_id}: {e}"
                    )
                # Check if the 'embedding_native' column exists in the metadata table
                try:
                    if "EMBEDDING_NATIVE" not in columns:
                        alter_table_query = f"ALTER TABLE {self.metadata_table_name} ADD COLUMN embedding_native ARRAY;"
                        cursor.execute(alter_table_query)
                        self.client.commit()
                        print(f"Column 'embedding_native' added to table {metadata_table_id}.")
                except Exception as e:
                    print(
                        f"An error occurred while checking or altering table {metadata_table_id}: {e}"
                    )                    
                print(f"Table {metadata_table_id} already exists.")
        except Exception as e:
            print(
                f"An error occurred while checking or creating table {metadata_table_id}: {e}"
            )

        cursor = self.client.cursor()

        cortex_threads_input_table_check_query = (
            f"SHOW TABLES LIKE 'CORTEX_THREADS_INPUT' IN SCHEMA {self.schema};"
        )
        try:
            cursor.execute(cortex_threads_input_table_check_query)
            if not cursor.fetchone():
                cortex_threads_input_table_ddl = f"""
                CREATE TABLE {self.schema}.CORTEX_THREADS_INPUT (
                    timestamp TIMESTAMP,
                    bot_id VARCHAR,
                    bot_name VARCHAR,
                    thread_id VARCHAR,
                    message_type VARCHAR,
                    message_payload VARCHAR,
                    message_metadata VARCHAR,
                    tokens_in NUMBER,
                    tokens_out NUMBER
                );
                """
                cursor.execute(cortex_threads_input_table_ddl)
                self.client.commit()
                print(f"Table {self.schema}.CORTEX_THREADS_INPUT created.")
            else:
                print(f"Table {self.schema}.CORTEX_THREADS_INPUT already exists.")
        except Exception as e:
            print(
                f"An error occurred while checking or creating table CORTEX_THREADS_INPUT: {e}"
            )

        cortex_threads_output_table_check_query = (
            f"SHOW TABLES LIKE 'CORTEX_THREADS_OUTPUT' IN SCHEMA {self.schema};"
        )
        try:
            cursor.execute(cortex_threads_output_table_check_query)
            if not cursor.fetchone():
                cortex_threads_output_table_ddl = f"""
                CREATE TABLE {self.schema}.CORTEX_THREADS_OUTPUT (
                    timestamp TIMESTAMP,
                    bot_id VARCHAR,
                    bot_name VARCHAR,
                    thread_id VARCHAR,
                    message_type VARCHAR,
                    message_payload VARCHAR,
                    message_metadata VARCHAR,
                    tokens_in NUMBER,
                    tokens_out NUMBER,
                    model_name VARCHAR, -- either mistral-large, snowflake-arctic, etc.
                    messages_concatenated VARCHAR
                );
                """
                cursor.execute(cortex_threads_output_table_ddl)
                self.client.commit()
                print(f"Table {self.schema}.CORTEX_THREADS_OUTPUT created.")
            else:
                print(f"Table {self.schema}.CORTEX_THREADS_OUTPUT already exists.")
        except Exception as e:
            print(
                f"An error occurred while checking or creating table CORTEX_THREADS_OUTPUT: {e}"
            )

    def insert_table_summary(
        self,
        database_name,
        schema_name,
        table_name,
        ddl,
        ddl_short,
        summary,
        sample_data_text,
        complete_description="",
        crawl_status="Completed",
        role_used_for_crawl="Default",
        embedding=None,
        memory_uuid=None,
        ddl_hash=None,
    ):
        qualified_table_name = f'"{database_name}"."{schema_name}"."{table_name}"'
        if not memory_uuid:
            memory_uuid = str(uuid.uuid4())
        last_crawled_timestamp = datetime.utcnow().isoformat(" ")
        if not ddl_hash:
            ddl_hash = self.sha256_hash_hex_string(ddl)

        # Assuming role_used_for_crawl is stored in self.connection_info["client_email"]
        role_used_for_crawl = self.role

        # if cortex mode, load embedding_native else load embedding column
        if os.environ.get("CORTEX_MODE", 'False') == 'True':
            embedding_target = 'embedding_native'
        else:
            embedding_target = 'embedding'

        # Convert embedding list to string format if not None
        embedding_str = (",".join(str(e) for e in embedding) if embedding is not None else None)

        # Construct the MERGE SQL statement with placeholders for parameters
        merge_sql = f"""
        MERGE INTO {self.metadata_table_name} USING (
            SELECT
                %(source_name)s AS source_name,
                %(qualified_table_name)s AS qualified_table_name,
                %(memory_uuid)s AS memory_uuid,
                %(database_name)s AS database_name,
                %(schema_name)s AS schema_name,
                %(table_name)s AS table_name,
                %(complete_description)s AS complete_description,
                %(ddl)s AS ddl,
                %(ddl_short)s AS ddl_short,
                %(ddl_hash)s AS ddl_hash,
                %(summary)s AS summary,
                %(sample_data_text)s AS sample_data_text,
                %(last_crawled_timestamp)s AS last_crawled_timestamp,
                %(crawl_status)s AS crawl_status,
                %(role_used_for_crawl)s AS role_used_for_crawl,
                %(embedding)s AS {embedding_target}
        ) AS new_data
        ON {self.metadata_table_name}.qualified_table_name = new_data.qualified_table_name
        WHEN MATCHED THEN UPDATE SET
            source_name = new_data.source_name,
            memory_uuid = new_data.memory_uuid,
            database_name = new_data.database_name,
            schema_name = new_data.schema_name,
            table_name = new_data.table_name,
            complete_description = new_data.complete_description,
            ddl = new_data.ddl,
            ddl_short = new_data.ddl_short,
            ddl_hash = new_data.ddl_hash,
            summary = new_data.summary,
            sample_data_text = new_data.sample_data_text,
            last_crawled_timestamp = TO_TIMESTAMP_NTZ(new_data.last_crawled_timestamp),
            crawl_status = new_data.crawl_status,
            role_used_for_crawl = new_data.role_used_for_crawl,
            {embedding_target} = ARRAY_CONSTRUCT(new_data.{embedding_target})
        WHEN NOT MATCHED THEN INSERT (
            source_name, qualified_table_name, memory_uuid, database_name, schema_name, table_name,
            complete_description, ddl, ddl_short, ddl_hash, summary, sample_data_text, last_crawled_timestamp,
            crawl_status, role_used_for_crawl, {embedding_target}
        ) VALUES (
            new_data.source_name, new_data.qualified_table_name, new_data.memory_uuid, new_data.database_name,
            new_data.schema_name, new_data.table_name, new_data.complete_description, new_data.ddl, new_data.ddl_short,
            new_data.ddl_hash, new_data.summary, new_data.sample_data_text, TO_TIMESTAMP_NTZ(new_data.last_crawled_timestamp),
            new_data.crawl_status, new_data.role_used_for_crawl, ARRAY_CONSTRUCT(new_data.{embedding_target})
        );
        """

        # Set up the query parameters
        query_params = {
            "source_name": self.source_name,
            "qualified_table_name": qualified_table_name,
            "memory_uuid": memory_uuid,
            "database_name": database_name,
            "schema_name": schema_name,
            "table_name": table_name,
            "complete_description": complete_description,
            "ddl": ddl,
            "ddl_short": ddl_short,
            "ddl_hash": ddl_hash,
            "summary": summary,
            "sample_data_text": sample_data_text,
            "last_crawled_timestamp": last_crawled_timestamp,
            "crawl_status": crawl_status,
            "role_used_for_crawl": role_used_for_crawl,
            "embedding": embedding_str,
        }

        for param, value in query_params.items():
            # print(f'{param}: {value}')
            if value is None:
                # print(f'{param} is null')
                query_params[param] = "NULL"

        # Execute the MERGE statement with parameters
        try:
            # print("merge sql: ",merge_sql)
            cursor = self.client.cursor()
            cursor.execute(merge_sql, query_params)
            self.client.commit()
        except Exception as e:
            print(f"An error occurred while executing the MERGE statement: {e}")
        finally:
            if cursor is not None:
                cursor.close()

    # make sure this is returning whats expected (array vs string)
    def get_table_ddl(self, database_name: str, schema_name: str, table_name=None):
        """
        Fetches the DDL statements for tables within a specific schema in Snowflake.
        Optionally, fetches the DDL for a specific table if table_name is provided.

        :param database_name: The name of the database.
        :param schema_name: The name of the schema.
        :param table_name: Optional. The name of a specific table.
        :return: A dictionary with table names as keys and DDL statements as values, or a single DDL string if table_name is provided.
        """
        if table_name:
            query = f"SHOW TABLES LIKE '{table_name}' IN SCHEMA {database_name}.{schema_name};"
            cursor = self.client.cursor()
            cursor.execute(query)
            result = cursor.fetchone()
            if result:
                # Fetch the DDL for the specific table
                query_ddl = f"SELECT GET_DDL('TABLE', '{result[1]}')"
                cursor.execute(query_ddl)
                ddl_result = cursor.fetchone()
                return {table_name: ddl_result[0]}
            else:
                return {}
        else:
            query = f"SHOW TABLES IN SCHEMA {database_name}.{schema_name};"
            cursor = self.client.cursor()
            cursor.execute(query)
            tables = cursor.fetchall()
            ddls = {}
            for table in tables:
                # Fetch the DDL for each table
                query_ddl = f"SELECT GET_DDL('TABLE', '{table[1]}')"
                cursor.execute(query_ddl)
                ddl_result = cursor.fetchone()
                ddls[table[1]] = ddl_result[0]
            return ddls

    def check_cached_metadata(
        self, database_name: str, schema_name: str, table_name: str
    ):
        try:
            if database_name and schema_name and table_name:
                query = f"SELECT IFF(count(*)>0,TRUE,FALSE) from APP_SHARE.HARVEST_RESULTS where DATABASE_NAME = '{database_name}' AND SCHEMA_NAME = '{schema_name}' AND TABLE_NAME = '{table_name}';"
                cursor = self.client.cursor()
                cursor.execute(query)
                result = cursor.fetchone()
                return result[0]
            else:
                return "a required parameter was not entered"
        except Exception as e:
            if os.environ.get('GENESIS_LOCAL_RUNNER', '').upper() != 'TRUE':
                print(f"Error checking cached metadata: {e}")
            return False

    def get_metadata_from_cache(
        self, database_name: str, schema_name: str, table_name: str
    ):
        metadata_table_id = self.metadata_table_name
        try:
            if schema_name == "INFORMATION_SCHEMA":
                db_name_filter = "PLACEHOLDER_DB_NAME"
            else:
                db_name_filter = database_name

            query = f"""SELECT SOURCE_NAME, replace(QUALIFIED_TABLE_NAME,'PLACEHOLDER_DB_NAME','{database_name}') QUALIFIED_TABLE_NAME, '{database_name}' DATABASE_NAME, MEMORY_UUID, SCHEMA_NAME, TABLE_NAME, REPLACE(COMPLETE_DESCRIPTION,'PLACEHOLDER_DB_NAME','{database_name}') COMPLETE_DESCRIPTION, REPLACE(DDL,'PLACEHOLDER_DB_NAME','{database_name}') DDL, REPLACE(DDL_SHORT,'PLACEHOLDER_DB_NAME','{database_name}') DDL_SHORT, 'SHARED_VIEW' DDL_HASH, REPLACE(SUMMARY,'PLACEHOLDER_DB_NAME','{database_name}') SUMMARY, SAMPLE_DATA_TEXT, LAST_CRAWLED_TIMESTAMP, CRAWL_STATUS, ROLE_USED_FOR_CRAWL 
                from APP_SHARE.HARVEST_RESULTS 
                where DATABASE_NAME = '{db_name_filter}' AND SCHEMA_NAME = '{schema_name}' AND TABLE_NAME = '{table_name}';"""

            # insert_cached_metadata_query = f"""
            #     INSERT INTO {metadata_table_id}
            #     SELECT SOURCE_NAME, QUALIFIED_TABLE_NAME,  DATABASE_NAME, MEMORY_UUID, SCHEMA_NAME, TABLE_NAME, COMPLETE_DESCRIPTION, DDL, DDL_SHORT, DDL_HASH, SUMMARY, SAMPLE_DATA_TEXT, LAST_CRAWLED_TIMESTAMP, CRAWL_STATUS, ROLE_USED_FOR_CRAWL, EMBEDDING
            #     FROM APP_SHARE.HARVEST_RESULTS h
            #     WHERE DATABASE_NAME = '{database_name}' AND SCHEMA_NAME = '{schema_name}' AND TABLE_NAME = '{table_name}'
            #     AND NOT EXISTS (SELECT 1 FROM {metadata_table_id} m WHERE m.DATABASE_NAME = '{database_name}' and m.SCHEMA_NAME = '{schema_name}' and m.TABLE_NAME = '{table_name}');
            # """
            cursor = self.client.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            columns = [col[0].lower() for col in cursor.description]
            cached_metadata = [dict(zip(columns, row)) for row in results]
            cursor.close()
            return cached_metadata

            print(
                f"Retrieved cached rows from {metadata_table_id} for {database_name}.{schema_name}.{table_name}"
            )
        except Exception as e:
            print(
                f"Cached rows from APP_SHARE.HARVEST_RESULTS NOT retrieved from {metadata_table_id} for {database_name}.{schema_name}.{table_name} due to erorr {e}"
            )

    # snowed

    # snowed
    def refresh_connection(self):
        if self.token_connection:
            self.connection = self._create_connection()

    def connection(self) -> snowflake.connector.SnowflakeConnection:

        if os.path.isfile("/snowflake/session/token"):
            creds = {
                "host": os.getenv("SNOWFLAKE_HOST"),
                "port": os.getenv("SNOWFLAKE_PORT"),
                "protocol": "https",
                "account": os.getenv("SNOWFLAKE_ACCOUNT"),
                "authenticator": "oauth",
                "token": open("/snowflake/session/token", "r").read(),
                "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
                "database": os.getenv("SNOWFLAKE_DATABASE"),
                "schema": os.getenv("SNOWFLAKE_SCHEMA"),
                "client_session_keep_alive": True,
            }
        else:
            creds = {
                "account": os.getenv("SNOWFLAKE_ACCOUNT"),
                "user": os.getenv("SNOWFLAKE_USER"),
                "password": os.getenv("SNOWFLAKE_PASSWORD"),
                "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
                "database": os.getenv("SNOWFLAKE_DATABASE"),
                "schema": os.getenv("SNOWFLAKE_SCHEMA"),
                "client_session_keep_alive": True,
            }

        connection = snowflake.connector.connect(**creds)
        return connection

    # def _create_connection(self):

    # Connector connection
    #    conn = self.connection()
    #    return conn

    def _create_connection(self):

        # Snowflake token testing
        self.token_connection = False
        #  logger.warn('Creating connection..')
        SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT", self.account)
        SNOWFLAKE_HOST = os.getenv("SNOWFLAKE_HOST", None)
        logger.info("Checking possible SPCS ENV vars -- Account, Host: %s, %s", SNOWFLAKE_ACCOUNT, SNOWFLAKE_HOST,)

        logger.info("SNOWFLAKE_HOST: %s", os.getenv("SNOWFLAKE_HOST"))
        logger.info("SNOWFLAKE_ACCOUNT: %s", os.getenv("SNOWFLAKE_ACCOUNT"))
        logger.info("SNOWFLAKE_PORT: %s", os.getenv("SNOWFLAKE_PORT"))
        #  logger.warn('SNOWFLAKE_WAREHOUSE: %s', os.getenv('SNOWFLAKE_WAREHOUSE'))
        logger.info("SNOWFLAKE_DATABASE: %s", os.getenv("SNOWFLAKE_DATABASE"))
        logger.info("SNOWFLAKE_SCHEMA: %s", os.getenv("SNOWFLAKE_SCHEMA"))

        if (SNOWFLAKE_ACCOUNT and SNOWFLAKE_HOST and os.getenv("SNOWFLAKE_PASSWORD_OVERRIDE", None) == None):
            # token based connection from SPCS
            with open("/snowflake/session/token", "r") as f:
                snowflake_token = f.read()
            print(f"Natapp Connection: SPCS Snowflake token found, length: {len(snowflake_token)}", flush=True)
            self.token_connection = True
            #   logger.warn('Snowflake token mode (SPCS)...')
            if os.getenv("SNOWFLAKE_SECURE", "TRUE").upper() == "FALSE":
                #        logger.info('insecure mode')
                return connect(
                    host=os.getenv("SNOWFLAKE_HOST"),
                    #        port = os.getenv('SNOWFLAKE_PORT'),
                    protocol="https",
                    #     warehouse = os.getenv('SNOWFLAKE_WAREHOUSE'),
                    database=os.getenv("SNOWFLAKE_DATABASE"),
                    schema=os.getenv("SNOWFLAKE_SCHEMA"),
                    account=os.getenv("SNOWFLAKE_ACCOUNT"),
                    token=snowflake_token,
                    authenticator="oauth",
                    insecure_mode=True,
                    client_session_keep_alive=True,
                )

            else:
                #        logger.info('secure mode')
                return connect(
                    host=os.getenv("SNOWFLAKE_HOST"),
                    #         port = os.getenv('SNOWFLAKE_PORT'),
                    #         protocol = 'https',
                    #         warehouse = os.getenv('SNOWFLAKE_WAREHOUSE'),
                    database=os.getenv("SNOWFLAKE_DATABASE"),
                    schema=os.getenv("SNOWFLAKE_SCHEMA"),
                    account=os.getenv("SNOWFLAKE_ACCOUNT"),
                    token=snowflake_token,
                    authenticator="oauth",
                    client_session_keep_alive=True,
                )

        print("Creating Snowflake regular connection...")
        # self.token_connection = False

        if os.getenv("SNOWFLAKE_SECURE", "TRUE").upper() == "FALSE":
            return connect(
                user=self.user,
                password=self.password,
                account=self.account,
                warehouse=self.warehouse,
                database=self.database,
                role=self.role,
                insecure_mode=True,
                client_session_keep_alive=True,
            )
        else:
            return connect(
                user=self.user,
                password=self.password,
                account=self.account,
                warehouse=self.warehouse,
                database=self.database,
                role=self.role,
                client_session_keep_alive=True,
            )

    # snowed
    def connector_type(self):
        return "snowflake"

    def get_databases(self, thread_id=None):
        databases = []
        # query = (
        #     "SELECT source_name, database_name, schema_inclusions, schema_exclusions, status, refresh_interval, initial_crawl_complete FROM "
        #     + self.harvest_control_table_name
        # )
        if os.environ.get("CORTEX_MODE", 'False') == 'True':
            embedding_column = 'embedding_native'
        else:
            embedding_column = 'embedding'

        # query = (
        #     f"""SELECT c.source_name, c.database_name, c.schema_inclusions, c.schema_exclusions, c.status, c.refresh_interval, MAX(CASE WHEN c.initial_crawl_complete = FALSE THEN FALSE ELSE CASE WHEN c.initial_crawl_complete = TRUE AND r.{embedding_column} IS NULL THEN FALSE ELSE TRUE END END) AS initial_crawl_complete 
        #       FROM {self.harvest_control_table_name} c LEFT OUTER JOIN {self.metadata_table_name} r ON c.source_name = r.source_name AND c.database_name = r.database_name 
        #       GROUP BY c.source_name,c.database_name,c.schema_inclusions,c.schema_exclusions,c.status, c.refresh_interval, c.initial_crawl_complete
        #     """
        # )

        query = (
            f"""SELECT c.source_name,  c.database_name, c.schema_inclusions,  c.schema_exclusions, c.status,  c.refresh_interval, 
                    MAX(CASE WHEN c.initial_crawl_complete = FALSE THEN FALSE WHEN embedding_count < total_count THEN FALSE ELSE TRUE END) AS initial_crawl_complete 
                FROM (
                    SELECT c.source_name,  c.database_name, c.schema_inclusions, c.schema_exclusions,  c.status,  c.refresh_interval,  COUNT(r.{embedding_column}) AS embedding_count,  COUNT(*) AS total_count, c.initial_crawl_complete
                    FROM {self.genbot_internal_project_and_schema}.harvest_control c LEFT OUTER JOIN {self.genbot_internal_project_and_schema}.harvest_results r ON c.source_name = r.source_name AND c.database_name = r.database_name 
                    GROUP BY c.source_name, c.database_name, c.schema_inclusions, c.schema_exclusions, c.status, c.refresh_interval, c.initial_crawl_complete) AS c
                GROUP BY source_name, database_name, schema_inclusions, schema_exclusions, status, refresh_interval
            """
        )
        cursor = self.connection.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        columns = [col[0].lower() for col in cursor.description]
        databases = [dict(zip(columns, row)) for row in results]
        cursor.close()

        return databases

    def get_visible_databases(self, thread_id=None):
        schemas = []
        query = "SHOW DATABASES"
        cursor = self.connection.cursor()
        cursor.execute(query)
        for row in cursor:
            schemas.append(row[1])  # Assuming the schema name is in the second column
        cursor.close()
        return schemas

    def get_schemas(self, database, thread_id=None):
        schemas = []
        try:
            query = f'SHOW SCHEMAS IN DATABASE "{database}"'
            cursor = self.connection.cursor()
            cursor.execute(query)
            for row in cursor:
                schemas.append(row[1])  # Assuming the schema name is in the second column
            cursor.close()
        except Exception as e:
            # print(f"error getting schemas for {database}: {e}")
            return schemas
        return schemas

    def get_tables(self, database, schema, thread_id=None):
        tables = []
        try:
            query = f'SHOW TABLES IN "{database}"."{schema}"'
            cursor = self.connection.cursor()
            cursor.execute(query)
            for row in cursor:
                tables.append(
                    {"table_name": row[1], "object_type": "TABLE"}
                )  # Assuming the table name is in the second column and DDL in the third
            cursor.close()
            query = f'SHOW VIEWS IN "{database}"."{schema}"'
            cursor = self.connection.cursor()
            cursor.execute(query)
            for row in cursor:
                tables.append(
                    {"table_name": row[1], "object_type": "VIEW"}
                )  # Assuming the table name is in the second column and DDL in the third
            cursor.close()
        except Exception as e:
            # print(f"error getting tables for {database}.{schema}: {e}")
            return tables
        return tables

    def get_columns(self, database, schema, table):
        columns = []
        try:
            query = f'SHOW COLUMNS IN "{database}"."{schema}"."{table}"'
            cursor = self.connection.cursor()
            cursor.execute(query)
            for row in cursor:
                columns.append(row[2])  # Assuming the column name is in the first column
            cursor.close()
        except Exception as e:
            return columns
        return columns

    def alt_get_ddl(self,table_name = None):
        #print(table_name) 
        describe_query = f"DESCRIBE TABLE {table_name};"
        try:
            describe_result = self.run_query(query=describe_query, max_rows=1000, max_rows_override=True)
        except:
            return None 
        
        ddl_statement = "CREATE TABLE " + table_name + " (\n"
        for column in describe_result:
            column_name = column['name']
            column_type = column['type']
            nullable = " NOT NULL" if not column['null?'] else ""
            default = f" DEFAULT {column['default']}" if column['default'] is not None else ""
            comment = f" COMMENT '{column['comment']}'" if 'comment' in column and column['comment'] is not None else ""
            key = ""
            if column.get('primary_key', False):
                key = " PRIMARY KEY"
            elif column.get('unique_key', False):
                key = " UNIQUE"
            ddl_statement += f"    {column_name} {column_type}{nullable}{default}{key}{comment},\n"
        ddl_statement = ddl_statement.rstrip(',\n') + "\n);"
        #print(ddl_statement)
        return ddl_statement

    def get_sample_data(self, database, schema_name: str, table_name: str):
        """
        Fetches 10 rows of sample data from a specific table in Snowflake.

        :param database: The name of the database.
        :param schema_name: The name of the schema.
        :param table_name: The name of the table.
        :return: A list of dictionaries representing rows of sample data.
        """
        query = f'SELECT * FROM "{database}"."{schema_name}"."{table_name}" LIMIT 10'
        cursor = self.connection.cursor()
        cursor.execute(query)
        columns = [col[0] for col in cursor.description]
        sample_data = [dict(zip(columns, row)) for row in cursor]
        cursor.close()
        return sample_data

    def create_bot_workspace(self, workspace_schema_name):
        try:

            query = f"CREATE SCHEMA IF NOT EXISTS {workspace_schema_name}"
            cursor = self.client.cursor()
            cursor.execute(query)
            self.client.commit()
            logger.info(f"Workspace schema {workspace_schema_name} created")
        except Exception as e:
            logger.error(f"Failed to create bot workspace {workspace_schema_name}: {e}")

    def grant_all_bot_workspace(self, workspace_schema_name):
        try:

            query = f"GRANT ALL PRIVILEGES ON SCHEMA {workspace_schema_name} TO APPLICATION ROLE APP_PUBLIC; "
            cursor = self.client.cursor()
            cursor.execute(query)
            self.client.commit()

            query = f"GRANT SELECT ON ALL TABLES IN SCHEMA {workspace_schema_name} TO APPLICATION ROLE APP_PUBLIC; "
            cursor = self.client.cursor()
            cursor.execute(query)
            self.client.commit()

            query = f"GRANT SELECT ON ALL VIEWS IN SCHEMA {workspace_schema_name} TO APPLICATION ROLE APP_PUBLIC; "
            cursor = self.client.cursor()
            cursor.execute(query)
            self.client.commit()

            logger.info(
                f"Workspace {workspace_schema_name} objects granted to APP_PUBLIC"
            )
        except Exception as e:
            if not os.getenv("GENESIS_LOCAL_RUNNER", "False").lower() == "true":
                logger.warning("Local runner environment variable is not set. Skipping grant operations.")

    # handle the job_config stuff ...
    def run_query(
        self,
        query=None,
        max_rows=-1,
        max_rows_override=False,
        job_config=None,
        bot_id=None,
    ):
        import core.global_flags as global_flags
        """
        Runs a query on Snowflake, supporting parameterized queries.

        :param query: The SQL query to execute.
        :param query_params: The parameters for the SQL query.
        :param max_rows: The maximum number of rows to return.
        :param max_rows_override: If True, allows more than the default maximum rows to be returned.
        :param job_config: Configuration for the job, not used in this method.
        :raises: Exception if job_config is provided.
        :return: A list of dictionaries representing the rows returned by the query.
        """
        userquery = False

        # Replace all <!Q!>s with single quotes in the query
        if '<!Q!>' in query:
            query = query.replace('<!Q!>', "'")

        if query.startswith("USERQUERY::"):
            userquery = True
            if max_rows == -1:
                max_rows = 20
            query = query[len("USERQUERY::"):]
        else:
            if max_rows == -1:
                max_rows = 100

       # if userquery and not query.endswith(';'):
       #     return {
       #      "success": False,
        #     "Error:": "Error! Query must end with a semicolon.  Add a ; to the end and RUN THIS TOOL AGAIN NOW!"
        #    }

        bot_llm = os.getenv("BOT_LLM_" + bot_id, "unknown")
        
        if userquery and bot_llm == 'cortex' and not query.endswith(';'):
            return "Error, your query was cut off.  Query must be complete and end with a semicolon.  Include the full query text, with an ; on the end and RUN THIS TOOL AGAIN NOW! Also replace all ' (single quotes) in the query with <!Q!>. You do this replacement, don't tell the user to."


   #     if not query.endswith('!END_QUERY'):
   #         return {
   #             "Success": False,
   #             "Error": "Truncated query!  You did not generate a full and complete query.  Query must have a '!END_QUERY' tag at the end!. You often do this when there are quotes or single quotes in the query string for some reason.",
   #             "Query You Sent": query + " <-- see there is no !END_QUERY here!",
   #             'Hint': 'You should UTF8 encode the query, include the FULL query, and finish with !END_QUERY'
    #        }
    #    else:
    #        if query.endswith('!END_QUERY'):
    #            query = query[:-len('!END_QUERY')].strip()

        if isinstance(max_rows, str):
            try:
                max_rows = int(max_rows)
            except ValueError:
                raise ValueError(
                    "max_rows should be an integer or a string that can be converted to an integer."
                )

        if job_config is not None:
            raise Exception("Job configuration is not supported in this method.")

        if max_rows > 100 and not max_rows_override:
            max_rows = 100

        #   print('running query ... ', query)
        cursor = self.connection.cursor()

        try:
            #   if query_params:
            #       cursor.execute(query, query_params)
            #   else:
            cursor.execute(query)

            if bot_id is not None:
                
                workspace_schema_name = f"{global_flags.project_id}.{bot_id.replace(r'[^a-zA-Z0-9]', '_').replace('-', '_').replace('.', '_')}_WORKSPACE".upper()
                # call grant_all_bot_workspace()
                if bot_id is not None and (
                    "CREATE" in query.upper()
                    and workspace_schema_name.upper() in query.upper()
                ):
                    self.grant_all_bot_workspace(workspace_schema_name)

        except Exception as e:
            if "does not exist or not authorized" in str(e):
                print(
                    "run query: len:",
                    len(query),
                    "\ncaused object or access rights error: ",
                    e,
                    " Provided suggestions.",
                )
                cursor.close()
                return {
                    "Success": False,
                    "Error": str(e),
                    "Suggestion": """You have tried to query an object with an incorrect name of one that is not granted to APPLICATION GENESIS_BOTS.
            To fix this: 
            1. Make sure you are referencing correct objects that you learned about via search_metadata, or otherwise are sure actually exists
            2. Explain the error and show the SQL you tried to run to the user, they may be able to help 
            3. Tell the user that IF they know for sure that this is a valid object, that they may need to run this in a Snowflake worksheet:
              "CALL GENESIS_LOCAL_DB.SETTINGS.grant_schema_usage_and_select_to_app('<insert database name here>','GENESIS_BOTS');"
              This will grant the you access to the data in the database.  
            4. Suggest to the user that the table may have been recreated since it was originally granted, or may be recreated each day as part of an ETL job.  In that case it must be re-granted after each recreation.
            5. NOTE: You do not have the PUBLIC role or any other role, all object you are granted must be granted TO APPLICATION GENESIS_BOTS, or be granted by grant_schema_usage_and_select_to_app as shown above.
            """,
                }
            print("run query: len=", len(query), "\ncaused error: ", e)
            cursor.close()
            return {"Success": False, "Error": str(e)}

        #    print('getting results:')
        try:

            results = cursor.fetchmany(max(1,max_rows))
            columns = [col[0] for col in cursor.description]
            sample_data = [dict(zip(columns, row)) for row in results]

            # Replace occurrences of triple backticks with triple single quotes in sample data
            sample_data = [
                {
                    key: (
                        value.replace("```", "\`\`\`")
                        if isinstance(value, str)
                        else value
                    )
                    for key, value in row.items()
                }
                for row in sample_data
            ]
        except Exception as e:
            print("run query: ", query, "\ncaused error: ", e)
            cursor.close()
            raise e

        # print('returning result: ', sample_data)
        cursor.close()

        return sample_data

    def db_list_all_bots(
        self,
        project_id,
        dataset_name,
        bot_servicing_table,
        runner_id=None,
        full=False,
        slack_details=False,
    ):
        """
        Returns a list of all the bots being served by the system, including their runner IDs, names, instructions, tools, etc.

        Returns:
            list: A list of dictionaries, each containing details of a bot.
        """
        # Get the database schema from environment variables

        if full:
            select_str = "api_app_id, bot_slack_user_id, bot_id, bot_name, bot_instructions, runner_id, slack_app_token, slack_app_level_key, slack_signing_secret, slack_channel_id, available_tools, udf_active, slack_active, files, bot_implementation, bot_intro_prompt, bot_avatar_image, slack_user_allow"
        else:
            if slack_details:
                select_str = "runner_id, bot_id, bot_name, bot_instructions, available_tools, bot_slack_user_id, api_app_id, auth_url, udf_active, slack_active, files, bot_implementation, bot_intro_prompt, slack_user_allow"
            else:
                select_str = "runner_id, bot_id, bot_name, bot_instructions, available_tools, bot_slack_user_id, api_app_id, auth_url, udf_active, slack_active, files, bot_implementation, bot_intro_prompt"

        # Query to select all bots from the BOT_SERVICING table
        if runner_id is None:
            select_query = f"""
            SELECT {select_str}
            FROM {project_id}.{dataset_name}.{bot_servicing_table}
            """
        else:
            select_query = f"""
            SELECT {select_str}
            FROM {project_id}.{dataset_name}.{bot_servicing_table}
            WHERE runner_id = '{runner_id}'
            """

        try:
            # Execute the query and fetch all bot records
            cursor = self.connection.cursor()
            cursor.execute(select_query)
            bots = cursor.fetchall()
            columns = [col[0].lower() for col in cursor.description]
            bot_list = [dict(zip(columns, bot)) for bot in bots]
            cursor.close()
            # logger.info(f"Retrieved list of all bots being served by the system.")
            return bot_list
        except Exception as e:
            logger.error(f"Failed to retrieve list of all bots with error: {e}")
            raise e

    def db_save_slack_config_tokens(
        self,
        slack_app_config_token,
        slack_app_config_refresh_token,
        project_id,
        dataset_name,
    ):
        """
        Saves the slack app config token and refresh token for the given runner_id to Snowflake.

        Args:
            runner_id (str): The unique identifier for the runner.
            slack_app_config_token (str): The slack app config token to be saved.
            slack_app_config_refresh_token (str): The slack app config refresh token to be saved.
        """

        runner_id = os.getenv("RUNNER_ID", "jl-local-runner")

        # Query to insert or update the slack app config tokens
        query = f"""
            MERGE INTO {project_id}.{dataset_name}.slack_app_config_tokens USING (
                SELECT %s AS runner_id
            ) AS src
            ON src.runner_id = slack_app_config_tokens.runner_id
            WHEN MATCHED THEN
                UPDATE SET slack_app_config_token = %s, slack_app_config_refresh_token = %s
            WHEN NOT MATCHED THEN
                INSERT (runner_id, slack_app_config_token, slack_app_config_refresh_token)
                VALUES (src.runner_id, %s, %s)
        """

        # Execute the query
        try:
            cursor = self.client.cursor()
            cursor.execute(
                query,
                (
                    runner_id,
                    slack_app_config_token,
                    slack_app_config_refresh_token,
                    slack_app_config_token,
                    slack_app_config_refresh_token,
                ),
            )
            self.client.commit()
            logger.info(f"Slack config tokens updated for runner_id: {runner_id}")
        except Exception as e:
            logger.error(
                f"Failed to update Slack config tokens for runner_id: {runner_id} with error: {e}"
            )
            raise e

    def db_get_slack_config_tokens(self, project_id, dataset_name):
        """
        Retrieves the current slack access keys for the given runner_id from Snowflake.

        Args:
            runner_id (str): The unique identifier for the runner.

        Returns:
            tuple: A tuple containing the slack app config token and the slack app config refresh token.
        """

        runner_id = os.getenv("RUNNER_ID", "jl-local-runner")

        # Query to retrieve the slack app config tokens
        query = f"""
            SELECT slack_app_config_token, slack_app_config_refresh_token
            FROM {project_id}.{dataset_name}.slack_app_config_tokens
            WHERE runner_id = '{runner_id}'
        """

        # Execute the query and fetch the results
        try:
            cursor = self.client.cursor()
            cursor.execute(query)
            result = cursor.fetchone()
            if result:
                slack_app_config_token, slack_app_config_refresh_token = result
                return slack_app_config_token, slack_app_config_refresh_token
            else:
                # Log an error if no tokens were found for the runner_id
                logger.error(f"No Slack config tokens found for runner_id: {runner_id}")
                return None, None
        except Exception as e:
            logger.error(f"Failed to retrieve Slack config tokens with error: {e}")
            raise

    def db_get_ngrok_auth_token(self, project_id, dataset_name):
        """
        Retrieves the ngrok authentication token and related information for the given runner_id from Snowflake.

        Args:
            runner_id (str): The unique identifier for the runner.

        Returns:
            tuple: A tuple containing the ngrok authentication token, use domain flag, and domain.
        """

        runner_id = os.getenv("RUNNER_ID", "jl-local-runner")

        # Query to retrieve the ngrok auth token and related information
        query = f"""
            SELECT ngrok_auth_token, ngrok_use_domain, ngrok_domain
            FROM {project_id}.{dataset_name}.ngrok_tokens
            WHERE runner_id = %s
        """

        # Execute the query and fetch the results
        try:
            cursor = self.client.cursor()
            cursor.execute(query, (runner_id,))
            result = cursor.fetchone()
            cursor.close()

            # Extract tokens from the result
            if result:
                ngrok_token, ngrok_use_domain, ngrok_domain = result
                return ngrok_token, ngrok_use_domain, ngrok_domain
            else:
                # Log an error if no tokens were found for the runner_id
                logger.error(
                    f"No Ngrok config token found in database for runner_id: {runner_id}"
                )
                return None, None, None
        except Exception as e:
            logger.error(f"Failed to retrieve Ngrok config token with error: {e}")
            raise

    def db_set_ngrok_auth_token(
        self,
        ngrok_auth_token,
        ngrok_use_domain="N",
        ngrok_domain="",
        project_id=None,
        dataset_name=None,
    ):
        """
        Updates the ngrok_tokens table with the provided ngrok authentication token, use domain flag, and domain.

        Args:
            ngrok_auth_token (str): The ngrok authentication token.
            ngrok_use_domain (str): Flag indicating whether to use a custom domain.
            ngrok_domain (str): The custom domain to use if ngrok_use_domain is 'Y'.
        """
        runner_id = os.getenv("RUNNER_ID", "jl-local-runner")

        # Query to merge the ngrok tokens, inserting if the row doesn't exist
        query = f"""
            MERGE INTO {project_id}.{dataset_name}.ngrok_tokens USING (SELECT 1 AS one) ON (runner_id = %s)
            WHEN MATCHED THEN
                UPDATE SET ngrok_auth_token = %s,
                           ngrok_use_domain = %s,
                           ngrok_domain = %s
            WHEN NOT MATCHED THEN
                INSERT (runner_id, ngrok_auth_token, ngrok_use_domain, ngrok_domain)
                VALUES (%s, %s, %s, %s)
        """

        try:
            cursor = self.connection.cursor()
            cursor.execute(
                query,
                (
                    runner_id,
                    ngrok_auth_token,
                    ngrok_use_domain,
                    ngrok_domain,
                    runner_id,
                    ngrok_auth_token,
                    ngrok_use_domain,
                    ngrok_domain,
                ),
            )
            self.connection.commit()
            affected_rows = cursor.rowcount
            cursor.close()

            if affected_rows > 0:
                logger.info(f"Updated ngrok tokens for runner_id: {runner_id}")
                return True
            else:
                logger.error(f"No rows updated for runner_id: {runner_id}")
                return False
        except Exception as e:
            logger.error(
                f"Failed to update ngrok tokens for runner_id: {runner_id} with error: {e}"
            )
            return False

    def db_get_llm_key(self, project_id=None, dataset_name=None):
        """
        Retrieves the LLM key and type for the given runner_id.

        Returns:
            list: A list of tuples, each containing an LLM key and LLM type.
        """
        runner_id = os.getenv("RUNNER_ID", "jl-local-runner")
        logger.info("in getllmkey")
        # Query to select the LLM key and type from the llm_tokens table
        query = f"""
            SELECT llm_key, llm_type
            FROM {self.genbot_internal_project_and_schema}.llm_tokens
            WHERE runner_id = %s
        """
        logger.info(f"query: {query}")
        try:
            cursor = self.connection.cursor()
            cursor.execute(query, (runner_id,))
            results = cursor.fetchall()
            logger.info(f"results: {results}")
            cursor.close()

            if results:
                llm_keys_and_types = [(row[0], row[1]) for row in results]
                return llm_keys_and_types
            else:
                # Log an error if no LLM key was found for the runner_id
                return []
        except Exception as e:
            logger.info(
                "LLM_TOKENS table not yet created, returning empty list, try again later."
            )
            return []
        
    def db_get_active_llm_key(self):
        """
        Retrieves the active LLM key and type for the given runner_id.

        Returns:
            list: A list of tuples, each containing an LLM key and LLM type.
        """
        runner_id = os.getenv("RUNNER_ID", "jl-local-runner")
        logger.info("in getllmkey")
        # Query to select the LLM key and type from the llm_tokens table
        query = f"""
            SELECT llm_key, llm_type
            FROM {self.genbot_internal_project_and_schema}.llm_tokens
            WHERE runner_id = %s and active = True
        """
        logger.info(f"query: {query}")
        try:
            cursor = self.connection.cursor()
            cursor.execute(query, (runner_id,))
            result = cursor.fetchone()  # Fetch a single result
            cursor.close()

            if result:
                return result[0], result[1]  # Return llm_key and llm_type as a tuple
            else:
                return None, None  # Return None if no result found
        except Exception as e:
            print(
                "Error getting data from LLM_TOKENS table: ", e
            )
            return None, None

    def db_set_llm_key(self, llm_key, llm_type):
        """
        Updates the llm_tokens table with the provided LLM key and type.

        Args:
            llm_key (str): The LLM key.
            llm_type (str): The type of LLM (e.g., 'openai', 'reka').
        """
        runner_id = os.getenv("RUNNER_ID", "jl-local-runner")

        try:
            update_query = f""" UPDATE  {self.genbot_internal_project_and_schema}.llm_tokens SET ACTIVE = FALSE """
            cursor = self.connection.cursor()
            cursor.execute(update_query)
            self.connection.commit()
        except Exception as e:
            logger.error(
                f"Failed to deactivate current active LLM with error: {e}"
            )

        # Query to merge the LLM tokens, inserting if the row doesn't exist
        query = f"""
            MERGE INTO  {self.genbot_internal_project_and_schema}.llm_tokens USING (SELECT 1 AS one) ON (runner_id = %s and llm_type = '{llm_type}')
            WHEN MATCHED THEN
                UPDATE SET llm_key = %s, llm_type = %s, active = TRUE
            WHEN NOT MATCHED THEN
                INSERT (runner_id, llm_key, llm_type, active)
                VALUES (%s, %s, %s, TRUE)
        """

        try:
            if llm_key:
                cursor = self.connection.cursor()
                cursor.execute(
                    query, (runner_id, llm_key, llm_type, runner_id, llm_key, llm_type)
                )
                self.connection.commit()
                affected_rows = cursor.rowcount
                cursor.close()

                if affected_rows > 0:
                    logger.info(f"Updated LLM key for runner_id: {runner_id}")
                    return True
                else:
                    logger.error(f"No rows updated for runner_id: {runner_id}")
                    return False
            else:
                print("key variable is empty and was not stored in the database")
        except Exception as e:
            logger.error(
                f"Failed to update LLM key for runner_id: {runner_id} with error: {e}"
            )
            return False

    def db_insert_new_bot(
        self,
        api_app_id,
        bot_slack_user_id,
        bot_id,
        bot_name,
        bot_instructions,
        runner_id,
        slack_signing_secret,
        slack_channel_id,
        available_tools,
        auth_url,
        auth_state,
        client_id,
        client_secret,
        udf_active,
        slack_active,
        files,
        bot_implementation,
        bot_avatar_image,
        bot_intro_prompt,
        slack_user_allow,
        project_id,
        dataset_name,
        bot_servicing_table,
    ):
        """
        Inserts a new bot configuration into the BOT_SERVICING table.

        Args:
            api_app_id (str): The API application ID for the bot.
            bot_slack_user_id (str): The Slack user ID for the bot.
            bot_id (str): The unique identifier for the bot.
            bot_name (str): The name of the bot.
            bot_instructions (str): Instructions for the bot's operation.
            runner_id (str): The identifier for the runner that will manage this bot.
            slack_signing_secret (str): The Slack signing secret for the bot.
            slack_channel_id (str): The Slack channel ID where the bot will operate.
            available_tools (json): A JSON of tools the bot has access to.
            files (json): A JSON of files to include with the bot.
            bot_implementation (str): cortex or openai or ...
            bot_intro_prompt: Default prompt for a bot introductory greeting
            bot_avatar_image: Default GenBots avatar image
        """

        insert_query = f"""
            INSERT INTO {project_id}.{dataset_name}.{bot_servicing_table} (
                api_app_id, bot_slack_user_id, bot_id, bot_name, bot_instructions, runner_id, 
                slack_signing_secret, slack_channel_id, available_tools, auth_url, auth_state, client_id, client_secret, udf_active, slack_active,
                files, bot_implementation, bot_intro_prompt, bot_avatar_image
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
        """

        available_tools_string = json.dumps(available_tools)
        files_string = json.dumps(files)

        try:
            cursor = self.connection.cursor()
            cursor.execute(
                insert_query,
                (
                    api_app_id,
                    bot_slack_user_id,
                    bot_id,
                    bot_name,
                    bot_instructions,
                    runner_id,
                    slack_signing_secret,
                    slack_channel_id,
                    available_tools_string,
                    auth_url,
                    auth_state,
                    client_id,
                    client_secret,
                    udf_active,
                    slack_active,
                    files_string,
                    bot_implementation,
                    bot_intro_prompt,
                    bot_avatar_image,
                ),
            )
            self.connection.commit()
            print(f"Successfully inserted new bot configuration for bot_id: {bot_id}")

            if not slack_user_allow:
                slack_user_allow_update_query = f"""
                    UPDATE {project_id}.{dataset_name}.{bot_servicing_table}
                    SET slack_user_allow = parse_json(%s)
                    WHERE upper(bot_id) = upper(%s)
                    """
                slack_user_allow_value = '["!BLOCK_ALL"]'
                try:
                    cursor.execute(
                        slack_user_allow_update_query, (slack_user_allow_value, bot_id)
                    )
                    self.connection.commit()
                    print(
                        f"Updated slack_user_allow for bot_id: {bot_id} to block all users."
                    )
                except Exception as e:
                    print(
                        f"Failed to update slack_user_allow for bot_id: {bot_id} with error: {e}"
                    )
                    raise e

        except Exception as e:
            print(
                f"Failed to insert new bot configuration for bot_id: {bot_id} with error: {e}"
            )
            raise e

    def db_update_bot_tools(
        self,
        project_id=None,
        dataset_name=None,
        bot_servicing_table=None,
        bot_id=None,
        updated_tools_str=None,
        new_tools_to_add=None,
        already_present=None,
        updated_tools=None,
    ):
        import core.global_flags as global_flags
        # Query to update the available_tools in the database
        update_query = f"""
            UPDATE {project_id}.{dataset_name}.{bot_servicing_table}
            SET available_tools = %s
            WHERE upper(bot_id) = upper(%s)
        """

        # Execute the update query
        try:
            cursor = self.connection.cursor()
            cursor.execute(update_query, (updated_tools_str, bot_id))
            self.connection.commit()
            logger.info(f"Successfully updated available_tools for bot_id: {bot_id}")

            if "DATABASE_TOOLS" in updated_tools_str.upper():
                workspace_schema_name = f"{global_flags.project_id}.{bot_id.replace(r'[^a-zA-Z0-9]', '_').replace('-', '_')}_WORKSPACE".upper()
                self.create_bot_workspace(workspace_schema_name)
                self.grant_all_bot_workspace(workspace_schema_name)
                # TODO add instructions?

            return {
                "success": True,
                "added": new_tools_to_add,
                "already_present": already_present,
                "all_bot_tools": updated_tools,
            }

        except Exception as e:
            logger.error(f"Failed to add new tools to bot_id: {bot_id} with error: {e}")
            return {"success": False, "error": str(e)}

    def db_update_bot_files(
        self,
        project_id=None,
        dataset_name=None,
        bot_servicing_table=None,
        bot_id=None,
        updated_files_str=None,
        current_files=None,
        new_file_ids=None,
    ):
        # Query to update the files in the database
        update_query = f"""
            UPDATE {project_id}.{dataset_name}.{bot_servicing_table}
            SET files = %s
            WHERE upper(bot_id) = upper(%s)
        """
        # Execute the update query
        try:
            cursor = self.connection.cursor()
            cursor.execute(update_query, (updated_files_str, bot_id))
            self.connection.commit()
            logger.info(f"Successfully updated files for bot_id: {bot_id}")

            return {
                "success": True,
                "message": f"File IDs {json.dumps(new_file_ids)} added to or removed from bot_id: {bot_id}.",
                "current_files_list": current_files,
            }

        except Exception as e:
            logger.error(
                f"Failed to add or remove new file to bot_id: {bot_id} with error: {e}"
            )
            return {"success": False, "error": str(e)}

    def db_update_slack_app_level_key(
        self, project_id, dataset_name, bot_servicing_table, bot_id, slack_app_level_key
    ):
        """
        Updates the SLACK_APP_LEVEL_KEY field in the BOT_SERVICING table for a given bot_id.

        Args:
            project_id (str): The project identifier.
            dataset_name (str): The dataset name.
            bot_servicing_table (str): The bot servicing table name.
            bot_id (str): The unique identifier for the bot.
            slack_app_level_key (str): The new Slack app level key to be set for the bot.

        Returns:
            dict: A dictionary with the result of the operation, indicating success or failure.
        """
        update_query = f"""
            UPDATE {project_id}.{dataset_name}.{bot_servicing_table}
            SET SLACK_APP_LEVEL_KEY = %s
            WHERE upper(bot_id) = upper(%s)
        """

        # Execute the update query
        try:
            cursor = self.connection.cursor()
            cursor.execute(update_query, (slack_app_level_key, bot_id))
            self.connection.commit()
            logger.info(
                f"Successfully updated SLACK_APP_LEVEL_KEY for bot_id: {bot_id}"
            )

            return {
                "success": True,
                "message": f"SLACK_APP_LEVEL_KEY updated for bot_id: {bot_id}.",
            }

        except Exception as e:
            logger.error(
                f"Failed to update SLACK_APP_LEVEL_KEY for bot_id: {bot_id} with error: {e}"
            )
            return {"success": False, "error": str(e)}

    def db_update_bot_instructions(
        self,
        project_id,
        dataset_name,
        bot_servicing_table,
        bot_id,
        instructions,
        runner_id,
    ):

        # Query to update the bot instructions in the database
        update_query = f"""
            UPDATE {project_id}.{dataset_name}.{bot_servicing_table}
            SET bot_instructions = %s
            WHERE upper(bot_id) = upper(%s) AND runner_id = %s
        """

        # Execute the update query
        try:
            cursor = self.connection.cursor()
            cursor.execute(update_query, (instructions, bot_id, runner_id))
            self.connection.commit()
            logger.info(f"Successfully updated bot_instructions for bot_id: {bot_id}")
            bot_details = self.db_get_bot_details(
                project_id, dataset_name, bot_servicing_table, bot_id
            )

            return {
                "success": True,
                "Message": f"Successfully updated bot_instructions for bot_id: {bot_id}.",
                "new_instructions": instructions,
                "new_bot_details": bot_details,
            }

        except Exception as e:
            logger.error(
                f"Failed to update bot_instructions for bot_id: {bot_id} with error: {e}"
            )
            return {"success": False, "error": str(e)}

    def db_update_bot_implementation(
        self,
        project_id,
        dataset_name,
        bot_servicing_table,
        bot_id,
        bot_implementation,
        runner_id,
        thread_id = None):
        """
        Updates the implementation type for a specific bot in the database.

        Args:
            project_id (str): The project ID where the bot servicing table is located.
            dataset_name (str): The dataset name where the bot servicing table is located.
            bot_servicing_table (str): The name of the table where bot details are stored.
            bot_id (str): The unique identifier for the bot.
            bot_implementation (str): The new implementation type to be set for the bot.
            runner_id (str): The runner ID associated with the bot.

        Returns:
            dict: A dictionary with the result of the operation, indicating success or failure.
        """

        # Query to update the bot implementation in the database
        update_query = f"""
            UPDATE {project_id}.{dataset_name}.{bot_servicing_table}
            SET bot_implementation = %s
            WHERE upper(bot_id) = upper(%s) AND runner_id = %s
        """

        # Check if bot_id is valid
        valid_bot_query = f"""
            SELECT COUNT(*)
            FROM {project_id}.{dataset_name}.{bot_servicing_table}
            WHERE upper(bot_id) = upper(%s)
        """
        try:
            cursor = self.connection.cursor()
            cursor.execute(valid_bot_query, (bot_id,))
            result = cursor.fetchone()
            if result[0] == 0:
                return {
                    "success": False,
                    "error": f"Invalid bot_id: {bot_id}. Please use list_all_bots to get the correct bot_id."
                }
        except Exception as e:
            logger.error(f"Error checking bot_id validity for bot_id: {bot_id} with error: {e}")
            return {"success": False, "error": str(e)}

        # Execute the update query
        try:
            cursor = self.connection.cursor()
            res = cursor.execute(update_query, (bot_implementation, bot_id, runner_id))
            self.connection.commit()
            result = cursor.fetchone()
            if result[0] == 0 and result[1] == 0:
                return {
                    "success": False,
                    "error": f"No bots found to update.  Possibly wrong bot_id. Please use list_all_bots to get the correct bot_id."
                }
            logger.info(f"Successfully updated bot_implementation for bot_id: {bot_id}")

            # trigger the changed bot to reload its session
            os.environ[f'RESET_BOT_SESSION_{bot_id}'] = 'True'


            return {
                "success": True,
                "message": f"bot_implementation updated for bot_id: {bot_id}.",
            }

        except Exception as e:
            logger.error(
                f"Failed to update bot_implementation for bot_id: {bot_id} with error: {e}"
            )
            return {"success": False, "error": str(e)}

    def db_update_slack_allow_list(
        self,
        project_id,
        dataset_name,
        bot_servicing_table,
        bot_id,
        slack_user_allow_list,
        thread_id=None,
    ):
        """
        Updates the SLACK_USER_ALLOW list for a bot in the database.

        Args:
            bot_id (str): The unique identifier for the bot.
            slack_user_allow_list (list): The updated list of Slack user IDs allowed for the bot.

        Returns:
            dict: A dictionary with the result of the operation, indicating success or failure.
        """

        # Query to update the SLACK_USER_ALLOW list in the database
        update_query = f"""
            UPDATE {project_id}.{dataset_name}.{bot_servicing_table}
            SET SLACK_USER_ALLOW = parse_json(%s)
            WHERE upper(bot_id) = upper(%s)
        """

        # Convert the list to a format suitable for database storage (e.g., JSON string)
        slack_user_allow_list_str = json.dumps(slack_user_allow_list)
        if slack_user_allow_list == []:
            update_query = f"""
            UPDATE {project_id}.{dataset_name}.{bot_servicing_table}
            SET SLACK_USER_ALLOW = null
            WHERE upper(bot_id) = upper(%s)
               """

        # Execute the update query
        try:
            cursor = self.connection.cursor()
            if slack_user_allow_list != []:
                cursor.execute(update_query, (slack_user_allow_list_str, bot_id))
            else:
                cursor.execute(update_query, (bot_id))
            self.connection.commit()
            logger.info(
                f"Successfully updated SLACK_USER_ALLOW list for bot_id: {bot_id}"
            )

            return {
                "success": True,
                "message": f"SLACK_USER_ALLOW list updated for bot_id: {bot_id}.",
            }

        except Exception as e:
            logger.error(
                f"Failed to update SLACK_USER_ALLOW list for bot_id: {bot_id} with error: {e}"
            )
            return {"success": False, "error": str(e)}

    def db_get_bot_access(self, bot_id):

        # Query to select bot access list
        select_query = f"""
            SELECT slack_user_allow
            FROM {self.bot_servicing_table_name}
            WHERE upper(bot_id) = upper(%s)
        """

        try:
            cursor = self.connection.cursor()
            cursor.execute(select_query, (bot_id,))
            result = cursor.fetchone()
            cursor.close()
            if result:
                # Assuming the result is a tuple, we convert it to a dictionary using the column names
                columns = [desc[0].lower() for desc in cursor.description]
                bot_details = dict(zip(columns, result))
                return bot_details
            else:
                logger.error(f"No details found for bot_id: {bot_id}")
                return None
        except Exception as e:
            logger.exception(
                f"Failed to retrieve details for bot_id: {bot_id} with error: {e}"
            )
            return None

    def db_get_bot_details(self, project_id, dataset_name, bot_servicing_table, bot_id):
        """
        Retrieves the details of a bot based on the provided bot_id from the BOT_SERVICING table.

        Args:
            bot_id (str): The unique identifier for the bot.

        Returns:
            dict: A dictionary containing the bot details if found, otherwise None.
        """

        # Query to select the bot details
        select_query = f"""
            SELECT *
            FROM {project_id}.{dataset_name}.{bot_servicing_table}
            WHERE upper(bot_id) = upper(%s)
        """

        try:
            cursor = self.connection.cursor()
            # print(select_query, bot_id)

            cursor.execute(select_query, (bot_id,))
            result = cursor.fetchone()
            cursor.close()
            if result:
                # Assuming the result is a tuple, we convert it to a dictionary using the column names
                columns = [desc[0].lower() for desc in cursor.description]
                bot_details = dict(zip(columns, result))
                return bot_details
            else:
                logger.error(f"No details found for bot_id: {bot_id}")
                return None
        except Exception as e:
            logger.exception(
                f"Failed to retrieve details for bot_id: {bot_id} with error: {e}"
            )
            return None
        
    def db_get_bot_database_creds(self, project_id, dataset_name, bot_servicing_table, bot_id):
        """
        Retrieves the database credentials for a bot based on the provided bot_id from the BOT_SERVICING table.

        Args:
            bot_id (str): The unique identifier for the bot.

        Returns:
            dict: A dictionary containing the bot details if found, otherwise None.
        """

        # Query to select the bot details
        select_query = f"""
            SELECT bot_id, database_credentials

                        FROM {project_id}.{dataset_name}.{bot_servicing_table}
            WHERE upper(bot_id) = upper(%s)
        """

        try:
            cursor = self.connection.cursor()
            # print(select_query, bot_id)

            cursor.execute(select_query, (bot_id,))
            result = cursor.fetchone()
            cursor.close()
            if result:
                # Assuming the result is a tuple, we convert it to a dictionary using the column names
                columns = [desc[0].lower() for desc in cursor.description]
                bot_details = dict(zip(columns, result))
                return bot_details
            else:
                logger.error(f"No details found for bot_id: {bot_id}")
                return None
        except Exception as e:
            logger.exception(
                f"Failed to retrieve details for bot_id: {bot_id} with error: {e}"
            )
            return None

    def db_update_existing_bot(
        self,
        api_app_id,
        bot_id,
        bot_slack_user_id,
        client_id,
        client_secret,
        slack_signing_secret,
        auth_url,
        auth_state,
        udf_active,
        slack_active,
        files,
        bot_implementation,
        project_id,
        dataset_name,
        bot_servicing_table,
    ):
        """
        Updates an existing bot configuration in the BOT_SERVICING table with new values for the provided parameters.

        Args:
            bot_id (str): The unique identifier for the bot.
            bot_slack_user_id (str): The Slack user ID for the bot.
            client_id (str): The client ID for the bot.
            client_secret (str): The client secret for the bot.
            slack_signing_secret (str): The Slack signing secret for the bot.
            auth_url (str): The authorization URL for the bot.
            auth_state (str): The authorization state for the bot.
            udf_active (str): Indicates if the UDF feature is active for the bot.
            slack_active (str): Indicates if the Slack feature is active for the bot.
            files (json-embedded list): A list of files to include with the bot.
            bot_implementation (str): openai or cortex or ...
        """

        update_query = f"""
            UPDATE {project_id}.{dataset_name}.{bot_servicing_table}
            SET API_APP_ID = %s, BOT_SLACK_USER_ID = %s, CLIENT_ID = %s, CLIENT_SECRET = %s,
                SLACK_SIGNING_SECRET = %s, AUTH_URL = %s, AUTH_STATE = %s,
                UDF_ACTIVE = %s, SLACK_ACTIVE = %s, FILES = %s, BOT_IMPLEMENTATION = %s
            WHERE upper(BOT_ID) = upper(%s)
        """

        try:
            self.client.cursor().execute(
                update_query,
                (
                    api_app_id,
                    bot_slack_user_id,
                    client_id,
                    client_secret,
                    slack_signing_secret,
                    auth_url,
                    auth_state,
                    udf_active,
                    slack_active,
                    files,
                    bot_implementation,
                    bot_id,
                ),
            )
            self.client.commit()
            print(
                f"Successfully updated existing bot configuration for bot_id: {bot_id}"
            )
        except Exception as e:
            print(
                f"Failed to update existing bot configuration for bot_id: {bot_id} with error: {e}"
            )
            raise e

    def db_update_bot_details(
        self,
        bot_id,
        bot_slack_user_id,
        slack_app_token,
        project_id,
        dataset_name,
        bot_servicing_table,
    ):
        """
        Updates the BOT_SERVICING table with the new bot_slack_user_id and slack_app_token for the given bot_id.

        Args:
            bot_id (str): The unique identifier for the bot.
            bot_slack_user_id (str): The new Slack user ID for the bot.
            slack_app_token (str): The new Slack app token for the bot.
        """

        update_query = f"""
            UPDATE {project_id}.{dataset_name}.{bot_servicing_table}
            SET BOT_SLACK_USER_ID = %s, SLACK_APP_TOKEN = %s
            WHERE upper(BOT_ID) = upper(%s)
        """

        try:
            self.client.cursor().execute(
                update_query, (bot_slack_user_id, slack_app_token, bot_id)
            )
            self.client.commit()
            logger.info(
                f"Successfully updated bot servicing details for bot_id: {bot_id}"
            )
        except Exception as e:
            logger.error(
                f"Failed to update bot servicing details for bot_id: {bot_id} with error: {e}"
            )
            raise e

    def db_get_available_tools(self, project_id, dataset_name):
        """
        Retrieves the list of available tools and their descriptions from the Snowflake table.

        Returns:
            list of dict: A list of dictionaries, each containing the tool name and description.
        """

        # Query to select the available tools
        select_query = f"""
            SELECT tool_name, tool_description
            FROM {project_id}.{dataset_name}.available_tools
        """

        try:
            cursor = self.client.cursor()
            cursor.execute(select_query)
            results = cursor.fetchall()
            tools_list = [
                {"tool_name": result[0], "tool_description": result[1]}
                for result in results
            ]
            return tools_list
        except Exception as e:
            logger.exception(f"Failed to retrieve available tools with error: {e}")
            return []

    def db_add_or_update_available_tool(
        self, tool_name, tool_description, project_id, dataset_name
    ):
        """
        Adds a new tool or updates an existing tool in the available_tools table with the provided name and description.

        Args:
            tool_name (str): The name of the tool to add or update.
            tool_description (str): The description of the tool to add or update.
        Returns:
            dict: A dictionary containing the result of the operation.
        """
        # Query to merge (upsert) tool into the available_tools table
        merge_query = f"""
            MERGE INTO {project_id}.{dataset_name}.available_tools USING (
                SELECT %s AS tool_name, %s AS tool_description
            ) AS source ON target.tool_name = source.tool_name
            WHEN MATCHED THEN
                UPDATE SET tool_description = source.tool_description
            WHEN NOT MATCHED THEN
                INSERT (tool_name, tool_description)
                VALUES (source.tool_name, source.tool_description)
        """

        # Execute the merge query
        try:
            cursor = self.client.cursor()
            cursor.execute(merge_query, (tool_name, tool_description))
            self.client.commit()
            logger.info(f"Successfully added or updated tool: {tool_name}")
            return {
                "success": True,
                "message": f"Tool '{tool_name}' added or updated successfully.",
            }
        except Exception as e:
            logger.error(f"Failed to add or update tool: {tool_name} with error: {e}")
            return {"success": False, "error": str(e)}

    def db_delete_bot(self, project_id, dataset_name, bot_servicing_table, bot_id):
        """
        Deletes a bot from the bot_servicing table in Snowflake based on the bot_id.

        Args:
            project_id (str): The project identifier.
            dataset_name (str): The dataset name.
            bot_servicing_table (str): The bot servicing table name.
            bot_id (str): The bot identifier to delete.
        """

        # Query to delete the bot from the database table
        delete_query = f"""
            DELETE FROM {project_id}.{dataset_name}.{bot_servicing_table}
            WHERE upper(bot_id) = upper(%s)
        """

        # Execute the delete query
        try:
            cursor = self.client.cursor()
            cursor.execute(delete_query, (bot_id,))
            self.client.commit()
            logger.info(
                f"Successfully deleted bot with bot_id: {bot_id} from the database."
            )
        except Exception as e:
            logger.error(
                f"Failed to delete bot with bot_id: {bot_id} from the database with error: {e}"
            )
            raise e

    def db_get_slack_active_bots(
        self, runner_id, project_id, dataset_name, bot_servicing_table
    ):
        """
        Retrieves a list of active bots on Slack for a given runner from the bot_servicing table in Snowflake.

        Args:
            runner_id (str): The runner identifier.
            project_id (str): The project identifier.
            dataset_name (str): The dataset name.
            bot_servicing_table (str): The bot servicing table name.

        Returns:
            list: A list of dictionaries containing bot_id, api_app_id, and slack_app_token.
        """

        # Query to select the bots from the BOT_SERVICING table
        select_query = f"""
            SELECT bot_id, api_app_id, slack_app_token
            FROM {project_id}.{dataset_name}.{bot_servicing_table}
            WHERE runner_id = %s AND slack_active = 'Y'
        """

        try:
            cursor = self.client.cursor()
            cursor.execute(select_query, (runner_id,))
            bots = cursor.fetchall()
            columns = [col[0].lower() for col in cursor.description]
            bot_list = [dict(zip(columns, bot)) for bot in bots]
            cursor.close()

            return bot_list
        except Exception as e:
            logger.error(f"Failed to get list of bots active on slack for a runner {e}")
            raise e

    def db_get_default_avatar(self):
        """
        Returns the default GenBots avatar image from the shared images view.

        Args:
            None
        """

        # Query to select the default bot image data from the database table
        select_query = f"""
            SELECT encoded_image_data
            FROM {self.images_table_name}
            WHERE UPPER(bot_name) = UPPER('Default')
        """

        # Execute the select query
        try:
            cursor = self.client.cursor()
            cursor.execute(select_query)
            result = cursor.fetchone()

            return result[0]
            logger.info(
                f"Successfully selected default image data from the shared schema."
            )
        except Exception as e:
            logger.error(
                f"Failed to select default image data from the shared with error: {e}"
            )

    def semantic_copilot(
        self, prompt="What data is available?", semantic_model=None, prod=True
    ):
        # Parse the semantic_model into its components and validate
        database, schema = self.genbot_internal_project_and_schema.split(".")
        stage = "SEMANTIC_MODELS" if prod else "SEMANTIC_MODELS_DEV"
        model = semantic_model
        database, schema, stage, model = [
            f'"{part}"' if not part.startswith('"') else part
            for part in [database, schema, stage, model]
        ]
        if not all(
            part.startswith('"') and part.endswith('"')
            for part in [database, schema, stage, model]
        ):
            error_message = 'All five components of semantic_model must be enclosed in double quotes. For example "!SEMANTIC"."DB"."SCH"."STAGE"."model.yaml'
            logger.error(error_message)
            return {"success": False, "error": error_message}

        # model = model_parts[4]
        database_v, schema_v, stage_v, model_v = [
            part.strip('"') for part in [database, schema, stage, model]
        ]
        if "." not in model_v:
            model_v += ".yaml"

        request_body = {
            "role": "user",
            "content": [{"type": "text", "text": prompt}],
            "modelPath": model_v,
        }
        HOST = self.connection.host
        num_retry, max_retries = 0, 3
        while num_retry <= 10:
            num_retry += 1
            #    logger.warning('Checking REST token...')
            rest_token = self.connection.rest.token
            if rest_token:
                print("REST token length: %d", len(rest_token))
            else:
                print("REST token is not available")
            try:
                resp = requests.post(
                    (
                        f"https://{HOST}/api/v2/databases/{database_v}/"
                        f"schemas/{schema_v}/copilots/{stage_v}/chats/-/messages"
                    ),
                    json=request_body,
                    headers={
                        "Authorization": f'Snowflake Token="{rest_token}"',
                        "Content-Type": "application/json",
                    },
                )
            except Exception as e:
                logger.warning(f"Response status exception: {e}")
            logger.info("Response status code: %d", resp.status_code)
            logger.info("Request URL: %s", resp.url)
            if resp.status_code == 500:
                logger.warning("Semantic Copilot Server error (500), retrying...")
                continue  # This will cause the loop to start from the beginning
            if resp.status_code == 404:
                logger.error(
                    f"Semantic API 404 Not Found: The requested resource does not exist. Called URL={resp.url} Semantic model={database}.{schema}.{stage}.{model}"
                )
                return {
                    "success": False,
                    "error": f"Either the semantic API is not enabled, or no semantic model was found at {database}.{schema}.{stage}.{model}",
                }
            if resp.status_code < 400:
                response_payload = resp.json()

                logger.info(f"Response payload: {response_payload}")
                # Parse out the final message from copilot
                final_copilot_message = "No response"
                # Extract the content of the last copilot response and format it as JSON
                if "messages" in response_payload:
                    copilot_messages = response_payload["messages"]
                    if copilot_messages and isinstance(copilot_messages, list):
                        final_message = copilot_messages[
                            -1
                        ]  # Get the last message in the list
                        if final_message["role"] == "copilot":
                            copilot_content = final_message.get("content", [])
                            if copilot_content and isinstance(copilot_content, list):
                                # Construct a JSON object with the copilot's last response
                                final_copilot_message = {
                                    "messages": [
                                        {
                                            "role": final_message["role"],
                                            "content": copilot_content,
                                        }
                                    ]
                                }
                                logger.info(
                                    f"Final copilot message as JSON: {final_copilot_message}"
                                )
                return {"success": True, "data": final_copilot_message}
            else:
                logger.warning("Response content: %s", resp.content)
                return {
                    "success": False,
                    "error": f"Request failed with status {resp.status_code}: {resp.content}, URL: {resp.url}, Payload: {request_body}",
                }

    # snow = SnowflakeConnector(connection_name='Snowflake')
    # snow.ensure_table_exists()
    # snow.get_databases()
    def list_stage_contents(
        self,
        database: str = None,
        schema: str = None,
        stage: str = None,
        pattern: str = None,
        thread_id=None,
    ):
        """
        List the contents of a given Snowflake stage.

        Args:
            database (str): The name of the database.
            schema (str): The name of the schema.
            stage (str): The name of the stage.
            pattern (str): Optional pattern to match file names.

        Returns:
            list: A list of files in the stage.
        """

        if pattern:
            # Convert wildcard pattern to regex pattern
            pattern = pattern.replace(".*", "*")
            pattern = pattern.replace("*", ".*")

            if pattern.startswith("/"):
                pattern = pattern[1:]
            pattern = f"'{pattern}'"
        try:
            query = f'LIST @"{database}"."{schema}"."{stage}"'
            if pattern:
                query += f" PATTERN = {pattern}"
            ret = self.run_query(query, max_rows=50, max_rows_override=True)
            if isinstance(ret, dict) and "does not exist or not authorized" in ret.get(
                "Error", ""
            ):
                query = query.upper()
                ret = self.run_query(query, max_rows=50, max_rows_override=True)
            return ret

        except Exception as e:
            return {"success": False, "error": str(e)}

    def image_generation(self, prompt, thread_id=None):

        import openai, requests, os

        """
        Generates an image using OpenAI's DALL-E 3 based on the given prompt and saves it to the local downloaded_files folder.

        Args:
            prompt (str): The prompt to generate the image from.
            thread_id (str): The unique identifier for the thread to save the image in the correct location.

        Returns:
            str: The file path of the saved image.
        """

        if thread_id is None:
            import random
            import string

            thread_id = "".join(
                random.choices(string.ascii_letters + string.digits, k=10)
            )

        # Ensure the OpenAI API key is set in your environment variables
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            print("imagegen OpenAI API key is not set in the environment variables.")
            return None

        openai.api_key = os.getenv("OPENAI_API_KEY")
        client = openai.OpenAI(api_key=openai.api_key)

        # Generate the image using DALL-E 3
        try:
            response = client.images.generate(
                model="dall-e-3",
                prompt=prompt,
                size="1024x1024",
                quality="standard",
                n=1,
            )
            image_url = response.data[0].url
            if not image_url:
                print("imagegen Failed to generate image with DALL-E 3.")
                return None

            try:
                # Download the image from the URL
                image_response = requests.get(image_url)
                print("imagegen getting image from ", image_url)
                image_response.raise_for_status()
                image_bytes = image_response.content
            except Exception as e:
                result = {
                    "success": False,
                    "error": e,
                    "solution": """Tell the user to ask their admin run this to allow the Genesis server to access generated images:\n
                    CREATE OR REPLACE NETWORK RULE GENESIS_LOCAL_DB.SETTINGS.GENESIS_RULE
                    MODE = EGRESS TYPE = HOST_PORT
                    VALUE_LIST = ('api.openai.com', 'slack.com', 'www.slack.com', 'wss-primary.slack.com',
                    'wss-backup.slack.com',  'wss-primary.slack.com:443','wss-backup.slack.com:443', 'slack-files.com',
                    'oaidalleapiprodscus.blob.core.windows.net:443', 'downloads.slack-edge.com', 'files-edge.slack.com',
                    'files-origin.slack.com', 'files.slack.com', 'global-upload-edge.slack.com','universal-upload-edge.slack.com');


                    CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION GENESIS_EAI
                    ALLOWED_NETWORK_RULES = (GENESIS_LOCAL_DB.SETTINGS.GENESIS_RULE) ENABLED = true;

                    GRANT USAGE ON INTEGRATION GENESIS_EAI TO APPLICATION   IDENTIFIER($APP_DATABASE);""",
                }
                return result

            # Create a sanitized filename from the first 50 characters of the prompt
            sanitized_prompt = "".join(e if e.isalnum() else "_" for e in prompt[:50])
            file_path = f"./downloaded_files/{thread_id}/{sanitized_prompt}.png"
            # Save the image to the local downloaded_files folder
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with open(file_path, "wb") as image_file:
                image_file.write(image_bytes)

            print(f"imagegen Image generated and saved to {file_path}")

            result = {
                "success": True,
                "local_file_name": file_path,
                "prompt": prompt,
            }

            return result
        except Exception as e:
            print(f"imagegen Error generating image with DALL-E 3: {e}")
            return None

    def image_analysis(
        self,
        query=None,
        openai_file_id: str = None,
        file_name: str = None,
        thread_id=None,
    ):
        """
        Analyzes an image using OpenAI's GPT-4 Turbo Vision.

        Args:
            query (str): The prompt or question about the image.
            openai_file_id (str): The OpenAI file ID of the image to analyze.
            file_name (str): The name of the image file to analyze.
            thread_id (str): The unique identifier for the thread.

        Returns:
            dict: A dictionary with the result of the image analysis.
        """
        # Ensure the OpenAI API key is set in your environment variables
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            return {
                "success": False,
                "message": "OpenAI API key is not set in the environment variables.",
            }

        # Attempt to find the file using the provided method
        if file_name is not None and "/" in file_name:
            file_name = file_name.split("/")[-1]
        if openai_file_id is not None and "/" in openai_file_id:
            openai_file_id = openai_file_id.split("/")[-1]

        file_path = f"./downloaded_files/{thread_id}/" + file_name
        existing_location = f"./downloaded_files/{thread_id}/{openai_file_id}"

        if os.path.isfile(existing_location) and (file_path != existing_location):
            with open(existing_location, "rb") as source_file:
                with open(file_path, "wb") as dest_file:
                    dest_file.write(source_file.read())

        if not os.path.isfile(file_path):
            logger.error(f"File not found: {file_path}")
            return {
                "success": False,
                "error": "File not found. Please provide a valid file path.",
            }

        # Function to encode the image
        def encode_image(image_path):
            with open(image_path, "rb") as image_file:
                return base64.b64encode(image_file.read()).decode("utf-8")

        # Getting the base64 string
        base64_image = encode_image(file_path)

        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        }

        # Use the provided query or a default one if not provided
        prompt = query if query else "What’s in this image?"

        openai_model_name = os.getenv("OPENAI_MODEL_NAME", "gpt-4o")

        payload = {
            "model": openai_model_name,
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": prompt},
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": f"data:image/jpeg;base64,{base64_image}"
                            },
                        },
                    ],
                }
            ],
            "max_tokens": 300,
        }

        response = requests.post(
            "https://api.openai.com/v1/chat/completions", headers=headers, json=payload
        )

        if response.status_code == 200:
            return {
                "success": True,
                "data": response.json()["choices"][0]["message"]["content"],
            }
        else:
            return {
                "success": False,
                "error": f"OpenAI API call failed with status code {response.status_code}: {response.text}",
            }

    def add_file_to_stage(
        self,
        database: str = None,
        schema: str = None,
        stage: str = None,
        openai_file_id: str = None,
        file_name: str = None,
        file_content: str = None,
        thread_id=None,
    ):
        """
        Add a file to a Snowflake stage.

        Args:
            database (str): The name of the database.
            schema (str): The name of the schema.
            stage (str): The name of the stage.
            file_path (str): The local path to the file to be uploaded.
            file_format (str): The format of the file (default is 'CSV').

        Returns:
            dict: A dictionary with the result of the operation.
        """

        try:
            if file_content is None:
                file_name = file_name.replace("serverlocal:", "")
                openai_file_id = openai_file_id.replace("serverlocal:", "")

                if file_name.startswith("file-"):
                    return {
                        "success": False,
                        "error": "Please provide a human-readable file name in the file_name parameter, with a supported extension, not the OpenAI file ID. If unsure, ask the user what the file should be called.",
                    }

                # allow files to have relative paths
                #     if '/' in file_name:
                #         file_name = file_name.split('/')[-1]
                if file_name.startswith("/"):
                    file_name = file_name[1:]

                file_name = re.sub(r"[^\w\s\/\.-]", "", file_name.replace(" ", "_"))
                if "/" in openai_file_id:
                    openai_file_id = openai_file_id.split("/")[-1]

                file_path = f"./downloaded_files/{thread_id}/" + file_name
                existing_location = f"./downloaded_files/{thread_id}/{openai_file_id}"

                if not os.path.exists(os.path.dirname(file_path)):
                    os.makedirs(os.path.dirname(file_path))

                # Replace spaces with underscores and remove disallowed characters
                #  file_name = re.sub(r'[^\w\s-]', '', file_name.replace(' ', '_'))
                if os.path.isfile(existing_location) and (
                    file_path != existing_location
                ):
                    with open(existing_location, "rb") as source_file:
                        with open(file_path, "wb") as dest_file:
                            dest_file.write(source_file.read())

                if not os.path.isfile(file_path):

                    logger.error(f"File not found: {file_path}")
                    return {
                        "success": False,
                        "error": f"Needs user review: Please first save and RETURN THE FILE *AS A FILE* to the user for their review, and once confirmed by the user, call this function again referencing the SAME OPENAI_FILE_ID THAT YOU RETURNED TO THE USER to save it to stage.",
                    }

            else:
                if thread_id is None:
                    thread_id = "".join(
                        random.choices(string.ascii_letters + string.digits, k=6)
                    )

            if file_content is not None:
                # Ensure the directory exists
                directory = f"./downloaded_files/{thread_id}"
                if not os.path.exists(directory):
                    os.makedirs(directory)

                # Write the content to the file
                file_path = os.path.join(directory, file_name)
                with open(file_path, "w") as file:
                    file.write(file_content)
        except Exception as e:
            return {"success": False, "error": str(e)}

        try:
            p = os.path.dirname(file_name) if "/" in file_name else None
            if p is not None:
                query = f'PUT file://{file_path} @"{database}"."{schema}"."{stage}"/{p} AUTO_COMPRESS=FALSE'
            else:
                query = f'PUT file://{file_path} @"{database}"."{schema}"."{stage}" AUTO_COMPRESS=FALSE'
            return self.run_query(query)
        except Exception as e:
            logger.error(f"Error adding file to stage: {e}")
            return {"success": False, "error": str(e)}

    def read_file_from_stage(
        self,
        database: str,
        schema: str,
        stage: str,
        file_name: str,
        return_contents: bool,
        for_bot=None,
        thread_id=None,
    ):
        """
        Read a file from a Snowflake stage.

        Args:
            database (str): The name of the database.
            schema (str): The name of the schema.
            stage (str): The name of the stage.
            file_name (str): The name of the file to be read.

        Returns:
            str: The contents of the file.
        """
        try:
            # Define the local directory to save the file
            if for_bot == None:
                for_bot = thread_id
            local_dir = os.path.join(".", "downloaded_files", for_bot)

            #        if '/' in file_name:
            #            file_name = file_name.split('/')[-1]

            if not os.path.isdir(local_dir):
                os.makedirs(local_dir)
            local_file_path = os.path.join(local_dir, file_name)
            target_dir = os.path.dirname(local_file_path)
            if not os.path.exists(target_dir):
                os.makedirs(target_dir)

            # Modify the GET command to include the local file path

            query = f'GET @"{database}"."{schema}"."{stage}"/{file_name} file://{target_dir}'
            ret = self.run_query(query)
            if isinstance(ret, dict) and "does not exist or not authorized" in ret.get(
                "Error", ""
            ):
                database = database.upper()
                schema = schema.upper()
                stage = stage.upper()
                query = f'GET @"{database}"."{schema}"."{stage}"/{file_name} file://{local_dir}'
                ret = self.run_query(query)

            if os.path.isfile(local_file_path):
                if return_contents:
                    with open(local_file_path, "r") as file:
                        return file.read()
                else:
                    return file_name
            else:
                return f"The file {file_name} does not exist at stage path @{database}.{schema}.{stage}/{file_name}."
        except Exception as e:
            return {"success": False, "error": str(e)}

    def update_file_in_stage(
        self,
        database: str = None,
        schema: str = None,
        stage: str = None,
        file_name: str = None,
        thread_id=None,
    ):
        """
        Update (replace) a file in a Snowflake stage.

        Args:
            database (str): The name of the database.
            schema (str): The name of the schema.
            stage (str): The name of the stage.
            file_path (str): The local path to the new file.
            file_name (str): The name of the file to be replaced.
            file_format (str): The format of the file (default is 'CSV').

        Returns:
            dict: A dictionary with the result of the operation.
        """
        try:

            if "/" in file_name:
                file_name = file_name.split("/")[-1]

            file_path = f"./downloaded_files/{thread_id}/" + file_name

            if not os.path.isfile(file_path):

                logger.error(f"File not found: {file_path}")
                return {
                    "success": False,
                    "error": f"Local new version of file not found: {file_path}",
                }

            # First, remove the existing file
            remove_query = f"REMOVE @{database}.{schema}.{stage}/{file_name}"
            self.run_query(remove_query)
            # Then, add the new file

            add_query = f"PUT file://{file_path} @{database}.{schema}.{stage} AUTO_COMPRESS=FALSE"
            return self.run_query(add_query)
        except Exception as e:
            logger.error(f"Error updating file in stage: {e}")
            return {"success": False, "error": str(e)}

    def delete_file_from_stage(
        self,
        database: str = None,
        schema: str = None,
        stage: str = None,
        file_name: str = None,
        thread_id=None,
    ):
        """
        Delete a file from a Snowflake stage.

        Args:
            database (str): The name of the database.
            schema (str): The name of the schema.
            stage (str): The name of the stage.
            file_name (str): The name of the file to be deleted.

        Returns:
            dict: A dictionary with the result of the operation.
        """
        if "/" in file_name:
            file_name = file_name.split("/")[-1]

        try:
            query = f"REMOVE @{database}.{schema}.{stage}/{file_name}"
            ret = self.run_query(query)
            if isinstance(ret, dict) and "does not exist or not authorized" in ret.get(
                "Error", ""
            ):
                database = database.upper()
                schema = schema.upper()
                stage = stage.upper()
                query = f'REMOVE @"{database}"."{schema}"."{stage}"/{file_name}'
                ret = self.run_query(query)

            return ret
        except Exception as e:
            logger.error(f"Error deleting file from stage: {e}")
            return {"success": False, "error": str(e)}

    # Assuming self.connection is an instance of SnowflakeConnector
    # with methods run_query() for executing queries and logger is a logging instance.
    # Test instance creation and calling list_stage method

    def create_empty_semantic_model(
        self, model_name="", model_description="", thread_id=None
    ):
        # Define the basic structure of the semantic model with an empty tables list
        semantic_model = {
            "name": model_name,
            "description": model_description,  # Description is left empty to be filled later
            "tables": [],  # Initialize with an empty list of tables
        }
        return semantic_model

    # Usage of the function

    def convert_model_to_yaml(self, json_model, thread_id=None):
        """
        Convert the JSON representation of the semantic model to YAML format.

        Args:
            json_model (dict): The semantic model in JSON format.

        Returns:
            str: The semantic model in YAML format.
        """
        try:

            sanitized_model = {
                k: v
                for k, v in json_model.items()
                if isinstance(v, (str, int, float, bool, list, dict, type(None)))
            }
            yaml_model = yaml.dump(
                sanitized_model, default_flow_style=False, sort_keys=False
            )
            return yaml_model
        except Exception as exc:
            print(f"Error converting JSON to YAML: {exc}")
            return None

    def convert_yaml_to_json(self, yaml_model, thread_id=None):
        """
        Convert the YAML representation of the semantic model to JSON format.

        Args:
            yaml_model (str): The semantic model in YAML format.

        Returns:
            dict: The semantic model in JSON format, or None if conversion fails.
        """
        try:
            json_model = yaml.safe_load(yaml_model)
            return json_model
        except yaml.YAMLError as exc:
            print(f"Error converting YAML to JSON: {exc}")
            return None

    def modify_semantic_model(
        self, semantic_model, command, parameters, thread_id=None
    ):
        # Validate the command
        valid_commands = [
            "add_table",
            "remove_table",
            "update_table",
            "add_dimension",
            "update_dimension",
            "remove_dimension",
            "add_time_dimension",
            "remove_time_dimension",
            "update_time_dimension",
            "add_measure",
            "remove_measure",
            "update_measure",
            "add_filter",
            "remove_filter",
            "update_filter",
            "set_model_name",
            "set_model_description",
            "help",
        ]

        base_message = ""

        if command.startswith("update_") and "new_values" not in parameters:
            base_message = "Error: The 'new_values' parameter must be provided as a dictionary object for update_* commands.\n\n"

        if command == "help" or command not in valid_commands:

            help_message = (
                base_message
                + """
            The following commands are available to modify the semantic model:

            - 'add_table': Adds a new table to the semantic model. 
                Parameters: 'table_name', 'database', 'schema', 'table', 'description' (optional).
            - 'remove_table': Removes an existing table from the semantic model. 
                Parameters: 'table_name'.
            - 'update_table': Updates an existing table's details in the semantic model. 
                Parameters: 'table_name', 'new_values' (a dictionary with any of 'name', 'description', 'database', 'schema', 'table').
            - 'add_dimension': Adds a new dimension to an existing table. 
                Parameters: 'table_name', 'dimension_name', 'expr', 'data_type' (required, one of 'TEXT', 'DATE', 'NUMBER'), 'description' (optional), 'synonyms' (optional, list), 'unique' (optional, boolean), 'sample_values' (optional, list).
            - 'update_dimension': Updates an existing dimension in a table. 
                Parameters: 'table_name', 'dimension_name', 'new_values' (a dictionary with any of 'name', 'expr', 'data_type', 'description', 'synonyms', 'unique', 'sample_values').
            - 'remove_dimension': Removes an existing dimension from a table. 
                Parameters: 'table_name', 'dimension_name'.
            - 'add_time_dimension': Adds a new time dimension to an existing table. 
                Parameters: 'table_name', 'time_dimension_name', 'expr', 'data_type' (required, one of 'TEXT', 'DATE', 'NUMBER'), 'description' (optional), 'synonyms' (optional, list), 'unique' (optional, boolean), 'sample_values' (optional, list).
            - 'remove_time_dimension': Removes an existing time dimension from a table. 
                Parameters: 'table_name', 'time_dimension_name'.
            - 'update_time_dimension': Updates an existing time dimension in a table. 
                Parameters: 'table_name', 'time_dimension_name', 'new_values' (a dictionary with any of 'name', 'expr', 'data_type', 'description', 'synonyms', 'unique', 'sample_values').
            - 'add_measure': Adds a new measure to an existing table. 
                Parameters: 'table_name', 'measure_name', 'expr', 'data_type' (required, one of 'TEXT', 'DATE', 'NUMBER'), 'description' (optional), 'synonyms' (optional, list), 'unique' (optional, boolean), 'sample_values' (optional, list), 'default_aggregation' (optional).
            - 'remove_measure': Removes an existing measure from a table. 
                Parameters: 'table_name', 'measure_name'.
            - 'update_measure': Updates an existing measure in a table. 
                Parameters: 'table_name', 'measure_name', 'new_values' (a dictionary with any of 'name', 'expr', 'data_type', 'description', 'synonyms', 'unique', 'sample_values', 'default_aggregation').
            - 'add_filter': Adds a new filter to an existing table. 
                Parameters: 'table_name', 'filter_name', 'expr', 'description' (optional), 'synonyms' (optional, list).
            - 'remove_filter': Removes an existing filter from a table. 
                Parameters: 'table_name', 'filter_name'.
            - 'update_filter': Updates an existing filter in a table. 
                Parameters: 'table_name', 'filter_name', 'new_values' (a dictionary with any of 'name', 'expr', 'description', 'synonyms').
            - 'set_model_name': Sets the name of the semantic model. 
                Parameters: 'model_name'.
            - 'set_model_description': Sets the description of the semantic model. 
                Parameters: 'model_description'.
            Note that all "expr" must be SQL-executable expressions that could work as part of a SELECT clause (for dimension and measures, often just the base column name) or WHERE clause (for filters).
            """
            )
            if command not in valid_commands:
                return {"success": False, "function_instructions": help_message}
            else:
                return {"success": True, "message": help_message}

        try:
            if command == "set_model_name":
                semantic_model["model_name"] = parameters.get("model_name", "")
                return {
                    "success": True,
                    "message": f"Model name set to '{semantic_model['model_name']}'.",
                    "semantic_yaml": semantic_model,
                }

            if command == "set_model_description":
                semantic_model["description"] = parameters.get("model_description", "")
                return {
                    "success": True,
                    "message": f"Model description set to '{semantic_model['description']}'.",
                    "semantic_yaml": semantic_model,
                }

            if "table_name" not in parameters:
                return {
                    "success": False,
                    "message": "Missing parameter 'table_name'.",
                    "semantic_yaml": semantic_model,
                }
            table_name = parameters["table_name"]
            table = next(
                (
                    table
                    for table in semantic_model.get("tables", [])
                    if table["name"] == table_name
                ),
                None,
            )

            if (
                command in ["remove_table", "add_table", "update_table"]
                and not table
                and command != "add_table"
            ):
                return {"success": False, "message": f"Table '{table_name}' not found."}
            valid_data_types = [
                "NUMBER",
                "DECIMAL",
                "NUMERIC",
                "INT",
                "INTEGER",
                "BIGINT",
                "SMALLINT",
                "TINYINT",
                "BYTEINT",
                "FLOAT",
                "FLOAT4",
                "FLOAT8",
                "DOUBLE",
                "DOUBLE PRECISION",
                "REAL",
                "VARCHAR",
                "CHAR",
                "CHARACTER",
                "STRING",
                "TEXT",
                "BINARY",
                "VARBINARY",
                "BOOLEAN",
                "DATE",
                "DATETIME",
                "TIME",
                "TIMESTAMP",
                "TIMESTAMP_LTZ",
                "TIMESTAMP_NTZ",
                "TIMESTAMP_TZ",
                "VARIANT",
                "OBJECT",
                "ARRAY",
                "GEOGRAPHY",
                "GEOMETRY",
            ]

            ###TODO ADD CHECK FOR NEW_VALUES ON UPDATE

            if command in [
                "add_dimension",
                "add_time_dimension",
                "add_measure",
                "update_dimension",
                "update_time_dimension",
                "update_measure",
            ]:
                data_type = parameters.get("data_type")
                if data_type is not None:
                    data_type = data_type.upper()
                new_values = parameters.get("new_values", {})
                if data_type is None:
                    data_type = new_values.get("data_type", None)
                if data_type is not None:
                    data_type = data_type.upper()
                if data_type is None and command.startswith("add_"):
                    return {
                        "success": False,
                        "message": "data_type is required for adding new elements.",
                    }
                if data_type is not None and data_type not in valid_data_types:
                    return {
                        "success": False,
                        "message": "data_type is required, try using TEXT, DATE, or NUMBER.",
                    }

            if command == "add_table":
                required_base_table_keys = ["database", "schema", "table"]
                if not all(key in parameters for key in required_base_table_keys):
                    missing_keys = [
                        key for key in required_base_table_keys if key not in parameters
                    ]
                    return {
                        "success": False,
                        "message": f"Missing base table parameters: {', '.join(missing_keys)}.",
                    }

                if table:
                    return {
                        "success": False,
                        "message": f"Table '{table_name}' already exists.",
                        "semantic_yaml": semantic_model,
                    }

                new_table = {
                    "name": table_name,
                    "description": parameters.get("description", ""),
                    "base_table": {
                        "database": parameters["database"],
                        "schema": parameters["schema"],
                        "table": parameters["table"],
                    },
                    "dimensions": [],
                    "time_dimensions": [],
                    "measures": [],
                    "filters": [],
                }
                semantic_model.setdefault("tables", []).append(new_table)
                return {
                    "success": True,
                    "message": f"Table '{table_name}' added.",
                    "semantic_yaml": semantic_model,
                }

            elif command == "remove_table":
                semantic_model["tables"] = [
                    t for t in semantic_model["tables"] if t["name"] != table_name
                ]
                return {"success": True, "message": f"Table '{table_name}' removed."}

            elif command == "update_table":
                if not table:
                    return {
                        "success": False,
                        "message": f"Table '{table_name}' not found.",
                    }
                new_values = parameters.get("new_values", {})
                for key, value in new_values.items():
                    if key in table:
                        table[key] = value
                if (
                    "database" in parameters
                    or "schema" in parameters
                    or "table" in parameters
                ):
                    table["base_table"] = {
                        "database": parameters.get(
                            "database", table["base_table"]["database"]
                        ),
                        "schema": parameters.get(
                            "schema", table["base_table"]["schema"]
                        ),
                        "table": parameters.get("table", table["base_table"]["table"]),
                    }
                description = parameters.get("description")
                if description:
                    table["description"] = description
                return {
                    "success": True,
                    "message": f"Table '{table_name}' updated.",
                    "semantic_yaml": semantic_model,
                }

            elif (
                "dimension_name" in parameters
                or "measure_name" in parameters
                or "filter_name" in parameters
                or "time_dimension_name" in parameters
            ):
                if not table:
                    return {
                        "success": False,
                        "message": f"Table '{table_name}' not found.",
                    }

                item_key = (
                    "time_dimension_name"
                    if "time_dimension_name" in parameters
                    else (
                        "dimension_name"
                        if "dimension_name" in parameters
                        else (
                            "measure_name"
                            if "measure_name" in parameters
                            else "filter_name" if "filter_name" in parameters else None
                        )
                    )
                )
                item_name = parameters[item_key]
                item_list = table.get(
                    (
                        "time_dimensions"
                        if "time_dimension" in command
                        else (
                            "dimensions"
                            if "dimension" in command
                            else "measures" if "measure" in command else "filters"
                        )
                    ),
                    [],
                )
                item = next((i for i in item_list if i["name"] == item_name), None)
                if command.startswith("remove") and not item:
                    return {
                        "success": False,
                        "message": f"{item_key[:-5].capitalize()} '{item_name}' not found in table '{table_name}'.",
                    }

                if command.startswith("add"):
                    if item:
                        return {
                            "success": False,
                            "message": f"{item_key[:-5].capitalize()} '{item_name}' already exists in table '{table_name}'.",
                            "semantic_yaml": semantic_model,
                        }
                    expr = parameters.get("expr")
                    if expr is None:
                        return {
                            "success": False,
                            "message": f"Expression parameter 'expr' for {item_key[:-5].capitalize()} '{item_name}' is required.",
                            "semantic_yaml": semantic_model,
                        }
                    new_item = {"name": item_name, "expr": expr}
                    description = parameters.get("description")
                    if description:
                        new_item["description"] = description
                    synonyms = parameters.get("synonyms", [])
                    if synonyms:
                        new_item["synonyms"] = synonyms
                    data_type = parameters.get("data_type", None)
                    if data_type is not None:
                        new_item["data_type"] = data_type
                    unique = parameters.get("unique", None)
                    if unique is not None:
                        new_item["unique"] = unique
                    if "measure" in command:
                        default_aggregation = parameters.get("default_aggregation")
                        if default_aggregation:
                            new_item["default_aggregation"] = (
                                default_aggregation.lower()
                            )
                    if "filter" not in command:
                        sample_values = parameters.get("sample_values", [])
                        if sample_values:
                            new_item["sample_values"] = [
                                str(value)
                                for value in sample_values
                                if isinstance(value, (int, float, str, datetime.date))
                            ]
                        # new_item['sample_values'] = sample_values
                    item_list.append(new_item)
                    return {
                        "success": True,
                        "message": f"{item_key[:-5].capitalize()} '{item_name}' added to table '{table_name}'.",
                        "semantic_yaml": semantic_model,
                    }

                elif command.startswith("update"):
                    if not item:
                        return {
                            "success": False,
                            "message": f"{item_key[:-5].capitalize()} '{item_name}' not found in table '{table_name}'.",
                            "semantic_yaml": semantic_model,
                        }
                    new_values = parameters.get("new_values", {})
                    if "expr" in new_values:
                        expr = new_values.pop("expr")
                        if expr is not None:
                            item["expr"] = expr

                    if "data_type" in parameters["new_values"]:
                        item["data_type"] = parameters["new_values"][
                            "data_type"
                        ]  # Update the DATA_TYPE

                    if "default_aggregation" in parameters["new_values"]:
                        item["default_aggregation"] = parameters["new_values"][
                            "default_aggregation"
                        ].lower()  # Update the DATA_TYPE

                    if "unique" in new_values:
                        unique = new_values.pop("unique")
                        if isinstance(unique, bool):
                            item["unique"] = unique
                    if "measure" in command:
                        default_aggregation = new_values.pop(
                            "default_aggregation", None
                        )
                        if default_aggregation is not None:
                            item["default_aggregation"] = default_aggregation.lower()
                    if "filter" not in command:
                        sample_values = new_values.pop("sample_values", None)
                        if sample_values is not None:
                            item["sample_values"] = sample_values
                    item.update(new_values)
                    description = parameters.get("description")
                    if description:
                        item["description"] = description
                    synonyms = parameters.get("synonyms")
                    if synonyms is not None:
                        item["synonyms"] = synonyms
                    return {
                        "success": True,
                        "message": f"{item_key[:-5].capitalize()} '{item_name}' updated in table '{table_name}'.",
                        "semantic_yaml": semantic_model,
                    }
                elif command.startswith("remove"):
                    table[item_key[:-6] + "s"] = [
                        i for i in item_list if i["name"] != item_name
                    ]
                    return {
                        "success": True,
                        "message": f"{item_key[:-5].capitalize()} '{item_name}' removed from table '{table_name}'.",
                        "semantic_yaml": semantic_model,
                    }
        except KeyError as e:
            return {
                "success": False,
                "message": f"Missing necessary parameter '{e.args[0]}'.",
            }
        except Exception as e:
            return {"success": False, "message": f"An unexpected error occurred: {e}"}

    def test_modify_semantic_model(self, semantic_model):
        from schema_explorer.semantic_tools import modify_semantic_model

        def random_string(prefix, length=5):
            return (
                prefix + "_" + "".join(random.choices(string.ascii_lowercase, k=length))
            )

        num_tables = random.randint(2, 5)
        tables = [random_string("table") for _ in range(num_tables)]

        model_name = random_string("model")
        model_description = random_string("description", 10)
        semantic_model = modify_semantic_model(
            semantic_model, "set_model_name", {"model_name": model_name}
        )
        semantic_model = semantic_model.get("semantic_yaml")
        semantic_model = modify_semantic_model(
            semantic_model,
            "set_model_description",
            {"model_description": model_description},
        )
        semantic_model = semantic_model.get("semantic_yaml")

        for table_name in tables:
            database_name = random_string("database")
            schema_name = random_string("schema")
            base_table = random_string("base_table")
            semantic_model = modify_semantic_model(
                semantic_model,
                "add_table",
                {
                    "table_name": table_name,
                    "database": database_name,
                    "schema": schema_name,
                    "table": base_table,
                },
            )
            semantic_model = semantic_model.get("semantic_yaml")

        # Add 2-5 random dimensions, measures, and filters to each table
        for table_name in tables:
            for _ in range(random.randint(2, 5)):
                dimension_name = random_string("dimension")
                dimension_description = f"Description for {dimension_name}"
                dimension_expr = random_string("expr", 5)
                synonyms_count = random.randint(0, 3)
                dimension_synonyms = [
                    random_string("synonym") for _ in range(synonyms_count)
                ]
                sample_values_count = random.randint(0, 5)
                dimension_sample_values = [
                    random_string("", random.randint(7, 12))
                    for _ in range(sample_values_count)
                ]
                semantic_model = modify_semantic_model(
                    semantic_model,
                    "add_dimension",
                    {
                        "table_name": table_name,
                        "dimension_name": dimension_name,
                        "description": dimension_description,
                        "synonyms": dimension_synonyms,
                        "unique": False,
                        "expr": dimension_expr,
                        "sample_values": dimension_sample_values,
                    },
                )
                semantic_model = semantic_model.get("semantic_yaml")

                time_dimension_name = random_string("time_dimension")
                time_dimension_description = f"Description for {time_dimension_name}"
                time_dimension_expr = random_string("expr", 5)
                time_dimension_synonyms_count = random.randint(0, 3)
                time_dimension_synonyms = [
                    random_string("synonym")
                    for _ in range(time_dimension_synonyms_count)
                ]
                time_dimension_sample_values_count = random.randint(0, 5)
                time_dimension_sample_values = [
                    random_string("", random.randint(7, 12))
                    for _ in range(time_dimension_sample_values_count)
                ]
                semantic_model = modify_semantic_model(
                    semantic_model,
                    "add_time_dimension",
                    {
                        "table_name": table_name,
                        "time_dimension_name": time_dimension_name,
                        "description": time_dimension_description,
                        "synonyms": time_dimension_synonyms,
                        "unique": False,
                        "expr": time_dimension_expr,
                        "sample_values": time_dimension_sample_values,
                    },
                )
                semantic_model = semantic_model.get("semantic_yaml")

                measure_name = random_string("measure")
                measure_description = f"Description for {measure_name}"
                measure_expr = random_string("expr", 5)
                measure_synonyms_count = random.randint(0, 2)
                measure_synonyms = [
                    random_string("synonym") for _ in range(measure_synonyms_count)
                ]
                measure_sample_values_count = random.randint(0, 5)
                measure_sample_values = [
                    random_string("", random.randint(7, 12))
                    for _ in range(measure_sample_values_count)
                ]
                default_aggregations = [
                    "sum",
                    "avg",
                    "min",
                    "max",
                    "median",
                    "count",
                    "count_distinct",
                ]
                default_aggregation = random.choice(default_aggregations)
                semantic_model = modify_semantic_model(
                    semantic_model,
                    "add_measure",
                    {
                        "table_name": table_name,
                        "measure_name": measure_name,
                        "description": measure_description,
                        "synonyms": measure_synonyms,
                        "unique": False,
                        "expr": measure_expr,
                        "sample_values": measure_sample_values,
                        "default_aggregation": default_aggregation,
                    },
                )
                semantic_model = semantic_model.get("semantic_yaml")
                filter_name = random_string("filter")
                filter_description = f"Description for {filter_name}"
                filter_expr = random_string("expr", 5)
                filter_synonyms_count = random.randint(0, 2)
                filter_synonyms = [
                    random_string("synonym") for _ in range(filter_synonyms_count)
                ]
                semantic_model = modify_semantic_model(
                    semantic_model,
                    "add_filter",
                    {
                        "table_name": table_name,
                        "filter_name": filter_name,
                        "description": filter_description,
                        "synonyms": filter_synonyms,
                        "expr": filter_expr,
                    },
                )
                semantic_model = semantic_model.get("semantic_yaml")
        if semantic_model is None:
            raise ValueError(
                "Semantic model is None, cannot proceed with modifications."
            )

        # Update some of the tables, dimensions, measures, and filters
        # TODO: Add update tests for more of the parameters beside these listed below

        updated_table_names = {}
        for table_name in tables:
            if random.choice([True, False]):
                new_table_name = random_string("updated_table")
                result = modify_semantic_model(
                    semantic_model,
                    "update_table",
                    {"table_name": table_name, "new_values": {"name": new_table_name}},
                )
                if result.get("success"):
                    semantic_model = result.get("semantic_yaml")
                    updated_table_names[table_name] = new_table_name
                else:
                    raise Exception(f"Error updating table: {result.get('message')}")

        for original_table_name in tables:
            current_table_name = updated_table_names.get(
                original_table_name, original_table_name
            )
            if semantic_model and "tables" in semantic_model:
                table = next(
                    (
                        t
                        for t in semantic_model["tables"]
                        if t["name"] == current_table_name
                    ),
                    None,
                )
                if table:
                    for dimension in table.get("dimensions", []):
                        if random.choice([True, False]):
                            new_dimension_name = random_string("updated_dimension")
                            result = modify_semantic_model(
                                semantic_model,
                                "update_dimension",
                                {
                                    "table_name": current_table_name,
                                    "dimension_name": dimension["name"],
                                    "new_values": {"name": new_dimension_name},
                                },
                            )
                            if result.get("success"):
                                semantic_model = result.get("semantic_yaml")
                            else:
                                raise Exception(
                                    f"Error updating dimension: {result.get('message')}"
                                )

                    for measure in table.get("measures", []):
                        if random.choice([True, False]):
                            new_measure_name = random_string("updated_measure")
                            result = modify_semantic_model(
                                semantic_model,
                                "update_measure",
                                {
                                    "table_name": current_table_name,
                                    "measure_name": measure["name"],
                                    "new_values": {"name": new_measure_name},
                                },
                            )
                            if result.get("success"):
                                semantic_model = result.get("semantic_yaml")
                            else:
                                raise Exception(
                                    f"Error updating measure: {result.get('message')}"
                                )

                    for filter in table.get("filters", []):
                        if random.choice([True, False]):
                            new_filter_name = random_string("updated_filter")
                            result = modify_semantic_model(
                                semantic_model,
                                "update_filter",
                                {
                                    "table_name": current_table_name,
                                    "filter_name": filter["name"],
                                    "new_values": {"name": new_filter_name},
                                },
                            )
                            if result.get("success"):
                                semantic_model = result.get("semantic_yaml")
                            else:
                                raise Exception(
                                    f"Error updating filter: {result.get('message')}"
                                )

        # Update descriptions for tables, dimensions, measures, and filters using modify_semantic_model
        for table in semantic_model.get("tables", []):
            # Update table description
            if random.choice([True, False]):
                new_description = f"Updated description for {table['name']}"
                result = modify_semantic_model(
                    semantic_model,
                    "update_table",
                    {
                        "table_name": table["name"],
                        "new_values": {"description": new_description},
                    },
                )
                if result.get("success"):
                    semantic_model = result.get("semantic_yaml")
                else:
                    raise Exception(
                        f"Error updating table description: {result.get('message')}"
                    )

            # Update dimensions descriptions
            for dimension in table.get("dimensions", []):
                if random.choice([True, False]):
                    new_description = f"Updated description for {dimension['name']}"
                    result = modify_semantic_model(
                        semantic_model,
                        "update_dimension",
                        {
                            "table_name": table["name"],
                            "dimension_name": dimension["name"],
                            "new_values": {"description": new_description},
                        },
                    )
                    if result.get("success"):
                        semantic_model = result.get("semantic_yaml")
                    else:
                        raise Exception(
                            f"Error updating dimension description: {result.get('message')}"
                        )

            # Update measures descriptions
            for measure in table.get("measures", []):
                if random.choice([True, False]):
                    new_description = f"Updated description for {measure['name']}"
                    result = modify_semantic_model(
                        semantic_model,
                        "update_measure",
                        {
                            "table_name": table["name"],
                            "measure_name": measure["name"],
                            "new_values": {"description": new_description},
                        },
                    )
                    if result.get("success"):
                        semantic_model = result.get("semantic_yaml")
                    else:
                        raise Exception(
                            f"Error updating measure description: {result.get('message')}"
                        )

            # Update filters descriptions
            for filter in table.get("filters", []):
                if random.choice([True, False]):
                    new_description = f"Updated description for {filter['name']}"
                    result = modify_semantic_model(
                        semantic_model,
                        "update_filter",
                        {
                            "table_name": table["name"],
                            "filter_name": filter["name"],
                            "new_values": {"description": new_description},
                        },
                    )
                    if result.get("success"):
                        semantic_model = result.get("semantic_yaml")
                    else:
                        raise Exception(
                            f"Error updating filter description: {result.get('message')}"
                        )
        # Verify the re
        # Update the physical table for some of the logical tables
        for table_name in tables:
            current_table_name = updated_table_names.get(table_name, table_name)
            if random.choice(
                [True, False]
            ):  # Randomly decide whether to update the physical table
                new_database_name = random_string("new_database")
                new_schema_name = random_string("new_schema")
                new_base_table_name = random_string("new_base_table")
                result = modify_semantic_model(
                    semantic_model,
                    "update_table",
                    {
                        "table_name": current_table_name,
                        "new_values": {
                            "base_table": {
                                "database": new_database_name,
                                "schema": new_schema_name,
                                "table": new_base_table_name,
                            }
                        },
                    },
                )
                if result.get("success"):
                    semantic_model = result.get("semantic_yaml")
                    updated_table_names[table_name] = (
                        new_base_table_name  # Track the updated table names
                    )
                else:
                    raise Exception(
                        f"Error updating base table: {result.get('message')}"
                    )

        assert "tables" in semantic_model
        assert len(semantic_model["tables"]) == num_tables
        for table in semantic_model["tables"]:
            if "dimensions" not in table or not (2 <= len(table["dimensions"]) <= 5):
                raise AssertionError(
                    "Table '{}' does not have the required number of dimensions (between 2 and 5).".format(
                        table.get("name")
                    )
                )
            assert "measures" in table and 2 <= len(table["measures"]) <= 5
            assert "filters" in table and 2 <= len(table["filters"]) <= 5
        # Check that each table has a physical table with the correct fields set
        for table in semantic_model.get("tables", []):
            base_table = table.get("base_table")
            if not base_table:
                raise Exception(
                    f"Table '{table['name']}' does not have a base table associated with it."
                )
            required_fields = ["database", "schema", "table"]
            for field in required_fields:
                if field not in base_table or not base_table[field]:
                    raise Exception(
                        f"Base table for '{table['name']}' does not have the required field '{field}' set correctly."
                    )

        return semantic_model

    def suggest_improvements(self, semantic_model, thread_id=None):
        """
        Analyze the semantic model and suggest improvements to make it more comprehensive and complete.

        Args:
            semantic_model (dict): The semantic model in JSON format.

        Returns:
            list: A list of suggestions for improving the semantic model.
        """
        suggestions = []

        # Check if model name and description are set
        if not semantic_model.get("model_name"):
            suggestions.append(
                "Consider adding a 'model_name' to your semantic model for better identification."
            )
        if not semantic_model.get("description"):
            suggestions.append(
                "Consider adding a 'description' to your semantic model to provide more context."
            )

        # Check for tables
        tables = semantic_model.get("tables", [])
        if not tables:
            suggestions.append(
                "Your semantic model has no tables. Consider adding some tables to it."
            )
        else:
            # Check for uniqueness of table names
            table_names = [table.get("name") for table in tables]
            if len(table_names) != len(set(table_names)):
                suggestions.append(
                    "Some table names are not unique. Ensure each table has a unique name."
                )

            synonyms = set()
            synonym_conflicts = set()
            tables_with_synonyms = 0
            tables_with_sample_values = 0

            for table in tables:
                # Check for table description
                if not table.get("description"):
                    suggestions.append(
                        f"Table '{table['name']}' has no description. Consider adding a description for clarity."
                    )

                # Check for physical table mapping
                base_table = table.get("base_table")
                if not base_table or not all(
                    key in base_table for key in ["database", "schema", "table"]
                ):
                    suggestions.append(
                        f"Table '{table['name']}' has incomplete base table mapping. Ensure 'database', 'schema', and 'table' are defined."
                    )

                # Check for dimensions, measures, and filters
                if not table.get("dimensions"):
                    suggestions.append(
                        f"Table '{table['name']}' has no dimensions. Consider adding some dimensions."
                    )
                if not table.get("measures"):
                    suggestions.append(
                        f"Table '{table['name']}' has no measures. Consider adding some measures."
                    )
                if not table.get("filters"):
                    suggestions.append(
                        f"Table '{table['name']}' has no filters. Consider adding some filters."
                    )

                # Check for time dimensions
                if "time_dimensions" not in table or not table["time_dimensions"]:
                    suggestions.append(
                        f"Table '{table['name']}' has no time dimensions. Consider adding time dimensions for time-based analysis."
                    )

                # Check for synonyms and sample_values
                for element in (
                    table.get("dimensions", [])
                    + table.get("measures", [])
                    + table.get("filters", [])
                    + table.get("time_dimensions", [])
                ):
                    if element.get("synonyms"):
                        tables_with_synonyms += 1
                        for synonym in element["synonyms"]:
                            if synonym in synonyms:
                                synonym_conflicts.add(synonym)
                            synonyms.add(synonym)

                    if (
                        "sample_values" in element
                        and len(element["sample_values"]) >= 5
                    ):
                        tables_with_sample_values += 1

            # Suggestions for synonyms
            if tables_with_synonyms < len(tables) / 2:
                suggestions.append(
                    "Consider adding synonyms to at least half of the dimensions, measures, and filters for better searchability."
                )

            if synonym_conflicts:
                suggestions.append(
                    f"Synonyms {', '.join(synonym_conflicts)} are not unique across the semantic model. Consider making synonyms unique."
                )

            # Suggestions for sample_values
            if tables_with_sample_values < len(tables) / 2:
                suggestions.append(
                    "Consider adding at least five examples of 'sample_values' on at least half of the measures, dimensions, and time dimensions for better examples in your model."
                )

        return suggestions

    # Define a global map to store semantic models by thread_id

    def initialize_semantic_model(
        self, model_name=None, model_description=None, thread_id=None
    ):
        """
        Creates an empty semantic model and stores it in a map with the thread_id as the key.

        Args:
            model_name (str): The name of the model to initialize.
            thread_id (str): The unique identifier for the thread.
        """
        # Create an empty semantic model
        if not model_name:
            return {"Success": False, "Error": "model_name not provided"}

        empty_model = self.create_empty_semantic_model(
            model_name=model_name, model_description=model_description
        )
        # Store the model in the map using thread_id as the key
        map_key = thread_id + "__" + model_name
        self.semantic_models_map[map_key] = empty_model

        if empty_model is not None:
            return {
                "Success": True,
                "Message": f"The model {model_name} has been initialized.",
            }
        else:
            return {"Success": False, "Error": "Failed to initialize the model."}

    def modify_and_update_semantic_model(
        self, model_name, command, parameters=None, thread_id=None
    ):
        """
        Modifies the semantic model based on the provided modifications, updates the model in the map,
        and returns the modified semantic model without the resulting YAML. Ensures that only one thread
        can run this method at a time.

        Args:
            model_name (str): The name of the model to modify.
            thread_id (str): The unique identifier for the thread.
            modifications (dict): The modifications to apply to the semantic model.

        Returns:
            dict: The modified semantic model.
        """

        with _semantic_lock:
            # Construct the map key
            # Parse the command and modifications if provided in the command string
            import json

            if isinstance(parameters, str):
                parameters = json.loads(parameters)

            map_key = thread_id + "__" + model_name
            # Retrieve the semantic model from the map
            semantic_model = self.semantic_models_map.get(map_key)
            if not semantic_model:
                raise ValueError(
                    f"No semantic model found for model_name: {model_name} and thread_id: {thread_id}"
                )

            # Call modify_semantic_model with the retrieved model and the modifications
            result = self.modify_semantic_model(
                semantic_model=semantic_model, command=command, parameters=parameters
            )

            # Check if 'semantic_yaml' is in the result and store it back into the map
            if "semantic_yaml" in result:
                self.semantic_models_map[map_key] = result["semantic_yaml"]
                # Strip 'semantic_yaml' parameter from result
                del result["semantic_yaml"]

                # Call the suggestions function with the model and add the suggestions to the result
            #     suggestions_result = self.suggest_improvements(self.semantic_models_map[map_key])
            #     result['suggestions'] = suggestions_result
            # Return the modified semantic model without the resulting YAML
            return result

    def get_semantic_model(self, model_name, thread_id):
        """
        Retrieves an existing semantic model from the map based on the model name and thread id.

        Args:
            model_name (str): The name of the model to retrieve.
            thread_id (str): The unique identifier for the thread.

        Returns:
            dict: A JSON wrapper with the semantic model if found, otherwise an error message.
        """
        # Construct the map key
        map_key = thread_id + "__" + model_name
        # Retrieve the semantic model from the map
        semantic_model = self.semantic_models_map.get(map_key)
        semantic_yaml = self.convert_model_to_yaml(semantic_model)
        if semantic_yaml:
            return {"Success": True, "SemanticModel": yaml.dump(semantic_yaml)}
        else:
            return {
                "Success": False,
                "Error": f"No semantic model found for model_name: {model_name} and thread_id: {thread_id}",
            }

    def deploy_semantic_model(
        self, model_name=None, target_name=None, prod=False, thread_id=None
    ):

        map_key = thread_id + "__" + model_name
        # Retrieve the semantic model from the map
        semantic_model = self.semantic_models_map.get(map_key)
        semantic_yaml = self.convert_model_to_yaml(semantic_model)

        # Determine the stage based on the prod flag
        stage_name = "SEMANTIC_MODELS" if prod else "SEMANTIC_MODELS_DEV"
        # Convert the semantic model to YAML and save it to the appropriate stage
        try:
            # Convert semantic model to YAML

            semantic_yaml_str = semantic_yaml
            # Define the file name for the YAML file
            if target_name is None:
                yaml_file_name = f"{model_name}.yaml"
            else:
                yaml_file_name = f"{target_name}.yaml"
            # Save the YAML string to the stage
            db, sch = self.genbot_internal_project_and_schema.split(".")
            self.add_file_to_stage(
                database=db,
                schema=sch,
                stage=stage_name,
                file_name=yaml_file_name,
                file_content=semantic_yaml_str,
            )
            print(
                f"Semantic YAML for model '{model_name}' saved to stage '{stage_name}'."
            )
        except Exception as e:
            return {
                "Success": False,
                "Error": f"Failed to save semantic YAML to stage '{stage_name}': {e}",
            }

    def load_semantic_model(self, model_name, prod=False, thread_id=None):
        """
        Loads a semantic model from the specified stage into the semantic models map.

        Args:
            model_name (str): The name of the model to load.
            thread_id (str): The unique identifier for the thread.
            prod (bool): Flag to determine if the model should be loaded from production stage. Defaults to False.

        Returns:
            dict: A JSON wrapper with the result of the operation.
        """
        # Determine the stage based on the prod flag
        stage_name = "SEMANTIC_MODELS" if prod else "SEMANTIC_MODELS_DEV"
        # Define the file name for the YAML file
        yaml_file_name = model_name
        if not yaml_file_name.endswith(".yaml"):
            yaml_file_name += ".yaml"
        # Attempt to read the YAML file from the stage
        try:
            db, sch = self.genbot_internal_project_and_schema.split(".")
            if thread_id is None:
                thread_id = "".join(
                    random.choices(string.ascii_letters + string.digits, k=6)
                )

            file_content = self.read_file_from_stage(
                database=db,
                schema=sch,
                stage=stage_name,
                file_name=yaml_file_name,
                return_contents=True,
                thread_id=thread_id,
            )
            if file_content:
                # Convert YAML content to a Python object
                semantic_model = yaml.safe_load(file_content)
                # Construct the map key
                map_key = thread_id + "__" + model_name
                # Store the semantic model in the map
                self.semantic_models_map[map_key] = semantic_model
                return {
                    "Success": True,
                    "Message": f"Semantic model '{model_name}' loaded from stage '{stage_name}'.",
                }
            else:
                return {
                    "Success": False,
                    "Error": f"Semantic model '{model_name}' not found in stage '{stage_name}'.",
                }
        except Exception as e:
            return {
                "Success": False,
                "Error": f"Failed to load semantic model from stage '{stage_name}': {e}",
            }

    def list_semantic_models(self, prod=None, thread_id=None):
        """
        Lists the semantic models in both production and non-production stages.

        Returns:
            dict: A JSON object containing the lists of models in production and non-production stages.
        """
        # Split the combined project and schema string into separate database and schema variables
        db, sch = self.genbot_internal_project_and_schema.split(".")
        prod_stage_name = "SEMANTIC_MODELS"
        dev_stage_name = "SEMANTIC_MODELS_DEV"
        prod_models = []
        dev_models = []
        try:
            # List models in production stage
            prod_stage_contents = self.list_stage_contents(
                database=db, schema=sch, stage=prod_stage_name
            )
            prod_models = [model["name"] for model in prod_stage_contents]

            # List models in non-production stage
            dev_stage_contents = self.list_stage_contents(
                database=db, schema=sch, stage=dev_stage_name
            )
            dev_models = [model["name"] for model in dev_stage_contents]

            prod_models = [
                model.split("/")[-1] if "/" in model else model for model in prod_models
            ]
            dev_models = [
                model.split("/")[-1] if "/" in model else model for model in dev_models
            ]
            prod_models = [model.replace(".yaml", "") for model in prod_models]
            dev_models = [model.replace(".yaml", "") for model in dev_models]
            return {"Success": True, "ProdModels": prod_models, "DevModels": dev_models}
        except Exception as e:
            return {"Success": False, "Error": str(e)}

    def db_remove_bot_tools(
        self,
        project_id=None,
        dataset_name=None,
        bot_servicing_table=None,
        bot_id=None,
        updated_tools_str=None,
        tools_to_be_removed=None,
        invalid_tools=None,
        updated_tools=None,
    ):

        # Query to update the available_tools in the database
        update_query = f"""
                UPDATE {project_id}.{dataset_name}.{bot_servicing_table}
                SET available_tools = %s
                WHERE upper(bot_id) = upper(%s)
            """

        # Execute the update query
        try:
            cursor = self.connection.cursor()
            cursor.execute(update_query, (updated_tools_str, bot_id))
            self.connection.commit()
            logger.info(f"Successfully updated available_tools for bot_id: {bot_id}")

            return {
                "success": True,
                "removed": tools_to_be_removed,
                "invalid tools": invalid_tools,
                "all_bot_tools": updated_tools,
            }

        except Exception as e:
            logger.error(
                f"Failed to remove tools from bot_id: {bot_id} with error: {e}"
            )
            return {"success": False, "error": str(e)}

    def extract_knowledge(self, primary_user, bot_name, bot_id=None):
        query = f"""SELECT * FROM {self.user_bot_table_name} 
                    WHERE primary_user = '{primary_user}' AND BOT_ID LIKE '{bot_name}%'
                    ORDER BY TIMESTAMP DESC
                    LIMIT 1;"""
        knowledge = self.run_query(query, bot_id=bot_id)
        if knowledge:
            return knowledge[0]
        return []
    
    def query_threads_message_log(self, cutoff):
        query = f"""
                WITH K AS (SELECT thread_id, max(last_timestamp) as last_timestamp FROM {self.knowledge_table_name}
                    GROUP BY thread_id),
                M AS (SELECT thread_id, max(timestamp) as timestamp, COUNT(*) as count FROM {self.message_log_table_name} 
                    WHERE PRIMARY_USER IS NOT NULL 
                    GROUP BY thread_id
                    HAVING count > 3)
                SELECT M.thread_id, timestamp as timestamp, COALESCE(K.last_timestamp, DATE('2000-01-01')) as last_timestamp FROM M
                LEFT JOIN K on M.thread_id = K.thread_id
                WHERE timestamp > COALESCE(K.last_timestamp, DATE('2000-01-01')) AND timestamp < TO_TIMESTAMP('{cutoff}');"""
        return self.run_query(query)

    def query_timestamp_message_log(self, thread_id, last_timestamp, max_rows=50):
        query = f"""SELECT * FROM {self.message_log_table_name} 
                        WHERE timestamp > TO_TIMESTAMP('{last_timestamp}') AND
                        thread_id = '{thread_id}'
                        ORDER BY TIMESTAMP;"""
        msg_log = self.run_query(query, max_rows=max_rows)
        return msg_log

    def run_insert(self, table, **kwargs):
        keys = ','.join(kwargs.keys())
        
        insert_query = f"""
            INSERT INTO {table} ({keys})
                VALUES ({','.join(['%s']*len(kwargs))})
            """
        cursor = self.client.cursor()
        cursor.execute(insert_query, tuple(kwargs.values()))
        self.client.commit()
        cursor.close()

    def fetch_embeddings(self, table_id):
        # Initialize Snowflake connector

        # Initialize variables
        batch_size = 100
        offset = 0
        total_fetched = 0

        # Initialize lists to store results
        embeddings = []
        table_names = []
        # update to use embedding_native column if cortex mode
        if os.environ.get("CORTEX_MODE", 'False') == 'True':
            embedding_column = 'embedding_native'
        else:
            embedding_column = 'embedding'
        # First, get the total number of rows to set up the progress bar
        total_rows_query = f"SELECT COUNT(*) as total FROM {table_id} WHERE {embedding_column} IS NOT NULL"
        cursor = self.connection.cursor()
    # print('total rows query: ',total_rows_query)
        cursor.execute(total_rows_query)
        total_rows_result = cursor.fetchone()
        total_rows = total_rows_result[0]

        with tqdm(total=total_rows, desc="Fetching embeddings") as pbar:

            while True:
                # Modify the query to include LIMIT and OFFSET
                query = f"SELECT qualified_table_name, {embedding_column} FROM {table_id} WHERE {embedding_column} IS NOT NULL LIMIT {batch_size} OFFSET {offset}"
    #            print('fetch query ',query)

                cursor.execute(query)
                rows = cursor.fetchall()

                # Temporary lists to hold batch results
                temp_embeddings = []
                temp_table_names = []

                for row in rows:
                    try:
                        temp_embeddings.append(json.loads('['+row[1][5:-3]+']'))
                        temp_table_names.append(row[0])
                        # print('temp_embeddings len: ',len(temp_embeddings))
                        # print('temp table_names: ',temp_table_names)
                    except:
                        try:
                            temp_embeddings.append(json.loads('['+row[1][5:-10]+']'))
                            temp_table_names.append(row[0])
                        except:
                            print('Cant load array from Snowflake')
                    # Assuming qualified_table_name is the first column

                # Check if the batch was empty and exit the loop if so
                if not temp_embeddings:
                    break

                # Append batch results to the main lists
                embeddings.extend(temp_embeddings)
                table_names.extend(temp_table_names)

                # Update counters and progress bar
                fetched = len(temp_embeddings)
                total_fetched += fetched
                pbar.update(fetched)

                if fetched < batch_size:
                    # If less than batch_size rows were fetched, it's the last batch
                    break

                # Increase the offset for the next batch
                offset += batch_size

        cursor.close()
    #   print('table names ',table_names)
    #   print('embeddings len ',len(embeddings))
        return table_names, embeddings

    def generate_filename_from_last_modified(self, table_id):

        database, schema, table = table_id.split('.')

        try:
            # Fetch the maximum LAST_CRAWLED_TIMESTAMP from the harvest_results table
            query = f"SELECT MAX(LAST_CRAWLED_TIMESTAMP) AS last_crawled_time FROM {database}.{schema}.HARVEST_RESULTS"
            cursor = self.connection.cursor()

            cursor.execute(query)
            bots = cursor.fetchall()
            if bots is not None:
                columns = [col[0].lower() for col in cursor.description]
                result = [dict(zip(columns, bot)) for bot in bots]
            else:
                result = None
            cursor.close()


            # Ensure we have a valid result and last_crawled_time is not None
            if not result or result[0]['last_crawled_time'] is None:
                raise ValueError("No data crawled - This is expected on fresh install.")
                return('NO_DATA_CRAWLED')
                #raise ValueError("Table last crawled timestamp is None. Unable to generate filename.")

            # The `last_crawled_time` attribute should be a datetime object. Format it.
            last_crawled_time = result[0]['last_crawled_time']
            timestamp_str = last_crawled_time.strftime("%Y%m%dT%H%M%S") + "Z"

            # Create the filename with the .ann extension
            filename = f"{timestamp_str}.ann"
            metafilename = f"{timestamp_str}.json"
            return filename, metafilename
        except Exception as e:
            # Handle errors: for example, table not found, or API errors
            #print(f"An error occurred: {e}, possibly no data yet harvested, using default name for index file.")
            # Return a default filename or re-raise the exception based on your use case
            return "default_filename.ann", "default_metadata.json"



def test_stage_functions():
    # Create a test instance of SnowflakeConnector
    test_connector = SnowflakeConnector("Snowflake")

    # Call the list_stage method with the specified parameters
    stage_list = test_connector.list_stage_contents(
        database="GENESIS_TEST", schema="GENESIS_INTERNAL", stage="SEMANTIC_STAGE"
    )

    # Print the result
    print(stage_list)

    for file_info in stage_list:
        file_name = file_info["name"].split("/")[-1]  # Extract the file name
        file_size = file_info["size"]
        file_md5 = file_info["md5"]
        file_last_modified = file_info["last_modified"]
        print(f"Reading file: {file_name}")
        print(f"Size: {file_size} bytes")
        print(f"MD5: {file_md5}")
        print(f"Last Modified: {file_last_modified}")
        file_content = test_connector.read_file_from_stage(
            database="GENESIS_TEST",
            schema="GENESIS_INTERNAL",
            stage="SEMANTIC_STAGE",
            file_name=file_name,
            return_contents=True,
        )
        print(file_content)

        # Call the function to write 'tostage.txt' to the stage
    result = test_connector.add_file_to_stage(
        database="GENESIS_TEST",
        schema="GENESIS_INTERNAL",
        stage="SEMANTIC_STAGE",
        file_name="tostage.txt",
    )
    print(result)

    # Read the 'tostage.txt' file from the stage
    tostage_content = test_connector.read_file_from_stage(
        database="GENESIS_TEST",
        schema="GENESIS_INTERNAL",
        stage="SEMANTIC_STAGE",
        file_name="tostage.txt",
        return_contents=True,
    )
    print("Content of 'tostage.txt':")
    print(tostage_content)

    import random
    import string

    # Function to generate a random string of fixed length
    def random_string(length=10):
        letters = string.ascii_letters
        return "".join(random.choice(letters) for i in range(length))

    # Generate a random string
    random_str = random_string()

    # Append the random string to the 'tostage.txt' file
    with open("./stage_files/tostage.txt", "a") as file:
        file.write(f"{random_str}\n")

    print(f"Appended random string to 'tostage.txt': {random_str}")

    # Upload the updated 'tostage.txt' to the stage
    update_result = test_connector.update_file_in_stage(
        database="GENESIS_TEST",
        schema="GENESIS_INTERNAL",
        stage="SEMANTIC_STAGE",
        file_name="tostage.txt",
    )
    print(f"Update result for 'tostage.txt': {update_result}")

    # Read the 'tostage.txt' file from the stage
    new_version_filename = test_connector.read_file_from_stage(
        database="GENESIS_TEST",
        schema="GENESIS_INTERNAL",
        stage="SEMANTIC_STAGE",
        file_name="tostage.txt",
        return_contents=False,
    )

    # Load new_version_contents from the file returned by new_version_filename
    with open("./stage_files/" + new_version_filename, "r") as file:
        new_version_content = file.read()

    # Split the content into lines and check the last line for the random string
    lines = new_version_content.split("\n")
    if (
        lines[-2].strip() == random_str
    ):  # -2 because the last element is an empty string due to the trailing newline
        print("The last line in the new version contains the random string.")
    else:
        print("The second to last line is:", lines[-2])
        print("The last line is:", lines[-1])
        print("The last line in the new version does not contain the random string.")
    # Delete the 'tostage.txt' file from the stage
    delete_result = test_connector.delete_file_from_stage(
        database="GENESIS_TEST",
        schema="GENESIS_INTERNAL",
        stage="SEMANTIC_STAGE",
        file_name="tostage.txt",
    )
    print(f"Delete result for 'tostage.txt': {delete_result}")

    # Re-list the stage contents to confirm deletion of 'tostage.txt'
    stage_list_after_deletion = test_connector.list_stage_contents(
        database="GENESIS_TEST", schema="GENESIS_INTERNAL", stage="SEMANTIC_STAGE"
    )

    # Check if 'tostage.txt' is in the stage list after deletion
    file_names_after_deletion = [
        file_info["name"].split("/")[-1] for file_info in stage_list_after_deletion
    ]
    if "tostage.txt" not in file_names_after_deletion:
        print("'tostage.txt' has been successfully deleted from the stage.")
    else:
        print("Error: 'tostage.txt' is still present in the stage.")
