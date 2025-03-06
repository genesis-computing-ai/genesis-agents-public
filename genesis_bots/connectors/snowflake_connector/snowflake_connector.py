from snowflake.connector import connect, SnowflakeConnection

import os
import json
import uuid
import os
import hashlib
import time
import requests
import pandas as pd
import pytz
import sys
import pkgutil
import inspect
import functools

from datetime import datetime

from genesis_bots.llm.llm_openai.openai_utils import get_openai_client

from .snowflake_connector_base import SnowflakeConnectorBase
from ..connector_helpers import llm_keys_and_types_struct
from ..sqlite_adapter import SQLiteAdapter
from .sematic_model_utils import *
from .stage_utils import add_file_to_stage, read_file_from_stage, update_file_in_stage, delete_file_from_stage, list_stage_contents, test_stage_functions
from .ensure_table_exists import ensure_table_exists, one_time_db_fixes, get_process_info, get_processes_list

from genesis_bots.google_sheets.g_sheets import (
    create_google_sheet_from_export,
)

from genesis_bots.core.bot_os_tools2 import (
    BOT_ID_IMPLICIT_FROM_CONTEXT,
    THREAD_ID_IMPLICIT_FROM_CONTEXT,
    ToolFuncGroup,
    ToolFuncParamDescriptor,
    gc_tool,
)

from genesis_bots.core.bot_os_llm import BotLlmEngineEnum

# from database_connector import DatabaseConnector
from threading import Lock
import base64
import requests
import re
from tqdm import tqdm
from textwrap import dedent

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
import jwt

from genesis_bots.core.logging_config import logger

def dict_list_to_markdown_table(data):
    """
    Convert a list of dictionaries to a Markdown table string.
    Args:
        data (list): The list of dictionaries to convert.
    Returns:
        str: The Markdown table string.
    """
    if not data:
        return ""

    headers = list(data[0].keys())

    table = "| " + " | ".join(headers) + " |\n"
    table += "| " + " | ".join(["---"] * len(headers)) + " |\n"

    for row in data:
        table += "| " + " | ".join(map(str, row.values())) + " |\n"

    return table

class SnowflakeConnector(SnowflakeConnectorBase):
    def __init__(self, connection_name, bot_database_creds=None):
        super().__init__()

        if not os.getenv("GENESIS_INTERNAL_DB_SCHEMA") and os.getenv("SNOWFLAKE_METADATA", "FALSE").upper() != "TRUE":
            os.environ["GENESIS_INTERNAL_DB_SCHEMA"] = "NONE.NONE"

        # used to get the default value if not none, otherwise get env var. allows local mode to work with bot credentials
        def get_env_or_default(value, env_var):
            return value if value is not None else os.getenv(env_var)

        if os.getenv("SNOWFLAKE_METADATA", "False").lower() == "false":
            # Use SQLite with compatibility layer
            # Set default LLM engine to openai if not specified
            logger.warning('Using SQLite for connection...')
            if not os.getenv("BOT_OS_DEFAULT_LLM_ENGINE"):
                os.environ["BOT_OS_DEFAULT_LLM_ENGINE"] = "openai"
            db_path = os.getenv("SQLITE_DB_PATH", "genesis.db")
            self.client = SQLiteAdapter(db_path)
            self.connection = self.client
            # Set other required attributes
            self.schema = "main"  # SQLite default schema
            self.database = db_path
            self.source_name = "SQLite"
            self.user = "local"
            self.role = 'default'
        else:
            logger.info('Using Snowflake for connection...')
            account, database, user, password, warehouse, role = [None] * 6

            if bot_database_creds:
                account = bot_database_creds.get("account")
                database = bot_database_creds.get("database")
                user = bot_database_creds.get("user")
                password = bot_database_creds.get("pwd")
                warehouse = bot_database_creds.get("warehouse")
                role = bot_database_creds.get("role")

            self.account = get_env_or_default(account, "SNOWFLAKE_ACCOUNT_OVERRIDE")
            self.user = get_env_or_default(user, "SNOWFLAKE_USER_OVERRIDE")
            self.password = get_env_or_default(password, "SNOWFLAKE_PASSWORD_OVERRIDE")
            self.database = get_env_or_default(database, "SNOWFLAKE_DATABASE_OVERRIDE")
            self.warehouse = get_env_or_default(warehouse, "SNOWFLAKE_WAREHOUSE_OVERRIDE")
            self.role = get_env_or_default(role, "SNOWFLAKE_ROLE_OVERRIDE")
            self.source_name = "Snowflake"

            self.default_data = pd.DataFrame()

            # logger.info('Calling _create_connection...')
            self.token_connection = False
            self.connection: SnowflakeConnection = self._create_connection()

            self.semantic_models_map = {}

            self.client = self.connection

            self.schema = os.getenv("GENESIS_INTERNAL_DB_SCHEMA", "GENESIS_INTERNAL")

        self.llm_engine = os.getenv("CORTEX_PREMIERE_MODEL") or os.getenv("CORTEX_MODEL") or 'claude-3-5-sonnet'

        self.genbot_internal_project_and_schema = os.getenv("GENESIS_INTERNAL_DB_SCHEMA", "None")
        if self.genbot_internal_project_and_schema == "None":
            # Todo remove, internal note
            logger.info("ENV Variable GENESIS_INTERNAL_DB_SCHEMA is not set.")
        if self.genbot_internal_project_and_schema is not None:
            self.genbot_internal_project_and_schema = (self.genbot_internal_project_and_schema.upper() )

        if self.database:
            self.project_id = self.database
        else:
            db, sch = self.genbot_internal_project_and_schema.split('.')
            self.project_id = db

        self.genbot_internal_harvest_table = os.getenv("GENESIS_INTERNAL_HARVEST_RESULTS_TABLE", "harvest_results" )
        self.genbot_internal_harvest_control_table = os.getenv("GENESIS_INTERNAL_HARVEST_CONTROL_TABLE", "harvest_control")
        self.genbot_internal_processes_table = os.getenv("GENESIS_INTERNAL_PROCESSES_TABLE", "PROCESSES" )
        self.genbot_internal_process_history_table = os.getenv("GENESIS_INTERNAL_PROCESS_HISTORY_TABLE", "PROCESS_HISTORY" )
        self.app_share_schema = "APP_SHARE"

        # logger.info("genbot_internal_project_and_schema: ", self.genbot_internal_project_and_schema)
        self.metadata_table_name = self.genbot_internal_project_and_schema+ "."+ self.genbot_internal_harvest_table
        self.harvest_control_table_name = self.genbot_internal_project_and_schema + "."+ self.genbot_internal_harvest_control_table
        self.message_log_table_name = self.genbot_internal_project_and_schema+ "."+ os.getenv("GENESIS_INTERNAL_MESSAGE_LOG_TABLE", "MESSAGE_LOG")
        self.knowledge_table_name = self.genbot_internal_project_and_schema+ "."+ os.getenv("GENESIS_INTERNAL_KNOWLEDGE_TABLE", "KNOWLEDGE")
        self.processes_table_name = self.genbot_internal_project_and_schema+ "."+ self.genbot_internal_processes_table
        self.process_history_table_name = self.genbot_internal_project_and_schema+ "."+ self.genbot_internal_process_history_table
        self.user_bot_table_name = self.genbot_internal_project_and_schema+ "."+ os.getenv("GENESIS_INTERNAL_USER_BOT_TABLE", "USER_BOT")
        self.tool_knowledge_table_name = self.genbot_internal_project_and_schema+ "."+ os.getenv("GENESIS_INTERNAL_TOOL_KNOWLEDGE_TABLE", "TOOL_KNOWLEDGE")
        self.data_knowledge_table_name = self.genbot_internal_project_and_schema+ "."+ os.getenv("GENESIS_INTERNAL_DATA_KNOWLEDGE_TABLE", "DATA_KNOWLEDGE")
        self.proc_knowledge_table_name = self.genbot_internal_project_and_schema+ "."+ os.getenv("GENESIS_INTERNAL_PROC_KNOWLEDGE_TABLE", "PROC_KNOWLEDGE")
        self.slack_tokens_table_name = self.genbot_internal_project_and_schema + "." + "SLACK_APP_CONFIG_TOKENS"
        self.available_tools_table_name = self.genbot_internal_project_and_schema + "." + "AVAILABLE_TOOLS"
        self.bot_servicing_table_name = self.genbot_internal_project_and_schema + "." + "BOT_SERVICING"
        self.ngrok_tokens_table_name = self.genbot_internal_project_and_schema + "." + "NGROK_TOKENS"
        self.cust_db_connections_table_name = self.genbot_internal_project_and_schema + "." + "CUST_DB_CONNECTIONS"
        self.images_table_name = self.app_share_schema + "." + "IMAGES"

    def ensure_table_exists(self):
        return ensure_table_exists(self)

    def one_time_db_fixes(self):
        return one_time_db_fixes(self)

    def get_processes_list(self, bot_id='all'):
        return get_processes_list(self,bot_id)

    def get_process_info(self, bot_id, process_name):
        return get_process_info(self, bot_id, process_name)

    def add_file_to_stage(
        self,
        database: str = None,
        schema: str = None,
        stage: str = None,
        openai_file_id: str = None,
        file_name: str = None,
        file_content: str = None,
        thread_id=None,
        bot_id=None,
    ):
        return add_file_to_stage(self, database,schema,stage,openai_file_id,file_name,file_content,thread_id)

    def read_file_from_stage(self, database, schema, stage, file_name, return_contents=True,is_binary=False,for_bot=None,thread_id=None, bot_id=None):
        return read_file_from_stage(self, database, schema, stage, file_name, return_contents, is_binary, for_bot, thread_id)
    def update_file_in_stage(self, database=None, schema=None, stage=None, file_name=None, thread_id=None, bot_id=None):
        return update_file_in_stage(self, database, schema, stage, file_name, thread_id)

    def delete_file_from_stage(self, database=None, schema=None, stage=None, file_name=None, thread_id=None, bot_id=None):
        return delete_file_from_stage(self, database, schema, stage, file_name, thread_id)

    def list_stage_contents(self, database=None, schema=None, stage=None, pattern=None, thread_id=None, bot_id=None):
        return list_stage_contents(self, database, schema, stage, pattern, thread_id)

    def test_stage_functions():
        return test_stage_functions()

    @functools.cached_property
    def is_using_local_runner(self):
        val = os.environ.get('SPCS_MODE', 'FALSE')
        if val.lower() == 'true':
            return False
        else:
            return True

    # def process_scheduler(self,action, bot_id, task_id=None, task_details=None, thread_id=None, history_rows=10):
    #     process_scheduler(self, action, bot_id, task_id=None, task_details=None, thread_id=None, history_rows=10)

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
        if self.source_name == "Snowflake" and os.getenv("CORTEX_AVAILABLE", "False").lower() == 'true':
            return True
        else:
            return False

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
            logger.info(f"Cortex not available: {e}")
            sp_session = None
        return sp_session

    def _cortex_complete(self, model="llama3.1-405b", prompt=None):
        try:
            from snowflake.cortex import Complete

            result = Complete(model, str(prompt))
        except Exception as e:
            logger.info(f"Cortex not available: {e}")
            self.sp_session = None
            result = None
        return result

    def sha256_hash_hex_string(self, input_string):
        # Encode the input string to bytes, then create a SHA256 hash and convert it to a hexadecimal string
        return hashlib.sha256(input_string.encode()).hexdigest()

    def get_harvest_control_data_as_json(self, thread_id=None, bot_id=None):
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
        connection_id = None,
        database_name = None,
        initial_crawl_complete=False,
        refresh_interval=1,
        schema_exclusions=None,
        schema_inclusions=None,
        status="Include",
        thread_id=None,
        source_name=None,
        bot_id=None,
    ):
        """
        Inserts or updates a row in the harvest control table using simple SQL statements.

        Args:
            source_name (str): The source name for the harvest control data.
            database_name (str): The database name for the harvest control data.
            initial_crawl_complete (bool): Flag indicating if the initial crawl is complete. Defaults to False.
            refresh_interval (int): The interval at which the data is refreshed. Defaults to 1.
            schema_exclusions (list): A list of schema names to exclude. Defaults to an empty list.
            schema_inclusions (list): A list of schema names to include. Defaults to an empty list.
            status (str): The status of the harvest control. Defaults to 'Include'.
        """
        if source_name is not None and connection_id is None:
            connection_id = source_name
        source_name = connection_id
        try:
            # Set default values for schema_exclusions and schema_inclusions if None
            if schema_exclusions is None:
                schema_exclusions = []
            if schema_inclusions is None:
                schema_inclusions = []

            # Validate database and schema names for Snowflake source
            if source_name == 'Snowflake' and self.source_name == 'Snowflake':
                databases = self.get_visible_databases()
                if database_name not in databases:
                    return {
                        "Success": False,
                        "Error": f"Database {database_name} does not exist.",
                    }

                schemas = self.get_schemas(database_name)
                for schema in schema_exclusions:
                    if schema.upper() not in (s.upper() for s in schemas):
                        return {
                            "Success": False,
                            "Error": f"Schema exclusion {schema} does not exist in database {database_name}.",
                        }
                for schema in schema_inclusions:
                    if schema.upper() not in (s.upper() for s in schemas):
                        return {
                            "Success": False,
                            "Error": f"Schema inclusion {schema} does not exist in database {database_name}.",
                        }

                # Match case with existing database and schema names
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
            else:
                # For non-Snowflake sources, validate the connection_id exists
                from genesis_bots.connectors.data_connector import DatabaseConnector
                connector = DatabaseConnector()
                connections = connector.list_database_connections(bot_id=bot_id)
                if not connections['success']:
                    return {
                        "Success": False,
                        "Error": f"Failed to validate connection: {connections.get('error')}",
                    }

                valid_connections = [c['connection_id'] for c in connections['connections']]
                if connection_id not in valid_connections:
                    return {
                        "Success": False,
                        "Error": f"Connection '{connection_id}' not found. Please add it first using the database connection tools.",
                        "Valid Connections": str(valid_connections)
                    }

            if self.source_name != 'Snowflake':
                # Check if record exists
                check_query = f"""
                SELECT COUNT(*)
                FROM {self.harvest_control_table_name}
                WHERE source_name = %s AND database_name = %s
                """
                cursor = self.client.cursor()
                cursor.execute(check_query, (source_name, database_name))
                exists = cursor.fetchone()[0] > 0

                if exists:
                    # Update existing record
                    if self.source_name != 'Snowflake':
                        update_query = f"""
                        UPDATE {self.harvest_control_table_name}
                        SET initial_crawl_complete = %s,
                            refresh_interval = %s,
                            schema_exclusions = %s,
                            schema_inclusions = %s,
                            status = %s
                        WHERE source_name = %s AND database_name = %s
                        """
                        schema_exclusions = str(schema_exclusions)
                        schema_inclusions = str(schema_inclusions)
                        cursor.execute(
                        update_query,
                        (
                            initial_crawl_complete,
                            refresh_interval,
                            schema_exclusions,
                            schema_inclusions,
                            status,
                            source_name,
                            database_name,
                        ),
                    )
                else:
                    # Insert new record
                    insert_query = f"""
                    INSERT INTO {self.harvest_control_table_name}
                    (source_name, database_name, initial_crawl_complete, refresh_interval,
                    schema_exclusions, schema_inclusions, status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """
                    if self.source_name != 'Snowflake':
                        schema_exclusions = str(schema_exclusions)
                        schema_inclusions = str(schema_inclusions)
                    cursor.execute(
                        insert_query,
                        (
                        source_name,
                        database_name,
                        initial_crawl_complete,
                        refresh_interval,
                        schema_exclusions,
                        schema_inclusions,
                        status,
                    ),
                )
            else:
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



            self.client.commit()

            # Trigger immediate harvest after successful update - don't wait for result
            try:
                from genesis_bots.demo.app.genesis_app import genesis_app
                if hasattr(genesis_app, 'scheduler'):
                    genesis_app.scheduler.modify_job(
                        'harvester_job',
                        next_run_time=datetime.datetime.now()
                    )
            except Exception as e:
                logger.info(f"Non-critical error triggering immediate harvest: {e}")

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

    def get_shared_schemas(self, database_name):
        try:
            query = f"SELECT DISTINCT SCHEMA_NAME FROM {self.metadata_table_name} where DATABASE_NAME = '{database_name}'"
            cursor = self.client.cursor()
            cursor.execute(query)
            schemas = cursor.fetchall()
            schema_list = [schema[0] for schema in schemas]
            # for schema in schema_list:
            #     logger.info(f"can we see baseball and f1?? {schema}")
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
            runner_id = os.getenv("RUNNER_ID", "jl-local-runner")
            query = f"""
        SELECT LLM_TYPE, ACTIVE, LLM_KEY, LLM_ENDPOINT
        FROM {self.genbot_internal_project_and_schema}.LLM_TOKENS
        WHERE LLM_KEY is not NULL
        AND   RUNNER_ID = '{runner_id}'
        """
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
            err = f"An error occurred while getting llm info: {e}"
            return {"Success": False, "Error": err}

    def check_eai_assigned(self):
        """
        Retrieves the eai list if set.

        Returns:
            list: An eai list, if set.
        """
        try:
            show_query = f"SHOW SERVICES IN SCHEMA {self.schema}"
            cursor = self.client.cursor()
            cursor.execute(show_query)

            query = f"""SELECT DISTINCT UPPER("external_access_integrations") EAI_LIST FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) LIMIT 1"""
            cursor = self.client.cursor()
            cursor.execute(query)
            eai_info = cursor.fetchone()

            # Ensure eai_info is not None
            if eai_info:
                columns = [col[0].lower() for col in cursor.description]
                eai_list = [dict(zip(columns, eai_info))]  # Wrap eai_info in a list since fetchone returns a single row
                json_data = json.dumps(eai_list)
            else:
                json_data = json.dumps([])  # Return an empty list if no results

            return {"Success": True, "Data": json_data}

        except Exception as e:
            err = f"An error occurred while getting email address: {e}"
            return {"Success": False, "Error": err}

    def get_endpoints(self):
        """
        Retrieves a list of all custom endpoints.

        Returns:
            list: A list of custom endpionts.
        """
        try:
            query = dedent(f"""
                SELECT LISTAGG(ENDPOINT, ', ') WITHIN GROUP (ORDER BY ENDPOINT) AS ENDPOINTS, GROUP_NAME
                FROM {self.genbot_internal_project_and_schema}.CUSTOM_ENDPOINTS
                WHERE TYPE = 'CUSTOM'
                GROUP BY GROUP_NAME
                ORDER BY GROUP_NAME
                """)
            cursor = self.client.cursor()
            cursor.execute(query)
            endpoints = cursor.fetchall()
            columns = [col[0].lower() for col in cursor.description]
            endpoint_list = [dict(zip(columns, endpoint)) for endpoint in endpoints]
            json_data = json.dumps(
                endpoint_list, default=str
            )

            return {"Success": True, "Data": json_data}

        except Exception as e:
            err = f"An error occurred while getting llm info: {e}"
            return {"Success": False, "Error": err}

    def delete_endpoint_group(self, group_name):
        try:
            delete_query = f"""DELETE FROM {self.genbot_internal_project_and_schema}.CUSTOM_ENDPOINTS WHERE GROUP_NAME = %s;"""
            cursor = self.client.cursor()
            cursor.execute(delete_query, (group_name,))

            # Commit the changes
            self.client.commit()

            json_data = json.dumps([{'Success': True}])
            return {"Success": True, "Data": json_data}
        except Exception as e:
            err = f"An error occurred while deleting custom endpoint: {e}"
            return {"Success": False, "Data": err}

    def set_endpoint(self, group_name, endpoint_name, type):
        try:
            insert_query = f"""INSERT INTO {self.genbot_internal_project_and_schema}.CUSTOM_ENDPOINTS (GROUP_NAME, ENDPOINT, TYPE)
            SELECT %s AS group_name, %s AS endpoint, %s AS type
            WHERE NOT EXISTS (
                SELECT 1
                FROM {self.genbot_internal_project_and_schema}.CUSTOM_ENDPOINTS
                WHERE GROUP_NAME = %s
                AND ENDPOINT = %s
                AND TYPE = %s
            );"""
            cursor = self.client.cursor()
            cursor.execute(insert_query, (group_name, endpoint_name, type, group_name, endpoint_name, type,))

            # Commit the changes
            self.client.commit()

            json_data = json.dumps([{'Success': True}])
            return {"Success": True, "Data": json_data}
        except Exception as e:
            err = f"An error occurred while inserting custom endpoint: {e}"
            return {"Success": False, "Data": err}

    def get_jira_config_params(self):
        """
        Retrieves a list of all custom endpoints.

        Returns:
            list: A list of custom endpionts.
        """
        try:

            query = f"SELECT parameter, value FROM {self.schema}.EXT_SERVICE_CONFIG WHERE ext_service_name = 'jira';"
            cursor = self.client.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()

            if not rows:
                return False

            jira_params_list = [dict(zip(["parameter", "value"], row)) for row in rows]
            json_data = json.dumps(
                jira_params_list, default=str
            )

            return {"Success": True, "Data": json_data}

        except Exception as e:
            err = f"An error occurred while getting jira info: {e}"
            return {"Success": False, "Error": err}

    def get_github_config_params(self):
        """
        Retrieves GitHub configuration parameters from the database.

        Returns:
            dict: A dictionary containing GitHub configuration parameters.
        """
        try:
            query = f"SELECT parameter, value FROM {self.schema}.EXT_SERVICE_CONFIG WHERE ext_service_name = 'github';"
            cursor = self.client.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()

            if not rows:
                return {"Success": False, "Error": "No GitHub configuration found"}

            github_params_list = [dict(zip(["parameter", "value"], row)) for row in rows]
            json_data = json.dumps(github_params_list, default=str)

            return {"Success": True, "Data": json_data}

        except Exception as e:
            err = f"An error occurred while getting GitHub config params: {e}"
            return {"Success": False, "Error": err}

    def set_api_config_params(self, service_name, key_pairs_str):
        try:

            cursor = self.client.cursor()
            key_pairs = json.loads(key_pairs_str)

            for key, value in key_pairs.items():
                if isinstance(value, str):

                    if key == 'private_key':
                        value = value.replace("\\n", "&")
                    else:
                        value = value.replace("\n", "")


                    # Check if record exists
                    check_query = f"""
                    SELECT COUNT(*) FROM {self.genbot_internal_project_and_schema}.EXT_SERVICE_CONFIG
                    WHERE ext_service_name = '{service_name}' AND parameter = '{key}'
                    """
                    cursor.execute(check_query)
                    exists = cursor.fetchone()[0] > 0

                    if exists:
                        # Update existing record
                        update_query = f"""
                        UPDATE {self.genbot_internal_project_and_schema}.EXT_SERVICE_CONFIG
                        SET value = '{value}',
                            updated = CURRENT_TIMESTAMP()
                        WHERE ext_service_name = '{service_name}'
                        AND parameter = '{key}'
                        """
                        cursor.execute(update_query)
                    else:
                        # Insert new record
                        insert_query = dedent(f"""
                        INSERT INTO {self.genbot_internal_project_and_schema}.EXT_SERVICE_CONFIG
                        (ext_service_name, parameter, value, user, created, updated)
                        VALUES ('{service_name}', '{key}', '{value}', '{self.user}',
                        CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
                        """)
                        cursor.execute(insert_query)

                # Commit the changes
                self.client.commit()

            if service_name == 'g-sheets':
                self.create_google_sheets_creds()

            json_data = json.dumps([{'Success': True}])
            return {"Success": True, "Data": json_data}
        except Exception as e:
            err = f"An error occurred while inserting {service_name} api config params: {e}"
            return {"Success": False, "Data": err}

    def create_google_sheets_oauth_creds(self):
        hard_coded_email = 'jeff.davidson@genesiscomputing.ai'
        query = f"SELECT parameter, value FROM {self.schema}.EXT_SERVICE_CONFIG WHERE ext_service_name = 'g-drive-oauth2' and user='{hard_coded_email}';"
        cursor = self.client.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()

        if not rows:
            return False

        creds_dict = {row[0]: row[1] for row in rows if row[0].casefold() != "shared_folder_id"}

        # creds_dict["private_key"] = creds_dict.get("private_key","").replace("&", "\n")

        creds_json = json.dumps(creds_dict, indent=4)
        with open(f'g-workspace-sa-credentials.json', 'w') as json_file:
            json_file.write(creds_json)
        return True

    def create_google_sheets_creds(self):
        query = f"SELECT parameter, value FROM {self.schema}.EXT_SERVICE_CONFIG WHERE ext_service_name = 'g-sheets';"

        # # TEMP PATCH TO SWITCH USER SINCE self.user is not being set
        # query = f"SELECT parameter, value FROM {self.schema}.EXT_SERVICE_CONFIG WHERE (ext_service_name = 'g-sheets' AND user = 'Jeff') OR (ext_service_name = 'g-sheets' AND user != 'Justin');"

        cursor = self.client.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()

        if not rows:
            return False

        creds_dict = {row[0]: row[1] for row in rows if row[0].casefold() != "shared_folder_id"}

        creds_dict["private_key"] = creds_dict.get("private_key","").replace("&", "\n")

        creds_json = json.dumps(creds_dict, indent=4)
        with open(f'g-workspace-sa-credentials.json', 'w') as json_file:
            json_file.write(creds_json)


        return True

    def create_g_drive_oauth_creds(self):
        temp_hard_code = "jeff.davidson@genesiscomputing.ai"
        query = f"SELECT parameter, value FROM {self.schema}.EXT_SERVICE_CONFIG WHERE ext_service_name = 'g-drive-oauth2' and user='{temp_hard_code}';"
        cursor = self.client.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()

        if not rows:
            return False

        creds_dict = {row[0]: row[1] for row in rows}

        if 'redirect_uris' in creds_dict:
            try:
                # First, parse the string as JSON
                redirect_uris = json.loads(creds_dict['redirect_uris'])
                # Update the dictionary with the parsed array
                creds_dict['redirect_uris'] = redirect_uris
            except json.JSONDecodeError as e:
                logger.error(f"Error parsing redirect_uris: {e}")

        os.environ['GOOGLE_CLOUD_PROJECT'] = creds_dict['project_id'] # 'genesis-workspace-project'

        wrapped_creds_dict = {"web": creds_dict}

        creds_json = json.dumps(wrapped_creds_dict, indent=4)
        with open(f'google_oauth_credentials.json', 'w') as json_file:
            json_file.write(creds_json)
        return True

    def get_model_params(self):
        """
        Retrieves the model and embedding model names for the active LLM from the database.

        Returns:
            dict: A dictionary containing:
                - Success (bool): Whether the operation was successful
                - Data (str): JSON string containing model_name and embedding_model_name if successful,
                            or error message if unsuccessful
        """
        runner_id = os.getenv("RUNNER_ID", "jl-local-runner")

        try:
            if self.source_name.lower() == "snowflake":
                query = f"""
                SELECT model_name, embedding_model_name
                FROM {self.genbot_internal_project_and_schema}.LLM_TOKENS
                WHERE runner_id = %s AND llm_type = 'openai'
                """
                cursor = self.client.cursor()
                cursor.execute(query, (runner_id,))
                result = cursor.fetchone()
            else:
                query = """
                SELECT model_name, embedding_model_name
                FROM llm_tokens
                WHERE runner_id = ? AND llm_type = 'openai'
                """
                cursor = self.client.cursor()
                cursor.execute(query, (runner_id,))
                result = cursor.fetchone()

            if result:
                model_name, embedding_model_name = result
                json_data = json.dumps({
                    'model_name': model_name,
                    'embedding_model_name': embedding_model_name
                })
                return {"Success": True, "Data": json_data}
            else:
                return {"Success": False, "Data": "No active model parameters found"}

        except Exception as e:
            err = f"An error occurred while retrieving model parameters: {e}"
            return {"Success": False, "Data": err}



    def update_model_params(self, model_name, embedding_model_name):
        """
        Updates or inserts the model and embedding model names for the LLM in the database.

        This method performs a SQL MERGE operation to update the LLM model name and embedding model name
        if a record with the same LLM type ('openai') exists, or inserts a new record if not.

        Args:
            model_name (str): The name of the LLM model to set or update.
            embedding_model_name (str): The name of the embedding model to set or update.

        Returns:
            dict: A dictionary containing the success status and the resulting data.
                If successful, returns {"Success": True, "Data": json_data}, where `json_data` is
                a JSON string indicating success.
                If an error occurs, returns {"Success": False, "Data": err}, where `err` contains the error message.

        Raises:
            Exception: If an error occurs during the SQL execution or database commit.
        """

        runner_id = os.getenv("RUNNER_ID", "jl-local-runner")

        if self.source_name.lower() == "snowflake":
            try:

                upsert_query = dedent(f"""
                MERGE INTO {self.genbot_internal_project_and_schema}.LLM_TOKENS t
                USING (SELECT %s AS llm_type, %s AS model_name, %s AS embedding_model_name, %s AS runner_id) s
                ON (t.LLM_TYPE = s.llm_type AND t.RUNNER_ID = s.runner_id)
                WHEN MATCHED THEN
                    UPDATE SET t.MODEL_NAME = s.model_name, t.EMBEDDING_MODEL_NAME = s.embedding_model_name
                WHEN NOT MATCHED THEN
                    INSERT (MODEL_NAME, EMBEDDING_MODEL_NAME, LLM_TYPE, RUNNER_ID)
                    VALUES (s.model_name, s.embedding_model_name, s.llm_type, s.runner_id)
                """)

                cursor = self.client.cursor()
                cursor.execute(upsert_query, ('openai', model_name, embedding_model_name,runner_id))

                # Commit the changes
                self.client.commit()

                json_data = json.dumps([{'Success': True}])
                return {"Success": True, "Data": json_data}
            except Exception as e:
                err = f"An error occurred while inserting model names: {e}"
                return {"Success": False, "Data": err}
            finally:
                if cursor is not None:
                    cursor.close()
        else:
            try:
                cursor = self.client.cursor()

                # First check if record exists
                select_query = f"""
                    SELECT 1
                    FROM {self.genbot_internal_project_and_schema}.LLM_TOKENS
                    WHERE LLM_TYPE = %s AND RUNNER_ID = %s
                """
                cursor.execute(select_query, ('openai', runner_id))
                exists = cursor.fetchone() is not None

                if exists:
                    # Update existing record
                    update_query = f"""
                        UPDATE {self.genbot_internal_project_and_schema}.LLM_TOKENS
                        SET MODEL_NAME = %s,
                            EMBEDDING_MODEL_NAME = %s
                        WHERE LLM_TYPE = %s AND RUNNER_ID = %s
                    """
                    cursor.execute(update_query, (model_name, embedding_model_name, 'openai', runner_id))
                else:
                    # Insert new record
                    insert_query = f"""
                        INSERT INTO {self.genbot_internal_project_and_schema}.LLM_TOKENS
                        (MODEL_NAME, EMBEDDING_MODEL_NAME, LLM_TYPE, RUNNER_ID)
                        VALUES (%s, %s, %s, %s)
                    """
                    cursor.execute(insert_query, (model_name, embedding_model_name, 'openai', runner_id))

                # Commit the changes
                self.client.commit()

                json_data = json.dumps([{'Success': True}])
                return {"Success": True, "Data": json_data}
            except Exception as e:
                err = f"An error occurred while inserting model names: {e}"
                return {"Success": False, "Data": err}
            finally:
                if cursor is not None:
                    cursor.close()

    def eai_test(self, site):
        try:
            azure_endpoint = "https://example.com"
            eai_list_query = f"""CALL CORE.GET_EAI_LIST('{self.schema}')"""
            cursor = self.client.cursor()
            cursor.execute(eai_list_query)
            eai_list = cursor.fetchone()
            if not eai_list:
                return {"Success": False, "Error": "Cannot check EAI status. No EAI set up."}
            else:

                if site == "azureopenai":
                    azure_query = f"""
                        SELECT LLM_ENDPOINT
                        FROM {self.genbot_internal_project_and_schema}.LLM_TOKENS
                        WHERE UPPER(LLM_TYPE) = 'OPENAI'"""
                    cursor = self.client.cursor()
                    cursor.execute(azure_query)
                    azure_endpoint = cursor.fetchone()
                    if azure_endpoint is None or azure_endpoint == '':
                        azure_endpoint = "https://example.com"

                create_function_query = f"""
CREATE OR REPLACE FUNCTION {self.project_id}.CORE.CHECK_URL_STATUS(site string)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = 3.11
HANDLER = 'get_status'
EXTERNAL_ACCESS_INTEGRATIONS = ({eai_list[0]})
PACKAGES = ('requests')
AS
$$
import requests

def get_status(site):
    check_command = "options"

    if site == 'slack':
        url = "https://slack.com"  # Replace with the allowed URL
    elif site == 'openai':
        url = "https://api.openai.com/v1/models"  # Replace with the allowed URL
    elif site == 'google':
        url = "https://accounts.google.com"  # Replace with the allowed URL
        check_command = "put"
    elif site == 'jira':
        url = "https://www.atlassian.net/jira/your-work"  # Replace with the allowed URL
    elif site == 'serper':
        url = "https://google.serper.dev"  # Replace with the allowed URL
    elif site == 'azureopenai':
        url = "{azure_endpoint}"  # Replace with the allowed URL
    else:
        # TODO allow custom endpoints to be tested
        return f"Invalid site: {{site}}"

    try:
        # Make an HTTP GET request to the allowed URL
        # response = requests.get(url, timeout=10)
        if check_command == "options":
            response = requests.options(url)
        else:
            response = requests.put(url)
        if response.ok or response.status_code == 302:   # alternatively you can use response.status_code == 200
            result = "Success"
        else:
            result = f"Failure"
    except (requests.exceptions.HTTPError, requests.exceptions.ConnectionError) as e:
        result = f"Failure - Unable to establish connection: {{e}}."
    except Exception as e:
        result = f"Failure - Unknown error occurred: {{e}}."

    return result
    $$;
                """
                try:
                    function_success = False
                    cursor = self.client.cursor()
                    cursor.execute(create_function_query)

                    if site:
                        function_test_success = False
                        select_query = f"select {self.project_id}.CORE.CHECK_URL_STATUS('{site}')"
                        cursor.execute(select_query)
                        eai_test_result = cursor.fetchone()

                        if 'Success' in eai_test_result:
                            function_test_success = True
                except Exception as e:
                    logger.info(f"An error occurred while creating/testing EAI test function: {e}")
                    function_test_success = True

                # check for existing EAI assigned to services
                show_query = f"show services in application {self.project_id}"
                cursor.execute(show_query)
                check_eai_query = """
                                    SELECT f.VALUE::string FROM table(result_scan(-1)) a,
                                    LATERAL FLATTEN(input => parse_json(a."external_access_integrations")) AS f
                                    WHERE "name" = 'GENESISAPP_SERVICE_SERVICE';
                                """

                cursor.execute(check_eai_query)
                check_eai_result = cursor.fetchone()

                if check_eai_result:
                    function_success = True

        except Exception as e:
            err = f"An error occurred while creating/testing EAI test function: {e}"
            return {"Success": False, "Error": err}

        if function_success == True and function_test_success == True:
            json_data = json.dumps([{'Success': True}])
            return {"Success": True, "Data": json_data}
        else:
            return {"Success": False, "Error": "EAI test failed or EAI not assigned to Genesis"}

    def get_email(self):
        """
        Retrieves the email address if set.

        Returns:
            list: An email address, if set.
        """
        try:
            # Check if DEFAULT_EMAIL table exists
            check_table_query = f"SHOW TABLES LIKE 'DEFAULT_EMAIL' IN {self.genbot_internal_project_and_schema}"
            cursor = self.client.cursor()
            cursor.execute(check_table_query)
            table_exists = cursor.fetchone() is not None

            if not table_exists:
                return {"Success": False, "Error": "Default email is not set because the DEFAULT_EMAIL table does not exist."}

            query = f"SELECT DEFAULT_EMAIL FROM {self.genbot_internal_project_and_schema}.DEFAULT_EMAIL"
            cursor.execute(query)
            email_info = cursor.fetchall()
            columns = [col[0].lower() for col in cursor.description]
            email_list = [dict(zip(columns, email)) for email in email_info]
            json_data = json.dumps(
                email_list, default=str
            )  # default=str to handle datetime and other non-serializable types

            return {"Success": True, "Data": json_data}

        except Exception as e:
            err = f"An error occurred while getting email address: {e}"
            return {"Success": False, "Error": err}

    def send_test_email(self, email_addr, thread_id=None):
        """
        Tests sending an email and stores the email address in a table.

        Returns:
            json: success or failure.
        """
        try:

            query = f"""
                CALL SYSTEM$SEND_EMAIL(
                    'genesis_email_int',
                    $${email_addr}$$,
                    $${'Test Email'}$$,
                    $${'Test Email from Genesis Server'}$$
                );
                """

            cursor = self.client.cursor()
            cursor.execute(query)
            email_result = cursor.fetchall()

            columns = [col[0].lower() for col in cursor.description]
            email_result = [dict(zip(columns, row)) for row in email_result]
            json_data = json.dumps(
                email_result, default=str
            )  # default=str to handle datetime and other non-serializable types

            # Check if DEFAULT_EMAIL table exists using SHOW TABLES LIKE
            check_table_query = f"""
            SHOW TABLES LIKE 'DEFAULT_EMAIL' IN {self.genbot_internal_project_and_schema}
            """
            cursor.execute(check_table_query)
            table_exists = cursor.fetchone() is not None

            if not table_exists:
                # Create the table if it doesn't exist
                create_table_query = f"""
                CREATE TABLE {self.genbot_internal_project_and_schema}.DEFAULT_EMAIL (
                    DEFAULT_EMAIL VARCHAR(255)
                )
                """
                cursor.execute(create_table_query)

                # Insert or update the default email
                upsert_query = f"""
                MERGE INTO {self.genbot_internal_project_and_schema}.DEFAULT_EMAIL t
                USING (SELECT %s AS email) s
                ON (1=1)
                WHEN MATCHED THEN
                    UPDATE SET t.DEFAULT_EMAIL = s.email
                WHEN NOT MATCHED THEN
                    INSERT (DEFAULT_EMAIL) VALUES (s.email)
                """
                cursor.execute(upsert_query, (email_addr,))

                # Commit the changes
                self.client.commit()

            return {"Success": True, "Data": json_data}

        except Exception as e:
            err = f"An error occurred while sending test email: {e}"
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
            logger.info(f"An error occurred while checking if the table summary exists: {e}")
            return False

    def check_logging_status(self):
        query = f"""
        CALL {self.project_id}.CORE.CHECK_APPLICATION_SHARING()
        """
        try:
            cursor = self.client.cursor()
            cursor.execute(query)
            result = cursor.fetchone()

            return result[0]  # Returns True, False otherwise
        except Exception as e:
            logger.info(f"An error occurred while checking logging status: {e}")
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
        from datetime import datetime
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
            logger.info(
                f"Encountered errors while inserting into chat history table row: {e}"
            )
        finally:
            if cursor is not None:
                cursor.close()

    def get_current_time_with_timezone(self):
        from datetime import datetime
        current_time = datetime.now().astimezone()
        return current_time.strftime("%Y-%m-%d %H:%M:%S %Z")

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
        cursor = None
        try:
            cursor = self.client.cursor()
            cursor.execute(insert_query, (uu, message))
            self.client.commit()
            cursor.close()
            logger.info(f"LLM result row inserted successfully for uu: {uu}")
        except Exception as e:
            logger.info(f"An error occurred while inserting the LLM result row: {e}")
            if cursor is not None:
                cursor.close()

    def db_update_llm_results(self, uu, message):
        """
        Update a row in the LLM_RESULTS table.

        Args:
            uu (str): The unique identifier for the result.
            message (str): The message to store.
        """
        update_query = f"""
            UPDATE {self.schema}.LLM_RESULTS
            SET message = %s
            WHERE uu = %s
        """
        cursor = None
        try:
            cursor = self.client.cursor()
            cursor.execute(update_query, (message, uu))
            self.client.commit()
            cursor.close()
        #     logger.info(f"LLM result row inserted successfully for uu: {uu}")
        except Exception as e:
            logger.info(f"An error occurred while inserting the LLM result row: {e}")
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
        cursor = None
        try:
            cursor = self.client.cursor()
            cursor.execute(select_query, (uu,))
            result = cursor.fetchone()
            cursor.close()
            if result is not None:
                return result[0]
            else:
                return ''
        except Exception as e:
            logger.info(f"An error occurred while retrieving the LLM result: {e}")
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
        cursor = None
        try:
            cursor = self.client.cursor()
            cursor.execute(delete_query)
            self.client.commit()
            cursor.close()
            logger.info(
                "LLM result rows older than 10 minutes have been successfully deleted."
            )
        except Exception as e:
            logger.info(f"An error occurred while deleting old LLM result rows: {e}")
            if cursor is not None:
                cursor.close()

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
        matching_connection=None,
    ):
        qualified_table_name = f'"{database_name}"."{schema_name}"."{table_name}"'
        if not memory_uuid:
            memory_uuid = str(uuid.uuid4())
        last_crawled_timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat(" ")
        if not ddl_hash:
            ddl_hash = self.sha256_hash_hex_string(ddl)

        # Use self.role if available, otherwise keep existing role_used_for_crawl
        if self.role is not None:
            role_used_for_crawl = self.role

        # if cortex mode, load embedding_native else load embedding column
        if os.environ.get("CORTEX_MODE", 'False') == 'True':
            embedding_target = 'embedding_native'
        else:
            embedding_target = 'embedding'

        # Convert embedding list to string format if not None
        embedding_str = (",".join(str(e) for e in embedding) if embedding is not None else None)

        query_params = {
            "source_name": matching_connection['connection_id'] if matching_connection is not None else self.source_name,
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
        if self.source_name == 'Snowflake':
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

            for param, value in query_params.items():
                # logger.info(f'{param}: {value}')
                if value is None:
                    # logger.info(f'{param} is null')
                    query_params[param] = "NULL"

            # Execute the MERGE statement with parameters
            try:
                # logger.info("merge sql: ",merge_sql)
                cursor = self.client.cursor()
                cursor.execute(merge_sql, query_params)
                self.client.commit()
            except Exception as e:
                logger.info(f"An error occurred while executing the MERGE statement: {e}")
            finally:
                if cursor is not None:
                    cursor.close()
        else:
            # Check if row exists
            check_query = f"""
                SELECT COUNT(*)
                FROM {self.metadata_table_name}
                WHERE source_name = :source_name
                AND qualified_table_name = :qualified_table_name
            """
            cursor = None
            try:
                cursor = self.client.cursor()
                cursor.execute(check_query, query_params)
                count = cursor.fetchone()[0]

                if count > 0:
                    # Update existing row
                    update_sql = f"""
                        UPDATE {self.metadata_table_name}
                        SET complete_description = :complete_description,
                            ddl = :ddl,
                            ddl_short = :ddl_short,
                            ddl_hash = :ddl_hash,
                            summary = :summary,
                            sample_data_text = :sample_data_text,
                            last_crawled_timestamp = :last_crawled_timestamp,
                            crawl_status = :crawl_status,
                            role_used_for_crawl = :role_used_for_crawl,
                            {embedding_target} = :embedding
                        WHERE source_name = :source_name
                        AND qualified_table_name = :qualified_table_name
                    """
                    cursor.execute(update_sql, query_params)
                else:
                    # Insert new row
                    insert_sql = f"""
                        INSERT INTO {self.metadata_table_name}  (
                            source_name, qualified_table_name, memory_uuid, database_name,
                            schema_name, table_name, complete_description, ddl, ddl_short,
                            ddl_hash, summary, sample_data_text, last_crawled_timestamp,
                            crawl_status, role_used_for_crawl, {embedding_target}
                        ) VALUES (
                            :source_name, :qualified_table_name, :memory_uuid, :database_name,
                            :schema_name, :table_name, :complete_description, :ddl, :ddl_short,
                            :ddl_hash, :summary, :sample_data_text, :last_crawled_timestamp,
                            :crawl_status, :role_used_for_crawl, :embedding)

                    """
                    cursor.execute(insert_sql, query_params)

                self.client.commit()
            except Exception as e:
                logger.info(f"An error occurred while executing the update/insert: {e}")
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
        if self.source_name != 'Snowflake':
            return False
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
            if os.environ.get('SPCS_MODE', '').upper() == 'TRUE':
                logger.info(f"Error checking cached metadata: {e}")
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

            logger.info(
                f"Retrieved cached rows from {metadata_table_id} for {database_name}.{schema_name}.{table_name}"
            )
        except Exception as e:
            logger.info(
                f"Cached rows from APP_SHARE.HARVEST_RESULTS NOT retrieved from {metadata_table_id} for {database_name}.{schema_name}.{table_name} due to erorr {e}"
            )

    # snowed

    # snowed
    def refresh_connection(self):
        if self.token_connection:
            self.connection = self._create_connection()

    # def connection(self) -> snowflake.connector.SnowflakeConnection:

    #     if os.path.isfile("/snowflake/session/token"):
    #         creds = {
    #             "host": os.getenv("SNOWFLAKE_HOST"),
    #             "port": os.getenv("SNOWFLAKE_PORT"),
    #             "protocol": "https",
    #             "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    #             "authenticator": "oauth",
    #             "token": open("/snowflake/session/token", "r").read(),
    #             "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    #             "database": os.getenv("SNOWFLAKE_DATABASE"),
    #             "schema": os.getenv("SNOWFLAKE_SCHEMA"),
    #             "client_session_keep_alive": True,
    #         }
    #     else:
    #         creds = {
    #             "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    #             "user": os.getenv("SNOWFLAKE_USER"),
    #             "password": os.getenv("SNOWFLAKE_PASSWORD"),
    #             "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    #             "database": os.getenv("SNOWFLAKE_DATABASE"),
    #             "schema": os.getenv("SNOWFLAKE_SCHEMA"),
    #             "client_session_keep_alive": True,
    #         }

    #     connection = snowflake.connector.connect(**creds)
    #     return connection

    def _create_connection(self):
        # Snowflake token testing

        if os.getenv("SNOWFLAKE_METADATA", "False").upper() == "FALSE":
            return self.client

        self.token_connection = False
        #  logger.warn('Creating connection..')
        SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT", self.account)
        SNOWFLAKE_HOST = os.getenv("SNOWFLAKE_HOST", None)
        logger.info("Checking possible SPCS ENV vars -- Account, Host: {}, {}".format(SNOWFLAKE_ACCOUNT, SNOWFLAKE_HOST))

        #     logger.info("SNOWFLAKE_HOST: %s", os.getenv("SNOWFLAKE_HOST"))
        #     logger.info("SNOWFLAKE_ACCOUNT: %s", os.getenv("SNOWFLAKE_ACCOUNT"))
        #     logger.info("SNOWFLAKE_PORT: %s", os.getenv("SNOWFLAKE_PORT"))
        #  logger.warn('SNOWFLAKE_WAREHOUSE: %s', os.getenv('SNOWFLAKE_WAREHOUSE'))
        #     logger.info("SNOWFLAKE_DATABASE: %s", os.getenv("SNOWFLAKE_DATABASE"))
        #     logger.info("SNOWFLAKE_SCHEMA: %s", os.getenv("SNOWFLAKE_SCHEMA"))

        if (SNOWFLAKE_ACCOUNT and SNOWFLAKE_HOST and os.getenv("SNOWFLAKE_PASSWORD_OVERRIDE", None) == None):
            # token based connection from SPCS
            with open("/snowflake/session/token", "r") as f:
                snowflake_token = f.read()
            logger.info(f"Natapp Connection: SPCS Snowflake token found, length: {len(snowflake_token)}")
            self.token_connection = True
            #   logger.warn('Snowflake token mode (SPCS)...')
            if os.getenv("SNOWFLAKE_SECURE", "TRUE").upper() == "FALSE":
                #        logger.info('insecure mode')
                return connect(
                    host=os.getenv("SNOWFLAKE_HOST"),
                    #        port = os.getenv('SNOWFLAKE_PORT'),
                    protocol="https",
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
                    database=os.getenv("SNOWFLAKE_DATABASE"),
                    schema=os.getenv("SNOWFLAKE_SCHEMA"),
                    account=os.getenv("SNOWFLAKE_ACCOUNT"),
                    token=snowflake_token,
                    authenticator="oauth",
                    client_session_keep_alive=True,
                )

        logger.info("Creating Snowflake regular connection...")
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
            rows = cursor.fetchall()
            for row in rows:
                if self.source_name == 'SQLite':
                    schema_col = 0
                else:
                    schema_col = 1
                schemas.append(row[schema_col])  # Assuming schema name is in second column
            cursor.close()
        except Exception as e:
            # logger.info(f"error getting schemas for {database}: {e}")
            return schemas
        return schemas

    def get_tables(self, database, schema, thread_id=None):
        tables = []
        try:
            query = f'SHOW TABLES IN "{database}"."{schema}"'
            cursor = self.connection.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            for row in rows:
                if self.source_name == 'SQLite':
                    table_col = 0
                else:
                    table_col = 1
                tables.append(
                    {"table_name": row[table_col], "object_type": "TABLE"}
                )  # Assuming the table name is in the second column and DDL in the third
            cursor.close()
            query = f'SHOW VIEWS IN "{database}"."{schema}"'
            cursor = self.connection.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            for row in rows:
                if self.source_name == 'SQLite':
                    table_col = 0
                else:
                    table_col = 1
                tables.append(
                    {"table_name": row[table_col], "object_type": "VIEW"}
                )  # Assuming the table name is in the second column and DDL in the third
            cursor.close()
        except Exception as e:
            # logger.info(f"error getting tables for {database}.{schema}: {e}")
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
        # logger.info(table_name)
        describe_query = f"DESCRIBE TABLE {table_name};"
        try:
            describe_result = self.run_query(query=describe_query, max_rows=1000, max_rows_override=True)
        except:
            return None

        ddl_statement = "CREATE TABLE " + table_name + " (\n"
        for column in describe_result:
            column_name = column['NAME']
            column_type = column['TYPE']
            nullable = " NOT NULL" if not column['NULL?'] else ""
            default = f" DEFAULT {column['DEFAULT']}" if column['DEFAULT'] is not None else ""
            comment = f" COMMENT '{column['COMMENT']}'" if 'COMMENT' in column and column['COMMENT'] is not None else ""
            key = ""
            if column.get('PRIMARY_KEY', False):
                key = " PRIMARY KEY"
            elif column.get('UNIQUE_KEY', False):
                key = " UNIQUE"
            ddl_statement += f"    {column_name} {column_type}{nullable}{default}{key}{comment},\n"
        ddl_statement = ddl_statement.rstrip(',\n') + "\n);"
        # logger.info(ddl_statement)
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
            logger.info(f"Workspace schema {workspace_schema_name} verified or created")
            query = f"CREATE STAGE IF NOT EXISTS {workspace_schema_name}.MY_STAGE"
            cursor = self.client.cursor()
            cursor.execute(query)
            self.client.commit()
            logger.info(f"Workspace stage {workspace_schema_name}.MY_STAGE verified or created")
        except Exception as e:
            logger.error(f"Failed to create bot workspace {workspace_schema_name}: {e}")

    def grant_all_bot_workspace(self, workspace_schema_name):
        try:
            if os.getenv("SPCS_MODE", "False").lower() == "false":
                grant_fragment = "ROLE PUBLIC"
            else:
                grant_fragment = "APPLICATION ROLE APP_PUBLIC"

            query = f"GRANT USAGE ON SCHEMA {workspace_schema_name} TO {grant_fragment}; "
            cursor = self.client.cursor()
            cursor.execute(query)
            self.client.commit()

            query = f"GRANT SELECT ON ALL TABLES IN SCHEMA {workspace_schema_name} TO {grant_fragment}; "
            cursor = self.client.cursor()
            cursor.execute(query)
            self.client.commit()

            query = f"GRANT SELECT ON ALL VIEWS IN SCHEMA {workspace_schema_name} TO {grant_fragment}; "
            cursor = self.client.cursor()
            cursor.execute(query)
            self.client.commit()

            query = f"GRANT USAGE ON ALL STAGES IN SCHEMA {workspace_schema_name} TO {grant_fragment}; "
            cursor = self.client.cursor()
            cursor.execute(query)
            self.client.commit()

            query = f"GRANT USAGE ON ALL FUNCTIONS IN SCHEMA {workspace_schema_name} TO {grant_fragment}; "
            cursor = self.client.cursor()
            cursor.execute(query)
            self.client.commit()

            query = f"GRANT USAGE ON ALL PROCEDURES IN SCHEMA {workspace_schema_name} TO {grant_fragment}; "
            cursor = self.client.cursor()
            cursor.execute(query)
            self.client.commit()

            logger.info(
                f"Workspace {workspace_schema_name} objects granted to {grant_fragment}"
            )
        except Exception as e:
            logger.warning(f"Failed to grant workspace {workspace_schema_name} objects to {grant_fragment}: {e}")

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

    def cortex_search(self,
        query: str,
        service_name: str='service',
        top_n: int=1,
        thread_id=None):
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

        return

    # handle the job_config stuff ...
    def run_query(
        self,
        query=None,
        max_rows=-1,
        max_rows_override=False,
        job_config=None,
        bot_id=None,
        connection=None,
        thread_id=None,
        note_id = None,
        note_name = None,
        note_type = None,
        max_field_size = 5000,
        export_to_google_sheet = False,
        export_title=None,
        keep_db_schema = False
    ):
        """
        Executes a SQL query on Snowflake, with support for parameterized queries.

        :param query: The SQL query string to be executed.
        :param max_rows: The maximum number of rows to return. Defaults to 100 for non-user queries (special queries that starst with 'USERQUERY::' have a different default).
        :param max_rows_override: If set to True, allows returning more than the default maximum rows.
        :param job_config: Deprecated. Do not use.
        :param bot_id: Identifier for the bot executing the query.
        :param connection: The database connection object to use for executing the query.
        :param thread_id: Identifier for the current thread.
        :param note_id: Identifier for the note from which to retrieve the query.
        :param note_name: Name of the note from which to retrieve the query.
        :param note_type: The type of note, expected to be 'sql' for executing SQL queries.
        :raises ValueError: If the note type is not 'sql'.
        :return: A dictionary.
            In case of error the result will have the following fields
                'Success' (bool)
                'Error' (str, if exception occured)
                    "Query you sent" (str, on certain errors)
                    "Action needed" (str, on certain errors)
                    "Suggestion" (str, on certain errors)
            In case of success, the result will be a list of dictionaries representing the resultset
        """
        from ...core import global_flags
        from .stage_utils import (
            read_file_from_stage
        )

        userquery = False
        fieldTrunced = False

        if (query is None and note_id is None and note_name is None) or (query is not None and (note_id is not None or note_name is not None)):
            return {
                "success": False,
                "error": "Either a query or a (note_id or note_name) must be provided, but not both, and not neither.",
            }

        try:
            if note_id is not None or note_name is not None:
                note_id = '' if note_id is None else note_id
                if note_id == '':
                    note_id = note_name
                note_name = '' if note_name is None else note_name
                if note_name == '':
                    note_name = note_id
                get_note_query = f"SELECT note_content, note_params, note_type FROM {self.schema}.NOTEBOOK WHERE (NOTE_ID = '{note_id}') or (NOTE_NAME = '{note_name}') and BOT_ID='{bot_id}'"
                cursor = self.connection.cursor()
                cursor.execute(get_note_query)
                query_cursor = cursor.fetchone()

                if query_cursor is None:
                    return {
                        "success": False,
                        "error": "Note not found.",
                        }

                query = query_cursor[0]
                note_type = query_cursor[2]

                if note_type != 'sql':
                    raise ValueError(f"Note type must be 'sql' to run sql with the query_database tool.  This note is type: {note_type}")
        except ValueError as e:
            return {
                "success": False,
                "error": str(e),
            }

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

        if bot_id is not None:
            bot_llm = os.getenv("BOT_LLM_" + bot_id, "unknown")
            workspace_schema_name = f"{bot_id.replace(r'[^a-zA-Z0-9]', '_').replace('-', '_').replace('.', '_')}_WORKSPACE".upper()
            workspace_full_schema_name = f"{global_flags.project_id}.{workspace_schema_name}"
        else:
            bot_llm = 'unknown'
            workspace_full_schema_name = None
            workspace_schema_name = None

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

        if export_to_google_sheet:
            max_rows = 500

        #   logger.info('running query ... ', query)
        cursor = self.connection.cursor()

        if userquery and bot_llm == 'cortex' and "\\'" in query:
            query = query.replace("\\'","'")

        if userquery and bot_llm == 'cortex' and not query.endswith(';'):
            return { "Success": False,
                     "Error": "Your query is missing a ; semicolon on the end, or was cut off in your tool call",
                     "Query you sent": query,
                     "Action needed": "Resubmit your complete query, including a semicolon at the end;"
}

        try:
            if keep_db_schema and self.source_name == 'SQLite':
                cursor.execute(f"KEEPSCHEMA::{query}")
            else:
                cursor.execute(query)

            if bot_id is not None and ("CREATE" in query.upper() and workspace_schema_name.upper() in query.upper()):
                self.grant_all_bot_workspace(workspace_full_schema_name)

        except Exception as e:

            if e.errno == 390114 or 'Authentication token has expired' in e.msg:
                logger.info('Snowflake token expired, re-authenticating...')
                self.connection: SnowflakeConnection = self._create_connection()
                self.client = self.connection
                cursor = self.connection.cursor()
                try:
                    cursor.execute(query)
                    if bot_id is not None and ("CREATE" in query.upper() and workspace_schema_name.upper() in query.upper()):
                        self.grant_all_bot_workspace(workspace_full_schema_name)
                except Exception as e:
                    pass

            if "does not exist or not authorized" in str(e):
                logger.info(
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
                    "Suggestion": dedent("""
                            You have tried to query an object with an incorrect name of one that is not granted to APPLICATION GENESIS_BOTS.
                            To fix this:
                            1. Make sure you are referencing correct objects that you learned about via search_metadata, or otherwise are sure actually exists
                            2. Explain the error and show the SQL you tried to run to the user, they may be able to help
                            3. Tell the user that IF they know for sure that this is a valid object, that they may need to run this in a Snowflake worksheet:
                                "CALL GENESIS_LOCAL_DB.SETTINGS.grant_schema_usage_and_select_to_app('<insert database name here>','GENESIS_BOTS');"
                                This will grant the you access to the data in the database.
                            4. Suggest to the user that the table may have been recreated since it was originally granted, or may be recreated each day as part of an ETL job.  In that case it must be re-granted after each recreation.
                            5. NOTE: You do not have the PUBLIC role or any other role, all object you are granted must be granted TO APPLICATION GENESIS_BOTS, or be granted by grant_schema_usage_and_select_to_app as shown above.
                            """),
                }

            logger.info("run query: len=", len(query), "\ncaused error: ", e)
            cursor.close()
            return {"Success": False, "Error": str(e)}

        #    logger.info('getting results:')
        try:

            results = cursor.fetchmany(max(1,max_rows))
            columns = [col[0].upper() for col in cursor.description]

            fieldTrunced = False
            if userquery and max_field_size > 0:
                updated_results = []
                for row in results:
                    updated_row = list(row)
                    for i, value in enumerate(row):
                        if isinstance(value, str) and len(value) > max_field_size:
                            updated_row[i] = value[:max_field_size] + f"[!!FIELD OVER {max_field_size} (max_field_size) bytes--TRUNCATED!!]"
                            fieldTrunced = True
                    updated_results.append(tuple(updated_row))
                results = updated_results

            sample_data = [dict(zip(columns, row)) for row in results]
            #   logger.info('query results: ',sample_data)

            # Replace occurrences of triple backticks with triple single quotes in sample data
            sample_data = [
                {
                    key: (
                        value.replace("```", "\\`\\`\\`")
                        if isinstance(value, str)
                        else value
                    )
                    for key, value in row.items()
                }
                for row in sample_data
            ]
        except Exception as e:
            logger.info("run query: ", query, "\ncaused error: ", e)
            cursor.close()
            raise e

        cursor.close()

        def get_root_folder_id():
            cursor = self.connection.cursor()
            # cursor.execute(
            #     f"call core.run_arbitrary($$ grant read,write on stage app1.bot_git to application role app_public $$);"
            # )

            query = f"SELECT value from {self.schema}.EXT_SERVICE_CONFIG WHERE ext_service_name = 'g-sheets' AND parameter = 'shared_folder_id'"
            cursor.execute(query)
            row = cursor.fetchone()
            cursor.close()
            if row is not None:
                return {"Success": True, "result": row[0]}
            else:
                raise Exception("Missing shared folder ID")

        if export_to_google_sheet:
            from datetime import datetime

            shared_folder_id = get_root_folder_id()
            timestamp = datetime.now().strftime("%m%d%Y_%H:%M:%S")

            if export_title is None:
                export_title = 'Genesis Export'
            result = create_google_sheet_from_export(self, shared_folder_id['result'], title=f"{export_title}", data=sample_data )

            return {
                "Success": True,
                "result": "Data successfully sent to Google Sheets",
                "folder_url": result["folder_url"],
                "file_url": result["file_url"],
                "file_id": result["file_id"],
            }

        return sample_data
    def db_list_all_bots(
        self,
        project_id,
        dataset_name,
        bot_servicing_table,
        runner_id=None,
        full=False,
        slack_details=False,
        with_instructions=False,
    ):
        """
        Returns a list of all the bots being served by the system, including their runner IDs, names, instructions, tools, etc.

        Returns:
            list: A list of dictionaries, each containing details of a bot.
        """
        # Get the database schema from environment variables

        # Convert with_instructions to boolean if it's a string
        if isinstance(with_instructions, str):
            with_instructions = with_instructions.lower() == 'true'

        if full:
            select_str = "api_app_id, bot_slack_user_id, bot_id, bot_name, bot_instructions, runner_id, slack_app_token, slack_app_level_key, slack_signing_secret, slack_channel_id, available_tools, udf_active, slack_active, \
                files, bot_implementation, bot_intro_prompt, bot_avatar_image, slack_user_allow, teams_active, teams_app_id, teams_app_password, teams_app_type, teams_app_tenant_id"
        else:
            if slack_details:
                select_str = "runner_id, bot_id, bot_name, bot_instructions, available_tools, bot_slack_user_id, api_app_id, auth_url, udf_active, slack_active, files, bot_implementation, bot_intro_prompt, slack_user_allow"
            else:
                select_str = "runner_id, bot_id, bot_name, bot_instructions, available_tools, bot_slack_user_id, api_app_id, auth_url, udf_active, slack_active, files, bot_implementation, bot_intro_prompt"
        if not with_instructions and not full:
            select_str = select_str.replace("bot_instructions, ", "")
        # Remove bot_instructions if not requested
        if not with_instructions and not full:
            select_str = select_str.replace("bot_instructions, ", "")
            select_str = select_str.replace(", bot_intro_prompt", "")

        # Use the bot_servicing_table name for bot_table
        bot_table = bot_servicing_table
        # Extract table name after last dot if dots are present
        if '.' in bot_table:
            bot_table = bot_table.split('.')[-1]

        # Query to select all bots from the BOT_SERVICING table
        if runner_id is None:
            select_query = f"""
            SELECT {select_str}
            FROM {project_id}.{dataset_name}.{bot_table}
            """
        else:
            select_query = f"""
            SELECT {select_str}
            FROM {project_id}.{dataset_name}.{bot_table}
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
           #     logger.info(f"No Slack config tokens found for runner_id: {runner_id}")
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

        # First check if row exists
        check_query = f"""
            SELECT COUNT(*)
            FROM {project_id}.{dataset_name}.ngrok_tokens
            WHERE runner_id = %s
        """

        try:
            cursor = self.connection.cursor()
            cursor.execute(check_query, (runner_id,))
            exists = cursor.fetchone()[0] > 0

            if exists:
                # Update existing row
                update_query = f"""
                    UPDATE {project_id}.{dataset_name}.ngrok_tokens
                    SET ngrok_auth_token = %s,
                        ngrok_use_domain = %s,
                        ngrok_domain = %s
                    WHERE runner_id = %s
                """
                cursor.execute(
                    update_query,
                    (ngrok_auth_token, ngrok_use_domain, ngrok_domain, runner_id)
                )
            else:
                # Insert new row
                insert_query = f"""
                    INSERT INTO {project_id}.{dataset_name}.ngrok_tokens
                    (runner_id, ngrok_auth_token, ngrok_use_domain, ngrok_domain)
                    VALUES (%s, %s, %s, %s)
                """
                cursor.execute(
                    insert_query,
                    (runner_id, ngrok_auth_token, ngrok_use_domain, ngrok_domain)
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
        Retrieves all LLM keys, types, and endpoints for the current runner.

        Args:
            project_id: Unused, kept for interface compatibility.
            dataset_name: Unused, kept for interface compatibility.

        Returns:
            list: A list of structs containing LLM key, type, and endpoint.
        """
        runner_id = os.getenv("RUNNER_ID", "jl-local-runner")
        query = f"""
            SELECT llm_key, llm_type, llm_endpoint
            FROM {self.genbot_internal_project_and_schema}.llm_tokens
            WHERE runner_id = %s
        """
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, (runner_id,))
                results = cursor.fetchall()

            if results:
                return [llm_keys_and_types_struct(llm_type=result[1], llm_key=result[0], llm_endpoint=result[2]) for result in results]
            else:
                logger.info("No LLM tokens found for runner_id: %s", runner_id)
                return []
        except Exception as e:
            logger.error("Error retrieving LLM tokens: %s", str(e))
            return []

    def db_get_active_llm_key(self, i = -1):
        """
        Retrieves the active LLM key and type for the given runner_id.

        Returns:
            list: A list of tuples, each containing an LLM key and LLM type.
        """
        runner_id = os.getenv("RUNNER_ID", "jl-local-runner")
        # logger.info("in getllmkey")
        # Query to select the LLM key and type from the llm_tokens table
        query = f"""
            SELECT llm_key, llm_type, llm_endpoint, model_name, embedding_model_name
            FROM {self.genbot_internal_project_and_schema}.llm_tokens
            WHERE runner_id = %s and active = True
        """
        # logger.info(f"query: {query}")
        try:
            cursor = self.connection.cursor()
            cursor.execute(query, (runner_id,))
            result = cursor.fetchone()  # Fetch a single result
            cursor.close()

            if result:
                return llm_keys_and_types_struct(llm_type=result[1], llm_key=result[0], llm_endpoint=result[2], model_name=result[3], embedding_model_name=result[4])
            else:
                return llm_keys_and_types_struct()  # Return None if no result found
        except Exception as e:
            if "identifier 'ACTIVE" in e.msg:
                if i == 0:
                    logger.info('Waiting on upgrade of LLM_TOKENS table with ACTIVE column in primary service...')
            else:
                logger.info(
                    "Error getting data from LLM_TOKENS table: ", e
                )
            return None, None

    def db_set_llm_key(self, llm_key, llm_type, llm_endpoint):
        """
        Updates the llm_tokens table with the provided LLM key and type.

        Args:
            llm_key (str): The LLM key.
            llm_type (str|BotLlmEngineEnum): The type of LLM (e.g., 'openai', 'reka').
            llm_endpoint (str): endpoint for LLM like azure openai
        """
        runner_id = os.getenv("RUNNER_ID", "jl-local-runner")

        # validate llm_type; use the str value for the rest of this method
        llm_type = BotLlmEngineEnum(llm_type).value # use the str value (e.g. 'openai)

        if self.source_name.lower() == "snowflake":

            # deactivate the current active LLM key
            try:
                update_query = f"""
        UPDATE  {self.genbot_internal_project_and_schema}.llm_tokens
        SET ACTIVE = FALSE
        WHERE RUNNER_ID = '{runner_id}'
        """
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
                    UPDATE SET llm_key = %s, llm_type = %s, active = TRUE, llm_endpoint = %s
                WHEN NOT MATCHED THEN
                    INSERT (runner_id, llm_key, llm_type, active, llm_endpoint)
                    VALUES (%s, %s, %s, TRUE, %s)
            """

            try:
                if llm_key:
                    cursor = self.connection.cursor()
                    cursor.execute(
                        query, (runner_id, llm_key, llm_type, llm_endpoint, runner_id, llm_key, llm_type, llm_endpoint)
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
                    logger.info("key variable is empty and was not stored in the database")
            except Exception as e:
                logger.error(
                    f"Failed to update LLM key for runner_id: {runner_id} with error: {e}"
                )
                return False
        else:  # sqlite

            # deactivate the current active LLM key
            try:
                update_query = f"""
        UPDATE  {self.genbot_internal_project_and_schema}.llm_tokens
        SET ACTIVE = FALSE
        WHERE RUNNER_ID = '{runner_id}'
        """
                cursor = self.connection.cursor()
                cursor.execute(update_query)
                self.connection.commit()
            except Exception as e:
                logger.error(
                    f"Failed to deactivate current active LLM with error: {e}"
                )

            # Check if record exists
            select_query = f"""
                SELECT 1
                FROM {self.genbot_internal_project_and_schema}.llm_tokens
                WHERE runner_id = %s AND llm_type = %s
            """

            try:
                if llm_key:
                    cursor = self.connection.cursor()
                    cursor.execute(select_query, (runner_id, llm_type))
                    exists = cursor.fetchone() is not None

                    if exists:
                        # Update existing record
                        update_query = f"""
                            UPDATE {self.genbot_internal_project_and_schema}.llm_tokens
                            SET llm_key = %s,
                                llm_type = %s,
                                active = TRUE,
                                llm_endpoint = %s
                            WHERE runner_id = %s AND llm_type = %s
                        """
                        cursor.execute(update_query, (llm_key, llm_type, llm_endpoint, runner_id, llm_type))
                    else:
                        # Insert new record
                        insert_query = f"""
                            INSERT INTO {self.genbot_internal_project_and_schema}.llm_tokens
                            (runner_id, llm_key, llm_type, active, llm_endpoint)
                            VALUES (%s, %s, %s, TRUE, %s)
                        """
                        cursor.execute(insert_query, (runner_id, llm_key, llm_type, llm_endpoint))

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
                    logger.info("key variable is empty and was not stored in the database")
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
            INSERT INTO {bot_servicing_table} (
                api_app_id, bot_slack_user_id, bot_id, bot_name, bot_instructions, runner_id,
                slack_signing_secret, slack_channel_id, available_tools, auth_url, auth_state, client_id, client_secret, udf_active, slack_active,
                files, bot_implementation, bot_intro_prompt, bot_avatar_image
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
        """

        available_tools_string = json.dumps(available_tools)
        files_string = json.dumps(files) if files else ''

        # validate certain params
        bot_implementation = BotLlmEngineEnum(bot_implementation).value if bot_implementation else None

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
            logger.info(f"Successfully inserted new bot configuration for bot_id: {bot_id}")

            if not slack_user_allow:
                if self.source_name.lower() == "snowflake":
                    slack_user_allow_update_query = f"""
                        UPDATE {bot_servicing_table}
                        SET slack_user_allow = parse_json(%s)
                        WHERE upper(bot_id) = upper(%s)
                        """
                else:
                    slack_user_allow_update_query = f"""
                        UPDATE {bot_servicing_table}
                        SET slack_user_allow = %s
                        WHERE upper(bot_id) = upper(%s)
                        """
                slack_user_allow_value = '["!BLOCK_ALL"]'
                try:
                    cursor.execute(
                        slack_user_allow_update_query, (slack_user_allow_value, bot_id)
                    )
                    self.connection.commit()
                    logger.info(
                        f"Updated slack_user_allow for bot_id: {bot_id} to block all users."
                    )
                except Exception as e:
                    logger.info(
                        f"Failed to update slack_user_allow for bot_id: {bot_id} with error: {e}"
                    )
                    raise e

        except Exception as e:
            logger.info(
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
        from ...core import global_flags
        # Query to update the available_tools in the database
        update_query = f"""
            UPDATE {bot_servicing_table}
            SET available_tools = %s
            WHERE upper(bot_id) = upper(%s)
        """

        # Execute the update query
        try:
            cursor = self.connection.cursor()
            cursor.execute(update_query, (updated_tools_str, bot_id))
            self.connection.commit()
            logger.info(f"Successfully updated available_tools for bot_id: {bot_id}")

            if "SNOWFLAKE_TOOLS" in updated_tools_str.upper():
                # TODO JD - Verify this change ^^
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
            UPDATE {bot_servicing_table}
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
            UPDATE {bot_servicing_table}
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
            UPDATE {bot_servicing_table}
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

        # validate inputs
        bot_implementation = BotLlmEngineEnum(bot_implementation).value if bot_implementation else None

        # Query to update the bot implementation in the database
        update_query = f"""
            UPDATE {bot_servicing_table}
            SET bot_implementation = %s
            WHERE upper(bot_id) = upper(%s) AND runner_id = %s
        """

        # Check if bot_id is valid
        valid_bot_query = f"""
            SELECT COUNT(*)
            FROM {bot_servicing_table}
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
            logger.info(f"Successfully updated bot_implementation for bot_id: {bot_id} to {bot_implementation}")

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
        if self.source_name.lower() == "snowflake":
            update_query = f"""
                UPDATE {bot_servicing_table}
                SET slack_user_allow = parse_json(%s)
                WHERE upper(bot_id) = upper(%s)
                """
        else:
            update_query = f"""
                UPDATE {bot_servicing_table}
                SET slack_user_allow = %s
                WHERE upper(bot_id) = upper(%s)
                """

        # Convert the list to a format suitable for database storage (e.g., JSON string)
        slack_user_allow_list_str = json.dumps(slack_user_allow_list)
        if slack_user_allow_list == []:
            update_query = f"""
            UPDATE {bot_servicing_table}
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
            FROM {bot_servicing_table}
            WHERE upper(bot_id) = upper(%s)
        """

        try:
            cursor = self.connection.cursor()
            # logger.info(select_query, bot_id)

            cursor.execute(select_query, (bot_id,))
            result = cursor.fetchone()
            cursor.close()
            if result:
                # Assuming the result is a tuple, we convert it to a dictionary using the column names
                columns = [desc[0].lower() for desc in cursor.description]
                bot_details = dict(zip(columns, result))
                return bot_details
            else:
                logger.info(f"No details found for bot_id: {bot_id} in {bot_servicing_table}")
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

                        FROM {bot_servicing_table}
            WHERE upper(bot_id) = upper(%s)
        """

        try:
            cursor = self.connection.cursor()
            # logger.info(select_query, bot_id)

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
        # validate inputs
        bot_implementation = BotLlmEngineEnum(bot_implementation).value if bot_implementation else None

        update_query = f"""
            UPDATE {bot_servicing_table}
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
            logger.info(
                f"Successfully updated existing bot configuration for bot_id: {bot_id}"
            )
        except Exception as e:
            logger.info(
                f"Failed to update existing bot configuration for bot_id: {bot_id} with error: {e}"
            )
            raise e

    def db_update_existing_bot_basics(
        self,
        bot_id,
        bot_name,
        bot_implementation,
        files,
        available_tools,
        bot_instructions,
        project_id,
        dataset_name,
        bot_servicing_table,
    ):
        """
        Updates basic bot configuration fields in the BOT_SERVICING table.

        Args:
            bot_id (str): The unique identifier for the bot.
            bot_name (str): The name of the bot.
            bot_implementation (str): openai or cortex or ...
            files (json-embedded list): A list of files to include with the bot.
            available_tools (list): List of tools available to the bot.
            bot_instructions (str): Instructions for the bot.
            project_id (str): The Snowflake project ID.
            dataset_name (str): The Snowflake dataset name.
            bot_servicing_table (str): The name of the bot servicing table.
        """
        # validate inputs
        bot_implementation = BotLlmEngineEnum(bot_implementation).value if bot_implementation else None

        available_tools_string = json.dumps(available_tools)
        files_string = json.dumps(files) if not isinstance(files, str) else files

        update_query = f"""
            UPDATE {bot_servicing_table}
            SET BOT_NAME = %s,
                BOT_IMPLEMENTATION = %s,
                FILES = %s,
                AVAILABLE_TOOLS = %s,
                BOT_INSTRUCTIONS = %s
            WHERE upper(BOT_ID) = upper(%s)
        """

        try:
            self.client.cursor().execute(
                update_query,
                (
                    bot_name,
                    bot_implementation,
                    files_string,
                    available_tools_string,
                    bot_instructions,
                    bot_id,
                ),
            )
            self.client.commit()
            logger.info(
                f"Successfully updated basic bot configuration for bot_id: {bot_id}"
            )
        except Exception as e:
            logger.info(
                f"Failed to update basic bot configuration for bot_id: {bot_id} with error: {e}"
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
            UPDATE {bot_servicing_table}
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
            DELETE FROM {bot_servicing_table}
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
            FROM {bot_servicing_table}
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
            logger.info(
                f"Default image data from share not available (expected in non-Snowflake modes): {e}"
            )

    def db_get_endpoint_ingress_url(self, endpoint_name: str) -> str:
        """
        Retrieves the ingress URL for a specified endpoint registered within the Genesis (native) App service.
        Call this method only when running in Native app mode.

        Args:
            endpoint_name (str, optional): The name of the endpoint to retrieve the ingress URL for. Defaults to None.

        Returns:
            str or None: The ingress URL of the specified endpoint if found, otherwise None.
        """
        alt_service_name = os.getenv("ALT_SERVICE_NAME", None)
        if alt_service_name:
            query1 = f"SHOW ENDPOINTS IN SERVICE {alt_service_name};"
        else:
            query1 = f"SHOW ENDPOINTS IN SERVICE {self.genbot_internal_project_and_schema}.GENESISAPP_SERVICE_SERVICE;"
        try:
            results = self.run_query(query1)
            udf_endpoint_url = None
            for endpoint in results:
                if endpoint["NAME"] == endpoint_name:
                    udf_endpoint_url = endpoint["INGRESS_URL"]
                    break
            return udf_endpoint_url
        except Exception as e:
            logger.warning(f"Failed to get {endpoint_name} endpoint URL with error: {e}")
            return None

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
            logger.info("imagegen OpenAI API key is not set in the environment variables.")
            return {
                "success": False,
                "error": "OpenAI key is required to generate images, but one was not found to be available."
            }

        client = get_openai_client()

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
                logger.info("imagegen Failed to generate image with DALL-E 3.")
                return None

            try:
                # Download the image from the URL
                image_response = requests.get(image_url)
                logger.info("imagegen getting image from ", image_url)
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
            file_path = f"./runtime/downloaded_files/{thread_id}/{sanitized_prompt}.png"
            # Save the image to the local downloaded_files folder
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with open(file_path, "wb") as image_file:
                image_file.write(image_bytes)

            logger.info(f"imagegen Image generated and saved to {file_path}")

            result = {
                "success": True,
                "result": f'Image generated and saved to server. Output a link like this so the user can see it [description of image](sandbox:/mnt/data/{sanitized_prompt}.png)',
                "prompt": prompt,
            }

            return result
        except Exception as e:
            logger.info(f"imagegen Error generating image with DALL-E 3: {e}")
            return None

    def _OLD_OLD_REMOVE_image_analysis(
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

        file_path = f"./runtime/downloaded_files/{thread_id}/" + file_name
        existing_location = f"./runtime/downloaded_files/{thread_id}/{openai_file_id}"

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
        prompt = query if query else "What's in this image?"

        openai_model_name = os.getenv("OPENAI_MODEL_NAME", "gpt-4o-2024-11-20")

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

    # Assuming self.connection is an instance of SnowflakeConnector
    # with methods run_query() for executing queries and logger is a logging instance.
    # Test instance creation and calling list_stage method

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
                UPDATE {bot_servicing_table}
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

    def extract_knowledge(self, primary_user, bot_id, k = 1):

        query = f"""SELECT * FROM {self.user_bot_table_name}
                    WHERE primary_user = '{primary_user}' AND BOT_ID = '{bot_id}'
                    ORDER BY TIMESTAMP DESC
                    LIMIT 1;"""
        knowledge = self.run_query(query)
        if knowledge:
            knowledge = knowledge[0]
            knowledge['HISTORY'] = ''
            if k > 1:
                query = f"""SELECT * FROM {self.knowledge_table_name}
                        WHERE primary_user LIKE '%{primary_user}%' AND BOT_ID = '{bot_id}'
                        ORDER BY LAST_TIMESTAMP DESC
                        LIMIT {k};"""
                history = self.run_query(query)
                if history:
                    output = ['By the way the current system date and time is {} and below are the summary of last {} conversations:'.format(self.get_current_time_with_timezone(), len(history))]
                    for row in history:
                        if type(row['LAST_TIMESTAMP']) is not str:
                            row['LAST_TIMESTAMP'] = row['LAST_TIMESTAMP'].strftime('%Y-%m-%d %H:%M')
                        output.append('\n\n{}:\n{}'.format(row['LAST_TIMESTAMP'], row['THREAD_SUMMARY']))
                knowledge['HISTORY'] += ''.join(output)
            return knowledge
        return {}

    def query_threads_message_log(self, cutoff):
        query = f"""
                WITH K AS (SELECT thread_id, max(last_timestamp) as last_timestamp FROM {self.knowledge_table_name}
                    GROUP BY thread_id),
                M AS (SELECT thread_id, max(timestamp) as timestamp, COUNT(*) as c FROM {self.message_log_table_name}
                    WHERE PRIMARY_USER IS NOT NULL
                    GROUP BY thread_id
                    HAVING c > 3)
                SELECT M.thread_id, timestamp as timestamp, COALESCE(K.last_timestamp, DATE('2000-01-01')) as last_timestamp FROM M
                LEFT JOIN K on M.thread_id = K.thread_id
                WHERE timestamp > COALESCE(K.last_timestamp, DATE('2000-01-01')) AND timestamp < TO_TIMESTAMP('{cutoff}') order by timestamp;"""
        return self.run_query(query)

    def query_timestamp_message_log(self, thread_id, last_timestamp, max_rows=50):
        query = f"""SELECT * FROM {self.message_log_table_name}
                        WHERE timestamp > TO_TIMESTAMP('{last_timestamp}') AND
                        thread_id = '{thread_id}'
                        ORDER BY TIMESTAMP;"""
        msg_log = self.run_query(query, max_rows=max_rows)
        return msg_log

    def run_insert(self, table, **kwargs):
        keys = ', '.join(kwargs.keys())

        insert_query = f"""
            INSERT INTO {table} ({keys}) VALUES ({', '.join(['%s']*len(kwargs))});
            """
        cursor = self.client.cursor()
        cursor.execute(insert_query, tuple(kwargs.values()))
        # Get the results from the query
        results = cursor.fetchall()

        self.client.commit()
        cursor.close()

        # Check if there are any results
        if results:
            # Process the results if needed
            # For example, you might want to return them or do something with them
            return results
        else:
            # If no results, you might want to return None or an empty list
            return None

    def fetch_embeddings(self, table_id, bot_id="system"):
        # Initialize Snowflake connector

        # Initialize variables
        batch_size = 100
        offset = 0
        total_fetched = 0

        # Initialize lists to store results
        embeddings = []
        table_names = []
        # update to use embedding_native column if cortex mode

        # Get array of allowed bots
        allowed_connections_query = f"""
        select connection_id from {self.cust_db_connections_table_name}
        where owner_bot_id = '{bot_id}'
        OR allowed_bot_ids = '*'
        OR allowed_bot_ids = '{bot_id}'
        OR allowed_bot_ids like '%,{bot_id}'
        OR allowed_bot_ids like '{bot_id},%'
        OR allowed_bot_ids like '%,{bot_id},%'
        """
        cursor = self.connection.cursor()
        cursor.execute(allowed_connections_query)
        allowed_connections = [row[0] for row in cursor.fetchall()]

        # Format list of connections with proper quoting
        connection_list = ','.join([f"'{x}'" for x in allowed_connections])
        if connection_list == '':
            connection_list = "('Snowflake')"
        else:
            connection_list = f"('Snowflake',{connection_list})"

        # Build queries using the formatted connection list
        total_rows_query_openai = f"""
            SELECT COUNT(*) as total
            FROM {table_id}
            WHERE embedding IS NOT NULL
        """

        total_rows_query_native = f"""
            SELECT COUNT(*) as total
            FROM {table_id}
            WHERE embedding_native IS NOT NULL
        """

        missing_native_count = f"""
            SELECT COUNT(*) as total
            FROM {table_id}
            WHERE embedding_native IS NULL
            AND embedding IS NULL
        """

        cursor = self.connection.cursor()
        cursor.execute(total_rows_query_openai)
        total_rows_result_openai = cursor.fetchone()
        total_rows_openai = total_rows_result_openai[0]
        cursor.execute(total_rows_query_native)
        total_rows_result_native = cursor.fetchone()
        total_rows_native = total_rows_result_native[0]

        logger.info(f"Total rows with OpenAI embeddings: {total_rows_openai}")
        logger.info(f"Total rows with native embeddings: {total_rows_native}")

        if total_rows_openai >= total_rows_native:
            embedding_column = 'embedding'
            logger.info(f"Selected embedding column: {embedding_column} (OpenAI embeddings are more or equal)")
        else:
            embedding_column = 'embedding_native'
            logger.info(f"Selected embedding column: {embedding_column} (Native embeddings are more)")

        new_total_rows_query = f"""
            SELECT COUNT(*) as total
            FROM {table_id}
            WHERE {embedding_column} IS NOT NULL
            and source_name in {connection_list}
            """
        cursor = self.connection.cursor()
        cursor.execute(new_total_rows_query)
        total_rows_result = cursor.fetchone()
        total_rows = total_rows_result[0]

        with tqdm(total=total_rows, desc=f"Fetching embeddings for {bot_id}") as pbar:

            while True:
                # Modify the query to include LIMIT and OFFSET
                query = f"""SELECT qualified_table_name, {embedding_column}, source_name
                    FROM {table_id}
                    WHERE {embedding_column} IS NOT NULL
                    AND (source_name IN {connection_list})
                    LIMIT {batch_size} OFFSET {offset}"""
                #            logger.info('fetch query ',query)

                cursor.execute(query)
                rows = cursor.fetchall()

                # Temporary lists to hold batch results
                temp_embeddings = []
                temp_table_names = []

                for row in rows:
                    try:
                        if self.source_name == 'Snowflake':
                            temp_embeddings.append(json.loads('['+row[1][5:-3]+']'))
                        else:
                            temp_embeddings.append(json.loads('['+row[1]+']'))
                        temp_table_names.append(row[2]+"."+row[0])
                        # logger.info('temp_embeddings len: ',len(temp_embeddings))
                        # logger.info('temp table_names: ',temp_table_names)
                    except:
                        try:
                            temp_embeddings.append(json.loads('['+row[1][5:-10]+']'))
                            temp_table_names.append(row[0])
                        except:
                            logger.info('Cant load array from Snowflake')
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
        #   logger.info('table names ',table_names)
        #   logger.info('embeddings len ',len(embeddings))
        return table_names, embeddings

    def generate_filename_from_last_modified(self, table_id, bot_id=None):

        database, schema, table = table_id.split('.')

        if bot_id is None:
            bot_id = 'default'

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
                # raise ValueError("Table last crawled timestamp is None. Unable to generate filename.")

            # The `last_crawled_time` attribute should be a datetime object. Format it.
            last_crawled_time = result[0]['last_crawled_time']
            if isinstance(last_crawled_time, str):
                timestamp_str = last_crawled_time
                if timestamp_str.endswith(':00'):
                    timestamp_str = timestamp_str[:-3]
                timestamp_str = timestamp_str.replace(" ", "T")
                timestamp_str = timestamp_str.replace(".", "")
                timestamp_str = timestamp_str.replace("+", "")
                timestamp_str = timestamp_str.replace("-", "")
                timestamp_str = timestamp_str.replace(":", "")
                timestamp_str = timestamp_str + "Z"
            else:
                timestamp_str = last_crawled_time.strftime("%Y%m%dT%H%M%S") + "Z"

            # Create the filename with the .ann extension
            filename = f"{timestamp_str}_{bot_id}.ann"
            metafilename = f"{timestamp_str}_{bot_id}.json"
            return filename, metafilename
        except Exception as e:
            # Handle errors: for example, table not found, or API errors
            # logger.info(f"An error occurred: {e}, possibly no data yet harvested, using default name for index file.")
            # Return a default filename or re-raise the exception based on your use case
            return f"default_filename_{bot_id}.ann", f"default_metadata_{bot_id}.json"

    def chat_completion_for_escallation(self, message):
        # self.write_message_log_row(db_adapter, bot_id, bot_name, thread_id, 'Supervisor Prompt', message, message_metadata)
        return_msg = None
        default_env_override = os.getenv("BOT_OS_DEFAULT_LLM_ENGINE")
        bot_os_llm_engine = BotLlmEngineEnum(default_env_override) if default_env_override else None
        if bot_os_llm_engine is BotLlmEngineEnum.openai:
            api_key = os.getenv("OPENAI_API_KEY")
            if not api_key:
                logger.info("OpenAI API key is not set in the environment variables.")
                return None

            openai_model = os.getenv("OPENAI_MODEL_SUPERVISOR",os.getenv("OPENAI_MODEL_NAME","gpt-4o-2024-11-20"))

            logger.info('snowpark escallation using model: ', openai_model)
            try:
                client = get_openai_client()
                response = client.chat.completions.create(
                    model=openai_model,
                    messages=[
                        {
                            "role": "user",
                            "content": message,
                        },
                    ],
                )
            except Exception as e:
                if os.getenv("OPENAI_MODEL_SUPERVISOR", None) is not None:
                    openai_model = os.getenv("OPENAI_MODEL_NAME","gpt-4o-2024-11-20")
                    logger.info('retry snowpark escallation using model: ', openai_model)
                    try:
                        client = get_openai_client()
                        response = client.chat.completions.create(
                            model=openai_model,
                            messages=[
                                {
                                    "role": "user",
                                    "content": message,
                                },
                            ],
                        )
                    except Exception as e:
                        logger.info(f"Error occurred while calling OpenAI API with snowpark escallation model {openai_model}: {e}")
                        return None
                else:
                    logger.info(f"Error occurred while calling OpenAI API: {e}")
                    return None

            return_msg = response.choices[0].message.content
        else:
            if bot_os_llm_engine is BotLlmEngineEnum.cortex:
                response, status_code = self.cortex_chat_completion(message)
                if status_code != 200:
                    logger.info(f"Error occurred while calling Cortex API: {response}")
                    return None
                return_msg = response

        return return_msg

    def escallate_for_advice(self, purpose, code, result, packages):
        if True:

            if packages is None or packages == '':
                packages_list = 'No packages specified'
            else:
                packages_list = packages
            message = f"""A less smart AI bot is trying to write code to run in Snowflake Snowpark

### PURPOSE OF CODE: This is the task they are trying to accomplish:

{purpose}

### PACKAGES LIST: The bot said these non-standard python packages would be used and they were indeed successfully installed:

{packages_list}

### CODE: The bot wrote this code:

{code}

### RESULT: The result of trying to run it is:

{result}

### GENERAL SNOWPARK TIPS: Here are some general tips on how to use Snowpark in this environment:

1. If you want to access a file, first save it to stage, and then access it at its stage path, not just /tmp.
2. Be sure to return the result in the global scope at the end of your code.
3. If you want to return a file, save it to /tmp (not root) then base64 encode it and respond like this: image_bytes = base64.b64encode(image_bytes).decode('utf-8')
   result = {{ 'type': 'base64file', 'filename': file_name, 'content': image_bytes, 'mime_type': <mime_type>}}.
4. Do not create a new Snowpark session, use the 'session' variable that is already available to you.
5. Use regular loops not list comprehension
6. If packages are missing, make sure they are included in the PACKAGES list. Many such as matplotlib, pandas, etc are supported.


### SNOWPARK EXAMPLE: Here is an example of successfully using Snowpark for a different task that may be helpful to you:

from snowflake.snowpark.types import StructType, StructField, StringType, IntegerType
from snowflake.snowpark.functions import udf, col

# Define the stage path
stage_file_path = '@GENESIS_BOTS.JANICE_7G8H9J_WORKSPACE.MY_STAGE/state.py'

# Create a schema for reading the CSV file
schema = StructType([
    StructField("value", StringType(), True)
])

# Read the CSV file from the stage
file_df = session.read.schema(schema).option("COMPRESSION", "NONE").csv(stage_file_path)

# Define a Python function to count characters
def count_characters(text):
    return len(text) if text else 0

# Register the UDF to be used in the Snowpark
count_characters_udf = udf(count_characters, return_type=IntegerType(), input_types=[StringType()])

# Apply the UDF to calculate the total number of characters
character_counts = file_df.withColumn("char_count", count_characters_udf(col("value")))

# Sum all character counts
total_chars = character_counts.agg({{"char_count": "sum"}}).collect()[0][0]

# Return the total number of characters
result = total_chars
"""

            message += """

### SNOWPARK EXAMPLE: Here is an example of successfully using Snowpark for a different task (drawing a) that may be helpful to you:

import matplotlib.pyplot as plt

# Load data from the Snowflake table into a Snowpark DataFrame
df = session.table('GENESIS_BOTS.JANICE_7G8H9J_WORKSPACE.RANDOM_TRIPLES')

# Collect the data to local for plotting
rows = df.collect()
x = [row['X'] for row in rows]
y = [row['Y'] for row in rows]
z = [row['Z'] for row in rows]

# Create bubble chart
plt.figure(figsize=(10, 6))
plt.scatter(x, y, s=[size * 10 for size in z], alpha=0.5, c=z, cmap='viridis')
plt.colorbar(label='Z Value')
plt.title('Bubble Chart of Random Triples')
plt.xlabel('X Value')
plt.ylabel('Y Value')
plt.grid(True)

# Save the chart as an image file
plt.savefig('/tmp/bubble_chart.png')

# Encode and return image
import base64
with open('/tmp/bubble_chart.png', 'rb') as image_file:
    image_bytes = base64.b64encode(image_file.read()).decode('utf-8')

result = {'type': 'base64file', 'filename': 'bubble_chart.png', 'content': image_bytes}
"""

            message  +=  """

### SNOWPARK EXAMPLE: Here is an example of successfully using Snowpark for a different task (generating data and saving to a table) that may be helpful to you:

import numpy as np

# Generate random triples without the unnecessary parameter
random_triples = [{'x': int(np.random.randint(1, 1001)),
                   'y': int(np.random.randint(1, 1001)),
                   'z': int(np.random.randint(1, 21))} for _ in range(500)]

# Create a Snowpark DataFrame from the random triples
df = session.create_dataframe(random_triples, schema=['x', 'y', 'z'])

# Define the table name
table_name = 'GENESIS_BOTS.JANICE_7G8H9J_WORKSPACE.RANDOM_TRIPLES'

# Write the DataFrame to a Snowflake table
df.write.mode('overwrite').save_as_table(table_name)

# Return the result indicating success
result = {'message': 'Table created successfully', 'full_table_name': table_name}
"""

            if 'is not defined' in result["Error"]:
                message += """

### NOTE ON IMPORTS: If you def functions in your code, include any imports needed by the function inside the function, as the imports outside function won't convey. For example:

import math

def calc_area_of_circle(radius):
    import math  # import again here as otherwise it wont work

    area = math.pi * radius ** 2
    return round(area, 2)

result = f'The area of a circle of radius 1 is {calc_area_of_circle(1)} using pi as {math.pi}'
"""

            if 'csv' in code:
                message += """

### SNOWPARK CSV EXAMPLE: I see you may be trying to handle CSV files. If useful here's an example way to handle CSVs in Snowpark:

from snowflake.snowpark.functions import col

stage_name = "<fully qualified location>"
file_path = "<csv file name>"

# Read the CSV file from the stage into a DataFrame
df = session.read.option("field_delimiter", ",").csv(f"@{stage_name}/{file_path}")

# Define the table name where you want to save the data
table_name = "<fully qualified output table name with your workspace database and schema specified>"

# Save the DataFrame to the specified table
df.write.mode("overwrite").save_as_table(table_name)

# Verify that the data was saved
result_df = session.table(table_name)
row_count = result_df.count()

result = f'Table {table_name} created, row_count {row_count}.  If the CSV had a header, they are in the first row of the table and can be handled with post-processing SQL to apply them as column names and then remove that row.'"""

            if 'Faker' in code or 'faker' in code:
                message += """

### SNOWPARK FAKER EXAMPLE: Here is an example of how to import and use Faker thay may be helpful to you to fix this error:
from faker import Faker

# Create fake data
fake = Faker()
data = []

# use a regular for loop, NOT list comprehension
for i in range(20):
    data.append({'name': fake.name(), 'email': fake.email(), 'address': fake.address()})

# Drop existing table if it exists
session.sql('DROP TABLE IF EXISTS GENESIS_BOTS.<workspace schema here>.FAKE_CUST').collect()

# Create a new dataframe from the fake data
dataframe = session.createDataFrame(data, schema=['name', 'email', 'address'])

# Write the dataframe to the table
dataframe.write.saveAsTable('<your workspace db.schema>.FAKE_CUST', mode='overwrite')

# Set the result message
result = 'Table FAKE_CUST created successfully.'
"""

            message += """\n\n### YOUR ACTION: So, now, please provide suggestions to the bot on how to fix this code so that it runs successfully in Snowflake Snowpark.\n"""

            potential_result = self.chat_completion_for_escallation(message=message)
            # logger.info(potential_result)
            return potential_result

        else:
            return None

    def add_hints(self, purpose, result, code, packages):

        if isinstance(result, str) and result.startswith('Error:'):
            result = {"Error": result}

        if isinstance(result, dict) and 'Error' in result:
            potential_result = self.escallate_for_advice(purpose, code, result, packages)
            if potential_result is not None:
                # result = potential_result
                result['Suggestion'] = potential_result
            # return potential_result

        return result

    def run_python_code(self,
                        purpose: str = None,
                        code: str = None,
                        packages: str = None,
                        thread_id=None,
                        bot_id=None,
                        note_id=None,
                        note_name = None,
                        note_type = None,
                        return_base64 = False,
                        save_artifacts=False
                        ) -> str|dict:
        """
        Executes a given Python code snippet within a Snowflake Snowpark environment, handling various
        scenarios such as code retrieval from notes, package management, and result processing.

        Parameters:
        - purpose (str, optional): The intended purpose of the code execution.
        - code (str, optional): The Python code to be executed.
        - packages (str, optional): A comma-separated list of additional Python packages required.
        - thread_id: Identifier for the current thread.
        - bot_id: Identifier for the bot executing the code.
        - note_id: Identifier for the note from which to retrieve code.
        - note_name: Name of the note from which to retrieve code.
        - return_base64 (bool, optional): Whether to return results as base64 encoded content.
        - save_artifacts (bool, optional): Whether to save output as Artifacts (an arrifact_id will be included in the response)

        Returns:
        - str: The result JSON of the code execution, which may include error messages, file references,
               and/or base64 encoded content.
        """
        # IMPORTANT: keep the description/parameters of this method in sync with the tool description given to the bots (see snowflake_tools.py)

        # Some solid examples to make bots invoke this:
        # use snowpark to create 5 rows of synthetic customer data using faker, return it in json
        # ... save 100 rows of synthetic data like this to a table called CUSTFAKE1 in your workspace
        #
        # use snowpark python to generate a txt file containing the words "hello world". DO NOT save as an artifact.
        # use snowpark python to geneate a plot of the sin() function for all degrees from 0 to 180. DO NOT save as an artifact. Do not return a path to /tmp - instead, return a base64 encoded content as instrcuted in the function description
        # use snowpark python to geneate a plot of the sin() function for all degrees  from 0 to 180. Use save_artifact=True. Do not return a path to /tmp - instead, return a base64 encoded content as instrcuted in the function description
        # use snowpark python code to generate an chart that plots the result of the following query as a timeseries: query SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.ORDERS table and count the number of orders per date in the last 30 available dates. use save_artifact=false

        import ast
        import os

        def cleanup(proc_name):         # Drop the temporary stored procedure if it was created
            if proc_name is not None and proc_name != 'EXECUTE_SNOWPARK_CODE':
                drop_proc_query = f"DROP PROCEDURE IF EXISTS {self.schema}.{proc_name}(STRING)"
                try:
                    self.run_query(drop_proc_query)
                    logger.info(f"Temporary stored procedure {proc_name} dropped successfully.")
                except Exception as e:
                    logger.info(f"Error dropping temporary stored procedure {proc_name}: {e}")

        try:
            if note_id is not None or note_name is not None:
                note_name = '' if note_name is None else note_name
                note_id = '' if note_id is None else note_id
                get_note_query = f"SELECT note_content, note_params, note_type FROM {self.schema}.NOTEBOOK WHERE NOTE_ID = '{note_id}' OR NOTE_NAME = '{note_name}'"
                cursor = self.connection.cursor()
                cursor.execute(get_note_query)
                code_cursor = cursor.fetchone()

                if code_cursor is None:
                    raise IndexError("Code not found for this note.")

                code = code_cursor[0]
                note_type = code_cursor[2]

                if note_type != 'snowpark_python':
                    raise ValueError("Note type must be 'snowpark_python' for running python code.")
        except IndexError:
            logger.info("Error: The list 'code' is empty or does not have an element at index 0.")
            return {
                    "success": False,
                    "error": "Note was not found.",
                    }

        except ValueError:
            logger.info("Note type must be 'snowpark_python' for code retrieval.")
            return {
                    "success": False,
                    "error": "Wrong tool called. Note type must be 'snowpark_python' to use this tool.",
                    }

        if (bot_id not in ['eva-x1y2z3', 'Armen2-ps73td', os.getenv("O1_OVERRIDE_BOT","")]) and (bot_id is not None and not bot_id.endswith('-o1or')):
            if '\\n' in code:
                if '\n' not in code.replace('\\n', ''):
                    code = code.replace('\\n','\n')
                    code = code.replace('\\n','\n')
            code = code.replace("'\\\'","\'")
        # Check if code contains Session.builder
        if "Session.builder" in code:
            return {
                "success": False,
                "error": "You don't need to make a new snowpark session. Use the session already provided in the session variable without recreating it.",
                "reminder": "Also be sure to return the result in the global scope at the end of your code. "
                            "And if you want to return a file, save it to /tmp (not root) then base64 encode it and respond like this: "
                                 "image_bytes = base64.b64encode(image_bytes).decode('utf-8')\nresult = { 'type': 'base64file', 'filename': file_name, 'content': image_bytes}."
            }
        if "plt.show" in code:
            return {
                "success": False,
                "error": "You can't use plt.show, instead save and return a base64 encoded file.",
                "reminder": "Also be sure to return the result in the global scope at the end of your code. "
                            "And if you want to return a file, save it to /tmp (not root) then base64 encode it and respond like this: "
                                 "image_bytes = base64.b64encode(image_bytes).decode('utf-8')\nresult = { 'type': 'base64file', 'filename': file_name, 'content': image_bytes}."
            }
        if "@MY_STAGE" in code:
            from ...core import global_flags
            workspace_schema_name = f"{global_flags.project_id}.{bot_id.replace(r'[^a-zA-Z0-9]', '_').replace('-', '_')}_WORKSPACE".upper()
            code = code.replace('@MY_STAGE',f'@{workspace_schema_name}.MY_STAGE')
        if "sandbox:/mnt/data" in code:
            from ...core import global_flags
            workspace_schema_name = f"{global_flags.project_id}.{bot_id.replace(r'[^a-zA-Z0-9]', '_').replace('-', '_')}_WORKSPACE".upper()
            return {
                "success": False,
                "error": "You can't reference files in sandbox:/mnt/data, instead add them to your stage and reference them in the stage.",
                "your_stage": workspace_schema_name+".MY_STAGE",
                "reminder": "Also be sure to return the result in the global scope at the end of your code. "
                            "And if you want to return a file, save it to /tmp (not root) then base64 encode it and respond like this: "
                                 "image_bytes = base64.b64encode(image_bytes).decode('utf-8')\nresult = { 'type': 'base64file', 'filename': file_name, 'content': image_bytes}."
            }
        # Check if libraries are provided
        proc_name = 'EXECUTE_SNOWPARK_CODE'
        if packages == '':
            packages = None
        if packages is not None:
            # Split the libraries string into a list
            if ' ' in packages and ',' not in packages:
                packages = packages.replace(' ', ',')
            library_list = [lib.strip() for lib in packages.split(',') if lib.strip() not in ['snowflake-snowpark-python', 'snowflake.snowpark','snowflake','base64','pandas']]
            # Remove any Python standard packages from the library_list
            standard_libs = {name for _, name, _ in pkgutil.iter_modules() if name in sys.stdlib_module_names}
            library_list = [lib for lib in library_list if lib not in standard_libs]

            # Create a new stored procedure with the specified libraries
            libraries_str = ', '.join(f"'{lib}'" for lib in library_list)
            import uuid
            # 'matplotlib', 'scikit-learn'
            if (libraries_str is None or libraries_str != ''):
                proc_name = f"sp_{uuid.uuid4().hex}"
                old_new_stored_proc_ddl = dedent(
                    f"""
                    CREATE OR REPLACE PROCEDURE {self.schema}.{proc_name}(
                        code STRING
                    )
                    RETURNS STRING
                    LANGUAGE PYTHON
                    RUNTIME_VERSION = '3.'
                    PACKAGES = ('snowflake-snowpark-python', 'pandas', {libraries_str})
                    HANDLER = 'run'
                    AS
                    $$
                    import snowflake.snowpark as snowpark
                    import pandas as pd

                    def run(session: snowpark.Session, code: str) -> str:
                        local_vars = {{}}
                        local_vars["session"] = session

                        exec(code, globals(), local_vars)

                        if 'result' in local_vars:
                            return str(local_vars['result'])
                        else:
                            return "Error: 'result' is not defined in the executed code"
                    $$;""")

                new_stored_proc_ddl = dedent(
                    f"""
                    CREATE OR REPLACE PROCEDURE {self.schema}.{proc_name}( code STRING )
                    RETURNS STRING
                    LANGUAGE PYTHON
                    RUNTIME_VERSION = '3.11'
                    PACKAGES = ('snowflake-snowpark-python', 'pandas', {libraries_str})
                    HANDLER = 'run'
                    AS
                    $$
                    import snowflake.snowpark as snowpark
                    import re, importlib

                    def run(session: snowpark.Session, code: str) -> str:
                        # Normalize line endings
                        code = code.replace('\\\\r\\\\n', '\\\\n').replace('\\\\r', '\\\\n')

                        # Find all import statements, including 'from ... import ...'
                        import_statements = re.findall(r'^\\s*(import\\s+.*|from\\s+.*\\s+import\\s+.*)$', code, re.MULTILINE)
                        # Additional regex to find 'from ... import ... as ...' statements
                        import_statements += re.findall(r'^from\\s+(\\S+)\\s+import\\s+(\\S+)\\s+as\\s+(\\S+)', code, re.MULTILINE)

                        global_vars = globals().copy()

                        # Handle imports
                        for import_statement in import_statements:
                            try:
                                exec(import_statement, global_vars)
                            except ImportError as e:
                                return f"Error: Unable to import - {{str(e)}}"

                        local_vars = {{}}
                        local_vars["session"] = local_vars["session"] = session

                        try:
                            # Remove import statements from the code before execution
                            code_without_imports = re.sub(r'^\\s*(import\\s+.*|from\\s+.*\\s+import\\s+.*)$', '', code, flags=re.MULTILINE)
                            exec(code_without_imports, global_vars, local_vars)

                            if 'result' in local_vars:
                                return local_vars['result']
                            else:
                                return "Error: 'result' is not defined in the executed code"
                        except Exception as e:
                            return f"Error: {{str(e)}}"
                    $$
                    """)

                # Execute the new stored procedure creation
                result = self.run_query(new_stored_proc_ddl)

                # Check if the result is a list and if Success is False
                if isinstance(result, dict) and 'Success' in result and result['Success'] == False:
                    result['reminder'] = 'You do not need to specify standard python packages in the packages parameter'
                    return result

                # Update the stored procedure call to use the new procedure
                stored_proc_call = f"CALL {self.schema}.{proc_name}($${code}$$)"
            else:
                stored_proc_call = f"CALL {self.schema}.execute_snowpark_code($${code}$$)"

        else:
            # Use the default stored procedure if no libraries are specified
            stored_proc_call = f"CALL {self.schema}.execute_snowpark_code($${code}$$)"

        result = self.run_query(stored_proc_call)

        if isinstance(result, list):
            result_json = result
            # Check if result is a list and has at least one element
            if isinstance(result, list) and len(result) > 0:
                # Check if 'EXECUTE_SNOWPARK_CODE' key exists in the first element
                proc_name = proc_name.upper()
                if proc_name in result[0]:
                    # If it exists, use its value as the result
                    result = result[0][proc_name]
                    # Try to parse the result as JSON
                    try:
                        result_json = ast.literal_eval(result)
                    except Exception as e:
                        # If it's not valid JSON, keep the original string
                        cleanup(proc_name)
                        result_json = result
                else:
                    # If 'EXECUTE_SNOWPARK_CODE' doesn't exist, use the entire result as is
                    cleanup(proc_name)
                    result_json = result
            else:
                # If result is not a list or is empty, use it as is
                cleanup(proc_name)
                result_json = result

            # Check if 'type' and 'filename' are in the JSON
            if isinstance(result_json, dict) and 'type' in result_json and 'filename' in result_json:
                mime_type = result_json.get("mime_type") # may be missing
                if result_json['type'] == 'base64file':
                    import base64
                    import os

                    # Create the directory if it doesn't exist
                    os.makedirs(f'./runtime/downloaded_files/{thread_id}', exist_ok=True)

                    # Decode the base64 content
                    file_content = base64.b64decode(result_json['content'])

                    if save_artifacts:
                        # Use the artifacts infra to create an artifact from this content
                        from core.bot_os_artifacts import get_artifacts_store
                        af = get_artifacts_store(self)
                        # Build the metadata for this artifact
                        mime_type = mime_type or 'image/png' # right now we assume png is the defualt for type=base64file
                        metadata = dict(mime_type=mime_type,
                                        thread_id=thread_id,
                                        bot_id=bot_id,
                                        title_filename=result_json["filename"],
                                        func_name=inspect.currentframe().f_code.co_name,
                                        )
                        locl = locals()
                        for inp_field, m_field in (('purpose', 'short_description'),
                                                   ('code', 'python_code'),
                                                   ('note_id', 'note_id'),
                                                   ('note_name', 'note_name'),
                                                   ('note_type', 'note_type')):
                            v = locl.get(inp_field)
                            if v:
                                metadata[m_field] = v

                        # Create artifact
                        aid = af.create_artifact_from_content(file_content, metadata, content_filename=result_json["filename"])
                        logger.info(f"Artifact {aid} created for output from python code named {result_json['filename']}")
                        ref_notes = af.get_llm_artifact_ref_instructions(aid)
                        result = {
                            "success": True,
                            "result": f"Output from snowpark is an artifact, which can be later refernced using artifact_id={aid}. "
                                      f"The descriptive name of the file is `{result_json['filename']}`. "
                                      f"The mime type of the file is {mime_type}. "
                                      f"Note: {ref_notes}"
                        }
                    else:
                        # Save the file to 'sandbox'
                        file_path = f'./runtime/downloaded_files/{thread_id}/{result_json["filename"]}'
                        with open(file_path, 'wb') as file:
                            file.write(file_content)
                        logger.info(f"File saved to {file_path}")
                        if return_base64:
                            result = {
                                "success": True,
                                "base64_object": {
                                    "filename": result_json["filename"],
                                    "content": result_json["content"]
                                },
                                "result": "An image or graph has been successfully displayed to the user."
                            }
                        else:
                            result = {
                                "success": True,
                                #"result": f'Snowpark output a file. Output a link like this so the user can see it [description of file](sandbox:/mnt/data/{result_json["filename"]})'
                                "result": f"Output from snowpark is a file. "
                                          f"The descriptive name of the file is `{result_json['filename']}`. "
                                          f"Output a link to this file so the user can see it, using the following formatting rules:"
                                          f" (i) If responding to the user in plain text mode, use markdown like this: '[descriptive name of the file](sandbox:/mnt/data/{result_json['filename']})'. "
                                          f" (ii) If responding to the user in HTML mode, use the most relevant HTML tag to refrence this resource using the url 'sandbox:/mnt/data/{result_json['filename']}' "
                                }
                    cleanup(proc_name)
                    if (bot_id not in ['eva-x1y2z3','Armen2-ps73td',  os.getenv("O1_OVERRIDE_BOT","")]) and (bot_id is not None and not bot_id.endswith('-o1or')):
                        result = self.add_hints(purpose, result, code, packages)
                    return result

                # If conditions are not met, return the original result
                if (bot_id not in ['eva-x1y2z3', 'Armen2-ps73td', os.getenv("O1_OVERRIDE_BOT","")]) and (bot_id is not None and not bot_id.endswith('-o1or')):
                    result_json = self.add_hints(purpose, result_json, code, packages)
                cleanup(proc_name)
                return result_json

            cleanup(proc_name)
            if (bot_id not in ['eva-x1y2z3','Armen2-ps73td', os.getenv("O1_OVERRIDE_BOT","")]) and (bot_id is not None and not bot_id.endswith('-o1or')):

                result_json = self.add_hints(purpose, result_json, code, packages)
            return result_json

        # Check if result is a dictionary and contains 'Error'

        cleanup(proc_name)
        if (bot_id not in ['eva-x1y2z3', 'Armen2-ps73td', os.getenv("O1_OVERRIDE_BOT","")]) and (bot_id is not None and not bot_id.endswith('-o1or')):
            result = self.add_hints(purpose, result, code, packages)
        return result

    def disable_cortex(self):
        query = f'''
            UPDATE {self.genbot_internal_project_and_schema}.LLM_TOKENS
            SET ACTIVE = False
            WHERE LLM_TYPE = 'cortex'
        '''
        res = self.run_query(query)

        query = f'''
            DELETE FROM {self.genbot_internal_project_and_schema}.LLM_TOKENS
            WHERE LLM_TYPE = 'openai'
        '''
        res = self.run_query(query)

        openai_token = os.getenv("OPENAI_API_KEY", "")
        runner_id = os.getenv("RUNNER_ID", "jl-local-runner")
        query = f'''
            INSERT INTO {self.genbot_internal_project_and_schema}.LLM_TOKENS
            (RUNNER_ID, LLM_KEY, LLM_TYPE, ACTIVE) VALUES ('{runner_id}', '{openai_token}', 'openai', True)
        '''
        res = self.run_query(query)


snowflake_tools = ToolFuncGroup(
    name="snowflake_tools",
    description=(
        "Tools for managing and querying database connections, including adding new connections, deleting connections, "
        "listing available connections, and running queries against connected databases"
    ),
    lifetime="PERSISTENT",
)


@gc_tool(
    database="The name of the database.",
    schema="The name of the schema.",
    stage="The name of the stage to list contents for.",
    pattern="The pattern to match when listing contents.",
    bot_id=BOT_ID_IMPLICIT_FROM_CONTEXT,
    thread_id=THREAD_ID_IMPLICIT_FROM_CONTEXT,
    _group_tags_=[snowflake_tools],
)
def _list_stage_contents(
    database: str,
    schema: str,
    stage: str,
    pattern: str = None,
    bot_id: str = None,
    thread_id: str = None,
):
    """
    Lists the contents of a given Snowflake stage, up to 50 results (use pattern param if more than that).
    Run SHOW STAGES IN SCHEMA <database>.<schema> to find stages.
    """
    return SnowflakeConnector("Snowflake").list_stage_contents(
        database=database,
        schema=schema,
        stage=stage,
        pattern=pattern,
        bot_id=bot_id,
        thread_id=thread_id,
    )

@gc_tool(
    database="The name of the database. Use your WORKSPACE database unless told to use something else.",
    schema="The name of the schema.  Use your WORKSPACE schema unless told to use something else.",
    stage="The name of the stage to add the file to. Use your WORKSPACE stage unless told to use something else.",
    file_name="The original filename of the file, human-readable. Can optionally include a relative path, such as bot_1_files/file_name.txt",
    bot_id=BOT_ID_IMPLICIT_FROM_CONTEXT,
    thread_id=THREAD_ID_IMPLICIT_FROM_CONTEXT,
    _group_tags_=[snowflake_tools]
)
def _add_file_to_stage(
    database: str,
    schema: str,
    stage: str,
    file_name: str,
    bot_id: str = None,
    thread_id: str = None,
):
    """
    Uploads a file from an OpenAI FileID to a Snowflake stage. Replaces if exists.
    """
    return SnowflakeConnector("Snowflake").add_file_to_stage(
        database=database,
        schema=schema,
        stage=stage,
        file_name=file_name,
        bot_id=bot_id,
        thread_id=thread_id,
    )

@gc_tool(
    database="The name of the database. Use your WORKSPACE database unless told to use something else.",
    schema="The name of the schema.  Use your WORKSPACE schema unless told to use something else.",
    stage="The name of the stage to add the file to. Use your WORKSPACE stage unless told to use something else.",
    file_name="The original filename of the file, human-readable. Can optionally include a relative path, such as bot_1_files/file_name.txt",
    bot_id=BOT_ID_IMPLICIT_FROM_CONTEXT,
    thread_id=THREAD_ID_IMPLICIT_FROM_CONTEXT,
    _group_tags_=[snowflake_tools]
)
def _delete_file_from_stage(
    database: str,
    schema: str,
    stage: str,
    file_name: str,
    bot_id: str = None,
    thread_id: str = None,
    ):
    """
    Deletes a file from a Snowflake stage.
    """
    return SnowflakeConnector("Snowflake").delete_file_from_stage(
        database=database,
        schema=schema,
        stage=stage,
        file_name=file_name,
        bot_id=bot_id,
        thread_id=thread_id,
    )

@gc_tool(
    database="The name of the database. Use your WORKSPACE database unless told to use something else.",
    schema="The name of the schema.  Use your WORKSPACE schema unless told to use something else.",
    stage="The name of the stage to add the file to. Use your WORKSPACE stage unless told to use something else.",
    file_name="The original filename of the file, human-readable. Can optionally include a relative path, such as bot_1_files/file_name.txt",
    return_contents="Whether to return the contents of the file or just the file name.",
    is_binary="Whether to return the contents of the file as binary or text.",
    bot_id=BOT_ID_IMPLICIT_FROM_CONTEXT,
    thread_id=THREAD_ID_IMPLICIT_FROM_CONTEXT,
    _group_tags_=[snowflake_tools],)
def _read_file_from_stage(
        database: str,
        schema: str,
        stage: str,
        file_name: str,
        return_contents: bool = False,
        is_binary: bool = False,
        bot_id: str = None,
        thread_id: str = None,
    ):
    """
    Reads a file from a Snowflake stage.
    """
    return SnowflakeConnector("Snowflake").read_file_from_stage(
        database=database,
        schema=schema,
        stage=stage,
        file_name=file_name,
        return_contents=return_contents,
        is_binary=is_binary,
        bot_id=bot_id,
        thread_id=thread_id,
    )

@gc_tool(
        query="A short search query of what kind of data the user is looking for.",
        service_name="Name of the service. You must know this in advance and specify it exactly.",
        top_n="How many of the top results to return, max 25, default 15.  Use 15 to start.",
        bot_id=BOT_ID_IMPLICIT_FROM_CONTEXT,
        thread_id=THREAD_ID_IMPLICIT_FROM_CONTEXT,
        _group_tags_=[snowflake_tools],
        )
def _cortex_search(
    query: str,
    service_name: str,
    top_n: int = 15,
    bot_id: str = None,
    thread_id: str = None,
):
    """
    Use this to search a cortex full text search index.  Do not use this to look for database metadata or tables, for
    that use search_metadata instead.
    """
    return SnowflakeConnector("Snowflake").cortex_search(
        query=query,
        service_name=service_name,
        top_n=top_n,
        bot_id=bot_id,
        thread_id=thread_id,
    )


@gc_tool(
    purpose="A detailed explanation in English of what this code is supposed to do. This will be used to help validate the code.",
    code=dedent(
    """
    The Python code to execute in Snowflake Snowpark. The snowpark 'session' is already
    created and ready for your code's use, do NOT create a new session. Run queries inside of
    Snowpark versus inserting a lot of static data in the code. Use the full names of any stages
    with database and schema. If you want to access a file, first save it to stage, and then access
    it at its stage path, not just /tmp. Always set 'result' variable at the end of the code execution
    in the global scope to what you want to return. DO NOT return a path to a file. Instead, return
    the file content by first saving the content to /tmp (not root) then base64-encode it and respond
    like this: image_bytes = base64.b64encode(image_bytes).decode('utf-8')\nresult = { 'type': 'base64file',
    'filename': file_name, 'content': image_bytes, mime_type: <mime_type>}. Be sure to properly escape any
    double quotes in the code.
    """
    ),
    packages=dedent(
        """A comma-separated list of required non-default Python packages to be pip installed for code execution
        (do not include any standard python libraries). For graphing, include matplotlib in this list."""
    ),
    note_id=dedent(
        """An id for a note in the notebook table.  The note_id will be used to look up the
        python code from the note content in lieu of the code field. A note_id will take precedence
        over the code field, that is, if the note_id is not empty, the contents of the note will be run
        instead of the content of the code field."""
    ),
    save_artifacts=dedent(
        """A flag determining whether to save any output from the executed python code
        (encoded as a base64 string) as an 'artifact'. When this flag is set, the result will contain
        a UUID called 'artifact_id' for referencing the output in the future. When this flag is not set,
        any output from the python code will be saved to a local file and the result will contain a path
        to that file.  This local file should not be considered accessible by outside systems."""
    ),
    bot_id=BOT_ID_IMPLICIT_FROM_CONTEXT,
    thread_id=THREAD_ID_IMPLICIT_FROM_CONTEXT,
    _group_tags_=[snowflake_tools],
)
def _run_snowpark_python(
    purpose: str = None,
    code: str = None,
    packages: str = None,
    note_id: str = None,
    save_artifacts: bool = True,
    bot_id: str = None,
    thread_id: str = None,
    ):
    """
    This function accepts a string containing Python code and executes it using Snowflake's Snowpark python environment.
    Code is run using a precreated and provided Snowpark 'session', do not create a new session.
    Results should only have a single object.  Multiple objects are not allowed.  Provide EITHER the 'code' field with the
    python code to run, or the 'note_id' field with the id of the note referencing the pre-saved program you want to run.
    """
    return SnowflakeConnector("Snowflake").run_python_code(
        purpose=purpose,
        code=code,
        packages=packages,
        note_id=note_id,
        bot_id=bot_id,
        thread_id=thread_id,
        save_artifacts=save_artifacts,
    )

_all_snowflake_connector_functions = [
    _list_stage_contents,
    _add_file_to_stage,
    _delete_file_from_stage,
    _read_file_from_stage,
    _cortex_search,
    _run_snowpark_python,]



# Called from bot_os_tools.py to update the global list of data connection tool functions
def get_snowflake_connector_functions():
    return _all_snowflake_connector_functions
