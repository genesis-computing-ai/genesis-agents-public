# Patch the deprecated package before importing snowflake
import sys
from functools import wraps

# Check if deprecated is already in sys.modules
if 'deprecated' in sys.modules:
    # Get the original deprecated module
    deprecated_module = sys.modules['deprecated']
    
    # Save the original deprecated function
    original_deprecated = deprecated_module.deprecated
    
    # Create a patched version that handles the 'name' parameter
    @wraps(original_deprecated)
    def patched_deprecated(*args, **kwargs):
        # Remove the 'name' parameter if present
        if 'name' in kwargs:
            del kwargs['name']
        return original_deprecated(*args, **kwargs)
    
    # Replace the original with our patched version
    deprecated_module.deprecated = patched_deprecated

# Now import snowflake connector
from snowflake.connector import connect, SnowflakeConnection

import os
import json
import uuid
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
        # Trim whitespace from genbot_internal_project_and_schema if it exists
        if self.genbot_internal_project_and_schema:
            self.genbot_internal_project_and_schema = self.genbot_internal_project_and_schema.strip()
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
        self.index_manager_table_name = self.genbot_internal_project_and_schema + "." + "INDEX_MANAGER"

    from .ensure_table_exists import (ensure_table_exists, one_time_db_fixes, get_process_info,
                                      get_processes_list)
    
    from .stage_utils import (add_file_to_stage, read_file_from_stage, update_file_in_stage,
                              delete_file_from_stage, list_stage_contents, test_stage_functions)

    from .cortex_utils import (check_cortex_available, test_cortex, test_cortex_via_rest,
                               cortex_chat_completion, get_cortex_search_service, cortex_search)

    from .harvest_utils import (get_harvest_control_data_as_json, set_harvest_control_data, remove_harvest_control_data,
                                get_harvest_summary, get_available_databases, check_cached_metadata,
                                get_metadata_from_cache, get_databases, generate_filename_from_last_modified)

    from .bot_utils import (db_insert_new_bot, db_update_bot_tools, db_update_bot_files, db_update_slack_app_level_key,
                            db_update_bot_instructions, db_update_bot_implementation, db_update_slack_allow_list,
                            db_get_bot_access, db_get_bot_details, db_get_bot_database_creds, db_update_existing_bot,
                            db_update_existing_bot_basics, db_update_bot_details, db_delete_bot, db_remove_bot_tools,
                            db_list_all_bots)

    from .snowpark_utils import (_create_snowpark_connection, escallate_for_advice, add_hints, run_python_code,
                                 chat_completion_for_escallation, check_eai_assigned, get_endpoints, delete_endpoint_group,
                                 set_endpoint, eai_test, db_get_endpoint_ingress_url)
    
    @functools.cached_property
    def is_using_local_runner(self):
        val = os.environ.get('SPCS_MODE', 'FALSE')
        if val.lower() == 'true':
            return False
        else:
            return True

    def get_credentials(self, credential_type):
        # Return the credentials for Google Drive & WebAccess
        query = f"""
        SELECT * FROM {self.genbot_internal_project_and_schema}.EXT_SERVICE_CONFIG WHERE ext_service_name = %s
        """
        # Execute the query
        cursor = self.client.cursor()
        cursor.execute(query, (credential_type,))
        rows = cursor.fetchall()
        if rows:
            filtered_rows = [row if row[1].lower() != 'private_key' else (row[0], row[1], 'Secret') for row in rows]
            return {'Success': True, 'Data': json.dumps([list(map(lambda x: x.isoformat() if isinstance(x, datetime.datetime) else x, row)) for row in filtered_rows], default=str)}
        return {'Success': False, 'Error': f"No credentials found for {credential_type}"}

    def sha256_hash_hex_string(self, input_string):
        # Encode the input string to bytes, then create a SHA256 hash and convert it to a hexadecimal string
        return hashlib.sha256(input_string.encode()).hexdigest()

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

    def get_dbtcloud_config_params(self):
        """
        Retrieves dbt cloud configuration parameters from the database.

        Returns:
            dict: A dictionary containing dbt cloud configuration parameters.
        """
        try:
            query = f"SELECT parameter, value FROM {self.schema}.EXT_SERVICE_CONFIG WHERE ext_service_name = 'dbtcloud';"
            cursor = self.client.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()

            if not rows:
                return {"Success": False, "Error": "No dbt cloud configuration found"}

            params = {}
            for row in rows:
                params[row[0]] = row[1]

            return dict(Success=True, Config=params)

        except Exception as e:
            return dict(Success=False, Error=f"An error occurred while getting dbt cloud config params: {e}")

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

            if service_name == 'dbtcloud':
                self.create_dbtcloud_creds()

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

        creds_dict = {row[0]: row[1] for row in rows}

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

    def create_dbtcloud_creds(self):
        # Query to get dbtcloud configuration parameters
        query = f"SELECT parameter, value FROM {self.schema}.EXT_SERVICE_CONFIG WHERE ext_service_name = 'dbtcloud' and parameter = 'dbtcloud_access_url';"
        
        cursor = self.client.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        
        if not rows:
            return False
        
        # Convert rows to dictionary
        config_dict = {row[0]: row[1] for row in rows}
        
        # Get the access URL
        access_url = config_dict.get('dbtcloud_access_url')
        if not access_url:
            return False
        
        # Insert/update the endpoint in CUSTOM_ENDPOINTS table
        upsert_query = f"""
        MERGE INTO {self.schema}.CUSTOM_ENDPOINTS t
        USING (SELECT 'DBTCLOUD' as type, %s as endpoint) s
        ON t.TYPE = s.type
        WHEN MATCHED THEN
            UPDATE SET t.ENDPOINT = s.endpoint
        WHEN NOT MATCHED THEN
            INSERT (TYPE, ENDPOINT) VALUES (s.type, s.endpoint)
        """
        
        cursor.execute(upsert_query, (access_url,))
        self.client.commit()
        
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
        bot_os_thread=None
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

            if bot_os_thread is not None and bot_os_thread.thread_id != thread_id:
                thread_id = bot_os_thread.thread_id

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
        catalog_supplement=None,
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

        catalog_supplement_loaded = None
        if catalog_supplement:
            catalog_supplement_loaded = 'TRUE'

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
            "catalog_supplement": catalog_supplement,
            "catalog_supplement_loaded": catalog_supplement_loaded
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
                    %(embedding)s AS {embedding_target},
                    %(catalog_supplement)s AS catalog_supplement,
                    %(catalog_supplement_loaded)s AS catalog_supplement_loaded
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
                {embedding_target} = ARRAY_CONSTRUCT(new_data.{embedding_target}),
                catalog_supplement = new_data.catalog_supplement,
                catalog_supplement_loaded = new_data.catalog_supplement_loaded
            WHEN NOT MATCHED THEN INSERT (
                source_name, qualified_table_name, memory_uuid, database_name,
                schema_name, table_name, complete_description, ddl, ddl_short,
                ddl_hash, summary, sample_data_text, last_crawled_timestamp,
                crawl_status, role_used_for_crawl, {embedding_target},
                catalog_supplement, catalog_supplement_loaded
            ) VALUES (
                new_data.source_name, new_data.qualified_table_name, new_data.memory_uuid, new_data.database_name,
                new_data.schema_name, new_data.table_name, new_data.complete_description, new_data.ddl, new_data.ddl_short,
                new_data.ddl_hash, new_data.summary, new_data.sample_data_text, TO_TIMESTAMP_NTZ(new_data.last_crawled_timestamp),
                new_data.crawl_status, new_data.role_used_for_crawl, ARRAY_CONSTRUCT(new_data.{embedding_target}),
                new_data.catalog_supplement, new_data.catalog_supplement_loaded
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
                            {embedding_target} = :embedding,
                            catalog_supplement = :catalog_supplement,
                            catalog_supplement_loaded = :catalog_supplement_loaded
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
                            crawl_status, role_used_for_crawl, {embedding_target},
                            catalog_supplement, catalog_supplement_loaded
                        ) VALUES (
                            :source_name, :qualified_table_name, :memory_uuid, :database_name,
                            :schema_name, :table_name, :complete_description, :ddl, :ddl_short,
                            :ddl_hash, :summary, :sample_data_text, :last_crawled_timestamp,
                            :crawl_status, :role_used_for_crawl, :embedding,
                            :catalog_supplement, :catalog_supplement_loaded)

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


    # snowed
    def refresh_connection(self):
        if self.token_connection:
            self.connection = self._create_connection()

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
                "message": result.get("message", None),
                **({"folder_url": result["folder_url"]} if "folder_url" in result else {}),
                **({"file_url": result["file_url"]} if "file_url" in result else {}), 
                **({"file_id": result["file_id"]} if "file_id" in result else {})
            }

        return sample_data

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
            cursor = self.client.cursor()
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


    def read_thread_messages(self, thread_id):
        """
        Query messages from a specific thread, filtering for user prompts and assistant responses.
        If no results found with the given thread_id, try to find a valid thread_id from message_metadata.
        
        Args:
            thread_id (str): The thread ID to query messages for
            
        Returns:
            list: List of message records from the thread
        """
        query = f"""
            SELECT message_type, message_payload, bot_id FROM {self.message_log_table_name}
            WHERE thread_id = %s
            AND (message_type = 'User Prompt' OR message_type = 'Assistant Response')
            AND message_payload <> 'Tool call completed, results'
            ORDER BY timestamp
        """

        try:
            cursor = self.client.cursor()
            cursor.execute(query, (thread_id,))
            results = cursor.fetchall()

            # If no results found, try to find a valid thread_id from message_metadata
            if not results:
                fallback_query = f"""
                    SELECT any_value(thread_id) as thread_id FROM {self.message_log_table_name}
                    WHERE message_metadata LIKE %s
                    AND (message_type = 'User Prompt' OR message_type = 'Assistant Response')
                    AND message_payload <> 'Tool call completed, results'
                    ORDER BY timestamp DESC
                    LIMIT 1
                """
                cursor.execute(fallback_query, (f'%{thread_id}%',))
                thread_id_result = cursor.fetchone()

                # If we found a valid thread_id, retry the original query
                if thread_id_result:
                    new_thread_id = thread_id_result[0]
                    cursor.execute(query, (new_thread_id,))
                    results = cursor.fetchall()

            cursor.close()
            return results
        except Exception as e:
            logger.error(f"Error querying thread messages: {e}")
            return []

    def get_list_threads(self, bot_name):
        """
        get list of threads for a bot
        
        Args:
            bot_id (str): The bot ID to query messages for
            
        Returns:
            list: List of thread IDs
        """
        query = f"""
            SELECT thread_id, max(timestamp) as timestamp FROM {self.message_log_table_name}
            WHERE bot_name = '{bot_name}'
            AND (message_type = 'User Prompt' OR message_type = 'Assistant Response')
            AND message_payload <> 'Tool call completed, results'
            GROUP BY thread_id
            HAVING COUNT(*) > 2
            ORDER BY timestamp DESC
        """
        
        try:
            res = self.run_query(query)
            return res
        except Exception as e:
            logger.error(f"Error querying thread messages: {e}")
            return []


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
    file_name=ToolFuncParamDescriptor(
        name="file_name",
        description="The full local path to the file to add to stage",
        required=True,
        llm_type_desc=dict(type="string"),
    ),
    target_path="The relative path of the file as you'd like it to be located at on stage, such as my_files/good_files, not including the file name (optional)",
    bot_id=BOT_ID_IMPLICIT_FROM_CONTEXT,
    thread_id=THREAD_ID_IMPLICIT_FROM_CONTEXT,
    _group_tags_=[snowflake_tools]
)
def _add_file_to_stage(
    database: str,
    schema: str,
    stage: str,
    file_name: str,
    target_path: str = None,
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
        target_path=target_path,
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
    max_bytes="The maximum number of bytes of content to return. Default is 10000.",
    is_binary="True to return contents of a binary file.",
    bot_id=BOT_ID_IMPLICIT_FROM_CONTEXT,
    thread_id=THREAD_ID_IMPLICIT_FROM_CONTEXT,
    _group_tags_=[snowflake_tools],)
def _read_file_from_stage(
        database: str,
        schema: str,
        stage: str,
        file_name: str,
        return_contents: bool = False,
        max_bytes: int = 10000,
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
        max_bytes=max_bytes,
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
