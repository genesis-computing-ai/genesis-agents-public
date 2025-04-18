import glob
import sqlite3
import pandas as pd
import pytz
from snowflake.connector import connect
from tqdm import tqdm

import os
import json
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

from genesis_bots.llm.llm_openai.openai_utils import get_openai_client
from genesis_bots.connectors.data_connector import DatabaseConnector
from genesis_bots.connectors.connector_helpers import llm_keys_and_types_struct
from genesis_bots.core.bot_os_defaults import (
    BASE_EVE_BOT_INSTRUCTIONS,
    ELIZA_DATA_ANALYST_INSTRUCTIONS,
    STUART_DATA_STEWARD_INSTRUCTIONS,
    EVE_INTRO_PROMPT,
    ELIZA_INTRO_PROMPT,
    STUART_INTRO_PROMPT,
)

from genesis_bots.core.bot_os_llm import BotLlmEngineEnum

# from database_connector import DatabaseConnector
from threading import Lock
import base64
import requests
import re
from openai import OpenAI


import genesis_bots.core.bot_os_tool_descriptions

from genesis_bots.core.logging_config import logger

_semantic_lock = Lock()


class SqliteConnector(DatabaseConnector):
    def __init__(self, connection_name):
        super().__init__(connection_name)
        self.account = os.getenv("SNOWFLAKE_ACCOUNT_OVERRIDE", None)
        self.user = os.getenv("SNOWFLAKE_USER_OVERRIDE", None)
        self.password = os.getenv("SNOWFLAKE_PASSWORD_OVERRIDE", None)
        self.database = os.getenv("SNOWFLAKE_DATABASE_OVERRIDE", None)
        self.warehouse = os.getenv("SNOWFLAKE_WAREHOUSE_OVERRIDE", None)
        self.role = os.getenv("SNOWFLAKE_ROLE_OVERRIDE", None)
        self.client = sqlite3.connect(os.getenv("SQLITE_DB", 'genesis.db'), check_same_thread=False)
        self.stages = os.getenv("SQLITE_STAGES", "sqlite_stages")
        self.metadata_table_name = os.getenv("GENESIS_INTERNAL_HARVEST_RESULTS_TABLE", "harvest_results")
        self.harvest_control_table_name = os.getenv("GENESIS_INTERNAL_HARVEST_CONTROL_TABLE", "harvest_control")
        self.message_log_table_name = os.getenv("GENESIS_INTERNAL_MESSAGE_LOG_TABLE", "MESSAGE_LOG" )
        self.knowledge_table_name = os.getenv("GENESIS_INTERNAL_KNOWLEDGE_TABLE", "KNOWLEDGE")
        self.user_bot_table_name = os.getenv("GENESIS_INTERNAL_USER_BOT_TABLE", "USER_BOT")
        self.slack_tokens_table_name = "SLACK_APP_CONFIG_TOKENS"
        self.bot_servicing_table_name = "BOT_SERVICING"
        self.ngrok_tokens_table_name = "NGROK_TOKENS"
        self.images_table_name =  "APP_SHARE_IMAGES"
        self.source_name = "Sqlite"

    def is_using_local_runner(self):
        val = os.environ.get('SPCS_MODE', 'FALSE')
        if val.lower() == 'true':
            return False
        else:
            return True

    def check_cortex_available(self):
        return False

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
            cursor = self.client.cursor()
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
                UPDATE {self.harvest_control_table_name}
                    SET initial_crawl_complete = :initial_crawl_complete,
                        refresh_interval = :refresh_interval,
                        schema_exclusions = :schema_exclusions,
                        schema_inclusions = :schema_inclusions,
                        status = :status
                    WHERE source_name = :source_name AND database_name = :database_name;
            """
            cursor = self.client.cursor()
            cnt = cursor.execute(
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
            cursor.close()
            if cnt.rowcount == 0:
                self.run_insert(self.harvest_control_table_name,
                        source_name= source_name,
                        database_name= database_name,
                        initial_crawl_complete= initial_crawl_complete,
                        refresh_interval= refresh_interval,
                        schema_exclusions= str(schema_exclusions),
                        schema_inclusions= str(schema_inclusions),
                        status= status)

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
            # Construct the query to delete the row
            # query = f"""
            # DELETE FROM {self.harvest_control_table_name}
            # WHERE UPPER(source_name) = UPPER(?) AND UPPER(database_name) = UPPER(?)
            # """
            # TODO test!! Construct the query to exclude the row
            query = f"""
            UPDATE {self.harvest_control_table_name}
            SET STATUS = 'Exclude'
            WHERE UPPER(source_name) = UPPER(?) AND UPPER(database_name) = UPPER(?) AND STATUS = 'Include'
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
            WHERE source_name = ? AND database_name = ?
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
            return {"Success": True, "Databases": self.database}

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
            json_data = json.dumps(
                bot_list, default=str
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
        WHERE qualified_table_name = ?
        """
        try:
            cursor = self.client.cursor()
            cursor.execute(query, (qualified_table_name,))
            result = cursor.fetchone()

            return result[0] > 0  # Returns True if a row exists, False otherwise
        except Exception as e:
            logger.info(f"An error occurred while checking if the table summary exists: {e}")
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
            formatted_timestamp = timestamp.strftime("%Y-%m-%d %H:%M:%S") if isinstance(timestamp, datetime) else timestamp
            if isinstance(message_metadata, dict):
                message_metadata = json.dumps(message_metadata)

            insert_query = f"""
            INSERT INTO {self.message_log_table_name}
                (timestamp, bot_id, bot_name, thread_id, message_type, message_payload, message_metadata, tokens_in, tokens_out, files, channel_type, channel_name, primary_user, task_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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

    # ========================================================================================================

    def get_processes_list(self, bot_id="all"):
        cursor = self.client.cursor()
        try:
            if bot_id == "all":
                list_query = f"SELECT process_id, bot_id, process_name, process_instructions FROM PROCESSES"
                cursor.execute(list_query)
            else:
                list_query = f"SELECT process_id, bot_id, process_name, process_instructions FROM PROCESSES WHERE upper(bot_id) = upper(%s)"
                cursor.execute(list_query, (bot_id,))
            processs = cursor.fetchall()
            process_list = []
            for process in processs:
                process_dict = {
                    "process_id": process[0],
                    "bot_id": process[1],
                    "process_name": process[2],
#                    "process_details": process[4],
                    "process_instructions": process[3],
            #        "process_reporting_instructions": process[5],
                }
                process_list.append(process_dict)
            return {"Success": True, "processes": process_list}

        except Exception as e:
            return {
                "Success": False,
                "Error": f"Failed to list processs for bot {bot_id}: {e}",
            }

    def get_process_info(self, bot_id, process_name):
        cursor = self.client.cursor()
        try:
            query = f"SELECT * FROM {self.schema}.PROCESSES WHERE bot_id like ? AND process_name LIKE ?"
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

    def OLD_OLD_manage_processes(
        self, action, bot_id=None, process_id=None, process_details=None, thread_id=None
    ):
        """
        Manages processs in the PROCESSES table with actions to create, delete, or update a process.

        Args:
            action (str): The action to perform - 'CREATE', 'DELETE','UPDATE', 'LIST','SHOW'.
            bot_id (str): The bot ID associated with the process.
            process_id (str): The process ID for the process to manage.
            process_details (dict, optional): The details of the process for create or update actions.

        Returns:
            dict: A dictionary with the result of the operation.
        """
        required_fields_create = [
            "process_name",
            "process_details",
            "process_instructions",
            "process_reporting_instructions",
        ]

        required_fields_update = [
            "process_details",
            "process_instructions",
            "process_reporting_instructions",
        ]

        action = action.upper()

        if action == "TIME":
            return {
                "current_system_time": datetime.now().strftime("%Y-%m-%d %H:%M:? %Z")
            }

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
                default_eng_override = os.getenv("BOT_OS_DEFAULT_LLM_ENGINE")
                default_llm_engine = BotLlmEngineEnum(default_eng_override) if default_eng_override else None
                if default_llm_engine is BotLlmEngineEnum.openai:
                    api_key = os.getenv("OPENAI_API_KEY")
                    if not api_key:
                        logger.info("OpenAI API key is not set in the environment variables.")
                        return None

                    openai_api_key = os.getenv("OPENAI_API_KEY")
                    client = OpenAI(api_key=openai_api_key)
                    response = client.chat.completions.create(
                        model="gpt-4o-2024-11-20",
                        messages=[
                            {
                                "role": "user",
                                "content": tidy_process_instructions,
                            },
                        ],
                    )

                    process_details['process_instructions'] = response.choices[0].message.content

            #     elif os.getenv("BOT_OS_DEFAULT_LLM_ENGINE") == 'cortex':
            #         if not self.check_cortex_available():
            #             logger.info("Cortex is not available.")
            #             return None
            #         else:
            #             response, status_code = self.cortex_chat_completion(tidy_process_instructions)
            #             process_details['process_instructions'] = response

            if action == "CREATE":
                return {
                    "Success": False,
                    "Confirmation_Needed": "Please reconfirm all the process details with the user, then call this function again with the action CREATE_CONFIRMED to actually create the process.",
                    "Info": f"By the way the current system time is {datetime.now().strftime('%Y-%m-%d %H:%M:? %Z')}",
                }

            if action == "UPDATE":
                return {
                    "Success": False,
                    "Confirmation_Needed": "Please reconfirm all the process details with the user, especially that you're altering the correct process_ID, then call this function again with the action UPDATE_CONFIRMED to actually update the process.  Call with LIST to double-check the process_id if you aren't sure.",
                    "Info": f"By the way the current system time is {datetime.now().strftime('%Y-%m-%d %H:%M:? %Z')}",
                }

        except Exception as e:
            return {"Success": False, "Error": f"Error connecting to LLM: {e}"}

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

        if action not in ["CREATE", "DELETE", "UPDATE", "LIST"]:
            return {"Success": False, "Error": "Invalid action specified."}

        cursor = self.client.cursor()

        if action == "LIST":
            logger.info("Running get processes list")
            return self.get_processes_list("all")

        if action == "SHOW":
            logger.info("Running show process info")
            return self.get_process_info(bot_id)

        if process_id is None:
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

        if action == "UPDATE" and process_details.get("process_active", False):
            if "next_check_ts" not in process_details:
                return {
                    "Success": False,
                    "Error": "The 'next_check_ts' field is required when updating an active process.",
                }

        # Convert timestamp from string in format 'YYYY-MM-DD HH:MM:SS' to a Snowflake-compatible timestamp
        if process_details is not None and process_details.get("process_active", False):
            try:
                formatted_next_check_ts = datetime.strptime(
                    process_details["next_check_ts"], "%Y-%m-%d %H:%M:?"
                )
            except ValueError as ve:
                return {
                    "Success": False,
                    "Error": f"Invalid timestamp format for 'next_check_ts'. Required format: 'YYYY-MM-DD HH:MM:SS' in system timezone. Error details: {ve}",
                    "Info": f"Current system time in system timezone is {datetime.now().strftime('%Y-%m-%d %H:%M:?')}. The system timezone is {datetime.now().strftime('%Z')}. Please note that the timezone should not be included in the submitted timestamp.",
                }
            if formatted_next_check_ts < datetime.now():
                return {
                    "Success": False,
                    "Error": "The 'next_check_ts' is in the past.",
                    "Info": f"Current system time is {datetime.now().strftime('%Y-%m-%d %H:%M:? %Z')}",
                }

        try:
            if action == "CREATE":
                insert_query = f"""
                    INSERT INTO PROCESSES (
                        process_id, bot_id, bot_slack_user_id, process_name, process_details, process_instructions, process_reporting_instructions
                    ) VALUES (
                        %(process_id)s, %(bot_id)s, %(bot_slack_user_id)s, %(process_name)s, %(process_details)s, %(process_instructions)s, %(process_reporting_instructions)s
                    )
                """

                # Generate 6 random alphanumeric characters
                random_suffix = "".join(
                    random.choices(string.ascii_letters + string.digits, k=6)
                )
                process_id_with_suffix = process_id + "_" + random_suffix
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
                }

            elif action == "DELETE":
                delete_query = f"""
                    DELETE FROM PROCESSES
                    WHERE process_id = ?
                """
                cursor.execute(delete_query, (process_id))
                self.client.commit()

            elif action == "UPDATE":
                update_query = f"""
                    UPDATE PROCESSES
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
            INSERT INTO PROCESS_HISTORY (
                process_id, work_done_summary, process_status, updated_process_learnings,
                report_message, done_flag, needs_help_flag, process_clarity_comments
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?
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
            logger.info(
                f"Process history row inserted successfully for process_id: {process_id}"
            )
        except Exception as e:
            logger.info(f"An error occurred while inserting the process history row: {e}")
            if cursor is not None:
                cursor.close()

    # ========================================================================================================

    def manage_tasks(
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
            "task_instructions",
            "reporting_instructions",
            "last_task_status",
            "task_learnings",
            "task_active",
        ]

        required_fields_update = ["last_task_status", "task_learnings", "task_active"]

        if action == "TIME":
            return {
                "current_system_time": datetime.now().strftime("%Y-%m-%d %H:%M:? %Z")
            }
        action = action.upper()

        if action == "CREATE":
            return {
                "Success": False,
                "Confirmation_Needed": "Please reconfirm all the task details with the user, then call this function again with the action CREATE_CONFIRMED to actually create the task.   Make sure to be clear in the action_trigger_details field whether the task is to be triggered one time, or if it is ongoing and recurring.",
                "Info": f"By the way the current system time is {datetime.now().strftime('%Y-%m-%d %H:%M:? %Z')}",
            }
        if action == "CREATE_CONFIRMED":
            action = "CREATE"

        if action == "UPDATE":
            return {
                "Success": False,
                "Confirmation_Needed": "Please reconfirm all the task details with the user, especially that you're altering the correct TASK_ID, then call this function again with the action UPDATE_CONFIRMED to actually update the task.  Call with LIST to double-check the task_id if you aren't sure.",
                "Info": f"By the way the current system time is {datetime.now().strftime('%Y-%m-%d %H:%M:? %Z')}",
            }
        if action == "UPDATE_CONFIRMED":
            action = "UPDATE"

        if action == "DELETE":
            return {
                "Success": False,
                "Confirmation_Needed": "Please reconfirm that you are deleting the correct TASK_ID, and double check with the user they want to delete this task, then call this function again with the action DELETE_CONFIRMED to actually delete the task.  Call with LIST to double-check the task_id if you aren't sure that its right.",
            }

        if action == "DELETE_CONFIRMED":
            action = "DELETE"

        if action not in ["CREATE", "DELETE", "UPDATE", "LIST"]:
            return {"Success": False, "Error": "Invalid action specified."}

        cursor = self.client.cursor()

        if action == "LIST":
            try:
                list_query = (
                    f"SELECT * FROM TASKS WHERE upper(bot_id) = upper(?)"
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
                        "next_check_ts": task[5].strftime("%Y-%m-%d %H:%M:?"),
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

        # Convert timestamp from string in format 'YYYY-MM-DD HH:MM:SS' to a Snowflake-compatible timestamp
        if task_details is not None and task_details.get("task_active", False):
            try:
                formatted_next_check_ts = datetime.strptime(
                    task_details["next_check_ts"], "%Y-%m-%d %H:%M:?"
                )
            except ValueError as ve:
                return {
                    "Success": False,
                    "Error": f"Invalid timestamp format for 'next_check_ts'. Required format: 'YYYY-MM-DD HH:MM:SS' in system timezone. Error details: {ve}",
                    "Info": f"Current system time in system timezone is {datetime.now().strftime('%Y-%m-%d %H:%M:?')}. The system timezone is {datetime.now().strftime('%Z')}. Please note that the timezone should not be included in the submitted timestamp.",
                }
            if formatted_next_check_ts < datetime.now():
                return {
                    "Success": False,
                    "Error": "The 'next_check_ts' is in the past.",
                    "Info": f"Current system time is {datetime.now().strftime('%Y-%m-%d %H:%M:? %Z')}",
                }

        try:
            if action == "CREATE":
                insert_query = f"""
                    INSERT INTO TASKS (
                        task_id, bot_id, task_name, primary_report_to_type, primary_report_to_id,
                        next_check_ts, action_trigger_type, action_trigger_details, task_instructions,
                        reporting_instructions, last_task_status, task_learnings, task_active
                    ) VALUES (
                        %(task_id)s, %(bot_id)s, %(task_name)s, %(primary_report_to_type)s, %(primary_report_to_id)s,
                        %(next_check_ts)s, %(action_trigger_type)s, %(action_trigger_details)s, %(task_instructions)s,
                        %(reporting_instructions)s, %(last_task_status)s, %(task_learnings)s, %(task_active)s
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
                    DELETE FROM TASKS
                    WHERE task_id = ? AND bot_id = ?
                """
                cursor.execute(delete_query, (task_id, bot_id))
                self.client.commit()

            elif action == "UPDATE":
                update_query = f"""
                    UPDATE TASKS
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
            INSERT INTO TASK_HISTORY (
                task_id, work_done_summary, task_status, updated_task_learnings,
                report_message, done_flag, needs_help_flag, task_clarity_comments
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?
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
            logger.info(f"Task history row inserted successfully for task_id: {task_id}")
        except Exception as e:
            logger.info(f"An error occurred while inserting the task history row: {e}")
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
            INSERT INTO LLM_RESULTS (uu, message, created)
            VALUES (?, ?, CURRENT_TIMESTAMP)
        """
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
        Inserts a row into the LLM_RESULTS table.

        Args:
            uu (str): The unique identifier for the result.
            message (str): The message to store.
        """
        update_query = f"""
            UPDATE LLM_RESULTS
            SET message = ?
            WHERE uu = ?
        """
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
            FROM LLM_RESULTS
            WHERE uu = ?
        """
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
            DELETE FROM LLM_RESULTS
            WHERE CURRENT_TIMESTAMP - created > INTERVAL '10 MINUTES'
        """
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

    def make_date_tz_aware(self, date, tz='UTC'):
        """
        Makes a date object timezone-aware.

        Args:
        date (datetime): The date to make timezone-aware.
        tz (str): The timezone to use.

        Returns:
            datetime: The date string with timezone information.
        """
        if type(date) is not str and date is not None and not pd.isna(date):
            # Ensure row['CREATED_AT'] is timezone-aware
            if date.tzinfo is None:
                date = date.tz_localize(pytz.timezone(tz))
            else:
                date = date.astimezone(pytz.timezone(tz))
            date_str = date.strftime('%Y-%m-%d %H:%M:%S')
        else:
            date_str = None

        return date_str

    def load_default_processes_and_notebook(self, cursor):
        folder_path = 'golden_defaults/golden_processes'
        self.process_data = pd.DataFrame()

        files = glob.glob(os.path.join(folder_path, '*.yaml'))

        for filename in files:
            with open(filename, 'r') as file:
                yaml_data = yaml.safe_load(file)

                data = pd.DataFrame.from_dict(yaml_data, orient='index')
                data.reset_index(inplace=True)
                data.rename(columns={'index': 'PROCESS_ID'}, inplace=True)

                self.process_defaults = pd.concat([self.process_data, data], ignore_index=True)

            # Ensure TIMESTAMP column is timezone-aware
            self.process_defaults['TIMESTAMP'] = pd.to_datetime(self.process_defaults['TIMESTAMP'], format='ISO8601', utc=True)

            updated_process = False

            for _, process_default in self.process_defaults.iterrows():
                process_id = process_default['PROCESS_ID']

                timestamp_str = self.make_date_tz_aware(process_default['TIMESTAMP'])

                query = f"SELECT * FROM PROCESSES WHERE PROCESS_ID = ?"
                cursor.execute(query, (process_id,))
                result = cursor.fetchone()
                process_columns = [desc[0] for desc in cursor.description]

                updated_process = False
                process_found = False
                if result is not None:
                    process_found = True
                    db_timestamp = result[process_columns.index('UPDATED_AT')] if len(result) > 0 else None

                    # Ensure db_timestamp is timezone-aware
                    if db_timestamp is None or db_timestamp == '':
                        db_timestamp = datetime.now(pytz.UTC)
                    elif db_timestamp.tzinfo is None:
                        db_timestamp = db_timestamp.replace(tzinfo=pytz.UTC)

                    if process_default['PROCESS_ID'] == process_id and db_timestamp < process_default['TIMESTAMP']:
                        # Remove old process
                        query = f"DELETE FROM PROCESSES WHERE PROCESS_ID = ?"
                        cursor.execute(query, (process_id,))
                        updated_process = True
                    elif result[process_columns.index('PROCESS_ID')] == process_id:
                        continue

                if process_found == False or (process_found == True and updated_process == True):
                    placeholders = ', '.join(['?'] * len(process_columns))

                    insert_values = []
                    for key in process_columns:
                        if key.lower() == 'process_id':
                            insert_values.append(process_id)
                        elif key.lower() == 'timestamp' or key.lower() == 'updated_at' or key.lower() == 'created_at':
                            insert_values.append(timestamp_str)
                        elif key.lower() == 'process_instructions':
                            insert_values.append(process_default['PROCESS_INSTRUCTIONS'])

                            # Check to see if the process_instructions are already in a note in the NOTEBOOK table
                            check_exist_query = f"SELECT * FROM NOTEBOOK WHERE bot_id = ? AND note_content = ?"
                            cursor.execute(check_exist_query, (process_default['BOT_ID'], process_default['PROCESS_INSTRUCTIONS']))
                            result = cursor.fetchone()

                            if False and result is None:
                                # Use this code to insert the process_instructions into the NOTEBOOK table
                                characters = string.ascii_letters + string.digits
                                process_default['NOTE_ID'] = process_default['BOT_ID'] + '_' + ''.join(random.choice(characters) for i in range(10))
                                note_type = 'process'
                                insert_query = f"""
                                    INSERT INTO NOTEBOOK (bot_id, note_content, note_type, note_id)
                                    VALUES (?, ?, ?, ?)
                                """
                                cursor.execute(insert_query, (process_default['BOT_ID'], process_default['PROCESS_INSTRUCTIONS'], note_type, process_default['NOTE_ID']))
                                self.client.commit()

                                insert_values.append(process_default['NOTE_ID'])
                                logger.info(f"Note_id {process_default['NOTE_ID']} inserted successfully for process {process_id}")
                        elif key.lower() == 'hidden':
                            insert_values.append(False)
                        else:
                            val = process_default.get(key, '') if process_default.get(key, '') is not None else ''
                            if pd.isna(val):
                                val = ''
                            insert_values.append(val)

                    insert_query = f"INSERT INTO PROCESSES ({', '.join(process_columns)}) VALUES ({placeholders})"
                    cursor.execute(insert_query, insert_values)
                    if updated_process:
                        logger.info(f"Process {process_id} updated successfully.")
                        updated_process = False
                    else:
                        logger.info(f"Process {process_id} inserted successfully.")
                else:
                    logger.info(f"Process {process_id} already in PROCESSES and it is up to date.")
            cursor.close()

    def ensure_table_exists(self):
        import genesis_bots.core.bot_os_tool_descriptions

        llm_results_table_check_query = "SELECT name FROM sqlite_master WHERE type='table' and name like 'LLM_RESULTS'"
        try:
            cursor = self.client.cursor()
            cursor.execute(llm_results_table_check_query)
            if not cursor.fetchone():
                create_llm_results_table_ddl = f"""
                    CREATE TABLE LLM_RESULTS (
                        uu VARCHAR(40) PRIMARY KEY,
                        message VARCHAR NOT NULL,
                        created TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                """
                cursor.execute(create_llm_results_table_ddl)
                self.client.commit()
                logger.info(f"Table LLM_RESULTS created successfully.")
            else:
                logger.info(f"Table LLM_RESULTS already exists.")
        except Exception as e:
            logger.info(
                f"An error occurred while checking or creating the LLM_RESULTS table: {e}"
            )
        finally:
            if cursor is not None:
                cursor.close()

        # TODO ADD PROCESSES TABLE

        processes_table_check_query = "SELECT name FROM sqlite_master WHERE type='table' AND name='PROCESSES';"
        cursor = self.client.cursor()
        cursor.execute(processes_table_check_query)
        if not cursor.fetchone():
            create_process_table_ddl = """
            CREATE TABLE PROCESSES (
                PROCESS_ID TEXT NOT NULL PRIMARY KEY,
                BOT_ID TEXT,
                PROCESS_NAME TEXT NOT NULL,
                PROCESS_INSTRUCTIONS TEXT,
                PROCESS_CONFIG TEXT,
                TIMESTAMP TEXT NOT NULL
            );
            """
            cursor.execute(create_process_table_ddl)
            self.client.commit()
            logger.info("Table PROCESSES created successfully.")

            # Assuming load_default_processes is a method that needs to be called
            self.load_default_processes_and_notebook(cursor)
        else:
            logger.info("Table PROCESSES already exists.")

        tasks_table_check_query = "SELECT name FROM sqlite_master WHERE type='table' and name like 'TASKS'"
        try:
            cursor = self.client.cursor()
            cursor.execute(tasks_table_check_query)
            if not cursor.fetchone():
                create_tasks_table_ddl = f"""
                    CREATE TABLE TASKS (
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
                logger.info(f"Table TASKS created successfully.")
            else:
                logger.info(f"Table TASKS already exists.")
        except Exception as e:
            logger.info(f"An error occurred while checking or creating the TASKS table: {e}")
        finally:
            if cursor is not None:
                cursor.close()

        task_history_check_query = "SELECT name FROM sqlite_master WHERE type='table' and name like 'TASK_HISTORY'"
        try:
            cursor = self.client.cursor()
            cursor.execute(task_history_check_query)
            if not cursor.fetchone():
                create_task_history_table_ddl = f"""
                    CREATE TABLE TASK_HISTORY (
                        task_id VARCHAR(255),
                        run_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
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
                logger.info(f"Table TASK_HISTORY created successfully.")
            else:
                logger.info(f"Table TASK_HISTORY already exists.")
        except Exception as e:
            logger.info(
                f"An error occurred while checking or creating the TASK_HISTORY table: {e}"
            )
        finally:
            if cursor is not None:
                cursor.close()

        try:
            os.makedirs(os.path.join(self.stages, 'SEMANTIC_MODELS_DEV'))
            logger.info(f"Stage SEMANTIC_MODELS_DEV created.")
        except Exception as e:
            logger.info(f"Stage SEMANTIC_MODELS_DEV already exists.")

        try:
            os.makedirs(os.path.join(self.stages, 'SEMANTIC_MODELS'))
            logger.info(f"Stage SEMANTIC_MODELS created.")
        except Exception as e:
            logger.info(f"Stage SEMANTIC_MODELS already exists.")

        udf_check_query = f"SHOW USER FUNCTIONS LIKE 'SET_BOT_APP_LEVEL_KEY' IN SCHEMA;"
        try:
            cursor = self.client.cursor()
            cursor.execute(udf_check_query)
            if not cursor.fetchone():
                udf_creation_ddl = f"""
                CREATE OR REPLACE FUNCTION .set_bot_app_level_key (bot_id VARCHAR, slack_app_level_key VARCHAR)
                RETURNS VARCHAR
                SERVICE=GENESISAPP_SERVICE_SERVICE
                ENDPOINT=udfendpoint AS '/udf_proxy/set_bot_app_level_key';
                """
                cursor.execute(udf_creation_ddl)
                self.client.commit()
                logger.info(f"UDF set_bot_app_level_key created in schema ")
            else:
                logger.info(
                    f"UDF set_bot_app_level_key already exists in schema "
                )
        except Exception as e:
            logger.info(f"UDF not created in  {e}.  This is expected in Sqlite mode." )

        try:
            os.makedirs(os.path.join(self.stages, 'BOT_FILES_STAGE'))
            logger.info(f"Stage BOT_FILES_STAGE created.")
        except Exception as e:
            logger.info(f"Stage BOT_FILES_STAGE already exists.")

        llm_config_table_check_query = "SELECT name FROM sqlite_master WHERE type='table' AND name='LLM_TOKENS';"
        try:
            runner_id = os.getenv("RUNNER_ID", "jl-local-runner")
            cursor = self.client.cursor()
            cursor.execute(llm_config_table_check_query)
            if not cursor.fetchone():
                llm_config_table_ddl = """
                CREATE TABLE LLM_TOKENS (
                    RUNNER_ID TEXT,
                    LLM_KEY TEXT,
                    LLM_TYPE TEXT,
                    ACTIVE BOOLEAN,
                    LLM_ENDPOINT TEXT,
                    MODEL_NAME TEXT,
                    EMBEDDING_MODEL_NAME TEXT
                );
                """
                cursor.execute(llm_config_table_ddl)
                self.client.commit()
                logger.info("Table LLM_TOKENS created.")

                insert_initial_row_query = """
                    INSERT INTO LLM_TOKENS (RUNNER_ID, LLM_KEY, LLM_TYPE, ACTIVE, LLM_ENDPOINT)
                    VALUES (?, NULL, NULL, 0, NULL);
                """
                cursor.execute(insert_initial_row_query, (runner_id,))
                self.client.commit()
        except Exception as e:
            logger.info(f"An error occurred while checking or creating table LLM_TOKENS: {e}")

        # Check if LLM_ENDPOINT column exists in LLM_TOKENS table
        # check_llm_endpoint_query = f"DESCRIBE TABLE {self.genbot_internal_project_and_schema}.LLM_TOKENS;"
        # try:
        #     cursor = self.client.cursor()
        #     cursor.execute(check_llm_endpoint_query)
        #     columns = [col[0] for col in cursor.fetchall()]
        #
        #     if "LLM_ENDPOINT" not in columns:
        #         # Add LLM_ENDPOINT column if it doesn't exist
        #         alter_table_query = f"ALTER TABLE {self.genbot_internal_project_and_schema}.LLM_TOKENS ADD COLUMN LLM_ENDPOINT VARCHAR(16777216);"
        #         cursor.execute(alter_table_query)
        #         self.client.commit()
        #         logger.info(
        #             f"Column 'LLM_ENDPOINT' added to table {self.genbot_internal_project_and_schema}.LLM_TOKENS."
        #         )
        # except Exception as e:
        #     logger.error(
        #         f"An error occurred while checking or altering table {self.genbot_internal_project_and_schema}.LLM_TOKENS to add LLM_ENDPOINT column: {e}"
        #     )
        # finally:
        #     if cursor is not None:
        #         cursor.close()

        slack_tokens_table_check_query = "SELECT name FROM sqlite_master WHERE type='table' AND name='SLACK_APP_CONFIG_TOKENS'"
        try:
            cursor = self.client.cursor()
            cursor.execute(slack_tokens_table_check_query)
            if not cursor.fetchone():
                slack_tokens_table_ddl = f"""
                CREATE TABLE {self.slack_tokens_table_name} (
                    RUNNER_ID TEXT,
                    SLACK_APP_CONFIG_TOKEN TEXT,
                    SLACK_APP_CONFIG_REFRESH_TOKEN TEXT
                );
                """
                cursor.execute(slack_tokens_table_ddl)
                self.client.commit()
                logger.info(f"Table {self.slack_tokens_table_name} created.")

                # Insert a row with the current runner_id and NULL values for the tokens
                runner_id = os.getenv("RUNNER_ID", "jl-local-runner")
                insert_initial_row_query = f"""
                    INSERT INTO {self.slack_tokens_table_name} (RUNNER_ID, SLACK_APP_CONFIG_TOKEN, SLACK_APP_CONFIG_REFRESH_TOKEN)
                    VALUES (?, NULL, NULL);
                """
                cursor.execute(insert_initial_row_query, (runner_id,))
                self.client.commit()
                logger.info(
                    f"Inserted initial row into {self.slack_tokens_table_name} with runner_id: {runner_id}"
                )
            else:
                logger.info(f"Table {self.slack_tokens_table_name} already exists.")
        except Exception as e:
            logger.info(
                f"An error occurred while checking or creating table {self.slack_tokens_table_name}: {e}"
            )

        bot_servicing_table_check_query = "SELECT name FROM sqlite_master WHERE type='table' and name like 'BOT_SERVICING'"
        try:
            cursor = self.client.cursor()
            cursor.execute(bot_servicing_table_check_query)
            if not cursor.fetchone():
                bot_servicing_table_ddl = f"""
                CREATE TABLE {self.bot_servicing_table_name} (
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
                logger.info(f"Table {self.bot_servicing_table_name} created.")

                # Insert a row with specified values and NULL for the rest
                runner_id = os.getenv("RUNNER_ID", "jl-local-runner")
                bot_id = "Eve-"
                bot_id += "".join(
                    random.choices(string.ascii_letters + string.digits, k=6)
                )
                bot_name = "Eve"
                bot_instructions = BASE_EVE_BOT_INSTRUCTIONS
                available_tools = '["slack_tools", "make_baby_bot", "snowflake_tools", "data_connector_tools", "image_tools"]'
                udf_active = "Y"
                slack_active = "N"
                bot_intro_prompt = EVE_INTRO_PROMPT

                insert_initial_row_query = f"""
                    INSERT INTO {self.bot_servicing_table_name} (
                        RUNNER_ID, BOT_ID, BOT_NAME, BOT_INSTRUCTIONS, AVAILABLE_TOOLS, UDF_ACTIVE, SLACK_ACTIVE, BOT_INTRO_PROMPT
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?);
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
                logger.info(
                    f"Inserted initial Eve row into {self.bot_servicing_table_name} with runner_id: {runner_id}"
                )

                runner_id = os.getenv("RUNNER_ID", "jl-local-runner")
                bot_id = "Eliza-"
                bot_id += "".join(
                    random.choices(string.ascii_letters + string.digits, k=6)
                )
                bot_name = "Eliza"
                bot_instructions = ELIZA_DATA_ANALYST_INSTRUCTIONS
                available_tools = '["slack_tools", "data_connector_tools", "snowflake_tools", "image_tools"]'
                udf_active = "Y"
                slack_active = "N"
                bot_intro_prompt = ELIZA_INTRO_PROMPT

                insert_initial_row_query = f"""
                INSERT INTO {self.bot_servicing_table_name} (
                    RUNNER_ID, BOT_ID, BOT_NAME, BOT_INSTRUCTIONS, AVAILABLE_TOOLS, UDF_ACTIVE, SLACK_ACTIVE, BOT_INTRO_PROMPT
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?);
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
                logger.info(
                    f"Inserted initial Eliza row into {self.bot_servicing_table_name} with runner_id: {runner_id}"
                )

            #          runner_id = os.getenv('RUNNER_ID', 'jl-local-runner')
            #          bot_id = 'Stuart-'
            #          bot_id += ''.join(random.choices(string.ascii_letters + string.digits, k=6))
            #          bot_name = "Stuart"
            #          bot_instructions = STUART_DATA_STEWARD_INSTRUCTIONS
            #          available_tools = '["slack_tools", "data_connector_tools", "snowflake_tools", "snowflake_semantic_tools", "image_tools", "autonomous_tools"]'
            #          udf_active = "Y"
            #          slack_active = "N"
            #          bot_intro_prompt = STUART_INTRO_PROMPT

            #          insert_initial_row_query = f"""
            #         INSERT INTO {self.bot_servicing_table_name} (
            #              RUNNER_ID, BOT_ID, BOT_NAME, BOT_INSTRUCTIONS, AVAILABLE_TOOLS, UDF_ACTIVE, SLACK_ACTIVE, BOT_INTRO_PROMPT
            #          )
            #          VALUES (?, ?, ?, ?, ?, ?, ?, ?);
            #          """
            #          cursor.execute(insert_initial_row_query, (runner_id, bot_id, bot_name, bot_instructions, available_tools, udf_active, slack_active, bot_intro_prompt))
            #          self.client.commit()
            #          logger.info(f"Inserted initial Stuart row into {self.bot_servicing_table_name} with runner_id: {runner_id}")

            else:
                # Check if the 'ddl_short' column exists in the metadata table

                # update_query = f"""
                #     UPDATE {self.bot_servicing_table_name}
                #     SET AVAILABLE_TOOLS = REPLACE(REPLACE(AVAILABLE_TOOLS, 'vision_chat_analysis', 'image_tools'),)
                #     WHERE AVAILABLE_TOOLS LIKE '%vision_chat_analysis%'
                #     """
                # cursor.execute(update_query)
                # self.client.commit()
                # logger.info(
                #     f"Updated 'vision_chat_analysis' to 'image_analysis' in AVAILABLE_TOOLS where applicable in {self.bot_servicing_table_name}."
                # )

                check_query = f"PRAGMA table_info([{self.bot_servicing_table_name}]);"
                try:
                    cursor.execute(check_query)
                    columns = [col[1] for col in cursor.fetchall()]
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
                    logger.info(
                        f"An error occurred while checking or altering table {self.bot_servicing_table_name} to add BOT_IMPLEMENTATION column: {e}"
                    )
                # except Exception as e:
                #     logger.info(
                #         f"An error occurred while checking or altering table {metadata_table_id}: {e}"
                #     )
                logger.info(f"Table {self.bot_servicing_table_name} already exists.")
            # # update bot servicing table bot avatars from shared images table
            # insert_images_query = f"""
            #     UPDATE  {self.bot_servicing_table_name}
            #     SET BOT_AVATAR_IMAGE = (
            #         SELECT ENCODED_IMAGE_DATA
            #         FROM (
            #             SELECT S.ENCODED_IMAGE_DATA, R.BOT_NAME
            #             FROM {self.images_table_name} S,  {self.bot_servicing_table_name} R
            #             WHERE UPPER(S.BOT_NAME) = UPPER(R.BOT_NAME)
            #             UNION
            #             SELECT P.ENCODED_IMAGE_DATA, Q.BOT_NAME
            #             FROM {self.images_table_name} P,  {self.bot_servicing_table_name} Q
            #             WHERE UPPER(P.BOT_NAME) = 'DEFAULT'
            #             AND Q.BOT_NAME NOT IN (SELECT BOT_NAME FROM {self.images_table_name})
            #         ) AS a
            #         WHERE UPPER(a.BOT_NAME) = UPPER(BOT_SERVICING.BOT_NAME)
            #     );
            # """
            # cursor.execute(insert_images_query)
            # self.client.commit()
            # logger.info(
            #     f"Initial 'BOT_AVATAR_IMAGE' data inserted into table {self.bot_servicing_table_name}."
            # )
        except Exception as e:
            logger.info(
                f"An error occurred while checking or creating table {self.bot_servicing_table_name}: {e}"
            )
        finally:
            if cursor is not None:
                cursor.close()

        ngrok_tokens_table_check_query = "SELECT name FROM sqlite_master WHERE type='table' and name like 'NGROK_TOKENS'"
        try:
            cursor = self.client.cursor()
            cursor.execute(ngrok_tokens_table_check_query)
            if not cursor.fetchone():
                ngrok_tokens_table_ddl = f"""
                CREATE TABLE {self.ngrok_tokens_table_name} (
                    RUNNER_ID VARCHAR(16777216),
                    NGROK_AUTH_TOKEN VARCHAR(16777216),
                    NGROK_USE_DOMAIN VARCHAR(16777216),
                    NGROK_DOMAIN VARCHAR(16777216)
                );
                """
                cursor.execute(ngrok_tokens_table_ddl)
                self.client.commit()
                logger.info(f"Table {self.ngrok_tokens_table_name} created.")

                # Insert a row with the current runner_id and NULL values for the tokens and domain, 'N' for use_domain
                runner_id = os.getenv("RUNNER_ID", "jl-local-runner")
                insert_initial_row_query = f"""
                INSERT INTO {self.ngrok_tokens_table_name} (RUNNER_ID, NGROK_AUTH_TOKEN, NGROK_USE_DOMAIN, NGROK_DOMAIN)
                VALUES (?, NULL, 'N', NULL);
                """
                cursor.execute(insert_initial_row_query, (runner_id,))
                self.client.commit()
                logger.info(
                    f"Inserted initial row into {self.ngrok_tokens_table_name} with runner_id: {runner_id}"
                )
            else:
                logger.info(f"Table {self.ngrok_tokens_table_name} already exists.")
        except Exception as e:
            logger.info(
                f"An error occurred while checking or creating table {self.ngrok_tokens_table_name}: {e}"
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
                logger.info("Inserted 'snowflake_semantic_tools' into available_tools table.")
        except Exception as e:
            logger.info(
                f"An error occurred while inserting 'snowflake_semantic_tools' into available_tools table: {e}"
            )
        finally:
            if cursor is not None:
                cursor.close()

        # CHAT HISTORY TABLE
        chat_history_table_id = self.message_log_table_name
        chat_history_table_check_query = "SELECT name FROM sqlite_master WHERE type='table' and name like 'MESSAGE_LOG'"

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
                logger.info(f"Table {self.message_log_table_name} created.")
            else:
                check_query = f"PRAGMA table_info([{chat_history_table_id}]);"
                try:
                    cursor.execute(check_query)
                    columns = [col[1].upper() for col in cursor.fetchall()]
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
                    logger.info("Error adding column FILES to MESSAGE_LOG: ", e)
                logger.info(f"Table {self.message_log_table_name} already exists.")
        except Exception as e:
            logger.info(
                f"An error occurred while checking or creating table {self.message_log_table_name}: {e}"
            )

        # KNOWLEDGE TABLE
        knowledge_table_check_query = "SELECT name FROM sqlite_master WHERE type='table' and name like 'KNOWLEDGE'"
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
                logger.info(f"Table {self.knowledge_table_name} created.")
            else:
                check_query = f"DESCRIBE TABLE {self.knowledge_table_name};"
                logger.info(f"Table {self.knowledge_table_name} already exists.")
        except Exception as e:
            logger.info(
                f"An error occurred while checking or creating table {self.knowledge_table_name}: {e}"
            )

        # KNOWLEDGE TABLE
        user_bot_table_check_query = "SELECT name FROM sqlite_master WHERE type='table' and name like 'USER_BOT'"
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
                logger.info(f"Table {self.user_bot_table_name} created.")
            else:
                check_query = f"DESCRIBE TABLE {self.user_bot_table_name};"
                logger.info(f"Table {self.user_bot_table_name} already exists.")
        except Exception as e:
            logger.info(
                f"An error occurred while checking or creating table {self.user_bot_table_name}: {e}"
            )

        # HARVEST CONTROL TABLE
        hc_table_id = self.harvest_control_table_name
        hc_table_check_query = f"SELECT name FROM sqlite_master WHERE type='table' and name like '{hc_table_id.upper()}'"

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
                logger.info(f"Table {hc_table_id} created.")
            else:
                logger.info(f"Table {hc_table_id} already exists.")
        except Exception as e:
            logger.info(
                f"An error occurred while checking or creating table {hc_table_id}: {e}"
            )

        # METADATA TABLE FOR HARVESTER RESULTS
        metadata_table_id = self.metadata_table_name
        metadata_table_check_query = f"SELECT name FROM sqlite_master WHERE type='table' and name like '{metadata_table_id.upper()}'"

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
                    embedding ARRAY
                );
                """
                cursor.execute(metadata_table_ddl)
                self.client.commit()
                logger.info(f"Table {metadata_table_id} created.")

                try:
                    insert_initial_metadata_query = f"""
                    INSERT INTO {metadata_table_id} (SOURCE_NAME, QUALIFIED_TABLE_NAME, DATABASE_NAME, MEMORY_UUID, SCHEMA_NAME, TABLE_NAME, COMPLETE_DESCRIPTION, DDL, DDL_SHORT, DDL_HASH, SUMMARY, SAMPLE_DATA_TEXT, LAST_CRAWLED_TIMESTAMP, CRAWL_STATUS, ROLE_USED_FOR_CRAWL, EMBEDDING)
                    SELECT SOURCE_NAME, replace(QUALIFIED_TABLE_NAME,'APP_NAME', CURRENT_DATABASE()) QUALIFIED_TABLE_NAME,  CURRENT_DATABASE() DATABASE_NAME, MEMORY_UUID, SCHEMA_NAME, TABLE_NAME, REPLACE(COMPLETE_DESCRIPTION,'APP_NAME', CURRENT_DATABASE()) COMPLETE_DESCRIPTION, REPLACE(DDL,'APP_NAME', CURRENT_DATABASE()) DDL, REPLACE(DDL_SHORT,'APP_NAME', CURRENT_DATABASE()) DDL_SHORT, 'SHARED_VIEW' DDL_HASH, REPLACE(SUMMARY,'APP_NAME', CURRENT_DATABASE()) SUMMARY, SAMPLE_DATA_TEXT, LAST_CRAWLED_TIMESTAMP, CRAWL_STATUS, ROLE_USED_FOR_CRAWL, EMBEDDING
                    FROM HARVEST_RESULTS WHERE SCHEMA_NAME IN ('BASEBALL','FORMULA_1') AND DATABASE_NAME = 'APP_NAME'
                    """
                    cursor.execute(insert_initial_metadata_query)
                    self.client.commit()
                    logger.info(f"Inserted initial rows into {metadata_table_id}")
                except Exception as e:
                    logger.info(
                        f"Initial rows from APP_SHARE.HARVEST_RESULTS NOT ADDED into {metadata_table_id} due to erorr {e}"
                    )

            else:
                # Check if the 'ddl_short' column exists in the metadata table
                ddl_short_check_query = f"PRAGMA table_info([{self.metadata_table_name}]);"
                try:
                    cursor.execute(ddl_short_check_query)
                    columns = [col[1].upper() for col in cursor.fetchall()]
                    if "DDL_SHORT" not in columns:
                        alter_table_query = f"ALTER TABLE {self.metadata_table_name} ADD COLUMN ddl_short STRING;"
                        cursor.execute(alter_table_query)
                        self.client.commit()
                        logger.info(f"Column 'ddl_short' added to table {metadata_table_id}.")
                except Exception as e:
                    logger.info(
                        f"An error occurred while checking or altering table {metadata_table_id}: {e}"
                    )
                logger.info(f"Table {metadata_table_id} already exists.")
        except Exception as e:
            logger.info(
                f"An error occurred while checking or creating table {metadata_table_id}: {e}"
            )

        cursor = self.client.cursor()

        cortex_threads_input_table_check_query = f"SELECT name FROM sqlite_master WHERE type='table' and name like 'CORTEX_THREADS_INPUT'"
        try:
            cursor.execute(cortex_threads_input_table_check_query)
            if not cursor.fetchone():
                cortex_threads_input_table_ddl = f"""
                CREATE TABLE CORTEX_THREADS_INPUT (
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
                logger.info(f"Table CORTEX_THREADS_INPUT created.")
            else:
                logger.info(f"Table CORTEX_THREADS_INPUT already exists.")
        except Exception as e:
            logger.info(
                f"An error occurred while checking or creating table CORTEX_THREADS_INPUT: {e}"
            )

        cortex_threads_output_table_check_query = f"SELECT name FROM sqlite_master WHERE type='table' and name like 'CORTEX_THREADS_OUTPUT'"
        try:
            cursor.execute(cortex_threads_output_table_check_query)
            if not cursor.fetchone():
                cortex_threads_output_table_ddl = f"""
                CREATE TABLE CORTEX_THREADS_OUTPUT (
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
                logger.info(f"Table CORTEX_THREADS_OUTPUT created.")
            else:
                logger.info(f"Table CORTEX_THREADS_OUTPUT already exists.")
        except Exception as e:
            logger.info(
                f"An error occurred while checking or creating table CORTEX_THREADS_OUTPUT: {e}"
            )

        baseball_table_check_query = f"SELECT name FROM sqlite_master WHERE type='table' and name like 'all_star'"
        try:
            cursor.execute(baseball_table_check_query)
            if not cursor.fetchone():
                with open('spider_load/database/baseball_1_new/schema.sql', 'r') as f:
                    baseball_table_query = f.read()
                cursor.executescript(baseball_table_query)
                self.client.commit()
            else:
                logger.info(f"Baseball tables already exist.")
        except Exception as e:
            logger.info(f"An error occurred while creating baseball tables: {e}")

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
        last_crawled_timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat(" ")
        if not ddl_hash:
            ddl_hash = self.sha256_hash_hex_string(ddl)

        # Assuming role_used_for_crawl is stored in self.connection_info["client_email"]
        role_used_for_crawl = self.role

        # if cortex mode, load embedding_native else load embedding column
        if os.environ.get("CORTEX_MODE", 'False') == 'True':
            embedding_target = 'embedding_native'
        else:
            embedding_target = 'embedding'

        try:
            query = f"""SELECT COUNT(*) AS CNT  FROM  {self.metadata_table_name}
                        WHERE qualified_table_name = '{qualified_table_name}';"""
            if_exist = self.run_query(query)
            if if_exist[0]['CNT']: # udpate
                query = f"""DROP FROM {self.metadata_table_name}
                        WHERE qualified_table_name = '{qualified_table_name}';"""
            self.run_insert(self.metadata_table_name,
                source_name=self.source_name, qualified_table_name=qualified_table_name, memory_uuid=memory_uuid,
                database_name=database_name, schema_name=schema_name, table_name=table_name,
                complete_description=complete_description, ddl=ddl, ddl_short=ddl_short, ddl_hash=ddl_hash,
                summary=summary, sample_data_text=sample_data_text, last_crawled_timestamp=last_crawled_timestamp,
                crawl_status=crawl_status, role_used_for_crawl=role_used_for_crawl, **{embedding_target: json.dumps(embedding)})

        except Exception as e:
            logger.info(f"An error occurred while executing the MERGE statement: {e}")

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
                query = f"SELECT IIF(count(*)>0,TRUE,FALSE) from HARVEST_RESULTS where DATABASE_NAME = '{database_name}' AND SCHEMA_NAME = '{schema_name}' AND TABLE_NAME = '{table_name}';"
                cursor = self.client.cursor()
                cursor.execute(query)
                result = cursor.fetchone()
                return result[0]
            else:
                return "a required parameter was not entered"
        except Exception as e:
            if os.environ.get('GENESIS_LOCAL_RUNNER', '').upper() != 'TRUE':
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

            query = f"""SELECT SOURCE_NAME, replace(QUALIFIED_TABLE_NAME,'PLACEHOLDER_DB_NAME','{database_name}') QUALIFIED_TABLE_NAME, '{database_name}' DATABASE_NAME, MEMORY_UUID, SCHEMA_NAME, TABLE_NAME, REPLACE(COMPLETE_DESCRIPTION,'PLACEHOLDER_DB_NAME','{database_name}') COMPLETE_DESCRIPTION, REPLACE(DDL,'PLACEHOLDER_DB_NAME','{database_name}') DDL, REPLACE(DDL_SHORT,'PLACEHOLDER_DB_NAME','{database_name}') DDL_SHORT, 'SHARED_VIEW' DDL_HASH, REPLACE(SUMMARY,'PLACEHOLDER_DB_NAME','{database_name}') SUMMARY, SAMPLE_DATA_TEXT, LAST_CRAWLED_TIMESTAMP, CRAWL_STATUS, ROLE_USED_FOR_CRAWL, EMBEDDING
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

        #  logger.warn('Creating connection..')
        SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT", None)
        SNOWFLAKE_HOST = os.getenv("SNOWFLAKE_HOST", None)
        logger.info(
            "Checking possible SPCS ENV vars -- Account, Host: ?, ?",
            SNOWFLAKE_ACCOUNT,
            SNOWFLAKE_HOST,
        )

        logger.info("SNOWFLAKE_HOST: ?", os.getenv("SNOWFLAKE_HOST"))
        logger.info("SNOWFLAKE_ACCOUNT: ?", os.getenv("SNOWFLAKE_ACCOUNT"))
        logger.info("SNOWFLAKE_PORT: ?", os.getenv("SNOWFLAKE_PORT"))
        #  logger.warn('SNOWFLAKE_WAREHOUSE: ?', os.getenv('SNOWFLAKE_WAREHOUSE'))
        logger.info("SNOWFLAKE_DATABASE: ?", os.getenv("SNOWFLAKE_DATABASE"))
        logger.info("SNOWFLAKE_SCHEMA: ?", os.getenv("SNOWFLAKE_SCHEMA"))

        if (
            SNOWFLAKE_ACCOUNT
            and SNOWFLAKE_HOST
            and os.getenv("SNOWFLAKE_PASSWORD_OVERRIDE", None) == None
        ):
            with open("/snowflake/session/token", "r") as f:
                snowflake_token = f.read()
            logger.info("SPCS Snowflake token found, length: %d", len(snowflake_token))
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

        logger.info("Creating Snowflake regular connection...")
        self.token_connection = False

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
        query = (
            "SELECT source_name, database_name, schema_inclusions, schema_exclusions, status, refresh_interval, initial_crawl_complete FROM "
            + self.harvest_control_table_name
        )
        cursor = self.client.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        columns = [col[0].lower() for col in cursor.description]
        databases = [dict(zip(columns, row)) for row in results]
        cursor.close()

        return databases

    def get_visible_databases(self, thread_id=None):
        schemas = []
        query = "SELECT * FROM pragma_database_list;"
        cursor = self.client.cursor()
        cursor.execute(query)
        for row in cursor:
            schemas.append(row[1])  # Assuming the schema name is in the second column
        cursor.close()
        return schemas

    def get_schemas(self, database, thread_id=None):
        schemas = []
        query = "SELECT name FROM sqlite_master WHERE type='table';"
        cursor = self.client.cursor()
        cursor.execute(query)
        for row in cursor:
            schemas.append(row[0])  # Assuming the schema name is in the second column
        cursor.close()
        return schemas

    def get_tables(self, database, schema, thread_id=None):
        return [{'table_name': schema}]

    def get_columns(self, database, schema, table):
        columns = []
        query = f'PRAGMA table_info({table});'
        cursor = self.client.cursor()
        cursor.execute(query)
        for row in cursor:
            columns.append(row[1])  # Assuming the column name is in the first column
        cursor.close()
        return columns

    def get_sample_data(self, database, schema_name: str, table_name: str):
        """
        Fetches 10 rows of sample data from a specific table in Snowflake.

        :param database: The name of the database.
        :param schema_name: The name of the schema.
        :param table_name: The name of the table.
        :return: A list of dictionaries representing rows of sample data.
        """
        query = f'SELECT * FROM {table_name} LIMIT 10'
        cursor = self.client.cursor()
        cursor.execute(query)
        columns = [col[0] for col in cursor.description]
        sample_data = [dict(zip(columns, row)) for row in cursor]
        cursor.close()
        return sample_data

    def alt_get_ddl(self, table_name):
        table_name = table_name.split('.')[-1]
        describe_query = f"PRAGMA table_info({table_name});"
        try:
            describe_result = self.run_query(query=describe_query, max_rows=1000, max_rows_override=True)
        except:
            return None

        ddl_statement = "CREATE TABLE " + table_name + " (\n"
        for column in describe_result:
            column_name = column['NAME']
            column_type = column['TYPE']
            nullable = " NOT NULL" if not column['NOTNULL'] else ""
            default = f" DEFAULT {column['DFLT_VALUE']}" if column['DFLT_VALUE'] is not None else ""
            comment = ''
            key = ""
            if column.get('primary_key', False):
                key = " PRIMARY KEY"
            elif column.get('unique_key', False):
                key = " UNIQUE"
            ddl_statement += f"    {column_name} {column_type}{nullable}{default}{key}{comment},\n"
        ddl_statement = ddl_statement.rstrip(',\n') + "\n);"
        # logger.info(ddl_statement)
        return ddl_statement

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
        max_rows=20,
        max_rows_override=False,
        job_config=None,
        connection=None,
        export_to_google_sheet = False,
        bot_id=None,
    ):
        from genesis_bots.core import global_flags
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
        if query.startswith("USERQUERY::"):
            userquery = True
            query = query[len("USERQUERY::"):]

        if userquery and not query.endswith(';'):
            return {
             "success": False,
             "Error:": "Error! Query must end with a semicolon.  Add a ; to the end and RUN THIS TOOL AGAIN NOW! Also replace all ' (single quotes) with 🏑"
            }

        if isinstance(max_rows, str):
            try:
                max_rows = int(max_rows)
            except ValueError:
                raise ValueError("max_rows should be an integer or a string that can be converted to an integer.")

        if job_config is not None:
            raise Exception("Job configuration is not supported in this method.")

        if max_rows > 100 and not max_rows_override:
            max_rows = 100

        #   logger.info('running query ... ', query)
        cursor = self.client.cursor()
        try:
            #   if query_params:
            #       cursor.execute(query, query_params)
            #   else:
            cursor.execute(query)

            workspace_schema_name = f"{global_flags.project_id}.{bot_id.replace(r'[^a-zA-Z0-9]', '_').replace('-', '_').replace('.', '_')}_WORKSPACE".upper()
            # call grant_all_bot_workspace()
            if bot_id is not None and (
                "CREATE" in query.upper()
                and workspace_schema_name.upper() in query.upper()
            ):
                self.grant_all_bot_workspace(workspace_schema_name)

        except Exception as e:
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
            logger.info("run query: len=", len(query), "\ncaused error: ", e)
            cursor.close()
            return {"Success": False, "Error": str(e)}

        #    logger.info('getting results:')
        try:
            results = cursor.fetchmany(max_rows)
            columns = [col[0].upper() for col in cursor.description]
            sample_data = [dict(zip(columns, row)) for row in results]

            # Replace occurrences of triple backticks with triple single quotes in sample data
            sample_data = [
                {key: (value.replace("```", r"\`\`\`") if isinstance(value, str) else value) for key, value in row.items()}
                for row in sample_data
            ]
        except Exception as e:
            logger.info("run query: ", query, "\ncaused error: ", e)
            cursor.close()
            raise e

        # logger.info('returning result: ', sample_data)
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
        with_instructions=False
    ):
        """
        Returns a list of all the bots being served by the system, including their runner IDs, names, instructions, tools, etc.

        Returns:
            list: A list of dictionaries, each containing details of a bot.
        """
        # Get the database schema from environment variables
        if isinstance(with_instructions, str):
            with_instructions = with_instructions.lower() == 'true'

        if full:
            select_str = "api_app_id, bot_slack_user_id, bot_id, bot_name, bot_instructions, runner_id, slack_app_token, slack_app_level_key, slack_signing_secret, slack_channel_id, available_tools, udf_active, slack_active, files, bot_implementation, bot_intro_prompt, bot_avatar_image, slack_user_allow"
        else:
            if slack_details:
                select_str = "runner_id, bot_id, bot_name, bot_instructions, available_tools, bot_slack_user_id, api_app_id, auth_url, udf_active, slack_active, files, bot_implementation, bot_intro_prompt, slack_user_allow"
            else:
                select_str = "runner_id, bot_id, bot_name, bot_instructions, available_tools, bot_slack_user_id, api_app_id, auth_url, udf_active, slack_active, files, bot_implementation, bot_intro_prompt"
        if not with_instructions and not full:
            select_str = select_str.replace("bot_instructions, ", "")

        # Query to select all bots from the BOT_SERVICING table
        if runner_id is None:
            select_query = f"""
            SELECT {select_str}
            FROM {bot_servicing_table}
            """
        else:
            select_query = f"""
            SELECT {select_str}
            FROM {bot_servicing_table}
            WHERE runner_id = '{runner_id}'
            """

        try:
            # Execute the query and fetch all bot records
            cursor = self.client.cursor()
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
            MERGE INTO slack_app_config_tokens USING (
                SELECT ? AS runner_id
            ) AS src
            ON src.runner_id = slack_app_config_tokens.runner_id
            WHEN MATCHED THEN
                UPDATE SET slack_app_config_token = ?, slack_app_config_refresh_token = ?
            WHEN NOT MATCHED THEN
                INSERT (runner_id, slack_app_config_token, slack_app_config_refresh_token)
                VALUES (src.runner_id, ?, ?)
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
            FROM slack_app_config_tokens
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
            FROM ngrok_tokens
            WHERE runner_id = ?
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
            MERGE INTO ngrok_tokens USING (SELECT 1 AS one) ON (runner_id = ?)
            WHEN MATCHED THEN
                UPDATE SET ngrok_auth_token = ?,
                           ngrok_use_domain = ?,
                           ngrok_domain = ?
            WHEN NOT MATCHED THEN
                INSERT (runner_id, ngrok_auth_token, ngrok_use_domain, ngrok_domain)
                VALUES (?, ?, ?, ?)
        """

        try:
            cursor = self.client.cursor()
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
            self.client.commit()
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
        Retrieves the LLM key and type for the given runner_id from BigQuery.

        Returns:
            list: A list of tuples, each containing an LLM key and LLM type.
        """
        runner_id = os.getenv("RUNNER_ID", "jl-local-runner")
        logger.info("in getllmkey")
        # Query to select the LLM key and type from the llm_tokens table
        query = f"""
            SELECT llm_key, llm_type
            FROM llm_tokens
            WHERE runner_id = ?
        """
        logger.info(f"query: {query}")
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, (runner_id,))
                result = cursor.fetchone()

            if result:
                return llm_keys_and_types_struct(llm_type=result[1], llm_key=result[0], llm_endpoint=result[2])
            else:
                logger.info("No LLM tokens found for runner_id: %s", runner_id)
                return llm_keys_and_types_struct()
        except Exception as e:
            logger.error("Error retrieving LLM tokens: %s", str(e))
            return llm_keys_and_types_struct()

    def db_get_active_llm_key(self) -> llm_keys_and_types_struct:
        """
        Retrieves the active LLM key and type for the given runner_id.

        Returns:
            list: A list of tuples, each containing an LLM key and LLM type.
        """
        runner_id = os.getenv("RUNNER_ID", "jl-local-runner")
        logger.info("in getllmkey")
        # Query to select the LLM key and type from the llm_tokens table
        query = f"""
            SELECT llm_key, llm_type, llm_endpoint, model_name, embedding_model_name
            FROM llm_tokens
            WHERE runner_id = ? and active = True
        """
        logger.info(f"query: {query}")
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
            logger.info(
                "LLM_TOKENS table not yet created, returning empty list, try again later."
            )
            return llm_keys_and_types_struct()

    def db_set_llm_key(self, llm_key, llm_type, project_id=None, dataset_name=None):
        """
        Updates the llm_tokens table with the provided LLM key and type.

        Args:
            llm_key (str): The LLM key.
            llm_type (str): The type of LLM (e.g., 'openai', 'reka').
        """
        runner_id = os.getenv("RUNNER_ID", "jl-local-runner")

        try:
            query = f""" UPDATE {project_id}.{dataset_name}.llm_tokens SET ACTIVE = 0 """
            self.client.execute(query)
        except Exception as e:
            logger.error(
                f"Failed to deactivate current active LLM with error: {e}"
            )

        # Query to merge the LLM tokens, inserting if the row doesn't exist
        query = f"""
            MERGE INTO llm_tokens USING (SELECT 1 AS one) ON (runner_id = ? and llm_type = '{llm_type}')
            WHEN MATCHED THEN
                UPDATE SET llm_key = ?, llm_type = ?, ACTIVE = 1
            WHEN NOT MATCHED THEN
                INSERT (runner_id, llm_key, llm_type, ACTIVE)
                VALUES (?, ?, ?, 1)
        """

        try:
            cursor = self.client.cursor()
            cursor.execute(
                query, (runner_id, llm_key, llm_type, runner_id, llm_key, llm_type)
            )
            self.client.commit()
            affected_rows = cursor.rowcount
            cursor.close()

            if affected_rows > 0:
                logger.info(f"Updated LLM key for runner_id: {runner_id}")
                return True
            else:
                logger.error(f"No rows updated for runner_id: {runner_id}")
                return False
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
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
        """

        available_tools_string = json.dumps(available_tools)
        files_string = json.dumps(files)

        try:
            cursor = self.client.cursor()
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
            self.client.commit()
            logger.info(f"Successfully inserted new bot configuration for bot_id: {bot_id}")

            if not slack_user_allow:
                slack_user_allow_update_query = f"""
                    UPDATE {bot_servicing_table}
                    SET slack_user_allow = parse_json(?)
                    WHERE upper(bot_id) = upper(?)
                    """
                slack_user_allow_value = '["!BLOCK_ALL"]'
                try:
                    cursor.execute(
                        slack_user_allow_update_query, (slack_user_allow_value, bot_id)
                    )
                    self.client.commit()
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

        from genesis_bots.core import global_flags
        # Query to update the available_tools in the database
        update_query = f"""
            UPDATE {bot_servicing_table}
            SET available_tools = ?
            WHERE upper(bot_id) = upper(?)
        """

        # Execute the update query
        try:
            cursor = self.client.cursor()
            cursor.execute(update_query, (updated_tools_str, bot_id))
            self.client.commit()
            logger.info(f"Successfully updated available_tools for bot_id: {bot_id}")

            if "SNOWFLAKE_TOOLS" in updated_tools_str.upper():
                # TODO JD - VERIFY THIS CHANGE
                workspace_schema_name = f"{global_flags.project_id}.{bot_id.replace(r'[^a-zA-Z0-9]', '_').replace('-', '_').replace('.', '_')}_WORKSPACE".upper()
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
            SET files = ?
            WHERE upper(bot_id) = upper(?)
        """
        # Execute the update query
        try:
            cursor = self.client.cursor()
            cursor.execute(update_query, (updated_files_str, bot_id))
            self.client.commit()
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
            SET SLACK_APP_LEVEL_KEY = ?
            WHERE upper(bot_id) = upper(?)
        """

        # Execute the update query
        try:
            cursor = self.client.cursor()
            cursor.execute(update_query, (slack_app_level_key, bot_id))
            self.client.commit()
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
            SET bot_instructions = ?
            WHERE upper(bot_id) = upper(?) AND runner_id = ?
        """

        # Execute the update query
        try:
            cursor = self.client.cursor()
            cursor.execute(update_query, (instructions, bot_id, runner_id))
            self.client.commit()
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
    ):
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
            UPDATE {bot_servicing_table}
            SET bot_implementation = ?
            WHERE upper(bot_id) = upper(?) AND runner_id = ?
        """

        # Execute the update query
        try:
            cursor = self.client.cursor()
            cursor.execute(update_query, (bot_implementation, bot_id, runner_id))
            self.client.commit()
            logger.info(f"Successfully updated bot_implementation for bot_id: {bot_id}")

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
            UPDATE {bot_servicing_table}
            SET SLACK_USER_ALLOW = parse_json(?)
            WHERE upper(bot_id) = upper(?)
        """

        # Convert the list to a format suitable for database storage (e.g., JSON string)
        slack_user_allow_list_str = json.dumps(slack_user_allow_list)
        if slack_user_allow_list == []:
            update_query = f"""
            UPDATE {bot_servicing_table}
            SET SLACK_USER_ALLOW = null
            WHERE upper(bot_id) = upper(?)
               """

        # Execute the update query
        try:
            cursor = self.client.cursor()
            if slack_user_allow_list != []:
                cursor.execute(update_query, (slack_user_allow_list_str, bot_id))
            else:
                cursor.execute(update_query, (bot_id))
            self.client.commit()
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
            WHERE upper(bot_id) = upper(?)
        """

        try:
            cursor = self.client.cursor()
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
            WHERE upper(bot_id) = upper(?)
        """

        try:
            cursor = self.client.cursor()
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

        update_query = f"""
            UPDATE {bot_servicing_table}
            SET API_APP_ID = ?, BOT_SLACK_USER_ID = ?, CLIENT_ID = ?, CLIENT_SECRET = ?,
                SLACK_SIGNING_SECRET = ?, AUTH_URL = ?, AUTH_STATE = ?,
                UDF_ACTIVE = ?, SLACK_ACTIVE = ?, FILES = ?, BOT_IMPLEMENTATION = ?
            WHERE upper(BOT_ID) = upper(?)
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
            SET BOT_SLACK_USER_ID = ?, SLACK_APP_TOKEN = ?
            WHERE upper(BOT_ID) = upper(?)
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
            WHERE upper(bot_id) = upper(?)
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
            WHERE runner_id = ? AND slack_active = 'Y'
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
                logger.info("REST token length: %d", len(rest_token))
            else:
                logger.info("REST token is not available")
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
            logger.info("Request URL: ?", resp.url)
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
                logger.warning("Response content: ?", resp.content)
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
            logger.info("imagegen OpenAI API key is not set in the environment variables.")
            return None

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
                "local_file_name": file_path,
                "prompt": prompt,
            }

            return result
        except Exception as e:
            logger.info(f"imagegen Error generating image with DALL-E 3: {e}")
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

                file_path = f"./runtime/downloaded_files/{thread_id}/" + file_name
                existing_location = f"./runtime/downloaded_files/{thread_id}/{openai_file_id}"

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
                directory = f"./runtime/downloaded_files/{thread_id}"
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

            file_path = f"./runtime/downloaded_files/{thread_id}/" + file_name

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
            logger.info(f"Error converting JSON to YAML: {exc}")
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
            logger.info(f"Error converting YAML to JSON: {exc}")
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
        from genesis_bots.schema_explorer.semantic_tools import modify_semantic_model

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
            logger.info(
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
                UPDATE {bot_servicing_table}
                SET available_tools = ?
                WHERE upper(bot_id) = upper(?)
            """

        # Execute the update query
        try:
            cursor = self.client.cursor()
            cursor.execute(update_query, (updated_tools_str, bot_id))
            self.client.commit()
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

    def extract_knowledge(self, primary_user, bot_name):
        query = f"""SELECT * FROM {self.user_bot_table_name}
                    WHERE primary_user = '{primary_user}' AND BOT_ID LIKE '{bot_name}%'
                    ORDER BY TIMESTAMP DESC
                    LIMIT 1;"""
        knowledge = self.run_query(query)
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
                WHERE timestamp > COALESCE(K.last_timestamp, DATE('2000-01-01')) AND timestamp < DATETIME('{cutoff}');"""
        return self.run_query(query)

    def query_timestamp_message_log(self, thread_id, last_timestamp, max_rows=50):
        query = f"""SELECT * FROM {self.message_log_table_name}
                        WHERE timestamp > DATETIME('{last_timestamp}') AND
                        thread_id = '{thread_id}'
                        ORDER BY TIMESTAMP;"""
        msg_log = self.run_query(query, max_rows=max_rows)
        return msg_log

    def run_insert(self, table, **kwargs):
        keys = ','.join(kwargs.keys())

        insert_query = f"""
            INSERT INTO {table} ({keys})
                VALUES ({','.join(['?']*len(kwargs))})
            """
        cursor = self.client.cursor()
        cursor.execute(insert_query, tuple(kwargs.values()))
        self.client.commit()

    def fetch_embeddings(self, table_id, bot_id=None):
        # Initialize Snowflake connector

        # Initialize variables
        batch_size = 100
        offset = 0
        total_fetched = 0

        # Initialize lists to store results
        embeddings = []
        table_names = []

        # First, get the total number of rows to set up the progress bar
        total_rows_query = f"SELECT COUNT(*) as total FROM {table_id}"
        cursor = self.client.cursor()
        # logger.info('total rows query: ',total_rows_query)
        cursor.execute(total_rows_query)
        total_rows_result = cursor.fetchone()
        total_rows = total_rows_result[0]

        with tqdm(total=total_rows, desc="Fetching embeddings") as pbar:
            while True:
                # Modify the query to include LIMIT and OFFSET
                query = f"SELECT qualified_table_name, embedding FROM {table_id} LIMIT {batch_size} OFFSET {offset}"
                #            logger.info('fetch query ',query)
                cursor.execute(query)
                rows = cursor.fetchall()

                # Temporary lists to hold batch results
                temp_embeddings = []
                temp_table_names = []

                for row in rows:
                    try:
                        temp_embeddings.append(json.loads('['+row[1][5:-3]+']'))
                        temp_table_names.append(row[0])
                    #                    logger.info('temp_embeddings len: ',len(temp_embeddings))
                    #                    logger.info('temp table_names: ',temp_table_names)
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

        try:
            # Fetch the maximum LAST_CRAWLED_TIMESTAMP from the harvest_results table
            query = f"SELECT MAX(LAST_CRAWLED_TIMESTAMP) AS last_crawled_time FROM HARVEST_RESULTS"
            cursor = self.client.cursor()

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
            timestamp_str = last_crawled_time.strftime("%Y%m%dT%H%M%S") + "Z"

            # Create the filename with the .ann extension
            filename = f"{timestamp_str}.ann"
            metafilename = f"{timestamp_str}.json"
            return filename, metafilename
        except Exception as e:
            # Handle errors: for example, table not found, or API errors
            # logger.info(f"An error occurred: {e}, possibly no data yet harvested, using default name for index file.")
            # Return a default filename or re-raise the exception based on your use case
            return "default_filename.ann", "default_metadata.json"

    def one_time_db_fixes(self):
        # Remove BOT_FUNCTIONS if it exists
        bot_functions_table_check_query = "SELECT name FROM sqlite_master WHERE type='table' AND name='BOT_FUNCTIONS';"
        cursor = self.client.cursor()
        cursor.execute(bot_functions_table_check_query)

        if cursor.fetchone():
            query = "DROP TABLE BOT_FUNCTIONS"
            cursor.execute(query)
            logger.info("Table BOT_FUNCTIONS dropped.")

        # REMOVE BOT_NOTEBOOK if it exists
        bot_notebook_table_check_query = "SELECT name FROM sqlite_master WHERE type='table' AND name='BOT_NOTEBOOK';"
        cursor = self.client.cursor()
        cursor.execute(bot_notebook_table_check_query)

        if cursor.fetchone():
            query = "DROP TABLE BOT_NOTEBOOK"
            cursor.execute(query)
            logger.info("Table BOT_NOTEBOOK dropped.")

        # Add manage_notebook_tool to existing bots
        bots_table_check_query = "SELECT name FROM sqlite_master WHERE type='table' AND name='BOT_SERVICING';"
        cursor = self.client.cursor()
        cursor.execute(bots_table_check_query)

        if cursor.fetchone():
            # Fetch all existing bots
            fetch_bots_query = "SELECT BOT_NAME, AVAILABLE_TOOLS FROM BOT_SERVICING;"
            cursor.execute(fetch_bots_query)
            bots = cursor.fetchall()

            for bot in bots:
                bot_name, tools = bot
                if tools:
                    tools_list = json.loads(tools)
                    if 'notebook_manager_tools' not in tools_list:
                        tools_list.append('notebook_manager_tools')
                        updated_tools = json.dumps(tools_list)
                        update_query = """
                        UPDATE BOT_SERVICING
                        SET AVAILABLE_TOOLS = ?
                        WHERE BOT_NAME = ?
                        """
                        cursor.execute(update_query, (updated_tools, bot_name))
                else:
                    update_query = """
                    UPDATE BOT_SERVICING
                    SET AVAILABLE_TOOLS = '[notebook_manager_tools]'
                    WHERE BOT_NAME = ?
                    """
                    cursor.execute(update_query, (bot_name,))

            self.client.commit()
            logger.info("Added notebook_manager_tools to all existing bots.")
        else:
            logger.info("BOTS table does not exist. Skipping tool addition.")

        check_llm_endpoint_query = "PRAGMA table_info(BOT_SERVICING);"
        try:
            cursor = self.client.cursor()
            cursor.execute(check_llm_endpoint_query)
            columns = [col[1] for col in cursor.fetchall()]

            if "TEAMS_ACTIVE" not in columns:
                # Add TEAMS_ACTIVE column if it doesn't exist
                alter_table_query = """ALTER TABLE BOT_SERVICING ADD COLUMN TEAMS_ACTIVE TEXT,
                    TEAMS_APP_ID TEXT,
                    TEAMS_APP_PASSWORD TEXT,
                    TEAMS_APP_TYPE TEXT,
                    TEAMS_APP_TENANT_ID TEXT;"""
                cursor.execute(alter_table_query)
                self.client.commit()
                logger.info("Column 'TEAMS_ACTIVE' added to table BOT_SERVICING.")

                set_to_false_query = "UPDATE BOT_SERVICING SET TEAMS_ACTIVE = 'N';"
                cursor.execute(set_to_false_query)
                self.client.commit()
                logger.info("Column 'TEAMS_ACTIVE' set to 'N' for all rows in table BOT_SERVICING.")
        except Exception as e:
            logger.error(f"An error occurred while checking or altering table BOT_SERVICING to add TEAMS_ACTIVE column: {e}")
        finally:
            if cursor is not None:
                cursor.close()

        return

    def get_llm_info(self, thread_id=None):
        """
        Retrieves a list of all llm types and keys.

        Returns:
            list: A list of llm keys, llm types, and the active switch.
        """
        try:
            runner_id = os.getenv("RUNNER_ID", "jl-local-runner")
            query = """
            SELECT LLM_TYPE, ACTIVE, LLM_KEY, LLM_ENDPOINT
            FROM LLM_TOKENS
            WHERE LLM_KEY is not NULL
            AND RUNNER_ID = ?
            """
            cursor = self.client.cursor()
            cursor.execute(query, (runner_id,))
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

    def get_email(self):
        """
        Retrieves the email address if set.

        Returns:
            list: An email address, if set.
        """
        try:
            query = "SELECT DEFAULT_EMAIL FROM DEFAULT_EMAIL"
            cursor = self.client.cursor()
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
