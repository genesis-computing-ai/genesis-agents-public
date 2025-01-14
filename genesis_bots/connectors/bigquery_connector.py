from google.cloud import bigquery
from google.oauth2 import service_account
from genesis_bots.connectors.data_connector import DatabaseConnector
from google.cloud.exceptions import NotFound
from itertools import islice
from datetime import datetime
import uuid
import os
import time
import hashlib
import json
from tqdm import tqdm

from genesis_bots.core.logging_config import logger

class BigQueryConnector(DatabaseConnector):
    def __init__(self, connection_info, connection_name):
        super().__init__(connection_info, connection_name)
        self.client = self._create_client()
        self.genbot_internal_project_and_schema = os.getenv('GENESIS_INTERNAL_DB_SCHEMA','None')
        if  self.genbot_internal_project_and_schema is None:
            self.genbot_internal_project_and_schema = os.getenv('ELSA_INTERNAL_DB_SCHEMA','None')
            logger.info("!! Please switch from using ELSA_INTERNAL_DB_SCHEMA ENV VAR to GENESIS_INTERNAL_DB_SCHEMA !!")
        if self.genbot_internal_project_and_schema == 'None':
            # Todo remove, internal note
            logger.info("ENV Variable GENBOT_INTERNAL_DB_SCHEMA is not set.")
        if self.genbot_internal_project_and_schema is not None:
           self.genbot_internal_project_and_schema = self.genbot_internal_project_and_schema.upper()
        self.genbot_internal_harvest_table = os.getenv('GENESIS_INTERNAL_HARVEST_RESULTS_TABLE','harvest_results')
        self.genbot_internal_harvest_control_table = os.getenv('GENESIS_INTERNAL_HARVEST_CONTROL_TABLE','harvest_control')
        self.genbot_internal_message_log = os.getenv('GENESIS_INTERNAL_MESSAGE_LOG_TABLE','message_log')

        logger.info("genbot_internal_project_and_schema: ", self.genbot_internal_project_and_schema)
        self.metadata_table_name = self.genbot_internal_project_and_schema+'.'+self.genbot_internal_harvest_table
        self.harvest_control_table_name = self.genbot_internal_project_and_schema+'.'+self.genbot_internal_harvest_control_table
        self.message_log_table_name = self.genbot_internal_project_and_schema+'.'+self.genbot_internal_message_log

        logger.info("harvest_control_table_name: ", self.harvest_control_table_name)
        logger.info("metadata_table_name: ", self.metadata_table_name)
        logger.info("message_log_table_name: ", self.genbot_internal_message_log)

        self.project_id = connection_info["project_id"]
        # make sure harvester control and results tables are available, if not create them
        #self.ensure_table_exists()
        self.source_name = 'BigQuery'

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
            query = f"SELECT * FROM `{self.harvest_control_table_name}`"
            query_job = self.client.query(query)  # Make an API request.
            results = query_job.result()  # Wait for the job to complete.

            # Convert the query results to a list of dictionaries
            rows = [dict(row) for row in results]

            # Convert the list of dictionaries to a JSON object
            json_data = json.dumps(rows, default=str)  # default=str to handle datetime and other non-serializable types

            return {"Success": True, "Data": json_data}

        except Exception as e:
            err = f"An error occurred while retrieving the harvest summary: {e}"
            return {"Success": False, "Error": err}


    def set_harvest_control_data(self, source_name, database_name, initial_crawl_complete=False, refresh_interval=1, schema_exclusions=None, schema_inclusions=None, status='Include', thread_id=None):
        """
        Inserts or updates a row in the harvest control table using MERGE statement with explicit parameters.

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

            # Prepare the MERGE statement
            merge_statement = f"""
            MERGE `{self.harvest_control_table_name}` T
            USING (SELECT @source_name AS source_name, @database_name AS database_name) S
            ON T.source_name = S.source_name AND T.database_name = S.database_name
            WHEN MATCHED THEN
              UPDATE SET
                initial_crawl_complete = @initial_crawl_complete,
                refresh_interval = @refresh_interval,
                schema_exclusions = @schema_exclusions,
                schema_inclusions = @schema_inclusions,
                status = @status
            WHEN NOT MATCHED THEN
              INSERT (source_name, database_name, initial_crawl_complete, refresh_interval, schema_exclusions, schema_inclusions, status)
              VALUES (@source_name, @database_name, @initial_crawl_complete, @refresh_interval, @schema_exclusions, @schema_inclusions, @status)
            """
            query_params = [
                bigquery.ScalarQueryParameter("source_name", "STRING", source_name),
                bigquery.ScalarQueryParameter("database_name", "STRING", database_name),
                bigquery.ScalarQueryParameter("initial_crawl_complete", "BOOL", initial_crawl_complete),
                bigquery.ScalarQueryParameter("refresh_interval", "INT64", refresh_interval),
                bigquery.ArrayQueryParameter("schema_exclusions", "STRING", schema_exclusions),
                bigquery.ArrayQueryParameter("schema_inclusions", "STRING", schema_inclusions),
                bigquery.ScalarQueryParameter("status", "STRING", status)
            ]
            job_config = bigquery.QueryJobConfig(query_parameters=query_params)
            query_job = self.client.query(merge_statement, job_config=job_config)
            query_job.result()  # Wait for the job to complete

            return {"Success": True, "Message": "Harvest control data set successfully."}

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
            query = f"""
            DELETE FROM `{self.harvest_control_table_name}`
            WHERE source_name = @source_name AND database_name = @database_name
            """
            query_params = [
                bigquery.ScalarQueryParameter("source_name", "STRING", source_name),
                bigquery.ScalarQueryParameter("database_name", "STRING", database_name)
            ]
            job_config = bigquery.QueryJobConfig(query_parameters=query_params)

            # Execute the query
            query_job = self.client.query(query, job_config=job_config)
            query_job.result()  # Wait for the job to complete

            if query_job.num_dml_affected_rows == 0:
                return {"Success": False, "Message": "No harvest records were found for that source and database.  You should check the source_name and database_name with the get_harvest_control_data tool ?"}
            else:
                return {f"Success": True, "Message": "Harvest control data removed successfully. {query_job.num_dml_affected_rows} rows affected."}


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
            DELETE FROM `{self.metadata_table_name}`
            WHERE source_name = @source_name AND database_name = @database_name
            """
            query_params = [
                bigquery.ScalarQueryParameter("source_name", "STRING", source_name),
                bigquery.ScalarQueryParameter("database_name", "STRING", database_name)
            ]
            job_config = bigquery.QueryJobConfig(query_parameters=query_params)

            # Execute the query
            query_job = self.client.query(delete_query, job_config=job_config)
            query_job.result()  # Wait for the job to complete

            return {"Success": True, "Message": "Metadata rows removed successfully."}

        except Exception as e:
            err = f"An error occurred while removing the metadata rows: {e}"
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
        FROM `{self.metadata_table_name}`
        GROUP BY source_name, database_name, schema_name, role_used_for_crawl
        ORDER BY source_name, database_name, schema_name, role_used_for_crawl;
        """
        try:
            query_job = self.client.query(query)  # Make an API request.
            results = query_job.result()  # Wait for the job to complete.

            # Convert the query results to a list of dictionaries
            summary = [dict(row) for row in results]

            json_data = json.dumps(summary, default=str)  # default=str to handle datetime and other non-serializable types

            return {"Success": True, "Data": json_data}

        except Exception as e:
            err = f"An error occurred while retrieving the harvest summary: {e}"
            return {"Success": False, "Error": err}


    def table_summary_exists(self, qualified_table_name):
            query = f"""
            SELECT COUNT(*)
            FROM `{self.metadata_table_name}`
            WHERE qualified_table_name = '{qualified_table_name}'
            """
            query_job = self.client.query(query)  # Make an API request and return the query results.
            results = query_job.result()

            for row in results:
                return row[0] > 0  # Returns True if a row exists, False otherwise

    def insert_chat_history_row(self, timestamp, bot_id=None, bot_name=None, thread_id=None, message_type=None, message_payload=None, message_metadata=None, tokens_in=None, tokens_out=None):
        """
        Inserts a single row into the chat history table using BigQuery's streaming insert.

        :param timestamp: TIMESTAMP field, format should be compatible with BigQuery.
        :param bot_id: STRING field representing the bot's ID.
        :param bot_name: STRING field representing the bot's name.
        :param thread_id: STRING field representing the thread ID, can be NULL.
        :param message_type: STRING field representing the type of message.
        :param message_payload: STRING field representing the message payload, can be NULL.
        :param message_metadata: STRING field representing the message metadata, can be NULL.
        :param tokens_in: INTEGER field representing the number of tokens in, can be NULL.
        :param tokens_out: INTEGER field representing the number of tokens out, can be NULL.
        """

         # Ensure the timestamp is in the correct format for BigQuery
        formatted_timestamp = timestamp.isoformat(" ") if isinstance(timestamp, datetime) else timestamp
        if isinstance(message_metadata, dict):
                message_metadata = json.dumps(message_metadata)

        table_id = self.message_log_table_name
        row_to_insert = [{
            "timestamp": formatted_timestamp,
            "bot_id": bot_id,
            "bot_name": bot_name,
            "thread_id": thread_id,
            "message_type": message_type,
            "message_payload": message_payload,
            "message_metadata": message_metadata,
            "tokens_in": tokens_in,
            "tokens_out": tokens_out,
        }]

        errors = self.client.insert_rows_json(table_id, row_to_insert)  # Make an API request.
        if errors == []:
            #logger.info("New row has been added.")
            pass
        else:
            logger.info("Encountered errors while inserting into chat history table row: {}".format(errors))

    def ensure_table_exists(self):

        # CHAT HISTORY TABLE
        chat_history_table_id = self.message_log_table_name
        schema_chat_history = [
            bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("bot_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("bot_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("thread_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("message_type", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("message_payload", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("message_metadata", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("tokens_in", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("tokens_out", "INTEGER", mode="NULLABLE"),
        ]

        table_chat_history = bigquery.Table(chat_history_table_id, schema=schema_chat_history)

        # Check if the chat history table exists
        try:
            self.client.get_table(table_chat_history)  # Make an API request.
            logger.info(f"Table {chat_history_table_id} already exists.")
        except NotFound:
            # If the table does not exist, create it
            self.client.create_table(table_chat_history)  # Make an API request.
            logger.info(f"Table {chat_history_table_id} created.")


        # HARVEST CONTROL TABLE
        hc_table_id = self.harvest_control_table_name
        schema_hc = [
            bigquery.SchemaField("source_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("database_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("schema_inclusions", "STRING", mode="REPEATED"),
            bigquery.SchemaField("schema_exclusions", "STRING", mode="REPEATED"),
            bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("refresh_interval", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("initial_crawl_complete", "BOOLEAN", mode="REQUIRED"),


        ]

        table_hc = bigquery.Table(hc_table_id, schema=schema_hc)

        # Check if the table exists
        try:
            self.client.get_table(table_hc)  # Make an API request.
            logger.info(f"Table {hc_table_id} already exists.")
        except NotFound:
            # If the table does not exist, create it
            self.client.create_table(table_hc)  # Make an API request.
            logger.info(f"Table {hc_table_id} created.")
            self.client.get_table(table_hc)

            query = f"""
            INSERT INTO `{self.harvest_control_table_name}` (source_name, database_name, schema_exclusions, status, refresh_interval, initial_crawl_complete)
            VALUES ('{self.source_name}','{self.project_id}',ARRAY['ELSA_INTERNAL','INFORMATION_SCHEMA'],'Include', 1, False);
            """
            query_job = self.client.query(query)  # Make an API request.
            results = query_job.result()  # Wait for the job to complete.
            logger.info("insert base harvest control row: ",results)

        # METADATA TABLE FOR HARVESTER RESULTS
        table_id = self.metadata_table_name
        schema = [
            bigquery.SchemaField("source_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("qualified_table_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("database_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("memory_uuid", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("schema_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("table_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("complete_description", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("ddl", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("ddl_hash", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("summary", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("sample_data_text", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("last_crawled_timestamp", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("crawl_status", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("role_used_for_crawl", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("embedding", "FLOAT64", mode="REPEATED"),  # New field for embeddings
        ]

        table = bigquery.Table(table_id, schema=schema)

        # Check if the table exists
        try:
            self.client.get_table(table)  # Make an API request.
            logger.info(f"Table {table_id} already exists.")
        except NotFound:
            # If the table does not exist, create it
            self.client.create_table(table)  # Make an API request.

            query = f"""
            CREATE SEARCH INDEX `{self.metadata_table_name}_index`
            ON `{self.metadata_table_name}`(source_name, qualified_table_name) OPTIONS (analyzer = 'NO_OP_ANALYZER');
            """
            query_job = self.client.query(query)  # Make an API request.
            results = query_job.result()  # Wait for the job to complete.
            logger.info("index create results: ",results)

            logger.info(f"Table {table_id} created.")


    def insert_table_summary(self, database_name, schema_name, table_name, ddl, summary, sample_data_text, complete_description="", crawl_status="Completed", role_used_for_crawl="Default", embedding=None):

        qualified_table_name = f"{database_name}.{schema_name}.{table_name}"
        memory_uuid = str(uuid.uuid4())
        last_crawled_timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat(" ")
        ddl_hash = self.sha256_hash_hex_string(ddl)

        # Assuming role_used_for_crawl is stored in self.connection_info["client_email"]
        role_used_for_crawl = self.connection_info["client_email"]

        last_crawled_timestamp_literal = f"TIMESTAMP '{last_crawled_timestamp}'"

        # Construct the MERGE SQL statement with placeholders for parameters
        merge_sql = f"""
        MERGE INTO `{self.metadata_table_name}` T
        USING (SELECT @source_name as source_name,
                    @qualified_table_name as qualified_table_name,
                    @memory_uuid as memory_uuid,
                    @database_name as database_name,
                    @schema_name as schema_name,
                    @table_name as table_name,
                    @complete_description as complete_description,
                    @ddl as ddl,
                    @ddl_hash as ddl_hash,
                    @summary as summary,
                    @sample_data_text as sample_data_text,
                    {last_crawled_timestamp_literal} as last_crawled_timestamp,
                    @crawl_status as crawl_status,
                    @role_used_for_crawl as role_used_for_crawl,
                    @embedding as embedding) S
        ON T.qualified_table_name = S.qualified_table_name
        WHEN MATCHED THEN
            UPDATE SET source_name = S.source_name,
                    memory_uuid = S.memory_uuid,
                    database_name = S.database_name,
                    schema_name = S.schema_name,
                    table_name = S.table_name,
                    complete_description = S.complete_description,
                    ddl = S.ddl,
                    ddl_hash = S.ddl_hash,
                    summary = S.summary,
                    sample_data_text = S.sample_data_text,
                    last_crawled_timestamp = S.last_crawled_timestamp,
                    crawl_status = S.crawl_status,
                    role_used_for_crawl = S.role_used_for_crawl,
                    embedding = S.embedding
        WHEN NOT MATCHED THEN
            INSERT (source_name, qualified_table_name, memory_uuid, database_name, schema_name, table_name, complete_description, ddl, ddl_hash, summary, sample_data_text, last_crawled_timestamp, crawl_status, role_used_for_crawl, embedding)
            VALUES (@source_name, @qualified_table_name, @memory_uuid, @database_name, @schema_name, @table_name, @complete_description, @ddl, @ddl_hash, @summary, @sample_data_text, @last_crawled_timestamp, @crawl_status, @role_used_for_crawl, @embedding);
        """

        # Set up the query parameters
        query_params = [
            bigquery.ScalarQueryParameter("source_name", "STRING", self.source_name),
            bigquery.ScalarQueryParameter("qualified_table_name", "STRING", qualified_table_name),
            bigquery.ScalarQueryParameter("memory_uuid", "STRING", memory_uuid),
            bigquery.ScalarQueryParameter("database_name", "STRING", database_name),
            bigquery.ScalarQueryParameter("schema_name", "STRING", schema_name),
            bigquery.ScalarQueryParameter("table_name", "STRING", table_name),
            bigquery.ScalarQueryParameter("complete_description", "STRING", complete_description),
            bigquery.ScalarQueryParameter("ddl", "STRING", ddl),
            bigquery.ScalarQueryParameter("ddl_hash", "STRING", ddl_hash),
            bigquery.ScalarQueryParameter("summary", "STRING", summary),
            bigquery.ScalarQueryParameter("sample_data_text", "STRING", sample_data_text),
            bigquery.ScalarQueryParameter("last_crawled_timestamp", "TIMESTAMP", last_crawled_timestamp),
            bigquery.ScalarQueryParameter("crawl_status", "STRING", crawl_status),
            bigquery.ScalarQueryParameter("role_used_for_crawl", "STRING", role_used_for_crawl),
            bigquery.ArrayQueryParameter("embedding", "FLOAT64", embedding if embedding is not None else [])   ]

        job_config = bigquery.QueryJobConfig(query_parameters=query_params)

        # Execute the MERGE statement with parameters
        query_job = self.client.query(merge_sql, job_config=job_config)  # Make an API request.
        query_job.result()  # Wait for the job to complete.



    def get_table_ddl(self, database_name:str, schema_name:str, table_name=None):
        """
        Fetches the DDL statements for tables within a specific dataset in BigQuery.
        Optionally, fetches the DDL for a specific table if table_name is provided.

        :param dataset_name: The name of the dataset.
        :param table_name: Optional. The name of a specific table.
        :return: A dictionary with table names as keys and DDL statements as values, or a single DDL string if table_name is provided.
        """
        if table_name:
            query = f"""
            SELECT table_name, ddl
            FROM `{database_name}.{schema_name}.INFORMATION_SCHEMA.TABLES`
            WHERE table_name = '{table_name}';
            """
        else:
            query = f"""
            SELECT table_name, ddl
            FROM `{database_name}.{schema_name}.INFORMATION_SCHEMA.TABLES`;
            """
        query_job = self.client.query(query)  # Make an API request.
        results = query_job.result()  # Wait for the job to complete.

        if table_name:
            return {row.table_name: row.ddl for row in results}[table_name]
        else:
            return {row.table_name: row.ddl for row in results}

    def _create_client(self):
        credentials = service_account.Credentials.from_service_account_info(self.connection_info)
        return bigquery.Client(credentials=credentials, project=self.connection_info['project_id'])

    def connector_type(self):
        return 'bigquery'

    def get_databases(self):
        databases = []
        query = "SELECT source_name, database_name, schema_inclusions, schema_exclusions, status, refresh_interval, initial_crawl_complete,  from `"+self.harvest_control_table_name+"`"
        query_job = self.client.query(query)
        for row in query_job:
            databases.append(row)
        return databases

    def get_schemas(self, database):
        schemas = []
        query = "SELECT schema_name as sch FROM `"+database+".INFORMATION_SCHEMA.SCHEMATA`"
        query_job = self.client.query(query)
        for row in query_job:
            schemas.append(row.sch)
        return schemas

    def get_tables(self, database, schema):
        tables = []
        query = f"SELECT table_name, TO_HEX(SHA256(ddl)) as ddl_hash FROM `{database}.{schema}.INFORMATION_SCHEMA.TABLES`"
        query_job = self.client.query(query)
        for row in query_job:
            tables.append({"table_name":row.table_name, "ddl_hash":row.ddl_hash})
        return tables

    def get_columns(self, database, schema, table):
        columns = []
        query = f"SELECT column_name FROM {database}.{schema}.INFORMATION_SCHEMA.COLUMNS WHERE table_name = '{table}'"
        query_job = self.client.query(query)
        for row in query_job:
            columns.append(row.column_name)
        return columns

    def get_sample_data(self, database, schema_name:str, table_name:str):
        """
        Fetches 10 rows of sample data from a specific table in BigQuery.

        :param dataset_name: The name of the dataset.
        :param table_name: The name of the table.
        :return: A list of dictionaries representing rows of sample data.
        """
        query = f"SELECT * FROM `{database}.{schema_name}.{table_name}` LIMIT 10;"
        query_job = self.client.query(query)  # Make an API request.
        results = query_job.result()  # Wait for the job to complete.

        sample_data = [dict(row) for row in results]
        return sample_data


    def run_query(self, query, max_rows=20, max_rows_override = False, job_config=None):
        """
        Placeholder method to run a query.
        This method should be overridden in subclasses for specific database types.

        """

        if (max_rows > 100 and not max_rows_override):
            max_rows = 100
        #Why does this get run twice each time?

        if job_config:
            query_job = self.client.query(query, job_config=job_config)  # Make an API request.
        else:
            query_job = self.client.query(query)  # Make an API request.

        results = query_job.result()  # Wait for the job to complete.

        sample_data = [dict(row) for row in islice(results, max_rows)]
        return sample_data


    def db_list_all_bots(self, project_id, dataset_name, bot_servicing_table, runner_id=None, full=False):
        """
        Returns a list of all the bots being served by the system, including their runner IDs, names, instructions, tools, etc.

        Returns:
            list: A list of dictionaries, each containing details of a bot.
        """
        # Get the database schema from environment variables

        if full:
            select_str = "api_app_id, bot_slack_user_id, bot_id, bot_name, bot_instructions, runner_id, slack_app_token, slack_signing_secret, slack_channel_id, available_tools, udf_active, slack_active, files, bot_implementation"
        else:
            select_str = "runner_id, bot_id, bot_name, bot_instructions, available_tools, bot_slack_user_id, api_app_id, auth_url, udf_active, slack_active, files, bot_implementation"

        # Query to select all bots from the BOT_SERVICING table
        if runner_id is None:
            select_query = f"""
            SELECT {select_str}
            FROM `{project_id}.{dataset_name}.{bot_servicing_table}`;"""
        else:
            select_query = f"""
            SELECT {select_str}
            FROM `{project_id}.{dataset_name}.{bot_servicing_table}`
            WHERE runner_id = '{runner_id}';"""

        try:
            # Execute the query and fetch all bot records
            bots = self.run_query(query=select_query)
            # Convert the bot records to a list of dictionaries
            bot_list = [dict(bot) for bot in bots]
            logger.info(f"Retrieved list of all bots being served by the system.")
            return bot_list
        except Exception as e:
            logger.error(f"Failed to retrieve list of all bots with error: {e}")
            raise e

    def get_bot_details_by_bot_id(self, project_id, dataset_name, bot_servicing_table, bot_id):
        """
        Retrieves the details of a bot given its bot_id.

        Args:
            project_id (str): The project ID where the bot servicing table is located.
            dataset_name (str): The dataset name where the bot servicing table is located.
            bot_servicing_table (str): The name of the bot servicing table.
            bot_id (str): The unique identifier of the bot.

        Returns:
            dict: A dictionary containing the details of the bot if found, None otherwise.
        """
        select_query = f"""
        SELECT *
        FROM `{project_id}.{dataset_name}.{bot_servicing_table}`
        WHERE bot_id = '{bot_id}'
        LIMIT 1;"""

        try:
            # Execute the query and fetch the bot record
            bot_details = self.run_query(query=select_query)
            # Convert the bot record to a dictionary
            bot_details_dict = dict(next(bot_details, None))
            logger.info(f"Retrieved details for bot_id: {bot_id}")
            return bot_details_dict
        except Exception as e:
            logger.error(f"Failed to retrieve details for bot_id: {bot_id} with error: {e}")
            return None


    def db_save_slack_config_tokens(self, slack_app_config_token, slack_app_config_refresh_token, project_id, dataset_name):
        """
        Saves the slack app config token and refresh token for the given runner_id to BigQuery.

        Args:
            runner_id (str): The unique identifier for the runner.
            slack_app_config_token (str): The slack app config token to be saved.
            slack_app_config_refresh_token (str): The slack app config refresh token to be saved.
        """

        runner_id = os.getenv('RUNNER_ID','jl-local-runner')

        # Query to insert or update the slack app config tokens
        query = f"""
            MERGE `{project_id}.{dataset_name}.slack_app_config_tokens` T
            USING (SELECT '{runner_id}' as runner_id) S
            ON T.runner_id = S.runner_id
            WHEN MATCHED THEN
                UPDATE SET slack_app_config_token = '{slack_app_config_token}', slack_app_config_refresh_token = '{slack_app_config_refresh_token}'
            WHEN NOT MATCHED THEN
                INSERT (runner_id, slack_app_config_token, slack_app_config_refresh_token)
                VALUES ('{runner_id}', '{slack_app_config_token}', '{slack_app_config_refresh_token}');"""

        # Execute the query
        try:
            self.run_query(query=query)
            logger.info(f"Slack config tokens updated for runner_id: {runner_id}")
        except Exception as e:
            logger.error(f"Failed to update Slack config tokens for runner_id: {runner_id} with error: {e}")

    def db_get_slack_config_tokens(self, project_id, dataset_name):
        """
        Retrieves the current slack access keys for the given runner_id from BigQuery.

        Args:
            runner_id (str): The unique identifier for the runner.

        Returns:
            tuple: A tuple containing the slack app config token and the slack app config refresh token.
        """

        runner_id = os.getenv('RUNNER_ID','jl-local-runner')

        # Query to retrieve the slack app config tokens
        query = f"""
            SELECT slack_app_config_token, slack_app_config_refresh_token
            FROM `{project_id}.{dataset_name}.slack_app_config_tokens`
            WHERE runner_id = '{runner_id}';"""

        # Execute the query and fetch the results
        result = self.run_query(query=query)

        # Extract tokens from the result
        if result:
            slack_app_config_token = result[0].get('slack_app_config_token')
            slack_app_config_refresh_token = result[0].get('slack_app_config_refresh_token')
            return slack_app_config_token, slack_app_config_refresh_token
        else:
            # Log an error if no tokens were found for the runner_id
            logger.error(f"No Slack config tokens found for runner_id: {runner_id}")
            return None, None

    def db_get_ngrok_auth_token(self, project_id, dataset_name):
        """
        Retrieves the ngrok authentication token and related information for the given runner_id from BigQuery.

        Args:
            runner_id (str): The unique identifier for the runner.

        Returns:
            tuple: A tuple containing the ngrok authentication token, use domain flag, and domain.
        """

        runner_id = os.getenv('RUNNER_ID','jl-local-runner')

        # Query to retrieve the ngrok auth token and related information
        query = f"""
            SELECT ngrok_auth_token, ngrok_use_domain, ngrok_domain
            FROM `{project_id}.{dataset_name}.ngrok_tokens`
            WHERE runner_id = '{runner_id}'
        """

        # Execute the query and fetch the results
        result = self.run_query(query=query)

        # Extract tokens from the result
        if result:
            ngrok_token = result[0].get('ngrok_auth_token')
            ngrok_use_domain = result[0].get('ngrok_use_domain')
            ngrok_domain = result[0].get('ngrok_domain')
            return ngrok_token, ngrok_use_domain, ngrok_domain
        else:
            # Log an error if no tokens were found for the runner_id
            logger.error(f"No Ngrok config token found in database for runner_id: {runner_id}")
            return None, None, None

    def db_set_ngrok_auth_token(self, ngrok_auth_token, ngrok_use_domain='N', ngrok_domain='', project_id=None, dataset_name=None):
        """
        Updates the ngrok_tokens table with the provided ngrok authentication token, use domain flag, and domain.

        Args:
            ngrok_auth_token (str): The ngrok authentication token.
            ngrok_use_domain (str): Flag indicating whether to use a custom domain.
            ngrok_domain (str): The custom domain to use if ngrok_use_domain is 'Y'.
        """
        runner_id = os.getenv('RUNNER_ID', 'jl-local-runner')

        # Query to update the ngrok tokens with bind variables
        query = f"""
            UPDATE `{project_id}.{dataset_name}.ngrok_tokens`
            SET ngrok_auth_token = @ngrok_auth_token,
                ngrok_use_domain = @ngrok_use_domain,
                ngrok_domain = @ngrok_domain
            WHERE runner_id = @runner_id
        """

        query_params = [
            bigquery.ScalarQueryParameter("ngrok_auth_token", "STRING", ngrok_auth_token),
            bigquery.ScalarQueryParameter("ngrok_use_domain", "STRING", ngrok_use_domain),
            bigquery.ScalarQueryParameter("ngrok_domain", "STRING", ngrok_domain),
            bigquery.ScalarQueryParameter("runner_id", "STRING", runner_id)
        ]

        job_config = bigquery.QueryJobConfig(query_parameters=query_params)

        try:
            result = self.run_query(query=query, job_config=job_config)
        except Exception as e:
            logger.error(f"Failed to update ngrok tokens for runner_id: {runner_id} with error: {e}")
            return False

        if result is not None:
            logger.info(f"Updated ngrok tokens for runner_id: {runner_id}")
        else:
            logger.error(f"Failed to update ngrok tokens for runner_id: {runner_id}")
            return False

        return True

    def db_insert_new_bot(self, api_app_id, bot_slack_user_id, bot_id, bot_name, bot_instructions, runner_id, slack_signing_secret,
                    slack_channel_id, available_tools, auth_url, auth_state, client_id, client_secret, udf_active,
                    slack_active, files, bot_implementation, project_id, dataset_name, bot_servicing_table):
        """
        Inserts a new bot configuration into the BOT_SERVICING table.

        Args:
            api_app_id (str): The API application ID for the bot.
            bot_slack_user_id (str): The Slack user ID for the bot.
            bot_id (str): The unique identifier for the bot.
            bot_name (str): The name of the bot.
            bot_instructions (str): Instructions for the bot's operation.
            runner_id (str): The identifier for the runner that will manage this bot.
            slack_app_token (str): The Slack app token for the bot.
            slack_signing_secret (str): The Slack signing secret for the bot.
            slack_channel_id (str): The Slack channel ID where the bot will operate.
            tools (str): A list of tools the bot has access to.
            files (json-embedded list): A list of files to include with the bot.
            bot_implementation (str): openai or cortex or ...
        """

        insert_query = f"""
            INSERT INTO  `{project_id}.{dataset_name}.{bot_servicing_table}` (
                api_app_id, bot_slack_user_id, bot_id, bot_name, bot_instructions, runner_id,
                slack_signing_secret, slack_channel_id, available_tools, auth_url, auth_state, client_id, client_secret, udf_active, slack_active,
                files, bot_implementation
            ) VALUES (
                @api_app_id, @bot_slack_user_id, @bot_id, @bot_name, @bot_instructions,
                @runner_id, @slack_signing_secret, @slack_channel_id,
                @available_tools, @auth_url, @auth_state, @client_id, @client_secret, @udf_active, @slack_active,
                @files, @bot_implementation
            )
        """

        available_tools_string = str(available_tools).replace('\'','"')
        # Set the query parameters
        query_params = [
            bigquery.ScalarQueryParameter("api_app_id", "STRING", api_app_id),
            bigquery.ScalarQueryParameter("bot_slack_user_id", "STRING", bot_slack_user_id),
            bigquery.ScalarQueryParameter("bot_id", "STRING", bot_id),
            bigquery.ScalarQueryParameter("bot_name", "STRING", bot_name),
            bigquery.ScalarQueryParameter("bot_instructions", "STRING", bot_instructions),
            bigquery.ScalarQueryParameter("runner_id", "STRING", runner_id),
            bigquery.ScalarQueryParameter("slack_signing_secret", "STRING", slack_signing_secret),
            bigquery.ScalarQueryParameter("slack_channel_id", "STRING", slack_channel_id),
            bigquery.ScalarQueryParameter("available_tools", "STRING", available_tools_string),
            bigquery.ScalarQueryParameter("auth_url", "STRING", auth_url),
            bigquery.ScalarQueryParameter("auth_state", "STRING", auth_state),
            bigquery.ScalarQueryParameter("client_id", "STRING", client_id),
            bigquery.ScalarQueryParameter("client_secret", "STRING", client_secret),
            bigquery.ScalarQueryParameter("udf_active", "STRING", udf_active),
            bigquery.ScalarQueryParameter("slack_active", "STRING", slack_active),
            bigquery.ScalarQueryParameter("files", "STRING", files),
            bigquery.ScalarQueryParameter("bot_implementation", "STRING", bot_implementation)
        ]

        job_config = bigquery.QueryJobConfig(query_parameters=query_params)

        try:
            self.run_query(query=insert_query, job_config=job_config)
            logger.info(f"Successfully inserted new bot configuration for bot_id: {bot_id}")
        except Exception as e:
            logger.info(f"Failed to insert new bot configuration for bot_id: {bot_id} with error: {e}")
            raise e


    def db_update_bot_tools(self, project_id=None,dataset_name=None,bot_servicing_table=None, bot_id=None, updated_tools_str=None, new_tools_to_add=None, already_present=None, updated_tools=None):

    # Query to update the available_tools in the database
        update_query = f"""
            UPDATE `{project_id}.{dataset_name}.{bot_servicing_table}`
            SET available_tools = @updated_tools
            WHERE bot_id = @bot_id;"""

        # Set the query parameters
        query_params = [
            bigquery.ScalarQueryParameter("bot_id", "STRING", bot_id),
            bigquery.ScalarQueryParameter("updated_tools", "STRING", updated_tools_str),
        ]

        job_config = bigquery.QueryJobConfig(query_parameters=query_params)

        # Execute the update query
        try:
            self.run_query(query=update_query, job_config=job_config)
            logger.info(f"Successfully updated available_tools for bot_id: {bot_id}")

            return {
                "success": True,
                "added": new_tools_to_add,
                "already_present": already_present,
                "all_bot_tools": updated_tools
            }

        except Exception as e:
            logger.error(f"Failed to add new tools to bot_id: {bot_id} with error: {e}")
            return {"success": False, "error": str(e)}


    def db_update_bot_files(self, project_id=None, dataset_name=None, bot_servicing_table=None, bot_id=None, updated_files_str=None, current_files=None, new_file_ids=None):
    # Query to update the files in the database
        update_query = f"""
            UPDATE `{project_id}.{dataset_name}.{bot_servicing_table}`
            SET files = @updated_files
            WHERE bot_id = @bot_id
        """

            # Set the query parameters
        query_params = [
            bigquery.ScalarQueryParameter("bot_id", "STRING", bot_id),
            bigquery.ScalarQueryParameter("updated_files", "STRING", updated_files_str),
        ]

        job_config = bigquery.QueryJobConfig(query_parameters=query_params)

        # Execute the update query
        try:
            self.run_query(query=update_query, job_config=job_config)
            logger.info(f"Successfully updated files for bot_id: {bot_id}")

            return {
                "success": True,
                "message": f"File IDs {json.dumps(new_file_ids)} added to bot_id: {bot_id}.",
                "current_files_list": current_files
            }

        except Exception as e:
            logger.error(f"Failed to add new file to bot_id: {bot_id} with error: {e}")
            return {"success": False, "error": str(e)}


    def db_update_bot_instructions(self, project_id, dataset_name, bot_servicing_table, bot_id, instructions, runner_id):

        # Query to update the bot instructions in the database
        update_query = f"""
            UPDATE `{project_id}.{dataset_name}.{bot_servicing_table}`
            SET bot_instructions = @new_instructions
            WHERE bot_id = @bot_id AND runner_id = @runner_id
        """

        # Set the query parameters
        query_params = [
            bigquery.ScalarQueryParameter("bot_id", "STRING", bot_id),
            bigquery.ScalarQueryParameter("new_instructions", "STRING", instructions),
            bigquery.ScalarQueryParameter("runner_id", "STRING", runner_id),
        ]

        job_config = bigquery.QueryJobConfig(query_parameters=query_params)

        # Execute the update query
        try:
            self.run_query(query=update_query, job_config=job_config)
            logger.info(f"Successfully updated bot_instructions for bot_id: {bot_id}")

            return {
                "Success": True,
                "Message": f"Successfully updated bot_instructions for bot_id: {bot_id}.",
                "new_instructions": instructions
            }

        except Exception as e:
            logger.error(f"Failed to update bot_instructions for bot_id: {bot_id} with error: {e}")
            return {"success": False, "error": str(e)}

    def db_update_bot_implementation(self, project_id, dataset_name, bot_servicing_table, bot_id, bot_implementation, runner_id):
        """
        Updates the bot_implementation field in the BOT_SERVICING table for a given bot_id.

        Args:
            project_id (str): The project ID where the BOT_SERVICING table resides.
            dataset_name (str): The dataset name where the BOT_SERVICING table resides.
            bot_servicing_table (str): The name of the table where bot details are stored.
            bot_id (str): The unique identifier for the bot.
            bot_implementation (str): The new implementation type to be set for the bot (e.g., 'openai', 'cortex').
            runner_id (str): The runner ID associated with the bot.

        Returns:
            dict: A dictionary indicating the success or failure of the operation.
        """

        # Query to update the bot_implementation in the database
        update_query = f"""
            UPDATE `{project_id}.{dataset_name}.{bot_servicing_table}`
            SET bot_implementation = @new_implementation
            WHERE bot_id = @bot_id AND runner_id = @runner_id
        """

        # Set the query parameters
        query_params = [
            bigquery.ScalarQueryParameter("bot_id", "STRING", bot_id),
            bigquery.ScalarQueryParameter("new_implementation", "STRING", bot_implementation),
            bigquery.ScalarQueryParameter("runner_id", "STRING", runner_id),
        ]

        job_config = bigquery.QueryJobConfig(query_parameters=query_params)

        # Execute the update query
        try:
            self.run_query(query=update_query, job_config=job_config)
            logger.info(f"Successfully updated bot_implementation for bot_id: {bot_id}")

            return {
                "success": True,
                "message": f"Successfully updated bot_implementation for bot_id: {bot_id}.",
                "new_implementation": bot_implementation
            }

        except Exception as e:
            logger.error(f"Failed to update bot_implementation for bot_id: {bot_id} with error: {e}")
            return {"success": False, "error": str(e)}

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
            FROM `{project_id}.{dataset_name}.{bot_servicing_table}`
            WHERE bot_id = '{bot_id}'
        """

        try:
            result = self.run_query(query=select_query)
            if result:
                # Assuming the result is a list of dictionaries, we return the first one
                return result[0]
            else:
                logger.error(f"No details found for bot_id: {bot_id}")
                return None
        except Exception as e:
            logger.exception(f"Failed to retrieve details for bot_id: {bot_id} with error: {e}")
            return None

    def db_update_existing_bot(self, api_app_id, bot_id, bot_slack_user_id, client_id, client_secret, slack_signing_secret,
                            auth_url, auth_state, udf_active, slack_active, files, bot_implementation, project_id, dataset_name, bot_servicing_table):
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
            bot_implementation: openai or cortex or ...
        """

        update_query = f"""
            UPDATE `{project_id}.{dataset_name}.{bot_servicing_table}`
            SET api_app_id = @api_app_id, bot_slack_user_id = @bot_slack_user_id, client_id = @client_id, client_secret = @client_secret,
                slack_signing_secret = @slack_signing_secret, auth_url = @auth_url, auth_state = @auth_state,
                udf_active = @udf_active, slack_active = @slack_active, files = @files, bot_implementation = @bot_implementation
            WHERE bot_id = @bot_id
        """

        # Set the query parameters
        query_params = [
            bigquery.ScalarQueryParameter("api_app_id", "STRING", api_app_id),
            bigquery.ScalarQueryParameter("bot_id", "STRING", bot_id),
            bigquery.ScalarQueryParameter("bot_slack_user_id", "STRING", bot_slack_user_id),
            bigquery.ScalarQueryParameter("client_id", "STRING", client_id),
            bigquery.ScalarQueryParameter("client_secret", "STRING", client_secret),
            bigquery.ScalarQueryParameter("slack_signing_secret", "STRING", slack_signing_secret),
            bigquery.ScalarQueryParameter("auth_url", "STRING", auth_url),
            bigquery.ScalarQueryParameter("auth_state", "STRING", auth_state),
            bigquery.ScalarQueryParameter("udf_active", "STRING", udf_active),
            bigquery.ScalarQueryParameter("slack_active", "STRING", slack_active),
            bigquery.ScalarQueryParameter("files", "STRING", files),
            bigquery.ScalarQueryParameter("bot_implementation", "STRING", bot_implementation)
        ]

        job_config = bigquery.QueryJobConfig(query_parameters=query_params)

        try:
            self.run_query(query=update_query, job_config=job_config)
            logger.info(f"Successfully updated existing bot configuration for bot_id: {bot_id}")
        except Exception as e:
            logger.info(f"Failed to update existing bot configuration for bot_id: {bot_id} with error: {e}")
            raise e

    def db_update_bot_details(self, bot_id, bot_slack_user_id, slack_app_token, project_id, dataset_name, bot_servicing_table):
        """
        Updates the BOT_SERVICING table with the new bot_slack_user_id and slack_app_token for the given bot_id.

        Args:
            bot_id (str): The unique identifier for the bot.
            bot_slack_user_id (str): The new Slack user ID for the bot.
            slack_app_token (str): The new Slack app token for the bot.
        """

        # Query to update the bot servicing details
        update_query = f"""
            UPDATE `{project_id}.{dataset_name}.{bot_servicing_table}`
            SET bot_slack_user_id = @bot_slack_user_id, slack_app_token = @slack_app_token
            WHERE bot_id = @bot_id
        """

        # Set the query parameters
        query_params = [
            bigquery.ScalarQueryParameter("bot_id", "STRING", bot_id),
            bigquery.ScalarQueryParameter("bot_slack_user_id", "STRING", bot_slack_user_id),
            bigquery.ScalarQueryParameter("slack_app_token", "STRING", slack_app_token),
        ]

        job_config = bigquery.QueryJobConfig(query_parameters=query_params)

        # Execute the update query
        try:
            self.run_query(query=update_query, job_config=job_config)
            logger.info(f"Successfully updated bot servicing details for bot_id: {bot_id}")
        except Exception as e:
            logger.error(f"Failed to update bot servicing details for bot_id: {bot_id} with error: {e}")
            raise e


    def db_get_available_tools(self, project_id, dataset_name):
        """
        Retrieves the list of available tools and their descriptions from the BigQuery table.

        Returns:
            list of dict: A list of dictionaries, each containing the tool name and description.
        """


        # Query to select the available tools
        select_query = f"""
            SELECT tool_name, tool_description
            FROM `{project_id}.{dataset_name}.available_tools`
        """

        try:
            results = self.run_query(query=select_query)
            tools_list = [{'tool_name': result['tool_name'], 'tool_description': result['tool_description']} for result in results]
            return tools_list
        except Exception as e:
            logger.exception(f"Failed to retrieve available tools with error: {e}")
            return []

    def db_add_or_update_available_tool(self, tool_name, tool_description, project_id, dataset_name):
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
            MERGE `{project_id}.{dataset_name}.available_tools` AS target
            USING (SELECT @tool_name AS tool_name, @tool_description AS tool_description) AS source
            ON target.tool_name = source.tool_name
            WHEN MATCHED THEN
                UPDATE SET tool_description = source.tool_description
            WHEN NOT MATCHED THEN
                INSERT (tool_name, tool_description)
                VALUES (source.tool_name, source.tool_description)
        """

        # Set the query parameters
        query_params = [
            bigquery.ScalarQueryParameter("tool_name", "STRING", tool_name),
            bigquery.ScalarQueryParameter("tool_description", "STRING", tool_description),
        ]

        job_config = bigquery.QueryJobConfig(query_parameters=query_params)

        # Execute the merge query
        try:
            self.run_query(query=merge_query, job_config=job_config)
            logger.info(f"Successfully added or updated tool: {tool_name}")
            return {"success": True, "message": f"Tool '{tool_name}' added or updated successfully."}
        except Exception as e:
            logger.error(f"Failed to add or update tool: {tool_name} with error: {e}")
            return {"success": False, "error": str(e)}


    def db_delete_bot(self, project_id, dataset_name, bot_servicing_table, bot_id):

        # Query to delete the bot from the database table
        delete_query = f"""
            DELETE FROM `{project_id}.{dataset_name}.{bot_servicing_table}`
            WHERE bot_id = @bot_id
        """
        # Set the query parameters
        query_params = [
            bigquery.ScalarQueryParameter("bot_id", "STRING", bot_id),
        ]

        job_config = bigquery.QueryJobConfig(query_parameters=query_params)

        # Execute the delete query
        try:
            self.run_query(query=delete_query, job_config=job_config)
            logger.info(f"Successfully deleted bot with bot_id: {bot_id} from the database.")
        except Exception as e:
            logger.error(f"Failed to delete bot with bot_id: {bot_id} from the database with error: {e}")
            raise e

    def db_get_slack_active_bots(self, runner_id, project_id, dataset_name, bot_servicing_table):

        # Query to select the bots from the BOT_SERVICING table
        select_query = f"""
            SELECT bot_id, api_app_id, slack_app_token
            FROM `{project_id}.{dataset_name}.{bot_servicing_table}`
            WHERE runner_id = '{runner_id}' and slack_active = 'Y'
            """

        try:
            return self.run_query(query=select_query)
        except Exception as e:
            logger.error(f"Failed to get list of bots active on slack for a runner {e}")
            raise e


    def generate_filename_from_last_modified(self, table_id):

        try:
            # Fetch the table
            table = self.client.get_table(table_id)

            # Ensure we have a valid datetime object for `modified`
            if table.modified is None:
                raise ValueError("Table modified time is None. Unable to generate filename.")

            # The `modified` attribute should be a datetime object. Format it.
            last_modified_time = table.modified
            timestamp_str = last_modified_time.strftime("%Y%m%dT%H%M%S") + "Z"

            # Create the filename with the .ann extension
            filename = f"{timestamp_str}.ann"
            metafilename = f"{timestamp_str}.json"
            return filename, metafilename
        except Exception as e:
            # Handle errors: for example, table not found, or API errors
            #logger.info(f"An error occurred: {e}")
            # Return a default filename or re-raise the exception based on your use case
            return "default_filename.ann", "default_metadata.json"


    def fetch_embeddings(self, table_id):

        # Initialize variables
        batch_size = 100
        offset = 0
        total_fetched = 0

        # Initialize lists to store results
        embeddings = []
        table_names = []

        # First, get the total number of rows to set up the progress bar
        total_rows_query = f"""
            SELECT COUNT(*) as total
            FROM `{table_id}`
        """
        total_rows_result = self.client.query(total_rows_query).to_dataframe()
        total_rows = total_rows_result.total[0]

        with tqdm(total=total_rows, desc="Fetching embeddings") as pbar:
            while True:
                # Modify the query to include LIMIT and OFFSET
                query = f"""
                    SELECT qualified_table_name, embedding
                    FROM `{table_id}`
                    LIMIT {batch_size} OFFSET {offset}
                """
                query_job = self.client.query(query)

                # Temporary lists to hold batch results
                temp_embeddings = []
                temp_table_names = []

                for row in query_job:
                    temp_embeddings.append(row.embedding)
                    temp_table_names.append(row.qualified_table_name)

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

        return table_names, embeddings
