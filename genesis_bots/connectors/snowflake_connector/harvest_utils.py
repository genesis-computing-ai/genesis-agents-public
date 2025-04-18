import os
import json
from datetime import datetime
from genesis_bots.core.logging_config import logger

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
            from datetime import datetime
            if hasattr(genesis_app, 'scheduler'):
                genesis_app.scheduler.modify_job(
                    'harvester_job',
                    next_run_time=datetime.now()
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
