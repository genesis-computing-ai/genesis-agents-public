from   genesis_bots.core.bot_os_tools2       import (BOT_ID_IMPLICIT_FROM_CONTEXT, THREAD_ID_IMPLICIT_FROM_CONTEXT,
                                        ToolFuncGroup, ToolFuncParamDescriptor,
                                        gc_tool)
from   genesis_bots.core.logging_config      import logger
import os
from   sqlalchemy               import create_engine, text
from   urllib.parse             import quote_plus
import boto3
import time
from datetime import datetime

from genesis_bots.google_sheets.g_sheets     import (
    create_google_sheet_from_export,
)
from genesis_bots.connectors.connector_helpers import llm_keys_and_types_struct
from genesis_bots.connectors.snowflake_connector.snowflake_connector import SnowflakeConnector
# Import moved to __init__ to avoid circular import


class DatabaseConnector:
    """
    DatabaseConnector is a singleton class responsible for managing database connections.

    This class provides methods to add, delete, list, and query database connections. It ensures
    that the necessary tables for storing connection metadata are created and maintained. The
    connections are stored as SQLAlchemy engines, and access control is managed through ownership
    and allowed bot IDs.

    Attributes:
        db_adapter: An adapter for interacting with the database.
        connections: A dictionary to store SQLAlchemy engines.
    """
    _instance = None  # Class variable to hold the single instance

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(DatabaseConnector, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        from genesis_bots.connectors import get_global_db_connector # to avoid circular import
        if not hasattr(self, '_initialized'):  # Check if already initialized
            self.db_adapter = get_global_db_connector()
            self.connections = {}
            self.connection_expiration = {} # Store SQLAlchemy engines
            self._ensure_tables_exist()
            self._initialized = True  # Mark as initialized

    def _ensure_tables_exist(self):
        """Create the necessary tables if they don't exist"""
        cursor = self.db_adapter.client.cursor()
        try:
            # Update DB_CONNECTIONS table to include ownership and access control
            create_connections_table = f"""
            CREATE TABLE IF NOT EXISTS {self.db_adapter.schema}.CUST_DB_CONNECTIONS (
                connection_id VARCHAR(255) PRIMARY KEY,
                db_type VARCHAR(50) NOT NULL,
                connection_string TEXT NOT NULL,
                owner_bot_id VARCHAR(255) NOT NULL,
                allowed_bot_ids TEXT,
                description TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
            cursor.execute(create_connections_table)
            self.db_adapter.client.commit()

            # Check if description column exists
            check_description_query = f"DESCRIBE TABLE {self.db_adapter.schema}.CUST_DB_CONNECTIONS;"
            cursor.execute(check_description_query)
            columns = [col[0] for col in cursor.fetchall()]

            if "DESCRIPTION" not in columns and "description" not in columns:
                # Add description column if it doesn't exist
                alter_table_query = f"ALTER TABLE {self.db_adapter.schema}.CUST_DB_CONNECTIONS ADD COLUMN DESCRIPTION TEXT;"
                cursor.execute(alter_table_query)
                self.db_adapter.client.commit()
                logger.info(f"Column 'DESCRIPTION' added to table {self.db_adapter.schema}.CUST_DB_CONNECTIONS.")
        finally:
            cursor.close()

    def add_connection(self, connection_id: str = None, connection_string: str = None,
                      bot_id: str=None, allowed_bot_ids: list = None, thread_id: str = None, description: str = None) -> dict:
        """
        Add or update a database connection configuration

        Args:
            connection_id: Unique identifier for the connection
            connection_string: Full SQLAlchemy connection string
            bot_id: ID of the bot creating/owning the connection
            allowed_bot_ids: List of bot IDs that can access this connection
            thread_id: Optional thread identifier for logging/tracking
        """
        try:
            processed_conn_string = None
            allowed_bots_str = ','.join(allowed_bot_ids) if isinstance(allowed_bot_ids, list) and allowed_bot_ids else ''

            # Check if allowed_bots_str is empty
            if allowed_bots_str == '':
                return {
                    'success': False,
                    'error': "allowed_bot_ids cannot be empty. Please provide either a list of allowed bot IDs or '*' to allow access to all bots."
                }

            # Decode URL-encoded connection string before testing
            try:
                from urllib.parse import unquote
                connection_string = unquote(connection_string)
            except Exception as e:
                return {
                    'success': False,
                    'error': f"Failed to decode connection string: {str(e)}"
                }

            # Test new connection first
            # URL encode any special characters in connection string
            # Check if description is provided
            if not description:
                return {
                    'success': False,
                    'error': "A description is required when adding a database connection. Please provide a description that explains the purpose and contents of this connection."
                }

            # URL encode special characters in connection string
           # connection_string = quote_plus(decoded_connection_string)

            # Check if connection_id is the reserved 'snowflake' name
            if connection_id.lower() == 'snowflake':
                return {
                    'success': False,
                    'error': "The connection_id 'Snowflake' is reserved. You can connect to Snowflake but please use a different connection_id string."
                }

            # Extract db_type from connection string
            db_type = connection_string.split('://')[0]
            if '+' in db_type:
                # Handle cases like oracle+oracledb:// or postgresql+psycopg2://
                db_type = db_type.split('+')[0]

            processed_conn_string = self.get_connection_string(connection_string)

            engine = create_engine(processed_conn_string)
            conn = engine.connect()
            try:
                # Oracle requires FROM DUAL for simple SELECT statements
                # and needs to be committed to avoid ORA-01000: maximum open cursors exceeded
                if db_type == 'oracle':
                    conn.execute(text('SELECT 1 FROM DUAL'))
                else:
                    conn.execute(text('SELECT 1'))
            finally:
                conn.close()

            # Verify allowed bot IDs exist in BOT_SERVICING table
            if allowed_bots_str and allowed_bots_str != '*':
                bot_ids_to_check = [bid.strip() for bid in allowed_bots_str.split(',')]
                cursor = self.db_adapter.client.cursor()
                try:
                    # Get all valid bot IDs from BOT_SERVICING
                    cursor.execute(
                        f"""
                        SELECT DISTINCT bot_id
                        FROM {self.db_adapter.schema}.BOT_SERVICING
                        """
                    )
                    valid_bot_ids = {row[0] for row in cursor.fetchall()}

                    # Check if all provided bot IDs are valid
                    invalid_bots = [bid for bid in bot_ids_to_check if bid not in valid_bot_ids]
                    if invalid_bots:
                        return {
                            'success': False,
                            'error': f"The following bot IDs are not valid: {', '.join(invalid_bots)}. Valid bot IDs are: {', '.join(valid_bot_ids)}"
                        }
                finally:
                    cursor.close()


            cursor = self.db_adapter.client.cursor()
            try:
                cursor.execute(
                    f"""
                    SELECT owner_bot_id
                    FROM {self.db_adapter.schema}.CUST_DB_CONNECTIONS
                    WHERE connection_id = %s
                    """,
                    (connection_id,)
                )
                existing = cursor.fetchone()

                if existing:
                    existing_owner = existing[0]
                    if existing_owner != bot_id:
                        raise ValueError("Only the owner bot can modify this connection")

                    cursor.execute(
                        f"""
                        UPDATE {self.db_adapter.schema}.CUST_DB_CONNECTIONS
                        SET connection_string = %s, allowed_bot_ids = %s, description = %s
                        WHERE connection_id = %s
                        """,
                        (connection_string, allowed_bots_str, description, connection_id)
                    )
                else:
                    cursor.execute(
                        f"""
                        INSERT INTO {self.db_adapter.schema}.CUST_DB_CONNECTIONS
                        (connection_id, db_type, connection_string, owner_bot_id, allowed_bot_ids, created_at, updated_at, description)
                        VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, %s)
                        """,
                        (connection_id, connection_string.split('://')[0], connection_string, bot_id, allowed_bots_str, description)
                    )

                self.db_adapter.client.commit()
                self.connections[connection_id] = engine
                return {
                    'success': True,
                    'message': f"Connection {connection_id} {'updated' if existing else 'added'} successfully",
                    'connection_string': connection_string,
                    **(None if processed_conn_string is None else {'processed_connection_string': processed_conn_string}),
                    'allowed_bot_ids': allowed_bots_str,
                    'description': description,
                    'note': "Remember: All bots that need access should be in a comma-separated string in allowed_bot_ids if more than one, including yourself if applicable (e.g. 'bot1,bot2'), or wildcard '*' to allow all bots",
                    'reminder': "Consider suggesting to next use the harvester tools _set_harvest_control_data function to add this new database connection and its database to the harvest to enable metadata search and discovery capabilities"
                }

            finally:
                cursor.close()

        except Exception as e:
            logger.error(f"Error adding connection: {str(e)}")
            resp =  {
                'success': False,
                'error': str(e),
                **(None if connection_string is None else {'original_connection_string': connection_string}),
                **(None if processed_conn_string is None else {'processed_connection_string': processed_conn_string})
            }
            if 'redshift' in str(connection_string).lower():
                resp['note'] = "For Redshift with IAM authentication, use format: postgresql+psycopg2://iam@<endpoint>:5439/<database>"
            if '/mnt/data' in connection_string:
                resp['hint'] = "Don't use /mnt/data, just provide the full or relative file path as provided by the user"
            return resp

    def query_database(
        self,
        connection_id: str = None,
        bot_id: str = None,
        query: str = None,
        params: dict = None,
        max_rows: int = 20,
        max_rows_override: bool = False,
        thread_id: str = None,
        bot_id_override: bool = False,
        note_id=None,
        note_name=None,
        note_type=None,
        export_to_google_sheet=False,
        export_title=None,
        database_name=None,
    ) -> dict:
        """Add thread_id parameter to docstring"""

        if connection_id is None and self.db_adapter.source_name == 'Snowflake':
            connection_id = 'Snowflake'

        # TODO - if connection_id (?) = Snowflake, run run_query
        if connection_id == 'Snowflake':
            if self.db_adapter.source_name != 'Snowflake':
                return {
                    "success": False,
                    "error": "Connection 'Snowflake' is not available when running in Sqlite mode. List database connections to see valid connections you can use."
                }
            snowflake_connector = self.db_adapter
            result = snowflake_connector.run_query(
           #     self,
                query=query,
                max_rows=max_rows,
                max_rows_override=False,
                job_config=None,
                bot_id=bot_id,
                connection=connection_id,
                thread_id=thread_id,
                note_id=note_id,
                note_name=note_name,
                note_type=note_type,
                max_field_size=5000,
                export_to_google_sheet=export_to_google_sheet,
                export_title=export_title,
                keep_db_schema=False,
            )
            return result

        try:
            if (query is None and note_id is None) or (query is not None and note_id is not None):
                return {
                    "success": False,
                    "error": "Either a query or a (note_id or note_name) must be provided, but not both, and not neither.",
                }

            if note_id is not None or note_name is not None:
                note_id = '' if note_id is None else note_id
                if note_id == '':
                    note_id = note_name
                note_name = '' if note_name is None else note_name
                if note_name == '':
                    note_name = note_id
                get_note_query = f"SELECT note_content, note_params, note_type FROM {self.db_adapter.schema}.NOTEBOOK WHERE (NOTE_ID = '{note_id}') or (NOTE_NAME = '{note_name}') and BOT_ID='{bot_id}'"
                cursor = self.db_adapter.client.cursor()
                cursor.execute(get_note_query)
                query_cursor = cursor.fetchone()
                print(query_cursor)
                if query_cursor is None:
                    return {
                        "success": False,
                        "error": "Note not found.",
                        }

                query = query_cursor[0]
                note_type = query_cursor[2]

                if note_type != 'sql':
                    raise ValueError(f"Note type must be 'sql' to run sql with the this tool.  This note is type: {note_type}")
        except ValueError as e:
            return {
                "success": False,
                "error": str(e),
            }
        # Check access permissions
        # Remove USERQUERY: prefix if present
        if query.startswith('USERQUERY::'):
            query = query[11:]  # Remove the prefix
        cursor = self.db_adapter.client.cursor()
        try:
            cursor.execute(
                f"""
                SELECT owner_bot_id, allowed_bot_ids, connection_string, db_type
                FROM {self.db_adapter.schema}.CUST_DB_CONNECTIONS
                WHERE connection_id = %s
                """,
                (connection_id,)
            )
            result = cursor.fetchone()

            if not result:
                raise ValueError(f"Connection {connection_id} not found")

            owner_id, allowed_bots, connection_string, db_type = result

            # For postgresql, parse database name from connection string if not provided
            if database_name is None and db_type.lower() == 'postgresql':
                if '/' in connection_string:
                    database_name_in_string = connection_string.split('/')[-1]
                    # Remove port number if present
                    if ':' in database_name_in_string:
                        database_name_in_string = database_name_in_string.split(':')[0]
                    return {
                        "success": False,
                        "error": f"When querying PostgreSQL databases, the database name must be specified using the database_name parameter. The default database is '{database_name_in_string}' but your query may reference a different database in the FROM clause."
                    }
                else:
                    return {
                        "success": False,
                        "error": "When querying PostgreSQL databases, the database name must be specified using the database_name parameter."
                    }

            owner_id, allowed_bots, connection_string, db_type = result  # Add connection_string to unpacking
            if not bot_id_override and (bot_id != owner_id and
                (not allowed_bots or (allowed_bots != '*' and bot_id not in allowed_bots.split(',')))):
                raise ValueError("Bot does not have permission to access this connection")

            # Execute query using SQLAlchemy
            # Create connection id string with database name for postgres
            connection_id_string = connection_id
            if db_type.lower() == 'postgresql' and database_name: # postgres needs seprate connections for each database
                connection_id_string = f"{connection_id}_{database_name}"
            if connection_id_string not in self.connections or (connection_id_string in self.connection_expiration and self.connection_expiration[connection_id_string] < time.time()):
                if db_type == 'sqlite':
                    # Verify SQLite file exists or parent directory is writable
                    db_path = connection_string.split('sqlite:///')[1]
                    if not os.path.exists(db_path):
                        dir_path = os.path.dirname(db_path)
                        if not os.path.exists(dir_path):
                            return {
                                'success': False,
                                'error': f"Directory does not exist for SQLite database: {dir_path}"
                            }
                        if not os.access(dir_path, os.W_OK):
                            return {
                                'success': False,
                                'error': f"No write permission for SQLite database directory: {dir_path}"
                            }
                # Add database name to postgresql connection string if not present
                if connection_string.lower().startswith('postgresql'):
                    if database_name and f'/{database_name}' not in connection_string:
                        # Replace /postgres with actual database name if present
                        if connection_string.endswith('/postgres'):
                            connection_string = connection_string[:-9] + f"/{database_name}"
                        elif connection_string.endswith('/'):
                            connection_string = f"{connection_string}/{database_name}"
                        elif connection_string[-4:].isdigit():
                            connection_string = f"{connection_string[:-4]}/{database_name}"
                try:
                    processed_connection_string = self.get_connection_string(connection_string)
                    self.connections[connection_id_string] = create_engine(processed_connection_string)
                    if processed_connection_string != connection_string:
                        self.connection_expiration[connection_id_string] = time.time() + 3500  # 1 hour
                except Exception as e:
                    error_msg = str(e)
                    if db_type == 'sqlite':
                        error_msg += "\nFor SQLite, ensure:\n" + \
                                   "1. The path is correct\n" + \
                                   "2. The directory exists\n" + \
                                   "3. You have read/write permissions\n" + \
                                   "4. Use 3 slashes for relative paths (sqlite:///path/to/db.sqlite)\n" + \
                                   "5. Use 4 slashes for absolute paths (sqlite:////abs/path/to/db.sqlite)"
                    return {
                        'success': False,
                        'error': error_msg
                    }
            engine = self.connections[connection_id_string]
            with engine.connect() as conn:
                trans = conn.begin()
                try:
                    query = query.replace('```', '')
                    if query.lower().startswith('sql'):
                        query = query[3:].lstrip()
                    query_text = text(query)

                    if params:
                        result = conn.execute(query_text, params)
                    else:
                        result = conn.execute(query_text)

                    if not result.returns_rows:
                        trans.commit()
                        # For non-select queries, return rowcount or 0 if None
                        columns = ['rows_affected']
                        rows = [[result.rowcount if result.rowcount is not None else 0]]

                        response = {
                            'success': True,
                            'columns': columns,
                            'rows': rows,
                            'row_count': len(rows)
                        }
                        if cursor:
                            cursor.close()
                        return response
                    else:
                        columns = list(result.keys())

                        # Fetch all rows to get total count if needed
                        all_rows = result.fetchall()
                        total_row_count = len(all_rows)

                        if export_to_google_sheet:
                            max_rows = 500

                        # Apply max_rows limit unless override is True
                        rows = [list(row) for row in all_rows[:max_rows if not max_rows_override else None]]
                        # Commit transaction and close cursor if it exists
                        trans.commit()
                        if cursor:
                            cursor.close()

                        response = {
                            'success': True,
                            'columns': columns,
                            'rows': rows,
                            'row_count': len(rows)
                        }

                        # Add message if rows were limited
                        if not max_rows_override and total_row_count > max_rows:
                            response['message'] = (
                                f"Results limited to {max_rows} rows out of {total_row_count} total rows. "
                                "Use max_rows parameter to increase limit or set max_rows_override=true to fetch all rows."
                            )
                            response['total_row_count'] = total_row_count

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
                                raise Exception("Missing shared folder ID, please configure the google workspace extension's shared folder ID via Streamlit")

                        if export_to_google_sheet:
                            from datetime import datetime

                            shared_folder_id = get_root_folder_id()
                            timestamp = datetime.now().strftime("%m%d%Y_%H:%M:%S")

                            if export_title is None:
                                export_title = 'Genesis Export'
                            result = create_google_sheet_from_export(self, shared_folder_id['result'], title=f"{export_title}", data=rows )

                            response["result"] = f'Data sent to Google Sheets - Link to folder: {result["folder_url"]} | Link to file: {result["file_url"]}'
                            del response["rows"]

                        return response

                except Exception as e:
                    trans.rollback()
                    return {
                        'success': False,
                        'error': str(e)
                    }

        except Exception as e:
            logger.error(f"Query execution error: {str(e)} ")
            return {
                'success': False,
                'error': str(e)
            }

        finally:
            cursor.close()

    def _test_postgresql(self):
        """Test method specifically for PostgreSQL connections"""
        try:
            # Get credentials from environment variables
            user = os.environ.get("POSTGRES_USER_OVERRIDE", "justin")  # Changed default from postgres to justin
            password = os.environ.get("POSTGRES_PASSWORD_OVERRIDE", "")  # Empty default password for local trust auth
            host = os.environ.get("POSTGRES_HOST_OVERRIDE", "localhost")
            port = os.environ.get("POSTGRES_PORT_OVERRIDE", "5432")
            database = os.environ.get("POSTGRES_DATABASE_OVERRIDE", "postgres")

            # URL encode credentials for connection string
            user = quote_plus(user)
            password = quote_plus(password)

            # For local connections with trust authentication
            test_conn_string = f"postgresql://{user}@{host}:{port}/{database}"
            logger.info(f"Attempting to connect to PostgreSQL at {host}:{port}")

            success = self.add_connection(
                connection_id="test_postgresql",
                connection_string=test_conn_string,
                bot_id="Eve",
                allowed_bot_ids=["Eve"],
                description="Demo PostgreSQL connection"
            )

            if not success or success.get('success') != True:
                raise Exception(f"Failed to add PostgreSQL test connection: {success.get('error', '')}")

            result = self.query_database(
                "test_postgresql",
                "Eve",
                "SELECT version()",
                database_name=database
            )

            if not result['success']:
                raise Exception(f"Failed to query PostgreSQL: {result.get('error')}")

            self._cleanup_test_connection("test_postgresql")
            return True

        except Exception as e:
            logger.error(f"PostgreSQL test connection failed: {str(e)}")
            raise

    def _test_mysql(self):
        """Test method specifically for MySQL connections"""
        try:

            # Get credentials from environment variables
            user = os.environ.get("MYSQL_USER_OVERRIDE", "root")
            password = os.environ.get("MYSQL_PASSWORD_OVERRIDE", "")  # Empty default password for local connections
            host = os.environ.get("MYSQL_HOST_OVERRIDE", "localhost")
            port = os.environ.get("MYSQL_PORT_OVERRIDE", "3306")
            database = os.environ.get("MYSQL_DATABASE_OVERRIDE", "mysql")

            # URL encode credentials for connection string
            user = quote_plus(user)
            password = quote_plus(password)
            test_conn_string = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
            success = self.add_connection(
                connection_id="test_mysql",
                connection_string=test_conn_string,
                bot_id="Eve",
                allowed_bot_ids=["Eve"],
                description="Demo MySQL connection"
            )

            if not success or success.get('success') != True:
                raise Exception("Failed to add MySQL test connection")

            result = self.query_database(
                "test_mysql",
                "Eve",
                "SELECT version()"
            )

            if not result['success']:
                raise Exception(f"Failed to query MySQL: {result.get('error')}")

            self._cleanup_test_connection("test_mysql")
            return True

        except Exception as e:
            logger.error(f"MySQL test connection failed: {str(e)}")
            raise

    def _test_snowflake(self):
        """Test method specifically for Snowflake connections"""
        try:
            # Get credentials from environment variables
            account = os.environ.get("SNOWFLAKE_ACCOUNT_OVERRIDE", "your_account")
            user = os.environ.get("SNOWFLAKE_USER_OVERRIDE", "your_user")
            password = os.environ.get("SNOWFLAKE_PASSWORD_OVERRIDE", "your_password")
            database = os.environ.get("SNOWFLAKE_DATABASE_OVERRIDE", "your_database")
            warehouse = os.environ.get("SNOWFLAKE_WAREHOUSE_OVERRIDE", "your_warehouse")

            # URL encode credentials for connection string
            user = quote_plus(user)
            password = quote_plus(password)
            database = quote_plus(database)
            warehouse = quote_plus(warehouse)

            # Extract account identifier (remove any .snowflakecomputing.com if present)
            account = account.replace('.snowflakecomputing.com', '')

            # Construct connection string with proper account format
            test_conn_string = (
                f"snowflake://{user}:{password}@{account}.snowflakecomputing.com/"
                f"?account={account}&warehouse={warehouse}&database={database}"
            )

            logger.info(f"Attempting to connect to Snowflake with account: {account}")

            success = self.add_connection(
                connection_id="test_snowflake",
                connection_string=test_conn_string,
                bot_id="Eve",
                allowed_bot_ids=["Eve"],
                description="Demo Snowflake connection"
            )

            if not success or success.get('success') != True:
                raise Exception(f"Failed to add Snowflake test connection: {success.get('error', '')}")

            result = self.query_database(
                "test_snowflake",
                "test_bot",
                "SELECT CURRENT_VERSION()"
            )

            if not result['success']:
                raise Exception(f"Failed to query Snowflake: {result.get('error')}")

            self._cleanup_test_connection("test_snowflake")
            return True

        except Exception as e:
            logger.error(f"Snowflake test connection failed: {str(e)}")
            raise

    def _cleanup_test_connection(self, connection_id: str):
        """Helper method to clean up test connections"""
        cursor = self.db_adapter.client.cursor()
        try:
            cursor.execute(
                f"""
                DELETE FROM {self.db_adapter.schema}.CUST_DB_CONNECTIONS
                WHERE connection_id = %s
                """,
                (connection_id,)
            )
            self.db_adapter.client.commit()
            self.connections.pop(connection_id, None)
            self.connection_expiration.pop(connection_id, None)
        finally:
            cursor.close()

    def delete_connection(self, connection_id: str, bot_id: str, thread_id: str = None) -> bool:
        """
        Delete a database connection configuration

        Args:
            connection_id: The ID of the connection to delete
            bot_id: ID of the bot requesting deletion
            thread_id: Optional thread identifier for logging/tracking
        """
        try:
            if connection_id == 'Snowflake':
                return {
                    'success': False,
                    'error': "The native Snowflake connection cannot be removed"
                }
            cursor = self.db_adapter.client.cursor()
            try:
                # Check ownership
                cursor.execute(
                    f"""
                    SELECT owner_bot_id FROM {self.db_adapter.schema}.CUST_DB_CONNECTIONS
                    WHERE connection_id = %s
                    """,
                    (connection_id,)
                )
                result = cursor.fetchone()

                if not result:
                    return {
                        "success": False,
                        "error": f"Connection '{connection_id}' not found"
                    }

                if result[0] != bot_id:
                    raise ValueError("Only the owner bot can delete this connection")

                # Proceed with deletion...
                cursor.execute(
                    f"""
                    DELETE FROM {self.db_adapter.schema}.CUST_DB_CONNECTIONS
                    WHERE connection_id = %s
                    """,
                    (connection_id,)
                )
                self.db_adapter.client.commit()

                if connection_id in self.connections:
                    del self.connections[connection_id]
                    self.connection_expiration.pop(connection_id, None)  # Safely remove if exists
                # Delete related records from harvest_control and harvest_summary
                cursor.execute(
                    f"""
                    SELECT COUNT(*) FROM {self.db_adapter.schema}.HARVEST_CONTROL
                    WHERE source_name = %s
                    """,
                    (connection_id,)
                )
                harvest_control_count = cursor.fetchone()[0]

                cursor.execute(
                    f"""
                    SELECT COUNT(*) FROM {self.db_adapter.schema}.HARVEST_RESULTS
                    WHERE source_name = %s
                    """,
                    (connection_id,)
                )
                harvest_results_count = cursor.fetchone()[0]

                cursor.execute(
                    f"""
                    DELETE FROM {self.db_adapter.schema}.HARVEST_CONTROL
                    WHERE source_name = %s
                    """,
                    (connection_id,)
                )

                cursor.execute(
                    f"""
                    DELETE FROM {self.db_adapter.schema}.HARVEST_RESULTS
                    WHERE source_name = %s
                    """,
                    (connection_id,)
                )
                self.db_adapter.client.commit()

                response = {
                    'success': True,
                    'message': 'Connection deleted successfully'
                }

                if harvest_control_count > 0 or harvest_results_count > 0:
                    response['harvest_data_removed'] = {
                        'harvest_control_records': harvest_control_count,
                        'harvest_results_records': harvest_results_count
                    }

                return response

            finally:
                cursor.close()

        except Exception as e:
            logger.error(f"Error deleting connection {connection_id}: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }

    def _test_redshift(self):
        """Test method specifically for Redshift connections"""
        try:
            # Test IAM auth with serverless
            result = self._test_redshift_connection(
                "postgresql+psycopg2://iam@default-workgroup.730335510242.us-east-2.redshift-serverless.amazonaws.com:5439/dev"
            )
            if not result['success']:
                raise Exception(f"Redshift serverless IAM test failed: {result.get('error')}")

            # Test direct auth with provisioned cluster
            result = self._test_redshift_connection(
                "postgresql+psycopg2://admin:my_password@default-workgroup.730335510242.us-east-2.redshift-serverless.amazonaws.com:5439/dev"
            )
            if not result['success']:
                raise Exception(f"Redshift provisioned direct auth test failed: {result.get('error')}")

            return True

        except Exception as e:
            logger.error(f"Redshift test connection failed: {str(e)}")
            raise


    def _test(self):
        """
        Run all database connector tests.
        """
        logger.info("Running database connector tests...")
        self._test_postgresql()
        self._test_redshift()
        self._test_mysql()
        self._test_snowflake()

        logger.info("All database connector tests completed successfully.")

    def list_database_connections(self, bot_id: str, thread_id: str = None, bot_id_override: bool = False) -> dict:
        """
        List all database connections accessible to a bot

        Args:
            bot_id: ID of the bot requesting the connection list
            thread_id: Optional thread identifier for logging/tracking

        Returns:
            Dictionary containing:
            - success: Boolean indicating if operation was successful
            - connections: List of connection details (if successful)
            - error: Error message (if unsuccessful)
        """
        try:
            cursor = self.db_adapter.client.cursor()
            try:
                if bot_id_override:
                    cursor.execute(
                        f"""
                        SELECT connection_id, db_type, owner_bot_id, allowed_bot_ids,
                               created_at, updated_at, description, connection_string
                        FROM {self.db_adapter.schema}.CUST_DB_CONNECTIONS
                        """
                    )
                else:
                    cursor.execute(
                        f"""
                        SELECT connection_id, db_type, owner_bot_id, allowed_bot_ids,
                               created_at, updated_at, description, connection_string
                        FROM {self.db_adapter.schema}.CUST_DB_CONNECTIONS
                        WHERE owner_bot_id = %s
                        OR allowed_bot_ids = '*'
                        OR allowed_bot_ids = %s
                        OR allowed_bot_ids LIKE %s
                        OR allowed_bot_ids LIKE %s
                        OR allowed_bot_ids LIKE %s
                        """,
                        (bot_id, bot_id, f"{bot_id},%", f"%,{bot_id},%", f"%,{bot_id}")
                    )
                connections = []
                for row in cursor.fetchall():
                    connection = {
                        'connection_id': row[0],
                        'db_type': row[1],
                        'owner_bot_id': row[2],
                        'created_at': str(row[4]),
                        'updated_at': str(row[5]),
                        'description': row[6]
                    }
                    if row[2] == bot_id or bot_id_override:
                        connection['allowed_bot_ids'] = row[3].split(',') if row[3] else []
                        connection['connection_string'] = row[7]
                    connections.append(connection)

                # Add snowflake connection id if not already in the list
                if self.db_adapter.source_name == "Snowflake":
                    snowflake_exists = any(conn['connection_id'] == 'Snowflake' for conn in connections)
                    if not snowflake_exists:
                        connections.append({
                            'connection_id': 'Snowflake',
                            'db_type': 'Snowflake',
                            'owner_bot_id': 'System',
                            'created_at': str(datetime(2025, 1, 1)),
                            'updated_at': str(datetime(2025, 1, 1)),
                            'description': 'Snowflake database connection',
                            'allowed_bot_ids': ['*'],
                            'connection_string': 'Native'
                        })

                return {
                    'success': True,
                    'connections': connections
                }

            finally:
                cursor.close()

        except Exception as e:
            logger.error(f"Error listing connections: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }

    def search_metadata(
        self,
        query: str = None,
        database: str = None,
        schema: str = None,
        table: str = None,
        scope: str = None,
        top_n: int = 10,
        verbosity: str = "low",
        full_ddl: bool = False,
        connection_id: str = None,
        search_string: str = None,
        knowledge_base_path: str = "./kb_vector",
        bot_id: str = None,
        thread_id: str = None,
    ) -> list | dict:
        """
        Search database metadata for tables, columns, and other objects

        Args:
            query: SQL query to execute
            database: Database name
            schema: Schema name
            table: Table name
            scope: database_metadata
            top_n: Number of rows to return
            verbosity: Level of verbosity in the response
            full_ddl: Return full DDL for the table
            connection_id: optional connection ID of the database connection to query
            knowledge_base_path: Path to the knowledge base directory
            bot_id: ID of the bot requesting the metadata search
            thread_id: Optional thread identifier for logging/tracking

        Returns:
            Dictionary containing:
            - success: Boolean indicating if operation was successful
            - metadata: List of metadata objects (if successful)
            - error: Error message (if unsuccessful)
        """
        query = query or search_string

        from genesis_bots.core.logging_config import logger
        from genesis_bots.core.bot_os_memory import BotOsKnowledgeAnnoy_Metadata
        from genesis_bots.core.bot_os import BotOsSession  # Add this import


        # logger.info(f"Search metadata called with query: {query}, scope: {scope}, top_n: {top_n}, verbosity: {verbosity}")
        try:

            if isinstance(top_n, str):
                try:
                    top_n = int(top_n)
                except ValueError:
                    top_n = 8
            logger.info(
                "Search metadata_detailed: query len=",
                len(query) if query else 0,
                " Top_n: ",
                top_n,
                " Verbosity: ",
                verbosity,
            )
            # Adjusted to include scope in the call to find_memory
            # logger.info(f"GETTING NEW ANNOY - Refresh True - --- Search metadata called with query: {query}, scope: {scope}, top_n: {top_n}, verbosity: {verbosity}")
            my_kb = BotOsSession.knowledge_implementations.get(bot_id)
            if my_kb is None:
                # If not found, create new one
                my_kb = BotOsKnowledgeAnnoy_Metadata(knowledge_base_path, refresh=True, bot_id=bot_id)
            else:


                my_kb.refresh_annoy()
            # logger.info(f"CALLING FIND MEMORY  --- Search metadata called with query: {query}, scope: {scope}, top_n: {top_n}, verbosity: {verbosity}")
            result = my_kb.find_memory(
                query,
                database=database,
                schema=schema,
                table=table,
                scope=scope,
                top_n=top_n,
                verbosity=verbosity,
                full_ddl=full_ddl,
                connection_id=connection_id,
            )
            return result
        except Exception as e:
            logger.error(f"Error in find_memory_openai_callable: {str(e)}")
            return {"error": "An error occurred while trying to find the memory."}

    def search_metadata_detailed(
        self,
        query: str = None,
        connection_id: str = None,
        scope="database_metadata",
        database=None,
        schema=None,
        table=None,
        top_n=8,
        verbosity="high",
        full_ddl="true",
        knowledge_base_path="./kb_vector",
        bot_id: str = None,
        thread_id: str = None,
    ):
        """
        Exposes the find_memory function to be callable by OpenAI.
        :param query: The query string to search memories for.
        :return: The search result from find_memory.
        """

        from genesis_bots.core.logging_config import logger
        from genesis_bots.core.bot_os_memory import BotOsKnowledgeAnnoy_Metadata
        from genesis_bots.core.bot_os import BotOsSession

        # logger.info(f"Search metadata called with query: {query}, scope: {scope}, top_n: {top_n}, verbosity: {verbosity}")
        try:

            if isinstance(top_n, str):
                try:
                    top_n = int(top_n)
                except ValueError:
                    top_n = 8
            logger.info(
                "Search metadata_detailed: ",
             #   len(query),
                " Top_n: ",
                top_n,
                " Verbosity: ",
                verbosity,
            )
            # Adjusted to include scope in the call to find_memory
            # logger.info(f"GETTING NEW ANNOY - Refresh True - --- Search metadata called with query: {query}, scope: {scope}, top_n: {top_n}, verbosity: {verbosity}")
            my_kb = BotOsSession.knowledge_implementations.get(bot_id)
            if my_kb is None:
                # If not found, create new one
                my_kb = BotOsKnowledgeAnnoy_Metadata(knowledge_base_path, refresh=True, bot_id=bot_id)
            else:
                my_kb.refresh_annoy()

            # logger.info(f"CALLING FIND MEMORY  --- Search metadata called with query: {query}, scope: {scope}, top_n: {top_n}, verbosity: {verbosity}")
            result = my_kb.find_memory(
                query,
                database=database,
                connection_id=connection_id,
                schema=schema,
                table=table,
                scope=scope,
                top_n=top_n,
                verbosity="high",
                full_ddl="true",
            )
            return result
        except Exception as e:
            logger.error(f"Error in find_memory_openai_callable: {str(e)}")
            return {"error": "An error occurred while trying to find the memory."}

    def get_connection_string(self, conn_string=None):
        """Process any database connection string"""
        # Parse the connection string
        if '://' not in conn_string:
            raise ValueError("Connection string must include protocol (e.g., postgresql://, mysql://, etc.)")

        if conn_string.startswith('redshift'):
            conn_string = 'postgresql+psycopg2' + conn_string[conn_string.index('://'):]
        # Return unmodified connection string if not PostgreSQL
        if not conn_string.startswith('postgresql'):
            return conn_string

        prefix, rest = conn_string.split('://')
        if '@' in rest:
            auth, host_part = rest.split('@')
        else:
            auth = ''
            host_part = rest

        # Extract host and database
        if '/' not in host_part:
            raise ValueError("Connection string must include database name")
        host_port, database = host_part.split('/', 1)
        if '?' in database:
            database = database.split('?')[0]

        # Check if this is a Redshift connection
        is_redshift = any(x in host_part.lower() for x in [
            'redshift-serverless.amazonaws.com',
            'redshift.amazonaws.com'
        ])


        # Handle Redshift IAM authentication
        # Replace <user> with 'iam' for Redshift IAM auth
        if is_redshift and auth.startswith('iam:'):
            auth = 'iam'
        if is_redshift and auth.lower() == 'iam':
            # Get workgroup/cluster name from host
            identifier = host_port.split('.')[0]
            is_serverless = 'redshift-serverless' in host_part.lower()

            try:
                session = boto3.session.Session()
                region = os.getenv('AWS_REGION') or session.region_name

                if is_serverless:
                    client = boto3.client('redshift-serverless', region_name=region)
                    credentials = client.get_credentials(
                        workgroupName=identifier,
                        dbName=database,
                        durationSeconds=3600
                    )
                    username, password = credentials['dbUser'], credentials['dbPassword']
                else:
                    client = boto3.client('redshift', region_name=region)
                    credentials = client.get_cluster_credentials(
                        DbUser='IAM_USER',
                        DbName=database,
                        ClusterIdentifier=identifier,
                        DurationSeconds=3600,
                        AutoCreate=False
                    )
                    username, password = credentials['DbUser'], credentials['DbPassword']

                auth = f"{quote_plus(username)}:{quote_plus(password)}"

            except Exception as e:
                logger.error(f"Error getting AWS credentials: {str(e)}")
                raise ValueError("Could not get AWS credentials for Redshift")

        # Construct connection string with appropriate parameters
        conn_params = []

        # Add SSL parameters based on host
        if prefix.startswith('postgresql'):
            is_local = host_port.startswith('localhost') or host_port.startswith('127.0.0.1')
            if is_local:
                conn_params.append("sslmode=disable")  # Disable SSL for local connections
            else:
                if "sslmode" not in conn_string and "sslmode" not in ' '.join(conn_params):
                    conn_params.append("sslmode=verify-full")
                if "sslrootcert" not in conn_string and "sslrootcert" not in ' '.join(conn_params):
                    conn_params.append("sslrootcert=system")

        # Add any existing parameters from the original connection string
        if '?' in conn_string:
            existing_params = conn_string.split('?')[1]
            if existing_params:
                conn_params.append(existing_params)

        # Build final connection string
        result = f"{prefix}://"
        if auth:
            result += f"{auth}@"
        result += f"{host_port}/{database}"
        if conn_params:
            result += '?' + '&'.join(conn_params)

        return result

    def _test_redshift_connection(self, connection_string: str) -> dict:
        """
        Test a Redshift connection string before adding it to the connections list
        
        Args:
            connection_string: Full SQLAlchemy connection string for Redshift

        Returns:
            dict: Result of connection test with success/error information
        """
        try:
            # Parse connection string to determine Redshift type
            if '://' not in connection_string:
                return {
                    'success': False,
                    'error': "Invalid connection string format. Must include protocol (e.g., postgresql://)"
                }

            host_part = connection_string.split('@')[1].split('/')[0] if '@' in connection_string else connection_string.split('/')[0]

            # Verify this is a Redshift connection
            if not any(x in host_part.lower() for x in [
                'redshift-serverless.amazonaws.com',
                'redshift.amazonaws.com'
            ]):
                return {
                    'success': False,
                    'error': "Not a Redshift connection string. Host should contain 'redshift.amazonaws.com' or 'redshift-serverless.amazonaws.com'"
                }

            # Get processed connection string (handles IAM if needed)
            try:
                final_conn_string = self.get_connection_string(connection_string)
            except Exception as e:
                return {
                    'success': False,
                    'error': f"Failed to process connection string: {str(e)}"
                }

            # Test the connection
            engine = create_engine(final_conn_string)
            with engine.connect() as conn:
                # Test query
                result = conn.execute(text('SELECT version()')).fetchone()
                version = result[0] if result else "Unknown"

                # Get Redshift type
                is_serverless = 'redshift-serverless' in host_part.lower()
                redshift_type = "Redshift Serverless" if is_serverless else "Redshift Provisioned"

                return {
                    'success': True,
                    'message': f"Successfully connected to {redshift_type}",
                    'version': version,
                    'type': redshift_type,
                    'note': "Connection test successful. You can now add this connection using add_database_connection()"
                }

        except Exception as e:
            error_msg = str(e)
            response = {
                'success': False,
                'error': f"Connection test failed: {error_msg}"
            }

            # Add helpful hints based on common errors
            if 'timeout' in error_msg.lower():
                response['hint'] = "Check VPC/Security Group settings and ensure port 5439 is open"
            elif 'password' in error_msg.lower():
                response['hint'] = "Check your credentials or IAM permissions"
            elif 'database' in error_msg.lower():
                response['hint'] = "Verify the database name exists and you have access"

            return response

    # I think this is unused, but leaving it here for now
    def get_db_connections(self) -> dict:
        """Get all database connections metadata"""
        try:
            cursor = self.db_adapter.client.cursor()
            try:
                cursor.execute(
                    f"""
                    SELECT
                        connection_id,
                        db_type,
                        owner_bot_id,
                        allowed_bot_ids,
                        created_at,
                        updated_at,
                        description
                    FROM {self.db_adapter.schema}.CUST_DB_CONNECTIONS
                    ORDER BY created_at DESC
                    """
                )

                columns = ['connection_id', 'db_type', 'owner_bot_id', 'allowed_bot_ids',
                          'created_at', 'updated_at', 'description']
                connections = []

                for row in cursor.fetchall():
                    connection = dict(zip(columns, row))
                    connections.append(connection)

                return {
                    "Success": True,
                    "Data": connections
                }

            finally:
                cursor.close()

        except Exception as e:
            logger.error(f"Error getting database connections: {str(e)}")
            return {
                "Success": False,
                "Error": str(e)
            }


data_connector_tools = ToolFuncGroup(
    name="data_connector_tools",
    description=(
        "Tools for managing and querying database connections, including adding new connections, deleting connections, "
        "listing available connections, and running queries against connected databases"
    ),
    lifetime="PERSISTENT",
)

@gc_tool(
    connection_id= "ID of the database connection to query",
    query= "SQL query to execute",
    params= "Optional parameters for the SQL query",
    max_rows= "Maximum number of rows to return (if not specified, default is 20, note this when considering the results of your queries as there may be more results available)",
    max_rows_override= "Override max rows limit if true (default False)",
    database_name= "Name of the database to query (required when querying postgres)",
    note_id= "Optional note ID to execute a saved query, instead of providing the query text in query param",
    note_name= "Optional note name to execute a saved query",
 #   note_type= "Optional note type to execute a saved query",
    export_to_google_sheet= "Export results to Google Sheets if true (default False)",
    export_title= "Title for the exported Google Sheet",
    bot_id=BOT_ID_IMPLICIT_FROM_CONTEXT,
    thread_id=THREAD_ID_IMPLICIT_FROM_CONTEXT,
    _group_tags_=[data_connector_tools]
    )
def _query_database(connection_id: str,
                    bot_id: str,
                    query: str = None,
                    params: dict = None,
                    max_rows: int = 20,
                    max_rows_override: bool = False,
                    database_name: str = None,
                    thread_id: str = None,
                    note_id: str = None,
                    note_name: str = None,
              #      note_type: str = None,
                    export_to_google_sheet: bool = False,
                    export_title: str = None,
                    ) -> dict:
    """
    Query a connected database with SQL

    Returns:
        dict: A dictionary containing the query results or an error message.  This will be limited to the first max_rows (default 20) rows returned.  If you get 20, there may be more results available with a higher max_rows or more focused query.
    """
    return DatabaseConnector().query_database(
        connection_id=connection_id,
        bot_id=bot_id,
        query=query,
        params=params,
        max_rows=max_rows,
        max_rows_override=max_rows_override,
        thread_id=thread_id,
        database_name=database_name,
        note_id=note_id,
        note_name=note_name,
    #    note_type=note_type,
        export_to_google_sheet=export_to_google_sheet,
        export_title=export_title,
    )


@gc_tool(connection_id= "ID of the database connection to create or update",
         connection_string= "Full SQLAlchemy connection string.",
         allowed_bot_ids= "List of bot IDs that can access this connection. Use '*' to allow all bots access, or provide comma-separated bot IDs (e.g., 'bot1,bot2') for specific access",
         description= "Description of the database connection",
         bot_id=BOT_ID_IMPLICIT_FROM_CONTEXT,
         thread_id=THREAD_ID_IMPLICIT_FROM_CONTEXT,
         _group_tags_=[data_connector_tools])
def _add_database_connection(connection_id: str,
                            connection_string: str,
                            bot_id: str,
                            allowed_bot_ids: list[str] = None,
                            description: str = None,
                            thread_id: str = None
                            ) -> dict:
    """
    Add a new named database connection, or update an existing one

    Returns:
        dict: A dictionary containing the result of the connection addition or update
    """
    return DatabaseConnector().add_connection(
        connection_id=connection_id,
        connection_string=connection_string,
        bot_id=bot_id,
        allowed_bot_ids=allowed_bot_ids,
        description=description,
        thread_id=thread_id
    )


@gc_tool(
    connection_id= "ID of the database connection to delete",
    bot_id=BOT_ID_IMPLICIT_FROM_CONTEXT,
    thread_id=THREAD_ID_IMPLICIT_FROM_CONTEXT,
    _group_tags_=[data_connector_tools])
def _delete_database_connection(
                        connection_id: str,
                        bot_id: str,
                        thread_id: str = None
                        ) -> bool:
    '''Delete an existing named database connection'''
    return DatabaseConnector().delete_connection(
        connection_id=connection_id,
        bot_id=bot_id,
        thread_id=thread_id
    )


@gc_tool(bot_id=BOT_ID_IMPLICIT_FROM_CONTEXT,
         thread_id=THREAD_ID_IMPLICIT_FROM_CONTEXT,
         _group_tags_=[data_connector_tools],)
def _list_database_connections(bot_id: str,
                               thread_id: str = None
                               ) -> dict:
    '''List all database connections accessible to a bot'''
    return DatabaseConnector().list_database_connections(
        bot_id=bot_id,
        thread_id=thread_id
    )


@gc_tool(
    search_string='String to search for in metadata',
    connection_id='ID of the database connection to optionally limit search to',
    database='Database name to optionally limit search to',
    schema='Schema name to optionally limit search to',
    table='Table name to optionally limit search to',
    top_n='Number of rows to return',
  #  knowledge_base_path="Path to the knowledge vector base",
    bot_id=BOT_ID_IMPLICIT_FROM_CONTEXT,
    thread_id=THREAD_ID_IMPLICIT_FROM_CONTEXT,
    _group_tags_=[data_connector_tools],
)
def _search_metadata(
    query: str = None,
    search_string: str = None,
    database: str = None,
    schema: str = None,
    table: str = None,
    top_n: int = 15,
    connection_id: str = None,
    knowledge_base_path: str = "./kb_vector",
    bot_id: str = None,
    thread_id: str = None,
):
    """Search database metadata for tables, columns, and other objects"""
    return DatabaseConnector().search_metadata(
        query=query or search_string,
        database=database,
        schema=schema,
        table=table,
        scope="database_metadata",
        top_n=top_n,
        verbosity="low",
        full_ddl="false",
        connection_id=connection_id,
        knowledge_base_path=knowledge_base_path,
        bot_id=bot_id,
        thread_id=thread_id,
    )


@gc_tool(
    search_string="String to search for in metadata",
    database="Database name (not valid for Sqlite)",
    schema="Schema name (not valid for Sqlite)",
    table="Table name",
    top_n="Number of rows to return",
    #knowledge_base_path="Path to the knowledge vector base",
    connection_id="ID of the database connection to query",
    bot_id=BOT_ID_IMPLICIT_FROM_CONTEXT,
    thread_id=THREAD_ID_IMPLICIT_FROM_CONTEXT,
    _group_tags_=[data_connector_tools],
)
def _data_explorer(
    search_string: str = None,
    database: str = None,
    schema: str = None,
    table: str = None,
    top_n: int = 10,
    knowledge_base_path: str = "./kb_vector",
    connection_id: str = None,
    bot_id: str = None,
    thread_id: str = None,
):
    """Fetch information about various database objects"""
    return DatabaseConnector().search_metadata(
        search_string=search_string,
        database=database,
        schema=schema,
        table=table,
        scope="database_metadata",
        top_n=top_n,
        verbosity="high",
        full_ddl="true",
        connection_id=connection_id,
        knowledge_base_path=knowledge_base_path,
        bot_id=bot_id,
        thread_id=thread_id,
    )


@gc_tool(
 #   query="SQL query to execute",
    connection_id="ID of the database connection",
    database="Database name",
    schema="Schema name",
    table="Table name",
   # top_n="Number of rows to return",
  #  knowledge_base_path="Path to the knowledge vector base",
    bot_id=BOT_ID_IMPLICIT_FROM_CONTEXT,
    thread_id=THREAD_ID_IMPLICIT_FROM_CONTEXT,
    _group_tags_=[data_connector_tools],
)
def _get_full_table_details(
    connection_id: str = None,
    query: str = None,
    database: str = None,
    schema: str = None,
    table: str = None,
    top_n: int = 10,
    knowledge_base_path: str = "./kb_vector",
    bot_id: str = None,
    thread_id: str = None,
):
    """Get full table details about a specific table"""
    return DatabaseConnector().search_metadata_detailed(
        query=query,
        connection_id=connection_id,
        database=database,
        schema=schema,
        table=table,
        scope="database_metadata",
        top_n=top_n,
        verbosity="high",
        full_ddl="false",
        knowledge_base_path=knowledge_base_path,
        bot_id=bot_id,
        thread_id=thread_id,
    )

# holds the list of all data connection tool functions
# NOTE: Update this list when adding new data connection tools (TODO: automate this by scanning the module?)
_all_data_connections_functions = [
    _query_database,
    _add_database_connection,
    _delete_database_connection,
    _list_database_connections,
    _search_metadata,
    _data_explorer,
    _get_full_table_details,
]


# Called from bot_os_tools.py to update the global list of data connection tool functions
def get_data_connections_functions():
    return _all_data_connections_functions


if __name__ == "__main__":
    DatabaseConnector()._test()

