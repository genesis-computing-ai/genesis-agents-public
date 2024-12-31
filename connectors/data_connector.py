from   core.bot_os_tools2       import (BOT_ID_IMPLICIT_FROM_CONTEXT, THREAD_ID_IMPLICIT_FROM_CONTEXT,
                                        ToolFuncGroup, ToolFuncParamDescriptor,
                                        gc_tool)
from   core.logging_config      import logger
import os
from   sqlalchemy               import create_engine, text
from   urllib.parse             import quote_plus

from google_sheets.g_sheets     import (
    create_google_sheet,
)
from .connector_helpers import llm_keys_and_types_struct
from .snowflake_connector.snowflake_connector import SnowflakeConnector
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
        from connectors import get_global_db_connector # to avoid circular import
        if not hasattr(self, '_initialized'):  # Check if already initialized
            self.db_adapter = get_global_db_connector()
            self.connections = {}  # Store SQLAlchemy engines
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
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
            cursor.execute(create_connections_table)
            self.db_adapter.client.commit()
        finally:
            cursor.close()

    def add_connection(self, connection_id: str = None, connection_string: str = None,
                      bot_id: str=None, allowed_bot_ids: list = None, thread_id: str = None) -> dict:
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
            allowed_bots_str = ','.join(allowed_bot_ids) if allowed_bot_ids else ''

            # Test new connection first
            # URL encode any special characters in connection string

            # Check if connection_id is the reserved 'snowflake' name
            if connection_id.lower() == 'snowflake':
                return {
                    'success': False,
                    'error': "The connection_id 'snowflake' is reserved. You can connect to Snowflake but please use a different connection_id string."
                }

            engine = create_engine(connection_string)
            with engine.connect() as conn:
                conn.execute(text('SELECT 1'))

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
                        SET connection_string = %s, allowed_bot_ids = %s
                        WHERE connection_id = %s
                        """,
                        (connection_string, allowed_bots_str, connection_id)
                    )
                else:
                    cursor.execute(
                        f"""
                        INSERT INTO {self.db_adapter.schema}.CUST_DB_CONNECTIONS 
                        (connection_id, db_type, connection_string, owner_bot_id, allowed_bot_ids, created_at, updated_at)
                        VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                        """,
                        (connection_id, connection_string.split('://')[0], connection_string, bot_id, allowed_bots_str)
                    )

                self.db_adapter.client.commit()
                self.connections[connection_id] = engine
                return {
                    'success': True
                }

            finally:
                cursor.close()

        except Exception as e:
            logger.error(f"Error adding connection: {str(e)}")
            resp =  {
                'success': False,
                'error': str(e)
            }
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
    ) -> dict:
        """Add thread_id parameter to docstring"""

        # TODO - if connection_id (?) = Snowflake, run run_query
        if connection_id == 'Snowflake':
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
                    "error": "Either a query or a note_id must be provided, but not both, and not neither.",
                }

            if note_id is not None or note_name is not None:
                note_id = '' if note_id is None else note_id
                note_name = '' if note_name is None else note_name
                get_note_query = f"SELECT note_content, note_params, note_type FROM {self.schema}.NOTEBOOK WHERE NOTE_ID = '{note_id}'"
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
                SELECT owner_bot_id, allowed_bot_ids, connection_string
                FROM {self.db_adapter.schema}.CUST_DB_CONNECTIONS 
                WHERE connection_id = %s
                """,
                (connection_id,)
            )
            result = cursor.fetchone()

            if not result:
                raise ValueError(f"Connection {connection_id} not found")

            owner_id, allowed_bots, connection_string = result  # Add connection_string to unpacking
            if not bot_id_override and (bot_id != owner_id and
                (not allowed_bots or bot_id not in allowed_bots.split(','))):
                raise ValueError("Bot does not have permission to access this connection")

            # Execute query using SQLAlchemy
            if connection_id not in self.connections:
                self.connections[connection_id] = create_engine(connection_string)
            engine = self.connections[connection_id]
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

                            query = f"SELECT value from {self.schema}.EXT_SERVICE_CONFIG WHERE ext_service_name = 'g-sheets' AND parameter = 'shared_folder_id' and user = '{self.user}'"
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
                            result = create_google_sheet(self, shared_folder_id['result'], title=f"{export_title}", data=rows )

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
            logger.error(f"Query execution error: {str(e)}")
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
                bot_id="test_bot",
                allowed_bot_ids=["test_bot"]
            )

            if not success or success.get('success') != True:
                raise Exception(f"Failed to add PostgreSQL test connection: {success.get('error', '')}")

            result = self.query_database(
                "test_postgresql",
                "test_bot",
                "SELECT version()"
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
                bot_id="test_bot",
                allowed_bot_ids=["test_bot"]
            )

            if not success or success.get('success') != True:
                raise Exception("Failed to add MySQL test connection")

            result = self.query_database(
                "test_mysql",
                "test_bot",
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
                bot_id="test_bot",
                allowed_bot_ids=["test_bot"]
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
                    return False

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

                return True

            finally:
                cursor.close()

        except Exception as e:
            logger.error(f"Error deleting connection {connection_id}: {str(e)}")
            return False

    def _test(self):
        """
        Run all database connector tests.
        """
        logger.info("Running database connector tests...")
        self._test_postgresql()
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
                               created_at, updated_at
                        FROM {self.db_adapter.schema}.CUST_DB_CONNECTIONS
                        """
                    )
                else:
                    cursor.execute(
                        f"""
                        SELECT connection_id, db_type, owner_bot_id, allowed_bot_ids, 
                               created_at, updated_at
                        FROM {self.db_adapter.schema}.CUST_DB_CONNECTIONS 
                        WHERE owner_bot_id = %s 
                        OR allowed_bot_ids LIKE %s
                        """,
                        (bot_id, f"%{bot_id}%")
                    )

                connections = []
                for row in cursor.fetchall():
                    connections.append({
                        'connection_id': row[0],
                        'db_type': row[1],
                        'owner_bot_id': row[2],
                        'allowed_bot_ids': row[3].split(',') if row[3] else [],
                        'created_at': row[4],
                        'updated_at': row[5]
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
        knowledge_base_path: str = "./kb_vector",
        bot_id: str = None,
        thread_id: str = None,
    ) -> dict:
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
            knowledge_base_path: Path to the knowledge base directory
            bot_id: ID of the bot requesting the metadata search
            thread_id: Optional thread identifier for logging/tracking
        
        Returns:
            Dictionary containing:
            - success: Boolean indicating if operation was successful
            - metadata: List of metadata objects (if successful)
            - error: Error message (if unsuccessful)
        """

        from core.logging_config import logger
        from core.bot_os_memory import BotOsKnowledgeAnnoy_Metadata

        # logger.info(f"Search metadata called with query: {query}, scope: {scope}, top_n: {top_n}, verbosity: {verbosity}")
        try:

            if isinstance(top_n, str):
                try:
                    top_n = int(top_n)
                except ValueError:
                    top_n = 8
            logger.info(
                "Search metadata: query len=",
                len(query),
                " Top_n: ",
                top_n,
                " Verbosity: ",
                verbosity,
            )
            # Adjusted to include scope in the call to find_memory
            # logger.info(f"GETTING NEW ANNOY - Refresh True - --- Search metadata called with query: {query}, scope: {scope}, top_n: {top_n}, verbosity: {verbosity}")
            my_kb = BotOsKnowledgeAnnoy_Metadata(knowledge_base_path, refresh=True)
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
            )
            return result
        except Exception as e:
            logger.error(f"Error in find_memory_openai_callable: {str(e)}")
            return "An error occurred while trying to find the memory."

    def _search_metadata_detailed(
        self,
        query: str,
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

        from core.logging_config import logger
        from core.bot_os_memory import BotOsKnowledgeAnnoy_Metadata

        # logger.info(f"Search metadata called with query: {query}, scope: {scope}, top_n: {top_n}, verbosity: {verbosity}")
        try:

            if isinstance(top_n, str):
                try:
                    top_n = int(top_n)
                except ValueError:
                    top_n = 8
            logger.info(
                "Search metadata_detailed: query len=",
                len(query),
                " Top_n: ",
                top_n,
                " Verbosity: ",
                verbosity,
            )
            # Adjusted to include scope in the call to find_memory
            # logger.info(f"GETTING NEW ANNOY - Refresh True - --- Search metadata called with query: {query}, scope: {scope}, top_n: {top_n}, verbosity: {verbosity}")
            my_kb = BotOsKnowledgeAnnoy_Metadata(knowledge_base_path, refresh=True)
            # logger.info(f"CALLING FIND MEMORY  --- Search metadata called with query: {query}, scope: {scope}, top_n: {top_n}, verbosity: {verbosity}")
            result = my_kb.find_memory(
                query,
                database=database,
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
            return "An error occurred while trying to find the memory."


database_connector_tools = ToolFuncGroup(
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
    max_rows= "Maximum number of rows to return (default 20)",
    max_rows_override= "Override max rows limit if true (default False)",
    bot_id=BOT_ID_IMPLICIT_FROM_CONTEXT,
    thread_id=THREAD_ID_IMPLICIT_FROM_CONTEXT,
    _group_tags_=[database_connector_tools]
    )
def _query_database(connection_id: str,
                    bot_id: str,
                    query: str,
                    params: dict = None,
                    max_rows: int = 20,
                    max_rows_override: bool = False,
                    thread_id: str = None,
                    ) -> dict:
    """
    Query a connected database with SQL

    Returns:
        dict: A dictionary containing the query results or an error message.
    """
    return DatabaseConnector().query_database(
        connection_id=connection_id,
        bot_id=bot_id,
        query=query,
        params=params,
        max_rows=max_rows,
        max_rows_override=max_rows_override,
        thread_id=thread_id
    )


@gc_tool(connection_id= "ID of the database connection to create",
         connection_string= "Full SQLAlchemy connection string.",
         allowed_bot_ids= "List of bot IDs that can access this connection",
         bot_id=BOT_ID_IMPLICIT_FROM_CONTEXT,
         thread_id=THREAD_ID_IMPLICIT_FROM_CONTEXT,
         _group_tags_=[database_connector_tools])
def _add_database_connection(connection_id: str,
                            connection_string: str,
                            bot_id: str,
                            allowed_bot_ids: list[str] = None,
                            thread_id: str = None
                            ) -> dict:
    """
    Add a new named database connection.

    Returns:
        dict: A dictionary containing the result of the connection addition.
    """
    return DatabaseConnector().add_connection(
        connection_id=connection_id,
        connection_string=connection_string,
        bot_id=bot_id,
        allowed_bot_ids=allowed_bot_ids,
        thread_id=thread_id
    )


@gc_tool(
    connection_id= "ID of the database connection to delete",
    bot_id=BOT_ID_IMPLICIT_FROM_CONTEXT,
    thread_id=THREAD_ID_IMPLICIT_FROM_CONTEXT,
    _group_tags_=[database_connector_tools])
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
         _group_tags_=[database_connector_tools],)
def _list_database_connections(bot_id: str,
                               thread_id: str = None
                               ) -> dict:
    '''List all database connections accessible to a bot'''
    return DatabaseConnector().list_database_connections(
        bot_id=bot_id,
        thread_id=thread_id
    )


@gc_tool(
    query='SQL query to execute',
    database='Database name',
    schema='Schema name',
    table='Table name',
    top_n='Number of rows to return',
    knowledge_base_path="Path to the knowledge vector base",
    bot_id=BOT_ID_IMPLICIT_FROM_CONTEXT,
    thread_id=THREAD_ID_IMPLICIT_FROM_CONTEXT,
    _group_tags_=[database_connector_tools],
)
def _search_metadata(
    query: str = None,
    database: str = None,
    schema: str = None,
    table: str = None,
    top_n: int = 8,
    knowledge_base_path: str = "./kb_vector",
    bot_id: str = None,
    thread_id: str = None,
):
    """Search database metadata for tables, columns, and other objects"""
    return DatabaseConnector().search_metadata(
        query=query,
        database=database,
        schema=schema,
        table=table,
        scope="database_metadata",
        top_n=top_n,
        verbosity="low",
        full_ddl="false",
        knowledge_base_path=knowledge_base_path,
        bot_id=bot_id,
        thread_id=thread_id,
    )


@gc_tool(
    query="SQL query to execute",
    database="Database name",
    schema="Schema name",
    table="Table name",
    top_n="Number of rows to return",
    knowledge_base_path="Path to the knowledge vector base",
    bot_id=BOT_ID_IMPLICIT_FROM_CONTEXT,
    thread_id=THREAD_ID_IMPLICIT_FROM_CONTEXT,
    _group_tags_=[database_connector_tools],
)
def _data_explorer(
    query: str = None,
    database: str = None,
    schema: str = None,
    table: str = None,
    top_n: int = 10,
    knowledge_base_path: str = "./kb_vector",
    bot_id: str = None,
    thread_id: str = None,
):
    """Explore data"""
    return DatabaseConnector().search_metadata(
        query=query,
        database=database,
        schema=schema,
        table=table,
        scope="database_metadata",
        top_n=8,
        verbosity="high",
        full_ddl="true",
        knowledge_base_path=knowledge_base_path,
        bot_id=bot_id,
        thread_id=thread_id,
    )


@gc_tool(
    query="SQL query to execute",
    database="Database name",
    schema="Schema name",
    table="Table name",
    top_n="Number of rows to return",
    knowledge_base_path="Path to the knowledge vector base",
    bot_id=BOT_ID_IMPLICIT_FROM_CONTEXT,
    thread_id=THREAD_ID_IMPLICIT_FROM_CONTEXT,
    _group_tags_=[database_connector_tools],
)
def _get_full_table_details(
    query: str = None,
    database: str = None,
    schema: str = None,
    table: str = None,
    top_n: int = 8,
    knowledge_base_path: str = "./kb_vector",
    bot_id: str = None,
    thread_id: str = None,
):
    """Get full table details"""
    return DatabaseConnector().search_metadata_detailed(
        query=query,
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
_all_database_connections_functions = (
    _query_database,
    _add_database_connection,
    _delete_database_connection,
    _list_database_connections,
    _search_metadata,
    _data_explorer,
    _get_full_table_details,
)


# Called from bot_os_tools.py to update the global list of data connection tool functions
def get_database_connections_functions():
    return _all_database_connections_functions
