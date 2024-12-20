from sqlalchemy import create_engine, text
import json
import os 
from datetime import datetime
from core.logging_config import logger
from urllib.parse import quote_plus

CUSTOMER_DATABASE_TOOL_DEFINITIONS = [
    {
        "type": "function",
        "function": {
            "name": "_query_database",
            "description": "Query a connected database with SQL",
            "parameters": {
                "type": "object",
                "properties": {
                    "connection_id": {
                        "type": "string",
                        "description": "ID of the database connection to query"
                    },
                    "query": {
                        "type": "string",
                        "description": "SQL query to execute"
                    },
                    "params": {
                        "type": "object",
                        "description": "Optional parameters for the SQL query",
                        "optional": True
                    },
                    "max_rows": {
                        "type": "integer",
                        "description": "Maximum number of rows to return (default 20)",
                        "optional": True
                    },
                    "max_rows_override": {
                        "type": "boolean",
                        "description": "Override max rows limit if true",
                        "optional": True
                    }
                },
                "required": ["connection_id", "query"]
            }
        }
    },
    {
        "type": "function", 
        "function": {
            "name": "_add_connection",
            "description": "Add a new database connection",
            "parameters": {
                "type": "object",
                "properties": {
                    "connection_id": {
                        "type": "string",
                        "description": "Unique identifier for the connection"
                    },
                    "connection_string": {
                        "type": "string",
                        "description": "Full SQLAlchemy connection string"
                    },
                    "allowed_bot_ids": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "List of bot IDs that can access this connection",
                        "optional": True
                    }
                },
                "required": ["connection_id", "connection_string"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "_delete_connection",
            "description": "Delete an existing database connection",
            "parameters": {
                "type": "object",
                "properties": {
                    "connection_id": {
                        "type": "string",
                        "description": "ID of the connection to delete"
                    },
                },
                "required": ["connection_id"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "_list_connections",
            "description": "List all database connections accessible to a bot",
            "parameters": {
                "type": "object",
                "properties": {},
                "required": []
            }
        }
    }
]


customer_data_tools = {
                "_query_database": "tool_belt.customer_data_connector_query_database",
                "_add_connection": "tool_belt.customer_data_connector_add_connection", 
                "_delete_connection": "tool_belt.customer_data_connector_delete_connection",
                "_list_connections": "tool_belt.customer_data_connector_list_database_connections"
            }


bot_dispatch_tools = {"_delegate_work": "tool_belt.delegate_work"}



customer_data_tools_overall_description = (
    "customer_data_tools",
    "Tools for managing and querying customer database connections, including adding new connections, deleting connections, listing available connections, and running queries against connected databases"
)

# Import tools_data from core.bot_os_tool_descriptions
from core.bot_os_tool_descriptions import tools_data

# Only append if not already present
if not any(tool[0] == "customer_data_tools" for tool in tools_data):
    tools_data.append(
        (
            "customer_data_tools",
            "Tools for managing and querying customer database connections, including adding new connections, deleting connections, listing available connections, and running queries against connected databases"
        )
    )

class CustomerDataConnector:
    # make it so we dont need anything specific for each db type

    def __init__(self, db_adapter):
        self.db_adapter = db_adapter  # For metadata storage
        self.connections = {}  # Store SQLAlchemy engines
        if not hasattr(CustomerDataConnector, '_tables_initialized'):
            CustomerDataConnector._tables_initialized = True
            self._ensure_tables_exist()
  #          self._test()

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

    def add_connection(self, connection_id: str, connection_string: str, 
                      bot_id: str, allowed_bot_ids: list = None, thread_id: str = None) -> dict:
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
            return {
                'success': False,
                'error': str(e)
            }

    def query_database(self, connection_id: str, bot_id: str, 
                      query: str, params: dict = None, 
                      max_rows: int = 20, max_rows_override: bool = False,
                      thread_id: str = None) -> dict:
        """Add thread_id parameter to docstring"""
        try:
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
                if (bot_id != owner_id and 
                    (not allowed_bots or bot_id not in allowed_bots.split(','))):
                    raise ValueError("Bot does not have permission to access this connection")
                
                # Execute query using SQLAlchemy
                if connection_id not in self.connections:
                    self.connections[connection_id] = create_engine(connection_string)
                engine = self.connections[connection_id]
                with engine.connect() as conn:
                    trans = conn.begin()
                    try:
                        result = conn.execute(text(query), params or {})
                        
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
                            
                            return response

                    except Exception as e:
                        trans.rollback()
                        return {
                            'success': False,
                            'error': str(e)
                        }

            finally:
                cursor.close()

        except Exception as e:
            logger.error(f"Query execution error: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }

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

    def list_database_connections(self, bot_id: str, thread_id: str = None) -> dict:
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

    def get_tool_definitions(self):
        """Get the LLM tool definitions for the database connector tools"""
        return DATABASE_TOOL_DEFINITIONS

