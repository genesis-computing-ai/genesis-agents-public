from sqlalchemy import create_engine, text
import json
import os 
from datetime import datetime
from core.logging_config import logger

class CustomerDataConnector:
    # make it so we dont need anything specific for each db type

    def __init__(self, db_adapter):
        self.db_adapter = db_adapter  # For metadata storage
        self.connections = {}  # Store SQLAlchemy engines
        self._ensure_tables_exist()

    def _ensure_tables_exist(self):
        """Create the necessary tables if they don't exist"""
        cursor = self.db_adapter.client.cursor()
        try:
            # Create DB_CONNECTIONS table in metadata
            create_connections_table = f"""
            CREATE TABLE IF NOT EXISTS {self.db_adapter.schema}.DB_CONNECTIONS (
                connection_id VARCHAR(255) PRIMARY KEY,
                db_type VARCHAR(50) NOT NULL,
                connection_params TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
            cursor.execute(create_connections_table)
            self.db_adapter.client.commit()
        finally:
            cursor.close()

    def add_connection(self, connection_id: str, db_type: str, connection_params: dict) -> bool:
        """
        Add or update a database connection configuration in metadata
        """
        try:
            # Test new connection first
            conn_string = self._create_connection_string(db_type, connection_params)
            engine = create_engine(conn_string)
            with engine.connect() as conn:
                conn.execute(text('SELECT 1'))
            
            # Check if connection exists and compare values
            cursor = self.db_adapter.client.cursor()
            try:
                cursor.execute(
                    f"""
                    SELECT db_type, connection_params 
                    FROM {self.db_adapter.schema}.DB_CONNECTIONS 
                    WHERE connection_id = %s
                    """,
                    (connection_id,)
                )
                existing = cursor.fetchone()
                
                if existing:
                    existing_type, existing_params = existing
                    existing_params = json.loads(existing_params)
                    
                    # If values are different, delete the old connection
                    if existing_type != db_type or existing_params != connection_params:
                        cursor.execute(
                            f"""
                            DELETE FROM {self.db_adapter.schema}.DB_CONNECTIONS 
                            WHERE connection_id = %s
                            """,
                            (connection_id,)
                        )
                        # Insert new values
                        cursor.execute(
                            f"""
                            INSERT INTO {self.db_adapter.schema}.DB_CONNECTIONS 
                            (connection_id, db_type, connection_params) 
                            VALUES (%s, %s, %s)
                            """,
                            (connection_id, db_type, json.dumps(connection_params))
                        )
                else:
                    # No existing connection, just insert
                    cursor.execute(
                        f"""
                        INSERT INTO {self.db_adapter.schema}.DB_CONNECTIONS 
                        (connection_id, db_type, connection_params) 
                        VALUES (%s, %s, %s)
                        """,
                        (connection_id, db_type, json.dumps(connection_params))
                    )
                
                self.db_adapter.client.commit()
                
                # Store/update engine in memory
                self.connections[connection_id] = engine
                return True
                
            finally:
                cursor.close()
                
        except Exception as e:
            print(f"Error adding connection: {str(e)}")
            return False

    def query_database(self, connection_id: str, query: str, params: dict = None, 
                      max_rows: int = 20, max_rows_override: bool = False) -> dict:
        """
        Execute a query on the specified database connection
        """
        try:
            if connection_id not in self.connections:
                # Load connection from metadata
                cursor = self.db_adapter.client.cursor()
                try:
                    cursor.execute(
                        f"""
                        SELECT * FROM {self.db_adapter.schema}.DB_CONNECTIONS 
                        WHERE connection_id = %s
                        """,
                        (connection_id,)
                    )
                    result = cursor.fetchone()
                    
                    if result:
                        # Assuming columns are in order: connection_id, db_type, connection_params, created_at, updated_at
                        conn_string = self._create_connection_string(
                            result[1],  # db_type
                            json.loads(result[2])  # connection_params
                        )
                        self.connections[connection_id] = create_engine(conn_string)
                    else:
                        raise ValueError(f"Connection {connection_id} not found")
                finally:
                    cursor.close()

            # Execute query using SQLAlchemy
            engine = self.connections[connection_id]
            with engine.connect() as conn:
                result = conn.execute(text(query), params or {})
                rows = result.fetchmany(max_rows if not max_rows_override else None)
                columns = result.keys()
                
                return {
                    'success': True,
                    'columns': columns,
                    'rows': rows,
                    'row_count': len(rows)
                }

        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }

    def _create_connection_string(self, db_type: str, params: dict) -> str:
        """
        Create SQLAlchemy connection string based on database type and parameters
        """
        CONNECTION_TEMPLATES = {
            'postgresql': {
                'template': "postgresql://{user}:{password}@{host}:{port}/{database}",
                'required_params': ['user', 'password', 'host', 'port', 'database']
            },
            'mysql': {
                'template': "mysql+pymysql://{user}:{password}@{host}:{port}/{database}",
                'required_params': ['user', 'password', 'host', 'port', 'database']
            },
            'snowflake': {
                'template': "snowflake://{user}:{password}@{account}",
                'required_params': ['user', 'password', 'account']
            }
        }
        
        if db_type not in CONNECTION_TEMPLATES:
            raise ValueError(f"Unsupported database type: {db_type}")
        
        config = CONNECTION_TEMPLATES[db_type]
        
        # Verify all required parameters are present
        missing_params = [param for param in config['required_params'] if param not in params]
        if missing_params:
            raise ValueError(f"Missing required parameters for {db_type}: {', '.join(missing_params)}")
        
        # Create base connection string
        conn_str = config['template'].format(**params)
        
        # Handle Snowflake's additional parameters
        if db_type == 'snowflake':
            # Add all other params as URL parameters
            other_params = {k:v for k,v in params.items() if k not in ['user', 'password', 'account']}
            if other_params:
                param_strings = [f"{k}={v}" for k,v in other_params.items()]
                conn_str += '?' + '&'.join(param_strings)
        
        return conn_str

    def _test_postgresql(self):
        """
        Test method specifically for PostgreSQL connections
        """
        import getpass
        username = getpass.getuser()
        try:
            test_params = {
                "host": "localhost", 
                "port": 5432,
                "database": "postgres",  # default database that always exists
                "user": username,        # your username is the superuser
                "password": ""           # local connections often don't need password on macOS
            }
            
            # Test connection string creation
            test_conn_string = self._create_connection_string("postgresql", test_params)
            if not test_conn_string:
                raise Exception("Failed to create connection string")
            
            # Add the test connection
            success = self.add_connection(
                connection_id="test_postgresql",
                db_type="postgresql", 
                connection_params=test_params
            )
            
            if not success:
                raise Exception("Failed to add PostgreSQL test connection")

            # Test a simple query
            result = self.query_database(
                "test_postgresql",
                "SELECT version()"
            )
            
            if not result['success']:
                raise Exception(f"Failed to query PostgreSQL: {result.get('error')}")

            # Clean up test connection
            cursor = self.db_adapter.client.cursor()
            try:
                cursor.execute(
                    f"""
                    DELETE FROM {self.db_adapter.schema}.DB_CONNECTIONS 
                    WHERE connection_id = 'test_postgresql'
                    """
                )
                self.db_adapter.client.commit()
                # Also remove from memory cache
                self.connections.pop('test_postgresql', None)
            finally:
                cursor.close()

            return True

        except Exception as e:
            print(f"PostgreSQL test connection failed: {str(e)}")
            raise


    def _test_mysql(self):
        """
        Test method specifically for MySQL connections
        """
        try:
            test_params = {
                "host": "localhost", 
                "port": 3306,
                "database": "mysql",  # default database that always exists
                "user": "root",       # default superuser
                "password": ""        # your root password if set
            }
            
            # Test connection string creation
            test_conn_string = self._create_connection_string("mysql", test_params)
            if not test_conn_string:
                raise Exception("Failed to create connection string")
            
            # Add the test connection
            success = self.add_connection(
                connection_id="test_mysql",
                db_type="mysql", 
                connection_params=test_params
            )
            
            if not success:
                raise Exception("Failed to add MySQL test connection")

            # Test a simple query
            result = self.query_database(
                "test_mysql",
                "SELECT version()"
            )
            
            if not result['success']:
                raise Exception(f"Failed to query MySQL: {result.get('error')}")

            # Clean up test connection
            cursor = self.db_adapter.client.cursor()
            try:
                cursor.execute(
                    f"""
                    DELETE FROM {self.db_adapter.schema}.DB_CONNECTIONS 
                    WHERE connection_id = 'test_mysql'
                    """
                )
                self.db_adapter.client.commit()
                # Also remove from memory cache
                self.connections.pop('test_mysql', None)
            finally:
                cursor.close()

            return True

        except Exception as e:
            print(f"MySQL test connection failed: {str(e)}")
            raise

    def _test_snowflake(self):
        """
        Test method specifically for Snowflake connections
        """
        try:
            test_params = {
                "account": os.environ.get("SNOWFLAKE_ACCOUNT_OVERRIDE", "your_account"),
                "user": os.environ.get("SNOWFLAKE_USER_OVERRIDE", "your_user"), 
                "password": os.environ.get("SNOWFLAKE_PASSWORD_OVERRIDE", "your_password"),
                "database": os.environ.get("SNOWFLAKE_DATABASE_OVERRIDE", "your_database"), 
                "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE_OVERRIDE", "your_warehouse"),
                "role": os.environ.get("SNOWFLAKE_ROLE_OVERRIDE", "your_role")
            }
            
            # Test connection string creation
            test_conn_string = self._create_connection_string("snowflake", test_params)
            if not test_conn_string:
                raise Exception("Failed to create connection string")
            
            # Add the test connection
            success = self.add_connection(
                connection_id="test_snowflake",
                db_type="snowflake", 
                connection_params=test_params
            )
            
            if not success:
                raise Exception("Failed to add Snowflake test connection")

            # Test a simple query
            result = self.query_database(
                "test_snowflake",
                "SELECT CURRENT_VERSION()"
            )
            
            if not result['success']:
                raise Exception(f"Failed to query Snowflake: {result.get('error')}")

            # Clean up test connection
            cursor = self.db_adapter.client.cursor()
            try:
                cursor.execute(
                    f"""
                    DELETE FROM {self.db_adapter.schema}.DB_CONNECTIONS 
                    WHERE connection_id = 'test_snowflake'
                    """
                )
                self.db_adapter.client.commit()
                # Also remove from memory cache
                self.connections.pop('test_snowflake', None)
            finally:
                cursor.close()

            return True

        except Exception as e:
            print(f"Snowflake test connection failed: {str(e)}")
            raise

    def delete_connection(self, connection_id: str) -> bool:
        """
        Delete a database connection configuration from metadata and memory
        
        Args:
            connection_id: The ID of the connection to delete
            
        Returns:
            bool: True if deletion was successful, False otherwise
        """
        try:
            cursor = self.db_adapter.client.cursor()
            try:
                # Delete from metadata DB
                cursor.execute(
                    f"""
                    DELETE FROM {self.db_adapter.schema}.DB_CONNECTIONS 
                    WHERE connection_id = %s
                    """,
                    (connection_id,)
                )
                self.db_adapter.client.commit()
                
                # Remove from memory if it exists
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
     #   self._test_postgresql()
     #   self._test_mysql()
        self._test_snowflake()
   
        logger.info("All database connector tests completed successfully.")



