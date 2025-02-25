import sqlite3
import re
import logging
from typing import Any
from datetime import datetime
from genesis_bots.core.logging_config import logger
import os
from pathlib import Path

from genesis_bots.core.bot_os_defaults import (
    BASE_EVE_BOT_INSTRUCTIONS,
    BASE_EVE_BOT_AVAILABLE_TOOLS,
    EVE_INTRO_PROMPT,
)


class SQLiteAdapter:
    """Adapts Snowflake-style operations to work with SQLite"""

    # Class-level flag to track if tables have been initialized
    _tables_initialized = False

    def __init__(self, db_path="genesis.db"):
        logger.info(f"Initializing SQLiteAdapter with db_path: {db_path}")
        self.db_path = db_path

        # Test database connection and write permissions
        try:
            if os.path.dirname(db_path):
                os.makedirs(os.path.dirname(db_path), exist_ok=True)
            self.connection = sqlite3.connect(db_path, check_same_thread=False)
            # Try to create and drop a test table
            with self.connection:
                self.connection.execute("CREATE TABLE IF NOT EXISTS _test_table (id INTEGER PRIMARY KEY)")
                self.connection.execute("DROP TABLE _test_table")
            logger.info("Database connection and write permissions verified")
        except sqlite3.Error as e:
            logger.error(f"Failed to initialize database connection: {e}")
            logger.error(f"location of db_path: {db_path}")
            raise Exception(f"Database initialization failed: {e}")

        # Ensure tables exist only once per class
        if not SQLiteAdapter._tables_initialized:
            try:
                self._ensure_bot_servicing_table()
                self._ensure_llm_tokens_table()
                self._ensure_slack_config_table()
                self._ensure_harvest_control_table()
                self._ensure_harvest_results_table()
                self._ensure_cust_db_connections_table()
                #self.export_harvest()                # Check and insert each connection if it doesn't exist

                SQLiteAdapter._tables_initialized = True
                logger.info("All tables initialized successfully")
            except Exception as e:
                logger.error(f"Failed to initialize tables: {e}")
                raise



    def _ensure_bot_servicing_table(self):
        """Ensure BOT_SERVICING table exists with correct constraints"""
        logger.info("Starting BOT_SERVICING table verification")
        cursor = self.connection.cursor()

        try:
            # First verify if table exists
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='BOT_SERVICING'")
            exists = cursor.fetchone() is not None
            logger.info(f"BOT_SERVICING table exists: {exists}")
            if not exists:
                logger.info("Creating BOT_SERVICING table")
                try:
                    # Use a with block for automatic transaction management
                    with self.connection:
                        # Create table with BOT_ID as PRIMARY KEY
                        create_table_sql = """
                            CREATE TABLE BOT_SERVICING (
                                BOT_ID TEXT PRIMARY KEY NOT NULL,  -- Explicitly make BOT_ID PRIMARY KEY and NOT NULL
                                API_APP_ID TEXT,
                                RUNNER_ID TEXT,
                                BOT_NAME TEXT,
                                BOT_INSTRUCTIONS TEXT,
                                AVAILABLE_TOOLS TEXT,
                                UDF_ACTIVE TEXT,
                                SLACK_ACTIVE TEXT,
                                BOT_INTRO_PROMPT TEXT,
                                BOT_AVATAR_IMAGE TEXT,
                                SLACK_APP_TOKEN TEXT,
                                SLACK_APP_LEVEL_KEY TEXT,
                                SLACK_SIGNING_SECRET TEXT,
                                SLACK_CHANNEL_ID TEXT,
                                FILES TEXT,
                                SLACK_USER_ALLOW TEXT,
                                TEAMS_ACTIVE INTEGER,
                                TEAMS_APP_ID TEXT,
                                TEAMS_APP_PASSWORD TEXT,
                                TEAMS_APP_TYPE TEXT,
                                TEAMS_APP_TENANT_ID TEXT,
                                BOT_SLACK_USER_ID TEXT,
                                BOT_IMPLEMENTATION TEXT,
                                AUTH_URL TEXT,
                                AUTH_STATE TEXT,
                                CLIENT_ID TEXT,
                                CLIENT_SECRET TEXT
                            )
                        """

                        # Drop the table if it exists (to ensure clean creation)
                        self.connection.execute("DROP TABLE IF EXISTS BOT_SERVICING")
                        self.connection.execute(create_table_sql)
                        self.connection.commit()

                        # Verify the primary key constraint
                        cursor.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='BOT_SERVICING'")
                        table_def = cursor.fetchone()[0]
                        #logger.info(f"Created BOT_SERVICING table with definition: {table_def}")

                        # Verify the primary key constraint exists
                        cursor.execute("PRAGMA table_info(BOT_SERVICING)")
                        columns = cursor.fetchall()
                        pk_columns = [col for col in columns if col[5] > 0]  # Column 5 is the pk flag
                       # logger.info(f"Primary key columns: {pk_columns}")

                except Exception as e:
                    logger.error(f"Error during table creation: {e}")
                    raise

            # Final verification
            cursor.execute("SELECT COUNT(*) FROM BOT_SERVICING")
            c = cursor.fetchone()
            if c:
                count = c[0]
            else:
                count = 0
            logger.info(f"Final verification successful. Row count: {count}")

            # After successful table creation, insert Eve
            if not exists:
                logger.info("Inserting initial Eve row")
                runner_id = os.getenv("RUNNER_ID", "jl-local-runner")
                bot_id = "Eve"
                bot_name = "Eve"
                bot_instructions = BASE_EVE_BOT_INSTRUCTIONS
                available_tools = BASE_EVE_BOT_AVAILABLE_TOOLS

                udf_active = 'Y'  # Using 1 instead of "Y" for SQLite boolean
                slack_active = 'N'  # Using 0 instead of "N" for SQLite boolean
                bot_intro_prompt = EVE_INTRO_PROMPT

                # Insert Eve using SQLite's UPSERT syntax
                insert_eve_query = """
                INSERT INTO BOT_SERVICING
                    (BOT_ID, RUNNER_ID, BOT_NAME, BOT_INSTRUCTIONS,
                    AVAILABLE_TOOLS, UDF_ACTIVE, SLACK_ACTIVE, BOT_INTRO_PROMPT)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(BOT_ID) DO UPDATE SET
                    RUNNER_ID = excluded.RUNNER_ID,
                    BOT_NAME = excluded.BOT_NAME,
                    BOT_INSTRUCTIONS = excluded.BOT_INSTRUCTIONS,
                    AVAILABLE_TOOLS = excluded.AVAILABLE_TOOLS,
                    UDF_ACTIVE = excluded.UDF_ACTIVE,
                    SLACK_ACTIVE = excluded.SLACK_ACTIVE,
                    BOT_INTRO_PROMPT = excluded.BOT_INTRO_PROMPT
                """

                cursor.execute(
                    insert_eve_query,
                    (
                        bot_id,
                        runner_id,
                        bot_name,
                        bot_instructions,
                        available_tools,
                        udf_active,
                        slack_active,
                        bot_intro_prompt,
                    )
                )
                self.connection.commit()
                logger.info("Initial Eve row inserted successfully")

        except Exception as e:
            logger.error(f"Error in _ensure_bot_servicing_table: {e}")
            raise

    def _ensure_llm_tokens_table(self):
        """Ensure llm_tokens table exists with correct constraints"""
        cursor = self.connection.cursor()
        # First drop the table to ensure clean creation
        # cursor.execute("DROP TABLE IF EXISTS llm_tokens")
        # self.commit()

        # Create the table with explicit constraints
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS llm_tokens (
                runner_id TEXT NOT NULL,
                llm_key TEXT,
                llm_type TEXT,
                model_name TEXT,
                active INTEGER DEFAULT 1,
                llm_endpoint TEXT,
                created_at DATETIME DEFAULT (strftime('%Y-%m-%d %H:%M:%S', 'now')),
                updated_at DATETIME DEFAULT (strftime('%Y-%m-%d %H:%M:%S', 'now')),
                embedding_model_name TEXT,
                PRIMARY KEY (runner_id)
            )
        """)
        self.commit()

    def _ensure_slack_config_table(self):
        """Ensure slack_app_config_tokens table exists"""
        cursor = self.connection.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS slack_app_config_tokens (
                runner_id TEXT PRIMARY KEY,
                slack_app_config_token TEXT,
                slack_app_config_refresh_token TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        self.commit()

    def _ensure_cust_db_connections_table(self):
        """Ensure CUST_DB_CONNECTIONS table exists with correct schema"""
        logger.info("Starting CUST_DB_CONNECTIONS table verification")
        cursor = self.connection.cursor()

        # Check if table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='CUST_DB_CONNECTIONS'")
        exists = cursor.fetchone() is not None
        logger.info(f"CUST_DB_CONNECTIONS table exists: {exists}")

        if not exists:
            try:
                # Create table if it doesn't exist
                create_table_sql = """
                    CREATE TABLE IF NOT EXISTS CUST_DB_CONNECTIONS (
                        connection_id TEXT PRIMARY KEY NOT NULL,
                        db_type TEXT NOT NULL,
                        connection_string TEXT NOT NULL,
                        owner_bot_id TEXT NOT NULL,
                        allowed_bot_ids TEXT,
                        description TEXT,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                """
                # Check if description column exists
                cursor.execute(create_table_sql)
                self.connection.commit()

                cursor.execute("PRAGMA table_info(CUST_DB_CONNECTIONS)")
                columns = cursor.fetchall()
                has_description = any(col[1] == 'description' for col in columns)

                if not has_description:
                    cursor.execute("ALTER TABLE CUST_DB_CONNECTIONS ADD COLUMN description TEXT")
                    self.connection.commit()
                    logger.info("Added description column to CUST_DB_CONNECTIONS table")

                # Define the connections to check/create
                # NOTE: Do not change the connection_string to point to the golden demo data. 
                #       Run "./genesis setup" prior to running in local mode.
                # Determine which path to use for each database
                baseball_path = "./genesis_sample/demo_data/baseball.sqlite" if os.path.exists("./genesis_sample/demo_data/baseball.sqlite") else "./genesis_bots/genesis_sample_golden/demo_data/baseball.sqlite"
                formula1_path = "./genesis_sample/demo_data/formula_1.sqlite" if os.path.exists("./genesis_sample/demo_data/formula_1.sqlite") else "./genesis_bots/genesis_sample_golden/demo_data/formula_1.sqlite"

                # Define the connections to check/create
                connections = [
                    {
                        'connection_id': 'baseball_sqlite',
                        'db_type': 'sqlite',
                        'connection_string': f'sqlite:///{baseball_path}',
                        'owner_bot_id': 'Eve',
                        'allowed_bot_ids': '*',
                        'description': 'Demo Baseball data up to 2015'
                    },
                    {
                        'connection_id': 'formula_1_sqlite',
                        'db_type': 'sqlite',
                        'connection_string': f'sqlite:///{formula1_path}',
                        'owner_bot_id': 'Eve',
                        'allowed_bot_ids': '*',
                        'description': 'Demo Formula 1 data up to 2024'
                    },
                    {
                        'connection_id': 'workspace_sqlite',
                        'db_type': 'sqlite',
                        'connection_string': 'sqlite:///./genesis_bots/genesis_sample_golden/demo_data/workspace.sqlite',
                        'owner_bot_id': 'Eve',
                        'allowed_bot_ids': '*',
                        'description': 'Workspace/scratchpad database you can use for storing data and creating new tables'
                    },
                ]

                for conn in connections:
                    cursor.execute("SELECT COUNT(*) FROM CUST_DB_CONNECTIONS WHERE connection_id = ?",
                                (conn['connection_id'],))
                    exists = cursor.fetchone()[0] > 0

                    if not exists:
                        insert_sql = """
                            INSERT INTO CUST_DB_CONNECTIONS (
                                connection_id,
                                db_type,
                                connection_string,
                                owner_bot_id,
                                allowed_bot_ids,
                                description
                            ) VALUES (?, ?, ?, ?, ?, ?)
                        """
                        cursor.execute(insert_sql, (
                            conn['connection_id'],
                            conn['db_type'],
                            conn['connection_string'],
                            conn['owner_bot_id'],
                            conn['allowed_bot_ids'],
                            conn['description']
                        ))
                        self.connection.commit()
                        logger.info(f"Inserted {conn['connection_id']} record")

                    self.import_harvest()

            except Exception as e:
                logger.error(f"Error in _ensure_cust_db_connections_table: {e}")
                raise

    def import_harvest(self):
        """Import HARVEST_RESULTS table from JSON file if table is empty"""
        try:
            cursor = self.connection.cursor()

            # Check if table has any rows
            cursor.execute("SELECT COUNT(*) FROM HARVEST_RESULTS")
            count = cursor.fetchone()[0]

            if count > 0:
                logger.info("HARVEST_RESULTS table already contains data, skipping import")
                return

            import json
            input_file = Path("genesis_sample/demo_data/demo_harvest_results.json")

            # Check if file exists
            if not input_file.exists():
                logger.warning(f"Harvest results file not found at {input_file}")
                return

            # Read JSON file
            with open(input_file, 'r') as f:
                data = json.load(f)

            if not data:
                logger.info("No data found in harvest results file")
                return

            # Insert data
            insert_sql = """
                INSERT OR REPLACE INTO HARVEST_RESULTS (
                    source_name,
                    qualified_table_name,
                    database_name,
                    memory_uuid,
                    schema_name,
                    table_name,
                    complete_description,
                    ddl,
                    ddl_short,
                    ddl_hash,
                    summary,
                    sample_data_text,
                    last_crawled_timestamp,
                    crawl_status,
                    role_used_for_crawl,
                    embedding,
                    embedding_native
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """

            for row in data:
                cursor.execute(insert_sql, (
                    row.get('source_name'),
                    row.get('qualified_table_name'),
                    row.get('database_name'),
                    row.get('memory_uuid'),
                    row.get('schema_name'),
                    row.get('table_name'),
                    row.get('complete_description'),
                    row.get('ddl'),
                    row.get('ddl_short'),
                    row.get('ddl_hash'),
                    row.get('summary'),
                    row.get('sample_data_text'),
                    row.get('last_crawled_timestamp'),
                    row.get('crawl_status'),
                    row.get('role_used_for_crawl'),
                    str(row.get('embedding')),  # Convert ARRAY to TEXT
                    str(row.get('embedding_native'))  # Convert ARRAY to TEXT
                ))

            self.connection.commit()
            logger.info(f"Successfully imported harvest results from {input_file}")

        except Exception as e:
            logger.error(f"Error importing harvest results: {e}")
            raise

    def export_harvest(self):
        """Export HARVEST_RESULTS table to JSON file"""
        try:
            cursor = self.connection.cursor()
            cursor.execute("SELECT * FROM HARVEST_RESULTS")
            rows = cursor.fetchall()

            # Get column names
            column_names = [description[0] for description in cursor.description]

            # Convert to list of dicts
            data = []
            for row in rows:
                data.append(dict(zip(column_names, row)))

            # Create demos/demo_data directory if it doesn't exist
            os.makedirs("./genesis_sample/demo_data", exist_ok=True)

            # Save to JSON file
            import json
            output_file = "./genesis_sample/demo_data/demo_harvest_results.json"
            with open(output_file, 'w') as f:
                json.dump(data, f, indent=2, default=str)

            logger.info(f"Successfully exported HARVEST_RESULTS to {output_file}")

        except Exception as e:
            logger.error(f"Error exporting HARVEST_RESULTS: {e}")
            raise

    def _ensure_harvest_control_table(self):
        """Ensure HARVEST_CONTROL table exists with correct schema and default entries"""
        logger.info("Starting HARVEST_CONTROL table verification")
        cursor = self.connection.cursor()

        try:
            # Create table if it doesn't exist
            create_table_sql = """
                CREATE TABLE IF NOT EXISTS HARVEST_CONTROL (
                    source_name TEXT NOT NULL,
                    database_name TEXT NOT NULL,
                    schema_inclusions TEXT,  -- ARRAY in Snowflake, TEXT in SQLite
                    schema_exclusions TEXT,  -- ARRAY in Snowflake, TEXT in SQLite
                    status TEXT NOT NULL,
                    refresh_interval INTEGER NOT NULL,
                    initial_crawl_complete INTEGER NOT NULL  -- BOOLEAN in Snowflake, INTEGER in SQLite
                )
            """
            cursor.execute(create_table_sql)
            self.connection.commit()

            # Define the default harvest control entries
            default_entries = [
                {
                    'source_name': 'baseball_sqlite',
                    'database_name': 'baseball_sqlite',
                    'schema_inclusions': '[]',
                    'schema_exclusions': '["INFORMATION_SCHEMA"]',
                    'status': 'Include',
                    'refresh_interval': 10,
                    'initial_crawl_complete': 0
                },
                {
                    'source_name': 'formula_1_sqlite',
                    'database_name': 'formula_1_sqlite',
                    'schema_inclusions': '[]',
                    'schema_exclusions': '["INFORMATION_SCHEMA"]',
                    'status': 'Include',
                    'refresh_interval': 10,
                    'initial_crawl_complete': 0
                },
                {
                    'source_name': 'workspace_sqlite',
                    'database_name': 'workspace_sqlite',
                    'schema_inclusions': '[]',
                    'schema_exclusions': '["INFORMATION_SCHEMA"]',
                    'status': 'Include',
                    'refresh_interval': 1,
                    'initial_crawl_complete': 0
                }
            ]

            # Check and add each entry
            for entry in default_entries:
                cursor.execute("SELECT COUNT(*) FROM HARVEST_CONTROL WHERE source_name = ?",
                             (entry['source_name'],))
                count = cursor.fetchone()[0]

                if count == 0:
                    insert_sql = """
                        INSERT INTO HARVEST_CONTROL (
                            source_name,
                            database_name,
                            schema_inclusions,
                            schema_exclusions,
                            status,
                            refresh_interval,
                            initial_crawl_complete
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """
                    cursor.execute(insert_sql, (
                        entry['source_name'],
                        entry['database_name'],
                        entry['schema_inclusions'],
                        entry['schema_exclusions'],
                        entry['status'],
                        entry['refresh_interval'],
                        entry['initial_crawl_complete']
                    ))
                    self.connection.commit()
                    logger.info(f"Added {entry['source_name']} entry to HARVEST_CONTROL")

        except Exception as e:
            logger.error(f"Error in _ensure_harvest_control_table: {e}")
            raise

    def _ensure_harvest_results_table(self):
        """Ensure HARVEST_RESULTS table exists with correct schema"""
        logger.info("Starting HARVEST_RESULTS table verification")
        cursor = self.connection.cursor()

        try:
            # Create table if it doesn't exist
            create_table_sql = """
                CREATE TABLE IF NOT EXISTS HARVEST_RESULTS (
                    source_name TEXT NOT NULL,
                    qualified_table_name TEXT NOT NULL,
                    database_name TEXT NOT NULL,
                    memory_uuid TEXT NOT NULL,
                    schema_name TEXT NOT NULL,
                    table_name TEXT NOT NULL,
                    complete_description TEXT NOT NULL,
                    ddl TEXT NOT NULL,
                    ddl_short TEXT,
                    ddl_hash TEXT NOT NULL,
                    summary TEXT NOT NULL,
                    sample_data_text TEXT NOT NULL,
                    last_crawled_timestamp TIMESTAMP NOT NULL,
                    crawl_status TEXT NOT NULL,
                    role_used_for_crawl TEXT NOT NULL,
                    embedding TEXT,
                    embedding_native TEXT,
                    PRIMARY KEY (source_name, qualified_table_name)
                )
            """
            cursor.execute(create_table_sql)
            self.connection.commit()
            logger.info("HARVEST_RESULTS table verified")


        except Exception as e:
            logger.error(f"Error in _ensure_harvest_results_table: {e}")
            raise

    def cursor(self):
        if self.connection is None:
            logger.error("No database connection")
            raise Exception("No database connection")
        try:
            # Test the connection
            self.connection.execute("SELECT 1")
            # Create a new cursor wrapper
            return SQLiteCursorWrapper(self.connection.cursor())
        except sqlite3.Error as e:
            logger.error(f"Error creating cursor: {e}")
            # Try to reconnect
            self.connection = sqlite3.connect(self.db_path, check_same_thread=False)
            return SQLiteCursorWrapper(self.connection.cursor())

    def commit(self):
        return self.connection.commit()

    def rollback(self):
        return self.connection.rollback()

class SQLiteCursorWrapper:
    def __init__(self, real_cursor):
        self.real_cursor = real_cursor

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False  # Re-raise any exceptions

    @property
    def description(self):
        return self.real_cursor.description or []

    @property
    def rowcount(self):
        return self.real_cursor.rowcount

    def execute(self, query: str, params: Any = None) -> Any:
        try:
            # Replace %s with ? for SQLite
            if isinstance(params, dict):
                converted_query = re.sub(r'%\(([a-zA-Z0-9_]+)\)s', r':\1', query)
            else:
                converted_query = re.sub(r'%\([a-zA-Z0-9_]*\)s|%s', '?', query)

            # Ensure params is in the correct format for SQLite
            if params is not None:
                if not isinstance(params, (list, tuple, dict)):
                    # Single parameter - wrap in a tuple
                    params = (params,)
                elif isinstance(params, list):
                    # Convert list to tuple
                    params = tuple(params)

            # Log original parameters
            logger.debug(f"Original params count: {len(params) if params else 0}")
            logger.debug(f"Original params: {params}")

            # Handle parameter count for MERGE/UPSERT operations before query transformation
            if params and isinstance(params, (list, tuple)):
                if len(params) == 8 and 'llm_tokens' in query.lower() and 'MERGE INTO' in query.upper():
                    # Take only the first 4 parameters for llm_tokens MERGE
                    params = params[:4]
                    logger.debug(f"Using first 4 params for llm_tokens: {params}")
                if len(params) == 5 and 'slack_app_config_tokens' in query.lower() and 'MERGE INTO' in query.upper():
                    # Adjust parameters to match the transformed query
                    params = params[:3]
                    logger.debug(f"Using first 3 params for slack_app_config_tokens: {params}")

            modified_query = self._transform_query(converted_query)
            logger.debug(f"Transformed query: {modified_query}")
            logger.debug(f"Final params count: {len(params) if params else 0}")

            # Execute the modified query
            if isinstance(modified_query, str):
                if params is None:
                    return self.real_cursor.execute(modified_query)
                return self.real_cursor.execute(modified_query, params)
            elif isinstance(modified_query, list):
                results = []
                for single_query in modified_query:
                    if params is None:
                        results.append(self.real_cursor.execute(single_query))
                    else:
                        results.append(self.real_cursor.execute(single_query, params))
                return results
        except sqlite3.OperationalError as e:
            # Check if this is a DEFAULT_EMAIL query
            if 'DEFAULT_EMAIL' in query.upper():
                logger.info(f"DEFAULT_EMAIL table not found (expected): {query}")
                return None
            # Check if error is about duplicate column
            elif 'duplicate column name' in str(e) or 'duplicate column' in str(e):
                logger.info(f"Column already exists (skipping): {query}")
                return None
            else:
                logger.error(f"SQLite error: {e}")
                logger.error(f"Query: {query}")
                logger.error(f"Params: {params}")
                raise e
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            logger.error(f"Query: {query}")
            logger.error(f"Params: {params}")
            raise

    def fetchone(self):
        return self.real_cursor.fetchone()

    def fetchall(self):
        try:
            return self.real_cursor.fetchall()
        except Exception:
            return []

    def fetchmany(self, size=None):
        """Fetch the next set of rows of a query result"""
        try:
            if size is None:
                return self.real_cursor.fetchall()
            return self.real_cursor.fetchmany(size)
        except Exception:
            return []

    def close(self):
        return self.real_cursor.close()

    def _transform_query(self, query: str, keep_db_schema: bool = False) -> str | list[str]:
        """Transform Snowflake SQL to SQLite compatible SQL"""

        if not query:
            return query

        # Check for KEEPSCHEMA:: prefix and set flag accordingly
        keep_db_schema = False
        if query.startswith('KEEPSCHEMA::'):
            keep_db_schema = True
            query = query[len('KEEPSCHEMA::'):]

        # Convert current_timestamp() to CURRENT_TIMESTAMP
        query = re.sub(r'current_timestamp\(\)', 'CURRENT_TIMESTAMP', query, flags=re.IGNORECASE)
        query = re.sub(r'CURRENT_TIMESTAMP\(\)', 'CURRENT_TIMESTAMP', query)

        # Remove schema prefix if it matches GENESIS_INTERNAL_DB_SCHEMA
        schema_prefix = os.environ.get('GENESIS_INTERNAL_DB_SCHEMA', '')
        if schema_prefix and schema_prefix + '.' in query:
            query = query.replace(schema_prefix + '.', '')

        # Clean up query for easier pattern matching
        query_clean = ' '.join(query.split())
        query_upper = query_clean.upper()

        # Convert HYBRID TABLE to just TABLE
        query = re.sub(r'(?i)HYBRID\s+TABLE', 'TABLE', query)

        # Remove INDEX definitions from CREATE TABLE statements
        query = re.sub(r',\s*INDEX\s+\w+\s*\([^)]+\)', '', query)

        # Handle GRANT statements - convert to no-op
        if query_upper.startswith('GRANT'):
            logger.debug(f"Converting GRANT statement to no-op: {query}")
            return "SELECT 1 WHERE 1=0"

        # First clean up any newlines/extra spaces for easier pattern matching
        query_clean = ' '.join(query.split())
        query_upper = query_clean.upper()

        # Replace %s with ? for SQLite
        query = query.replace('%s', '?')

        # Handle JSON extraction and type casting
        if '::varchar' in query:
            # Replace Snowflake's JSON parsing and type casting with SQLite's json_extract
            query = re.sub(
                r'parse_json\(([^)]+)\):(\w+)::varchar',
                r'json_extract(\1, "$.\2")',
                query
            )

        # Handle CALL statements (new)
        if query_upper.startswith('CALL'):
            logger.debug("Converting CALL statement to SELECT")
            # Convert CALL CORE.GET_EAI_LIST to a simple SELECT that returns NULL
            if 'GET_EAI_LIST' in query_upper:
                return "SELECT NULL as EAI_LIST WHERE 1=0"
            return "SELECT 1 WHERE 1=0"  # Default no-op for other CALL statements

        # Handle MERGE INTO statements for slack_app_config_tokens
        if query_upper.startswith('MERGE INTO') and 'SLACK_APP_CONFIG_TOKENS' in query_upper:
            return """
                INSERT INTO slack_app_config_tokens
                    (runner_id, slack_app_config_token, slack_app_config_refresh_token)
                VALUES
                    (?1, ?2, ?3)
                ON CONFLICT(runner_id)
                DO UPDATE SET
                    slack_app_config_token = ?2,
                    slack_app_config_refresh_token = ?3
            """

        # Skip certain operations that don't apply in SQLite mode

        skip_patterns = [
            r'(?i)ENCODED_IMAGE_DATA',
            r'(?i)APP_SHARE\.',
     #       r'(?i)UPDATE.*BOT_AVATAR_IMAGE',
     #       r'(?i)INSERT.*BOT_AVATAR_IMAGE',
            r'(?i)CREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION',
            r'(?i)CREATE\s+(?:OR\s+REPLACE\s+)?STAGE',
            r'(?i)RESULT_SCAN',
            r'(?i)LAST_QUERY_ID\(\)',
            r'(?i)UPDATE.*BOT_SERVICING.*SET.*FROM.*IMAGES'
        ]

        # Check if query should be skipped
        for pattern in skip_patterns:
            if re.search(pattern, query_clean):
                logger.debug(f"Skipping query due to APP_SHARE or other pattern match: {query_clean}")
                return "SELECT 1 WHERE 1=0"  # No-op query

        # Handle CREATE TABLE statements
        if query_upper.startswith('CREATE TABLE'):
            # Remove schema qualifiers
            query = re.sub(r'(?:[^.\s]+\.){1,2}([^\s(]+)', r'\1', query)

            # Convert types and defaults
            query = re.sub(r'VARCHAR\([^)]+\)', 'TEXT', query)
            query = re.sub(r'TIMESTAMP', 'DATETIME', query)
            query = re.sub(r'BOOLEAN', 'INTEGER', query)  # SQLite uses INTEGER for boolean
            query = re.sub(
                r'DEFAULT\s+CURRENT_TIMESTAMP\(\)',
                "DEFAULT CURRENT_TIMESTAMP",
                query
            )
            query = re.sub(
                r'DEFAULT\s+CURRENT_DATETIME\(\)',
                "DEFAULT CURRENT_TIMESTAMP",
                query
            )

            # Add IF NOT EXISTS to prevent errors
            if 'IF NOT EXISTS' not in query_upper:
                query = query.replace('CREATE TABLE', 'CREATE TABLE IF NOT EXISTS')

            return query

        # List of Snowflake-specific commands that should be no-ops in SQLite
        snowflake_specific_commands = [
            ('CREATE STAGE', 'stage creation'),
            ('SHOW ENDPOINTS', 'endpoints listing'),
            ('SHOW ENDPOINTS IN SERVICE', 'service endpoints listing'),
            ('SHOW SERVICES', 'services'),
            ('ALTER SERVICE', 'service alteration'),
            ('CREATE SERVICE', 'service creation'),
            ('DROP SERVICE', 'service deletion'),
            ('DESCRIBE SERVICE', 'service description'),
            ('DESCRIBE ENDPOINT', 'endpoint description'),
            ('CREATE OR REPLACE PROCEDURE', 'stored procedure creation'),
            ('CREATE PROCEDURE', 'stored procedure creation'),
        ]

        # Check for Snowflake-specific commands that should be no-ops
        for command, feature in snowflake_specific_commands:
            if command in query_upper:
                logger.info(f"Ignoring {feature} command - not supported in SQLite")
                return "SELECT 1 WHERE 1=0"  # No-op query

        # Handle SHOW commands
        if query_upper.startswith('SHOW'):
            if query_upper == 'SHOW DATABASES':
                return "SELECT name FROM pragma_database_list"

            elif query_upper == 'SHOW SCHEMAS' or query_upper.startswith('SHOW SCHEMAS IN'):
                # SQLite doesn't have schemas, return 'main' as default schema
                return "SELECT 'main' as name"

            elif query_upper.startswith('SHOW TABLES'):
                # Extract the LIKE pattern if it exists
                like_match = re.search(r"LIKE\s+'([^']+)'", query, re.IGNORECASE)
                like_pattern = like_match.group(1) if like_match else None

                base_query = """
                    SELECT name as "name"
                    FROM sqlite_master
                    WHERE type='table'
                    AND name NOT LIKE 'sqlite_%'
                """

                if like_pattern:
                    base_query += f" AND name LIKE '{like_pattern}'"

                return base_query + " ORDER BY name"

            elif query_upper.startswith('SHOW COLUMNS'):
                # Extract table name from "SHOW COLUMNS IN table"
                table_match = re.search(r'SHOW\s+COLUMNS\s+IN\s+(\w+)', query, re.IGNORECASE)
                if table_match:
                    table_name = table_match.group(1)
                    return f"PRAGMA table_info({table_name})"

            # Handle other SHOW commands as no-ops
            logger.info(f"Ignoring unsupported SHOW command in SQLite: {query}")
            return "SELECT 1 WHERE 1=0"

        # Remove only fully qualified database.schema.table patterns
        if not keep_db_schema:
            query = re.sub(r'"?[^".\s]+"\."[^".\s]+"."([^"\s]+)"?', r'\1', query)  # Remove "DB"."SCHEMA"."TABLE"
            query = re.sub(r'(\w+)\.(\w+)\.(\w+)(?=\s|$)', r'\3', query)           # Remove DB.SCHEMA.TABLE

        # Do NOT remove schema.table or alias.column patterns
        # Remove these lines:
        # query = re.sub(r'"?[^".\s]+"."([^"\s]+)"?', r'\1', query)              # Remove SCHEMA.TABLE
        # query = re.sub(r'(\w+)\.(\w+)(?!\s*=)', r'\2', query)                  # Remove unquoted SCHEMA.TABLE

        # Handle CREATE OR REPLACE TABLE
        if 'CREATE OR REPLACE TABLE' in query_upper:
            # Extract table name and column definition
            match = re.match(
                r'CREATE\s+OR\s+REPLACE\s+TABLE\s+(?:[^.\s]+\.)?(?:[^.\s]+\.)?([^\s(]+)\s*\((.*)\)',
                query_clean,
                re.IGNORECASE
            )

            if match:
                table_name = match.group(1)
                column_def = match.group(2)

                # Convert types
                column_def = re.sub(r'STRING', 'TEXT', column_def)
                column_def = re.sub(r'VARCHAR\([^)]+\)', 'TEXT', column_def)

                # Handle quoted column names
                column_def = re.sub(r'"([^"]+)"', r'`\1`', column_def)

                return [
                    f"DROP TABLE IF EXISTS {table_name}",
                    f"CREATE TABLE {table_name} ({column_def})"
                ]

        if 'CREATE OR REPLACE TABLE' in query_upper:
            match = re.match(
                r'CREATE OR REPLACE TABLE\s+(.+) AS SELECT \* FROM\s+(.+)',
                query_clean.replace(';', ''),
                re.IGNORECASE
            )

            if match:
                table_name = match.group(1)
                source_table = match.group(2)

                return [
                    f"DROP TABLE IF EXISTS {table_name}",
                    f"CREATE TABLE {table_name} AS SELECT * FROM {source_table}"
                ]

        # Handle MERGE INTO statements
        if query_upper.startswith('MERGE INTO'):
            # Extract table name and remove schema qualifiers
            table_match = re.search(r'MERGE\s+INTO\s+(?:[^.\s]+\.)?(?:[^.\s]+\.)?([^\s]+)', query, re.IGNORECASE)
            if table_match:
                table_name = table_match.group(1)

                # For llm_tokens table
                if 'llm_tokens' in table_name.lower():
                    logger.debug("Transforming llm_tokens MERGE query")
                    return [
                        """DELETE FROM llm_tokens
                           WHERE runner_id = ?1
                             AND llm_type = 'openai'
                             AND (?2 IS NOT NULL OR ?3 IS NOT NULL OR ?4 IS NOT NULL)""",
                        """INSERT INTO llm_tokens
                            (runner_id, llm_key, llm_type, active, llm_endpoint)
                           VALUES
                            (?1, ?2, ?3, 1, ?4)"""
                    ]

                # For BOT_SERVICING table with USING SELECT pattern
                if 'BOT_SERVICING' in table_name.upper() and 'USING (SELECT' in query_upper:
                    logger.debug("Transforming BOT_SERVICING MERGE query with USING SELECT")
                    return [
                        """DELETE FROM BOT_SERVICING WHERE BOT_ID = ?1 AND (?2 IS NOT NULL OR ?3 IS NOT NULL OR ?4 IS NOT NULL OR ?5 IS NOT NULL OR ?6 IS NOT NULL OR ?7 IS NOT NULL OR ?8 IS NOT NULL)""",
                        """
                        INSERT INTO BOT_SERVICING
                            (BOT_ID, RUNNER_ID, BOT_NAME, BOT_INSTRUCTIONS,
                             AVAILABLE_TOOLS, UDF_ACTIVE, SLACK_ACTIVE, BOT_INTRO_PROMPT)
                        VALUES
                            (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                        """
                    ]
        # Handle DESCRIBE TABLE command
        if query_upper.startswith('DESCRIBE TABLE'):
            # Extract table name, handling both quoted and unquoted names
            table_match = re.search(r'DESCRIBE\s+TABLE\s+(?:[^.\s]+\.)?(?:[^.\s]+\.)?([^\s;]+)', query, re.IGNORECASE)
            if table_match:
                table_name = table_match.group(1)
                # For DESCRIBE TABLE, just return the column names in the expected format
                return f"""
                    SELECT
                        name,           -- Column 0: name
                        type,           -- Column 1: type
                        "notnull",      -- Column 2: nullable
                        dflt_value,     -- Column 3: default
                        pk,             -- Column 4: primary key
                        cid            -- Column 5: column id
                    FROM pragma_table_info('{table_name}')
                    ORDER BY cid
                """

        # Convert %s placeholders to ? for SQLite
        query = re.sub(r'%s', '?', query)

        # Define Snowflake to SQLite syntax replacements
        replacements = {
            'CURRENT_TIMESTAMP\\(\\)': "datetime('now')",
            'CURRENT_TIMESTAMP': "datetime('now')",
       #     'TIMESTAMP': 'DATETIME',
            'VARCHAR\\([0-9]+\\)': 'TEXT',
            'BOOLEAN': 'INTEGER',
            'TIMESTAMP_NTZ': 'DATETIME',
            'VARIANT': 'TEXT',
            'IFF\\(([^,]+),([^,]+),([^)]+)\\)': r'CASE WHEN \1 THEN \2 ELSE \3 END',
            'REPLACE\\(REPLACE\\(([^,]+),([^,]+),([^)]+)\\),([^,]+),([^)]+)\\)':
                r'REPLACE(REPLACE(\1,\2,\3),\4,\5)',
            'TO_TIMESTAMP': 'DATETIME'
        }

        # Apply replacements
        for pattern, replacement in replacements.items():
            query = re.sub(pattern, replacement, query)

        # Handle TIMESTAMP keyword with special cases
        # Case 1: Double TIMESTAMP -> TIMESTAMP DATETIME
        query = re.sub(r'\bTIMESTAMP\s+TIMESTAMP\b', 'TIMESTAMP DATETIME', query)

        # Case 2: Don't modify TIMESTAMP in ORDER BY clause
        # Handle TIMESTAMP keyword, excluding ORDER BY clauses
        parts = query.split('ORDER BY')
        if len(parts) > 1:
            # Don't modify TIMESTAMP in ORDER BY clause
            modified_first = re.sub(r'\bTIMESTAMP\b(?!\s+TIMESTAMP)', 'DATETIME', parts[0])
            query = modified_first + 'ORDER BY' + parts[1]
        else:
            # No ORDER BY - replace all TIMESTAMP except when followed by TIMESTAMP
            query = re.sub(r'\bTIMESTAMP\b(?!\s+TIMESTAMP)', 'DATETIME', query)


        # Handle ALTER TABLE ADD COLUMN statements
        if 'ALTER TABLE' in query_upper and 'ADD COLUMN' in query_upper:
            # Remove schema qualifiers
            query = re.sub(r'(?:[^.\s]+\.){1,2}([^\s(]+)', r'\1', query)

            # Extract table name and columns
            match = re.match(r'ALTER TABLE\s+(\w+)\s+ADD\s+COLUMN\s+(.+)', query, re.IGNORECASE)
            if match:
                table_name = match.group(1)
                columns_str = match.group(2).strip(';')

                # Split multiple columns and create separate ALTER TABLE statements
                columns = [col.strip() for col in columns_str.split(',')]

                # Convert Snowflake types to SQLite types
                statements = []
                for col in columns:
                    # Convert types
                    col = re.sub(r'VARCHAR\([^)]+\)', 'TEXT', col)
                    col = re.sub(r'TIMESTAMP', 'DATETIME', col)
                    col = re.sub(r'ARRAY', 'TEXT', col)
                    col = re.sub(r'STRING', 'TEXT', col)
                    col = re.sub(r'BOOLEAN', 'INTEGER', col)

                    # Extract column name (part before first space)
                    col_name = col.split()[0]

                    # Only try to add column if it doesn't exist
                    statements.append(f"""
                        SELECT CASE
                            WHEN NOT EXISTS (
                                SELECT 1 FROM pragma_table_info('{table_name}')
                                WHERE name = '{col_name}'
                            )
                            THEN (
                                SELECT 1
                            )
                        END;
                    """)

                    # The actual ALTER will only be executed if the column doesn't exist
                    statements.append(f"""
                        SELECT CASE
                            WHEN NOT EXISTS (
                                SELECT 1 FROM pragma_table_info('{table_name}')
                                WHERE name = '{col_name}'
                            )
                            THEN (
                                SELECT sqlite_version()
                            )
                        END;
                    """)

                return statements

        # Handle SHOW COLUMNS or DESCRIBE TABLE commands
        if query_upper.startswith('SHOW COLUMNS') or query_upper.startswith('DESCRIBE TABLE'):
            # Extract table name
            table_match = re.search(r'(?:SHOW\s+COLUMNS\s+IN|DESCRIBE\s+TABLE)\s+(?:[^.\s]+\.)?(?:[^.\s]+\.)?([^\s;]+)', query, re.IGNORECASE)
            if table_match:
                table_name = table_match.group(1)
                return f"""
                    SELECT
                        name as "column_name",
                        CASE type
                            WHEN 'INTEGER' THEN 'NUMBER'
                            WHEN 'REAL' THEN 'FLOAT'
                            WHEN 'TEXT' THEN 'VARCHAR'
                            WHEN 'BLOB' THEN 'BINARY'
                            ELSE upper(type)
                        END as "data_type",
                        CASE
                            WHEN "notnull" = 0 THEN 'YES'
                            ELSE 'NO'
                        END as "is_nullable",
                        CASE
                            WHEN pk > 0 THEN 'YES'
                            ELSE 'NO'
                        END as "is_primary_key",
                        dflt_value as "column_default"
                    FROM pragma_table_info('{table_name}')
                    ORDER BY cid
                """

        # Convert boolean values
        query = re.sub(r'=\s*True\b', '= 1', query, flags=re.IGNORECASE)
        query = re.sub(r'=\s*False\b', '= 0', query, flags=re.IGNORECASE)

        # Handle specific llm_tokens queries
        if 'llm_tokens' in query_upper:
            query = query.replace('active = TRUE', 'active = 1')
            query = query.replace('active = FALSE', 'active = 0')

        # Handle UDF and STAGE creation (make them no-ops)
        if 'CREATE' in query_upper:
            if any(keyword in query_upper for keyword in ['FUNCTION', 'STAGE']):
                object_type = 'FUNCTION' if 'FUNCTION' in query_upper else 'STAGE'
                logger.debug(f"Skipping {object_type} creation in SQLite mode: {query}")
                return "SELECT 1 WHERE 1=0"

        # Handle Snowflake-specific functions like RESULT_SCAN
        if 'RESULT_SCAN' in query_upper or 'LAST_QUERY_ID()' in query_upper:
            logger.debug("Skipping RESULT_SCAN query in SQLite mode")
            return "SELECT NULL as EAI_LIST WHERE 1=0"  # No-op query that returns empty result with correct column

        # Handle Snowflake-specific functions
        snowflake_specific_functions = [
            'RESULT_SCAN',
            'LAST_QUERY_ID()',
            'GET_DDL',
            'SHOW_DDL'
        ]

        if any(func in query_upper for func in snowflake_specific_functions):
            # Extract the AS clause to get the column alias
            as_match = re.search(r'as\s+(\w+)', query_upper)
            column_name = as_match.group(1) if as_match else 'RESULT'

            logger.debug(f"Skipping Snowflake-specific function query in SQLite mode: {query}")
            return f"SELECT NULL as {column_name} WHERE 1=0"

        # Handle UPDATE with FROM clause
        if query_upper.startswith('UPDATE') and ' FROM ' in query_upper:
            # Extract the basic parts of the query
            match = re.match(
                r'UPDATE\s+(\w+)\s+\w+\s+SET\s+(.*?)\s+FROM\s*\((.*?)\)\s*(\w+)(?:\s+WHERE\s+(.*))?',
                query,
                re.IGNORECASE | re.DOTALL
            )

            if match:
                table = match.group(1)
                set_clause = match.group(2)
                subquery = match.group(3)
                where_clause = match.group(5) if match.group(5) else ''

                # Transform to SQLite syntax using a subquery in the SET clause
                return f"""
                    UPDATE {table}
                    SET {set_clause}
                    WHERE EXISTS (
                        SELECT 1
                        FROM ({subquery}) AS subq
                        WHERE {where_clause if where_clause else '1=1'}
                    )
                """

        # Skip certain operations that don't apply in SQLite mode
        skip_operations = {
            'CREATE OR REPLACE FUNCTION': 'UDF creation',
            'CREATE FUNCTION': 'UDF creation',
            'CREATE OR REPLACE STAGE': 'Stage creation',
            'CREATE STAGE': 'Stage creation',
            'RESULT_SCAN': 'Result scan',
            'LAST_QUERY_ID()': 'Last query ID',
            # Skip any queries involving encoded image data
            'ENCODED_IMAGE_DATA(?!.*SELECT)': 'Image data operation',
            r'APP_SHARE\.IMAGES(?!.*SELECT)': 'App share image query',
       #     'BOT_AVATAR_IMAGE(?!.*SELECT)': 'Bot avatar update'
        }

        # Check if query should be skipped
        for pattern, operation_type in skip_operations.items():
            if re.search(pattern, query_upper, re.IGNORECASE):
                logger.debug(f"Skipping {operation_type} in SQLite mode: {query}")
                return "SELECT 1 WHERE 1=0"  # No-op query

        # Handle CREATE OR REPLACE HYBRID TABLE
        if re.search(r'CREATE\s+(?:OR\s+REPLACE\s+)?(?:HYBRID\s+)?TABLE', query_upper):
            # Extract table name and column definitions
            match = re.search(r'CREATE\s+(?:OR\s+REPLACE\s+)?(?:HYBRID\s+)?TABLE\s+(?:[^.\s]+\.)?(?:[^.\s]+\.)?([^\s]+)\s*\((.*)\)', query, re.IGNORECASE | re.DOTALL)
            if match:
                table_name = match.group(1)
                column_defs = match.group(2).strip()

                # Remove INDEX definition
                column_defs = re.sub(r',\s*INDEX.*?\([^)]+\)', '', column_defs)

                # Convert data types
                column_defs = re.sub(r'VARCHAR\(\d+\)', 'TEXT', column_defs)
                column_defs = re.sub(r'VARCHAR', 'TEXT', column_defs)
                column_defs = re.sub(r'TIMESTAMP', 'DATETIME', column_defs)

                # Convert CURRENT_TIMESTAMP
                column_defs = re.sub(r'CURRENT_TIMESTAMP', "CURRENT_TIMESTAMP", column_defs)

                # Clean up any extra whitespace
                column_defs = re.sub(r'\s+', ' ', column_defs).strip()

                return [
                    f"DROP TABLE IF EXISTS {table_name}",
                    f"CREATE TABLE {table_name} ({column_defs})"
                ]

        # Handle column name differences between Snowflake and SQLite
        column_mappings = {
            'LAST_CRAWLED_DATETIME': 'LAST_CRAWLED_TIMESTAMP',
        #    'DATETIME': 'CREATED_AT'
        }

        # Apply column name mappings
        for old_name, new_name in column_mappings.items():
            query = re.sub(
                rf'\b{old_name}\b',  # Word boundary to match exact column name
                new_name,
                query,
                flags=re.IGNORECASE
            )

        # Remove schema qualifiers
      #  query = re.sub(r'(?:[^.\s]+\.){1,2}([^\s(]+)', r'\1', query)

        # Handle INSERT INTO statements for BOT_SERVICING
        if f'INSERT INTO {schema_prefix.upper()}.BOT_SERVICING' in query_upper:
            # Replace %s with ? for SQLite
            query = query.replace('%s', '?')
            # Remove schema qualifiers
            query = re.sub(f'{schema_prefix.upper()}.', '', query)
            return query
        # Handle the specific message_log thread query
        if "parse_json(message_metadata):thread_ts::varchar" in query:
            # Extract the bot_id parameter from the original query
            bot_id_match = re.search(r"bot_id\s*=\s*'([^']+)'", query)
            bot_id = bot_id_match.group(1) if bot_id_match else None

            transformed_query = """
                SELECT
                    json_extract(message_metadata, '$.thread_ts') as thread_ts,
                    max(timestamp) as max_ts
                FROM message_log
                WHERE message_metadata IS NOT NULL
                AND message_metadata LIKE '%"thread_ts"%'
                AND message_metadata NOT LIKE '%TextContentBlock%'
                AND bot_id = ?
                GROUP BY bot_id, json_extract(message_metadata, '$.thread_ts')
                ORDER BY max_ts DESC
                LIMIT 1000
            """
            logger.debug(f"Transformed thread query to: {transformed_query}")
            return transformed_query

        # Handle CREATE SCHEMA statements - convert to no-op
        if query_upper.startswith('CREATE SCHEMA'):
            logger.debug(f"Converting CREATE SCHEMA statement to no-op: {query}")
            return "SELECT 1 WHERE 1=0"

        # Handle MERGE INTO statements for BOT_SERVICING
        if query_upper.startswith('MERGE INTO') and 'BOT_SERVICING' in query_upper:
            # Extract the column order from the query
            insert_cols_match = re.search(r'INSERT\s*\((.*?)\)', query, re.IGNORECASE | re.DOTALL)
            if insert_cols_match:
                columns = [col.strip() for col in insert_cols_match.group(1).split(',')]
                placeholders = ','.join(['?' for _ in columns])

                return f"""
                    INSERT INTO BOT_SERVICING
                        ({', '.join(columns)})
                    VALUES
                        ({placeholders})
                    ON CONFLICT(BOT_ID)
                    DO UPDATE SET
                        RUNNER_ID = excluded.RUNNER_ID,
                        BOT_NAME = excluded.BOT_NAME,
                        BOT_INSTRUCTIONS = excluded.BOT_INSTRUCTIONS,
                        AVAILABLE_TOOLS = excluded.AVAILABLE_TOOLS,
                        UDF_ACTIVE = excluded.UDF_ACTIVE,
                        SLACK_ACTIVE = excluded.SLACK_ACTIVE,
                        BOT_INTRO_PROMPT = excluded.BOT_INTRO_PROMPT
                    WHERE BOT_ID = excluded.BOT_ID
                """

        # Return the original query if no transformations were applied
        return query
