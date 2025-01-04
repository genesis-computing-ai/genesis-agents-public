import os, csv, io
# import time
import simplejson as json
from openai import OpenAI
import random
#from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

from llm_openai.openai_utils import get_openai_client
from core.logging_config import logger
# Assuming OpenAI SDK initialization

class SchemaExplorer:
    def __init__(self, db_connector, llm_api_key):
        self.db_connector = db_connector
        self.llm_api_key = llm_api_key
        self.run_number = 0
        self._prompt_cache = {}

        self.initialize_model()

    def initialize_model(self):
        if os.environ.get("CORTEX_MODE", 'False') == 'True':
            self.cortex_model = os.getenv("CORTEX_HARVESTER_MODEL", 'reka-flash')
            self.embedding_model = os.getenv("CORTEX_EMBEDDING_MODEL", 'e5-base-v2')
            if os.getenv("CORTEX_EMBEDDING_AVAILABLE",'False') == 'False':
                if self.test_cortex():
                    if self.test_cortex_embedding() == '':
                        logger.info("cortex not available and no OpenAI API key present. Use streamlit to add OpenAI key")
                        os.environ["CORTEX_EMBEDDING_AVAILABLE"] = 'False'
                    else:
                        os.environ["CORTEX_EMBEDDING_AVAILABLE"] = 'True'
                else:
                    os.environ["CORTEX_EMBEDDING_AVAILABLE"] = 'False'
        else:
            self.client = get_openai_client()
            self.model=os.getenv("OPENAI_HARVESTER_MODEL", 'gpt-4o')
            self.embedding_model = os.getenv("OPENAI_HARVESTER_EMBEDDING_MODEL", 'text-embedding-3-large')

    def alt_get_ddl(self,table_name = None, dataset=None, matching_connection=None):
        if dataset['source_name'] == 'Snowflake':
            return self.db_connector.alt_get_ddl(table_name)
        else:
            try:
                from connectors.data_connector import DatabaseConnector
                connector = DatabaseConnector()
                
                db_type = matching_connection['db_type'].split('+')[0] if '+' in matching_connection['db_type'] else matching_connection['db_type']
                sql = self.load_custom_query(db_type, 'get_ddl')

                # Pre-defined DDL queries for common database types
                if sql is None:
                    ddl_queries = {
                    'postgresql': '''
                        SELECT 
                            'CREATE TABLE ' || table_schema || '.' || table_name || E'\n(\n' ||
                            string_agg(
                                '    ' || column_name || ' ' || 
                                CASE 
                                    WHEN udt_name = 'varchar' THEN 'character varying(' || character_maximum_length || ')'
                                    WHEN udt_name = 'bpchar' THEN 'character(' || character_maximum_length || ')'
                                    WHEN udt_name = 'numeric' AND numeric_precision IS NOT NULL AND numeric_scale IS NOT NULL 
                                        THEN 'numeric(' || numeric_precision || ',' || numeric_scale || ')'
                                    ELSE data_type
                                END ||
                                CASE 
                                    WHEN column_default IS NOT NULL THEN ' DEFAULT ' || column_default
                                    ELSE ''
                                END ||
                                CASE 
                                    WHEN is_nullable = 'NO' THEN ' NOT NULL'
                                    ELSE ''
                                END,
                                E',\n'
                                ORDER BY ordinal_position
                            ) ||
                            CASE 
                                WHEN (
                                    SELECT string_agg(
                                        E',\n    CONSTRAINT ' || constraint_name || ' ' || constraint_definition,
                                        ''
                                    )
                                    FROM (
                                        SELECT DISTINCT
                                            pgc.conname AS constraint_name,
                                            pg_get_constraintdef(pgc.oid) AS constraint_definition
                                        FROM pg_constraint pgc
                                        JOIN pg_namespace nsp ON nsp.oid = pgc.connamespace
                                        WHERE conrelid = (quote_ident(table_schema) || '.' || quote_ident(table_name))::regclass
                                        AND nsp.nspname = table_schema
                                    ) constraints
                                ) IS NOT NULL 
                                THEN E',\n' || (
                                    SELECT string_agg(
                                        E'    CONSTRAINT ' || constraint_name || ' ' || constraint_definition,
                                        E',\n'
                                    )
                                    FROM (
                                        SELECT DISTINCT
                                            pgc.conname AS constraint_name,
                                            pg_get_constraintdef(pgc.oid) AS constraint_definition
                                        FROM pg_constraint pgc
                                        JOIN pg_namespace nsp ON nsp.oid = pgc.connamespace
                                        WHERE conrelid = (quote_ident(table_schema) || '.' || quote_ident(table_name))::regclass
                                        AND nsp.nspname = table_schema
                                    ) constraints
                                )
                                ELSE ''
                            END ||
                            E'\n);' as ddl
                        FROM information_schema.columns
                        WHERE table_schema = '!schema_name!'
                          AND table_name = '!table_name!'
                        GROUP BY table_schema, table_name;
                    ''',
                    'mysql': 'SHOW CREATE TABLE !schema_name!.!table_name!',
                    'sqlite': 'SELECT sql FROM sqlite_master WHERE type=\'table\' AND name=!table_name!',
                    'snowflake': 'SELECT GET_DDL(\'TABLE\', \'!database_name!.!schema_name!.!table_name!\')'
                    }

                    if matching_connection['db_type'].lower() in ddl_queries:
                        sql = ddl_queries[matching_connection['db_type'].lower()]

                    if sql is None:
                        # Generate prompt to get SQL for DDL based on database type
                        p = [
                            {"role": "user", "content": f"Write a SQL query to get the DDL or CREATE TABLE statement for a table in a {matching_connection['db_type']} database.  You can include placeholders !table_name!, !schema_name!, !database_name! in the query to be replaced by the actual values at runtime.  Return only the SQL query without any explanation or additional text, with no markdown formatting. The query should return a single column containing the DDL text."}
                        ]
                    
                        sql = self.run_prompt(p)
                
                # Extract database, schema, table from qualified name
                parts = table_name.strip('"').split('.')
                database_name = parts[0].strip('"')
                schema_name = parts[1].strip('"')
                table_name = parts[2].strip('"')

                # Replace placeholders in SQL query
                sql = sql.replace('!table_name!', table_name)
                sql = sql.replace('!schema_name!', schema_name) 
                sql = sql.replace('!database_name!', database_name)
                
                # Execute the generated SQL query through the connector
                result = connector.query_database(
                    connection_id=dataset['source_name'],
                    bot_id='system',
                    query=sql,
                    max_rows=1,
                    max_rows_override=True,
                    bot_id_override=True,
                    database_name=database_name
                )
                
                if result and 'rows' in result and len(result['rows']) > 0:
                    if matching_connection['db_type'].lower() == 'sqlite' or matching_connection['db_type'].lower() == 'postgresql': # confirmed for sqlite and postgresql
                        return result['rows'][0][0]
                    elif matching_connection['db_type'].lower() == 'mysql':
                        return result['rows'][0][1]  # confirmed for mysql
                    else:
                        # Try each return type and use first valid non-empty result
                        try:
                            result_0 = result['rows'][0][0]
                            if result_0:
                                return result_0
                        except:
                            pass
                            
                        try:
                            result_1 = result['rows'][0][1] 
                            if result_1:
                                return result_1
                        except:
                            pass

                        logger.info(f'Could not get DDL for database type: {matching_connection["db_type"]}')
                        return ""
                return ""
                
            except Exception as e:
                logger.info(f'Error getting DDL: {e}')
                return ""

    def format_sample_data(self, sample_data):
        # Utility method to format sample data into a string
        # Implementation depends on how you want to present the data
        try:
            #j = json.dumps(sample_data, indent = 2, use_decimal=True)
            output = io.StringIO()
            writer = csv.DictWriter(output, fieldnames=sample_data[0].keys())
            writer.writeheader()
            writer.writerows(sample_data)
            j = output.getvalue()
            output.close()
            j = j[:1000]
        except TypeError:
            j = ""
        return j

    def store_table_memory(self, database, schema, table, summary=None, ddl=None, ddl_short=None, sample_data=None, dataset=None, matching_connection=None):
        """
        Generates a document including the DDL statement and a natural language description for a table.
        :param schema: The schema name.
        :param table: The table name.
        """
        try:
            if ddl is None:
                ddl = self.alt_get_ddl(table_name='"'+database+'"."'+schema+'"."'+table+'"')
                #ddl = self.db_connector.get_table_ddl(database_name=database, schema_name=schema, table_name=table)

            sample_data_str = ""
            if not sample_data:
                try:
                    sample_data = self.get_sample_data(dataset or {'source_name': 'Snowflake', 'database_name': database, 'schema_name': schema}, table)
                    sample_data_str = ""
                except Exception as e:
                    logger.info(f"Error getting sample data: {e}")
                    sample_data = None
                    sample_data_str = "error"
            if sample_data:
                try:
                    sample_data_str = self.format_sample_data(sample_data)
                except Exception as e:
                    sample_data_str = "format error"
                #sample_data_str = sample_data_str.replace("\n", " ")  # Replace newlines with spaces

            #logger.info('sample data string: ',sample_data_str)
            self.store_table_summary(database, schema, table, ddl=ddl, ddl_short=ddl_short,summary=summary, sample_data=sample_data_str, dataset=dataset, matching_connection=matching_connection)

        except Exception as e:
            logger.info(f"Harvester Error for an object: {e}")
            self.store_table_summary(database, schema, table, summary="Harvester Error: {e}", ddl="Harvester Error", ddl_short="Harvester Error", sample_data="Harvester Error")

    def test_cortex(self):

        newarray = [{"role": "user", "content": "hi there"} ]
        new_array_str = json.dumps(newarray)

        logger.info(f"schema_explorer test calling cortex {self.cortex_model} via SQL, content est tok len=",len(new_array_str)/4)

        # context_limit = 128000 * 4 #32000 * 4
        cortex_query = f"""
                        select SNOWFLAKE.CORTEX.COMPLETE('{self.cortex_model}', %s) as completion;
        """
        try:
            cursor = self.db_connector.connection.cursor()
            # start_time = time.time()
            try:
                cursor.execute(cortex_query, (new_array_str,))
            except Exception as e:
                if 'unknown model' in e.msg:
                    logger.info(f'Model {self.cortex_model} not available in this region, trying mistral-7b')
                    self.cortex_model = 'mistral-7b'
                    cortex_query = f"""
                        select SNOWFLAKE.CORTEX.COMPLETE('{self.cortex_model}', %s) as completion; """
                    cursor.execute(cortex_query, (new_array_str,))
                    logger.info('Ok that worked, changing CORTEX_HARVESTER_MODEL ENV VAR to mistral-7b')
                    os.environ['CORTEX_HARVESTER_MODEL'] = 'mistral-7b'
                else:
                    raise(e)
            self.db_connector.connection.commit()
            # elapsed_time = time.time() - start_time
            result = cursor.fetchone()
            completion = result[0] if result else None

            logger.info(f"schema_explorer test call result: ",completion)

            return True
        except Exception as e:
            logger.info('cortex not available, query error: ',e)
            self.db_connector.connection.rollback()
            return False

    def test_cortex_embedding(self):

        try:
            test_message = 'this is a test message to generate an embedding'

            try:
                # review function used once new regions are unlocked in snowflake
                embedding_result = self.db_connector.run_query(f"SELECT SNOWFLAKE.CORTEX.EMBED_TEXT_768('{self.embedding_model}', '{test_message}');")
                result_value = next(iter(embedding_result[0].values()))
                if result_value:
                    # os.environ['CORTEX_EMBEDDING_MODEL'] = self.embedding_model
                    logger.info(f"Test result value len embedding: {len(result_value)}")
            except Exception as e:
                if 'unknown model' in e.msg:
                    logger.info(f'Model {self.embedding_model} not available in this region, trying snowflake-arctic-embed-m')
                    self.embedding_model = 'snowflake-arctic-embed-m'
                    embedding_result = self.db_connector.run_query(f"SELECT SNOWFLAKE.CORTEX.EMBED_TEXT_768('{self.embedding_model}', '{test_message}');")
                    result_value = next(iter(embedding_result[0].values()))
                    if result_value:
                        logger.info(f"Test result value len embedding: {len(result_value)}")
                        logger.info('Ok that worked, changing CORTEX_EMBEDDING_MODEL ENV VAR to snowflake-arctic-embed-m')
                        os.environ['CORTEX_EMBEDDING_MODEL'] = 'snowflake-arctic-embed-m'
                else:
                    raise(e)

        except Exception as e:
            logger.info('Cortex embed not available, query error: ',e)
            result_value = ""
        return result_value


    def store_table_summary(self, database, schema, table, ddl, ddl_short="", summary="", sample_data="", memory_uuid="", ddl_hash="", dataset=None, matching_connection=None):
        """
        Stores a document including the DDL and summary for a table in the memory system.
        :param schema: The schema name.
        :param table: The table name.
        :param ddl: The DDL statement of the table.
        :param summary: A natural language summary of the table.
        """

        try:
            if ddl is None:
                ddl = self.alt_get_ddl(table_name='"'+database+'"."'+schema+'"."'+table+'"')

            if os.environ.get("CORTEX_MODE", 'False') == 'True':
                memory_content = f"<OBJECT>{database}.{schema}.{table}</OBJECT><DDL_SHORT>{ddl_short}</DDL_SHORT>"
                complete_description = memory_content
            else:
                memory_content = f"<OBJECT>{database}.{schema}.{table}</OBJECT><DDL>\n{ddl}\n</DDL>\n<SUMMARY>\n{summary}\n</SUMMARY><DDL_SHORT>{ddl_short}</DDL_SHORT>"
                if sample_data != "":
                    memory_content += f"\n\n<SAMPLE CSV DATA>\n{sample_data}\n</SAMPLE CSV DATA>"
                complete_description = memory_content
            embedding = self.get_embedding(complete_description)
            # logger.info("we got the embedding!")
            #sample_data_text = json.dumps(sample_data)  # Assuming sample_data needs to be a JSON text.

            # Now using the modified method to insert the data into BigQuery
            self.db_connector.insert_table_summary(database_name=database,
                                                schema_name=schema,
                                                table_name=table,
                                                ddl=ddl,
                                                ddl_short=ddl_short,
                                                summary=summary,
                                                sample_data_text=sample_data,
                                                complete_description=complete_description,
                                                embedding=embedding,
                                                memory_uuid=memory_uuid,
                                                ddl_hash=ddl_hash,
                                                matching_connection=matching_connection)

            logger.info(f"Stored summary for an object in Harvest Results.")

        except Exception as e:
            logger.info(f"Harvester Error for an object: {e}")
            self.store_table_summary(database, schema, table, summary="Harvester Error: {e}", ddl="Harvester Error", ddl_short="Harvester Error", sample_data="Harvester Error")

    def generate_summary(self, prompt):
        p = [
            {"role": "system", "content": "You are an assistant that is great at explaining database tables and columns in natural language."},
            {"role": "user", "content": prompt}
        ]
        return self.run_prompt(p)




    def run_prompt(self, messages):
        # Check if prompt is cached
        prompt_key = str(messages)
        if prompt_key in self._prompt_cache:
            return self._prompt_cache[prompt_key]
        
        if os.environ.get("CORTEX_MODE", 'False') == 'True':
            escaped_messages = str(messages).replace("'", '\\"')
            query = f"select snowflake.cortex.complete('{self.cortex_model}','{escaped_messages}');"
            # logger.info(query)
            completion_result = self.db_connector.run_query(query)
            try:
                result_value = next(iter(completion_result[0].values()))
                if result_value:
                    result_value = str(result_value).replace("\`\`\`","'''")
                    # logger.info(f"Result value: {result_value}")
            except:
                logger.info('Cortext complete didnt work')
                result_value = ""
            return result_value
        else:
            response = self.client.chat.completions.create(
                model=self.model,  # Adjust the model name as necessary
                messages=messages
            )
            # Cache the response
            self._prompt_cache[prompt_key] = response.choices[0].message.content
            return response.choices[0].message.content

    def get_ddl_short(self, ddl):
        prompt = f'Here is the full DDL for a table:\n{ddl}\n\nPlease make a new summarized version of the ddl for this table.  If there are 15 or fewer fields, just include them all. If there are more, combine any that are similar and explain that there are more, and then pick the more important 15 fields to include in the ddl_summary.  Express it as DDL, but include comments about other similar fields, and then a comment summarizing the rest of the fields and noting to see the FULL_DDL to see all columns.  Return ONLY the summarized, do NOT include preamble or other post-result commentary.  Express it as a CREATE TABE statement like a regular DDL, using the exact same table name as I mentioned above (dont modify the table name in any way).'

        messages = [
            {"role": "system", "content": "You are an assistant that is great at taking full table DDL and creating shorter DDL summaries."},
            {"role": "user", "content": prompt}
        ]

        response = self.run_prompt(messages)

        return response

    def get_embedding(self, text):
        # logic to handle switch between openai and cortex
        if os.getenv("CORTEX_MODE", 'False') == 'True':
            escaped_messages = str(text[:512]).replace("'", "\\'")
            try:
                # review function used once new regions are unlocked in snowflake
                embedding_result = self.db_connector.run_query(f"SELECT SNOWFLAKE.CORTEX.EMBED_TEXT_768('{self.embedding_model}', '{escaped_messages}');")

                result_value = next(iter(embedding_result[0].values()))
                if result_value:
                    logger.info(f"Result value len embedding: {len(result_value)}")
            except:
                logger.info('Cortex embed text didnt work in schema explorer')
                result_value = ""
            return result_value
        else:
            try:
                response = self.client.embeddings.create(
                    model=self.embedding_model,
                    input=text[:8000].replace("\n", " ")  # Replace newlines with spaces
                )
                embedding = response.data[0].embedding
                if embedding:
                    logger.info(f"Result value len embedding: {len(embedding)}")
            except:
                logger.info('Openai embed text didnt work in schema explorer')
                embedding = ""
            return embedding

    def explore_schemas(self):
        try:
            for schema in self.db_connector.get_schemas():
            #    logger.info(f"Schema: {schema}")
                tables = self.db_connector.get_tables(schema)
                for table in tables:
        #           logger.info(f"  Table: {table}")
                    columns = self.db_connector.get_columns(schema, table)
                    for column in columns:
                        pass
                #      logger.info(f"    Column: {column}")
        except Exception as e:
            logger.info(f'Error running explore schemas Error: {e}')

    def generate_table_summary_prompt(self, database, schema, table, columns):
        prompt = f"Please provide a brief summary of a database table in the '{database}.{schema}' schema named '{table}'. This table includes the following columns: {', '.join(columns)}."
        return prompt

    def generate_column_summary_prompt(self, database, schema, table, column, sample_values):
        prompt = f"Explain the purpose of the '{column}' column in the '{table}' table, part of the '{database}.{schema}' schema. Example values include: {', '.join(map(str, sample_values))}."
        return prompt

    def get_datasets(self, database):
        try:
            datasets = self.db_connector.get_schemas(database)  # Assume this method exists and returns a list of dataset IDs
            return datasets
        except Exception as e:
            logger.info(f'Error running get schemas Error: {e}')


    def get_active_databases(self):

        databases = self.db_connector.get_databases()
        return [item for item in databases if item['status'] == 'Include']


    def load_custom_query(self, db_type, query_type):
        """
        Loads custom SQL query from local config file if it exists.
        
        Args:
            db_type (str): Database type (e.g. postgresql, mysql) 
            query_type (str): Type of query (e.g. get_schemas, get_tables)
            
        Returns:
            str: Custom SQL query if found, None otherwise
        """
        try:
            import configparser
            config = configparser.ConfigParser()
            if not config.read('./harvester_queries.conf'):
                return None
                
            db_type = db_type.lower()
            if db_type not in config:
                return None
                
            if query_type not in config[db_type]:
                return None
                
            query = config[db_type][query_type]
            # Strip quotes from start/end if present
            if query.startswith('"') and query.endswith('"'):
                query = query[1:-1]
            return query
            
        except Exception as e:
            logger.info(f'Error loading custom query for {db_type}.{query_type}: {e}')
            return None


    def get_active_schemas(self, database):

        if database['source_name'] == 'Snowflake':
            schemas = self.db_connector.get_schemas(database["database_name"])
        else:
            # handle non-snowflake sources
            from connectors.data_connector import DatabaseConnector
            connector = DatabaseConnector()
            # Get connection type for the source
            connections = connector.list_database_connections(bot_id='system', bot_id_override=True)
            if connections['success']:
                connections = connections['connections']
            else:
                logger.info(f'Error listing connections: {connections.get("error")}')
                return []
            # Find matching connection for database source
            matching_connection = None
            for conn in connections:
                if conn['connection_id'] == database['source_name']:
                    matching_connection = conn
                    break
            
            if matching_connection is None:
                logger.info(f"No matching connection found for source {database['source_name']}")
                return []

            db_type = matching_connection['db_type']
            if '+' in db_type:
                    db_type = db_type.split('+')[0]

            database_name = database['database_name']

            sql = self.load_custom_query(db_type, 'get_schemas')

            if sql is None:  
                schema_queries = {
                    'mysql': 'SELECT SCHEMA_NAME FROM information_schema.schemata WHERE SCHEMA_NAME NOT IN ("information_schema", "mysql", "performance_schema", "sys")',
                    'postgresql': 'SELECT schema_name FROM !database_name!.information_schema.schemata WHERE catalog_name = \'!database_name!\' AND schema_name NOT IN (\'information_schema\', \'pg_catalog\', \'pg_toast\')',
                    'sqlite': 'SELECT \'main\' as schema_name',
                    'snowflake': 'SHOW SCHEMAS IN DATABASE !database_name!'
                }
        
                # Check if we have a pre-defined query for this database type
                if db_type.lower() in schema_queries:
                    sql = schema_queries[db_type.lower()]

            if sql is None:
            # Generate prompt to get SQL for schema listing based on database type
                p = [
                    {"role": "user", "content": f"Write a SQL query to list all schemas in a {db_type} database named with the placeholder !database_name!, which will be replaced by the actual database name at runtime. Return only the SQL query without any explanation or additional text, with no markdown formatting. If the database is a sqlite database or other schema-less database, return the schema name as 'main'."}
                ]
                
                sql = self.run_prompt(p)
                # Execute the generated SQL query through the connector
                # Replace placeholder in SQL query
                
            sql = sql.replace('!database_name!', database_name)
            try:
                schemas = connector.query_database(connection_id=matching_connection['connection_id'], bot_id='system', query=sql, bot_id_override=True, database_name=database_name)
                if isinstance(schemas, list):
                    # Extract schema names from result set based on first column
                    schemas = [row[0] for row in schemas if row[0]]
                elif isinstance(schemas, dict) and 'rows' in schemas:
                    # Extract schema names from rows in dictionary result
                    schemas_out = []
                    for row in schemas['rows']:
                        if row[0]:
                            if isinstance(row[0], list):
                                schemas_out.extend(row[0])
                            else:
                                schemas_out.append(row[0])
                    schemas = schemas_out
                else:
                    logger.info(f"Unexpected schema query result format for {db_type}")
                    schemas = []
            except Exception as e:
                logger.info(f"Error getting schemas for {db_type}: {e}")
                schemas = []

        try:
            inclusions = database["schema_inclusions"]
            if isinstance(inclusions, str):
                inclusions = json.loads(inclusions.replace("'", '"'))
            if inclusions is None:
                inclusions = []
            if len(inclusions) == 0:
                
                # get the app-shared schemas BASEBALL & FORMULA_1
                # logger.info(f"get schemas for database: {database['database_name']} == {self.db_connector.project_id}")
                if database["database_name"] == self.db_connector.project_id:
                    shared_schemas = self.db_connector.get_shared_schemas(database["database_name"])
                    if shared_schemas:
                        if schemas is None:
                            schemas = []
                        schemas.extend(shared_schemas)
            else:
                schemas = inclusions
            exclusions = database["schema_exclusions"]
            if isinstance(exclusions, str):
                exclusions = json.loads(exclusions.replace("'", '"'))
            if exclusions is None:
                exclusions = []
            schemas = [schema for schema in schemas if schema not in exclusions]
            return schemas
        except Exception as e:
            logger.info(f"error - {e}")
            return []

    def update_initial_crawl_flag(self, database_name, crawl_flag):

        if self.db_connector.source_name == 'Snowflake':
            query = f"""
                update {self.db_connector.harvest_control_table_name}
                set initial_crawl_complete = {crawl_flag}
                where source_name = '{self.db_connector.source_name}' and database_name = '{database_name}';"""
            update_query = self.db_connector.run_query(query)
        elif self.db_connector.source_name == 'Sqlite':
            query = f"""
                update {self.db_connector.harvest_control_table_name}
                set initial_crawl_complete = {crawl_flag}
                where source_name = '{self.db_connector.source_name}' and database_name = '{database_name}';"""
            cursor = self.db_connector.client.cursor()
            cursor.execute(query)
            self.db_connector.client.commit()

    def get_table_columns(self, dataset, table_name):
        """
        Gets list of columns for a table in the given dataset.

        Args:
            dataset (dict): Dictionary containing source_name, database_name and schema_name
            table_name (str): Name of the table to get columns for

        Returns:
            list: List of column names for the table
        """
        if dataset['source_name'] == 'Snowflake':
            try:
                columns = self.db_connector.get_columns(
                    dataset['database_name'],
                    dataset['schema_name'],
                    table_name
                )
                return columns
            except Exception as e:
                logger.info(f'Error getting columns for table {table_name}: {e}')
                return []
        else:
            try:
                from connectors.data_connector import DatabaseConnector
                connector = DatabaseConnector()
                
                # Get connection type for the source
                connections = connector.list_database_connections(bot_id='system', bot_id_override=True)
                if connections['success']:
                    connections = connections['connections']
                else:
                    logger.info(f'Error listing connections: {connections.get("error")}')
                    return []
                
                # Find matching connection for database source
                matching_connection = None
                for conn in connections:
                    if conn['connection_id'] == dataset['source_name']:
                        matching_connection = conn
                        break
                
                if matching_connection is None:
                    logger.info(f"No matching connection found for source {dataset['source_name']}")
                    return []

                # Pre-defined column listing queries for common database types
                sql = None

                db_type = matching_connection['db_type'].split('+')[0] if '+' in matching_connection['db_type'] else matching_connection['db_type']
                sql = self.load_custom_query(db_type, 'get_columns')

                if sql is None:
                    column_queries = {
                    'mysql': 'SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = \'!schema_name!\' AND TABLE_NAME = \'!table_name!\' ORDER BY ORDINAL_POSITION',
                    'postgresql': 'SELECT column_name FROM information_schema.columns WHERE table_catalog = \'!database_name!\' AND table_schema = \'!schema_name!\' AND table_name = \'!table_name!\' ORDER BY ordinal_position',
                    'sqlite': 'SELECT name FROM pragma_table_info(\'!table_name!\')',
                    'snowflake': 'SHOW COLUMNS IN TABLE !database_name!.!schema_name!.!table_name!'
                }

                # Check if we have a pre-defined query for this database type
                    if matching_connection['db_type'].lower() in column_queries:
                        sql = column_queries[matching_connection['db_type'].lower()]

                    if sql is None:
                        # Generate prompt to get SQL for column listing based on database type
                        p = [
                            {"role": "user", "content": f"Write a SQL query to list all columns in a {matching_connection['db_type']} database table. Use placeholders !database_name!, !schema_name!, and !table_name! which will be replaced at runtime. Return only the SQL query without explanation, with no markdown, no ``` and no ```sql. The query should return a single column containing the column names."}
                        ]
                        
                        sql = self.run_prompt(p)
                    
                # Replace placeholders in SQL query
                sql = sql.replace('!database_name!', dataset['database_name'])
                sql = sql.replace('!schema_name!', dataset['schema_name'])
                sql = sql.replace('!table_name!', table_name)
                
                # Execute the generated SQL query through the connector
                result = connector.query_database(
                    connection_id=dataset['source_name'],
                    bot_id='system',
                    query=sql,
                    max_rows=1000,
                    max_rows_override=True,
                    bot_id_override=True,
                    database_name=dataset['database_name']
                )
                
                if isinstance(result, dict) and result.get('success'):
                    return [row[0] for row in result['rows']]
                elif isinstance(result, list):
                    return [row[0] for row in result]
                else:
                    logger.info(f'Error getting columns from {dataset["source_name"]}: {result.get("error") if isinstance(result, dict) else "Unknown error"}')
                    return []

            except Exception as e:
                logger.info(f'Error getting columns for table {table_name}: {e}')
                return []



    def explore_and_summarize_tables_parallel(self, max_to_process=1000):
        # called by standalone_harvester.py
        try:
            self.run_number += 1
            databases = self.get_active_databases()
            schemas = []
            harvesting_databases = []

            for database in databases:
                # logger.info(f"checking db {database['database_name']} with initial crawl flag= {database['initial_crawl_complete']}")
                crawl_flag = False
                if (database["initial_crawl_complete"] == False):
                    crawl_flag = True
                    self.update_initial_crawl_flag(database["database_name"],True)
                else:
                    if (database["refresh_interval"] > 0):
                        if (self.run_number % database["refresh_interval"] == 0):
                            crawl_flag = True

                cur_time = datetime.now()
                if crawl_flag:
                    harvesting_databases.append(database)
                    schemas.extend([{
                        'source_name': database["source_name"],
                        'database_name': database["database_name"],
                        'schema_name': schema
                    } for schema in self.get_active_schemas(database)])
              #      logger.info(f'Checking a Database for new or changed objects (cycle#: {self.run_number}, refresh every: {database["refresh_interval"]}) {cur_time}')
              #  else:
              #      logger.info(f'Skipping a Database, not in current refresh cycle (cycle#: {self.run_number}, refresh every: {database["refresh_interval"]} {cur_time})')

            summaries = {}
            total_processed = 0
        except Exception as e:
            logger.info(f'Error explore and summarize tables parallel Error: {e}')

        #logger.info('checking schemas: ',schemas)

        # todo, first build list of objects to harvest, then harvest them



        def process_dataset_step1(dataset, max_to_process = 1000):
            potential_tables = []
            matching_connection = None 
            if dataset['source_name'] == 'Snowflake':
                try:
                    potential_tables = self.db_connector.get_tables(dataset['database_name'], dataset['schema_name'])
                except Exception as e:
                    logger.info(f'Error running get potential tables Error: {e}')
            else:
                try:
                    from connectors.data_connector import DatabaseConnector
                    connector = DatabaseConnector()
                    # Get connection type for the source
                    connections = connector.list_database_connections(bot_id='system', bot_id_override=True)
                    if connections['success']:
                        connections = connections['connections']
                    else:
                        logger.info(f'Error listing connections: {connections.get("error")}')
                        return None, None
                    
                    # Find matching connection for database source
                    matching_connection = None
                    for conn in connections:
                        if conn['connection_id'] == dataset['source_name']:
                            matching_connection = conn
                            break
                    
                    if matching_connection is None:
                        logger.info(f"No matching connection found for source {dataset['source_name']}")
                        return None, None

                    db_type = matching_connection['db_type']
                    if '+' in db_type:
                        db_type = db_type.split('+')[0]
                    
                    database_name = dataset['database_name']
                    schema_name = dataset['schema_name']

                    # Pre-defined table listing queries for common database types
                    sql = self.load_custom_query(db_type, 'get_tables')

                    if sql is None:
                        table_queries = {
                            'mysql': 'SELECT TABLE_NAME as table_name FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = \'!schema_name!\'',
                            'oracle': 'SELECT table_name FROM all_tables WHERE owner = \'!schema_name!\' AND tablespace_name NOT IN (\'SYSTEM\', \'SYSAUX\')',
                            'postgresql': 'SELECT table_name FROM information_schema.tables WHERE table_catalog = \'!database_name!\' AND table_schema = \'!schema_name!\' AND table_type = \'BASE TABLE\'',
                            'sqlite': 'SELECT name as table_name FROM sqlite_master WHERE type=\'table\'',
                            'snowflake': 'SHOW TABLES IN SCHEMA !database_name!.!schema_name!'
                        }

                        # Check if we have a pre-defined query for this database type

                        if db_type.lower() in table_queries:
                            sql = table_queries[db_type.lower()]
                        else:
                            # Generate prompt to get SQL for table listing based on database type
                            p = [
                                {"role": "user", "content": f"Write a SQL query to list all tables in a {db_type} database named with the placeholder !database_name!, in the schema named with the placeholder !schema_name!, which will be replaced by the actual database name and schema name at runtime. Return only the SQL query without any explanation or additional text, with no markdown formatting. For schema-less databases like sqlite, do not use the placeholders as they aren't applicable. The query should return a single column named 'table_name'."}
                            ]
                        
                            sql = self.run_prompt(p)
                    
                    # Replace placeholders in SQL query
                    sql = sql.replace('!database_name!', database_name)
                    sql = sql.replace('!schema_name!', schema_name)
                    # Execute the generated SQL query through the connector
                    result = connector.query_database(
                        connection_id=dataset['source_name'],
                        bot_id='system',
                        query=sql,
                        max_rows=1000,
                        max_rows_override=True,
                        bot_id_override=True,
                        database_name=database_name
                    )
                    
                    if isinstance(result, dict) and result.get('success'):
                        potential_tables = []
                        for row in result['rows']:
                            potential_tables.append({'table_name': row[0]})
                    elif isinstance(result, list):
                        potential_tables = [{'table_name': row[0]} for row in result]
                    else:
                        logger.info(f'Error getting tables from {dataset["source_name"]}: {result.get("error") if isinstance(result, dict) else "Unknown error"}')
                        return None, None

                except Exception as e:
                    logger.info(f'Error connecting to {dataset["source_name"]}: {e}')
                    return None, None

            #logger.info('potential tables: ',potential_tables)
            non_indexed_tables = []

            # Check all potential_tables at once using a single query with an IN clause
            table_names = [table_info['table_name'] for table_info in potential_tables]

            db, sch = dataset['database_name'], dataset['schema_name']
            #quoted_table_names = [f'\'"{db}"."{sch}"."{table}"\'' for table in table_names]
            #in_clause = ', '.join(quoted_table_names)

            self.initialize_model()
            if os.environ.get("CORTEX_MODE", 'False') == 'True':
                embedding_column = 'embedding_native'
            else:
                embedding_column = 'embedding'

            if self.db_connector.source_name == 'Snowflake':
                check_query = f"""
                SELECT qualified_table_name, table_name, ddl_hash, last_crawled_timestamp, ddl, ddl_short, summary, sample_data_text, memory_uuid, (SUMMARY = '{{!placeholder}}') as needs_full, NULLIF(COALESCE(ARRAY_TO_STRING({embedding_column}, ','), ''), '') IS NULL as needs_embedding
                FROM {self.db_connector.metadata_table_name}
                WHERE  source_name = '{dataset['source_name']}'
                AND database_name= '{db}' and schema_name = '{sch}';"""
            else:
                check_query = f"""
                SELECT qualified_table_name, table_name, ddl_hash, last_crawled_timestamp, ddl, ddl_short, summary, sample_data_text, memory_uuid, (SUMMARY = '{{!placeholder}}') as needs_full, {embedding_column} IS NULL as needs_embedding
                FROM {self.db_connector.metadata_table_name}
                WHERE source_name = '{dataset['source_name']}'
                AND database_name= '{db}' and schema_name = '{sch}';"""               

            try:
                existing_tables_info = self.db_connector.run_query(check_query, max_rows=1000, max_rows_override=True)
                # Determine which field name is used for qualified table names
                # Convert all dictionary keys to uppercase for consistency
                existing_tables_info = [{k.upper(): v for k, v in table.items()} for table in existing_tables_info]
                table_names_field = 'QUALIFIED_TABLE_NAME'
                existing_tables_set = {info[table_names_field] for info in existing_tables_info}
                non_existing_tables = [table for table in potential_tables if f'"{db}"."{sch}"."{table["table_name"]}"' not in existing_tables_set]
                needs_updating = [table[table_names_field]  for table in existing_tables_info if table["NEEDS_FULL"]]
                needs_embedding = [(table['QUALIFIED_TABLE_NAME'], table['TABLE_NAME']) for table in existing_tables_info if table["NEEDS_EMBEDDING"]]
                refresh_tables = [table for table in potential_tables if f'"{db}"."{sch}"."{table["table_name"]}"' in needs_updating]
                # Print counts of each variable
                # logger.info(f"{db}.{sch}")
                # for tb in existing_tables_info:
                #     if tb['NEEDS_EMBEDDING']:
                #         logger.info(f"{tb['QUALIFIED_TABLE_NAME']} needs embedding: {tb['NEEDS_EMBEDDING']}")
                # logger.info(f"{check_query}")
            except Exception as e:
                logger.info(f'Error running check query Error: {e}')
                return None, None

            non_existing_tables.extend(refresh_tables)
            for table_info in non_existing_tables:
              #  print(table_info)
                try:
                    table_name = table_info['table_name']
                    quoted_table_name = f'"{db}"."{sch}"."{table_name}"'
                    # logger.info(f"checking {table_name} which is {quoted_table_name}")
                    if quoted_table_name not in existing_tables_set or quoted_table_name in needs_updating:
                        # Table is not in metadata table
                        # Check to see if it exists in the shared metadata table
                        #print ("!!!! CACHING DIsABLED !!!! ")
                        # get metadata from cache and add embeddings for all schemas, incl baseball and f1
                        if sch == 'INFORMATION_SCHEMA':
                            shared_table_exists = self.db_connector.check_cached_metadata('PLACEHOLDER_DB_NAME', sch, table_name)
                        else:
                            shared_table_exists = self.db_connector.check_cached_metadata(db, sch, table_name)
                        # shared_table_exists = False
                        if shared_table_exists:
                            # print ("!!!! CACHING Working !!!! ")
                            # Get the record from the shared metadata table with database name modified from placeholder
                            logger.info(f"Object cache hit for {table_name}")
                            get_from_cache_result = self.db_connector.get_metadata_from_cache(db, sch, table_name)
                            for record in get_from_cache_result:
                                database = record['database_name']
                                schema = record['schema_name']
                                table = record['table_name']
                                summary = record['summary']
                                ddl = record['ddl']
                                ddl_short = record['ddl_short']
                                sample_data = record['sample_data_text']

                                # call store memory
                                self.store_table_memory(database, schema, table, summary, ddl=ddl, ddl_short=ddl_short, sample_data=sample_data, dataset=dataset)

                        else:
                            # Table is new, so get its DDL and hash
                            current_ddl = self.alt_get_ddl(table_name=quoted_table_name, dataset=dataset, matching_connection=matching_connection)
                            current_ddl_hash = self.db_connector.sha256_hash_hex_string(current_ddl)
                            new_table = {"qualified_table_name": quoted_table_name, "ddl_hash": current_ddl_hash, "ddl": current_ddl, "dataset": dataset, "matching_connection": matching_connection}
                            logger.info('Newly found object added to harvest array (no cache hit)')
                            non_indexed_tables.append(new_table)

                            # store quick summary
                            # logger.info(f"is the table in the existing list?")
                            if quoted_table_name not in existing_tables_set:
                                # logger.info(f"yep, storing summary")
                                self.store_table_summary(database=db, schema=sch, table=table_name, ddl=current_ddl, ddl_short=current_ddl, summary="{!placeholder}", sample_data="", matching_connection=matching_connection)

                except Exception as e:
                    logger.info(f'Error processing table in step1: {e}')

            for table_info in needs_embedding:
                try:
                    quoted_table_name = table_info[0]
                    table_name = table_info[1]
                    # logger.info(f"embedding needed for {quoted_table_name}")

                    for current_info in existing_tables_info:
                        if current_info["QUALIFIED_TABLE_NAME"] == quoted_table_name:
                            current_ddl = current_info['DDL']
                            ddl_short = current_info['DDL_SHORT']
                            summary = current_info['SUMMARY']
                            sample_data_text = current_info['SAMPLE_DATA_TEXT']
                            memory_uuid = current_info['MEMORY_UUID']
                            ddl_hash = current_info['DDL_HASH']
                            self.store_table_summary(database=db, schema=sch, table=table_name, ddl=current_ddl, ddl_short=ddl_short, summary=summary, sample_data=sample_data_text, memory_uuid=memory_uuid, ddl_hash=ddl_hash)

                except Exception as e:
                    logger.info(f'Error processing table in step1 embedding refresh: {e}')

            return non_indexed_tables

        def process_dataset_step2( non_indexed_tables, max_to_process = 1000):

                local_summaries = {}
                if len(non_indexed_tables) > 0:
                    logger.info(f'starting indexing of {len(non_indexed_tables)} objects...')
                for row in non_indexed_tables:
                    try:
                        dataset = row.get('dataset', None)
                        matching_connection = row.get('matching_connection', None)
                        qualified_table_name = row.get('qualified_table_name',row)
                        logger.info("     -> An object")
                        database, schema, table = (part.strip('"') for part in qualified_table_name.split('.', 2))

                        # Proceed with generating the summary
                        columns = self.get_table_columns(dataset, table)

                        prompt = self.generate_table_summary_prompt(database, schema, table, columns)
                        summary = self.generate_summary(prompt)
                        #logger.info(summary)
                        #embedding = self.get_embedding(summary)
                        ddl = row.get('ddl',None)
                        ddl_short = self.get_ddl_short(ddl)
                        #logger.info(f"storing: database: {database}, schema: {schema}, table: {table}, summary len: {len(summary)}, ddl: {ddl}, ddl_short: {ddl_short} ")
                        logger.info('Storing summary for new object')
                        self.store_table_memory(database, schema, table, summary, ddl=ddl, ddl_short=ddl_short, dataset=dataset, matching_connection=matching_connection)
                    except Exception as e:
                        logger.info(f"Harvester Error on Object: {e}")
                        self.store_table_memory(database, schema, table, summary=f"Harvester Error: {e}", ddl="Harvester Error", ddl_short="Harvester Error", dataset=dataset)


                    local_summaries[qualified_table_name] = summary
                return local_summaries

        # Using ThreadPoolExecutor to parallelize dataset processing

        # MAIN LOOP

        tables_for_full_processing = []
        random.shuffle(schemas)
        try:
            logger.info(f'Checking {len(schemas)} schemas for new (not changed) objects.')
        except Exception as e:
            logger.info(f'Error printing schema count log line. {e}')


        for schema in schemas:
            tables_for_full_processing.extend(process_dataset_step1(schema))
        random.shuffle(tables_for_full_processing)
        process_dataset_step2(tables_for_full_processing)

        return 'Processed'

        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_dataset = {executor.submit(process_dataset, schema, max_to_process): schema for schema in schemas if total_processed < max_to_process}

            for future in as_completed(future_to_dataset):
                dataset = future_to_dataset[future]
                try:
                    _, dataset_summaries = future.result()
                    if dataset_summaries:
                        summaries.update(dataset_summaries)
                        total_processed += len(dataset_summaries)
                        if total_processed >= max_to_process:
                            break
                except Exception as exc:
                    logger.info(f'Dataset {dataset} generated an exception: {exc}')


    def get_sample_data(self, dataset, table_name):
        """
        Gets sample data for a table in the given dataset.

        Args:
            dataset (dict): Dictionary containing source_name, database_name and schema_name
            table_name (str): Name of the table to get sample data for

        Returns:
            list: List of dictionaries containing sample data rows
        """
        if dataset['source_name'] == 'Snowflake':
            try:
                sample_data = self.db_connector.get_sample_data(
                    dataset['database_name'],
                    dataset['schema_name'],
                    table_name
                )
                return sample_data
            except Exception as e:
                logger.info(f'Error getting sample data for table {table_name}: {e}')
                return []
        else:
            try:
                from connectors.data_connector import DatabaseConnector
                connector = DatabaseConnector()
                
                # Get connection type for the source
                connections = connector.list_database_connections(bot_id='system', bot_id_override=True)
                if connections['success']:
                    connections = connections['connections']
                else:
                    logger.info(f'Error listing connections: {connections.get("error")}')
                    return []
                
                # Find matching connection for database source
                matching_connection = None
                for conn in connections:
                    if conn['connection_id'] == dataset['source_name']:
                        matching_connection = conn
                        break
                
                if matching_connection is None:
                    logger.info(f"No matching connection found for source {dataset['source_name']}")
                    return []

                # Pre-defined sample data queries for common database types
                db_type = matching_connection['db_type'].split('+')[0] if '+' in matching_connection['db_type'] else matching_connection['db_type']
                sql = self.load_custom_query(db_type, 'get_sample_data')
                if sql is None:
                    sample_queries = {
                        'mysql': 'SELECT * FROM !schema_name!.!table_name! ORDER BY RAND() LIMIT 5',
                        'postgresql': 'SELECT * FROM !database_name!.!schema_name!.!table_name! ORDER BY RANDOM() LIMIT 5', 
                        'sqlite': 'SELECT * FROM !table_name! ORDER BY RANDOM() LIMIT 5',
                        'snowflake': 'SELECT * FROM !database_name!.!schema_name!.!table_name! SAMPLE (5 ROWS)'
                    }

                    if matching_connection['db_type'].lower() in sample_queries:
                        sql = sample_queries[matching_connection['db_type'].lower()]

                if sql is None:
                    # Generate prompt to get SQL for sample data based on database type
                    p = [
                        {"role": "user", "content": f"Write a SQL query to get a sample of rows from a {matching_connection['db_type']} database table. Use placeholders !database_name!, !schema_name!, and !table_name! which will be replaced at runtime. The query should return a random sample of 5 rows. Return only the SQL query without explanation, with no markdown formatting. IF the database type does not support schemas (like sqlite), do not use the database and schema placeholders."}
                    ]
                    
                    sql = self.run_prompt(p)
                
                # Replace placeholders in SQL query
                sql = sql.replace('!database_name!', dataset['database_name'])
                sql = sql.replace('!schema_name!', dataset['schema_name'])
                sql = sql.replace('!table_name!', table_name)
                
                # Execute the generated SQL query through the connector
                result = connector.query_database(
                    connection_id=dataset['source_name'],
                    bot_id='system',
                    query=sql,
                    max_rows=5,
                    max_rows_override=True,
                    bot_id_override=True,
                    database_name=dataset['database_name']
                )
                
                if isinstance(result, dict) and result.get('success'):
                    # Convert rows to list of dictionaries with column names as keys
                    columns = [col for col in result['columns']]
                    return [dict(zip(columns, row)) for row in result['rows']]
                elif isinstance(result, list):
                    # Assume first row contains column names
                    columns = result[0]
                    return [dict(zip(columns, row)) for row in result[1:]]
                else:
                    logger.info(f'Error getting sample data from {dataset["source_name"]}: {result.get("error") if isinstance(result, dict) else "Unknown error"}')
                    return []

            except Exception as e:
                logger.info(f'Error getting sample data for table {table_name}: {e}')
                return []


