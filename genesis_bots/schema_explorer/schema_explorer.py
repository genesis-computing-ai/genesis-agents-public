import os, csv, io
# import time
import simplejson as json
from openai import OpenAI
import random
#from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

from genesis_bots.llm.llm_openai.openai_utils import get_openai_client
from genesis_bots.core.logging_config import logger
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
            self.cortex_model = os.getenv("CORTEX_HARVESTER_MODEL", 'llama3.1-405b')
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

    def alt_get_ddl(self, table_name=None, dataset=None, matching_connection=None, object_type='TABLE'):
        """Get DDL using the appropriate query based on object type"""
        if dataset['source_name'] == 'Snowflake':
            return self.db_connector.alt_get_ddl(table_name)  # Snowflake handles views internally
        else:
            try:
                from genesis_bots.connectors.data_connector import DatabaseConnector
                connector = DatabaseConnector()
                
                db_type = matching_connection['db_type'].split('+')[0] if '+' in matching_connection['db_type'] else matching_connection['db_type']
                
                if matching_connection.get('connection_string'):
                    if '.redshift.' in matching_connection['connection_string'].lower() or '.redshift-serverless.' in matching_connection['connection_string'].lower():
                        db_type = 'redshift'                

                # Use get_view_ddl for views, get_ddl for tables
                query_type = 'get_view_ddl' if object_type == 'VIEW' else 'get_ddl'
                sql = self.load_custom_query(db_type, query_type)
                # Handle Redshift DDL queries separately since they need special handling
                if sql is None and db_type == 'redshift':
                    if object_type == 'VIEW':
                        sql = """SELECT 'CREATE OR REPLACE VIEW ' || schemaname || '.' || viewname || ' AS ' || definition 
                                FROM pg_views 
                                WHERE schemaname = '!schema_name!' AND viewname = '!table_name!'"""
                    else:
                        # Get raw column data from pg_table_def
                        sql = """SELECT "column", "notnull", "type"
                                FROM pg_table_def 
                                WHERE schemaname = '!schema_name!' 
                                AND tablename = '!table_name!';"""
                        
                        try:
                            result = connector.query_database(
                                connection_id=dataset['source_name'],
                                bot_id='system',
                                query=sql.replace('!schema_name!', dataset['schema_name']).replace('!table_name!', table_name.split('.')[-1].strip('"')),
                                max_rows=1000,
                                max_rows_override=True,
                                bot_id_override=True,
                                database_name=dataset['database_name']
                            )
                            
                            if isinstance(result, dict) and result.get('success') and result.get('rows'):
                                # Build DDL in Python
                                columns = []
                                for row in result['rows']:
                                    # Convert all values to strings and handle None values
                                    column_name = str(row[0]) if row[0] is not None else ''
                                    column_type = str(row[1]) if row[1] is not None else ''
                                    is_notnull = bool(row[2]) if row[2] is not None else False
                                    
                                    # Only proceed if we have valid column name and type
                                    if column_name and column_type:
                                        column_def = f'    "{column_name}" {column_type}'
                                        if is_notnull:
                                            column_def += " NOT NULL"
                                        columns.append(column_def)
                                
                                if columns:  # Only create DDL if we have valid columns
                                    # Assemble the complete DDL
                                    table_name_clean = table_name.split('.')[-1].strip('"')
                                    ddl = f'CREATE TABLE "{dataset["schema_name"]}"."{table_name_clean}" (\n'
                                    ddl += ',\n'.join(columns)
                                    ddl += '\n);'
                                    return ddl
                                
                            return "CREATE TABLE (No columns found);"
                            
                        except Exception as e:
                            logger.info(f'Error building DDL from pg_table_def: {e}')
                            return f"CREATE TABLE (Error: {str(e)});"
                if sql:
                    sql = sql.replace('!database_name!', dataset['database_name'])
                    sql = sql.replace('!schema_name!', dataset['schema_name'])
                    sql = sql.replace('!table_name!', table_name.split('.')[-1].strip('"'))
                    
                    result = connector.query_database(
                        connection_id=dataset['source_name'],
                        bot_id='system',
                        query=sql,
                        max_rows=1,
                        max_rows_override=True,
                        bot_id_override=True,
                        database_name=dataset['database_name']
                    )
                    
                    if matching_connection['db_type'] == 'snowflake' and not result.get('success', True):
                        # to get ddl for shared tables in Snowflake
                        if table_name:
                            describe_query = f"DESCRIBE TABLE {table_name};"
                            try:
                                describe_result = connector.query_database(
                                    connection_id=dataset['source_name'],
                                    bot_id='system',
                                    query=describe_query,
                                    max_rows=1000,
                                    max_rows_override=True,
                                    bot_id_override=True,
                                    database_name=dataset['database_name']
                                )
                            except:
                                return "CREATE TABLE (Error: Failed to retrieve DDL from Snowflake);"

                            ddl_statement = f"CREATE TABLE {table_name} (\n"
                            columns = describe_result['columns']
                            rows = describe_result['rows']
                            for row in rows:
                                column_name = row[columns.index('name')]
                                column_type = row[columns.index('type')]
                                nullable = " NOT NULL" if row[columns.index('null?')] == 'N' else ""
                                default = f" DEFAULT {row[columns.index('default')]}" if row[columns.index('default')] is not None else ""
                                comment = f" COMMENT '{row[columns.index('comment')]}'" if row[columns.index('comment')] is not None else ""
                                key = ""
                                if row[columns.index('primary key')] == 'Y':
                                    key = " PRIMARY KEY"
                                elif row[columns.index('unique key')] == 'Y':
                                    key = " UNIQUE"
                                ddl_statement += f"    {column_name} {column_type}{nullable}{default}{key}{comment},\n"
                            ddl_statement = ddl_statement.rstrip(',\n') + "\n);"
                            return ddl_statement
                       
                    # Handle different result formats
                    if isinstance(result, dict) and result.get('success') and result.get('rows'):
                        if isinstance(result['rows'][0], dict):
                            return next(iter(result['rows'][0].values()))
                        else:
                            return result['rows'][0][0]
                    elif isinstance(result, list) and result:
                        if isinstance(result[0], dict):
                            return next(iter(result[0].values()))
                        else:
                            return result[0][0]
                
                return None
                
            except Exception as e:
                logger.info(f'Error getting DDL: {e}')
                return None

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

    def store_table_memory(self, database, schema, table, summary=None, ddl=None, ddl_short=None, sample_data=None, dataset=None, matching_connection=None, object_type='TABLE'):
        """
        Generates a document including the DDL statement and a natural language description for a table.
        :param schema: The schema name.
        :param table: The table name.
        :param object_type: The type of database object (TABLE or VIEW)
        """
        try:
            if ddl is None:
                ddl = self.alt_get_ddl(table_name='"'+database+'"."'+schema+'"."'+table+'"', 
                                     dataset=dataset, 
                                     matching_connection=matching_connection,
                                     object_type=object_type)

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
                    logger.info(f'Model {self.cortex_model} not available in this region, trying llama3.1-70b')
                    self.cortex_model = 'llama3.1-70b'
                    cortex_query = f"""
                        select SNOWFLAKE.CORTEX.COMPLETE('{self.cortex_model}', %s) as completion; """
                    cursor.execute(cortex_query, (new_array_str,))
                    logger.info('Ok that worked, changing CORTEX_HARVESTER_MODEL ENV VAR to mistral-7b')
                    os.environ['CORTEX_HARVESTER_MODEL'] = 'llama3.1-70b'
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
                    result_value = str(result_value).replace(r"\`\`\`","'''")
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
            if not config.read('./genesis_bots/default_config/harvester_queries.conf'):
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
            from genesis_bots.connectors.data_connector import DatabaseConnector
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

            # Check if connection string contains redshift and update db_type accordingly
            if matching_connection.get('connection_string'):
                if '.redshift.' in matching_connection['connection_string'].lower() or '.redshift-serverless.' in matching_connection['connection_string'].lower():
                    db_type = 'redshift'
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
                
                if db_type == 'snowflake' and matching_connection['connection_id'] != 'Snowflake':
                    # For Snowflake, find the 'name' column index
                    name_col = 1  # Default to 1 which is typical for SHOW SCHEMAS
                    if isinstance(schemas, dict) and 'columns' in schemas:
                        for i, col in enumerate(schemas['columns']):
                            if isinstance(col, str) and col.lower() == 'name':
                                name_col = i
                                break
                    # Update rows to use the correct name column
                    if 'rows' in schemas:
                        schemas = [row[name_col] for row in schemas['rows']]
                else:
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
                from genesis_bots.connectors.data_connector import DatabaseConnector
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

                db_type = matching_connection['db_type'].split('+')[0] if '+' in matching_connection['db_type'] else matching_connection['db_type']
                
                # Check if it's Redshift
                if matching_connection.get('connection_string'):
                    if '.redshift.' in matching_connection['connection_string'].lower() or '.redshift-serverless.' in matching_connection['connection_string'].lower():
                        db_type = 'redshift'
                
                if db_type == 'redshift':
                    # Use pg_table_def for Redshift
                    sql = """SELECT "column"
                            FROM pg_table_def 
                            WHERE schemaname = '!schema_name!' 
                            AND tablename = '!table_name!';
                           """
                else:
                    # Pre-defined column listing queries for other database types
                    sql = self.load_custom_query(db_type, 'get_columns')

                    if sql is None:
                        column_queries = {
                            'mysql': 'SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = \'!schema_name!\' AND TABLE_NAME = \'!table_name!\' ORDER BY ORDINAL_POSITION',
                            'postgresql': 'SELECT column_name FROM information_schema.columns WHERE table_catalog = \'!database_name!\' AND table_schema = \'!schema_name!\' AND table_name = \'!table_name!\' ORDER BY ordinal_position',
                            'sqlite': 'SELECT name FROM pragma_table_info(\'!table_name!\')',
                            'snowflake': 'SHOW COLUMNS IN TABLE !database_name!.!schema_name!.!table_name!'
                        }

                        if db_type.lower() in column_queries:
                            sql = column_queries[db_type.lower()]

                        if sql is None:
                            # Generate prompt to get SQL for column listing based on database type
                            p = [
                                {"role": "user", "content": f"Write a SQL query to list all columns in a {db_type} database table. Use placeholders !database_name!, !schema_name!, and !table_name! which will be replaced at runtime. Return only the SQL query without explanation, with no markdown, no ``` and no ```sql. The query should return a single column containing the column names."}
                            ]
                            sql = self.run_prompt(p)

                # Replace placeholders in SQL query
                sql = sql.replace('!database_name!', dataset['database_name'])
                sql = sql.replace('!schema_name!', dataset['schema_name'])
                sql = sql.replace('!table_name!', table_name.split('.')[-1].strip('"'))

                # Execute the query
                result = connector.query_database(
                    connection_id=dataset['source_name'],
                    bot_id='system',
                    query=sql,
                    max_rows=1000,
                    max_rows_override=True,
                    bot_id_override=True,
                    database_name=dataset['database_name']
                )

                # Find the column index containing the column names
                col_num = 0
                if 'columns' in result and result['columns']:
                    for i, col in enumerate(result['columns']):
                        if col.lower() == 'column_name' or col.lower() == 'name':
                            col_num = i
                            break
                if isinstance(result, dict) and result.get('success'):
                    # Extract column names from the result
                    columns = []
                    for row in result['rows']:
                        if row[col_num] is not None:  # Check for None values
                            columns.append(str(row[col_num]))  # Convert to string
                    return columns

                return []

            except Exception as e:
                logger.info(f'Error getting columns for table {table_name}: {e}')
                return []


    def explore_and_summarize_tables_parallel(self, max_to_process=1000, dataset_filter=None):
        try:
            self.run_number += 1
            databases = self.get_active_databases()
            
            # Apply dataset filter if provided
            if dataset_filter:
                databases = [db for db in databases if 
                           (not dataset_filter.get('database_name') or 
                            db['database_name'] == dataset_filter['database_name']) and
                           (not dataset_filter.get('source_name') or 
                            db['source_name'] == dataset_filter['source_name'])]
                
            schemas = []
            harvesting_databases = []

            for database in databases:
                crawl_flag = False
                if (database["initial_crawl_complete"] == False):
                    crawl_flag = True
                    self.update_initial_crawl_flag(database["database_name"], True)
                else:
                    if (database["refresh_interval"] > 0):
                        if (self.run_number % database["refresh_interval"] == 0):
                            crawl_flag = True

                # Force crawl if this is an immediate harvest request
                if dataset_filter:
                    crawl_flag = True

                cur_time = datetime.now()
                if crawl_flag:
                    harvesting_databases.append(database)
                    schemas.extend([{
                        'source_name': database["source_name"],
                        'database_name': database["database_name"],
                        'schema_name': schema
                    } for schema in self.get_active_schemas(database)])

            summaries = {}
            total_processed = 0
        except Exception as e:
            logger.info(f'Error explore and summarize tables parallel Error: {e}')

        #logger.info('checking schemas: ',schemas)

        # todo, first build list of objects to harvest, then harvest them

        def process_dataset_step1(dataset, max_to_process = 1000):
            potential_objects = []
            matching_connection = None
            if dataset['source_name'] == 'Snowflake':
                try:
                    # For Snowflake, just get tables as before - it already handles views
                    potential_objects = self.db_connector.get_tables(dataset['database_name'], dataset['schema_name'])
                    for obj in potential_objects:
                        obj['object_type'] = 'TABLE'  # Default, but Snowflake handles views internally
                except Exception as e:
                    logger.info(f'Error running get potential objects Error: {e}')
            else:
                try:
                    from genesis_bots.connectors.data_connector import DatabaseConnector
                    connector = DatabaseConnector()
                    connections = connector.list_database_connections(bot_id='system', bot_id_override=True)
                    if connections['success']:
                        connections = connections['connections']
                    else:
                        logger.info(f'Error listing connections: {connections.get("error")}')
                        return None

                    matching_connection = None
                    for conn in connections:
                        if conn['connection_id'] == dataset['source_name']:
                            matching_connection = conn
                            break

                    if matching_connection is None:
                        logger.info(f"No matching connection found for source {dataset['source_name']}")
                        return None

                    db_type = matching_connection['db_type']
                    if '+' in db_type:
                        db_type = db_type.split('+')[0]

                    database_name = dataset['database_name']
                    schema_name = dataset['schema_name']

                    if matching_connection.get('connection_string'):
                        if '.redshift.' in matching_connection['connection_string'].lower() or '.redshift-serverless.' in matching_connection['connection_string'].lower():
                            db_type = 'redshift'                

                    # Get tables
                    sql_tables = self.load_custom_query(db_type, 'get_tables')
                    if sql_tables:
                        sql_tables = sql_tables.replace('!database_name!', database_name)
                        sql_tables = sql_tables.replace('!schema_name!', schema_name)
                        result_tables = connector.query_database(
                            connection_id=dataset['source_name'],
                            bot_id='system',
                            query=sql_tables,
                            max_rows=1000,
                            max_rows_override=True,
                            bot_id_override=True,
                            database_name=database_name
                        )

                        col_num = 0
                        if 'columns' in result_tables and result_tables['columns']:
                            for i, col in enumerate(result_tables['columns']):
                                if col.lower() == 'name':
                                    col_num = i
                                    break
                        if isinstance(result_tables, dict) and result_tables.get('success'):
                            for row in result_tables['rows']:
                                potential_objects.append({
                                    'name': row[col_num],
                                    'object_type': 'TABLE'
                                })

                    # Get views
                    sql_views = self.load_custom_query(db_type, 'get_views')
                    if sql_views:
                        sql_views = sql_views.replace('!database_name!', database_name)
                        sql_views = sql_views.replace('!schema_name!', schema_name)
                        result_views = connector.query_database(
                            connection_id=dataset['source_name'],
                            bot_id='system',
                            query=sql_views,
                            max_rows=1000,
                            max_rows_override=True,
                            bot_id_override=True,
                            database_name=database_name
                        )
                        col_num = 0
                        if 'columns' in result_views and result_views['columns']:
                            for i, col in enumerate(result_views['columns']):
                                if col.lower() == 'name':
                                    col_num = i
                                    break
                        if isinstance(result_views, dict) and result_views.get('success'):
                            for row in result_views['rows']:
                                potential_objects.append({
                                    'name': row[col_num],
                                    'object_type': 'VIEW'
                                })
                except Exception as e:
                    logger.error(f"Error getting objects from database: {e}")
                    potential_objects = []

            non_indexed_tables = []

            # Check all potential_tables at once using a single query with an IN clause
            db, sch = dataset['database_name'], dataset['schema_name']

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
                existing_tables_info = [{k.upper(): v for k, v in table.items()} for table in existing_tables_info]
                table_names_field = 'QUALIFIED_TABLE_NAME'
                existing_tables_set = {info[table_names_field] for info in existing_tables_info}
                
                # Process new or updated objects
                for obj in potential_objects:
                    try:
                        if 'name' in obj and 'table_name' not in obj:
                            obj['table_name'] = obj['name']
                        qualified_name = f'"{db}"."{sch}"."{obj["table_name"]}"'
                        if qualified_name not in existing_tables_set:
                            # Table is new, so get its DDL and hash
                            current_ddl = self.alt_get_ddl(
                                table_name=qualified_name, 
                                dataset=dataset, 
                                matching_connection=matching_connection,
                                object_type=obj['object_type']
                            )
                            current_ddl_hash = self.db_connector.sha256_hash_hex_string(current_ddl)
                            new_table = {
                                "qualified_table_name": qualified_name, 
                                "ddl_hash": current_ddl_hash, 
                                "ddl": current_ddl, 
                                "dataset": dataset, 
                                "matching_connection": matching_connection,
                                "object_type": obj['object_type']
                            }
                            logger.info('Newly found object added to harvest array (no cache hit)')
                            non_indexed_tables.append(new_table)

                            if qualified_name not in existing_tables_set:
                                self.store_table_summary(
                                    database=db, 
                                    schema=sch, 
                                    table=obj["table_name"], 
                                    ddl=current_ddl, 
                                    ddl_short=current_ddl, 
                                    summary="{!placeholder}", 
                                    sample_data="", 
                                    matching_connection=matching_connection
                                )

                    except Exception as e:
                        logger.info(f'Error processing table in step1: {e}')

                # Process objects needing embedding updates
                needs_updating = [table['QUALIFIED_TABLE_NAME'] for table in existing_tables_info if table["NEEDS_FULL"]]
                needs_embedding = [(table['QUALIFIED_TABLE_NAME'], table['TABLE_NAME']) for table in existing_tables_info if table["NEEDS_EMBEDDING"]]
                
                for obj in potential_objects:
                    qualified_name = f'"{db}"."{sch}"."{obj["table_name"]}"'
                    if qualified_name in needs_updating:
                        current_ddl = self.alt_get_ddl(
                            table_name=qualified_name, 
                            dataset=dataset, 
                            matching_connection=matching_connection,
                            object_type=obj['object_type']
                        )
                        current_ddl_hash = self.db_connector.sha256_hash_hex_string(current_ddl)
                        new_table = {
                            "qualified_table_name": qualified_name, 
                            "ddl_hash": current_ddl_hash, 
                            "ddl": current_ddl, 
                            "dataset": dataset, 
                            "matching_connection": matching_connection,
                            "object_type": obj['object_type']
                        }
                        non_indexed_tables.append(new_table)

                for table_info in needs_embedding:
                    try:
                        qualified_name = table_info[0]
                        table_name = table_info[1]
                        
                        for current_info in existing_tables_info:
                            if current_info["QUALIFIED_TABLE_NAME"] == qualified_name:
                                current_ddl = current_info['DDL']
                                ddl_short = current_info['DDL_SHORT']
                                summary = current_info['SUMMARY']
                                sample_data_text = current_info['SAMPLE_DATA_TEXT']
                                memory_uuid = current_info['MEMORY_UUID']
                                ddl_hash = current_info['DDL_HASH']
                                self.store_table_summary(
                                    database=db, 
                                    schema=sch, 
                                    table=table_name, 
                                    ddl=current_ddl, 
                                    ddl_short=ddl_short, 
                                    summary=summary, 
                                    sample_data=sample_data_text, 
                                    memory_uuid=memory_uuid, 
                                    ddl_hash=ddl_hash
                                )

                    except Exception as e:
                        logger.info(f'Error processing table in step1 embedding refresh: {e}')

                return non_indexed_tables

            except Exception as e:
                logger.info(f'Error running check query Error: {e}')
                return None

        def process_dataset_step2(self, non_indexed_tables, max_to_process = 1000):
            try:
                local_summaries = {}
                if len(non_indexed_tables) > 0:
                    logger.info(f'starting indexing of {len(non_indexed_tables)} objects...')
                
                for row in non_indexed_tables:
                    database = None
                    schema = None
                    table = None
                    summary = None
                    qualified_table_name = None
                    
                    try:
                        dataset = row.get('dataset', None)
                        matching_connection = row.get('matching_connection', None)
                        object_type = row.get('object_type', 'TABLE')  # Default to TABLE if not specified
                        qualified_table_name = row.get('qualified_table_name',row)
                        logger.info("     -> An object")
                        database, schema, table = (part.strip('"') for part in qualified_table_name.split('.', 2))

                        # Proceed with generating the summary
                        columns = self.get_table_columns(dataset, table)

                        prompt = self.generate_table_summary_prompt(database, schema, table, columns)
                        summary = self.generate_summary(prompt)
                        ddl = row.get('ddl',None)
                        ddl_short = self.get_ddl_short(ddl)
                        logger.info('Storing summary for new object')
                        self.store_table_memory(
                            database, 
                            schema, 
                            table, 
                            summary, 
                            ddl=ddl, 
                            ddl_short=ddl_short, 
                            dataset=dataset, 
                            matching_connection=matching_connection,
                            object_type=object_type  # Pass through the object type
                        )
                        
                        if qualified_table_name:
                            local_summaries[qualified_table_name] = summary
                            
                    except Exception as e:
                        logger.info(f"Harvester Error on Object: {e}")
                        if all([database, schema, table]):
                            self.store_table_memory(
                                database, 
                                schema, 
                                table, 
                                summary=f"Harvester Error: {e}", 
                                ddl="Harvester Error", 
                                ddl_short="Harvester Error", 
                                dataset=dataset
                            )
                
                return local_summaries
                
            except Exception as e:
                logger.error(f"Error in process_dataset_step2: {e}")
                return {}

        # Using ThreadPoolExecutor to parallelize dataset processing

        # MAIN LOOP

        tables_for_full_processing = []
        random.shuffle(schemas)
        try:
            logger.info(f"Harvester checking {len(schemas)} schemas for new objects.")
        except Exception as e:
            logger.info(f'Error printing schema count log line. {e}')


        for schema in schemas:
            tables_for_full_processing.extend(process_dataset_step1(schema))
        random.shuffle(tables_for_full_processing)
        process_dataset_step2(self, tables_for_full_processing)

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
                from genesis_bots.connectors.data_connector import DatabaseConnector
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

                if matching_connection.get('connection_string'):
                    if '.redshift.' in matching_connection['connection_string'].lower() or '.redshift-serverless.' in matching_connection['connection_string'].lower():
                        db_type = 'redshift'                

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


