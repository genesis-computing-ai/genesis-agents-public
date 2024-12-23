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

    def alt_get_ddl(self,table_name = None):

        return self.db_connector.alt_get_ddl(table_name)

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

    def store_table_memory(self, database, schema, table, summary=None, ddl=None, ddl_short=None, sample_data=None):
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
                    sample_data = self.db_connector.get_sample_data(database, schema, table)
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
            self.store_table_summary(database, schema, table, ddl=ddl, ddl_short=ddl_short,summary=summary, sample_data=sample_data_str)

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


    def store_table_summary(self, database, schema, table, ddl, ddl_short="", summary="", sample_data="", memory_uuid="", ddl_hash=""):
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
                                                ddl_hash=ddl_hash)

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


    def get_active_schemas(self, database):

        try:
            inclusions = database["schema_inclusions"]
            if isinstance(inclusions, str):
                inclusions = json.loads(inclusions.replace("'", '"'))
            if inclusions is None:
                inclusions = []
            if len(inclusions) == 0:
                schemas = self.db_connector.get_schemas(database["database_name"])
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
                    schemas.extend([database["database_name"]+"."+schema for schema in self.get_active_schemas(database)])
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
            #logger.info("  Process_dataset: ",dataset)
            # query to find new
            # tables, or those with changes to their DDL

 #           logger.info('Checking a schema for new (not changed) objects.')
            if self.db_connector.source_name == 'Snowflake' or self.db_connector.source_name == 'Sqlite':
                try:
                    potential_tables = self.db_connector.get_tables(dataset.split('.')[0], dataset.split('.')[1])
                except Exception as e:
                    logger.info(f'Error running get potential tables Error: {e}')
                #logger.info('potential tables: ',potential_tables)
                non_indexed_tables = []

                # Check all potential_tables at once using a single query with an IN clause
                table_names = [table_info['table_name'] for table_info in potential_tables]
                db, sch = dataset.split('.')[0], dataset.split('.')[1]
                #quoted_table_names = [f'\'"{db}"."{sch}"."{table}"\'' for table in table_names]
                #in_clause = ', '.join(quoted_table_names)

                self.initialize_model()
                if os.environ.get("CORTEX_MODE", 'False') == 'True':
                    embedding_column = 'embedding_native'
                else:
                    embedding_column = 'embedding'

                check_query = f"""
                SELECT qualified_table_name, table_name, ddl_hash, last_crawled_timestamp, ddl, ddl_short, summary, sample_data_text, memory_uuid, (SUMMARY = '{{!placeholder}}') as needs_full, NULLIF(COALESCE(ARRAY_TO_STRING({embedding_column}, ','), ''), '') IS NULL as needs_embedding
                FROM {self.db_connector.metadata_table_name}
                WHERE source_name = '{self.db_connector.source_name}'
                AND database_name= '{db}' and schema_name = '{sch}';"""
                try:
                    existing_tables_info = self.db_connector.run_query(check_query, max_rows=1000, max_rows_override=True)
                    existing_tables_set = {info['QUALIFIED_TABLE_NAME'] for info in existing_tables_info}
                    non_existing_tables = [table for table in potential_tables if f'"{db}"."{sch}"."{table["table_name"]}"' not in existing_tables_set]
                    needs_updating = [table['QUALIFIED_TABLE_NAME']  for table in existing_tables_info if table["NEEDS_FULL"]]
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
                    print(table_info)
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
                                    self.store_table_memory(database, schema, table, summary, ddl=ddl, ddl_short=ddl_short, sample_data=sample_data)

                            else:
                                # Table is new, so get its DDL and hash
                                current_ddl = self.alt_get_ddl(table_name=quoted_table_name)
                                current_ddl_hash = self.db_connector.sha256_hash_hex_string(current_ddl)
                                new_table = {"qualified_table_name": quoted_table_name, "ddl_hash": current_ddl_hash, "ddl": current_ddl}
                                logger.info('Newly found object added to harvest array (no cache hit)')
                                non_indexed_tables.append(new_table)

                                # store quick summary
                                # logger.info(f"is the table in the existing list?")
                                if quoted_table_name not in existing_tables_set:
                                    # logger.info(f"yep, storing summary")
                                    self.store_table_summary(database=db, schema=sch, table=table_name, ddl=current_ddl, ddl_short=current_ddl, summary="{!placeholder}", sample_data="")

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
                        qualified_table_name = row.get('qualified_table_name',row)
                        logger.info("     -> An object")
                        database, schema, table = (part.strip('"') for part in qualified_table_name.split('.', 2))

                        # Proceed with generating the summary
                        columns = self.db_connector.get_columns(database, schema, table)
                        prompt = self.generate_table_summary_prompt(database, schema, table, columns)
                        summary = self.generate_summary(prompt)
                        #logger.info(summary)
                        #embedding = self.get_embedding(summary)
                        ddl = row.get('ddl',None)
                        ddl_short = self.get_ddl_short(ddl)
                        #logger.info(f"storing: database: {database}, schema: {schema}, table: {table}, summary len: {len(summary)}, ddl: {ddl}, ddl_short: {ddl_short} ")
                        logger.info('Storing summary for new object')
                        self.store_table_memory(database, schema, table, summary, ddl=ddl, ddl_short=ddl_short)
                    except Exception as e:
                        logger.info(f"Harvester Error on Object: {e}")
                        self.store_table_memory(database, schema, table, summary=f"Harvester Error: {e}", ddl="Harvester Error", ddl_short="Harvester Error")


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

