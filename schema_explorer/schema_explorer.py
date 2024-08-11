import os, csv, io
# import time
import simplejson as json
from openai import OpenAI
import random 
#from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
from datetime import datetime

# Assuming OpenAI SDK initialization

class SchemaExplorer:
    def __init__(self, db_connector, llm_api_key):
        from core.bot_os_llm import LLMKeyHandler
        self.db_connector = db_connector
        self.llm_api_key = llm_api_key
        self.run_number = 0

        #TODO fix this determine if using openAI or cortex here
        # llm_api_key = None
        # self.llm_key_handler = LLMKeyHandler()
        # api_key_from_env, llm_api_key = self.llm_key_handler.get_llm_key_from_env()
        # if api_key_from_env == False and self.db_connector.connection_name == "Snowflake":
        #     print('Checking LLM_TOKENS for saved LLM Keys:')
        #     llm_keys_and_types = []
        #     llm_keys_and_types = self.db_connector.db_get_llm_key()
        #     llm_api_key = self.llm_key_handler.check_llm_key(llm_keys_and_types)
        # if llm_api_key is None:
        #     pass
        self.initialize_model()

    def initialize_model(self):
        if os.environ["CORTEX_MODE"] == 'True':
            self.cortex_model = os.getenv("CORTEX_HARVESTER_MODEL", 'reka-flash')
            self.cortex_embedding_model = os.getenv("CORTEX_EMBEDDING_MODEL", 'e5-base-v2')
            if os.getenv("CORTEX_EMBEDDING_AVAILABLE",'False') == 'False':
                if self.test_cortex():
                    if self.test_cortex_embedding() == '':
                        print("cortex not available and no OpenAI API key present. Use streamlit to add OpenAI key")
                        os.environ["CORTEX_EMBEDDING_AVAILABLE"] = 'False'
                    else:
                        os.environ["CORTEX_EMBEDDING_AVAILABLE"] = 'True'
                else:
                    os.environ["CORTEX_EMBEDDING_AVAILABLE"] = 'False'
        else:
            self.client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
            self.model=os.getenv("OPENAI_HARVESTER_MODEL", 'gpt-4o')
            self.embedding_model = os.getenv("OPENAI_HARVESTER_EMBEDDING_MODEL", 'text-embedding-3-large')

    def alt_get_ddl(self,table_name = None):
        #print(table_name) 
        describe_query = f"DESCRIBE TABLE {table_name};"
        try:
            describe_result = self.db_connector.run_query(query=describe_query, max_rows=1000, max_rows_override=True)
        except:
            return None 
        
        ddl_statement = "CREATE TABLE " + table_name + " (\n"
        for column in describe_result:
            column_name = column['name']
            column_type = column['type']
            nullable = " NOT NULL" if not column['null?'] else ""
            default = f" DEFAULT {column['default']}" if column['default'] is not None else ""
            comment = f" COMMENT '{column['comment']}'" if 'comment' in column and column['comment'] is not None else ""
            key = ""
            if column.get('primary_key', False):
                key = " PRIMARY KEY"
            elif column.get('unique_key', False):
                key = " UNIQUE"
            ddl_statement += f"    {column_name} {column_type}{nullable}{default}{key}{comment},\n"
        ddl_statement = ddl_statement.rstrip(',\n') + "\n);"
        #print(ddl_statement)
        return ddl_statement
        
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
                    print(f"Error getting sample data: {e}", flush=True)
                    sample_data = None
                    sample_data_str = ""
            if sample_data:
                try:
                    sample_data_str = self.format_sample_data(sample_data)
                except Exception as e:
                    sample_data_str = ""
                #sample_data_str = sample_data_str.replace("\n", " ")  # Replace newlines with spaces
   
            #print('sample data string: ',sample_data_str)
            self.store_table_summary(database, schema, table, ddl=ddl, ddl_short=ddl_short,summary=summary, sample_data=sample_data_str)
  
        except Exception as e:
            print(f"Harvester Error for an object: {e}")
            self.store_table_summary(database, schema, table, summary="Harvester Error: {e}", ddl="Harvester Error", ddl_short="Harvester Error", sample_data="Harvester Error")

    def test_cortex(self):
        
        newarray = [{"role": "user", "content": "hi there"} ]
        new_array_str = json.dumps(newarray)

        print(f"schema_explorer test calling cortex {self.cortex_model} via SQL, content est tok len=",len(new_array_str)/4)

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
                    print(f'Model {self.cortex_model} not available in this region, trying mistral-7b')
                    self.cortex_model = 'mistral-7b'        
                    cortex_query = f"""
                        select SNOWFLAKE.CORTEX.COMPLETE('{self.cortex_model}', %s) as completion; """
                    cursor.execute(cortex_query, (new_array_str,))
                    print('Ok that worked, changing CORTEX_HARVESTER_MODEL ENV VAR to mistral-7b')
                    os.environ['CORTEX_HARVESTER_MODEL'] = 'mistral-7b'
                else:
                    raise(e)
            self.db_connector.connection.commit()
            # elapsed_time = time.time() - start_time
            result = cursor.fetchone()
            completion = result[0] if result else None

            print(f"schema_explorer test call result: ",completion)

            return True
        except Exception as e:
            print('cortex not available, query error: ',e)
            self.db_connector.connection.rollback()
            return False
             
    def test_cortex_embedding(self):
        
        try:
            test_message = 'this is a test message to generate an embedding'

            try:
                # review function used once new regions are unlocked in snowflake
                embedding_result = self.db_connector.run_query(f"SELECT SNOWFLAKE.CORTEX.EMBED_TEXT_768('{self.cortex_embedding_model}', '{test_message}');")
                result_value = next(iter(embedding_result[0].values()))
                if result_value:
                    os.environ['CORTEX_EMBEDDING_MODEL'] = self.cortex_embedding_model
                    print(f"Test result value len embedding: {len(result_value)}")            
            except Exception as e:
                if 'unknown model' in e.msg:
                    print(f'Model {self.cortex_embedding_model} not available in this region, trying snowflake-arctic-embed-m')
                    self.cortex_embedding_model = 'snowflake-arctic-embed-m'        
                    embedding_result = self.db_connector.run_query(f"SELECT SNOWFLAKE.CORTEX.EMBED_TEXT_768('{self.cortex_embedding_model}', '{test_message}');")
                    result_value = next(iter(embedding_result[0].values()))
                    if result_value:
                        print(f"Test result value len embedding: {len(result_value)}")
                        print('Ok that worked, changing CORTEX_EMBEDDING_MODEL ENV VAR to snowflake-arctic-embed-m')
                        os.environ['CORTEX_EMBEDDING_MODEL'] = 'snowflake-arctic-embed-m'
                else:
                    raise(e)

        except Exception as e:
            print('Cortex embed not available, query error: ',e)
            result_value = ""
        return result_value


    def store_table_summary(self, database, schema, table, ddl, ddl_short="", summary="", sample_data=""):
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

            if os.environ["CORTEX_AVAILABLE"] == 'True':
                memory_content = f"<OBJECT>{database}.{schema}.{table}</OBJECT><DDL_SHORT>{ddl_short}</DDL_SHORT>"
                complete_description = memory_content
            else:
                memory_content = f"<OBJECT>{database}.{schema}.{table}</OBJECT><DDL>\n{ddl}\n</DDL>\n<SUMMARY>\n{summary}\n</SUMMARY><DDL_SHORT>{ddl_short}</DDL_SHORT>"
                if sample_data != "":
                    memory_content += f"\n\n<SAMPLE CSV DATA>\n{sample_data}\n</SAMPLE CSV DATA>"
                complete_description = memory_content

            embedding = self.get_embedding(complete_description)  

            #sample_data_text = json.dumps(sample_data)  # Assuming sample_data needs to be a JSON text.

            # Now using the modified method to insert the data into BigQuery
            self.db_connector.insert_table_summary(schema_name=schema, 
                                                database_name=database,
                                                table_name=table, 
                                                ddl=ddl, 
                                                ddl_short=ddl_short,
                                                summary=summary, 
                                                sample_data_text=sample_data, 
                                                complete_description=complete_description,
                                                embedding=embedding)
            
            print(f"Stored summary for an object in Harvest Results.")
   
        except Exception as e:
            print(f"Harvester Error for an object: {e}", flush=True)
            self.store_table_summary(database, schema, table, summary="Harvester Error: {e}", ddl="Harvester Error", ddl_short="Harvester Error", sample_data="Harvester Error")
   
        ## Assuming an instance of BotOsKnowledgeLocal named memory_system exists
        #self.memory_system.store_memory(memory_content, scope='database_metadata')


    def generate_summary(self, prompt):
        p = [
            {"role": "system", "content": "You are an assistant that is great at explaining database tables and columns in natural language."},
            {"role": "user", "content": prompt}
        ]
        return self.run_prompt(p)
    
    def run_prompt(self, messages):
        if os.environ["CORTEX_AVAILABLE"] == 'True':
            escaped_messages = str(messages).replace("'", '\\"')
            query = f"select snowflake.cortex.complete('{self.cortex_model}','{escaped_messages}');"
            # print(query)
            completion_result = self.db_connector.run_query(query)
            try:
                result_value = next(iter(completion_result[0].values()))
                if result_value:
                    result_value = str(result_value).replace("\`\`\`","'''")
                    print(f"Result value: {result_value}")
            except:
                print('Cortext complete didnt work')
                result_value = ""
            return result_value
        else:
            response = self.client.chat.completions.create(
                model=self.model,  # Adjust the model name as necessary
                messages=messages
            )
            return response.choices[0].message.content
    
    def get_ddl_short(self, ddl):
        prompt = f'Here is the full DDL for a table:\n{ddl}\n\nPlease make a new ddl_summary for this table.  If there are 15 or fewer fields, just include them all. If there are more, combine any that are similar and explain that there are more, and then pick the more important 15 fields to include in the ddl_summary.  Express it as DDL, but include comments about other similar fields, and then a comment summarizing the rest of the fields and noting to see the FULL_DDL to see all columns.  Return ONLY the DDL_SUMMARY, do NOT include preamble or other post-result commentary.'
        
        messages = [
            {"role": "system", "content": "You are an assistant that is great at taking full table DDL and creating shorter DDL summaries."},
            {"role": "user", "content": prompt}
        ]
        
        response = self.run_prompt(messages)
        
        return response


    def get_embedding(self, text):
        # logic to handle switch between openai and cortex
        if os.environ["CORTEX_MODE"] == 'True':
            escaped_messages = str(text[:512]).replace("'", "\\'")
            
            # review function used once new regions are unlocked in snowflake
            embedding_result = self.db_connector.run_query(f"SELECT SNOWFLAKE.CORTEX.EMBED_TEXT_768('{self.cortex_embedding_model}', '{escaped_messages}');")
            try:
                result_value = next(iter(embedding_result[0].values()))
                if result_value:
                    print(f"Result value len embedding: {len(result_value)}")
            except:
                print('Cortex embed text didnt work')
                result_value = ""
            return result_value
        else:
            response = self.client.embeddings.create(
                model=self.embedding_model,
                input=text[:8000].replace("\n", " ")  # Replace newlines with spaces
            )
            embedding = response.data[0].embedding
            return embedding


    def explore_schemas(self):
        try:
            for schema in self.db_connector.get_schemas():
            #    print(f"Schema: {schema}")
                tables = self.db_connector.get_tables(schema)
                for table in tables:
        #           print(f"  Table: {table}")
                    columns = self.db_connector.get_columns(schema, table)
                    for column in columns:
                        pass
                #      print(f"    Column: {column}")
        except Exception as e:
            print(f'Error running explore schemas Error: {e}',flush=True)

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
            print(f'Error running get schemas Error: {e}',flush=True)


    def get_active_databases(self):
        
        databases = self.db_connector.get_databases()
        return [item for item in databases if item['status'] == 'Include']
        

    def get_active_schemas(self, database):

        try:
            inclusions = database["schema_inclusions"]
            if isinstance(inclusions, str):
                inclusions = json.loads(inclusions)
            if inclusions is None:
                inclusions = []
            if len(inclusions) == 0:
                schemas = self.db_connector.get_schemas(database["database_name"])
            else:
                schemas = inclusions
            exclusions = database["schema_exclusions"]
            if isinstance(exclusions, str):
                exclusions = json.loads(exclusions)
            if exclusions is None:
                exclusions = []
            schemas = [schema for schema in schemas if schema not in exclusions]
            return schemas
        except:
            return []

    def update_initial_crawl_flag(self, database_name, crawl_flag):

        if self.db_connector.source_name == 'Snowflake':
            query = f"""
                update {self.db_connector.harvest_control_table_name}
                set initial_crawl_complete = {crawl_flag}
                where source_name = '{self.db_connector.source_name}' and database_name = '{database_name}';"""
        else:
            query = f"""
                update `{self.db_connector.harvest_control_table_name}`
                set initial_crawl_complete = {crawl_flag}
                where source_name = '{self.db_connector.source_name}' and database_name = '{database_name}';"""
        update_query = self.db_connector.run_query(query)

    def explore_and_summarize_tables_parallel(self, max_to_process=1000):
        # called by standalone_harvester.py
        try:
            self.run_number += 1
            databases = self.get_active_databases()
            schemas = []
            harvesting_databases = []

            for database in databases:
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
                    print(f'Checking a Database for new or changed objects (cycle#: {self.run_number}, refresh every: {database["refresh_interval"]}) {cur_time}', flush=True)
                else: 
                    print(f'Skipping a Database, not in current refresh cycle (cycle#: {self.run_number}, refresh every: {database["refresh_interval"]} {cur_time})', flush=True)

            summaries = {}
            total_processed = 0
        except Exception as e:
            print(f'Error explore and summarize tables parallel Error: {e}',flush=True)

        #print('checking schemas: ',schemas)

        # todo, first build list of objects to harvest, then harvest them

        def process_dataset_step1(dataset, max_to_process = 1000):
            from core.bot_os_llm import LLMKeyHandler 
            llm_key_handler = LLMKeyHandler()
            #print("  Process_dataset: ",dataset)
            # query to find new
            # tables, or those with changes to their DDL

 #           print('Checking a schema for new (not changed) objects.', flush=True)
            if self.db_connector.source_name == 'Snowflake':
                try:
                    potential_tables = self.db_connector.get_tables(dataset.split('.')[0], dataset.split('.')[1])
                except Exception as e:
                    print(f'Error running get potential tables Error: {e}',flush=True)
                #print('potential tables: ',potential_tables)
                non_indexed_tables = []

                # Check all potential_tables at once using a single query with an IN clause
                table_names = [table_info['table_name'] for table_info in potential_tables]
                db, sch = dataset.split('.')[0], dataset.split('.')[1]
                #quoted_table_names = [f'\'"{db}"."{sch}"."{table}"\'' for table in table_names]
                #in_clause = ', '.join(quoted_table_names)
                print('Checking if LLM API Key updated for harvester...')
                api_key_from_env, llm_api_key, llm_type = llm_key_handler.get_llm_key_from_db(self.db_connector)
                self.initialize_model()
                if os.environ["CORTEX_MODE"] == 'True':
                    embedding_column = 'embedding_native'
                else:
                    embedding_column = 'embedding'
                    
                check_query = f"""
                SELECT qualified_table_name, ddl_hash, last_crawled_timestamp,  (SUMMARY = '{{!placeholder}}' OR {embedding_column} IS NULL) as needs_full
                FROM {self.db_connector.metadata_table_name}
                WHERE source_name = '{self.db_connector.source_name}'
                AND database_name= '{db}' and schema_name = '{sch}';"""
                try:
                    existing_tables_info = self.db_connector.run_query(check_query, max_rows=1000, max_rows_override=True)
                    existing_tables_set = {info['QUALIFIED_TABLE_NAME'] for info in existing_tables_info}
                    non_existing_tables = [table for table in potential_tables if f'"{db}"."{sch}"."{table["table_name"]}"' not in existing_tables_set]
                    needs_updating = [table['QUALIFIED_TABLE_NAME']  for table in existing_tables_info if table["NEEDS_FULL"]]
                    refresh_tables = [table for table in potential_tables if f'"{db}"."{sch}"."{table["table_name"]}"' in needs_updating]
                except Exception as e:
                    print(f'Error running check query Error: {e}',flush=True)
                    return None, None
                
                non_existing_tables.extend(refresh_tables)
                for table_info in non_existing_tables:
                    try:
                        table_name = table_info['table_name']
                        quoted_table_name = f'"{db}"."{sch}"."{table_name}"'
                        if quoted_table_name not in existing_tables_set or quoted_table_name in needs_updating:
                            # Table is not in metadata table
                            # Check to see if it exists in the shared metadata table
                            #print ("!!!! CACHING DIsABLED !!!! ", flush=True)
                            #TODO get metadata from cache and add embeddings for all schemas, incl baseball and f1
                            if sch == 'INFORMATION_SCHEMA':
                                shared_table_exists = self.db_connector.check_cached_metadata('PLACEHOLDER_DB_NAME', sch, table_name)
                            else:
                                shared_table_exists = self.db_connector.check_cached_metadata(db, sch, table_name)
                            # shared_table_exists = False 
                            if shared_table_exists:
                                # print ("!!!! CACHING Working !!!! ", flush=True)
                                # Get the record from the shared metadata table with database name modified from placeholder
                                print('Object cache hit',flush=True)
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
                                print('Newly found object added to harvest array (no cache hit)', flush=True)
                                non_indexed_tables.append(new_table)

                                # store quick summary
                                if quoted_table_name not in existing_tables_set:
                                    self.store_table_summary(database=db, schema=sch, table=table_name, ddl=current_ddl, ddl_short=current_ddl, summary="{!placeholder}", sample_data="")

                    except Exception as e:
                        print(f'Error processing table in step1: {e}', flush=True)


                   # else:
                   #     # Table exists, so check for updates as before
                   #     existing_table_info = next((info for info in existing_tables_info if info['qualified_table_name'] == quoted_table_name), None)
                   #     if existing_table_info:
                   #         last_crawled = existing_table_info['last_crawled_timestamp']
                   #         existing_ddl_hash = existing_table_info['ddl_hash']
                   #         shared_view = existing_ddl_hash == 'SHARED_VIEW'
                   #         cutoff_datetime = datetime(2024, 5, 1)
                    #        check_for_updated_ddl = last_crawled > cutoff_datetime
                    #        if shared_view:
                    #            check_for_updated_ddl = False                        # Override this for now to False while sorting out harvester slowness
                    #        check_for_updated_ddl = False
                    #        if check_for_updated_ddl:
                                # Fetch the DDL for the specific table and calculate its hash
                    #            current_ddl = self.alt_get_ddl(table_name=quoted_table_name)
                    #            if current_ddl:
                    #                current_ddl_hash = self.db_connector.sha256_hash_hex_string(current_ddl)
                    #                if existing_ddl_hash != current_ddl_hash:
                    #                    print('DDL has changed for', quoted_table_name, flush=True)
                    #                    non_indexed_tables.append({"qualified_table_name": quoted_table_name, "ddl_hash": current_ddl_hash, "ddl": current_ddl})
            else:
                # Bigquery 
                query = f"""
                SELECT CONCAT("{dataset}.", table_name) AS qualified_table_name
                FROM `{dataset}.INFORMATION_SCHEMA.TABLES`
                WHERE CONCAT("{dataset}.", table_name) NOT IN (
                SELECT qualified_table_name
                FROM `{self.db_connector.metadata_table_name}` where source_name = '{self.db_connector.source_name}')
                UNION DISTINCT
                SELECT hr.qualified_table_name
                FROM `{dataset}.INFORMATION_SCHEMA.TABLES` ist
                JOIN `{self.db_connector.metadata_table_name}` hr ON qualified_table_name = 
                    CONCAT("{dataset}.", ist.table_name) where TO_HEX(SHA256(ist.ddl)) <> hr.ddl_hash and hr.source_name = '{self.db_connector.source_name}';"""
                try:
                    non_indexed_tables = self.db_connector.run_query(query, max_rows = max_to_process, max_rows_override = True)
                except Exception as e:
                    print(f'Error running query  Error: {e}',flush=True)
            return non_indexed_tables
        
        def process_dataset_step2( non_indexed_tables, max_to_process = 1000):

                local_summaries = {}
                if len(non_indexed_tables) > 0:
                    print(f'starting indexing of {len(non_indexed_tables)} objects...', flush=True)
                for row in non_indexed_tables:
                    try:
                        qualified_table_name = row.get('qualified_table_name',row)
                        print("     -> An object", flush=True)
                        database, schema, table = (part.strip('"') for part in qualified_table_name.split('.', 2))

                        # Proceed with generating the summary
                        columns = self.db_connector.get_columns(database, schema, table)
                        prompt = self.generate_table_summary_prompt(database, schema, table, columns)
                        summary = self.generate_summary(prompt)
                        #print(summary)
                        #embedding = self.get_embedding(summary)  
                        ddl = row.get('ddl',None)
                        ddl_short = self.get_ddl_short(ddl)
                        #print(f"storing: database: {database}, schema: {schema}, table: {table}, summary len: {len(summary)}, ddl: {ddl}, ddl_short: {ddl_short} ", flush=True) 
                        print('Storing summary for new object',flush=True)
                        self.store_table_memory(database, schema, table, summary, ddl=ddl, ddl_short=ddl_short)
                    except Exception as e:
                        print(f"Harvester Error on Object: {e}",flush=True)
                        self.store_table_memory(database, schema, table, summary=f"Harvester Error: {e}", ddl="Harvester Error", ddl_short="Harvester Error")
                    
                    local_summaries[qualified_table_name] = summary
                return local_summaries

        # Using ThreadPoolExecutor to parallelize dataset processing
   
        # MAIN LOOP

        tables_for_full_processing = []
        random.shuffle(schemas)
        try:
            print(f'Checking {len(schemas)} schemas for new (not changed) objects.', flush=True)
        except Exception as e:
            print(f'Error printing schema count log line. {e}', flush=True)

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
                    print(f'Dataset {dataset} generated an exception: {exc}', flush=True)

