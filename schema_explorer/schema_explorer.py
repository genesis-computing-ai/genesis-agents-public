import os, csv, io
import simplejson as json
from openai import OpenAI
import random 
#from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
from datetime import datetime

# Assuming OpenAI SDK initialization

class SchemaExplorer:
    def __init__(self, db_connector):
        self.db_connector = db_connector
        self.openai_api_key = os.getenv('OPENAI_API_KEY')
        self.client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        self.model=os.getenv("OPENAI_HARVESTER_MODEL", 'gpt-4o')
        self.embedding_model = os.getenv("OPENAI_HARVESTER_EMBEDDING_MODEL", 'text-embedding-3-large')
        self.run_number = 0
        print("harvesting using models: ",self.model, self.embedding_model)

    def alt_get_ddl(self,table_name = None):
        #print(table_name) 
        describe_query = f"DESCRIBE TABLE {table_name}"
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
        except TypeError:
            j = ""        
        return j

    def store_table_memory(self, database, schema, table, summary=None, ddl=None, ddl_short=None ):
        """
        Generates a document including the DDL statement and a natural language description for a table.
        :param schema: The schema name.
        :param table: The table name.
        """
        try:
            if ddl is None:
                ddl = self.alt_get_ddl(table_name='"'+database+'"."'+schema+'"."'+table+'"')
                #ddl = self.db_connector.get_table_ddl(database_name=database, schema_name=schema, table_name=table)

            sample_data = self.db_connector.get_sample_data(database, schema, table)
            sample_data_str = ""
            if sample_data:
                sample_data_str = self.format_sample_data(sample_data)
                #sample_data_str = sample_data_str.replace("\n", " ")  # Replace newlines with spaces
   
            #print('sample data string: ',sample_data_str)
            self.store_table_summary(database, schema, table, ddl=ddl, ddl_short=ddl_short,summary=summary, sample_data=sample_data_str)
  
        except Exception as e:
            print(f"Harvester Error for {database}.{schema}.{table}: {e}")
            self.store_table_summary(database, schema, table, summary="Harvester Error: {e}", ddl="Harvester Error", ddl_short="Harvester Error", sample_data="Harvester Error")
   

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

 
            memory_content = f"<OBJECT>{database}.{schema}.{table}</OBJECT><DDL>\n{ddl}\n</DDL>\n<SUMMARY>\n{summary}\n</SUMMARY><DDL_SHORT>{ddl_short}</DDL_SHORT>"

            if sample_data != "":
                memory_content += f"\n\n<SAMPLE CSV DATA>\n{sample_data}\n</SAMPLE CSV DATA>"

            complete_description = memory_content
            embedding = self.get_embedding(complete_description[:8000])  

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
            
            print(f"Stored summary for {schema}.{table} in Harvest Results.")
   
        except Exception as e:
            print(f"Harvester Error for {database}.{schema}.{table}: {e}")
            self.store_table_summary(database, schema, table, summary="Harvester Error: {e}", ddl="Harvester Error", ddl_short="Harvester Error", sample_data="Harvester Error")
   
        ## Assuming an instance of BotOsKnowledgeLocal named memory_system exists
        #self.memory_system.store_memory(memory_content, scope='database_metadata')



    def generate_summary(self, prompt):

        response = self.client.chat.completions.create(

            model=self.model,  # Adjust the model name as necessary
            messages=[
                {"role": "system", "content": "You an assistant that is great at explaining database tables and columns in natural language."},
                {"role": "user", "content": prompt}
            ]
        )
        # Assuming the response structure aligns with your example,
        # extract the last message as the summary

        return response.choices[0].message.content
    
    def get_ddl_short(self, ddl):

        prompt = f'Here is the full DDL for a table:\n{ddl}\n\nPlease make a new ddl_summary for this table.  If there are 15 or fewer fields, just include them all. If there are more, combine any that are similar and explain that there are more, and then pick the more important 15 fields to include in the ddl_summary.  Express it as DDL, but include comments about other similar fields, and then a comment summarizing the rest of the fields and noting to see the FULL_DDL to see all columns.  Return ONLY the DDL_SUMMARY, do NOT include preamble or other post-result commentary.'
        response = self.client.chat.completions.create(

            model=self.model,  # Adjust the model name as necessary
            messages=[
                {"role": "system", "content": "You an assistant that is great taking full table DDL and creating shorter DDL summaries."},
                {"role": "user", "content": prompt}
            ]
        )
        # Assuming the response structure aligns with your example,
        # extract the last message as the summary

        return response.choices[0].message.content


    def get_embedding(self, text):

        response = self.client.embeddings.create(
            model=self.embedding_model,
            input=text.replace("\n", " ")  # Replace newlines with spaces
        )
        # Extracting the embedding from the response
        embedding = response.data[0].embedding
        return embedding


    def explore_schemas(self):
        for schema in self.db_connector.get_schemas():
            print(f"Schema: {schema}")
            tables = self.db_connector.get_tables(schema)
            for table in tables:
                print(f"  Table: {table}")
                columns = self.db_connector.get_columns(schema, table)
                for column in columns:
                    print(f"    Column: {column}")

    def generate_table_summary_prompt(self, database, schema, table, columns):
        prompt = f"Please provide a brief summary of a database table in the '{database}.{schema}' schema named '{table}'. This table includes the following columns: {', '.join(columns)}."
        return prompt

    def generate_column_summary_prompt(self, database, schema, table, column, sample_values):
        prompt = f"Explain the purpose of the '{column}' column in the '{table}' table, part of the '{database}.{schema}' schema. Example values include: {', '.join(map(str, sample_values))}."
        return prompt

    def get_datasets(self, database):
        datasets = self.db_connector.get_schemas(database)  # Assume this method exists and returns a list of dataset IDs
        return datasets


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
                where source_name = '{self.db_connector.source_name}' and database_name = '{database_name}'
                """
        else:
            query = f"""
                update `{self.db_connector.harvest_control_table_name}`
                set initial_crawl_complete = {crawl_flag}
                where source_name = '{self.db_connector.source_name}' and database_name = '{database_name}'
                """
        update_query = self.db_connector.run_query(query)

    def explore_and_summarize_tables_parallel(self, max_to_process=1000):
     
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

            if crawl_flag:
                harvesting_databases.append(database)
                schemas.extend([database["database_name"]+"."+schema for schema in self.get_active_schemas(database)])
                print(f'Checking {self.db_connector.source_name} Database {database["database_name"]} for new or changed objects (cycle#: {self.run_number}, refresh every: {database["refresh_interval"]})', flush=True)
            else: 
                print(f'Skipping {self.db_connector.source_name} Database {database["database_name"]}, not in current refresh cycle (cycle#: {self.run_number}, refresh every: {database["refresh_interval"]})', flush=True)

        summaries = {}
        total_processed = 0


        #print('checking schemas: ',schemas)

        # todo, first build list of objects to harvest, then harvest them

        def process_dataset_step1(dataset, max_to_process = 1000):

            #print("  Process_dataset: ",dataset)
            # query to find new
            # tables, or those with changes to their DDL

            print('Checking ',self.db_connector.source_name,' Schema ',dataset,' for new (not changed) objects.', flush=True)
            if self.db_connector.source_name == 'Snowflake':
                potential_tables = self.db_connector.get_tables(dataset.split('.')[0], dataset.split('.')[1])
                #print('potential tables: ',potential_tables)
                non_indexed_tables = []

                # Check all potential_tables at once using a single query with an IN clause
                table_names = [table_info['table_name'] for table_info in potential_tables]
                db, sch = dataset.split('.')[0], dataset.split('.')[1]
                #quoted_table_names = [f'\'"{db}"."{sch}"."{table}"\'' for table in table_names]
               #in_clause = ', '.join(quoted_table_names)
                check_query = f"""
                SELECT qualified_table_name, ddl_hash, last_crawled_timestamp, (SUMMARY = '{{!placeholder}}') as needs_full
                FROM {self.db_connector.metadata_table_name}
                WHERE source_name = '{self.db_connector.source_name}'
                AND database_name= '{db}' and schema_name = '{sch}'
                """
                try:
                    existing_tables_info = self.db_connector.run_query(check_query, max_rows=1000, max_rows_override=True)
                    existing_tables_set = {info['QUALIFIED_TABLE_NAME'] for info in existing_tables_info}
                    non_existing_tables = [table for table in potential_tables if f'"{db}"."{sch}"."{table["table_name"]}"' not in existing_tables_set]
                    needs_updating = [table['QUALIFIED_TABLE_NAME']  for table in existing_tables_info if table["NEEDS_FULL"]]
                    refresh_tables = [table for table in potential_tables if f'"{db}"."{sch}"."{table["table_name"]}"' in needs_updating]
                except Exception as e:
                    print(f'Error running check query: {check_query} Error: {e}')
                    return None, None
                
                non_existing_tables.extend(refresh_tables)
                for table_info in non_existing_tables:
                    table_name = table_info['table_name']
                    quoted_table_name = f'"{db}"."{sch}"."{table_name}"'
                    if quoted_table_name not in existing_tables_set or quoted_table_name in needs_updating:
                        # Table is not in metadata table
                        # Check to see if it exists in the shared metadata table
                       #print ("!!!! CACHING DIsABLED !!!! ", flush=True)
                        shared_table_exists = self.db_connector.check_cached_metadata(db, sch, table_name)
                        #shared_table_exists = False 
                        if shared_table_exists:
                            # Insert the record from the shared metadata table directly to the metadata table
                            insert_from_cache_result = self.db_connector.insert_metadata_from_cache(db, sch, table_name)
                            #print(insert_from_cache_result, flush=True)
                        else:
                            # Table is new, so get its DDL and hash
                            current_ddl = self.alt_get_ddl(table_name=quoted_table_name)
                            current_ddl_hash = self.db_connector.sha256_hash_hex_string(current_ddl)
                            new_table = {"qualified_table_name": quoted_table_name, "ddl_hash": current_ddl_hash, "ddl": current_ddl}
                            print('Newly found object added to harvest array: ', quoted_table_name, flush=True)
                            non_indexed_tables.append(new_table)

                            # store quick summary
                            if quoted_table_name not in existing_tables_set:
                                self.store_table_summary(database=db, schema=sch, table=table_name, ddl=current_ddl, ddl_short=current_ddl, summary="{!placeholder}", sample_data="")
                
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
                    CONCAT("{dataset}.", ist.table_name) where TO_HEX(SHA256(ist.ddl)) <> hr.ddl_hash and hr.source_name = '{self.db_connector.source_name}'
                """
                non_indexed_tables = self.db_connector.run_query(query, max_rows = max_to_process, max_rows_override = True)

            return non_indexed_tables
        
        def process_dataset_step2(dataset, non_indexed_tables, max_to_process = 1000):

                local_summaries = {}
                if len(non_indexed_tables) > 0:
                    print(f'starting indexing of {len(non_indexed_tables)} new or objects in {dataset}...', flush=True)
                for row in non_indexed_tables:
                    try:
                        qualified_table_name = row.get('qualified_table_name',row)
                        print("     -> ", qualified_table_name, flush=True)
                        database, schema, table = (part.strip('"') for part in qualified_table_name.split('.', 2))

                        # Proceed with generating the summary
                        columns = self.db_connector.get_columns(database, schema, table)
                        prompt = self.generate_table_summary_prompt(database, schema, table, columns)
                        summary = self.generate_summary(prompt)
                        #print(summary)
                        #embedding = self.get_embedding(summary)  
                        ddl = row.get('ddl',None)
                        ddl_short = self.get_ddl_short(ddl)
                        print(f"storing: database: {database}, schema: {schema}, table: {table}, summary len: {len(summary)}, ddl: {ddl}, ddl_short: {ddl_short} ", flush=True) 
                        self.store_table_memory(database, schema, table, summary, ddl=ddl, ddl_short=ddl_short)
                    except Exception as e:
                        print(f"Harvester Error on {qualified_table_name}: {e}")
                        self.store_table_memory(database, schema, table, summary="Harvester Error: {e}", ddl="Harvester Error", ddl_short="Harvester Error", flush=True)
                    
                    local_summaries[qualified_table_name] = summary
                return dataset, local_summaries

        # Using ThreadPoolExecutor to parallelize dataset processing
   
        # MAIN LOOP

        tables_for_full_processing = []
        random.shuffle(schemas)
        for schema in schemas:
            tables_for_full_processing.extend(process_dataset_step1(schema))
        random.shuffle(tables_for_full_processing)
        process_dataset_step2(schema,tables_for_full_processing)

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

