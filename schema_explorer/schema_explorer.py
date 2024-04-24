import openai
import os, csv, io
import simplejson as json
#import core.bot_os_memory
from openai import OpenAI
import random 
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

# Assuming OpenAI SDK initialization

class SchemaExplorer:
    def __init__(self, db_connector):
        self.db_connector = db_connector
        self.openai_api_key = os.getenv('OPENAI_API_KEY')
        self.client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        self.model=os.getenv("OPENAI_HARVESTER_MODEL", 'gpt-4-turbo-preview')
        self.embedding_model = os.getenv("OPENAI_HARVESTER_EMBEDDING_MODEL", 'text-embedding-3-large')
        self.run_number = 0
        print("harvesting using models: ",self.model, self.embedding_model)


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

    def store_table_memory(self, database, schema, table, summary, ddl=None, ddl_short=None,  ):
        """
        Generates a document including the DDL statement and a natural language description for a table.
        :param schema: The schema name.
        :param table: The table name.
        """
        try:
            if ddl is None:
                ddl = self.db_connector.get_table_ddl(database_name=database, schema_name=schema, table_name=table)

            sample_data = self.db_connector.get_sample_data(database, schema, table)
            sample_data_str = ""
            if sample_data:
                sample_data_str = self.format_sample_data(sample_data)
                #sample_data_str = sample_data_str.replace("\n", " ")  # Replace newlines with spaces
   
            #print('sample data string: ',sample_data_str)
            self.store_table_summary(database, schema, table, ddl=ddl, ddl_short=ddl_short,summary=summary, sample_data=sample_data_str)
  
        except NotImplementedError:
            print(f"The get_table_ddl method is not implemented for {type(self.db_connector)}.")

    def store_table_summary(self, database, schema, table, ddl, ddl_short, summary, sample_data):
        """
        Stores a document including the DDL and summary for a table in the memory system.
        :param schema: The schema name.
        :param table: The table name.
        :param ddl: The DDL statement of the table.
        :param summary: A natural language summary of the table.
        """
        memory_id = f"schema_information:table_ddl_summary:{database}.{schema}.{table}"
        memory_content = f"<OBJECT>{database}.{schema}.{table}</OBJECT><DDL>\n{ddl}\n</DDL>\n<SUMMARY>\n{summary}\n</SUMMARY>"
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
                print(f'Checking {self.db_connector.source_name} Database {database["database_name"]} for new or changed objects (cycle#: {self.run_number}, refresh every: {database["refresh_interval"]})')
            else: 
                print(f'Skipping {self.db_connector.source_name} Database {database["database_name"]}, not in current refresh cycle (cycle#: {self.run_number}, refresh every: {database["refresh_interval"]})')

        summaries = {}
        total_processed = 0

        random.shuffle(schemas)

        #print('checking schemas: ',schemas)

        # todo, first build list of objects to harvest, then harvest them

        def process_dataset(dataset, max_to_process = 1000):

            #print("  Process_dataset: ",dataset)
            # query to find new
            # tables, or those with changes to their DDL

            print('Checking ',self.db_connector.source_name,' Schema ',dataset,' for new or changed objects.')
            if self.db_connector.source_name == 'Snowflake':
                potential_tables = self.db_connector.get_tables(dataset.split('.')[0], dataset.split('.')[1])
                #print('potential tables: ',potential_tables)
                non_indexed_tables = []
                for table_info in potential_tables:
                    table_name = table_info['table_name']
                    #ddl_hash = table_info['ddl_hash']
                    db, sch = dataset.split('.')[0], dataset.split('.')[1]
                    quoted_table_name = f'"{db}"."{sch}"."{table_name}"'
                    # Check if the table is in the metadata table
                    check_query = f"""
                    SELECT qualified_table_name, ddl_hash
                    FROM {self.db_connector.metadata_table_name}
                    WHERE source_name = '{self.db_connector.source_name}'
                    AND qualified_table_name = '{quoted_table_name}'
                    """
                    #print('running check query: ',check_query)
                    existing_table_info = self.db_connector.run_query(check_query)
                    if not existing_table_info:
                        # Table is not in metadata table
                        query_ddl = f"SELECT GET_DDL('{table_info.get('object_type','TABLE')}', '{quoted_table_name}')"
                        #print(f'New table found, {quoted_table_name}')
                        ddl_result = self.db_connector.run_query(query_ddl)
                        #print('New table DDL: ',ddl_result)
                        o_type = table_info.get('object_type','TABLE')
                        field_name = f"GET_DDL('{o_type}', '{quoted_table_name.upper()}')"
                        #print('looking for: ',field_name)
                        current_ddl = ddl_result[0][field_name]
                        #print('current ddl: ',current_ddl)
                        current_ddl_hash = self.db_connector.sha256_hash_hex_string(current_ddl)
                        #print('current ddl hash: ',current_ddl_hash)
                        new_table = {"qualified_table_name": quoted_table_name, "ddl_hash": current_ddl_hash, "ddl": current_ddl}
                        print('Newly found object added to harvest array: ',new_table)
                        non_indexed_tables.append(new_table)
                    else:
                        # Table is in metadata table, check if DDL hash has changed
                        # Fetch the DDL for the specific table and calculate its hash
                        query_ddl = f"SELECT GET_DDL('{table_info.get('object_type','TABLE')}', '{quoted_table_name}')"
                        #print(f'existing, running query: {query_ddl}') 
                        try:
                            ddl_result = self.db_connector.run_query(query_ddl)
                        except Exception as e:
                            print('ddl result query error: ',e)
                            ddl_result = None
                        #print(f'ddl result: {ddl_result}') 
                        if ddl_result:
                            o_type = table_info.get('object_type','TABLE')
                            field_name = f"GET_DDL('{o_type}', '{quoted_table_name.upper()}')"
                            current_ddl = ddl_result[0][field_name]
                            current_ddl_hash = self.db_connector.sha256_hash_hex_string(current_ddl)
                            if existing_table_info[0].get('DDL_HASH',None) != current_ddl_hash:
                                print('Existing but modified object added to harvest array: ',new_table)
                                non_indexed_tables.append({"qualified_table_name": quoted_table_name, "ddl_hash": current_ddl_hash, "ddl": current_ddl})           
            else:
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

            local_summaries = {}
            if len(non_indexed_tables) > 0:
                print(f'starting indexing of {len(non_indexed_tables)} new or changed objects in {dataset}...')
            for row in non_indexed_tables:
                qualified_table_name = row['qualified_table_name']
                print("     -> ", qualified_table_name)
                database, schema, table = (part.strip('"') for part in qualified_table_name.split('.', 2))

                # Proceed with generating the summary
                columns = self.db_connector.get_columns(database, schema, table)
                prompt = self.generate_table_summary_prompt(database, schema, table, columns)
                summary = self.generate_summary(prompt)
                #print(summary)
                #embedding = self.get_embedding(summary)  
                ddl = row.get('ddl',None)
                ddl_short = self.get_ddl_short(ddl)
                print(f"storing: database: {database}, schema: {schema}, table: {table}, summary len: {len(summary)}, ddl: {ddl} ")                

                self.store_table_memory(database, schema, table, summary, ddl=ddl, ddl_short=ddl_short)
                
                local_summaries[qualified_table_name] = summary
            return dataset, local_summaries

        # Using ThreadPoolExecutor to parallelize dataset processing
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
                    print(f'Dataset {dataset} generated an exception: {exc}')

