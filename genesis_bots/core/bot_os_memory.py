from abc import abstractmethod
from annoy import AnnoyIndex
import json
from openai import AzureOpenAI, OpenAI
import os
import shutil
import subprocess
import time
import uuid
import traceback
import re
import sys
#import spacy
#from genesis_bots.connectors.bigquery_connector import BigQueryConnector
from genesis_bots.connectors.snowflake_connector.snowflake_connector import SnowflakeConnector
from genesis_bots.connectors.sqlite_connector import SqliteConnector
from genesis_bots.llm.llm_openai.openai_utils import get_openai_client
from  genesis_bots.schema_explorer.embeddings_index_handler import load_or_create_embeddings_index

from genesis_bots.core.logging_config import logger
class BotOsKnowledgeBase:
    @abstractmethod
    def find_memory(self, query:str, scope:str="database_metadata", top_n:int=15, verbosity:str="low", database:str=None, schema:str=None, table:str=None) -> list[str]:
        pass

    @abstractmethod
    def store_memory(self, memory:str, scope:str):
        pass

# Load the medium spaCy model
#nlp = spacy.load("en_core_web_md")

# class DEPRECIATED_BotOsKnowledgeLocal(BotOsKnowledgeBase):
#     def __init__(self, base_directory_path) -> None:
#         self.base_directory_path = base_directory_path
#         # Create the base directory if it doesn't exist
#         if not os.path.exists(self.base_directory_path):
#             logger.info(f"creating base directory: {self.base_directory_path}")
#             os.makedirs(self.base_directory_path)

#     def store_memory(self, memory, scope="user_preferences"):
#         """Stores a new memory in its own file within the specified scope directory."""
#         scope_directory = f'{self.base_directory_path}/{scope}'
#         if not os.path.exists(scope_directory):
#             os.makedirs(scope_directory)  # Create the scope directory if it doesn't exist

#         timestamp = time.strftime('%Y%m%d%H%M%S')
#         unique_id = uuid.uuid4()  # Generate a random UUID
#         file_name = f'memory_{timestamp}_{unique_id}.txt'
#         file_path = f'{scope_directory}/{file_name}'
#         with open(file_path, 'w') as file:
#             file.write(memory)

#     def find_memory(self, query, scope="user_preferences", thresh=0.2) -> list[str]:
#         """Finds and returns memories that contain the query string within the specified scope."""
#         scope_directory = f'{self.base_directory_path}/{scope}'
#         if not os.path.exists(scope_directory) or not os.listdir(scope_directory):
#             return []  # Return an empty list if the directory doesn't exist or is empty

#         query_doc = nlp(query)  # Process the query to get its vector representation
#         memories = []
#         # Iterate over each file in the scope directory
#         for filename in os.listdir(scope_directory):
#             file_path = os.path.join(scope_directory, filename)
#             # Skip directories
#             if os.path.isdir(file_path):
#                 continue
#             with open(file_path, 'r') as file:
#                 content = file.read()
#                 content_doc = nlp(content)  # Process the memory content to get its vector representation
#                 similarity = query_doc.similarity(content_doc)
#                 if similarity > thresh:  # Threshold for similarity, adjust as needed
#                     memories.append(content.strip())
#         return memories

#     def reset(self):
#         """Clears out the entire Knowledge Base by removing all scopes and memories."""
#         if os.path.exists(self.base_directory_path):
#             shutil.rmtree(self.base_directory_path)
#             os.makedirs(self.base_directory_path)  # Recreate the base directory after clearing


class AnnoyIndexSingleton:
    _instance = None
    _index = None
    _metadata_mapping = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(AnnoyIndexSingleton, cls).__new__(cls)
        return cls._instance

    def load_index_and_metadata(self, harvest_table_id, vector_size, refresh=True):
        if self._index is None or self._metadata_mapping is None:
            #self._index = AnnoyIndex(vector_size, 'angular')  # Using angular distance
            self._index, self._metadata_mapping = load_or_create_embeddings_index(harvest_table_id, refresh=refresh)
        return self._index, self._metadata_mapping

    @classmethod
    def get_index_and_metadata(cls, harvest_table_id, vector_size, refresh=True):
        if cls._instance is None:
            cls._instance = cls()
        i, m = cls._instance.load_index_and_metadata(harvest_table_id, vector_size, refresh=refresh)
        return i, m

class BotOsKnowledgeAnnoy_Metadata(BotOsKnowledgeBase):
    def __init__(self, base_directory_path, vector_size=3072, n_trees=10, refresh=True, bot_id=None ):
        # Add debug logging
        logger.info(f"Initializing with vector_size: {vector_size}")
        self.base_directory_path = base_directory_path
        self.vector_size = vector_size
        self.n_trees = n_trees
        #self.index = AnnoyIndex(vector_size, 'angular')  # Using angular distance
        self.id_map = {}  # Maps Annoy ids to memory identifiers
        self.next_id = 0
        self.bot_id = bot_id

        self.source_name = os.getenv('GENESIS_SOURCE',default="Snowflake")


        if self.source_name  == 'BigQuery':
            raise NotImplementedError("BigQueryConnector is not implemented")
        elif self.source_name  == 'Sqlite':
            self.meta_database_connector = SqliteConnector(connection_name='Sqlite')
            self.project_id = self.meta_database_connector.database
        elif self.source_name  == 'Snowflake':
            from genesis_bots.connectors import get_global_db_connector
            self.meta_database_connector = get_global_db_connector()
            self.project_id = self.meta_database_connector.database

        # check if cortex or openai
        if os.getenv("CORTEX_MODE", 'False') == 'True':
            self.embedding_model = os.getenv("CORTEX_EMBEDDING_MODEL", 'e5-base-v2')
        else:
            self.embedding_model = os.getenv("OPENAI_HARVESTER_EMBEDDING_MODEL", 'text-embedding-3-large')
            logger.info("setting openai key in knowledge init")
            self.client = get_openai_client()

        #self.index, self.metadata_mapping = AnnoyIndexSingleton.get_index_and_metadata(self.meta_database_connector.metadata_table_name, vector_size, refresh=refresh)
        self.index, self.metadata_mapping = load_or_create_embeddings_index(self.meta_database_connector.metadata_table_name, refresh=refresh, bot_id=bot_id)


    def refresh_annoy(self):
        self.index, self.metadata_mapping = load_or_create_embeddings_index(self.meta_database_connector.metadata_table_name, refresh=True, bot_id=self.bot_id)


    # Function to get embedding (reuse or modify your existing get_embedding function)
    # def get_embedding(self, text):

    #     response = self.client.embeddings.create(
    #         model=self.embedding_model,
    #         input=text.replace("\n", " ")  # Replace newlines with spaces
    #     )
    #     embedding = response.data[0].embedding
    #     return embedding

    # Function to get embedding (reuse or modify your existing get_embedding function)
    def get_embedding(self, text, embedding_size=-1):
        # logic to handle switch between openai and cortex

        if embedding_size == -1:
            if os.getenv("CORTEX_MODE", 'False') == 'True':
                embedding_size = 768
            else:
                embedding_size = 3072

        if embedding_size == 768:
            escaped_messages = str(text[:512]).replace("'", "\\'")
            try:
                # review function used once new regions are unlocked in snowflake
                model = os.getenv("CORTEX_EMBEDDING_MODEL", 'e5-base-v2')
                embedding_result = self.meta_database_connector.run_query(f"SELECT SNOWFLAKE.CORTEX.EMBED_TEXT_768('{model}', '{escaped_messages}');")

                result_value = next(iter(embedding_result[0].values()))
                if result_value:
                    logger.info(f"Result value len embedding: {len(result_value)}")
            except:
                logger.info('Cortex embed text didnt work in bot os memory')
                result_value = ""
            return result_value
        else:
            try:
                model = os.getenv("OPENAI_HARVESTER_EMBEDDING_MODEL", 'text-embedding-3-large')
                response = self.client.embeddings.create(
                    model=model,
                    input=text[:8000].replace("\n", " ")  # Replace newlines with spaces
                )
                embedding = response.data[0].embedding
                if embedding:
                    logger.info(f"Result value len embedding: {len(embedding)}")
            except:
                logger.info('Openai embed text didnt work in bot os memory')
                embedding = ""
            return embedding

    # def store_memory(self, memory, scope="user_preferences", thread_id=""):
    #     if (scope == "user_preferences" or scope == "general") and len(self.find_memory_local(memory, scope=scope)) > 0:
    #         logger.warn(f"store_memory - not storing duplicate memory {memory} in scope {scope} thread_id {thread_id}")
    #         return # FixMe: change memories to be accumulated asynchronously by Locutus
    #     """Stores a new memory in its own file within the specified scope directory."""
    #     scope_directory = f'{self.base_directory_path}/{scope}'
    #     if not os.path.exists(scope_directory):
    #         os.makedirs(scope_directory)  # Create the scope directory if it doesn't exist

    #     timestamp = time.strftime('%Y%m%d%H%M%S')
    #     unique_id = uuid.uuid4()  # Generate a random UUID
    #     file_name = f'memory_{timestamp}_{unique_id}.txt'
    #     file_path = f'{scope_directory}/{file_name}'
    #     with open(file_path, 'w') as file:
    #         file.write(memory)

    # def find_memory_local(self, query, scope="user_preferences", thresh=0.2) -> list[str]:
    #     """Finds and returns memories that contain the query string within the specified scope."""
    #     scope_directory = f'{self.base_directory_path}/{scope}'
    #     if not os.path.exists(scope_directory) or not os.listdir(scope_directory):
    #         return []  # Return an empty list if the directory doesn't exist or is empty

    #     query_doc = nlp(query)  # Process the query to get its vector representation
    #     memories = []
    #     # Iterate over each file in the scope directory
    #     for filename in os.listdir(scope_directory):
    #         file_path = os.path.join(scope_directory, filename)
    #         # Skip directories
    #         if os.path.isdir(file_path):
    #             continue
    #         with open(file_path, 'r') as file:
    #             content = file.read()
    #             content_doc = nlp(content)  # Process the memory content to get its vector representation
    #             similarity = query_doc.similarity(content_doc)
    #             if similarity > thresh:  # Threshold for similarity, adjust as needed
    #                 memories.append(content.strip())
    #     return memories


    def get_full_metadata_details(self, source_name, connection_id, database_name, schema_name, table_name):
        """
        Retrieves the full metadata details for the specified source database, schema, and table.

        Args:
            source_name (str): The name of the source database.
            database_name (str): The name of the database.
            schema_name (str): The name of the schema.
            table_name (str): The name of the table.

        Returns:
            dict: A dictionary containing the full metadata details or None if not found.
        """
        
        # Escape single quotes for SQL query
       # source_name_escaped = source_name.replace("'", "''") if source_name is not None else ""
        connection_id_escaped = connection_id.replace("'", "''") if connection_id is not None else ""
        database_name_escaped = database_name.replace("'", "''") if database_name is not None else ""
        schema_name_escaped = schema_name.replace("'", "''") if schema_name is not None else ""
        table_name_escaped = table_name.replace("'", "''") if table_name is not None else ""

        # Construct the SQL query to retrieve metadata
        query = f"""
            SELECT QUALIFIED_TABLE_NAME, COMPLETE_DESCRIPTION, DDL
            FROM {self.meta_database_connector.metadata_table_name}
            WHERE 1=1 
              {f"AND source_name = '{connection_id_escaped}'" if connection_id_escaped else ""}
              {f"AND database_name = '{database_name_escaped}'" if database_name_escaped else ""}
              {f"AND schema_name = '{schema_name_escaped}'" if schema_name_escaped else ""}
              {f"AND table_name = '{table_name_escaped}'" if table_name_escaped else ""};"""

        # Execute the query and fetch the result
        try:
            result = self.meta_database_connector.run_query(query)
            if result:
                # Assuming result is a list of dictionaries, return the first one
                return result[0]
            else:
                # If no result is found, try to describe the table using the database connector
                if connection_id == 'Snowflake':
                    try:
                        describe_query = f"DESCRIBE TABLE {database_name_escaped}.{schema_name_escaped}.{table_name_escaped}"
                        describe_result = self.meta_database_connector.run_query(describe_query)
                        if describe_result:
                            return {
                                "source_name": connection_id,
                                "database_name": database_name,
                                "schema_name": schema_name,
                                "table_name": table_name,
                                "describe_table_result": describe_result
                            }
                        else:
                            return None
                    except Exception as e:
                        logger.error(f"Error describing table: {e}")
                        return {
                            "success": False,
                            "error": f"Unable to describe table {database_name}.{schema_name}.{table_name}: {str(e)}",
                            "message": "The table was not found in harvested metadata and an error occurred trying to describe it directly from the database."
                        }
                else:
                    # For non-Snowflake databases, return message about table not being in harvest results
                    # but offering to describe the table if needed
                    return {
                        "success": False,
                        "message": f"Table not found in harvested metadata. "
                                 f"If you know this table exists, you can try using a query on the database's metadata (using the specific sql or other commands required by that type of database to list tables) "
                                 f"for the {connection_id} connection to get its details directly from the database. Also BTW database, schema, and table names are case sensitive."
                    }

        except Exception as e:
            logger.error(f"Error retrieving metadata details: {e}")
            return {
                "success": False,
                "error": f"Error retrieving metadata details: {str(e)}",
                "message": "An error occurred while trying to retrieve metadata from the database. Please check the connection settings and try again. If the problem persists, contact your system administrator."
            }



    def find_memory(self, query=None, scope="database_metadata", top_n=15, verbosity="low", database=None, schema=None, table=None, connection_id=None, full_ddl='false') -> list | dict:
        """
        Find relevant metadata using a combination of structural filtering and vector similarity search.

        Args:
            query (str): The search query
            scope (str): The search scope (default: "database_metadata")
            top_n (int): Number of results to return (default: 15)
            verbosity (str): Level of detail in results ("low" or "high")
            database (str): Optional database name to filter by
            schema (str): Optional schema name to filter by
            table (str): Optional table name to filter by
            connection_id (str): Optional connection id to filter by
        """
        if full_ddl.lower() == 'true':
            verbosity='high'
            
        # If table is provided with database.schema.table format but no database/schema parameters, split them
        if table and not database and not schema and table.count('.') == 2:
            database, schema, table = table.split('.')
            logger.debug(f"Split table reference into database: {database}, schema: {schema}, table: {table}")

        # If table is provided with schema.table format but no schema parameter, split them
        if table and not schema and table.count('.') == 1:
            schema, table = table.split('.', 1)
            logger.debug(f"Split table reference into schema: {schema}, table: {table}")

        try:
            if scope != "database_metadata":
                return {"error": f"Invalid scope '{scope}'. Only 'database_metadata' scope is currently supported."}

            # Handle empty index
            if len(self.metadata_mapping) <= 1:
                self.index, self.metadata_mapping = load_or_create_embeddings_index(
                    self.meta_database_connector.metadata_table_name,
                    refresh=True
                )

            # Check if connection_id is specified when filtering by database, schema, or table
            if (database or schema or table) and not connection_id:
                return {
                    "Success": False,
                    "Error": "Please specify a connection_id when filtering by database, schema, or table.",
                    "Message": "Use list_database_connections to see available connections first."
                }


            # Validate database if specified
            if database:
                databases_query = f"""
                    SELECT DISTINCT database_name 
                    FROM {self.meta_database_connector.metadata_table_name}
                    WHERE source_name = '{connection_id}'
                    ORDER BY database_name
                """
                databases = self.meta_database_connector.run_query(databases_query, max_rows=500, max_rows_override=True)
                database_list = [db['DATABASE_NAME'] for db in databases]
                if database.upper() not in [db.upper() for db in database_list]:
                    database_options = "\n- " + "\n- ".join(database_list)
                    return [f"Database '{database}' not found in harvested metadata. Available databases:{database_options}", "If you are sure this database exists, you may want to use harvester tools to add it to the harvest if the user agrees, or use database specific sql or metadata queries to find it using query_database."]

            # Validate schema if specified
            if schema:
                schemas_query = f"""
                    SELECT DISTINCT schema_name 
                    FROM {self.meta_database_connector.metadata_table_name}
                    WHERE source_name = '{connection_id}'
                    {f"AND database_name = '{database}'" if database else ""}
                    ORDER BY schema_name
                """
                schemas = self.meta_database_connector.run_query(schemas_query, max_rows=500, max_rows_override=True)
                schema_list = [s['SCHEMA_NAME'] for s in schemas]
                if schema.upper() not in [s.upper() for s in schema_list]:
                    schema_options = "\n- " + "\n- ".join(schema_list)
                    return [f"Schema '{schema}' not found in harvested metadata{' for database ' + database if database else ''}. Available schemas:{schema_options}", "If you are sure this schema exists, you may want to use harvester tools to add it to the harvest if the user agrees, or use database specific sql or metadata queries to find it using query_database."]

            if query is None:
                query = ""
            # Check for exact table match first
            match = re.match(r'^"?([^"\.]+)"?\."?([^"\.]+)"?\."?([^"\.]+)"?$', query.strip())
            if match:
                database, schema, table = [part.strip('"') for part in match.groups()]
                full_metadata = self.get_full_metadata_details(
                    source_name=self.source_name,
                    connection_id=connection_id,
                    database_name=database,
                    schema_name=schema,
                    table_name=table
                )
                if full_metadata:
                    return [full_metadata]
                return [f"No metadata details found for table '{table}' in schema '{schema}', database '{database}', source '{self.source_name}'."]

            if table:
                full_metadata = self.get_full_metadata_details(source_name=self.source_name, connection_id=connection_id, database_name=database, schema_name=schema, table_name=table)
                if full_metadata:
                    return [full_metadata]
                else:
                    return [f"No metadata details found for table '{table}' in schema '{schema}', database '{database}', source '{self.source_name}'."]

            # Build structural filters
            filtered_entries = None

            where_clauses = ["1=1"] #[f"source_name='{self.source_name.replace('', '')}'"]
            if database:
                where_clauses.append(f"database_name='{database.replace('', '')}'")
            if schema:
                where_clauses.append(f"schema_name='{schema.replace('', '')}'")
            if table:
                where_clauses.append(f"table_name='{table.replace('','')}'")
            if connection_id:
                where_clauses.append(f"source_name='{connection_id.replace('','')}'")

            where_statement = " AND ".join(where_clauses)

            if any([database, schema, table, connection_id]):

                # Get all entries matching structural criteria
                filtered_entries_query = f"""
                    SELECT source_name||'.'||qualified_table_name as QUALIFIED_TABLE_NAME
                    FROM {self.meta_database_connector.metadata_table_name}
                    WHERE {where_statement}
                """
                filtered_entries = self.meta_database_connector.run_query(filtered_entries_query, max_rows=1000, max_rows_override=True)

                if not filtered_entries:

                    if self.meta_database_connector.source_name == 'Snowflake':
                        # Get current tables in the schema from database
                        current_tables = self.meta_database_connector.get_tables(database, schema)
                        current_table_set = set(table['table_name'] for table in current_tables)

                        # Find tables that exist but aren't harvested
                        unharvested_tables = current_table_set

                        # Add note about unharvested tables if any found
                        if unharvested_tables:
                            unharvested_list = sorted(list(unharvested_tables))[:50]
                            msg = (f'Note: Found {len(unharvested_list)} tables in {schema} schema that may not be harvested yet: '
                                f'{", ".join(unharvested_list)}. '
                                f'You can use get_full_table_details to get more information about these tables.')

                            if len(unharvested_tables) > 50:
                                msg += f' (and {len(unharvested_tables)-50} more)'
                            content = []
                            content.append(msg)

                            if schema and schema.endswith('_WORKSPACE'):
                                content.append("Note: You searched within a bot workspace schema. If you didn't find what you were looking for, try using search_metadata without specifying a database and schema to search more broadly.")

                            return(content)

                        else:
                            if schema and schema.endswith('_WORKSPACE'):
                                return ["Note: You searched within a bot workspace schema and it was empty. If you didn't find what you were looking for, try using search_metadata without specifying a database and schema to search more broadly."]
                            else:
                                return ["No tables found matching the specified criteria."]
                    else:
                        # Build message about which criteria were provided
                        criteria_parts = []
                        if connection_id:
                            criteria_parts.append(f"connection '{connection_id}'")
                        if database:
                            criteria_parts.append(f"database '{database}'")
                        if schema:
                            criteria_parts.append(f"schema '{schema}'")
                        
                        criteria_msg = " and ".join(criteria_parts)
                        
                        return [
                            f"No harvested objects were found matching the specified {criteria_msg}. "
                            "This may be because Genesis is not yet set up to harvest schema meatdata from this location. "
                            "You may want to add this connection to the harvest using the harvester tools."
                            "You can try using regular SQL commands to list available tables in this location directly."
                        ]

            try:
                if self.metadata_mapping == ['empty_index']:
                    return ["There is no data harvested, the search index is empty. Tell the user to use the Genesis Streamlit GUI to grant access to their data to Genesis, or to specify a specfic DATABASE and SCHEMA that has already been granted to see what is in it."]
            except:
                pass

            # Enhance query with context if provided
            enhanced_query = query
            # Remove "USERQUERY::" prefix if present
            if enhanced_query.startswith("USERQUERY::"):
                enhanced_query = enhanced_query[len("USERQUERY::"):]
        #   if database:
        #       enhanced_query += f" {database}"
        #   if schema:
        #       enhanced_query += f" {schema}"

            # Get query embedding
            embedding_size = int(os.environ.get('EMBEDDING_SIZE', -1))
            embedding = self.get_embedding(enhanced_query, embedding_size=embedding_size)

            # # Debug the embedding
            # logger.info(f"Query embedding shape: {len(embedding)}")
            # logger.info(f"Query embedding sample: {embedding[:5]}")  # Look at first 5 values

            # # Before get_nns_by_vector
            # logger.info(f"Index size: {self.index.get_n_items()}")
            # logger.info(f"Index dimension: {self.index.f}")
            # Convert embedding to list of floats if it's not already
            embedding_vector = embedding if isinstance(embedding, list) else [embedding]

            # Get nearest neighbors using Annoy's get_nns_by_vector
            # Get 20x more results than requested to filter down to those in filtered_table_names
            nn_indices, nn_distances = self.index.get_nns_by_vector(embedding_vector, 1000, include_distances=True)

            # # Debug the results
            # logger.info(f"Unique distances: {set(nn_distances)}")
            # logger.info(f"First few indices and distances: {list(zip(nn_indices[:5], nn_distances[:5]))}")

            # Convert filtered entries to a set of qualified table names if structural filters were applied
            filtered_table_names = None
            if filtered_entries:
                filtered_table_names = {entry['QUALIFIED_TABLE_NAME'] for entry in filtered_entries}

            # Filter results to only include tables that match structural criteria
            results = []
            for idx, dist in zip(nn_indices, nn_distances):
                metadata = self.metadata_mapping[idx]
                if filtered_table_names is None:
                    results.append((idx, dist, metadata))
                elif metadata in filtered_table_names:
                    results.append((idx, dist, metadata))

            # Take top_n results
            top_results = results[:min(top_n, len(results))]

            # Fetch full metadata for top results
            qualified_names = [f"'{result[2]}'" for result in top_results]
            if verbosity == "high":
                content_query = f"""
                    SELECT
                        source_name as database_connection_id, qualified_table_name as full_table_name,
                        ddl as DDL_FULL,
                        sample_data_text as sample_data
                    FROM {self.meta_database_connector.metadata_table_name}
                    WHERE {where_statement}
                    {f"AND source_name || '.' || qualified_table_name IN ({','.join(qualified_names)})" if qualified_names else ""}
                """
            else:
                content_query = f"""
                    SELECT
                        source_name as database_connection_id, qualified_table_name as full_table_name,
                        ddl_short
                    FROM {self.meta_database_connector.metadata_table_name}
                    WHERE {where_statement}
                    {f"AND source_name || '.' || qualified_table_name IN ({','.join(qualified_names)})" if qualified_names else ""}
                """

            # Add error handling around the query execution
            try:
                content = self.meta_database_connector.run_query(content_query, keep_db_schema=True)
                if not content:
                    return ["No results found matching your criteria."]
            except Exception as e:
                logger.error(f"Error executing query: {str(e)}")
                return [f"Error executing search query: {str(e)}"]

            # Get next set of results for reference
            next_results = results[top_n:min(top_n * 3, len(results))]

            # Sort content to match order of qualified table names
            # Convert all dictionary keys to uppercase
            content = [{k.upper(): v for k, v in row.items()} for row in content]
            content_dict = {row['DATABASE_CONNECTION_ID']+"."+row['FULL_TABLE_NAME']: row for row in content}
            sorted_content = []
            for qualified_name in qualified_names:
                # Remove quotes around table name for lookup
                table_name = qualified_name.strip("'")

                if table_name in content_dict:
                    # Fix any _SUMMARY appended to table names in DDL_SHORT
                    if 'DDL_SHORT' in content_dict[table_name]:
                        table_name_no_quotes = table_name.split('.')[-1].strip('"')
                        content_dict[table_name]['DDL_SHORT'] = content_dict[table_name]['DDL_SHORT'].replace(
                            f"{table_name_no_quotes}_SUMMARY",
                            table_name_no_quotes
                        )
                    sorted_content.append(content_dict[table_name])
            content = sorted_content

        # If schema is specified, check for tables that might not be harvested yet
            current_tables = None
            if self.meta_database_connector.source_name == 'Snowflake' and schema is not None:
                try:
                    # Get current tables in the schema from database
                    current_tables = self.meta_database_connector.get_tables(database, schema)
                    current_table_set = set(table['table_name'] for table in current_tables)

                    # Extract just the table names from qualified names for comparison
                    harvested_tables = set()
                    for qname in filtered_table_names:
                        table = qname.strip("'").split('.')[-1]
                        # Remove quotes if present
                        table = table.strip('"')
                        harvested_tables.add(table)

                    # Find tables that exist but aren't harvested
                    unharvested_tables = current_table_set - harvested_tables

                    # Remove any results that no longer exist in the database
                    content = [row for row in content if isinstance(row, str) or
                            row['FULL_TABLE_NAME'].split('.')[-1].strip('"') in current_table_set]
                    # Remove tables from next_results that no longer exist in the database
                # if next_results:
                #     next_results = [result for result in next_results
                #                   if result[2].split('.')[-1] in current_table_set]

                    # Add note about unharvested tables if any found
                    if unharvested_tables:
                        unharvested_list = sorted(list(unharvested_tables))[:50]
                        msg = (f'Note: Found {len(unharvested_list)} tables in {schema} schema that may not be harvested yet: '
                            f'{", ".join(unharvested_list)}. '
                            f'You can use get_full_table_details to get more information about these tables.')
                        if len(unharvested_tables) > 50:
                            msg += f' (and {len(unharvested_tables)-50} more)'
                        content.append(msg)

                except Exception as e:
                    logger.warning(f"Error checking for unharvested tables: {str(e)}")

            if next_results:
                additional_tables = [result[2] for result in next_results]
                reference_msg = (f'For reference, the next {len(additional_tables)} most relevant tables are named: '
                            f'{", ".join(additional_tables)}')
                content.append(reference_msg)

            # Add note about potentially more results
            if len(results) > top_n * 3:
                msg = (f'Note! There may be more tables for this query, these were the first {top_n*3} results. '
                    f'If you don\'t see what you\'re looking for, try increasing top_n (up to 50) or use a more specific search query. '
                    f'For known table names, try get_full_table_details.')
                content.append(msg)

            # Add note about searching across all DBs/schemas if not specified
            if not connection_id and not database and not schema:
                msg = ("Note: This search was performed across all database connections, databases, and schemas. If you're having trouble finding "
                    "the right tables, we could get better results by narrowing the search to a specific database connection, database, and/or schema. "
                    "You may want to work with the user to see if they can constrain the search to a specific database or schema, unless you've found what you're looking for already with these results.")
                content.append(msg)


            logger.info(f'Search metadata: returned {len(content)} objects')
            return content

        except Exception as e:
            error_details = {
                "success": False,
                "error": str(e),
                "error_type": type(e).__name__,
                "error_traceback": traceback.format_exc()
            }
            logger.error(f"Error in search_table_metadata: {error_details}")
            return [error_details]
