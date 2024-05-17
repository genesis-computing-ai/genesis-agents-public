from abc import abstractmethod
import logging
from annoy import AnnoyIndex
import json
from openai import OpenAI
import os
import shutil
import subprocess
import time
import uuid
import sys
import spacy
from connectors.bigquery_connector import BigQueryConnector
from connectors.snowflake_connector import SnowflakeConnector
from  schema_explorer.embeddings_index_handler import load_or_create_embeddings_index

logger = logging.getLogger(__name__)
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.WARN, format='%(asctime)s - %(levelname)s - %(message)s')

class BotOsKnowledgeBase:
    @abstractmethod
    def find_memory(self, query:str, scope:str, top_n:int, verbosity:str) -> list[str]:
        pass

    @abstractmethod
    def store_memory(self, memory:str, scope:str):
        pass

# Load the medium spaCy model
nlp = spacy.load("en_core_web_md")

class BotOsKnowledgeLocal(BotOsKnowledgeBase):
    def __init__(self, base_directory_path) -> None:
        self.base_directory_path = base_directory_path
        # Create the base directory if it doesn't exist
        if not os.path.exists(self.base_directory_path):
            logger.info(f"creating base directory: {self.base_directory_path}")
            os.makedirs(self.base_directory_path)

    def store_memory(self, memory, scope="user_preferences"):
        """Stores a new memory in its own file within the specified scope directory."""
        scope_directory = f'{self.base_directory_path}/{scope}'
        if not os.path.exists(scope_directory):
            os.makedirs(scope_directory)  # Create the scope directory if it doesn't exist
        
        timestamp = time.strftime('%Y%m%d%H%M%S')
        unique_id = uuid.uuid4()  # Generate a random UUID
        file_name = f'memory_{timestamp}_{unique_id}.txt'
        file_path = f'{scope_directory}/{file_name}'
        with open(file_path, 'w') as file:
            file.write(memory)
    
    def find_memory(self, query, scope="user_preferences", thresh=0.2) -> list[str]:
        """Finds and returns memories that contain the query string within the specified scope."""
        scope_directory = f'{self.base_directory_path}/{scope}'
        if not os.path.exists(scope_directory) or not os.listdir(scope_directory):
            return []  # Return an empty list if the directory doesn't exist or is empty
        
        query_doc = nlp(query)  # Process the query to get its vector representation
        memories = []
        # Iterate over each file in the scope directory
        for filename in os.listdir(scope_directory):
            file_path = os.path.join(scope_directory, filename)
            # Skip directories
            if os.path.isdir(file_path):
                continue
            with open(file_path, 'r') as file:
                content = file.read()
                content_doc = nlp(content)  # Process the memory content to get its vector representation
                similarity = query_doc.similarity(content_doc)
                if similarity > thresh:  # Threshold for similarity, adjust as needed
                    memories.append(content.strip())
        return memories

    def reset(self):
        """Clears out the entire Knowledge Base by removing all scopes and memories."""
        if os.path.exists(self.base_directory_path):
            shutil.rmtree(self.base_directory_path)
            os.makedirs(self.base_directory_path)  # Recreate the base directory after clearing


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
    def __init__(self, base_directory_path, vector_size=3072, n_trees=10, refresh=True ):
        self.base_directory_path = base_directory_path
        self.vector_size = vector_size
        self.n_trees = n_trees
        #self.index = AnnoyIndex(vector_size, 'angular')  # Using angular distance
        self.id_map = {}  # Maps Annoy ids to memory identifiers
        self.next_id = 0

        self.source_name = os.getenv('GENESIS_SOURCE',default="BigQuery")

        if self.source_name  == 'BigQuery':
            credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS',default=".secrets/gcp.json")
            with open(credentials_path) as f:
                connection_info = json.load(f)
            # Initialize BigQuery client
            self.meta_database_connector = BigQueryConnector(connection_info,'BigQuery')
            self.project_id = connection_info["project_id"]
        else:    # Initialize BigQuery client
            self.meta_database_connector = SnowflakeConnector(connection_name='Snowflake')
            self.project_id = self.meta_database_connector.database

        self.embedding_model = os.getenv("OPENAI_HARVESTER_EMBEDDING_MODEL", 'text-embedding-3-large')
  
        #self.index, self.metadata_mapping = AnnoyIndexSingleton.get_index_and_metadata(self.meta_database_connector.metadata_table_name, vector_size, refresh=refresh)
        self.index, self.metadata_mapping = load_or_create_embeddings_index(self.meta_database_connector.metadata_table_name, refresh=False)

        # todo: have harvester add to this and save a new one 
        self.client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        logger.info(f"kb OpenAI API Key: {os.getenv('OPENAI_API_KEY')}")

    # Function to get embedding (reuse or modify your existing get_embedding function)
    def get_embedding(self, text):
        
        response = self.client.embeddings.create(
            model=self.embedding_model,
            input=text.replace("\n", " ")  # Replace newlines with spaces
        )
        embedding = response.data[0].embedding
        return embedding

    def store_memory(self, memory, scope="user_preferences", thread_id=""):
        if (scope == "user_preferences" or scope == "general") and len(self.find_memory_local(memory, scope=scope)) > 0:
            logger.warn(f"store_memory - not storing duplicate memory {memory} in scope {scope} thread_id {thread_id}")
            return # FixMe: change memories to be accumulated asynchronously by Locutus
        """Stores a new memory in its own file within the specified scope directory."""
        scope_directory = f'{self.base_directory_path}/{scope}'
        if not os.path.exists(scope_directory):
            os.makedirs(scope_directory)  # Create the scope directory if it doesn't exist
        
        timestamp = time.strftime('%Y%m%d%H%M%S')
        unique_id = uuid.uuid4()  # Generate a random UUID
        file_name = f'memory_{timestamp}_{unique_id}.txt'
        file_path = f'{scope_directory}/{file_name}'
        with open(file_path, 'w') as file:
            file.write(memory)
    
    def find_memory_local(self, query, scope="user_preferences", thresh=0.2) -> list[str]:
        """Finds and returns memories that contain the query string within the specified scope."""
        scope_directory = f'{self.base_directory_path}/{scope}'
        if not os.path.exists(scope_directory) or not os.listdir(scope_directory):
            return []  # Return an empty list if the directory doesn't exist or is empty
        
        query_doc = nlp(query)  # Process the query to get its vector representation
        memories = []
        # Iterate over each file in the scope directory
        for filename in os.listdir(scope_directory):
            file_path = os.path.join(scope_directory, filename)
            # Skip directories
            if os.path.isdir(file_path):
                continue
            with open(file_path, 'r') as file:
                content = file.read()
                content_doc = nlp(content)  # Process the memory content to get its vector representation
                similarity = query_doc.similarity(content_doc)
                if similarity > thresh:  # Threshold for similarity, adjust as needed
                    memories.append(content.strip())
        return memories

    
    def get_full_metadata_details(self, source_name, database_name, schema_name, table_name):
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
        source_name_escaped = source_name.replace("'", "''")
        database_name_escaped = database_name.replace("'", "''")
        schema_name_escaped = schema_name.replace("'", "''")
        table_name_escaped = table_name.replace("'", "''")
        
        # Construct the SQL query to retrieve metadata
        query = f"""
            SELECT *
            FROM {self.meta_database_connector.metadata_table_name}
            WHERE source_name = '{source_name_escaped}'
              AND database_name = '{database_name_escaped}'
              AND schema_name = '{schema_name_escaped}'
              AND table_name = '{table_name_escaped}'
        """
        
        # Execute the query and fetch the result
        try:
            result = self.meta_database_connector.run_query(query)
            if result:
                # Assuming result is a list of dictionaries, return the first one
                return result[0]
            else:
                # If no result is found, return None
                return None
        except Exception as e:
            logger.error(f"Error retrieving metadata details: {e}")
            return None

    
    def find_memory(self, query, scope="database_metadata", top_n=15, verbosity="low", database=None, schema=None, table=None) -> list[str]:
        
        if scope == "database_metadata":

            if table:
                full_metadata = self.get_full_metadata_details(source_name=self.source_name, database_name=database, schema_name=schema, table_name=table)
                if full_metadata:
                    return [full_metadata]
                else:
                    return [f"No metadata details found for table '{table}' in schema '{schema}', database '{database}', source '{self.source_name}'."]

            memories = []

            if len(self.metadata_mapping) <= 1:
                logger.info('getting fresh stuff')
                self.index, self.metadata_mapping = load_or_create_embeddings_index(self.meta_database_connector.metadata_table_name, refresh=True)

            embedding = self.get_embedding(query)

            logger.info(f'embedding len {len(embedding)}')

            if top_n > 50:
                top_n = 50
            top_matches = self.index.get_nns_by_vector(embedding, top_n, include_distances=True)

            logger.info(f'top_matches len {len(top_matches)}')
            logger.info(f'top_matches {top_matches}')

            logger.info(f'self index: {self.index}')

            file_names = []
            for match in top_matches[0]:
                logger.info(f'match: {match}')
                logger.info(f'self.metadata_mapping: {self.metadata_mapping}')
                logger.info(f'self.metadata_mapping[match]: {self.metadata_mapping[match]}')
                file_name = self.metadata_mapping[match]
                logger.info(f'filename: {file_name}')
                file_names.append(file_name)
            logger.info(f'file names {file_names}')

            file_names_str = "'" + "', '".join(file_names) + "'"
            source_name_escaped = self.source_name.replace("'", "''")

            where_clauses = [f"source_name='{source_name_escaped}'"]
            # this isnt good as it post filters improperly
#            if database:
#                database_escaped = database.replace("'", "''")
#                where_clauses.append(f"database_name='{database_escaped}'")
#            if schema:
#                schema_escaped = schema.replace("'", "''")
#                where_clauses.append(f"schema_name='{schema_escaped}'")
            
            where_statement = " AND ".join(where_clauses) + f" AND qualified_table_name IN ({file_names_str})"
            
            if verbosity == "high":
      
                q = f"SELECT qualified_table_name as full_table_name, ddl as DDL_FULL, sample_data_text as sample_data FROM {self.meta_database_connector.metadata_table_name} WHERE {where_statement}"
                logger.info(q)
                content = self.meta_database_connector.run_query(q)
            else:
                q = f"SELECT qualified_table_name as full_table_name, ddl_short FROM {self.meta_database_connector.metadata_table_name} WHERE {where_statement}"
                logger.info(q) 
                content = self.meta_database_connector.run_query(q)
     
            for row in content:
                try:
                    print(row["FULL_TABLE_NAME"][:200]+"...")
                    logger.info(row["FULL_TABLE_NAME"][:200]+"...")
                except: 
                    print(row["ddl"][:200]+"...")
   
      #      content.append({"SEMANTIC_MODEL_NAME": '"!SEMANTIC"."GENESIS_TEST"."GENESIS_INTERNAL"."SEMANTIC_STAGE"."revenue.yaml"', 
      #                      'DESCRIPTION': 'This semantic model points to data related to revenue history and revenue forecast, including COGS and other related items.',
      #                      'USAGE INSTRUCTIONS': "You can use the semantic_copilot function to use this semantic model to access data about these topics."})

            memories.append(content)
            logger.info(str(content))
            return memories
            #    file_name = self.metadata_mapping[idx[0]]

                # Todo, get the memories in a single query with an IN list, and add error handling
            #    content = self.meta_database_connector.run_query("SELECT ddl, summary, sample_data_text as sample_data from hello-prototype.ELSA_INTERNAL.database_metadata where memory_file_name='"+file_name+"'")
            #    print(f"Match in DB: {file_name}, Score: {idx[1]}, Content Preview: {content[:100]}\n")
            #    memories.append(content[0])

                #with open(self.base_directory_path+'/'+scope+'/'+file_name, 'r') as file:
                #    content = file.read()
                #    print(f"Match: {file_name}, Score: {idx[1]}, Content Preview: {content[:100]}\n")
                #    memories.append(content)

            #return memories
        else:
            return self.find_memory_local(scope=scope, query=query)

