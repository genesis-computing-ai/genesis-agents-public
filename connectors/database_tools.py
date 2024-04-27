import json
import os
from core.bot_os_memory import BotOsKnowledgeAnnoy_Metadata, BotOsKnowledgeBase
from connectors.bigquery_connector import BigQueryConnector
from connectors.snowflake_connector import SnowflakeConnector
from connectors.database_connector import DatabaseConnector

import logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.WARN, format='%(asctime)s - %(levelname)s - %(message)s')

database_tool_functions = [
    
    {
        "type": "function",
        "function": {
            "name": "search_metadata",
            "description": "Searches metadata to find the top relevant data assets.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "The search query to find relevant metadata about data assets."},
                    "top_n": {"type": "integer", "description": "How many of the top results to return, max 50, default 8", "default": 8},
                    "database": {"type": "string", "description": "Use when you want to constrain the search to a specific database."},
                    "schema": {"type": "string", "description": "Use to constrain the search to a specific schema."},
                },
                "required": ["query"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "run_query",
            "description": "Run a query against a data connection. Use either search_metadata first to determine the right object names to query, or use semantic_copilot to get ready-to-execute SQL.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "The SQL query to run. Be sure to fully qualify all object names with double-quoted 3-part names: \"<database>\".\"<schema>\".\"<table>\""},
                    "connection": {"type": "string", "description": "The name of the data connection to run the query on, for example BigQuery or Snowflake."},
                    "max_rows": {"type": "integer", "description": "The maximum number of rows to return.  This can be up to 100. The default is 20.", "default":20},
                },
                "required": ["query", "connection", "max_rows"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "semantic_copilot",
            "description": "Calls the Snowflake semantic copilot to generate proposed SQL against a semantic model.  Only use this when you know the name of an existing !SEMANTIC object, or get one in response to search_metadata.  If you can answer the users question by executing the resulting SQL, call run_query with that SQL to get the answers.",
            "parameters": {
                "type": "object",
                "properties": {
                    "prompt": {"type": "string", "description": "Brief but complete natural language description of what you want the resulting SQL to do."},
                    "semantic_model": {"type": "string", "description": "The semantic model in the format \"!SEMANTIC\".\"database\".\"schema\".\"stage\".\"model\" to use for the copilot."},
                },
                "required": ["prompt", "semantic_model"]
            }
        }
    },
        {
        "type": "function",
        "function": {
            "name": "get_full_table_details",
            "description": "Gets full verbose details for a specific table including full DDL and sample data.",
            "parameters": {
                "type": "object",
                "properties": {
                    "database": {"type": "string", "description": "The name of the database where the table is located."},
                    "schema": {"type": "string", "description": "The name of the schema where the table is located."},
                    "table": {"type": "string", "description": "The name of the table to retrieve full details for."},
                    "query": {"type": "string", "description": "Always use *."},
                },
                "required": ["database", "schema", "table", "query"]
            }
        }
    },
]

snowflake_semantic_functions = [
    {
        "type": "function",
        "function": {
            "name": "_get_semantic_model",
            "description": "Retrieves an existing semantic model from the map based on the model name and thread id.",
            "parameters": {
                "type": "object",
                "properties": {
                    "model_name": {"type": "string", "description": "The name of the model to retrieve."},
                },
                "required": ["model_name"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "_modify_semantic_model",
            "description": "Modifies an existing semantic model. Call command 'help' for full instructions.",
            "parameters": {
                "type": "object",
                "properties": {
                    "model_name": {"type": "string", "description": "The name of the model to modify."},
                    "command": {"type": "string", "description": "The command to run, call command 'help' for full instructions.."},
                    "parameters": {"type": "string", "description": "The command's parameters expressed in a JSON string."},
                },
                "required": ["model_name", "command", "parameters"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "_initialize_semantic_model",
            "description": "Creates an empty semantic model and stores it in a map with the thread_id as the key.",
            "parameters": {
                "type": "object",
                "properties": {
                    "model_name": {"type": "string", "description": "The name of the model to initialize."},
                    "model_description": {"type": "string", "description": "Description of the new semantic model."},
                },
                "required": ["model_name",]
            }
        }
    },
]


snowflake_stage_functions = [
    {
        "type": "function",
        "function": {
            "name": "_list_stage_contents",
            "description": "Lists the contents of a given Snowflake stage.",
            "parameters": {
                "type": "object",
                "properties": {
                    "database": {"type": "string", "description": "The name of the database."},
                    "schema": {"type": "string", "description": "The name of the schema."},
                    "stage": {"type": "string", "description": "The name of the stage to list contents for."},
                },
                "required": ["database", "schema", "stage"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "_add_file_to_stage",
            "description": "Uploads a file from an OpenAI FileID to a Snowflake stage. Replaces if exists.",
            "parameters": {
                "type": "object",
                "properties": {
                    "database": {"type": "string", "description": "The name of the database."},
                    "schema": {"type": "string", "description": "The name of the schema."},
                    "stage": {"type": "string", "description": "The name of the stage to add the file to."},
                    "openai_file_id": {"type": "string", "description": "A valid OpenAI FileID referencing the file to be loaded to stage."},
                    "file_name": {"type": "string", "description": "The original filename of the file, human-readable, NOT file-xxxx."}
                },
                "required": ["database", "schema", "stage", "openai_file_id", "file_name"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "_read_file_from_stage",
            "description": "Reads a file from a Snowflake stage.",
            "parameters": {
                "type": "object",
                "properties": {
                    "database": {"type": "string", "description": "The name of the database."},
                    "schema": {"type": "string", "description": "The name of the schema."},
                    "stage": {"type": "string", "description": "The name of the stage to read the file from."},
                    "file_name": {"type": "string", "description": "The name of the file to be read."},
                    "return_contents": {"type": "boolean", "description": "Whether to return the contents of the file or just the file name.", "default": True},
                },
                "required": ["database", "schema", "stage", "file_name"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "_delete_file_from_stage",
            "description": "Deletes a file from a Snowflake stage.",
            "parameters": {
                "type": "object",
                "properties": {
                    "database": {"type": "string", "description": "The name of the database."},
                    "schema": {"type": "string", "description": "The name of the schema."},
                    "stage": {"type": "string", "description": "The name of the stage to delete the file from."},
                    "file_name": {"type": "string", "description": "The name of the file to be deleted."},
                },
                "required": ["database", "schema", "stage", "file_name"]
            }
        }
    },
]

snowflake_semantic_tools = {
    "_get_semantic_model": "db_adapter.get_semantic_model",
    "_modify_semantic_model": "db_adapter.modify_and_update_semantic_model",
    "_initialize_semantic_model": "db_adapter.initialize_semantic_model",
}

database_tools = {"run_query": "run_query_f.local", "search_metadata": "search_metadata_f.local", 
                 "semantic_copilot": "semantic_copilot_f.local",
                 "get_full_table_details": "search_metadata_f.local",}

snowflake_stage_tools = {
    "_list_stage_contents": "db_adapter.list_stage_contents",
    "_add_file_to_stage": "db_adapter.add_file_to_stage",
    "_read_file_from_stage": "db_adapter.read_file_from_stage",
    "_delete_file_from_stage": "db_adapter.delete_file_from_stage",
}


def bind_semantic_copilot(data_connection_info):
    def _semantic_copilot(prompt:str, semantic_model:str):
     #   if connection == 'Snowflake':
        my_dc = SnowflakeConnector('Snowflake')
     #   else:
     #       raise ValueError("Semantic copilot is only available for Snowflake connections.")

        logger.info(f"Semantic copilot called with prompt: {prompt} and semantic model: {semantic_model}")
        try:
            result = my_dc.semantic_copilot(prompt, semantic_model)
            return result
        except Exception as e:
            logger.error(f"Error in semantic_copilot: {str(e)}")
            return f"An error occurred while trying to generate SQL from the semantic model. {e}"

    return _semantic_copilot


def bind_run_query(data_connection_info:list):
    def _run_query(query:str, connection:str, max_rows:int=20):
        if connection != 'BigQuery':
            my_dc = [SnowflakeConnector('Snowflake')]
        else:
            my_dc = [BigQueryConnector(ci,'BigQuery') for ci in data_connection_info]

        for a in my_dc:
            print(a.connection_name) #FixMe: check the connection_name matches
            print("Query: ",query, " Connection: ", connection, " Max rows: ", max_rows)
            logger.info(f"_run_query - {a.connection_name}: {query}")
            results = a.run_query(query, max_rows)
            return results

    return _run_query


def bind_search_metadata(knowledge_base_path):
    
    def _search_metadata(query:str, scope="database_metadata", database=None, schema=None, table=None, top_n=8, verbosity="low"):
        """
        Exposes the find_memory function to be callable by OpenAI.
        :param query: The query string to search memories for.
        :return: The search result from find_memory.
        """

        import logging
        logger = logging.getLogger(__name__)
        logging.basicConfig(level=logging.WARN, format='%(asctime)s - %(levelname)s - %(message)s')

        logger.info(f"Search metadata called with query: {query}, scope: {scope}, top_n: {top_n}, verbosity: {verbosity}")
        try:
            print("Search metadata: ",query," Scope: ",scope," Top_n: ",top_n," Verbosity: ", verbosity)
            # Adjusted to include scope in the call to find_memory
           # logger.info(f"GETTING NEW ANNOY - Refresh True - --- Search metadata called with query: {query}, scope: {scope}, top_n: {top_n}, verbosity: {verbosity}")
            my_kb = BotOsKnowledgeAnnoy_Metadata(knowledge_base_path, refresh=True)
           # logger.info(f"CALLING FIND MEMORY  --- Search metadata called with query: {query}, scope: {scope}, top_n: {top_n}, verbosity: {verbosity}")
            result = my_kb.find_memory(query, database=database, schema=schema, table=table, scope=scope, top_n=top_n, verbosity=verbosity)
            return result
        except Exception as e:
            logger.error(f"Error in find_memory_openai_callable: {str(e)}")
            return "An error occurred while trying to find the memory."
    
    return _search_metadata

