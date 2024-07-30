import json
import os
from core.bot_os_memory import BotOsKnowledgeAnnoy_Metadata, BotOsKnowledgeBase
from connectors.bigquery_connector import BigQueryConnector
from connectors.snowflake_connector import SnowflakeConnector
from connectors.database_connector import DatabaseConnector

import logging

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.WARN, format="%(asctime)s - %(levelname)s - %(message)s"
)

database_tool_functions = [
    {
        "type": "function",
        "function": {
            "name": "search_metadata",
            "description": "Searches metadata to find the top relevant tables or views. (This does not search sta.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "The search query to find relevant metadata tables or views.",
                    },
                    "top_n": {
                        "type": "integer",
                        "description": "How many of the top results to return, max 50, default 15.  Use 15 to",
                        "default": 15,
                    },
                    #                   "database": {"type": "string", "description": "Use when you want to constrain the search to a specific database, only use this when you already know for sure the name of the database."},
                    #                    "schema": {"type": "string", "description": "Use to constrain the search to a specific schema."},
                },
                "required": ["query"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "run_query",
            "description": "Run a query against a data connection. Use either search_metadata first to determine the right object names to query, or use semantic_copilot to get ready-to-execute SQL.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": 'The SQL query to run. Be sure to fully qualify all object names with double-quoted 3-part names: "<database>"."<schema>"."<table>".  If the user gives you a query to run without quoted names, use upper case for database, schema, and table names.',
                    },
                    "connection": {
                        "type": "string",
                        "description": "The name of the data connection, for example Snowflake.",
                    },
                    "max_rows": {
                        "type": "integer",
                        "description": "The maximum number of rows to return.  This can be up to 100. The default is 20.",
                        "default": 20,
                    },
                },
                "required": ["query", "connection", "max_rows"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "semantic_copilot",
            "description": "Calls the Snowflake semantic copilot to generate proposed SQL against a semantic model. Only this if you know the name of an existing semantic model.",
            "parameters": {
                "type": "object",
                "properties": {
                    "prompt": {
                        "type": "string",
                        "description": "Brief but complete natural language description of what you want the resulting SQL to do.",
                    },
                    "semantic_model": {
                        "type": "string",
                        "description": "The name of the semantic model.",
                    },
                    "prod": {
                        "type": "boolean",
                        "description": "True for a production model, false to use a dev non-prod model.",
                        "default": True,
                    },
                },
                "required": ["prompt", "semantic_model", "prod"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_full_table_details",
            "description": "Gets full verbose details for a specific table including full DDL and sample data.",
            "parameters": {
                "type": "object",
                "properties": {
                    "database": {
                        "type": "string",
                        "description": "The name of the database where the table is located.",
                    },
                    "schema": {
                        "type": "string",
                        "description": "The name of the schema where the table is located.",
                    },
                    "table": {
                        "type": "string",
                        "description": "The name of the table to retrieve full details for.",
                    },
                    "query": {"type": "string", "description": "Always use *."},
                },
                "required": ["database", "schema", "table", "query"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "_list_semantic_models",
            "description": "Lists the semantic models available in the system.",
            "parameters": {
                "type": "object",
                "properties": {
                    "prod": {
                        "type": "boolean",
                        "description": "True for production models, false for dev models. Omit for both.",
                        "default": False,
                    }
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "_get_semantic_model",
            "description": "Retrieves an existing semantic model from the map based on the model name and thread id.",
            "parameters": {
                "type": "object",
                "properties": {
                    "model_name": {
                        "type": "string",
                        "description": "The name of the model to retrieve.",
                    },
                },
                "required": ["model_name"],
            },
        },
    },
]

snowflake_semantic_functions = [
    {
        "type": "function",
        "function": {
            "name": "_modify_semantic_model",
            "description": "Modifies an existing semantic model. Call command 'help' for full instructions.",
            "parameters": {
                "type": "object",
                "properties": {
                    "model_name": {
                        "type": "string",
                        "description": "The name of the model to modify.",
                    },
                    "command": {
                        "type": "string",
                        "description": "The command to run, call command 'help' for full instructions..",
                    },
                    "parameters": {
                        "type": "string",
                        "description": "The command's parameters expressed in a JSON string.",
                    },
                },
                "required": ["model_name", "command", "parameters"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "_initialize_semantic_model",
            "description": "Creates an empty semantic model and stores it in a map with the thread_id as the key.",
            "parameters": {
                "type": "object",
                "properties": {
                    "model_name": {
                        "type": "string",
                        "description": "The name of the model to initialize.",
                    },
                    "model_description": {
                        "type": "string",
                        "description": "Description of the new semantic model.",
                    },
                },
                "required": [
                    "model_name",
                ],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "_deploy_semantic_model",
            "description": "Deploys / saves a semantic model to the production or dev based on the production flag.",
            "parameters": {
                "type": "object",
                "properties": {
                    "model_name": {
                        "type": "string",
                        "description": "The name of the model to deploy/save.",
                    },
                    "target_name": {
                        "type": "string",
                        "description": "The target name of the model to deploy/save, if different from model_name.",
                    },
                    "prod": {
                        "type": "boolean",
                        "description": "Flag to determine if the model should be deployed to production, or saved to dev. True deploy to production, False=save to dev.",
                        "default": False,
                    },
                },
                "required": ["model_name", "thread_id"],
            },
        },
    },
    # Section for loading a semantic model
    {
        "type": "function",
        "function": {
            "name": "_load_semantic_model",
            "description": "Loads a semantic model into the system for use.",
            "parameters": {
                "type": "object",
                "properties": {
                    "model_name": {
                        "type": "string",
                        "description": "The name of the semantic model to load.",
                    },
                    "prod": {
                        "type": "boolean",
                        "description": "Flag to indicate if the model is a production model. Defaults to false to load dev models.",
                        "default": False,
                    },
                },
                "required": ["model_name"],
            },
        },
    },
]

autonomous_functions = [
    {
        "type": "function",
        "function": {
            "name": "_manage_tasks",
            "description": "Manages autonomous tasks for bots, including creating, updating, and deleting autonomous tasks allowing bots to take scheduled autonomous actions.",
            "parameters": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "description": "The action to perform on the task: CREATE, UPDATE, or DELETE.  Or LIST to get details on all tasks for a bot, or TIME to get current system time.",
                    },
                    "bot_id": {
                        "type": "string",
                        "description": "The identifier of the bot for which to manage tasks.",
                    },
                    "task_id": {
                        "type": "string",
                        "description": "The unique identifier of the task, create as bot_id_<random 6 character string>. MAKE SURE TO DOUBLE-CHECK THAT YOU ARE USING THE CORRECT task_id ON UPDATES AND DELETES!",
                    },
                    "task_details": {
                        "type": "object",
                        "description": "The details of the task, required for create and update actions.",
                        "properties": {
                            "task_name": {
                                "type": "string",
                                "description": "The name of the task.",
                            },
                            "primary_report_to_type": {
                                "type": "string",
                                "description": "Set to SLACK_USER",
                            },
                            "primary_report_to_id": {
                                "type": "string",
                                "description": "The Slack USER ID of the person who told you to create the task.",
                            },
                            "next_check_ts": {
                                "type": "string",
                                "description": "The timestamp for the next check of the task in format 'YYYY-MM-DD HH:MM:SS'. Call action TIME to get current time and timezone. Make sure this time is in the future.",
                            },
                            "action_trigger_type": {
                                "type": "string",
                                "description": "TIMER or QUERY_ROWS",
                            },
                            "action_trigger_details": {
                                "type": "string",
                                "description": "For TIMER, a description of when to call the task, eg every hour, Tuesdays at 9am, every morning.  Also be clear about whether the task should be called one time, or is recurring, and if recurring if it should recur forever or stop at some point. For QUERY_ROWS the query for when any rows are returned the task should be triggered.",
                            },
                            "task_instructions": {
                                "type": "string",
                                "description": "Detailed instructions for completing the task.",
                            },
                            "reporting_instructions": {
                                "type": "string",
                                "description": "What information to report back on and how (post to channel, DM a user, etc.)",
                            },
                            "last_task_status": {
                                "type": "string",
                                "description": "The current status of the task.",
                            },
                            "task_learnings": {
                                "type": "string",
                                "description": "Leave blank on creation, don't change on update unless instructed to.",
                            },
                            "task_active": {
                                "type": "boolean",
                                "description": "Is task active",
                            },
                        },
                        "required": [
                            "task_name",
                            "action_trigger_details",
                            "task_instructions",
                            "reporting_instructions",
                            "last_task_status",
                            "task_learnings",
                            "task_active",
                        ],
                    },
                },
                "required": ["action", "bot_id"],
            },
        },
    }
]

process_manager_functions = [
    {
        "type": "function",
        "function": {
            "name": "_manage_processes",
            "description": "Manages processes for bots, including creating, updating, and deleting processes allowing bots to manage processes that can be run on other bots.  Remember that this is not used to create new bots",
            "parameters": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "description": "The action to perform on a process: CREATE, UPDATE, or DELETE.  LIST returns a list of all processes, SHOW shows details for a process, or TIME to get current system time.",
                    },
                    "bot_id": {
                        "type": "string",
                        "description": "The identifier of the bot that is having its processes managed.",
                    },
                    "process_id": {
                        "type": "string",
                        "description": "The unique identifier of the process, create as bot_id_<random 6 character string>. MAKE SURE TO DOUBLE-CHECK THAT YOU ARE USING THE CORRECT process_id ON UPDATES AND DELETES!  Required for CREATE, UPDATE, and DELETE.",
                    },
                    "process_details": {
                        "type": "object",
                        "description": "The details of the process, required for create and update actions.",
                        "properties": {
                            "process_name": {
                                "type": "string",
                                "description": "The name of the process.",
                            },
                       #     "process_details": {
                       #         "type": "string",
                       #         "description": "Details of the process",
                       #     },
                            "process_instructions": {
                                "type": "string",
                                "description": "Detailed instructions for completing the process.",
                            },
                         #   "process_reporting_instructions": {
                         #       "type": "string",
                         #       "description": "Detailed instructions for reporting the completed the process.",
                         #   },
                        },
                        "required": [
                            "process_name",
                    #        "process_details",
                            "process_instructions",
                         #   "process_reporting_instructions",
                        ],
                    },
                },
                "required": ["action", "bot_id"],
            },
        },
    }
]

snowflake_stage_functions = [
    {
        "type": "function",
        "function": {
            "name": "_list_stage_contents",
            "description": "Lists the contents of a given Snowflake stage, up to 50 results (use pattern param if more than that). Run SHOW STAGES IN SCHEMA <database>.<schema> to find stages.",
            "parameters": {
                "type": "object",
                "properties": {
                    "database": {
                        "type": "string",
                        "description": "The name of the database.",
                    },
                    "schema": {
                        "type": "string",
                        "description": "The name of the schema.",
                    },
                    "stage": {
                        "type": "string",
                        "description": "The name of the stage to list contents for.",
                    },
                    "pattern": {
                        "type": "string",
                        "description": "An optional regex pattern to limit the search for example /bot1_files/.* or document_.*",
                    },
                },
                "required": ["database", "schema", "stage"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "_add_file_to_stage",
            "description": "Uploads a file from an OpenAI FileID to a Snowflake stage. Replaces if exists.",
            "parameters": {
                "type": "object",
                "properties": {
                    "database": {
                        "type": "string",
                        "description": "The name of the database.",
                    },
                    "schema": {
                        "type": "string",
                        "description": "The name of the schema.",
                    },
                    "stage": {
                        "type": "string",
                        "description": "The name of the stage to add the file to.",
                    },
                    "openai_file_id": {
                        "type": "string",
                        "description": "A valid OpenAI FileID referencing the file to be loaded to stage.",
                    },
                    "file_name": {
                        "type": "string",
                        "description": "The original filename of the file, human-readable, NOT file-xxxx. Can optionally include a relative path, such as bot_1_files/file_name.txt",
                    },
                },
                "required": [
                    "database",
                    "schema",
                    "stage",
                    "openai_file_id",
                    "file_name",
                ],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "_read_file_from_stage",
            "description": "Reads a file from a Snowflake stage.",
            "parameters": {
                "type": "object",
                "properties": {
                    "database": {
                        "type": "string",
                        "description": "The name of the database.",
                    },
                    "schema": {
                        "type": "string",
                        "description": "The name of the schema.",
                    },
                    "stage": {
                        "type": "string",
                        "description": "The name of the stage to read the file from.",
                    },
                    "file_name": {
                        "type": "string",
                        "description": "The name of the file to be read.",
                    },
                    "return_contents": {
                        "type": "boolean",
                        "description": "Whether to return the contents of the file or just the file name.",
                        "default": True,
                    },
                },
                "required": ["database", "schema", "stage", "file_name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "_delete_file_from_stage",
            "description": "Deletes a file from a Snowflake stage.",
            "parameters": {
                "type": "object",
                "properties": {
                    "database": {
                        "type": "string",
                        "description": "The name of the database.",
                    },
                    "schema": {
                        "type": "string",
                        "description": "The name of the schema.",
                    },
                    "stage": {
                        "type": "string",
                        "description": "The name of the stage to delete the file from.",
                    },
                    "file_name": {
                        "type": "string",
                        "description": "The name of the file to be deleted.",
                    },
                },
                "required": ["database", "schema", "stage", "file_name"],
            },
        },
    },  # Section for listing semantic models
]

image_functions = [
    {
        "type": "function",
        "function": {
            "name": "_analyze_image",
            "description": "Generates a textual description of an image.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "The question about the image.",
                    },
                    "openai_file_id": {
                        "type": "string",
                        "description": "The OpenAI file ID of the image.",
                    },
                    "file_name": {
                        "type": "string",
                        "description": "The name of the image file.",
                    },
                },
                "required": ["query", "openai_file_id", "file_name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "_generate_image",
            "description": "Generates an image using OpenAI's DALL-E 3.",
            "parameters": {
                "type": "object",
                "properties": {
                    "prompt": {
                        "type": "string",
                        "description": "Description of the image to create.",
                    }
                },
                "required": ["prompt"],
            },
        },
    },
]

image_tools = {
    "_analyze_image": "db_adapter.image_analysis",
    "_generate_image": "db_adapter.image_generation",
}


snowflake_semantic_tools = {
    "_modify_semantic_model": "db_adapter.modify_and_update_semantic_model",
    "_initialize_semantic_model": "db_adapter.initialize_semantic_model",
    "_deploy_semantic_model": "db_adapter.deploy_semantic_model",
    "_load_semantic_model": "db_adapter.load_semantic_model",
}

database_tools = {
    "run_query": "run_query_f.local",
    "search_metadata": "search_metadata_f.local",
    "semantic_copilot": "semantic_copilot_f.local",
    "get_full_table_details": "search_metadata_f.local",
    "_list_semantic_models": "db_adapter.list_semantic_models",
    "_get_semantic_model": "db_adapter.get_semantic_model",
}

snowflake_stage_tools = {
    "_list_stage_contents": "db_adapter.list_stage_contents",
    "_add_file_to_stage": "db_adapter.add_file_to_stage",
    "_read_file_from_stage": "db_adapter.read_file_from_stage",
    "_delete_file_from_stage": "db_adapter.delete_file_from_stage",
}

autonomous_tools = {"_manage_tasks": "db_adapter.manage_tasks"}

process_runner_tools = {"_run_process": "tool_belt.run_process"}
process_manager_tools = {"_manage_processes": "db_adapter.manage_processes"}


def bind_semantic_copilot(data_connection_info):
    def _semantic_copilot(prompt: str, semantic_model: str, prod: bool = True):
        #   if connection == 'Snowflake':
        my_dc = SnowflakeConnector("Snowflake")
        #   else:
        #       raise ValueError("Semantic copilot is only available for Snowflake connections.")

        logger.info(
            f"Semantic copilot called with prompt: {prompt} and semantic model: {semantic_model}"
        )
        try:
            result = my_dc.semantic_copilot(
                prompt=prompt, semantic_model=semantic_model, prod=prod
            )
            return result
        except Exception as e:
            logger.error(f"Error in semantic_copilot: {str(e)}")
            return f"An error occurred while trying to generate SQL from the semantic model. {e}"

    return _semantic_copilot


def bind_run_query(data_connection_info: list):
    def _run_query(query: str, connection: str, max_rows: int = 20, bot_id: str = None):
        if connection != "BigQuery":
            my_dc = [SnowflakeConnector("Snowflake")]
        else:
            my_dc = [BigQueryConnector(ci, "BigQuery") for ci in data_connection_info]

        for a in my_dc:
            print(a.connection_name)  # FixMe: check the connection_name matches
            print(
                "Query: len=",
                len(query),
                " Connection: ",
                connection,
                " Max rows: ",
                max_rows,
            )
            logger.info(f"_run_query - {a.connection_name}: {query}")
            results = a.run_query(query, max_rows, bot_id=bot_id)
            return results

    return _run_query


def bind_search_metadata(knowledge_base_path):

    def _search_metadata(
        query: str,
        scope="database_metadata",
        database=None,
        schema=None,
        table=None,
        top_n=8,
        verbosity="low",
    ):
        """
        Exposes the find_memory function to be callable by OpenAI.
        :param query: The query string to search memories for.
        :return: The search result from find_memory.
        """

        import logging

        logger = logging.getLogger(__name__)
        logging.basicConfig(
            level=logging.WARN, format="%(asctime)s - %(levelname)s - %(message)s"
        )

        # logger.info(f"Search metadata called with query: {query}, scope: {scope}, top_n: {top_n}, verbosity: {verbosity}")
        try:
            if isinstance(top_n, str):
                try:
                    top_n = int(top_n)
                except ValueError:
                    top_n = 8
            print(
                "Search metadata: query len=",
                len(query),
                " Top_n: ",
                top_n,
                " Verbosity: ",
                verbosity,
            )
            # Adjusted to include scope in the call to find_memory
            # logger.info(f"GETTING NEW ANNOY - Refresh True - --- Search metadata called with query: {query}, scope: {scope}, top_n: {top_n}, verbosity: {verbosity}")
            my_kb = BotOsKnowledgeAnnoy_Metadata(knowledge_base_path, refresh=True)
            # logger.info(f"CALLING FIND MEMORY  --- Search metadata called with query: {query}, scope: {scope}, top_n: {top_n}, verbosity: {verbosity}")
            result = my_kb.find_memory(
                query,
                database=database,
                schema=schema,
                table=table,
                scope=scope,
                top_n=top_n,
                verbosity=verbosity,
            )
            return result
        except Exception as e:
            logger.error(f"Error in find_memory_openai_callable: {str(e)}")
            return "An error occurred while trying to find the memory."

    return _search_metadata
