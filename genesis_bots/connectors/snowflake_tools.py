import os
from genesis_bots.core.bot_os_memory import BotOsKnowledgeAnnoy_Metadata, BotOsKnowledgeBase
#from genesis_bots.connectors.bigquery_connector import BigQueryConnector
from genesis_bots.connectors.snowflake_connector.snowflake_connector import SnowflakeConnector
from genesis_bots.connectors.sqlite_connector import SqliteConnector
from genesis_bots.connectors.data_connector import DatabaseConnector
from genesis_bots.connectors.bot_snowflake_connector import bot_credentials

from genesis_bots.core.logging_config import logger

snowflake_functions = [
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
                        "description": "The name of the database. Use your WORKSPACE database unless told to use something else.",
                    },
                    "schema": {
                        "type": "string",
                        "description": "The name of the schema.  Use your WORKSPACE schema unless told to use something else.",
                    },
                    "stage": {
                        "type": "string",
                        "description": "The name of the stage to add the file to. Use your WORKSPACE stage unless told to use something else.",
                    },
                    "file_name": {
                        "type": "string",
                        "description": "The original filename of the file, human-readable. Can optionally include a relative path, such as bot_1_files/file_name.txt",
                    },
                },
                "required": [
                    "database",
                    "schema",
                    "stage",
            #        "openai_file_id",
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
                    "is_binary": {
                        "type": "boolean",
                        "description": "Whether to return the contents of the file as binary or text.",
                        "default": False,
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
    },
    {
        "type": "function",
        "function": {
            "name": "cortex_search",
            "description": "Use this to search a cortex full text search index.  Do not use this to look for database metadata or tables, for that use search_metadata instead.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "A short search query of what kind of data the user is looking for.",
                    },
                    "service_name": {
                        "type": "string",
                        "description": "Name of the service. You must know this in advance and specify it exactly.",
                    },
                    "top_n": {
                        "type": "integer",
                        "description": "How many of the top results to return, max 25, default 15.  Use 15 to start.",
                        "default": 1,
                    },
                },
                "required": ["query", "service_name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "_run_snowpark_python",
            "description": "Executes a string of Python snowflake snowpark code using a precreated and provided 'session', do not create a new session. Use this instead of code_interpreter when directed to use snowpark, or when you want to run python that can directly interact with the user's snowflake session, tables, and stages.  Results should only have a single object.  Multiple objects are not allowed.  Provide EITHER the 'code' field with the python code to run, or the 'note_id' field with the id of the note that contains the code you want to run. Do not ever attempt to load the code from the note.  If the note id is present, pass only id to the tool.  The tool will know how to get the code from the note. this function has an existing snowflake session inside that you can use called session so do not try to create a new session or connection.",
            "parameters": {
                "type": "object",
                "properties": {
                    "purpose": {
                        "type": "string",
                        "description": "A detailed explanation in English of what this code is supposed to do. This will be used to help validate and debug your code.",
                    },
                    "code": {
                        "type": "string",
                        "description": "The Python code to execute in Snowflake Snowpark. The snowpark 'session' is already created and ready for your code's use, do NOT create a new session. Run queries inside of Snowpark versus inserting a lot of static data in the code. Use the full names of any stages with database and schema. If you want to access a file, first save it to stage, and then access it at its stage path, not just /tmp. Always set 'result' variable at the end of the code execution in the global scope to what you want to return. DO NOT return a path to a file. Instead, return the file content by first saving the content to /tmp (not root) then base64-encode it and respond like this: image_bytes = base64.b64encode(image_bytes).decode('utf-8')\nresult = { 'type': 'base64file', 'filename': file_name, 'content': image_bytes, mime_type: <mime_type>}. Be sure to properly escape any double quotes in the code. NOTE: keep the above instructions in sync with escalate_for_advice()",
                    },
                    "packages": {
                        "type": "string",
                        "description": "A comma-separated list of required non-default Python packages to be pip installed for code execution (do not include any standard python libraries).",
                    },
                    "note_id": {
                        "type": "string",
                        "description": "An id for a note in the notebook table. The note_id will be used to look up the python code from the note content in lieu of the code field. A note_id will take precendent over the code field, that is, if the note_id is not empty, the contents of the note will be run instead of the content of the code field."
                    },
                    "save_artifacts": {
                        "type": "boolean",
                        # TODO: clarify that fetching artifacts is possible with a new tool when it is ready
                        "description": "A flag determining whether to save any output from the executed python code (encoded as a base64 string) as an 'artifact'  When this flag is set, the result will contain a UUID called 'artifact_id' for referencing the output in the future.  When this flag is not set, any output from the python code will be saved to a local file and the result will contain a path to that file. This local file should not be considered accessible by outside systems. "
                    },
                },
                "required": ["purpose"],
            },
        },
    }
]

snowflake_tools = {
    "_run_snowpark_python": "db_adapter.run_python_code",
    "_cortex_search": "db_adapter.cortex_search",
    "_list_stage_contents": "db_adapter.list_stage_contents",
    "_add_file_to_stage": "db_adapter.add_file_to_stage",
    "_read_file_from_stage": "db_adapter.read_file_from_stage",
    "_delete_file_from_stage": "db_adapter.delete_file_from_stage",
}