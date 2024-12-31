import os
from core.bot_os_memory import BotOsKnowledgeAnnoy_Metadata, BotOsKnowledgeBase
from connectors.bigquery_connector import BigQueryConnector
from connectors.snowflake_connector.snowflake_connector import SnowflakeConnector
from connectors.sqlite_connector import SqliteConnector
from connectors.data_connector import DatabaseConnector
from connectors.bot_snowflake_connector import bot_credentials

from core.logging_config import logger

notebook_manager_functions = [
    {
        "type": "function",
        "function": {
            "name": "_manage_notebook",
            "description": "Manages notes for bots, including creating, updating, listing and deleting notes, allowing bots to manage notebook.  Remember that this is not used to create new bots.  Make sure that the user is specifically asking for a note to be created, updated, or deleted. This tool is not used to run a note.  If you are asked to run a note, use the appropriate tool and pass the note_id, do not use this tool.  If you arent sure, ask the user to clarify.",
            "parameters": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "description": "The action to perform on a note: CREATE, UPDATE, DELETE, CREATE_NOTE_CONFIG, UPDATE_NOTE_CONFIG, DELETE_NOTE_CONFIG LIST returns a list of all notes, SHOW shows all fields of a note, or TIME to get current system time.",
                    },
                    "bot_id": {
                        "type": "string",
                        "description": "The identifier of the bot that is having its processes managed.",
                    },
                    "note_id": {
                        "type": "string",
                        "description": "The unique identifier of the note, create as bot_id_<random 6 character string>. MAKE SURE TO DOUBLE-CHECK THAT YOU ARE USING THE CORRECT note_id ON UPDATES AND DELETES!  Required for CREATE, UPDATE, and DELETE.",
                    },
                    "note_name": {
                        "type": "string",
                        "description": "Human reable unique name for the note.",
                    },
                    "note_type": {
                        "type": "string",
                        "description": "The type of note.  Should be 'process', 'snowpark_python', or 'sql'",
                    },
                    "note_content": {
                        "type": "string",
                        "description": "The body of the note",
                    },
                    "note_params": {
                        "type": "string",
                        "description": "Parameters that are used by the note",
                    },
                },
                "required": [
                    "action",
                    "bot_id",
                    "note_id",
                    "note_name",
                    "note_type",
                    "note_content",
                ],
            },
        },
    }
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
            "description": "Generates an image using OpenAI's DALL-E 3. Use this only to make pictures. To make PDFs or files, use Snowpark not this.",
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

autonomous_tools = {}
# autonomous_tools = {"_manage_tasks": "db_adapter.manage_tasks"}

notebook_manager_tools = {"_manage_notebook": "tool_belt.manage_notebook"}

