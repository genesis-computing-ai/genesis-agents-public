import os
from core.bot_os_memory import BotOsKnowledgeAnnoy_Metadata, BotOsKnowledgeBase
from connectors.bigquery_connector import BigQueryConnector
from connectors.snowflake_connector.snowflake_connector import SnowflakeConnector
from connectors.sqlite_connector import SqliteConnector
from connectors.data_connector import DatabaseConnector
from connectors.bot_snowflake_connector import bot_credentials

from core.logging_config import logger


process_scheduler_functions = [
    {
        "type": "function",
        "function": {
            "name": "_process_scheduler",
            "description": "Manages schedules to automatically run processes on a schedule (sometimes called tasks), including creating, updating, and deleting schedules for processes.",
            "parameters": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "description": "The action to perform on the process schedule: CREATE, UPDATE, or DELETE.  Or LIST to get details on all scheduled processes for a bot, or TIME to get current system time or HISTORY to get the history of a scheduled process by task_id.  For history lookup task_id first using LIST.  To deactive a schedule without deleting it, UPDATE it and set task_active to FALSE.",
                    },
                    "bot_id": {
                        "type": "string",
                        "description": "The identifier of the bot for which to manage scheduled_processes.",
                    },
                    "history_rows": {
                        "type": "integer",
                        "description": "For action HISTORY, how many history rows about runs of the task to return.",
                    },
                    "task_id": {
                        "type": "string",
                        "description": "The unique identifier of the process schedule, create as bot_id_<random 6 character string>. MAKE SURE TO DOUBLE-CHECK THAT YOU ARE USING THE CORRECT task_id, its REQUIRED ON CREATE, UPDATES AND DELETES! Note that this is not the same as the process_id",
                    },
                    "task_details": {
                        "type": "object",
                        "description": "The properties of this object are the details of the process schedule for use when creating and updating.",
                        "properties": {
                            "process_name": {
                                "type": "string",
                                "description": "The name of the process to run on a schedule. This must be a valid process name shown by _manage_processes LIST",
                            },
                            "primary_report_to_type": {
                                "type": "string",
                                "description": "Set to SLACK_USER",
                            },
                            "primary_report_to_id": {
                                "type": "string",
                                "description": "The Slack USER ID of the person who told you to create this schedule for a process.",
                            },
                            "next_check_ts": {
                                "type": "string",
                                "description": "The timestamp for the next run of the process 'YYYY-MM-DD HH:MM:SS'. Call action TIME to get current time and timezone. Make sure this time is in the future.",
                            },
                            "action_trigger_type": {
                                "type": "string",
                                "description": "Always set to TIMER",
                            },
                            "action_trigger_details": {
                                "type": "string",
                                "description": "For TIMER, a description of when to call the task, eg every hour, Tuesdays at 9am, every morning.  Also be clear about whether the task should be called one time, or is recurring, and if recurring if it should recur forever or stop at some point.",
                            },
                            "last_task_status": {
                                "type": "string",
                                "description": "The current status of the scheduled process.",
                            },
                            "task_learnings": {
                                "type": "string",
                                "description": "Leave blank on creation, don't change on update unless instructed to.",
                            },
                            "task_active": {
                                "type": "boolean",
                                "description": "TRUE if active, FALSE if not active.  Set to FALSE to Deactivate a schedule for a process.",
                            },
                        },
                        "required": [
                            "task_name",
                            "action_trigger_details",
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

# depreciated
autonomous_functions = []

artifact_manager_functions = [
    {
        "type": "function",
        "function": {
            "name": "_manage_artifact",
            "description": "Get information or manage artifacts. Artifacts are files that are generated by functions that save their outputs as artifacts.",
            "parameters": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "description": "The action to perform on the given artifact. Possible actions are: (a) DESCRIBE : fetch metadata for this artifact (including a refence to this artifact for creating a URL to it). (b) DELETE : delete the given artifact from internal storage."
                    },
                    "artifact_id": {
                        "type": "string",
                        "description": "A valid artifact ID, which is a UUID-4 string.",
                    },
                },
                "required": ["action", "artifact_id"],
            },
        },
    }
]

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

manage_tests_functions = [
    {
        "type": "function",
        "function": {
            "name": "_manage_tests",
            "description": "Manages tests that will run when when the project is deployed, including adding, updating, listing and deleting tests from the list of tests to run when the project is deployed, allowing bots to manage tests. Remember that this is not used to create new processes.  Make sure that the user is specifically asking for a test to be added to the deploy sequence, have its priority weighting updated, or deleted. This tool is not used to run a test. If you are asked to run a tests, use the run process tool and pass the manage_process_id, do not use this tool.  If you arent sure, ask the user to clarify. If you are asked to enable a test, set its test_type to enabled.  If you are asked to disable a test, set its test_type to disabled.",
            "parameters": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "description": """
                        The action to perform on a tests: ADD, UPDATE, DELETE,
                        LIST, ENABLE, DISABLE returns a list of all tests, SHOW shows all fields of a test,
                        or TIME to get current system time.""",
                    },
                    "bot_id": {
                        "type": "string",
                        "description": "The identifier of the bot that is having its processes tested.",
                    },
                    "test_process_id": {
                        "type": "string",
                        "description": "The unique identifier of the process_id. MAKE SURE TO DOUBLE-CHECK THAT YOU ARE USING THE CORRECT test_process_id ON UPDATES AND DELETES!  Required for CREATE, UPDATE, and DELETE.",
                    },
                    "test_process_name": {
                        "type": "string",
                        "description": "Human reable unique name for the test.",
                    },
                    "test_type": {
                        "type": "string",
                        "description": "The type of table, either enabled or disabled.",
                    },
                    "test_priority": {
                        "type": "integer",
                        "description": "Determines the order in which the tests will run.  Lower numbers run first.",
                    },
                },
                "required": ["action", "bot_id"],
            },
        },
    }
]

google_drive_functions = [
    {
        "type": "function",
        "function": {
            "name": "_google_drive",
            "description": "Performs certain actions on Google Drive, including logging in, listing files, setting the root folder, and getting the version number of a google file (g_file).  Other actions may be added in the future.",
            "parameters": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "description": "The action to be performed on Google Drive.  Possible actions are: LOGIN - Used to login in to Google Workspace with OAuth2.0.  Not implemented LIST - Gets list of files in a folder.  Same as DIRECTORY, DIR, GET FILES IN FOLDER SET_ROOT_FOLDER - Sets the root folder for the user on their drive GET_FILE_VERSION_NUM - Gets the version numbergiven a g_file id GET_COMMENTS - Gets the comments and replies for a file give a g_file_id ADD_COMMENT - Adds a comment to a file given a g_file_id ADD_REPLY_TO_COMMENT - Adds a reply to a comment given a g_file_id and a comment_id GET_SHEET - (Also can be READ_SHEET) - Gets the contents of a Google Sheet given a g_file_id EDIT_SHEET - (Also can be WRITE SHEET) - Edits a Google Sheet given a g_file_id and values.  Passing a cell range is optional GET_LINK_FROM_FILE_ID - Gets the url link to a file given a g_file_id GET_FILE_BY_NAME - Searches for a file by name and returns the file id SAVE_QUERY_RESULTS_TO_G_SHEET - Saves the results of a query to a Google Sheet",
                    },
                    "user": {
                        "type": "string",
                        "description": "The unique identifier of the process_id. MAKE SURE TO DOUBLE-CHECK THAT YOU ARE USING THE CORRECT test_process_id ON UPDATES AND DELETES!  Required for CREATE, UPDATE, and DELETE.",
                    },
                    "g_file_id": {
                        "type": "string",
                        "description": "The unique identifier of a file stored on Google Drive.",
                    },
                    "g_sheet_cell": {
                        "type": "string",
                        "description": "Cell in a Google Sheet to edit/update.",
                    },
                    "g_sheet_value": {
                        "type": "string",
                        "description": "Value to update the cell in a Google Sheet or update a comment.",
                    },
                    "g_file_comment_id": {
                        "type": "string",
                        "description": "The unique identifier of a comment stored on Google Drive.",
                    },
                    "g_folder_id": {
                        "type": "string",
                        "description": "The unique identifier of a folder stored on Google Drive.",
                    },
                    "g_file_name": {
                        "type": "string",
                        "description": "The name of a file, files, folder, or folders stored on Google Drive.",
                    },
                    "g_sheet_query": {
                        "type": "string",
                        "description": "Query string to run and save the results to a Google Sheet.",
                    },
                },
                "required": ["action"],
            },
        },
    }
]

process_manager_functions = [
    {
        "type": "function",
        "function": {
            "name": "_manage_processes",
            "description": "Manages processes for bots, including creating, updating, listing and deleting processes allowing bots to manage processes.  Remember that this is not used to create new bots",
            "parameters": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "description": "The action to perform on a process: CREATE, UPDATE, DELETE, CREATE_PROCESS_CONFIG, UPDATE_PROCESS_CONFIG, DELETE_PROCESS_CONFIG, ALLOW_CODE, HIDE_PROCESS, UNHIDE_PROCESS, LIST returns a list of all processes, SHOW shows full instructions and details for a process, SHOW_CONFIG shows the configuration for a process, HIDE_PROCESS hides the process from the list of processes, UNHIDE_PROCESS unhides the process from the list of processes, or TIME to get current system time.  If you are trying to deactivate a schedule for a task, use _process_scheduler instead, dont just DELETE the process. ALLOW_CODE is used to bypass the restriction that code must be added as a note",
                    },
                    "bot_id": {
                        "type": "string",
                        "description": "The identifier of the bot that is having its processes managed.",
                    },
                    "process_id": {
                        "type": "string",
                        "description": "The unique identifier of the process, create as bot_id_<random 6 character string>. MAKE SURE TO DOUBLE-CHECK THAT YOU ARE USING THE CORRECT process_id ON UPDATES AND DELETES!  Required for CREATE, UPDATE, and DELETE.",
                    },
                    "process_name": {
                        "type": "string",
                        "description": "The name of the process. Required for SHOW.",
                    },
                    "process_instructions": {
                        "type": "string",
                        "description": "DETAILED instructions for completing the process  Do NOT summarize or simplify instructions provided by a user.",
                    },
                    "process_config": {
                        "type": "string",
                        "description": "Configuration string used by process when running.",
                    },
                    "hidden": {
                        "type": "boolean",
                        "description": "If true, the process will not be shown in the list of processes.  This is used to create processes to test the bots functionality without showing them to the user.",
                        "default": False,
                    },
                },
                "required": ["action", "bot_id", "process_instructions"],
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

google_drive_tools = {"_google_drive": "tool_belt.google_drive"}
manage_tests_tools = {"_manage_tests": "tool_belt.manage_tests"}
process_manager_tools = {"_manage_processes": "tool_belt.manage_processes"}
process_scheduler_tools = {"_process_scheduler": "tool_belt.process_scheduler"}
notebook_manager_tools = {"_manage_notebook": "tool_belt.manage_notebook"}
artifact_manager_tools = {"_manage_artifact": "tool_belt.manage_artifact"}

