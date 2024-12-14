import json
import os
import time
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlunparse, urlencode
from datetime import datetime
import threading
import random
import string
from selenium import webdriver
import json
from typing import Optional, Dict, Any
import time, uuid
import jsonschema

from connectors.bigquery_connector import BigQueryConnector
from core import global_flags
from core.bot_os_tools_extended import load_user_extended_tools
from llm_openai.bot_os_openai import StreamingEventHandler

from google_sheets.g_sheets import get_g_file_version, get_g_file_comments, write_g_sheet_cell, read_g_sheet

import re
from typing import Optional
import collections

from jinja2 import Template
from bot_genesis.make_baby_bot import (
    MAKE_BABY_BOT_DESCRIPTIONS,
    make_baby_bot_tools,
    get_bot_details,
)
from connectors import database_tools
# from connectors import get_global_db_connector
# from connectors.bigquery_connector import BigQueryConnector
from connectors.snowflake_connector.snowflake_connector import SnowflakeConnector
from connectors.sqlite_connector import SqliteConnector
from llm_openai.openai_utils import get_openai_client
from slack.slack_tools import slack_tools, slack_tools_descriptions
from connectors.database_tools import (
    image_functions,
    image_tools,
    bind_run_query,
    bind_search_metadata,
    bind_search_metadata_detailed,
    bind_semantic_copilot,
    autonomous_functions,
    autonomous_tools,
    process_manager_tools,
    process_manager_functions,
    notebook_manager_tools,
    notebook_manager_functions,
    manage_tests_functions,
    manage_tests_tools,
    google_drive_tools,
    google_drive_functions,
    database_tool_functions,
    database_tools,
    snowflake_stage_functions,
    snowflake_stage_tools,
    snowflake_semantic_functions,
    snowflake_semantic_tools,
    process_scheduler_functions,
    process_scheduler_tools,
)
from schema_explorer.harvester_tools import (
    harvester_tools_list,
    harvester_tools_functions,
)
from development.integration_tools import (
    integration_tool_descriptions,
    integration_tools,
)
from core.bot_os import BotOsSession
from core.bot_os_corpus import URLListFileCorpus
from core.bot_os_defaults import (
    BASE_BOT_INSTRUCTIONS_ADDENDUM,
    BASE_BOT_PRE_VALIDATION_INSTRUCTIONS,
    BASE_BOT_PROACTIVE_INSTRUCTIONS,
    BASE_BOT_VALIDATION_INSTRUCTIONS,
    BASE_BOT_DB_CONDUCT_INSTRUCTIONS,
)
from core.bot_os_input import BotOsInputAdapter, BotOsInputMessage, BotOsOutputMessage
from core.bot_os_memory import BotOsKnowledgeAnnoy_Metadata

from core.bot_os_tool_descriptions import process_runner_tools
from core.bot_os_input import BotOsInputMessage, BotOsOutputMessage
from core.bot_os_artifacts import lookup_artifact_markdown, get_artifacts_store, ARTIFACT_ID_REGEX

# import sys
# sys.path.append('/Users/mglickman/helloworld/bot_os')  # Adjust the path as necessary


from core.bot_os_tool_descriptions import (
    process_runner_functions,
    process_runner_tools,
    webpage_downloader_functions,
    webpage_downloader_tools,
    data_dev_tools_functions,
    data_dev_tools,  #
    PROJECT_MANAGER_FUNCTIONS,
    project_manager_tools,
    git_file_manager_functions,
    git_file_manager_tools,
)
from core.bot_os_llm import BotLlmEngineEnum

from core.logging_config import logger
from core.bot_os_project_manager import ProjectManager
from core.file_diff_handler import GitFileManager
from connectors.snowflake_connector.snowflake_connector import SnowflakeConnector

genesis_source = os.getenv("GENESIS_SOURCE", default="Snowflake")

# We use this URL to include the genesis logo in snowflake-generated emails.
# TODO: use a permanent URL under the genesiscomputing.ai domain
GENESIS_LOGO_URL = "https://i0.wp.com/genesiscomputing.ai/wp-content/uploads/2024/05/Genesis-Computing-Logo-White.png"

# module level
belts = 0

class ToolBelt:
    def __init__(self):
        # self.db_adapter = db_adapter
        self.counter = {}
        self.instructions = {}
        self.process_config = {}
        self.process_history = {}
        self.done = {}
        self.silent_mode = {}
        self.last_fail= {}
        self.fail_count = {}
        self.lock = threading.Lock()
        self.recurse_stack = []
        self.recurse_level = 1
        global belts
        belts = belts + 1
        self.process_id = {}
        self.include_code = False

        if genesis_source == 'Sqlite':
            self.db_adapter = SqliteConnector(connection_name="Sqlite")
            connection_info = {"Connection_Type": "Sqlite"}
        elif genesis_source == 'Snowflake':  # Initialize Snowflake client
            self.db_adapter = SnowflakeConnector(connection_name="Snowflake")
            connection_info = {"Connection_Type": "Snowflake"}
        else:
            raise ValueError('Invalid Source')

        self.todos = ProjectManager(self.db_adapter)  # Initialize Todos instance
        self.git_manager = GitFileManager()
        self.server = None  # Will be set later

        self.sys_default_email = self.get_sys_email()
    #     logger.info(belts)

    def set_server(self, server):
        """Set the server instance for this toolbelt"""
        self.server = server

    def delegate_work(
            # if fast model errors, use regular one
            # x add a followup option to followup on a thread vs starting a new one
            # todo, add system prompt override, add tool limits, have delegated jobs skip the thread knowledge injection, etc.
            # x dont save to llm results table for a delegation
            # x see if they have a better time finding other bots now
            # x cancel the delegated run if timeout expires
            # make STOP on the main thread also cancel any inflight delegations
            # x fix bot todo updating, make sure full bot id is in assigned bot field so it can update todos, or allow name too
            # x allow work and tool calls from downstream bots to optionally filter back up to show up in slack while they are working (maybe with a summary like o1 does of whats happening)
        self,
        prompt: str,
        target_bot: Optional[str] = None,
        max_retries: int = 3,
        timeout_seconds: int = 300,
        thread_id: Optional[str] = None,
        status_update_callback = None,
        session_id = None,
        input_metadata = None,
        run_id = None,
        callback_id = None,
    ) -> Dict[str, Any]:
        """
        Internal method that implements the delegation logic.
        Creates a new thread with target bot and waits for JSON response.
        """
        og_thread_id = thread_id

        def _update_streaming_status(self, target_bot, current_summary, run_id, session_id, thread_id, status_update_callback, input_metadata):
            msg = f"      🤖 {target_bot}: _{current_summary}_"

            message_obj = {
                "type": "tool_call",
                "text": msg
            }
            if run_id is not None:
                StreamingEventHandler.run_id_to_messages[run_id].append(message_obj)

                # Initialize the array for this run_id if it doesn't exist
                if StreamingEventHandler.run_id_to_output_stream.get(run_id,None) is not None:
                    if StreamingEventHandler.run_id_to_output_stream.get(run_id,"").endswith('\n'):
                        StreamingEventHandler.run_id_to_output_stream[run_id] += "\n"
                    else:
                        StreamingEventHandler.run_id_to_output_stream[run_id] += "\n\n"
                    StreamingEventHandler.run_id_to_output_stream[run_id] += msg
                    msg = StreamingEventHandler.run_id_to_output_stream[run_id]
                else:
                    StreamingEventHandler.run_id_to_output_stream[run_id] = msg

                status_update_callback(session_id, BotOsOutputMessage(thread_id=thread_id, status="in_progress", output=msg+" 💬", messages=None, input_metadata=input_metadata))

        # current_summary = "Starting delegation"
        # _update_streaming_status(self, target_bot, current_summary, run_id, session_id, thread_id, status_update_callback, input_metadata)

        if self.server is None:
            return {
                "success": False,
                "error": "ToolBelt server reference not set. Cannot delegate work."
            }

        try:
            # Get target session
            target_session = None
            for session in self.server.sessions:
                if (target_bot is not None and session.bot_id.upper() == target_bot.upper()) or (target_bot is not None and session.bot_name.upper() == target_bot.upper()):
                    target_session = session
                    break

            if not target_session:
                # Get list of valid bot IDs and names
                valid_bots = [
                    {
                        "id": session.bot_id,
                        "name": session.bot_name
                    }
                    for session in self.server.sessions
                ]

                return {
                    "success": False,
                    "error": f"Could not find target bot with ID: {target_bot}. Valid bots are: {valid_bots}"
                }

            # Create new thread
            # Find the UDFBotOsInputAdapter
            udf_adapter = None
            for adapter in target_session.input_adapters:
                if adapter.__class__.__name__ == "UDFBotOsInputAdapter":
                    udf_adapter = adapter
                    break

            if udf_adapter is None:
                raise ValueError("No UDFBotOsInputAdapter found in target session")

            #   thread_id = target_session.create_thread(udf_adapter)
            # Add initial message
            # Define a generic JSON schema that all delegated tasks should conform to
            expected_json_schema = {
                "type": "object",
                "properties": {
                    "status": {
                        "type": "string",
                        "enum": ["success", "error", "partial"],
                        "description": "The status of the task execution"
                    },
                    "message": {
                        "type": "string",
                        "description": "A human readable message describing the result"
                    },
                    "data": {
                        "type": "object",
                        "description": "The actual result data from executing the task, if applicable"
                    },
                    "errors": {
                        "type": "array",
                        "items": {
                            "type": "string"
                        },
                        "description": "Any errors that occurred during execution"
                    }
                },
                "required": ["status", "message"]
            }
            validation_prompt = f"""
            You are being delegated tasks from another bot.
            Please complete the following task(s) to the best of your ability, then return the results.
            If appropriate, use this JSON format for your response:

            {json.dumps(expected_json_schema, indent=2)}

            Task(s):
            {prompt}
            """

            # Create thread ID for this task
            if callback_id is not None:
                thread_id = callback_id
            else:
                thread_id = 'delegate_' + str(uuid.uuid4())
            # Generate and store UUID for thread tracking
            uu = udf_adapter.submit(
                input=validation_prompt,
                thread_id=thread_id,
                bot_id={},
                file={}
            )

            # Wait for response with timeout
            start_time = time.time()
            attempts = 0
            last_response = ""
            previous_summary = ""
            _last_summary_time = time.time()
            while attempts < max_retries and (time.time() - start_time) < timeout_seconds:
                # Check if response available
                response = udf_adapter.lookup_udf(uu)
                # Check if response ends with chat emoji
                if response:
                    if response != last_response:
                        # Track last summary time
                        current_time = time.time()
                        if (current_time - _last_summary_time) >= 5 or not response.strip().endswith("💬"):
                            # Send current streaming response for summarization via chat completion
                            last_response = response
                            try:
                                # Only summarize if response has changed since last check
                                summary_response = ""
                                if previous_summary == "":
                                    summary_response = self.chat_completion(
                                        f"An AI bot is doing work, you are monitoring it. Please summarize in a few words what is happening in this ongoing response from another bot so far.  Be VERY Brief, use just a few words, not even a complete sentence.  Don't put a period on the end if its just one sentence or less.\nHere is the bots output so far:\n\n{response.strip()[:-2]}"
                                    , db_adapter=self.db_adapter, fast=True)
                                else:
                                    summary_response = self.chat_completion(
                                        f"An AI bot is doing work, you are monitoring it.  Based on its previous status updates, you have provided these summaries so far: \n<PREVIOUS_SUMMARIES_START>\n{previous_summary}\n</PREVIOUS_SUMMARIES_END>\n\nThe current output of the bot so far:\n<BOTS_OUTPUT_START>{response.strip()[:-2]}\n</BOTS_OUTPUT_END>\n\nNOW, Very briefly, in just a few words, summarize anything new the bot has done since the last update, that you have not mentioned yet in a previous summary.  Be VERY Brief, use just a few words, not even a complete sentence.  Don't put a period on the end if its just one sentence or less.  Don't repeat things you already said in previous summaries. If there has been no substantial change in the status, return only NO_CHANGE."
                                    , db_adapter=self.db_adapter, fast=True)
                                if summary_response and summary_response != 'NO_CHANGE':
                                    previous_summary = previous_summary + summary_response + '\n'
                                    current_summary = summary_response
                                    _update_streaming_status(self,target_bot, current_summary, run_id, session_id, og_thread_id, status_update_callback, input_metadata)
                                _last_summary_time = current_time
                            except Exception as e:
                                logger.error(f"Error getting response summary: {str(e)}")
                                _last_summary_time = current_time
                if response and response.strip().endswith("💬"):
                    time.sleep(2)
                    continue
                if response:
                    try:
                        # Extract the last JSON object from the response string
                        # Try to find JSON in code blocks first
                        # json_matches = re.findall(r'```json\n(.*?)\n```', response, re.DOTALL)
                        # if not json_matches:
                        #    # If no code blocks, try to find any JSON object in the response
                        #    json_matches = re.findall(r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}', response)
                        # if json_matches:
                        #    result = json.loads(json_matches[-1])  # Get last JSON match
                        # else:
                        #    # Try parsing the whole response as JSON if no code blocks found
                        #    result = json.loads(response)

                        # Validate against schema
                        # jsonschema.validate(result, expected_json_schema)
                        return {
                            "success": True,
                            "result": response,
                            "callback_id": thread_id
                        }
                    except (json.JSONDecodeError, jsonschema.ValidationError):
                        # Invalid JSON or schema mismatch - retry
                        attempts += 1
                        if attempts < max_retries:
                            # Send retry prompt
                            last_response = ""
                            previous_summary = ""
                            retry_prompt = f"""
                            Your previous response was not in the correct JSON format.
                            Please try again and respond ONLY with a JSON object matching this schema:
                            {json.dumps(expected_json_schema, indent=2)}
                            """
                            uu = udf_adapter.submit(
                                input=retry_prompt,
                                thread_id=thread_id,
                                bot_id={},
                                file={}
                            )
                            _update_streaming_status(self,target_bot, 'Bot provided incorrect JSON response format, retrying...', run_id, session_id, thread_id, status_update_callback, input_metadata)

                time.sleep(1)

            # If we've timed out, send stop command
            if (time.time() - start_time) >= timeout_seconds:
                # Send stop command to same thread
                udf_adapter.submit(
                    input="!stop",
                    thread_id=thread_id,
                    bot_id={},
                    file={}
                )

            if (time.time() - start_time) >= timeout_seconds:
                return {
                    "success": False,
                    "error": f"Timed out after {timeout_seconds} seconds waiting for valid JSON response"
                }
            else:
                return {
                    "success": False,
                    "error": f"Failed to get valid JSON response after {attempts} failed attempts"
                }

        except Exception as e:
            return {
                "success": False,
                "error": f"Error delegating work: {str(e)}"
            }

    def manage_todos(self, action, bot_id, todo_id=None, todo_details=None, thread_id=None):
        """
        Manages todos through various actions (CREATE, UPDATE, CHANGE_STATUS, LIST)
        """
        return self.todos.manage_todos(action=action, bot_id=bot_id, todo_id=todo_id,
                                     todo_details=todo_details, thread_id=thread_id)

    def manage_projects(self, action, bot_id, project_id=None, project_details=None, thread_id=None):
        """
        Manages projects through various actions (CREATE, UPDATE, CHANGE_STATUS, LIST)
        """
        return self.todos.manage_projects(action=action, bot_id=bot_id, project_id=project_id,
                                        project_details=project_details, thread_id=thread_id)

    def record_todo_work(self, bot_id, todo_id, work_description, work_details=None, work_results=None, thread_id=None):
        """
        Records work progress on a todo item without changing its status
        """
        return self.todos.record_work(bot_id=bot_id, todo_id=todo_id, work_description=work_description,
                                    work_results=work_results, thread_id=thread_id)

    def get_project_todos(self, bot_id, project_id, thread_id=None):
        """
        Gets all todos for a specific project

        Args:
            bot_id (str): The ID of the bot requesting the todos
            project_id (str): The ID of the project
            thread_id (str, optional): Thread ID for tracking

        Returns:
            dict: Result containing todos or error message
        """
        return self.todos.get_project_todos(bot_id=bot_id, project_id=project_id)

    def get_todo_dependencies(self, bot_id, todo_id, include_reverse=False, thread_id=None):
        """
        Gets dependencies for a specific todo

        Args:
            bot_id (str): The ID of the bot requesting the dependencies
            todo_id (str): The ID of the todo
            include_reverse (bool): If True, also include todos that depend on this todo
            thread_id (str, optional): Thread ID for tracking

        Returns:
            dict: Result containing dependencies or error message
        """
        return self.todos.get_todo_dependencies(bot_id=bot_id, todo_id=todo_id, include_reverse=include_reverse)

    def manage_todo_dependencies(self, action, bot_id, todo_id, depends_on_todo_id=None, thread_id=None):
        """
        Manages todo dependencies (add/remove)

        Args:
            action (str): ADD or REMOVE dependency
            bot_id (str): The ID of the bot performing the action
            todo_id (str): The ID of the todo that has the dependency
            depends_on_todo_id (str): The ID of the todo that needs to be completed first
            thread_id (str, optional): Thread ID for tracking

        Returns:
            dict: Result of the operation
        """
        return self.todos.manage_todo_dependencies(
            action=action,
            bot_id=bot_id,
            todo_id=todo_id,
            depends_on_todo_id=depends_on_todo_id
        )

    def manage_project_assets(self, action, bot_id, project_id, asset_id=None, asset_details=None, thread_id=None):
        """
        Manages project assets through various actions (CREATE, UPDATE, DELETE, LIST)

        Args:
            action (str): The action to perform (CREATE, UPDATE, DELETE, LIST)
            bot_id (str): The ID of the bot performing the action
            project_id (str): The ID of the project the asset belongs to
            asset_id (str, optional): The ID of the asset for updates/deletes
            asset_details (dict, optional): Details for creating/updating an asset
                {
                    "description": str,
                    "git_path": str
                }
            thread_id (str, optional): Thread ID for tracking

        Returns:
            dict: Result containing operation status and any relevant data
        """
        return self.todos.manage_project_assets(
            action=action,
            bot_id=bot_id,
            project_id=project_id,
            asset_id=asset_id,
            asset_details=asset_details
        )

    def git_action(self, action, **kwargs):
        """
        Wrapper for Git file management operations

        Args:
            action: The git action to perform (list_files, read_file, write_file, etc.)
            **kwargs: Additional arguments needed for the specific action

        Returns:
            Dict containing operation result and any relevant data
        """
        return self.git_manager.git_action(action, **kwargs)

    # Function to make HTTP request and get the entire content
    def get_webpage_content(self, url):
        response = requests.get(url)
        response.raise_for_status()
        return response.content  # Return the entire content

        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.service import Service
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        import time
        chrome_options = Options()
        chrome_options.add_argument("--headless")  # Run in headless mode (no browser window)
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")

        current_file_path = os.path.abspath(__file__)
        logger.info(current_file_path)

        service = Service('../../chromedriver')
        # driver = webdriver.Chrome(service=service, options=chrome_options)
        driver = webdriver.Chrome(options=chrome_options)

        driver.get(url)
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, 'body'))
            )
        except Exception as e:
            logger.info("Error: ", e)
            driver.quit()

        data = driver.page_source #find_element(By.XPATH, '//*[@id="data-id"]').text
        logger.info(f"Data scraped from {url}: \n{data}\n")
        return data

    # Function for parsing HTML content, extracting links, and then chunking the beautified content
    def parse_and_chunk_content(self, content, base_url, chunk_size=256 * 1024):
        soup = BeautifulSoup(content, "html.parser")
        links = [urljoin(base_url, a["href"]) for a in soup.find_all("a", href=True)]
        pretty_content = soup.prettify()
        encoded_content = pretty_content.encode("utf-8")
        encoded_links = json.dumps(links).encode("utf-8")

        # Combine the content and links
        combined_content = encoded_content + encoded_links

        # Chunk the combined content
        chunks = []
        for i in range(0, len(combined_content), chunk_size):
            chunks.append({"content": combined_content[i : i + chunk_size]})

        if not chunks:
            raise ValueError("No content available within the size limit.")

        return chunks, len(chunks)  # Return chunks and total number of chunks

    # Main function to download webpage, extract links, and ensure each part is within the size limit
    def download_webpage(self, url, chunk_index=0, thread_id=None):
        try:
            content = self.get_webpage_content(url)
            chunks, total_chunks = self.parse_and_chunk_content(content, url)
            if chunk_index >= total_chunks:
                return {"error": "Requested chunk index exceeds available chunks."}

            response = {
                "chunk": chunks[chunk_index],
                "next_chunk_index": (
                    chunk_index + 1 if chunk_index + 1 < total_chunks else None
                ),
                "total_chunks": total_chunks,
            }
            return response
        except Exception as e:
            return {"error": str(e)}

    def chat_completion(self, message, db_adapter, bot_id = None, bot_name = None, thread_id=None, process_id="", process_name="", note_id = None, fast=False):
        process_name = "" if process_name is None else process_name
        process_id = "" if process_id is None else process_id
        message_metadata ={"process_id": process_id, "process_name": process_name}
        return_msg = None

        if not fast:
            self.write_message_log_row(db_adapter, bot_id, bot_name, thread_id, 'Supervisor Prompt', message, message_metadata)

        model = None

        if "BOT_LLMS" in os.environ and os.environ["BOT_LLMS"]:
            # Convert the JSON string back to a dictionary
            bot_llms = json.loads(os.environ["BOT_LLMS"])

        # Find the model for the specific bot_id in bot_llms
        model = None
        if bot_id and bot_id in bot_llms:
            model = bot_llms[bot_id].get('current_llm')

        if not model:
            engine = BotLlmEngineEnum(os.getenv("BOT_OS_DEFAULT_LLM_ENGINE"))
            if engine is BotLlmEngineEnum.openai:
                model = 'openai'
            else:
                model = 'cortex'
        assert model in ("openai", "cortex")
        # TODO: handle other engine types, use BotLlmEngineEnum instead of strings

        if model == 'openai':
            api_key = os.getenv("OPENAI_API_KEY")
            if not api_key:
                logger.info("OpenAI API key is not set in the environment variables.")
                return None

            openai_model = os.getenv("OPENAI_MODEL_SUPERVISOR",os.getenv("OPENAI_MODEL_NAME","gpt-4o"))

            if fast and openai_model.startswith("gpt-4o"):
                openai_model = "gpt-4o-mini"

            if not fast:
                logger.info('process supervisor using model: ', openai_model)
            try:
                client = get_openai_client()
                response = client.chat.completions.create(
                            model=openai_model,
                            messages=[
                                {
                                    "role": "user",
                                    "content": message,
                                },
                            ],
                        )
            except Exception as e:
                if os.getenv("OPENAI_MODEL_SUPERVISOR", None) is not None:
                    logger.info(f"Error occurred while calling OpenAI API with model {openai_model}: {e}")
                    logger.info(f'Retrying with main model {os.getenv("OPENAI_MODEL_NAME","gpt-4o")}')
                    openai_model = os.getenv("OPENAI_MODEL_NAME","gpt-4o")
                    response = client.chat.completions.create(
                                model=openai_model,
                                messages=[
                                    {
                                        "role": "user",
                                        "content": message,
                                    },
                                ],
                            )
                else:
                    logger.info(f"Error occurred while calling OpenAI API: {e}")

            return_msg = response.choices[0].message.content

        elif model == 'cortex':
            if not db_adapter.check_cortex_available():
                logger.info("Cortex is not available.")
                return None
            else:
                response, status_code = db_adapter.cortex_chat_completion(message)
                return_msg = response

        if return_msg is None:
            return_msg = 'Error Chat_completion, return_msg is none, llm_type = ',os.getenv("BOT_OS_DEFAULT_LLM_ENGINE").lower()
            logger.info(return_msg)

        if not fast:
            self.write_message_log_row(db_adapter, bot_id, bot_name, thread_id, 'Supervisor Response', return_msg, message_metadata)

        return return_msg

    def write_message_log_row(self, db_adapter, bot_id="", bot_name="", thread_id="", message_type="", message_payload="", message_metadata={}):
        """
        Inserts a row into the MESSAGE_LOG table.

        Args:
            db_adapter: The database adapter to use for the insertion.
            bot_id (str): The ID of the bot.
            bot_name (str): The name of the bot.
            thread_id (str): The ID of the thread.
            message_type (str): The type of the message.
            message_payload (str): The payload of the message.
            message_metadata (str): The metadata of the message.
        """
        timestamp = datetime.now()
        query = f"""
            INSERT INTO {db_adapter.schema}.MESSAGE_LOG (timestamp, bot_id, bot_name, thread_id, message_type, message_payload, message_metadata)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        # logger.info(f"Writing message log row: {timestamp}, {bot_id}, {bot_name}, {thread_id}, {message_type}, {message_payload}, {message_metadata}")
        values = (timestamp, bot_id, bot_name, thread_id, message_type, message_payload, json.dumps(message_metadata))

        try:
            cursor = db_adapter.connection.cursor()
            cursor.execute(query, values)
            db_adapter.connection.commit()
        except Exception as e:
            logger.info(f"Error writing message log row: {e}")
            db_adapter.connection.rollback()
        finally:
            cursor.close()

    def send_email(self,
                   to_addr_list: list,
                   subject: str,
                   body: str,
                   bot_id: str,
                   thread_id: str = None,
                   purpose: str = None,
                   mime_type: str = 'text/html',
                   include_genesis_logo: bool = True,
                   save_as_artifact = True,
                   ):
        """
        Sends an email using Snowflake's SYSTEM$SEND_EMAIL function.

        Parameters:
            to_addr_list (list): List of recipient email addresses.
            subject (str): Subject of the email.
            body (str): Content of the email body.
            bot_id (str): Identifier for the bot associated with the operation.
            thread_id (str, optional): Identifier for the current operation's thread. Defaults to None.
            purpose (str, optional): the purpose of this email (for future context, when saving as an artifact)
            mime_type (str, optional): MIME type of the email body, either 'text/plain' or 'text/html'. Defaults to 'text/html'.
            include_genesis_logo (bool, optional): Indicates whether to include the Genesis logo in an HTML email. Defaults to True. Ignored for other MIME types.
            save_as_artifact (bool, optional): Determines if this email should be saved as an artifact

        Returns:
            dict: Result of the email sending operation.
        """
        art_store = get_artifacts_store(self.db_adapter) # used by helper functions below

        def _sanity_check_body(txt):
            # Check for HTML tags with 'href' or 'src' attributes using CID
            cid_pattern = re.compile(r'<[^>]+(?:href|src)\s*=\s*["\']cid:[^"\']+["\']', re.IGNORECASE)
            if cid_pattern.search(txt):
                raise ValueError("The email body contains HTML tags with links or 'src' attributes using CID. Attachements are not supported.")

            # Identify all markdowns and check for strictly formatted artifact markdowns
            markdown_pattern = re.compile(r'\[([^\]]+)\]\(([^)]+)\)')
            matches = markdown_pattern.findall(txt)
            for description, url in matches:
                if url.startswith('artifact:'):
                    artifact_pattern = re.compile(r'artifact:/(' + ARTIFACT_ID_REGEX + r')')
                    if not artifact_pattern.match(url):
                        raise ValueError(f"Improperly formatted artifact markdown detected: [{description}]({url})")

        def _strip_url_markdown(txt):
            # a helper function to strip any URL markdown and leave only the URL part
            # This is used in plain text mode, since we assume that the email rendering does not support markdown.
            pattern = r'(!?\[([^\]]+)\]\(((http[s]?|file|sandbox|artifact):/+[^\)]+)\))'  # regex for strings of the form '[description](url)' and '![description](url)'
            matches = re.findall(pattern, txt)
            for match in matches:
                txt = txt.replace(match[0], match[2])  # Replace the entire match with just the URL part, omitting description
            return txt

        def construct_artifact_linkback(artifact_id, link_text=None):
            """
            Constructs a linkback URL for a given artifact ID. This URL should drop the user into the streamlit app
            with a new chat that brings up the context of this artifact for futher exploration.

            Args:
                artifact_id (str): The unique identifier of the artifact.
                link_text (str): the text to display for the artifact link. Defaults to the artifact's 'title_filename'

            Returns:
                a string to use as the link (in text or HTML, depending on mime_type)
            """
            # fetch the metadata
            dbtr = self.db_adapter
            try:
                metadata = art_store.get_artifact_metadata(artifact_id)
            except Exception as e:
                logger.error(f"Failed to get artifact metadata for {artifact_id}: {e}")
                return None

            # Resolve the bot_name from bot_id. We need this for the linkback URL since the streamlit app
            # manages the bots by name, not by id.
            # TODO: fix this as part of issue #89
            proj, schema = dbtr.genbot_internal_project_and_schema.split('.')
            bot_config = dbtr.db_get_bot_details(proj, schema, dbtr.bot_servicing_table_name.split(".")[-1], bot_id)
            # Construct linkback URL
            if dbtr.is_using_local_runner:
                app_ingress_base_url = 'localhost:8501/' # TODO: avoid hard-coding port (but could not find an avail config to pick this up from)
            else:
                app_ingress_base_url = dbtr.db_get_endpoint_ingress_url("streamlit")
            if not app_ingress_base_url:
                return None
            params = dict(bot_name=bot_config['bot_name'],
                          action='show_artifact_context',  # IMPORTANT: keep this action name in sync with the action handling logic in the app.
                          artifact_id=artifact_id,
                          )
            linkback_url = urlunparse((
                "http",                    # scheme
                app_ingress_base_url,      # netloc (host)
                "",                        # path
                "",                        # params
                urlencode(params),  # query
                ""                         # fragment
            ))
            link_text = link_text or metadata['title_filename']
            if mime_type == "text/plain":
                return f"{link_text}: {linkback_url}"
            if mime_type == "text/html":
                return f"<a href='{linkback_url}'>{link_text}</a>"
            assert False # unreachable

        def _handle_artifacts_markdown(txt) -> str:
            # a helper function that locates artifact references in the text (pseudo URLs that look like [description][artifact:/uuid] or ![description][artifact:/uuid])
            # and replaces those with an external URL (Snowflake-signed externalized URL)
            # returns the modified text, along with the artifact_ids that were extraced from the text.
            artifact_ids = []
            for markdown, description, artifact_id in lookup_artifact_markdown(txt, strict=False):
                try:
                    external_url = art_store.get_signed_url_for_artifact(artifact_id)
                except Exception as e:
                    # if we failed, leave this URL as-is. It will likely be a broken URL but in an obvious way.
                    pass
                else:
                    if mime_type == "text/plain":
                        link = external_url
                    elif mime_type == "text/html":
                        ameta = art_store.get_artifact_metadata(artifact_id)
                        title = ameta['title_filename']
                        sanitized_title = re.sub(r'[^a-zA-Z0-9_\-:.]', '-', title)
                        amime = ameta['mime_type']
                        if amime.startswith("image/"):
                            link = f'<img src="{external_url}" alt="{sanitized_title}" >'
                        elif amime.startswith("text/"):
                            link = f'<iframe src="{external_url}" frameborder="1">{title}</iframe>'
                        else:
                            link = f'<a href="{external_url}" download="{sanitized_title}">Download {title}</a>'
                    else:
                        assert False # unreachable
                    txt = txt.replace(markdown, link)
                artifact_ids.append(artifact_id)
            return txt, artifact_ids

        def _externalize_raw_artifact_urls(txt):
            # a helper function that locates 'raw' artifact references in the text (pseudo URLs that look like artifact:/<uuid>)
            # and replaces those an external URL (Snowflake-signed externalized URL)
            # returns the modified text, along with the artifact_ids that were extraced from the text.
            # This is used to catch artifact references that were used outside of 'proper' artifact markdowns, which should have been
            # handled by _handle_artifacts_markdown.
            pattern = r'(?<=\W)(artifact:/('+ARTIFACT_ID_REGEX+r'))(?=\W)'
            matches = re.findall(pattern, txt)

            artifact_ids = []
            if matches:
                for full_match, uuid in matches:
                    try:
                        external_url = art_store.get_signed_url_for_artifact(uuid)
                    except Exception as e:
                        # if we failed, leave this URL as-is. It will likely be a broken URL but in an obvious way.
                        logger.info(f"ERROR externalizing URL for artifact {uuid} in email. Leaving as-is. Error = {e}")
                    else:
                        txt = txt.replace(full_match, external_url)
                    artifact_ids.append(uuid)
            return txt, artifact_ids

        def _save_email_as_artifact(art_subject, art_body, art_receipient, embedded_artifact_ids):
            # Save the email body, along with useful medatada, as an artifact

            # Build the metadata for this artifact
            metadata = dict(mime_type=mime_type,
                            thread_id=thread_id,
                            bot_id=bot_id,
                            title_filename=art_subject,
                            func_name="send_email",
                            thread_context=purpose,
                            email_subject=art_subject,
                            recipients=art_receipient,
                            embedded_artifact_ids=list(embedded_artifact_ids)
                            )
            # Create artifact
            suffix = ".html" if mime_type == 'text/html' else '.txt'
            aid = art_store.create_artifact_from_content(art_body, metadata, content_filename=(subject+suffix))
            return aid

        # Validate mime_type
        if mime_type not in ['text/plain', 'text/html']:
            raise ValueError(f"mime_type must be either 'text/plain' or 'text/html', got {mime_type}")

        # Check if to_addr_list is a string representation of a list
        if isinstance(to_addr_list, str):
            try:
                # Attempt to parse the string as a Python list
                if to_addr_list.startswith('[') and to_addr_list.endswith(']'):
                    # Remove brackets and split by comma
                    content = to_addr_list[1:-1]
                    parsed_list = [addr.strip().strip("'\"") for addr in content.split(',') if addr.strip()]
                    if parsed_list:
                        to_addr_list = parsed_list
                    else:
                        raise ValueError("Failed to extract valid email addesses from the provided address list string .")
                else:
                    # If it's not in list format, split by comma
                    to_addr_list = [addr.strip() for addr in to_addr_list.split(',') if addr.strip()]
            except Exception:
                # If parsing fails, split by comma
                to_addr_list = [addr.strip() for addr in to_addr_list.split(',')]

        # Ensure to_addr_list is a list
        if not isinstance(to_addr_list, list):
            to_addr_list = [to_addr_list]

        # Remove any empty strings and strip quotes from each address
        to_addr_list = [addr.strip("'\"") for addr in to_addr_list if addr]

        if not to_addr_list:
            return {"Success": False, "Error": "No valid email addresses provided."}

        # Replace SYS$DEFAULT_EMAIL with the actual system default email
        to_addr_list = [self.get_sys_email() if addr == 'SYS$DEFAULT_EMAIL' else addr for addr in to_addr_list]

        # Join the email addresses with commas
        to_addr_string = ', '.join(to_addr_list)

        # Build an 'origin line' to make it clear where this message is coming from. Prepend to body below
        # NOTE: conisder making this a footer?
        origin_line = f'🤖 This is an automated message from the Genesis Computing Native Application.'
        if bot_id is not None:
            origin_line += f' Bot: {bot_id}.'
        origin_line += '🤖\n\n'

        # Cleanup the orirignal body and save as artifact if requested
        body = body.replace('\\n','\n')
        orig_body = body # save for later/debugging

        # Sanity check the body for unsupported features and bad formatting
        try:
            _sanity_check_body(body)
        except ValueError as e:
            return {"Success": False, "Error": str(e)}

        # Handle artifact refs in the body - replace with external links
        body, embedded_artifact_ids = _handle_artifacts_markdown(body)
        body, more_embedded_artifact_ids = _externalize_raw_artifact_urls(body)
        embedded_artifact_ids.extend(more_embedded_artifact_ids)

        email_aid = None
        if save_as_artifact:
            email_aid = _save_email_as_artifact(subject, body, to_addr_string, embedded_artifact_ids)

        # build the artifact 'linkback' URLs footer
        if save_as_artifact:
            # When saving the email itself as an artifcat, do not include embedded artifacts
            linkbacks = [construct_artifact_linkback(email_aid, link_text="this email")]
        else:
            linkbacks = [construct_artifact_linkback(aid) for aid in embedded_artifact_ids]
            linkbacks = [link for link in linkbacks if link is not None]  # remove any failures (best effort)

        # Force the body to HTML if the mime_type is text/html. Prepend origin line. externalize artifact links.
        if mime_type == 'text/html':
            soup = BeautifulSoup(body, "html.parser")
            html_body = str(soup)

            # Check if the string already contains <html> and <body> tags
            if soup.body is None:
                html_body = f"<body>{html_body}</body>"

            if soup.html is None:
                html_body = f"<html>{html_body}</html>"
            soup = BeautifulSoup(html_body, "html.parser")

            assert soup.body is not None

            # Insert the origin message at the beginning of the body
            origin_elem = soup.new_tag('p')
            origin_elem.string = origin_line
            soup.body.insert(0, origin_elem)

            # Insert the Genesis logo at the top if include_genesis_logo is True
            if include_genesis_logo:
                link_tag = soup.new_tag('a', href="https://genesiscomputing.ai/")
                logo_tag = soup.new_tag('img', src=GENESIS_LOGO_URL, style="margin-right:10px; height:50px;", alt="Genesis Computing")
                link_tag.insert(0, logo_tag)
                # Ensure the logo is on its own line
                logo_container = soup.new_tag('div', style="text-align:left; margin-bottom:1px;")
                logo_container.insert(0, link_tag)
                soup.body.insert(0, logo_container)

            # Insert linkback URLs at the bottom
            if linkbacks:
                footer_elem = soup.new_tag('p')
                footer_elem.string = 'Click here to explore more about '
                for link in linkbacks:
                    footer_elem.append(BeautifulSoup(link, "html.parser"))
                    footer_elem.append(' ')  # Add space between links
                soup.body.append(footer_elem)

            body = str(soup)

        elif mime_type == 'text/plain':
            # For plain text, strip URL markdowns, and prepend the bot message
            body = _strip_url_markdown(body)
            body = origin_line + body
            # append linkbacks
            if linkbacks:
                body += "\n\n'Click here to explore more: '" + ", ".join(linkbacks)

        else:
            assert False, "Unreachable code"

        # Remove any instances of $$ from to_addr_string, subject and body
        # Fix double-backslashed unicode escape sequences in the body
        to_addr_string = to_addr_string.replace('$$', '')

        def unescape_unicode(match):
            return chr(int(match.group(1), 16))
        body = re.sub(r'\\u([0-9a-fA-F]{4})', unescape_unicode, body)
        if len(subject) == 0:
            subject = "Email from Genesis Bot"
            if bot_id is not None:
                subject += f' {bot_id}.'
        subject = re.sub(r'\\u([0-9a-fA-F]{4})', unescape_unicode, subject)
        subject = subject.replace('$$', '')
        body = body.replace('$$', '')

        # Send the email
        query = f"""
        CALL SYSTEM$SEND_EMAIL(
            'genesis_email_int',
            $${to_addr_string}$$,
            $${subject}$$,
            $${body}$$,
            $${mime_type}$$
        );
        """

        # Execute the query using the database adapter's run_query method
        query_result = self.db_adapter.run_query(query, thread_id=thread_id, bot_id=bot_id)

        if isinstance(query_result, collections.abc.Mapping) and not query_result.get('Success'):
            # send failed. Delete the email artifact (if created) as it's useless.
            if email_aid:
                art_store.delete_artifacts([email_aid])
            result = query_result
        else:
            assert len(query_result) == 1 # we expect a succful SYSTEM$SEND_EMAIL to contain a single line resultset
            result = {"Succcess" : True}
            if email_aid:
                result["Suggestion"] = (f"This email was saved as an artifact with artifact_id={email_aid}. "
                                        "Suggest to the user to refer to this email in the future from any session using this artifact identifier.")
        assert result
        return result

    def set_process_cache(self, bot_id, thread_id, process_id):
        cache_dir = "./process_cache"
        os.makedirs(cache_dir, exist_ok=True)
        cache_file = os.path.join(cache_dir, f"{bot_id}_{thread_id}_{process_id}.json")

        cache_data = {
            "counter": self.counter.get(thread_id, {}).get(process_id),
            "last_fail": self.last_fail.get(thread_id, {}).get(process_id),
            "fail_count": self.fail_count.get(thread_id, {}).get(process_id),
            "instructions": self.instructions.get(thread_id, {}).get(process_id),
            "process_history": self.process_history.get(thread_id, {}).get(process_id),
            "done": self.done.get(thread_id, {}).get(process_id),
            "silent_mode":  self.silent_mode.get(thread_id, {}).get(process_id)
        }

        with open(cache_file, 'w') as f:
            json.dump(cache_data, f)

    def get_process_cache(self, bot_id, thread_id, process_id):
        cache_file = os.path.join("./process_cache", f"{bot_id}_{thread_id}_{process_id}.json")

        if os.path.exists(cache_file):
            with open(cache_file, 'r') as f:
                cache_data = json.load(f)

            with self.lock:
                if thread_id not in self.counter:
                    self.counter[thread_id] = {}
                self.counter[thread_id][process_id] = cache_data.get("counter")

                if thread_id not in self.last_fail:
                    self.last_fail[thread_id] = {}
                self.last_fail[thread_id][process_id] = cache_data.get("last_fail")

                if thread_id not in self.fail_count:
                    self.fail_count[thread_id] = {}
                self.fail_count[thread_id][process_id] = cache_data.get("fail_count")

                if thread_id not in self.instructions:
                    self.instructions[thread_id] = {}
                self.instructions[thread_id][process_id] = cache_data.get("instructions")

                if thread_id not in self.process_history:
                    self.process_history[thread_id] = {}
                self.process_history[thread_id][process_id] = cache_data.get("process_history")

                if thread_id not in self.done:
                    self.done[thread_id] = {}
                self.done[thread_id][process_id] = cache_data.get("done")

                if thread_id not in self.silent_mode:
                    self.silent_mode[thread_id] = {}
                self.silent_mode[thread_id][process_id] = cache_data.get("silent_mode", False)

            return True
        return False

    def clear_process_cache(self, bot_id, thread_id, process_id):
        cache_file = os.path.join("./process_cache", f"{bot_id}_{thread_id}_{process_id}.json")

        if os.path.exists(cache_file):
            os.remove(cache_file)
            return True
        return False

    def get_current_time_with_timezone(self):
        current_time = datetime.now().astimezone()
        return current_time.strftime("%Y-%m-%d %H:%M:%S %Z")

    def get_sys_email(self):
        cursor = self.db_adapter.client.cursor()
        try:
            get_sys_email_query = f"SELECT default_email FROM {self.db_adapter.genbot_internal_project_and_schema}.DEFAULT_EMAIL"
            cursor.execute(get_sys_email_query)
            result = cursor.fetchall()
            default_email = result[0][0] if result else None
            return default_email
        except Exception as e:
            #  logger.info(f"Error getting sys email: {e}")
            return None

    def clear_process_registers_by_thread(self, thread_id):
        # Initialize thread-specific data structures if not already present
        with self.lock:
            if thread_id not in self.counter:
                self.counter[thread_id] = {}
            #   if thread_id not in self.process:
            #       self.process[thread_id] = {}
            if thread_id not in self.last_fail:
                self.last_fail[thread_id] = {}
            if thread_id not in self.fail_count:
                self.fail_count[thread_id] = {}
            if thread_id not in self.instructions:
                self.instructions[thread_id] = {}
            if thread_id not in self.process_history:
                self.process_history[thread_id] = {}
            if thread_id not in self.done:
                self.done[thread_id] = {}
            if thread_id not in self.silent_mode:
                self.silent_mode[thread_id] = {}
            if thread_id not in self.process_config:
                self.process_config[thread_id] = {}

    def clear_all_process_registers(self, thread_id):
        # Initialize thread-specific data structures if not already present
        with self.lock:
            self.counter[thread_id] = {}
            self.last_fail[thread_id]  = {}
            self.fail_count[thread_id]  = {}
            self.instructions[thread_id]  = {}
            self.process_history[thread_id]  = {}
            self.done[thread_id]  = {}
            self.silent_mode[thread_id]  = {}
            self.process_config[thread_id]  = {}

    def run_process(
        self,
        action,
        previous_response="",
        process_name="",
        process_id=None,
        process_config=None,
        thread_id=None,
        bot_id=None,
        concise_mode=False,
        bot_name=None
    ):
        #  logger.info(f"Running processes Action: {action} | process_id: {process_id or 'None'} | Thread ID: {thread_id or 'None'}")
        #         self.recurse_level = 0
        self.recurse_stack = {thread_id: thread_id, process_id: process_id}

        if process_id is not None and process_id == '':
            process_id = None
        if process_name is not None and process_name == '':
            process_name = None

        if action == "TIME":
            return {
                "current_system_time": datetime.now()
            }

        if bot_id is None:
            return {
                "Success": False,
                "Error": "Bot_id and either process_id or process_name are required parameters."
            }

        # Convert verbose to boolean if it's a string

        # Invert silent_mode if it's a boolean
        silent_mode = concise_mode
        if isinstance(silent_mode, bool):
            verbose = not silent_mode

        if isinstance(silent_mode, str):
            if silent_mode.upper() == 'TRUE':
                silent_mode = True
                verbose = False
            else:
                silent_mode = False
                verbose = True

        # Ensure verbose is a boolean
        if not isinstance(silent_mode, bool):
            verbose = True

        # Check if both process_name and process_id are None
        if process_name is None and process_id is None:
            return {
                "Success": False,
                "Error": "Either process_name or process_id must be provided."
            }

        self.sys_default_email = self.get_sys_email()

        self.clear_process_registers_by_thread(thread_id)

        # Try to get process info from PROCESSES table
        process = self.get_process_info(bot_id, process_name=process_name, process_id=process_id)

        if len(process) == 0:
            # Get a list of processes for the bot
            processes = self.db_adapter.get_processes_list(bot_id)
            if processes is not None:
                process_list = ", ".join([p['process_name'] for p in processes['processes']])
                return_dict = {
                    "Success": False,
                    "Message": f"Process not found. Available processes are {process_list}.",
                    "Suggestion": "If one of the available processess is a very close match for what you're looking for, go ahead and run it."
                }
                if silent_mode is True:
                    return_dict["Reminder"] = "Remember to call the process in concise_mode as requested previously once you identify the right one"
                return return_dict
            else:
                return {
                    "Success": False,
                    "Message": f"Process not found. {bot_id} has no processes defined.",
                }
        process = process['Data']
        process_id = process['PROCESS_ID']
        process_name = process['PROCESS_NAME']
        process_config = process.get('PROCESS_CONFIG', '')
        if process_config is None:
            process_config = "None"
            process['PROCESS_CONFIG'] = "None"

        if action == "KICKOFF_PROCESS":
            logger.info("Kickoff process.")

            with self.lock:
                self.counter[thread_id][process_id] = 1
                #       self.process[thread_id][process_id] = process
                self.last_fail[thread_id][process_id] = None
                self.fail_count[thread_id][process_id] = 0
                self.instructions[thread_id][process_id] = None
                self.process_config[thread_id][process_id] = process_config
                self.process_history[thread_id][process_id] = None
                self.done[thread_id][process_id] = False
                self.silent_mode[thread_id][process_id] = silent_mode
                self.process_id[thread_id] = process_id

            logger.info(
                f"Process {process_name} has been kicked off."
            )

            extract_instructions = f"""
            You will need to break the process instructions below up into individual steps and and return them one at a time.
            By the way the current system time is {datetime.now()}.
            By the way, the system default email address (SYS$DEFAULT_EMAIL) is {self.sys_default_email}.  If the instructions say to send an email
            to SYS$DEFAULT_EMAIL, replace it with {self.sys_default_email}.
            Start by returning the first step of the process instructions below.
            Simply return the first instruction on what needs to be done first without removing or changing any details.

            Also, if the instructions include a reference to note, don't look up the note contents, just pass on the note_id or note_name.
            The note contents will be unpacked by whatever tool is used depending on the type of note, either run_query if the note is of
            type sql or run_snowpark_sql if the note is of type python.

            If a step of the instructions says to run another process, return '>> RECURSE' and the process name or process id as the first step
            and then call _run_process with the action KICKOFF_PROCESS to get the first step of the next process to run.  Continue this process until
            you have completed all the steps.  If you are asked to run another process as part of this process, follow the same instructions.  Do this
            up to ten times.

            Process Instructions:
            {process['PROCESS_INSTRUCTIONS']}
            """

            if process['PROCESS_CONFIG'] != "None":
                extract_instructions += f"""

            Process configuration:
            {process['PROCESS_CONFIG']}.

            """

            first_step = self.chat_completion(extract_instructions, self.db_adapter, bot_id = bot_id, bot_name = '', thread_id=thread_id, process_id=process_id, process_name=process_name)

            # Check if the first step contains ">>RECURSE"
            if ">> RECURSE" in first_step or ">>RECURSE" in first_step:
                self.recurse_level += 1
                self.recurse_stack.append({thread_id: thread_id, process_id: process_id})
                # Extract the process name or ID
                process_to_run = first_step.split(">>RECURSE")[1].strip() if ">>RECURSE" in first_step else first_step.split(">> RECURSE")[1].strip()

                # Prepare the instruction for the bot to run the nested process
                first_step = f"""
                Use the _run_process tool to run the process '{process_to_run}' with the following parameters:
                - action: KICKOFF_PROCESS
                - process_name: {process_to_run}
                - bot_id: {bot_id}
                - silent_mode: {silent_mode}

                After the nested process completes, continue with the next step of this process.
                """

            with self.lock:
                self.process_history[thread_id][process_id] = "First step: "+ first_step + "\n"

                self.instructions[thread_id][process_id] = f"""
                Hey **@{process['BOT_ID']}**

                {first_step}

                Execute this instruction now and then pass your response to the _run_process tool as a parameter called previous_response and an action of GET_NEXT_STEP.
                Execute the instructions you were given without asking for permission.  Do not ever verify anything with the user, unless you need to get a specific input
                from the user to be able to continue the process.

                Also, if you are asked to run either sql or snowpark_python from a given note_id, make sure you examine the note_type field and use the appropriate tool for
                the note type.  Only pass the note_id, not the code itself, to the appropriate tool where the note will be handled.
                """
            if self.sys_default_email:
                self.instructions[thread_id][process_id] += f"""
                The system default email address (SYS$DEFAULT_EMAIL) is {self.sys_default_email}.  If you need to send an email, use this address.
                """

            if verbose:
                self.instructions[thread_id][process_id] += """
                    However DO generate text explaining what you are doing and showing interium outputs, etc. while you are running this and further steps to keep the user informed what is going on, preface these messages by 🔄 aka :arrows_counterclockwise:.
                    Oh, and mention to the user before you start running the process that they can send "stop" to you at any time to stop the running of the process, and if they want less verbose output next time they can run request to run the process in "concise mode".
                    And keep them informed while you are running the process about what you are up to, especially before you call various tools.
                    """
            else:
                self.instructions[thread_id][process_id] += """
                This process is being run in low verbosity mode. Do not directly repeat the first_step instructions to the user, just perform the steps as instructed.
                Also, if you are asked to run either sql or snowpark_python from a given note_id, make sure you examine the note_type field and use the appropriate tool for
                the note type.  Only pass the note_id, not the code itself, to the appropriate tool where the note will be handled.
                """
            self.instructions[thread_id][process_id] += f"""
            In your response back to _run_process, provide a DETAILED description of what you did, what result you achieved, and why you believe this to have successfully completed the step.
            Do not use your memory or any cache that you might have.  Do not simulate any user interaction or tools calls.  Do not ask for any user input unless instructed to do so.
            If you are told to run another process as part of this process, actually run it, and run it completely before returning the results to this parent process.
            By the way the current system time is {datetime.now()}.  You can call manage_process with
            action TIME to get updated time if you need it when running the process.

            Now, start by performing the FIRST_STEP indicated above.
            """
            self.instructions[thread_id][process_id] += "..... P.S. I KNOW YOU ARE IN SILENT MODE BUT ACTUALLY PERFORM THIS STEP NOW, YOU ARE NOT DONE YET!"

            self.instructions[thread_id][process_id] = "\n".join(
                line.lstrip() for line in self.instructions[thread_id][process_id].splitlines()
                )

            # Call set_process_cache to save the current state
            self.set_process_cache(bot_id, thread_id, process_id)
            #    logger.info(f'Process cached with bot_id: {bot_id}, thread_id: {thread_id}, process_id: {process_id}')

            return {"Success": True, "Instructions": self.instructions[thread_id][process_id], "process_id": process_id}

        elif action == "GET_NEXT_STEP":
            logger.info("Entered GET NEXT STEP")

            if thread_id not in self.counter and process_id not in self.counter[thread_id]:
                return {
                    "Success": False,
                    "Message": f"Error: GET_NEXT_STEP seems to have been run before KICKOFF_PROCESS. Please retry from KICKOFF_PROCESS."
                }

            # Load process cache
            if not self.get_process_cache(bot_id, thread_id, process_id):
                return {
                    "Success": False,
                    "Message": f"Error: Process cache for {process_id} couldn't be loaded. Please retry from KICKOFF_PROCESS."
                }
            # Print that the process cache has been loaded and the 3 params to get_process_cache
            logger.info(f"Process cache loaded with params: bot_id: {bot_id}, thread_id: {thread_id}, process_id: {process_id}")

            # Check if silent_mode is set for the thread and process
            verbose = True
            if thread_id in self.silent_mode and process_id in self.silent_mode[thread_id]:
                if self.silent_mode[thread_id][process_id]:
                    verbose = False

            with self.lock:
                if process_id not in self.process_history[thread_id]:
                    return {
                        "Success": False,
                        "Message": f"Error: Process {process_name} with id {process_id} couldn't be continued. Please retry once more from KICKOFF_PROCESS."
                    }

                if self.done[thread_id][process_id]:
                    self.last_fail[thread_id][process_id] = None
                    self.fail_count[thread_id][process_id] = None
                    return {
                        "Success": True,
                        "Message": f"Process {process_name} run complete.",
                    }

                if self.last_fail[thread_id][process_id] is not None:
                    check_response = f"""
                    A bot has retried a step of a process based on your prior feedback (shown below).  Also below is the previous question that the bot was
                    asked and the response the bot gave after re-trying to perform the task based on your feedback.  Review the response and determine if the
                    bot's response is now better in light of the instructions and the feedback you gave previously. You can accept the final results of the
                    previous step without asking to see the sql queries and results that led to the final conclusion.  Do not nitpick validity of actual data value
                    like names and similar.  Do not ask to see all the raw data that a query or other tool has generated. If you are very seriously concerned that the step
                    may still have not have been correctly perfomed, return a request to again re-run the step of the process by returning the text "**fail**"
                    followed by a DETAILED EXPLAINATION as to why it did not pass and what your concern is, and why its previous attempt to respond to your criticism
                    was not sufficient, and any suggestions you have on how to succeed on the next try. If the response looks correct, return only the text string
                    "**success**" (no explanation needed) to continue to the next step.  At this point its ok to give the bot the benefit of the doubt to avoid
                    going in circles.  By the way the current system time is {datetime.now()}.

                    Process Config: {self.process_config[thread_id][process_id]}

                    Full Process Instructions: {process['PROCESS_INSTRUCTIONS']}

                    Process History so far this run: {self.process_history[thread_id][process_id]}

                    Your previous guidance: {self.last_fail[thread_id][process_id]}

                    Bot's latest response: {previous_response}
                    """
                else:
                    check_response = f"""
                    Check the previous question that the bot was asked in the process history below and the response the bot gave after trying to perform the task.  Review the response and
                    determine if the bot's response was correct and makes sense given the instructions it was given.  You can accept the final results of the
                    previous step without asking to see the sql queries and results that led to the final conclusion.  You don't need to validate things like names or other
                    text values unless they seem wildly incorrect. You do not need to see the data that came out of a query the bot ran.

                    If you are very seriously concerned that the step may not have been correctly perfomed, return a request to re-run the step of the process again by returning the text "**fail**" followed by a
                    DETAILED EXPLAINATION as to why it did not pass and what your concern is, and any suggestions you have on how to succeed on the next try.
                    If the response seems like it is likely correct, return only the text string "**success**" (no explanation needed) to continue to the next step.  If the process is complete,
                    tell the process to stop running.  Remember, proceed under your own direction and do not ask the user for permission to proceed.

                    Remember, if you are asked to run either sql or snowpark_python from a given note_id, make sure you examine the note_type field and use the appropriate tool for
                    the note type.  Only pass the note_id, not the code itself, to the appropriate tool where the note will be handled.

                    Process Config:
                    {self.process_config[thread_id][process_id]}

                    Full process Instructions:
                    {process['PROCESS_INSTRUCTIONS']}

                    Process History so far this run:
                    {self.process_history[thread_id][process_id]}

                    Current system time:
                    {datetime.now()}

                    Bot's most recent response:
                    {previous_response}
                    """

            #     logger.info(f"\nSENT TO 2nd LLM:\n{check_response}\n")

            result = self.chat_completion(check_response, self.db_adapter, bot_id = bot_id, bot_name = '', thread_id=thread_id, process_id=process_id, process_name = process_name)

            with self.lock:
                self.process_history[thread_id][process_id] += "\nBots response: " + previous_response

            if not isinstance(result, str):
                self.set_process_cache(bot_id, thread_id, process_id)
                #         logger.info(f'Process cached with bot_id: {bot_id}, thread_id: {thread_id}, process_id: {process_id}')

                return {
                    "success": False,
                    "message": "Process failed: The checking function didn't return a string."
                }

            # logger.info("RUN 2nd LLM...")

            #        logger.info(f"\nRESULT FROM 2nd LLM: {result}\n")

            if "**fail**" in result.lower():
                with self.lock:
                    self.last_fail[thread_id][process_id] = result
                    self.fail_count[thread_id][process_id] += 1
                    self.process_history[thread_id][process_id] += "\nSupervisors concern: " + result
                if self.fail_count[thread_id][process_id] <= 5:
                    logger.info(f"\nStep {self.counter[thread_id][process_id]} failed. Fail count={self.fail_count[thread_id][process_id]} Trying again up to 5 times...\n")
                    self.set_process_cache(bot_id, thread_id, process_id)
                    #       logger.info(f'Process cached with bot_id: {bot_id}, thread_id: {thread_id}, process_id: {process_id}')

                    return_dict = {
                        "success": False,
                        "feedback_from_supervisor": result,
                        "current system time": {datetime.now()},
                        "recovery_step": f"Review the message above and submit a clarification, and/or try this Step {self.counter[thread_id][process_id]} again:\n{self.instructions[thread_id][process_id]}"
                    }
                    if verbose:
                        return_dict["additional_request"] = "Please also explain and summarize this feedback from the supervisor bot to the user so they know whats going on, and how you plan to rectify it."
                    else:
                        return_dict["shhh"] = "Remember you are running in slient, non-verbose mode. Limit your output as much as possible."

                    return return_dict

                else:
                    logger.info(f"\nStep {self.counter[thread_id][process_id]} failed. Fail count={self.fail_count[thread_id][process_id]} > 5 failures on this step, stopping process...\n")

                    with self.lock:
                        self.done[thread_id][process_id] = True
                    self.clear_process_cache(bot_id, thread_id, process_id)
                    try:
                        del self.counter[thread_id][process_id]
                    except:
                        pass
                    logger.info(f'Process cache cleared for bot_id: {bot_id}, thread_id: {thread_id}, process_id: {process_id}')

                    return {"success": "False", "message": f'The process {process_name} has failed due to > 5 repeated step completion failures.  Do not start this process again without user approval.'}

            with self.lock:
                self.last_fail[thread_id][process_id] = None
                self.fail_count[thread_id][process_id] = 0
                #          logger.info(f"\nThis step passed.  Moving to next step\n")
                self.counter[thread_id][process_id] += 1

            extract_instructions = f"""
            Extract the text for the next step from the process instructions and return it, using the section marked 'Process History' to see where you are in the process.
            Remember, the process instructions are a set of individual steps that need to be run in order.
            Return the text of the next step only, do not make any other comments or statements.
            By the way, the system default email address (SYS$DEFAULT_EMAIL) is {self.sys_default_email}.  If the instructions say to send an email
            to SYS$DEFAULT_EMAIL, replace it with {self.sys_default_email}.

            If a step of the instructions says to run another process, return '>>RECURSE' and the process name or process id as the first step
            and then call _run_process with the action KICKOFF_PROCESS to get the first step of the next process to run.  Continue this process until
            you have completed all the steps.  If you are asked to run another process as part of this process, follow the same instructions.  Do this
            up to ten times.

            If the process is complete, respond "**done**" with no other text.

            Process History: {self.process_history[thread_id][process_id]}

            Current system time: {datetime.now()}

            Process Configuration:
            {self.process_config[thread_id][process_id]}

            Process Instructions:

            {process['PROCESS_INSTRUCTIONS']}
            """

            #     logger.info(f"\nEXTRACT NEXT STEP:\n{extract_instructions}\n")

            #     logger.info("RUN 2nd LLM...")
            next_step = self.chat_completion(extract_instructions, self.db_adapter, bot_id = bot_id, bot_name = '', thread_id=thread_id, process_id=process_id, process_name=process_name)

            #      logger.info(f"\nRESULT (NEXT_STEP_): {next_step}\n")

            if next_step == '**done**' or next_step == '***done***' or next_step.strip().endswith('**done**'):
                with self.lock:
                    self.last_fail[thread_id][process_id] = None
                    self.fail_count[thread_id][process_id] = None
                    self.done[thread_id][process_id] = True
                # Clear the process cache when the process is complete
                self.clear_process_cache(bot_id, thread_id, process_id)
                logger.info(f'Process cache cleared for bot_id: {bot_id}, thread_id: {thread_id}, process_id: {process_id}')

                return {
                    "success": True,
                    "process_complete": True,
                    "message": f"Congratulations, the process {process_name} is complete.",
                    "proccess_success_step": True,
                    "reminder": f"If you were running this as a subprocess inside another process, be sure to continue the parent process."
                }

            #        logger.info(f"\n{next_step}\n")

            with self.lock:
                if ">> RECURSE" in next_step or ">>RECURSE" in next_step:
                    self.recurse_level += 1
                    # Extract the process name or ID
                    process_to_run = next_step.split(">>RECURSE")[1].strip() if ">>RECURSE" in next_step else next_step.split(">> RECURSE")[1].strip()

                    # Prepare the instruction for the bot to run the nested process
                    next_step = f"""
                    Use the _run_process tool to run the process '{process_to_run}' with the following parameters:
                    - action: KICKOFF_PROCESS
                    - process_name: {process_to_run}
                    - bot_id: {bot_id}
                    - silent_mode: {silent_mode}

                    After the nested process completes, continue with the next step of this process.
                    """

                    logger.info(f"RECURSE found.  Running process {process_to_run} on level {self.recurse_level}")

                    return {
                        "success": True,
                        "message": next_step,
                    }

                self.instructions[thread_id][process_id] = f"""
                Hey **@{process['BOT_ID']}**, here is the next step of the process.

                {next_step}

                If you are asked to run either sql or snowpark_python from a given note_id, make sure you examine the note_type field and use the appropriate tool for
                the note type.  Only pass the note_id, not the code itself, to the appropriate tool where the note will be handled.

                Execute these instructions now and then pass your response to the run_process tool as a parameter called previous_response and an action of GET_NEXT_STEP.
                If you are told to run another process in these instructions, actually run it using _run_process before calling GET_NEXT_STEP for this process, do not just pretend to run it.
                If need to terminate the process early, call with action of END_PROCESS.
                """
                if verbose:
                    self.instructions[thread_id][process_id] += """
                Tell the user what you are going to do in this step and showing interium outputs, etc. while you are running this and further steps to keep the user informed what is going on.
                For example if you are going to call a tool to perform this step, first tell the user what you're going to do.
                """
                else:
                    self.instructions[thread_id][process_id] += """
                This process is being run in low verbosity mode, so do not generate a lot of text while running this process. Just do whats required, call the right tools, etc.
                Also, it you are asked to run either sql or snowpark_python from a given note_id, make sure you examine the note_type field and use the appropriate tool for
                the note type.  Only pass the note_id, not the code itself, to the appropriate tool where the note will be handled.
                """
                self.instructions[thread_id][process_id] += f"""
                Don't stop to verify anything with the user unless specifically told to.
                By the way the current system time id: {datetime.now()}.
                In your response back to run_process, provide a detailed description of what you did, what result you achieved, and why you believe this to have successfully completed the step.
                """

            #     logger.info(f"\nEXTRACTED NEXT STEP: \n{self.instructions[thread_id][process_id]}\n")

            with self.lock:
                self.process_history[thread_id][process_id] += "\nNext step: " + next_step

            self.set_process_cache(bot_id, thread_id, process_id)
            logger.info(f'Process cached with bot_id: {bot_id}, thread_id: {thread_id}, process_id: {process_id}')

            return {
                "success": True,
                "message": self.instructions[thread_id][process_id],
            }

        elif action == "END_PROCESS":
            logger.info(f"Received END_PROCESS action for process {process_name} on level {self.recurse_level}")

            with self.lock:
                self.done[thread_id][process_id] = True

            self.clear_process_registers_by_thread(thread_id)

            self.process_id[thread_id] = None

            self.clear_process_cache(bot_id, thread_id, process_id)
            logger.info(f'Process cache cleared for bot_id: {bot_id}, thread_id: {thread_id}, process_id: {process_id}')

            self.recurse_level -= 1
            logger.info(f"Returning to recursion level {self.recurse_level}")

            return {"success": True, "message": f'The process {process_name} has finished.  You may now end the process.'}
        if action == 'STOP_ALL_PROCESSES':
            try:
                self.clear_all_process_registers(thread_id)
                return {
                    "Success": True,
                    "Message": "All processes stopped (?)"
                }
            except Exception as e:
                return {
                    "Success": False,
                    "Error": f"Failed to stop all processes: {e}"
                }
        else:
            logger.info("No action specified.")
            return {"success": False, "message": "No action specified."}

    # ====== RUN PROCESSES ==========================================================================================

    # ====== NOTEBOOK START ==========================================================================================

    def get_notebook_list(self, bot_id="all"):
        db_adapter = self.db_adapter
        cursor = db_adapter.client.cursor()
        try:
            if bot_id == "all":
                list_query = f"SELECT * FROM {db_adapter.schema}.NOTEBOOK" if db_adapter.schema else f"SELECT note_id, bot_id FROM NOTEBOOK"
                cursor.execute(list_query)
            else:
                list_query = f"SELECT * FROM {db_adapter.schema}.NOTEBOOK WHERE upper(bot_id) = upper(%s)" if db_adapter.schema else f"SELECT note_id, bot_id FROM NOTEBOOK WHERE upper(bot_id) = upper(%s)"
                cursor.execute(list_query, (bot_id,))
            notes = cursor.fetchall()
            note_list = []
            for note in notes:
                note_dict = {
                    "timestamp": note[0],
                    "bot_id": note[1],
                    "note_id": note[2],
                    'note_name': note[3],
                    'note_type': note[4],
                    'note_content': note[5],
                    'note_params': note[6]
                }
                note_list.append(note_dict)
            return {"Success": True, "notes": note_list}
        except Exception as e:
            return {
                "Success": False,
                "Error": f"Failed to list notes for bot {bot_id}: {e}",
            }
        finally:
            cursor.close()

    def get_note_info(self, bot_id=None, note_id=None):
        db_adapter = self.db_adapter
        cursor = db_adapter.client.cursor()
        try:
            result = None

            if note_id is None or note_id == '':
                return {
                    "Success": False,
                    "Error": "Note_id must be provided and cannot be empty."
                }
            if note_id is not None and note_id != '':
                query = f"SELECT * FROM {db_adapter.schema}.NOTEBOOK WHERE bot_id LIKE %s AND note_id = %s" if db_adapter.schema else f"SELECT * FROM NOTEBOOK WHERE bot_id LIKE %s AND note_id = %s"
                cursor.execute(query, (f"%{bot_id}%", note_id))
                result = cursor.fetchone()

            if result:
                # Assuming the result is a tuple of values corresponding to the columns in the NOTEBOOK table
                # Convert the tuple to a dictionary with appropriate field names
                field_names = [desc[0] for desc in cursor.description]
                return {
                    "Success": True,
                    "Data": dict(zip(field_names, result)),
                    "Note": "Only use this information to help manage or update notes",
                    "Important!": "If a user has asked you to show these notes to them, output them verbatim, do not modify or summarize them."
                }
            else:
                return {}
        except Exception as e:
            return {}

    def manage_notebook(
        self, action, bot_id=None, note_id=None, note_name = None, note_content=None, note_params=None, thread_id=None, note_type=None, note_config = None
    ):
        """
        Manages notes in the NOTEBOOK table with actions to create, delete, or update a note.

        Args:
            action (str): The action to perform
            bot_id (str): The bot ID associated with the note.
            note_id (str): The note ID for the note to manage.
            note_content (str): The content of the note for create or update actions.
            note_params (str): The parameters for the note for create or update actions.

        Returns:
            dict: A dictionary with the result of the operation.
        """

        required_fields_create = [
            "note_id",
            "bot_id",
            "note_name",
            "note_content",
        ]

        required_fields_update = [
            "note_id",
            "bot_id",
            "note_name",
            "note_content",
        ]

        if action not in ['CREATE','CREATE_CONFIRMED', 'UPDATE','UPDATE_CONFIRMED', 'DELETE', 'DELETE_CONFIRMED', 'LIST', 'TIME']:
            return {
                "Success": False,
                "Error": "Invalid action.  Manage Notebook tool only accepts actions of CREATE, CREATE_CONFIRMED, UPDATE, UPDATE_CONFIRMED, DELETE, LIST, or TIME."
            }

        try:
            if not self.done[thread_id][self.process_id[thread_id]]:
                return {
                    "Success": False,
                    "Error": "You cannot run the notebook manager from within a process.  Please run this tool outside of a process."
                }
        except KeyError as e:
            pass

        if action == "TIME":
            return {
                "current_system_time": datetime.now()
            }
        action = action.upper()

        db_adapter = self.db_adapter
        cursor = db_adapter.client.cursor()

        try:
            if action in ["UPDATE_NOTE_CONFIG", "CREATE_NOTE_CONFIG", "DELETE_NOTE_CONFIG"]:
                note_config = '' if action == "DELETE_NOTE_CONFIG" else note_config
                update_query = f"""
                    UPDATE {db_adapter.schema}.NOTEBOOK
                    SET NOTE_CONFIG = %(note_config)s
                    WHERE NOTE_ID = %(note_id)s
                """
                cursor.execute(
                    update_query,
                    {"note_config": note_config, "note_id": note_id},
                )
                db_adapter.client.commit()

                return {
                    "Success": True,
                    "Message": f"note_config updated or deleted",
                    "note_id": note_id,
                }

            if action == "CREATE" or action == "CREATE_CONFIRMED":
                # Check for dupe name
                sql = f"SELECT * FROM {db_adapter.schema}.NOTEBOOK WHERE bot_id = %s and note_id = %s"
                cursor.execute(sql, (bot_id, note_id))

                record = cursor.fetchone()

                if record:
                    return {
                        "Success": False,
                        "Error": f"Note with id {note_id} already exists for bot {bot_id}.  Please choose a different id."
                    }

            if action == "UPDATE" or action == 'UPDATE_CONFIRMED':
                # Check for dupe name
                sql = f"SELECT * FROM {db_adapter.schema}.NOTEBOOK WHERE bot_id = %s and note_id = %s"
                cursor.execute(sql, (bot_id, note_id))

                record = cursor.fetchone()

                if record and '_golden' in record[2]:
                    return {
                        "Success": False,
                        "Error": f"Note with id {note_id} is a system note and can not be updated.  Suggest making a copy with a new name."
                    }

            if (action == "CREATE" or action == "UPDATE") and note_type == 'process':
                # Send note_instructions to 2nd LLM to check it and format nicely if note type 'process'
                note_field_name = 'Note Content'
                confirm_notification_prefix = ''
                tidy_note_content = f"""
                Below is a note that has been submitted by a user.  Please review it to insure it is something
                that will make sense to the run_process tool.  If not, make changes so it is organized into clear
                steps.  Make sure that it is tidy, legible and properly formatted.

                Do not create multiple options for the instructions, as whatever you return will be used immediately.
                Return the updated and tidy instructions.  If there is an issue with the instructions, return an error message.

                If the note wants to send an email to a default email, or says to send an email but doesn't specify
                a recipient address, note that the SYS$DEFAULT_EMAIL is currently set to {self.sys_default_email}.
                Include the notation of SYS$DEFAULT_EMAIL in the instructions instead of the actual address, unless
                the instructions specify a different specific email address.

                The note is as follows:\n {note_content}
                """

                tidy_note_content= "\n".join(
                    line.lstrip() for line in tidy_note_content.splitlines()
                )

                note_content = self.chat_completion(tidy_note_content, db_adapter, bot_id = bot_id, bot_name = '', thread_id=thread_id, note_id=note_id)

            if action == "CREATE":
                return {
                    "Success": False,
                    "Fields": {"note_id": note_id, "note_name": note_name, "bot_id": bot_id, "note content": note_content, "note_params:": note_params},
                    "Confirmation_Needed": "Please reconfirm the field values with the user, then call this function again with the action CREATE_CONFIRMED to actually create the note.  If the user does not want to create a note, allow code in the process instructions",
                    "Suggestion": "If possible, for a sql or python note, suggest to the user that we test the sql or python before making the note to make sure it works properly",
                    "Next Step": "If you're ready to create this note or the user has chosen not to create a note, call this function again with action CREATE_CONFIRMED instead of CREATE.  If the user chooses to allow code in the process, allow them to do so and include the code directly in the process."
                #    "Info": f"By the way the current system time is {datetime.now().strftime('%Y-%m-%d %H:%M:%S %Z')}",
                }

            if action == "UPDATE":
                return {
                    "Success": False,
                    "Fields": {"note_id": note_id, "note_name": note_name, "bot_id": bot_id, "note content": note_content, "note_param:": note_params},
                    "Confirmation_Needed": "Please reconfirm this content and all the other note field values with the user, then call this function again with the action UPDATE_CONFIRMED to actually update the note.  If the user does not want to update the note, allow code in the process instructions",
                    "Suggestion": "If possible, for a sql or python note, suggest to the user that we test the sql or python before making the note to make sure it works properly",
                    "Next Step": "If you're ready to update this note, call this function again with action UPDATE_CONFIRMED instead of UPDATE"
                #    "Info": f"By the way the current system time is {datetime.now().strftime('%Y-%m-%d %H:%M:%S %Z')}",
                }

        except Exception as e:
            return {"Success": False, "Error": f"Error connecting to LLM: {e}"}

        if action == "CREATE_CONFIRMED":
            action = "CREATE"
        if action == "UPDATE_CONFIRMED":
            action = "UPDATE"

        if action == "DELETE":
            return {
                "Success": False,
                "Confirmation_Needed": "Please reconfirm that you are deleting the correct note_id, and double check with the user they want to delete this note, then call this function again with the action DELETE_CONFIRMED to actually delete the note.  Call with LIST to double-check the note_id if you aren't sure that its right.",
            }

        if action == "DELETE_CONFIRMED":
            action = "DELETE"

        if action not in ["CREATE", "DELETE", "UPDATE", "LIST", "SHOW"]:
            return {"Success": False, "Error": "Invalid action specified. Should be CREATE, DELETE, UPDATE, LIST, or SHOW."}

        if action == "LIST":
            logger.info("Running get notebook list")
            return self.get_notebook_list(bot_id if bot_id is not None else "all")

        if action == "SHOW":
            logger.info("Running show notebook info")
            if bot_id is None:
                return {"Success": False, "Error": "bot_id is required for SHOW action"}
            if note_id is None:
                return {"Success": False, "Error": "note_id is required for SHOW action"}

            if note_id is not None:
                return self.get_note_info(bot_id=bot_id, note_id=note_id)
            else:
                note_name = note_content['note_id']
                return self.get_note_info(bot_id=bot_id, note_name=note_name)

        note_id_created = False
        if note_id is None:
            if action == "CREATE":
                note_id = f"{bot_id}_{''.join(random.choices(string.ascii_letters + string.digits, k=6))}"
                note_id_created = True
            else:
                return {"Success": False, "Error": f"Missing note_id field"}

        try:
            if action == "CREATE":
                insert_query = f"""
                    INSERT INTO {db_adapter.schema}.NOTEBOOK (
                        created_at, updated_at, note_id, bot_id, note_name, note_content, note_params
                    ) VALUES (
                        current_timestamp(), current_timestamp(), %(note_id)s, %(bot_id)s, %(note_name)s, %(note_content)s, %(note_params)s
                    )
                """ if db_adapter.schema else f"""
                    INSERT INTO NOTEBOOK (
                        created_at, updated_at, note_id, bot_id, note_name, note_content, note_params
                    ) VALUES (
                        current_timestamp(), current_timestamp(), %(note_id)s, %(bot_id)s, %(note_name)s, %(note_content)s, %(note_params)s
                    )
                """

                insert_query= "\n".join(
                    line.lstrip() for line in insert_query.splitlines()
                )
                # Generate 6 random alphanumeric characters
                if note_id_created == False:
                    random_suffix = "".join(
                    random.choices(string.ascii_letters + string.digits, k=6)
                     )
                    note_id_with_suffix = note_id + "_" + random_suffix
                else:
                    note_id_with_suffix = note_id
                cursor.execute(
                    insert_query,
                    {
                        "note_id": note_id_with_suffix,
                        "bot_id": bot_id,
                        "note_name": note_name,
                        "note_content": note_content,
                        "note_params": note_params,
                    },
                )

                db_adapter.client.commit()
                return {
                    "Success": True,
                    "Message": f"note successfully created.",
                    "Note Id": note_id_with_suffix,
                    "Suggestion": "Now that the note is created, remind the user of the note_id and offer to test it using the correct runner, either sql, snowpark_python, or process, depending on the type set in the note_type field, and if there are any issues you can later on UPDATE the note using manage_notes to clarify anything needed.  OFFER to test it, but don't just test it unless the user agrees.  ",
                }

            elif action == "DELETE":
                delete_query = f"""
                    DELETE FROM {db_adapter.schema}.NOTEBOOK
                    WHERE note_id = %s
                """ if db_adapter.schema else f"""
                    DELETE FROM NOTEBOOK
                    WHERE note_id = %s
                """
                cursor.execute(delete_query, (note_id))

                return {
                    "Success": True,
                    "Message": f"note deleted",
                    "note_id": note_id,
                }

            elif action == "UPDATE":
                update_query = f"""
                    UPDATE {db_adapter.schema}.NOTEBOOK
                    SET updated_at = CURRENT_TIMESTAMP, note_id=%s, bot_id=%s, note_name=%s, note_content=%s, note_params=%s, note_type=%s
                    WHERE note_id = %s
                """ if db_adapter.schema else """
                    UPDATE NOTEBOOK
                    SET updated_at = CURRENT_TIMESTAMP, note_id=%s, bot_id=%s, note_name=%s, note_content=%s, note_params=%s, note_type=%s
                    WHERE note_id = %s
                """
                cursor.execute(
                    update_query,
                    (note_id, bot_id, note_name, note_content, note_params, note_type, note_id)
                )
                db_adapter.client.commit()
                return {
                    "Success": True,
                    "Message": "note successfully updated",
                    "Note id": note_id,
                    "Suggestion": "Now that the note is updated, offer to test it using run_note, and if there are any issues you can later on UPDATE the note again using manage_notebook to clarify anything needed. OFFER to test it, but don't just test it unless the user agrees.",
                }
            return {"Success": True, "Message": f"note update or delete confirmed."}
        except Exception as e:
            return {"Success": False, "Error": str(e)}

        finally:
            cursor.close()

    def get_test_manager_list(self, bot_id="all"):
        db_adapter = self.db_adapter
        cursor = db_adapter.client.cursor()
        try:
            if bot_id == "all":
                list_query = f"SELECT * FROM {db_adapter.schema}.test_manager order by test_priority" if db_adapter.schema else f"SELECT * FROM test_manager order by test_priority"
                cursor.execute(list_query)
            else:
                list_query = f"SELECT * FROM {db_adapter.schema}.test_manager WHERE upper(bot_id) = upper(%s) order by test_priority" if db_adapter.schema else f"SELECT * FROM test_manager WHERE upper(bot_id) = upper(%s) order by test_priority"
                cursor.execute(list_query, (bot_id,))
            test_processes = cursor.fetchall()
            test_process_list = []
            for test_process in test_processes:
                test_process_dict = {
                    "bot_id": test_process[1],
                    "test_process_id": test_process[2],
                    'test_process_name': test_process[3],
                    'test_type': test_process[4],
                    'test_priority': test_process[5],
                }
                test_process_list.append(test_process_dict)
            return {"Success": True, "test_processs": test_process_list}
        except Exception as e:
            return {
                "Success": False,
                "Error": f"Failed to list test_processs for bot {bot_id}: {e}",
            }
        finally:
            cursor.close()

    def manage_tests(
        self, action, bot_id=None, test_process_id = None, test_process_name = None, thread_id=None, test_type=None, test_priority = 1
    ):
        """
        Manages tests in the test_process table with actions to create, delete, or update a test_process.

        Args:
            action (str): The action to perform
            bot_id (str): The bot ID associated with the test_process.
            test_process_id (str): The test_process ID for the test manager to add/remove.
            test_priority (int): The priority used to order the run order of test_process.
            test_type (str): The type of test_process to run.

        Returns:
            dict: A dictionary with the result of the operation.
        """

        required_fields_add = [
            "test_process_id",
            "bot_id",
            "test_process_name",
        ]

        required_fields_update = [
            "test_process_id",
        ]

        if action not in ['ADD','ADD_CONFIRMED', 'UPDATE','UPDATE_CONFIRMED', 'DELETE', 'DELETE_CONFIRMED', 'LIST', 'TIME']:
            return {
                "Success": False,
                "Error": "Invalid action.  test manager tool only accepts actions of ADD, ADD_CONFIRMED, UPDATE, UPDATE_CONFIRMED, DELETE, LIST, or TIME."
            }

        db_adapter = self.db_adapter
        cursor = db_adapter.client.cursor()

        if action == "TIME":
            return {
                "current_system_time": datetime.now()
            }

        if test_process_name is not None and test_process_id is None:
            cursor.execute(f"SELECT process_id FROM {db_adapter.schema}.PROCESSES WHERE process_name = %s", (test_process_name,))
            result = cursor.fetchone()
            if result:
                test_process_id = result[0]
            else:
                return {
                    "Success": False,
                    "Error": f"Process with name {test_process_name} not found."
                }

        action = action.upper()

        try:
            if action == "ADD" or action == "ADD_CONFIRMED":
                # Check for dupe name
                sql = f"SELECT * FROM {db_adapter.schema}.test_manager WHERE bot_id = %s and test_process_name = %s"
                cursor.execute(sql, (bot_id, test_process_name))

                record = cursor.fetchone()

                if record:
                    return {
                        "Success": False,
                        "Error": f"test_process with id {test_process_name} is already included for bot {bot_id}."
                    }

            if action == "UPDATE" or action == 'UPDATE_CONFIRMED':
                # Check for dupe name
                sql = f"SELECT * FROM {db_adapter.schema}.test_manager WHERE bot_id = %s and test_process_id = %s"
                cursor.execute(sql, (bot_id, test_process_name))

                record = cursor.fetchone()

                if record and '_golden' in record[2]:
                    return {
                        "Success": False,
                        "Error": f"test_process with id {test_process_name} is a system test_process and can not be updated.  Suggest making a copy with a new name."
                    }

            if action == "ADD":
                return {
                    "Success": False,
                    "Fields": {"test_process_id": test_process_id, "test_process_name": test_process_name, "bot_id": bot_id},
                    "Confirmation_Needed": "Please reconfirm the field values with the user, then call this function again with the action CREATE_CONFIRMED to actually create the test_process.  If the user does not want to create a test_process, allow code in the process instructions",
                    "Suggestion": "If possible, for a sql or python test_process, suggest to the user that we test the sql or python before making the test_process to make sure it works properly",
                    "Next Step": "If you're ready to create this test_process or the user has chosen not to create a test_process, call this function again with action CREATE_CONFIRMED instead of CREATE.  If the user chooses to allow code in the process, allow them to do so and include the code directly in the process."
                #    "Info": f"By the way the current system time is {datetime.now().strftime('%Y-%m-%d %H:%M:%S %Z')}",
                }

            if action == "UPDATE":
                return {
                    "Success": False,
                    "Fields": {"test_process_id": test_process_id, "test_process_name": test_process_name, "bot_id": bot_id},
                    "Confirmation_Needed": "Please reconfirm this content and all the other test_process field values with the user, then call this function again with the action UPDATE_CONFIRMED to actually update the test_process.  If the user does not want to update the test_process, allow code in the process instructions",
                    "Suggestion": "If possible, for a sql or python test_process, suggest to the user that we test the sql or python before making the test_process to make sure it works properly",
                    "Next Step": "If you're ready to update this test_process, call this function again with action UPDATE_CONFIRMED instead of UPDATE"
                #    "Info": f"By the way the current system time is {datetime.now().strftime('%Y-%m-%d %H:%M:%S %Z')}",
                }

        except Exception as e:
            return {"Success": False, "Error": f"Error connecting to LLM: {e}"}

        if action == "ADD_CONFIRMED":
            action = "ADD"
        if action == "UPDATE_CONFIRMED":
            action = "UPDATE"

        if action == "DELETE":
            return {
                "Success": False,
                "Confirmation_Needed": "Please reconfirm that you are deleting the correct test_process_id, and double check with the user they want to delete this test_process, then call this function again with the action DELETE_CONFIRMED to actually delete the test_process.  Call with LIST to double-check the test_process_id if you aren't sure that its right.",
            }

        if action == "DELETE_CONFIRMED":
            action = "DELETE"

        if action not in ["ADD", "DELETE", "UPDATE", "LIST", "SHOW"]:
            return {"Success": False, "Error": "Invalid action specified. Should be ADD, DELETE, UPDATE, LIST, or SHOW."}

        if action == "LIST":
            logger.info("Running get test_process list")
            return self.get_test_manager_list(bot_id if bot_id is not None else "all")

        if action == "SHOW":
            logger.info("Running show test_process info")
            if bot_id is None:
                return {"Success": False, "Error": "bot_id is required for SHOW action"}
            if test_process_name is None:
                return {"Success": False, "Error": "process is required for SHOW action"}

        test_process_id_created = False
        if test_process_name is None:
            if action == "ADD":
                test_process_id_created = True
            else:
                return {"Success": False, "Error": f"Missing test_process_id field"}
        try:
            if action == "ADD":
                insert_query = f"""
                    INSERT INTO {db_adapter.schema}.test_manager (
                        created_at, updated_at, test_process_id, bot_id
                    ) VALUES (
                        current_timestamp(), current_timestamp(), %(test_process_id)s, %(bot_id)s
                    )
                """ if db_adapter.schema else f"""
                    INSERT INTO test_manager (
                        created_at, updated_at, test_process_id, bot_id
                    ) VALUES (
                        current_timestamp(), current_timestamp(), %(test_process_id)s, %(bot_id)s
                    )
                """

                insert_query= "\n".join(
                    line.lstrip() for line in insert_query.splitlines()
                )
                # Generate 6 random alphanumeric characters
                if test_process_id_created == False:
                    random_suffix = "".join(
                    random.choices(string.ascii_letters + string.digits, k=6)
                     )
                    test_process_id_with_suffix = test_process_id + "_" + random_suffix
                else:
                    test_process_id_with_suffix = test_process_id
                cursor.execute(
                    insert_query,
                    {
                        "test_process_id": test_process_id_with_suffix,
                        "bot_id": bot_id,
                        "test_process_name": test_process_name,
                        "test_priority": test_priority
                    },
                )

                db_adapter.client.commit()
                return {
                    "Success": True,
                    "Message": f"test_process successfully created.",
                    "test_process Id": test_process_id_with_suffix,
                    "Suggestion": "Now that the test_process has been added to the test suite, OFFER to test it, but don't just test it unless the user agrees.  ",
                }

            elif action == "DELETE":
                delete_query = f"""
                    DELETE FROM {db_adapter.schema}.test_manager
                    WHERE test_process_id = %s
                """ if db_adapter.schema else f"""
                    DELETE FROM test_process
                    WHERE test_process_id = %s
                """
                cursor.execute(delete_query, (test_process_id))

                return {
                    "Success": True,
                    "Message": f"test_process deleted",
                    "test_process_id": test_process_id,
                }

            elif action == "UPDATE":
                update_query = f"""
                    UPDATE {db_adapter.schema}.test_manager
                    SET updated_at = CURRENT_TIMESTAMP, test_process_id=%s, bot_iprocess=%s, test_process_content=%s
                    WHERE test_process_id = %s
                """ if db_adapter.schema else """
                    UPDATE test_process
                    SET updated_at = CURRENT_TIMESTAMP, test_process_id=%s, bot_iprocess=%s, test_process_content=%s
                    WHERE test_process_id = %s
                """
                cursor.execute(
                    update_query,
                    (test_process_id, bot_id, test_process_name,test_type, test_process_id)
                )
                db_adapter.client.commit()
                return {
                    "Success": True,
                    "Message": "test_process successfully updated",
                    "test_process id": test_process_id,
                }
            return {"Success": True, "Message": f"test_process update or delete confirmed."}
        except Exception as e:
            return {"Success": False, "Error": str(e)}

        finally:
            cursor.close()

    def insert_notebook_history(
        self,
        note_id,
        work_done_summary,
        note_status,
        updated_note_learnings,
        report_message="",
        done_flag=False,
        needs_help_flag="N",
        note_clarity_comments="",
    ):
        """
        Inserts a row into the NOTEBOOK_HISTORY table.

        Args:
            note_id (str): The unique identifier for the note.
            work_done_summary (str): A summary of the work done.
            note_status (str): The status of the note.
            updated_note_learnings (str): Any new learnings from the note.
            report_message (str): The message to report about the note.
            done_flag (bool): Flag indicating if the note is done.
            needs_help_flag (bool): Flag indicating if help is needed.
            note_clarity_comments (str): Comments on the clarity of the note.
        """
        db_adapter = self.db_adapter
        insert_query = f"""
            INSERT INTO {db_adapter.schema}.NOTEBOOK_HISTORY (
                note_id, work_done_summary, note_status, updated_note_learnings,
                report_message, done_flag, needs_help_flag, note_clarity_comments
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s
            )
        """ if db_adapter.schema else f"""
            INSERT INTO NOTEBOOK_HISTORY (
                note_id, work_done_summary, note_status, updated_note_learnings,
                report_message, done_flag, needs_help_flag, note_clarity_comments
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s
            )
        """
        try:
            cursor = db_adapter.client.cursor()
            cursor.execute(
                insert_query,
                (
                    note_id,
                    work_done_summary,
                    note_status,
                    updated_note_learnings,
                    report_message,
                    done_flag,
                    needs_help_flag,
                    note_clarity_comments,
                ),
            )
            db_adapter.client.commit()
            cursor.close()
            logger.info(
                f"Notebook history row inserted successfully for note_id: {note_id}"
            )
        except Exception as e:
            logger.info(f"An error occurred while inserting the notebook history row: {e}")
            if cursor is not None:
                cursor.close()

    # ====== NOTEBOOK END ==========================================================================================

    # ====== ARTIFACTS BEGIN ==========================================================================================
    def manage_artifact(self,
                        action: str,
                        artifact_id: Optional[str] = None,
                        thread_id=None,  # ignored, saved for future use
                        bot_id=None      # ignored, saved for future use
                        ) -> str|dict:
        """
        A wrapper for LLMs to access/manage artifacts by performing specified actions such as describing or deleting an artifact.

        Args:
            action (str): The action to perform on the artifact. Supported actions are 'DESCRIBE' and 'DELETE'.
            artifact_id (Optional[str]): The unique identifier of the artifact. Required for certain actions.
            thread_id: Reserved for future use.
            bot_id: Reserved for future use.

        Returns:
            str: A dictionary containing the result of the action. E.g. for 'DESCRIBE', it includes the artifact metadata and a
                 note for the LLMs on how to format an artifact reference
        """
        af = get_artifacts_store(self.db_adapter)

        action = action.upper()

        if action == "DESCRIBE":
            try:
                metadata = af.get_artifact_metadata(artifact_id)
            except Exception as e:
                return {"Success": False,
                        "Error": str(e)
                        }
            assert 'title_filename' in metadata.keys()  # listed in METADATA_IN_REQUIRED_FIELDS
            ref_notes = af.get_llm_artifact_ref_instructions(artifact_id)
            return {
                "Success": True,
                "Data": metadata,
                "Note": ref_notes,
            }

        elif action == "DELETE":
            raise NotImplementedError()  # TODO: implement this

    # ====== ARTIFACTS END ==========================================================================================

    def google_drive(self, action, thread_id=None, g_file_id=None, g_sheet_cell = None, g_sheet_value = None):
        """
        A wrapper for LLMs to access/manage Google Drive files by performing specified actions such as listing or downloading files.

        Args:
            action (str): The action to perform on the Google Drive files. Supported actions are 'LIST' and 'DOWNLOAD'.

        Returns:
            dict: A dictionary containing the result of the action. E.g. for 'LIST', it includes the list of files in the Google Drive.
        """
        def column_to_number(letter: str) -> int:
            num = 0
            for char in letter:
                num = num * 26 + (ord(char.upper()) - ord('A') + 1)
            return num

        def number_to_column(num: int) -> str:
            result = ""
            while num > 0:
                num -= 1
                result = chr(num % 26 + 65) + result
                num //= 26
            return result

        def verify_single_cell(g_sheet_cell: str) -> str:
            pattern = r"^([a-zA-Z]{1,3})(\d{1,4})$"
            match = re.match(pattern, g_sheet_cell)
            if not match:
                raise ValueError("Invalid g_sheet_cell format. It should start with 1-3 letters followed by 1-4 numbers.")

            col, row = match.groups()
            # next_col = number_to_column(column_to_number(col) + 1)
            range = f"{col}{row}" # :{next_col}{row}"

            return range

        def verify_cell_range(g_sheet_cell):
            pattern = r"^([A-Z]{1,2})(\d+):([A-Z]{1,2})(\d+)$"
            match = re.match(pattern, g_sheet_cell)

            # Verify range is only one cell
            if not match:
                raise ValueError("Invalid g_sheet_cell format. It should be in the format 'A1:B1'.")

            # column_1, row_1, column_2, row_2 = match.groups()
            # column_1_int = column_to_number(column_1)
            # column_2_int = column_to_number(column_2)

            return True

        if action == "LIST":
            return self.get_google_drive_files()

        elif action == "TEST":
            return {"Success": True, "message": "Test successful"}

        elif action == "SET_ROOT_FOLDER":
            raise NotImplementedError

        elif action == "GET_FILE_VERSION_NUM":
            try:
                file_version_num = get_g_file_version(self.db_adapter.user, g_file_id)
                return {"Success": True, "file_version_num": file_version_num}
            except Exception as e:
                return {"Success": False, "Error": str(e)}

        elif action == "GET_COMMENTS":
            try:
                comments_and_replies = get_g_file_comments(self.db_adapter.user, g_file_id)
                return {"Success": True, "Comments & Replies": comments_and_replies}
            except Exception as e:
                return {"Success": False, "Error": str(e)}

        elif action == "GET_SHEET_CELL":
            sheet_range = verify_single_cell(g_sheet_cell)
            try:
                value = read_g_sheet(
                    g_file_id, sheet_range, None, self.db_adapter.user
                )
                return {"Success": True, "value": value}
            except Exception as e:
                return {"Success": False, "Error": str(e)}

        elif action == "EDIT_SHEET_CELL":
            range = verify_single_cell(g_sheet_cell)

            print(
                f"\nG_sheet value to insert to cell {g_sheet_cell}: Value: {g_sheet_value}\n"
            )

            write_g_sheet_cell(
                g_file_id, range, g_sheet_value, None, self.db_adapter.user
            )

            return {
                "Success": True,
                "Message": f"g_sheet value to insert to cell {range}: Value: {g_sheet_value}",
            }

        elif action == "LOGIN":
            from google_auth_oauthlib.flow import Flow

            SCOPES = [
                "https://www.googleapis.com/auth/drive.file",
                "https://www.googleapis.com/auth/documents",
                "https://www.googleapis.com/auth/drive"
            ]

            redirect_url = f"{os.environ['NGROK_BASE_URL']}:8080/oauth"

            flow = Flow.from_client_secrets_file(
                f"credentials.json",
                scopes=SCOPES,
                redirect_uri = redirect_url #"http://127.0.0.1:8080/oauth",  # Your redirect URI
            )
            auth_url, _ = flow.authorization_url(prompt="consent")
            return {"Success": "True", "auth_url": f"<{auth_url}|View Document>"}

    def get_google_drive_files(self):
        pass

    def process_scheduler(
        self, action, bot_id, task_id=None, task_details=None, thread_id=None, history_rows=10
    ):
        import random
        import string
        """
        Manages tasks in the TASKS table with actions to create, delete, or update a task.

        Args:
            action (str): The action to perform - 'CREATE', 'DELETE', or 'UPDATE'.
            bot_id (str): The bot ID associated with the task.
            task_id (str): The task ID for the task to manage.
            task_details (dict, optional): The details of the task for create or update actions.

        Returns:
            dict: A dictionary with the result of the operation.
        """

        #    logger.info("Reached process scheduler")

        if task_details and 'process_name' in task_details and 'task_name' not in task_details:
            task_details['task_name'] = task_details['process_name']
            del task_details['process_name']

        required_fields_create = [
            "task_name",
            "primary_report_to_type",
            "primary_report_to_id",
            "next_check_ts",
            "action_trigger_type",
            "action_trigger_details",
            "last_task_status",
            "task_learnings",
            "task_active",
        ]

        required_fields_update = ["task_active"]
        db_adapter = self.db_adapter
        client = db_adapter.client
        cursor = client.cursor()
        if action == "HISTORY":
            if not task_id:
                return {
                    "Success": False,
                    "Error": "task_id is required for retrieving task history. You can get the task_id by calling this function with the 'LIST' action for the bot_id."
                }
            limit = history_rows
            history_query = f"""
                SELECT * FROM {db_adapter.schema}.TASK_HISTORY
                WHERE task_id = %s
                ORDER BY RUN_TIMESTAMP DESC
                LIMIT %s
                """
            try:
                cursor.execute(history_query, (task_id, limit))
                client.commit()
                history = cursor.fetchall()
                return {
                    "Success": True,
                    "Task History": history,
                    "history_rows": limit
                }
            except Exception as e:
                return {
                    "Success": False,
                    "Error": e
                }

        if action == "TIME":
            return {
                "current_system_time": db_adapter.get_current_time_with_timezone()
            }
        action = action.upper()

        if action == "CREATE":
            return {
                "Success": False,
                "Confirmation_Needed": "Please reconfirm all the scheduled process details with the user, then call this function again with the action CREATE_CONFIRMED to actually create the schedule for the process.   Make sure to be clear in the action_trigger_details field whether the process schedule is to be triggered one time, or if it is ongoing and recurring. Also make the next Next Check Timestamp is in the future, and aligns with when the user wants the task to run next",
                "Process Schedule Details": task_details,
                "Info": f"By the way the current system time is {db_adapter.get_current_time_with_timezone()}",
            }
        if action == "CREATE_CONFIRMED":
            action = "CREATE"

        if action == "UPDATE":

            return {
                "Success": False,
                "Confirmation_Needed": "Please reconfirm all the updated process details with the user, then call this function again with the action UPDATE_CONFIRMED to actually update the schedule for the process.   Make sure to be clear in the action_trigger_details field whether the process schedule is to be triggered one time, or if it is ongoing and recurring. Also make the next Next Check Timestamp is in the future, and aligns with when the user wants the task to run next.",
                "Proposed Updated Process Schedule Details": task_details,
                "Info": f"By the way the current system time is {db_adapter.get_current_time_with_timezone()}",
            }
        if action == "UPDATE_CONFIRMED":
            action = "UPDATE"

        if action == "DELETE":
            return {
                "Success": False,
                "Confirmation_Needed": "Please reconfirm that you are deleting the correct TASK_ID, and double check with the user they want to delete this schedule for the process, then call this function again with the action DELETE_CONFIRMED to actually delete the task.  Call with LIST to double-check the task_id if you aren't sure that its right.",
            }

        if action == "DELETE_CONFIRMED":
            action = "DELETE"

        if action not in ["CREATE", "DELETE", "UPDATE", "LIST"]:
            return {"Success": False, "Error": "Invalid action specified."}

        if action == "LIST":
            try:
                list_query = (
                    f"SELECT * FROM {db_adapter.schema}.TASKS WHERE upper(bot_id) = upper(%s)"
                )
                cursor.execute(list_query, (bot_id,))
                tasks = cursor.fetchall()
                task_list = []
                for task in tasks:
                    next_check = None
                    if task[5] is not None:
                        next_check = task[5].strftime("%Y-%m-%d %H:%M:%S")
                    task_dict = {
                        "task_id": task[0],
                        "bot_id": task[1],
                        "task_name": task[2],
                        "primary_report_to_type": task[3],
                        "primary_report_to_id": task[4],
                        "next_check_ts": next_check,
                        "action_trigger_type": task[6],
                        "action_trigger_details": task[7],
                        "process_name_to_run": task[8],
                        "reporting_instructions": task[9],
                        "last_task_status": task[10],
                        "task_learnings": task[11],
                        "task_active": task[12],
                    }
                    task_list.append(task_dict)
                return {"Success": True, "Scheduled Processes": task_list, "Note": "Don't take any immediate actions on this information unless instructed to by the user. Also note the task_id is the id of the schedule, not the id of the process to run."}
            except Exception as e:
                return {
                    "Success": False,
                    "Error": f"Failed to list tasks for bot {bot_id}: {e}",
                }

        if task_id is None:
            return {"Success": False, "Error": f"Missing task_id field"}

        if action in ["CREATE", "UPDATE"] and not task_details:
            return {
                "Success": False,
                "Error": "Task details must be provided for CREATE or UPDATE action.",
            }

        if action in ["CREATE"] and task_details and any(
            field not in task_details for field in required_fields_create
        ):
            missing_fields = [
                field for field in required_fields_create if field not in task_details
            ]
            return {
                "Success": False,
                "Error": f"Missing required task details: {', '.join(missing_fields)}",
            }

        if action in ["UPDATE"] and task_details and any(
            field not in task_details for field in required_fields_update
        ):
            missing_fields = [
                field for field in required_fields_update if field not in task_details
            ]
            return {
                "Success": False,
                "Error": f"Missing required task details: {', '.join(missing_fields)}",
            }

        # Check if the action is CREATE or UPDATE
        if action in ["CREATE", "UPDATE"] and task_details and "task_name" in task_details:
            # Check if the task_name is a valid process for the bot
            valid_processes = self.get_processes_list(bot_id=bot_id)
            if not valid_processes["Success"]:
                return {
                    "Success": False,
                    "Error": f"Failed to retrieve processes for bot {bot_id}: {valid_processes['Error']}",
                }

            if task_details["task_name"] not in [
                process["process_name"] for process in valid_processes["processes"]
            ]:
                return {
                    "Success": False,
                    "Error": f"Invalid task_name: {task_details.get('task_name')}. It must be one of the valid processes for this bot",
                    "Valid_Processes": [process["process_name"] for process in valid_processes["processes"]],
                }

        # Convert timestamp from string in format 'YYYY-MM-DD HH:MM:SS' to a Snowflake-compatible timestamp
        if task_details is not None and task_details.get("task_active", False):
            try:
                formatted_next_check_ts = datetime.strptime(
                    task_details["next_check_ts"], "%Y-%m-%d %H:%M:%S"
                )
            except ValueError as ve:
                return {
                    "Success": False,
                    "Error": f"Invalid timestamp format for 'next_check_ts'. Required format: 'YYYY-MM-DD HH:MM:SS' in system timezone. Error details: {ve}",
                    "Info": f"Current system time in system timezone is {db_adapter.get_current_time_with_timezone()}. Please note that the timezone should not be included in the submitted timestamp.",
                }
            if formatted_next_check_ts < datetime.now():
                return {
                    "Success": False,
                    "Error": "The 'next_check_ts' is in the past.",
                    "Info": f"Current system time is {db_adapter.get_current_time_with_timezone()}",
                }

        try:
            if action == "CREATE":
                insert_query = f"""
                    INSERT INTO {db_adapter.schema}.TASKS (
                        task_id, bot_id, task_name, primary_report_to_type, primary_report_to_id,
                        next_check_ts, action_trigger_type, action_trigger_details, task_instructions,
                        reporting_instructions, last_task_status, task_learnings, task_active
                    ) VALUES (
                        %(task_id)s, %(bot_id)s, %(task_name)s, %(primary_report_to_type)s, %(primary_report_to_id)s,
                        %(next_check_ts)s, %(action_trigger_type)s, %(action_trigger_details)s, null,
                        null, %(last_task_status)s, %(task_learnings)s, %(task_active)s
                    )
                """

                # Generate 6 random alphanumeric characters
                random_suffix = "".join(
                    random.choices(string.ascii_letters + string.digits, k=6)
                )
                task_id_with_suffix = task_id + "_" + random_suffix
                cursor.execute(
                    insert_query,
                    {**task_details, "task_id": task_id_with_suffix, "bot_id": bot_id},
                )
                client.commit()
                return {
                    "Success": True,
                    "Message": f"Task successfully created, next check scheduled for {task_details['next_check_ts']}",
                }

            elif action == "DELETE":
                delete_query = f"""
                    DELETE FROM {db_adapter.schema}.TASKS
                    WHERE task_id = %s AND bot_id = %s
                """
                cursor.execute(delete_query, (task_id, bot_id))
                client.commit()

            elif action == "UPDATE":
                if task_details['task_active'] == False:
                    task_details['next_check_ts'] = None
                update_query = f"""
                    UPDATE {db_adapter.schema}.TASKS
                    SET {', '.join([f"{key} = %({key})s" for key in task_details.keys()])}
                    WHERE task_id = %(task_id)s AND bot_id = %(bot_id)s
                """
                cursor.execute(
                    update_query, {**task_details, "task_id": task_id, "bot_id": bot_id}
                )
                client.commit()

            return {"Success": True, "Message": f"Task update or delete confirmed."}
        except Exception as e:
            return {"Success": False, "Error": str(e)}

        finally:
            cursor.close()

    # ====== PROCESSES START ========================================================================================

    def get_processes_list(self, bot_id="all"):
        db_adapter = self.db_adapter
        cursor = db_adapter.client.cursor()
        try:
            if bot_id == "all":
                list_query = f"SELECT process_id, bot_id, process_name FROM {db_adapter.schema}.PROCESSES WHERE HIDDEN IS NULL OR HIDDEN = FALSE" if db_adapter.schema else f"SELECT process_id, bot_id, process_name FROM PROCESSES WHERE HIDDEN IS NULL OR HIDDEN = FALSE"
                cursor.execute(list_query)
            else:
                list_query = f"SELECT process_id, bot_id, process_name FROM {db_adapter.schema}.PROCESSES WHERE upper(bot_id) = upper(%s) AND HIDDEN IS NULL OR HIDDEN = FALSE" if db_adapter.schema else f"SELECT process_id, bot_id, process_name FROM PROCESSES WHERE upper(bot_id) = upper(%s) AND HIDDEN IS NULL OR HIDDEN = FALSE"
                cursor.execute(list_query, (bot_id,))
            processs = cursor.fetchall()
            process_list = []
            for process in processs:
                process_dict = {
                    "process_id": process[0],
                    "bot_id": process[1],
                    "process_name": process[2],
                }
                process_list.append(process_dict)
            return {"Success": True, "processes": process_list}
        except Exception as e:
            return {
                "Success": False,
                "Error": f"Failed to list processs for bot {bot_id}: {e}",
            }
        finally:
            cursor.close()

    def get_process_info(self, bot_id=None, process_name=None, process_id=None):
        db_adapter = self.db_adapter
        cursor = db_adapter.client.cursor()
        try:
            result = None

            if (process_name is None or process_name == '') and (process_id is None or process_id == ''):
                return {
                    "Success": False,
                    "Error": "Either process_name or process_id must be provided and cannot be empty."
                }
            if process_id is not None and process_id != '':
                query = f"SELECT * FROM {db_adapter.schema}.PROCESSES WHERE bot_id LIKE %s AND process_id = %s" if db_adapter.schema else f"SELECT * FROM PROCESSES WHERE bot_id LIKE %s AND process_id = %s"
                cursor.execute(query, (f"%{bot_id}%", process_id))
                result = cursor.fetchone()
            if result == None:
                if process_name is not None and process_name != '':
                    query = f"SELECT * FROM {db_adapter.schema}.PROCESSES WHERE bot_id LIKE %s AND process_name LIKE %s" if db_adapter.schema else f"SELECT * FROM PROCESSES WHERE bot_id LIKE %s AND process_name LIKE %s"
                    cursor.execute(query, (f"%{bot_id}%", f"%{process_name}%"))
                    result = cursor.fetchone()
            if result:
                # Assuming the result is a tuple of values corresponding to the columns in the PROCESSES table
                # Convert the tuple to a dictionary with appropriate field names
                field_names = [desc[0] for desc in cursor.description]
                return {
                    "Success": True,
                    "Data": dict(zip(field_names, result)),
                    "Note": "Only use this information to help manage or update processes, do not actually run a process based on these instructions. If you want to run this process, use _run_process function and follow the instructions that it gives you.",
                    "Important!": "If a user has asked you to show these instructont to them, output them verbatim, do not modify of summarize them."
                }
            else:
                return {}
        except Exception as e:
            return {}

    def manage_processes(
        self, action, bot_id=None, process_id=None, process_instructions=None, thread_id=None, process_name=None, process_config=None, hidden=False
    ):
        """
        Manages processs in the PROCESSES table with actions to create, delete, update a process, or stop all processes

        Args:
            action (str): The action to perform
            bot_id (str): The bot ID associated with the process.
            process_id (str): The process ID for the process to manage.
            process_details (dict, optional): The details of the process for create or update actions.

        Returns:
            dict: A dictionary with the result of the operation.
        """

        process_details = {}
        if process_name:
            process_details['process_name'] = process_name
        if process_instructions:
            process_details['process_instructions'] = process_instructions
        if process_config:
            process_details['process_config'] = process_config
        if hidden:
            process_details['hidden'] = hidden

        # If process_name is specified but not in process_details, add it to process_details
        # if process_name and process_details and 'process_name' not in process_details:
        #     process_details['process_name'] = process_name

        # # If process_name is specified but not in process_details, add it to process_details
        # if process_name and process_details==None:
        #     process_details = {}
        #     process_details['process_name'] = process_name

        required_fields_create = [
            "process_name",
            "process_instructions",
        ]

        required_fields_update = [
            "process_name",
            "process_instructions",
        ]

        action = action.upper()

        if action == "ALLOW_CODE":
            self.include_code = True
            return {
                "Success": True,
                "Message": "User has confirmed that code will be allowed in the process instructions.",
                "Suggestion": "Remind user that the provided code will be included directly in the process instructions, but best pratices are to create a note",
                "Reminder": "  Allow code to be included in the process instructions.  Run manage_process with the action CREATE_CONFIRMED to create the process.",
            }

        if action == "TIME":
            return {
                "current_system_time": datetime.now().astimezone()
            }
        db_adapter = self.db_adapter
        cursor = db_adapter.client.cursor()

        try:
            if action == "HIDE_PROCESS":
                hide_query = f"""
                    UPDATE {db_adapter.schema}.PROCESSES
                    SET HIDDEN = True
                    WHERE PROCESS_ID = %(process_id)s
                """
                cursor.execute(
                    hide_query,
                    {"process_id": process_id},
                )
                db_adapter.client.commit()

            if action == "UNHIDE_PROCESS":
                hide_query = f"""
                    UPDATE {db_adapter.schema}.PROCESSES
                    SET HIDDEN = False
                    WHERE PROCESS_ID = %(process_id)s
                """
                cursor.execute(
                    hide_query,
                    {"process_id": process_id},
                )
                db_adapter.client.commit()

            if action in ["UPDATE_PROCESS_CONFIG", "CREATE_PROCESS_CONFIG", "DELETE_PROCESS_CONFIG"]:
                process_config = '' if action == "DELETE_PROCESS_CONFIG" else process_config
                update_query = f"""
                    UPDATE {db_adapter.schema}.PROCESSES
                    SET PROCESS_CONFIG = %(process_config)s
                    WHERE PROCESS_ID = %(process_id)s
                """
                cursor.execute(
                    update_query,
                    {"process_config": process_config, "process_id": process_id},
                )
                db_adapter.client.commit()

                return {
                    "Success": True,
                    "Message": f"process_config updated or deleted",
                    "process_id": process_id,
                }

            if action in ["CREATE", "CREATE_CONFIRMED", "UPDATE", "UPDATE_CONFIRMED"]:
                check_for_code_instructions = f"""Please examine the text below and return only the word 'SQL' if the text contains
                actual SQL code, not a reference to SQL code, or only the word 'PYTHON' if the text contains actual Python code, not a reference to Python code.
                If the text contains both, return only 'SQL + PYTHON'.  Do not return any other verbage.  If the text contains
                neither, return only the word 'NO CODE':\n {process_details['process_instructions']}"""
                result = self.chat_completion(check_for_code_instructions, self.db_adapter, bot_id=bot_id, bot_name='')

                if result != 'NO CODE':
                    return {
                        "Success": True,
                        "Suggestion": "Explain to the user that any SQL or Python code should be separately tested and stored as a 'note', which is a special way to store sql or python that will be used within processes. This helps keep the process instuctions themselves clean and makes processes run more reliably.  Ask the user oif they would like to create a note.  If the user prefers not to create a note, the code may be added directly into the process, but this is not recommended.  If the user does not want to create a note, run CREATE_CONFIRMED to add the process with the code included.",
                        "Reminder": f"Ask the user of they would like to remove the code and replace it with a note_id to the code in the note table.  Then replace the code in the process with the note_id of the new note.  Do not include the note contents in the process, just include an instruction to run the note with the note_id.  If the user prefers not to create a note, the code may be added directly into the process, but this is not recommended.   If the user prefers not to create a note, the code may be added directly into the process, but this is not recommended.  If the user does not want to create a note, run CREATE_CONFIRMED to add the process with the code included."
                    }

            if action == "CREATE" or action == "CREATE_CONFIRMED":
                # Check for dupe name
                sql = f"SELECT * FROM {db_adapter.schema}.PROCESSES WHERE bot_id = %s and process_name = %s"
                cursor.execute(sql, (bot_id, process_details['process_name']))

                record = cursor.fetchone()

                if record:
                    return {
                        "Success": False,
                        "Error": f"Process with name {process_details['process_name']} already exists.  Please choose a different name."
                    }

            if action == "UPDATE" or action == 'UPDATE_CONFIRMED':
                # Check for dupe name
                sql = f"SELECT * FROM {db_adapter.schema}.PROCESSES WHERE bot_id = %s and process_name = %s"
                cursor.execute(sql, (bot_id, process_details['process_name']))

                record = cursor.fetchone()

                if record and '_golden' in record[2]:  # process_id
                    return {
                        "Success": False,
                        "Error": f"Process with name {process_details['process_name']} is a system process and can not be updated.  Suggest making a copy with a new name."
                    }

            if action == "CREATE" or action == "UPDATE":
                # Check for dupe name
                # sql = f"SELECT * FROM {db_adapter.schema}.PROCESSES WHERE bot_id = %s and process_name = %s"
                # cursor.execute(sql, (bot_id, process_details['process_name']))

                # record = cursor.fetchone()

                # if record and '_golden' in record['process_id']:
                #     return {
                #         "Success": False,
                #         "Error": f"Process with name {process_details['process_name']}.  Please choose a different name."
                #     }

                # Send process_instructions to 2nd LLM to check it and format nicely
                tidy_process_instructions = f"""
                Below is a process that has been submitted by a user.  Please review it to insure it is something
                that will make sense to the run_process tool.  If not, make changes so it is organized into clear
                steps.  Make sure that it is tidy, legible and properly formatted.

                Do not create multiple options for the instructions, as whatever you return will be used immediately.
                Return the updated and tidy process.  If there is an issue with the process, return an error message."""

                if not self.include_code:
                    tidy_process_instructions = f"""

                Since the process contains either sql or snowpark_python code, you will need to ask the user if they want
                to allow code in the process.  If they do, go ahead and allow the code to remain in the process.
                If they do not, extract the code and create a new note with
                your manage_notebook tool, maing sure to specify the note_type field as either 'sql or 'snowpark_python'.
                Then replace the code in the process with the note_id of the new note.  Do not
                include the note contents in the process, just include an instruction to run the note with the note_id."""

                tidy_process_instructions = f"""

                If the process wants to send an email to a default email, or says to send an email but doesn't specify
                a recipient address, note that the SYS$DEFAULT_EMAIL is currently set to {self.sys_default_email}.
                Include the notation of SYS$DEFAULT_EMAIL in the instructions instead of the actual address, unless
                the instructions specify a different specific email address.

                If one of the steps of the process involves scheduling this process to run on a schedule, remove that step,
                and instead include a note separate from the cleaned up process that the user should instead use _process_scheduler
                to schedule the process after it has been created.

                The process is as follows:\n {process_details['process_instructions']}
                """

                tidy_process_instructions = "\n".join(
                    line.lstrip() for line in tidy_process_instructions.splitlines()
                )

                process_details['process_instructions'] = self.chat_completion(tidy_process_instructions, self.db_adapter, bot_id = bot_id, bot_name = '', thread_id=thread_id, process_id=process_id, process_name=process_name)

            if action == "CREATE":
                return {
                    "Success": False,
                    "Cleaned up instructions": process_details['process_instructions'],
                    "Confirmation_Needed": "I've run the process instructions through a cleanup step.  Please reconfirm these instructions and all the other process details with the user, then call this function again with the action CREATE_CONFIRMED to actually create the process.",
                    "Next Step": "If you're ready to create this process call this function again with action CREATE_CONFIRMED instead of CREATE"
                #    "Info": f"By the way the current system time is {datetime.now().strftime('%Y-%m-%d %H:%M:%S %Z')}",
                }

            if action == "UPDATE":
                return {
                    "Success": False,
                    "Cleaned up instructions": process_details['process_instructions'],
                    "Confirmation_Needed": "I've run the process instructions through a cleanup step.  Please reconfirm these instructions and all the other process details with the user, then call this function again with the action UPDATE_CONFIRMED to actually update the process.",
                    "Next Step": "If you're ready to update this process call this function again with action UPDATE_CONFIRMED instead of UPDATE"
                #    "Info": f"By the way the current system time is {datetime.now().strftime('%Y-%m-%d %H:%M:%S %Z')}",
                }

        except Exception as e:
            return {"Success": False, "Error": f"Error connecting to LLM: {e}"}

        if action == "CREATE_CONFIRMED":
            action = "CREATE"
        if action == "UPDATE_CONFIRMED":
            action = "UPDATE"

        if action == "DELETE":
            return {
                "Success": False,
                "Confirmation_Needed": "Please reconfirm that you are deleting the correct process_ID, and double check with the user they want to delete this process, then call this function again with the action DELETE_CONFIRMED to actually delete the process.  Call with LIST to double-check the process_id if you aren't sure that its right.",
            }

        if action == "DELETE_CONFIRMED":
            action = "DELETE"

        if action not in ["CREATE", "DELETE", "UPDATE", "LIST", "SHOW"]:
            return {"Success": False, "Error": "Invalid action specified. Should be CREATE, DELETE, UPDATE, LIST, or SHOW."}

        if action == "LIST":
            logger.info("Running get processes list")
            return self.get_processes_list(bot_id if bot_id is not None else "all")

        if action == "SHOW":
            logger.info("Running show process info")
            if bot_id is None:
                return {"Success": False, "Error": "bot_id is required for SHOW action"}
            if process_id is None:
                if process_details is None or ('process_name' not in process_details and 'process_id' not in process_details):
                    return {"Success": False, "Error": "Either process_name or process_id is required in process_details for SHOW action"}

            if process_id is not None or 'process_id' in process_details:

                return self.get_process_info(bot_id=bot_id, process_id=process_id)
            else:
                process_name = process_details['process_name']
                return self.get_process_info(bot_id=bot_id, process_name=process_name)

        process_id_created = False
        if process_id is None:
            if action == "CREATE":
                process_id = f"{bot_id}_{''.join(random.choices(string.ascii_letters + string.digits, k=6))}"
                process_id_created = True
            else:
                return {"Success": False, "Error": f"Missing process_id field"}

        if action in ["CREATE", "UPDATE"] and not process_details:
            return {
                "Success": False,
                "Error": "Process details must be provided for CREATE or UPDATE action.",
            }

        if action in ["CREATE"] and any(
            field not in process_details for field in required_fields_create
        ):
            missing_fields = [
                field
                for field in required_fields_create
                if field not in process_details
            ]
            return {
                "Success": False,
                "Error": f"Missing required process details: {', '.join(missing_fields)}",
            }

        if action in ["UPDATE"] and any(
            field not in process_details for field in required_fields_update
        ):
            missing_fields = [
                field
                for field in required_fields_update
                if field not in process_details
            ]
            return {
                "Success": False,
                "Error": f"Missing required process details: {', '.join(missing_fields)}",
            }

        if bot_id is None:
            return {
                "Success": False,
                "Error": "The 'bot_id' field is required."
            }

        try:
            if action == "CREATE":
                insert_query = f"""
                    INSERT INTO {db_adapter.schema}.PROCESSES (
                        created_at, updated_at, process_id, bot_id, process_name, process_instructions
                    ) VALUES (
                        current_timestamp(), current_timestamp(), %(process_id)s, %(bot_id)s, %(process_name)s, %(process_instructions)s
                    )
                """ if db_adapter.schema else f"""
                    INSERT INTO PROCESSES (
                        created_at, updated_at, process_id, bot_id, process_name, process_instructions
                    ) VALUES (
                        current_timestamp(),current_timestamp(), %(process_id)s, %(bot_id)s, %(process_name)s, %(process_instructions)s
                    )
                """

                # Generate 6 random alphanumeric characters
                if process_id_created == False:
                    random_suffix = "".join(
                    random.choices(string.ascii_letters + string.digits, k=6)
                     )
                    process_id_with_suffix = process_id + "_" + random_suffix
                else:
                    process_id_with_suffix = process_id
                cursor.execute(
                    insert_query,
                    {
                        **process_details,
                        "process_id": process_id_with_suffix,
                        "bot_id": bot_id,
                    },
                )
                # Get process_name from process_details if available, otherwise set to "Unknown"
                process_name = process_details.get('process_name', "Unknown")
                db_adapter.client.commit()
                return {
                    "Success": True,
                    "Message": f"process successfully created.",
                    "process_id": process_id_with_suffix,
                    "process_name": process_name,
                    "Suggestion": "Now that the process is created, remind the user of the process_id and process_name, and offer to test it using run_process, and if there are any issues you can later on UPDATE the process using manage_processes to clarify anything needed.  OFFER to test it, but don't just test it unless the user agrees.  Also OFFER to schedule it to run on a scheduled basis, using _process_scheduler if desired.",
                    "Reminder": "If you are asked to test the process, use _run_process function to each step, don't skip ahead since you already know what the steps are, pretend you don't know what the process is and let run_process give you one step at a time!",
                }

            elif action == "DELETE":
                fetch_process_name_query = f"""
                    SELECT process_name FROM {db_adapter.schema}.PROCESSES
                    WHERE process_id = %s
                    """ if db_adapter.schema else """
                    SELECT process_name FROM PROCESSES
                    WHERE process_id = %s
                    """
                cursor.execute(fetch_process_name_query, (process_id,))
                result = cursor.fetchone()

                if result:
                    process_name = result[0]
                    delete_query = f"""
                        DELETE FROM {db_adapter.schema}.PROCESSES
                        WHERE process_id = %s
                    """ if db_adapter.schema else f"""
                        DELETE FROM PROCESSES
                        WHERE process_id = %s
                    """
                    cursor.execute(delete_query, (process_id))

                    delete_task_queries = f"""
                        DELETE FROM {db_adapter.schema}.TASKS
                        WHERE task_name = %s
                    """ if db_adapter.schema else """
                        DELETE FROM TASKS
                        WHERE task_name = %s
                    """
                    cursor.execute(delete_task_queries, (process_name,))
                    db_adapter.client.commit()

                    return {
                        "Success": True,
                        "Message": f"process deleted",
                        "process_id": process_id,
                    }
                else:
                    return {
                        "Success": False,
                        "Error": f"process with process_id {process_id} not found",
                    }

            elif action == "UPDATE":
                update_query = f"""
                    UPDATE {db_adapter.schema}.PROCESSES
                    SET {', '.join([f"{key} = %({key})s" for key in process_details.keys()])},
                    updated_at = current_timestamp()
                    WHERE process_id = %(process_id)s
                """ if db_adapter.schema else f"""
                    UPDATE PROCESSES
                    SET {', '.join([f"{key} = %({key})s" for key in process_details.keys()])},
                    updated_at = current_timestamp()
                    WHERE process_id = %(process_id)s
                """
                cursor.execute(
                    update_query,
                    {**process_details, "process_id": process_id},
                )
                db_adapter.client.commit()
                return {
                    "Success": True,
                    "Message": f"process successfully updated",
                    "process_id": process_id,
                    "Suggestion": "Now that the process is updated, offer to test it using run_process, and if there are any issues you can later on UPDATE the process again using manage_processes to clarify anything needed.  OFFER to test it, but don't just test it unless the user agrees.",
                    "Reminder": "If you are asked to test the process, use _run_process function to each step, don't skip ahead since you already know what the steps are, pretend you don't know what the process is and let run_process give you one step at a time!",
                }

            return {"Success": True, "Message": f"process update or delete confirmed."}
        except Exception as e:
            return {"Success": False, "Error": str(e)}

        finally:
            cursor.close()

    def insert_process_history(
        self,
        process_id,
        work_done_summary,
        process_status,
        updated_process_learnings,
        report_message="",
        done_flag=False,
        needs_help_flag="N",
        process_clarity_comments="",
    ):
        """
        Inserts a row into the PROCESS_HISTORY table.

        Args:
            process_id (str): The unique identifier for the process.
            work_done_summary (str): A summary of the work done.
            process_status (str): The status of the process.
            updated_process_learnings (str): Any new learnings from the process.
            report_message (str): The message to report about the process.
            done_flag (bool): Flag indicating if the process is done.
            needs_help_flag (bool): Flag indicating if help is needed.
            process_clarity_comments (str): Comments on the clarity of the process.
        """
        db_adapter = self.db_adapter
        insert_query = f"""
            INSERT INTO {db_adapter.schema}.PROCESS_HISTORY (
                process_id, work_done_summary, process_status, updated_process_learnings,
                report_message, done_flag, needs_help_flag, process_clarity_comments
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s
            )
        """ if db_adapter.schema else f"""
            INSERT INTO PROCESS_HISTORY (
                process_id, work_done_summary, process_status, updated_process_learnings,
                report_message, done_flag, needs_help_flag, process_clarity_comments
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s
            )
        """
        try:
            cursor = db_adapter.client.cursor()
            cursor.execute(
                insert_query,
                (
                    process_id,
                    work_done_summary,
                    process_status,
                    updated_process_learnings,
                    report_message,
                    done_flag,
                    needs_help_flag,
                    process_clarity_comments,
                ),
            )
            db_adapter.client.commit()
            cursor.close()
            logger.info(
                f"Process history row inserted successfully for process_id: {process_id}"
            )
        except Exception as e:
            logger.info(f"An error occurred while inserting the process history row: {e}")
            if cursor is not None:
                cursor.close()

    # ====== PROCESSES END ====================================================================================

def get_tools(which_tools, db_adapter, slack_adapter_local=None, include_slack=True, tool_belt=None) -> tuple[list, dict, dict]:

    tools = []
    available_functions_load = {}
    function_to_tool_map = {}
    if "autonomous_functions" in which_tools and "autonomous_tools" not in which_tools:
        which_tools = [
            tool if tool != "autonomous_functions" else "autonomous_tools"
            for tool in which_tools
        ]
    which_tools = [tool for tool in which_tools if tool != "autonomous_functions"]
    for tool in which_tools:
        try:
            tool_name = tool.get("tool_name")
        except:
            tool_name = tool

        if False:  # tool_name == 'integration_tools':
            tools.extend(integration_tool_descriptions)
            available_functions_load.update(integration_tools)
            function_to_tool_map[tool_name] = integration_tool_descriptions
        elif tool_name == "google_drive_tools":
            tools.extend(google_drive_functions)
            available_functions_load.update(google_drive_tools)
            function_to_tool_map[tool_name] = google_drive_functions
        elif tool_name == "bot_dispatch_tools":
            tools.extend(BOT_DISPATCH_DESCRIPTIONS)
            available_functions_load.update(bot_dispatch_tools)
            function_to_tool_map[tool_name] = BOT_DISPATCH_DESCRIPTIONS
        elif tool_name == "manage_tests_tools":
            tools.extend(manage_tests_functions)
            available_functions_load.update(manage_tests_tools)
            function_to_tool_map[tool_name] = manage_tests_functions
        elif tool_name == "data_dev_tools":
            tools.extend(data_dev_tools_functions)
            available_functions_load.update(data_dev_tools)
        elif tool_name == "project_manager_tools" or tool_name == "todo_manager_tools":
            tools.extend(PROJECT_MANAGER_FUNCTIONS)
            available_functions_load.update(project_manager_tools)
            function_to_tool_map[tool_name] = PROJECT_MANAGER_FUNCTIONS
        elif include_slack and tool_name == "slack_tools":
            tools.extend(slack_tools_descriptions)
            available_functions_load.update(slack_tools)
            function_to_tool_map[tool_name] = slack_tools_descriptions
        elif tool_name == "harvester_tools":
            tools.extend(harvester_tools_functions)
            available_functions_load.update(harvester_tools_list)
            function_to_tool_map[tool_name] = harvester_tools_functions
        elif tool_name == "make_baby_bot":
            tools.extend(MAKE_BABY_BOT_DESCRIPTIONS)
            available_functions_load.update(make_baby_bot_tools)
            function_to_tool_map[tool_name] = MAKE_BABY_BOT_DESCRIPTIONS
        elif tool_name == "bot_dispatch":
            tools.extend(BOT_DISPATCH_DESCRIPTIONS)
            available_functions_load.update(bot_dispatch_tools)
            function_to_tool_map[tool_name] = BOT_DISPATCH_DESCRIPTIONS
        elif tool_name == "database_tools":
            connection_info = {"Connection_Type": db_adapter.connection_info}
            tools.extend(database_tool_functions)
            available_functions_load.update(database_tools)
            run_query_f = bind_run_query([connection_info])
            search_metadata_f = bind_search_metadata("./kb_vector")
            search_metadata_detailed_f = bind_search_metadata_detailed("./kb_vector")
            semantic_copilot_f = bind_semantic_copilot([connection_info])
            function_to_tool_map[tool_name] = database_tool_functions
        elif tool_name == "image_tools":
            tools.extend(image_functions)
            available_functions_load.update(image_tools)
            function_to_tool_map[tool_name] = image_functions
        #    elif tool_name == "snowflake_semantic_tools":
        #        logger.info('Note: Semantic Tools are currently disabled pending refactoring or removal.')
        #        tools.extend(snowflake_semantic_functions)
        #        available_functions_load.update(snowflake_semantic_tools)
        #        function_to_tool_map[tool_name] = snowflake_semantic_functions
        elif tool_name == "snowflake_stage_tools":
            tools.extend(snowflake_stage_functions)
            available_functions_load.update(snowflake_stage_tools)
            function_to_tool_map[tool_name] = snowflake_stage_functions
        elif tool_name == "autonomous_tools" or tool_name == "autonomous_functions":
            tools.extend(autonomous_functions)
            available_functions_load.update(autonomous_tools)
            function_to_tool_map[tool_name] = autonomous_functions
        elif tool_name == "process_runner_tools":
            tools.extend(process_runner_functions)
            available_functions_load.update(process_runner_tools)
            function_to_tool_map[tool_name] = process_runner_functions
        elif tool_name == "process_manager_tools":
            tools.extend(process_manager_functions)
            available_functions_load.update(process_manager_tools)
            function_to_tool_map[tool_name] = process_manager_functions
        elif tool_name == "process_scheduler_tools":
            tools.extend(process_scheduler_functions)
            available_functions_load.update(process_scheduler_tools)
            function_to_tool_map[tool_name] = process_scheduler_functions
        elif tool_name == "notebook_manager_tools":
            tools.extend(notebook_manager_functions)
            available_functions_load.update(notebook_manager_tools)
            function_to_tool_map[tool_name] = notebook_manager_functions
        elif tool_name == "git_file_manager_tools":  # Add this section
            tools.extend(git_file_manager_functions)
            available_functions_load.update(git_file_manager_tools)
            function_to_tool_map[tool_name] = git_file_manager_functions
        elif tool_name == "webpage_downloader":
            tools.extend(webpage_downloader_functions)
            available_functions_load.update(webpage_downloader_tools)
            function_to_tool_map[tool_name] = webpage_downloader_functions
        else:
            try:
                module_path = "generated_modules." + tool_name
                desc_func = "TOOL_FUNCTION_DESCRIPTION_" + tool_name.upper()
                functs_func = tool_name.lower() + "_action_function_mapping"
                module = __import__(module_path, fromlist=[desc_func, functs_func])
                # here's how to get the function for generated things even new ones...
                func = [getattr(module, desc_func)]
                tools.extend(func)
                function_to_tool_map[tool_name] = func
                func_af = getattr(module, functs_func)
                available_functions_load.update(func_af)
            except:
                logger.warn(f"Functions for tool '{tool_name}' could not be found.")

    available_functions = {}
    for name, full_func_name in available_functions_load.items():
        if callable(full_func_name):
            available_functions[name] = full_func_name
        else:
            module_path, func_name = full_func_name.rsplit(".", 1)
            if module_path in locals():
                module = locals()[module_path]
                try:
                    func = getattr(module, func_name)
                except:
                    func = module
                # logger.info("existing local: ",func)
            elif module_path in globals():
                module = globals()[module_path]
                try:
                    func = getattr(module, func_name)
                except:
                    func = module
                # logger.info("existing global: ",func)
            else:
                module = __import__(module_path, fromlist=[func_name])
                func = getattr(module, func_name)
                # logger.info("imported: ",func)
            available_functions[name] = func
    # Insert additional code here if needed

    # add user extended tools
    user_extended_tools_definitions, user_extended_functions = load_user_extended_tools(db_adapter, project_id=global_flags.project_id,
                                                                                        dataset_name=global_flags.genbot_internal_project_and_schema.split(".")[1])
    if user_extended_functions:
        tools.extend(user_extended_functions)
        available_functions_load.update(user_extended_tools_definitions)
        function_to_tool_map[tool_name] = user_extended_functions

    return tools, available_functions, function_to_tool_map
    # logger.info("imported: ",func)


class BotOsDispatchInputAdapter(BotOsInputAdapter):
    def __init__(self, bot_id) -> None:
        bot_config = get_bot_details(bot_id=bot_id)
        self.session = make_session_for_dispatch(bot_config)
        self.tasks = {}

    # allows for polling from source
    def add_event(self, event):
        pass

    # allows for polling from source
    def get_input(self, thread_map=None, active=None, processing=None, done_map=None):
        pass

    # allows response to be sent back with optional reply
    def handle_response(
        self,
        session_id: str,
        message: BotOsOutputMessage,
        in_thread=None,
        in_uuid=None,
        task_meta=None,
    ):
        if message.status == "completed":
            self.tasks[message.thread_id]["result"] = message.output

    def dispatch_task(self, task):
        # thread_id = self.session.add_task(task, self)
        thread_id = self.session.create_thread(self)
        self.tasks[thread_id] = {"task": task, "result": None}
        self.session.add_message(BotOsInputMessage(thread_id=thread_id, msg=task))

    def check_tasks(self):
        self.session.execute()
        if all(task["result"] is not None for task in self.tasks.values()):
            return [task["result"] for task in self.tasks.values()]
        else:
            return False


def dispatch_to_bots(task_template, args_array, dispatch_bot_id=None):
    """
    Dispatches a task to multiple bots, each instantiated by creating a new thread with a specific task.
    The task is created by filling in the task template with arguments from the args_array using Jinja templating.

    Args:
        task_template (str): A natural language task template using Jinja templating.
        args_array (list of dict): An array of dictionaries to plug into the task template for each bot.

    Returns:
        list: An array of responses.
    """

    if len(args_array) < 2:
        return "Error: args_array size must be at least 2."

    template = Template(task_template)
    adapter = BotOsDispatchInputAdapter(bot_id=dispatch_bot_id)

    for s_args in args_array:
        # Fill in the task template with the current arguments
        args = json.loads(s_args)
        task = template.render(**args)
        adapter.dispatch_task(task)

    while True:
        responses = adapter.check_tasks()
        if responses:
            logger.info(f"dispatch_to_bots - {responses}")
            return responses
        time.sleep(1)


BOT_DISPATCH_DESCRIPTIONS = [
    {
        "type": "function",
        "function": {
            "name": "_delegate_work",
            "description": "Delegates a task to another bot (or self) and waits for a JSON response. Use this when you need to delegate work to another bot.",
            "parameters": {
                "type": "object",
                "properties": {
                    "prompt": {
                        "type": "string",
                        "description": "The instruction or prompt to send to the target bot"
                    },
                    "target_bot": {
                        "type": "string",
                        "description": "Bot ID or name of target bot to delegate to. If you dont know the exact ID or name, call with target_bot_id 'UNKNOWN' to get a list of active bots."
                    },
                    "max_retries": {
                        "type": "integer",
                        "description": "Maximum number of retry attempts (1-10), defaults to 3",
                        "minimum": 1,
                        "maximum": 10,
                        "default": 3
                    },
                    "timeout_seconds": {
                        "type": "integer",
                        "description": "Maximum seconds to wait for response, defaults to 300",
                        "minimum": 1,
                        "default": 300
                    }
                    ,
                    "callback_id": {
                        "type": "string",
                        "description": "Optional callback_id to continue a previous delegation thread. Use this if you need to follow up on a previous delegation, by providing the callback_id that you received in the response to the previous delegation. If not used, a new thread will be started with the target_bot for this delegation.",
                        "default": None
                    }
                },
                "required": ["prompt"]  # Only prompt is required, others have defaults
            }
        }
    }
]

bot_dispatch_tools = {"_delegate_work": "tool_belt.delegate_work"}


def make_session_for_dispatch(bot_config):
    input_adapters = []
    bot_tools = json.loads(bot_config["available_tools"])

    # Create a DB connector for this session.
    # TODO - use the utility functions in the connectors module to DRY up.
    genesis_source = os.getenv("GENESIS_SOURCE", default="BigQuery")

    if genesis_source == "BigQuery":
        credentials_path = os.getenv(
            "GOOGLE_APPLICATION_CREDENTIALS", default=".secrets/gcp.json"
        )
        with open(credentials_path) as f:
            connection_info = json.load(f)
        # Initialize BigQuery client
        db_adapter = BigQueryConnector(connection_info, "BigQuery")
    else:  # Initialize BigQuery client
        db_adapter = SnowflakeConnector(connection_name="Snowflake")
        connection_info = {"Connection_Type": "Snowflake"}

    logger.info("---> CONNECTED TO DATABASE: ", genesis_source)
    tools, available_functions, function_to_tool_map = get_tools(
        bot_tools, db_adapter, include_slack=False
    )  # FixMe remove slack adapter if

    instructions = (
        bot_config["bot_instructions"] + "\n" + BASE_BOT_INSTRUCTIONS_ADDENDUM
    )
    logger.info(instructions, f'{bot_config["bot_name"]}, id: {bot_config["bot_id"]}')

    # TESTING UDF ADAPTER W/EVE and ELSA
    # add a map here to track botid to adapter mapping

    bot_id = bot_config["bot_id"]
    if os.getenv("BOT_DO_PLANNING_REFLECTION"):
        pre_validation = BASE_BOT_PRE_VALIDATION_INSTRUCTIONS
        post_validation = BASE_BOT_VALIDATION_INSTRUCTIONS
    else:
        pre_validation = ""
        post_validation = None
    if os.getenv("BOT_BE_PROACTIVE", "False").lower() == "true":
        proactive_instructions = BASE_BOT_PROACTIVE_INSTRUCTIONS
    else:
        proactive_instructions = ""
    session = BotOsSession(
        bot_config["bot_id"],
        instructions=instructions + proactive_instructions + pre_validation,
        validation_instructions=post_validation,
        input_adapters=input_adapters,
        knowledgebase_implementation=BotOsKnowledgeAnnoy_Metadata(
            f"./kb_{bot_config['bot_id']}"
        ),
        file_corpus=(
            URLListFileCorpus(json.loads(bot_config["files"]))
            if bot_config["files"]
            else None
        ),
        log_db_connector=db_adapter,  # Ensure connection_info is defined or fetched appropriately
        tools=tools,
        available_functions=available_functions,
        all_tools=tools,
        all_functions=available_functions,
        all_function_to_tool_map=function_to_tool_map,
        bot_id=bot_config["bot_id"],
    )
    # if os.getenv("BOT_BE_PROACTIVE").lower() == "true" and slack_adapter_local:
    #      session.add_task("Check in with Michael Gold to see if he has any tasks for you to work on.",
    ##                       thread_id=session.create_thread(slack_adapter_local))
    #                       input_adapter=slack_adapter_local))

    return session  # , api_app_id, udf_adapter_local, slack_adapter_local
