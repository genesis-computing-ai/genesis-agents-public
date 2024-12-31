from   bs4                      import BeautifulSoup
from   datetime                 import datetime
import json
import jsonschema
import os
import random
import requests
import string
import threading
import time
from   typing                   import Any, Callable, Dict, List
from   urllib.parse             import urlencode, urljoin, urlunparse
import uuid
from jinja2 import Template

from core.bot_os_artifacts import (
    ARTIFACT_ID_REGEX,
    get_artifacts_store,
    lookup_artifact_markdown,
)

from   connectors.bigquery_connector \
                                import BigQueryConnector
from   core                     import global_flags
from   core.bot_os_tools2       import (ToolFuncDescriptor,
                                        get_global_tools_registry,
                                        get_tool_func_descriptor)
from   core.bot_os_tools_extended \
                                import load_user_extended_tools
from   llm_openai.bot_os_openai import StreamingEventHandler

from   google_sheets.g_sheets   import (add_g_file_comment,
                                        add_reply_to_g_file_comment,
                                        find_g_file_by_name,
                                        get_g_file_comments,
                                        get_g_file_version,
                                        get_g_file_web_link,
                                        get_g_folder_directory, read_g_sheet,
                                        write_g_sheet_cell)

import collections
import re
from   typing                   import Optional

from   bot_genesis.make_baby_bot \
                                import (MAKE_BABY_BOT_DESCRIPTIONS,
                                        get_bot_details, make_baby_bot_tools)
# from connectors import get_global_db_connector
# from connectors.bigquery_connector import BigQueryConnector
from connectors.database_tools import (
    autonomous_functions,
    autonomous_tools,
    image_functions,
    image_tools,
)

from   development.integration_tools \
                                import (integration_tool_descriptions,
                                        integration_tools)
from   llm_openai.openai_utils  import get_openai_client
from   schema_explorer.harvester_tools \
                                import (harvester_tools_functions,
                                        harvester_tools_list)
from   slack.slack_tools        import slack_tools, slack_tools_descriptions

# Commented out gut left for reference: dagster tools are 'new type' tools - registred with the ToolsFuncRegistry
#
# from data_pipeline_tools.gc_dagster import (
#     dagster_tool_functions,
#     dagster_tools,
# )

from   core.bot_os              import BotOsSession
from   core.bot_os_corpus       import URLListFileCorpus
from   core.bot_os_defaults     import (BASE_BOT_INSTRUCTIONS_ADDENDUM,
                                        BASE_BOT_PRE_VALIDATION_INSTRUCTIONS,
                                        BASE_BOT_PROACTIVE_INSTRUCTIONS,
                                        BASE_BOT_VALIDATION_INSTRUCTIONS)
from   core.bot_os_input        import BotOsInputAdapter
from   core.bot_os_memory       import BotOsKnowledgeAnnoy_Metadata

from   core.bot_os_input        import BotOsInputMessage, BotOsOutputMessage

# import sys
# sys.path.append('/Users/mglickman/helloworld/bot_os')  # Adjust the path as necessary


from   core.bot_os_llm          import BotLlmEngineEnum
from   core.bot_os_tool_descriptions \
                                import (PROJECT_MANAGER_FUNCTIONS,
                                        data_dev_tools,
                                        data_dev_tools_functions,
                                        git_file_manager_functions,
                                        git_file_manager_tools,
                                        process_runner_functions,
                                        process_runner_tools,
                                        project_manager_tools,
                                        webpage_downloader_functions,
                                        webpage_downloader_tools)

from   connectors.snowflake_connector.snowflake_connector \
                                import SnowflakeConnector
from   core.bot_os_project_manager \
                                import ProjectManager
from   core.file_diff_handler   import GitFileManager
from   core.logging_config      import logger

from core.bot_os_tools2 import (
    BOT_ID_IMPLICIT_FROM_CONTEXT,
    THREAD_ID_IMPLICIT_FROM_CONTEXT,
    ToolFuncGroup,
    ToolFuncParamDescriptor,
    gc_tool,
)

from textwrap import dedent

from .tools.run_processes import run_process
from .tools.manage_processes import manage_processes
from .tools.manage_notebook import manage_notebook
from .tools.manage_tests import manage_tests
from .tools.google_drive import google_drive
from .tools.process_scheduler import process_scheduler
from .tools.send_email import send_email


genesis_source = os.getenv("GENESIS_SOURCE", default="Snowflake")

class ToolBelt:
    _instance = None  # Class variable to hold the single instance

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(ToolBelt, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        from connectors import get_global_db_connector # to avoid circular import
        if not hasattr(self, '_initialized'):  # Check if already initialized
            self.db_adapter = get_global_db_connector()
            self.connections = {}  # Store SQLAlchemy engines
            self._ensure_tables_exist()
            self._initialized = True  # Mark as initialized

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
            self.process_id = {}
            self.include_code = False

            #  if genesis_source == 'Sqlite':
            #      self.db_adapter = SqliteConnector(connection_name="Sqlite")
            #      connection_info = {"Connection_Type": "Sqlite"}
            #  elif genesis_source == 'Snowflake':  # Initialize Snowflake client

            if os.getenv("SQLITE_OVERRIDE", "").lower() == "true":
                from connectors import get_global_db_connector
                self.db_adapter = get_global_db_connector()
            else:
                self.db_adapter = SnowflakeConnector(connection_name="Snowflake")  # always use this for metadata
            connection_info = {"Connection_Type": "Snowflake"}
            # else:
            #     raise ValueError('Invalid Source')

            self.todos = ProjectManager(self.db_adapter)  # Initialize Todos instance
            self.git_manager = GitFileManager()
            self.server = None  # Will be set later

            self.sys_default_email = self._get_sys_email()

    def _set_server(self, server):
        """Set the server instance for this toolbelt"""
        self.server = server

    def manage_processes(self, action, process_id, bot_id, thread_id=None):
        return manage_processes(action=action, process_id=process_id, bot_id=bot_id, thread_id=thread_id)

    def run_process(self, process_id, bot_id, thread_id=None):
        return run_process(process_id=process_id, bot_id=bot_id, thread_id=thread_id)

    def manage_notebook(self, action, notebook_id, bot_id, thread_id=None):
        return manage_notebook(action=action, notebook_id=notebook_id, bot_id=bot_id, thread_id=thread_id)

    def manage_tests(self, action, test_id, bot_id, thread_id=None):
        return manage_tests(action=action, test_id=test_id, bot_id=bot_id, thread_id=thread_id)

    def google_drive(self, action, bot_id, thread_id=None):
        return google_drive(action=action, bot_id=bot_id, thread_id=thread_id)

    def process_scheduler(self, action, bot_id, thread_id=None):
        return process_scheduler(action=action, bot_id=bot_id, thread_id=thread_id)

    def send_email(self, action, bot_id, thread_id=None):
        return send_email(action=action, bot_id=bot_id, thread_id=thread_id)

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
                                    summary_response = self._chat_completion(
                                        f"An AI bot is doing work, you are monitoring it. Please summarize in a few words what is happening in this ongoing response from another bot so far.  Be VERY Brief, use just a few words, not even a complete sentence.  Don't put a period on the end if its just one sentence or less.\nHere is the bots output so far:\n\n{response.strip()[:-2]}"
                                    , db_adapter=self.db_adapter, fast=True)
                                else:
                                    summary_response = self._chat_completion(
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
        return self.todos.manage_todos(action=action, bot_id=bot_id, todo_id=todo_id,todo_details=todo_details, thread_id=thread_id)

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
        return self.todos._get_todo_dependencies(bot_id=bot_id, todo_id=todo_id, include_reverse=include_reverse)

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
    def _parse_and_chunk_content(self, content, base_url, chunk_size=256 * 1024):
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
            chunks, total_chunks = self._parse_and_chunk_content(content, url)
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

    def _chat_completion(self, message, db_adapter, bot_id = None, bot_name = None, thread_id=None, process_id="", process_name="", note_id = None, fast=False):
        process_name = "" if process_name is None else process_name
        process_id = "" if process_id is None else process_id
        message_metadata ={"process_id": process_id, "process_name": process_name}
        return_msg = None

        if not fast:
            self._write_message_log_row(db_adapter, bot_id, bot_name, thread_id, 'Supervisor Prompt', message, message_metadata)

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
                response, status_code = db_adapter.cortex__chat_completion(message)
                return_msg = response

        if return_msg is None:
            return_msg = 'Error _chat_completion, return_msg is none, llm_type = ',os.getenv("BOT_OS_DEFAULT_LLM_ENGINE").lower()
            logger.info(return_msg)

        if not fast:
            self._write_message_log_row(db_adapter, bot_id, bot_name, thread_id, 'Supervisor Response', return_msg, message_metadata)

        return return_msg

    def _write_message_log_row(self, db_adapter, bot_id="", bot_name="", thread_id="", message_type="", message_payload="", message_metadata={}):
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

    def _set_process_cache(self, bot_id, thread_id, process_id):
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

    def _get_process_cache(self, bot_id, thread_id, process_id):
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

    def _clear_process_cache(self, bot_id, thread_id, process_id):
        cache_file = os.path.join("./process_cache", f"{bot_id}_{thread_id}_{process_id}.json")

        if os.path.exists(cache_file):
            os.remove(cache_file)
            return True
        return False

    def _get_current_time_with_timezone(self):
        current_time = datetime.now().astimezone()
        return current_time.strftime("%Y-%m-%d %H:%M:%S %Z")

    def _get_sys_email(self):
        cursor = self.db_adapter.client.cursor()
        try:
            _get_sys_email_query = f"SELECT default_email FROM {self.db_adapter.genbot_internal_project_and_schema}.DEFAULT_EMAIL"
            cursor.execute(_get_sys_email_query)
            result = cursor.fetchall()
            default_email = result[0][0] if result else None
            return default_email
        except Exception as e:
            #  logger.info(f"Error getting sys email: {e}")
            return None

    def _clear_process_registers_by_thread(self, thread_id):
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

    def _clear_all_process_registers(self, thread_id):
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

    def _get_processes_list(self, bot_id="all"):
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

    def _get_process_info(self, bot_id=None, process_name=None, process_id=None):
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

    def _insert_process_history(
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

# ==================================================================================================================

tool_belt_tools = ToolFuncGroup(
    name="tool_belt_tools",
    description="Tools for managing processes, notes, tests, process scheduling, google drive, artifacts, todos, and more.",
    lifetime="PERSISTENT"
)

@gc_tool(
    action=ToolFuncParamDescriptor(
        name="action",
        description="Action to perform (CREATE, UPDATE, CHANGE_STATUS, LIST)",
        required=True,
        llm_type_desc=dict(
            type="string", enum=["CREATE", "UPDATE", "CHANGE_STATUS", "LIST"]
        ),
    ),
    bot_id="ID of the bot performing the action",
    todo_id="ID of the todo item (required for UPDATE and CHANGE_STATUS)",
    todo_details=ToolFuncParamDescriptor(
        name="todo_details",
        description="Details for the todo item. For CREATE: requires project_id, todo_name, what_to_do, depends_on. "
        "For CHANGE_STATUS: requires only new_status.",
        llm_type_desc=dict(
            type="object",
            properties=dict(
                project_id=dict(
                    type="string",
                    description="ID of the project this todo belongs to (required for CREATE)",
                ),
                todo_name=dict(type="string", description="Name of the todo item"),
                what_to_do=dict(
                    type="string", description="Description of what needs to be done"
                ),
                assigned_to_bot_id=dict(
                    type="string",
                    description="The bot_id (not just the name) of the bot assigned to this todo. Omit to assign it to yourself.",
                ),
                depends_on=dict(
                    type="string",
                    description="ID or array of IDs of todos that this todo depends on",
                ),
                new_status=dict(
                    type=["string", "array", "null"],
                    description="New status for the todo (required for CHANGE_STATUS)",
                    enum=["NEW", "IN_PROGRESS", "ON_HOLD", "COMPLETED", "CANCELLED"],
                ),
            ),
        ),
        required=False,
    ),
    _group_tags_=[tool_belt_tools],
)
def _manage_todos(
        action: str,
        todo_id: str,
        todo_details: dict,
        bot_id: str,
        allowed_bot_ids: list[str] = None,
        thread_id: str = None,
        _group_tags_=[tool_belt_tools],
        ) -> dict:
    """
    Manages todos through various actions (CREATE, UPDATE, CHANGE_STATUS, LIST)

    Returns:
        dict: A dictionary containing the result of the connection addition.
    """
    return ToolBelt().manage_todos(
        action=action,
        todo_id=todo_id,
        todo_details=todo_details,
        bot_id=bot_id,
        allowed_bot_ids=allowed_bot_ids,
        thread_id=thread_id
    )


@gc_tool(
    action=dedent(
        """The action to perform on a process: CREATE, UPDATE, DELETE, CREATE_PROCESS_CONFIG, UPDATE_PROCESS_CONFIG, DELETE_PROCESS_CONFIG, ALLOW_CODE, HIDE_PROCESS, UNHIDE_PROCESS
            LIST returns a list of all processes, SHOW shows full instructions and details for a process, SHOW_CONFIG shows the configuration for a process,
            HIDE_PROCESS hides the process from the list of processes, UNHIDE_PROCESS unhides the process from the list of processes,
            or TIME to get current system time.  If you are trying to deactivate a schedule for a task, use _process_scheduler instead, don't just DELETE the process.
            ALLOW_CODE is used to bypass the restriction that code must be added as a note"""
    ),
    process_id=dedent(
        """The unique identifier of the process, create as bot_id_<random 6 character string>. MAKE SURE TO DOUBLE-CHECK THAT YOU ARE USING THE CORRECT process_id ON UPDATES AND DELETES!  
            Required for CREATE, UPDATE, and DELETE."""
    ),
    process_name="The name of the process.  Required for SHOW.",
    process_instructions="DETAILED instructions for completing the process  Do NOT summarize or simplify instructions provided by a user.",
    process_config="Configuration string used by process when running.",
    hidden="If true, the process will not be shown in the list of processes.  This is used to create processes to test the bots functionality without showing them to the user.",
    bot_id=BOT_ID_IMPLICIT_FROM_CONTEXT,
    thread_id=THREAD_ID_IMPLICIT_FROM_CONTEXT,
    _group_tags_=[tool_belt_tools]
)
# required fields - "action", "bot_id", "process_instructions"
def _manage_processes(
        action: str,
        process_id: str,
        process_name: str,
        process_instructions: str,
        process_config: str,
        bot_id: str,
        thread_id: str,
        _group_tags_=[tool_belt_tools],
        ) -> dict:
    """
    Manages processes for bots, including creating, updating, listing and deleting processes allowing bots to manage processes.
    Remember that this is not used to create new bots

    Returns:
        dict: A dictionary containing the result of the operation.
    """
    return ToolBelt().manage_processes(
        action=action,
        process_id=process_id,
        process_name=process_name,
        process_instructions=process_instructions,
        process_config=process_config,
        bot_id=bot_id,
        thread_id=thread_id
    )


@gc_tool(
    action=dedent(
        """The action to perform on the process schedule: CREATE, UPDATE, or DELETE.  Or LIST to get details on all 
                      scheduled processes for a bot, or TIME to get current system time or HISTORY to get the history of a scheduled 
                      process by task_id.  For history lookup task_id first using LIST.  To deactive a schedule without deleting it, 
                      UPDATE it and set task_active to FALSE."""
    ),
    bot_id="BOT_ID_IMPLICIT_FROM_CONTEXT",
    task_id=dedent(
        """The unique identifier of the process schedule, create as bot_id_<random 6 character string>. MAKE SURE TO 
                       DOUBLE-CHECK THAT YOU ARE USING THE CORRECT task_id, its REQUIRED ON CREATE, UPDATES AND DELETES! Note that this 
                       is not the same as the process_id"""
    ),
    task_details=ToolFuncParamDescriptor(
        name="task_details",
        description="The properties of this object are the details of the process schedule for use when creating and updating.",
        llm_type_desc=dict(
            type="object",
            properties=dict(
                process_name=dict(
                    type="string",
                    description="The name of the process to run on a schedule. This must be a valid process name shown by _manage_processes LIST",
                ),
                primary_report_to_type=dict(
                    type="string",
                    description="Set to SLACK_USER",
                ),
                primary_report_to_id=dict(
                    type="string",
                    description="The Slack USER ID of the person who told you to create this schedule for a process."
                ),
                next_check_ts=dict(
                    type="string",
                    description="The timestamp for the next run of the process 'YYYY-MM-DD HH:MM:SS'. Call action TIME to get current time and timezone. Make sure this time is in the future.",
                ),
                action_trigger_type=dict(
                    type="string",
                    description="Always set to TIMER",
                ),
                action_trigger_details=dict(
                    type="string",
                    description="""For TIMER, a description of when to call the task, eg every hour, Tuesdays at 9am, every morning.  Also be clear about whether the task should be called one time, or is recurring, and if recurring if it should recur forever or stop at some point.""",
                ),
            ),
        ),
        required=False,
    ),
    history_rows=10,
    thread_id="THREAD_ID_IMPLICIT_FROM_CONTEXT",
    _group_tags_=[tool_belt_tools],
)
def _process_scheduler(
        action: str,
        bot_id: str,
        task_id: str,
        task_details: str,
        thread_id: str,
        history_rows: int = 10,
    ):
    """
    Manages schedules to automatically run processes on a schedule (sometimes called tasks), including creating, updating,
    and deleting schedules for processes.
    """
    return ToolBelt().process_scheduler(
        action=action,
        bot_id=bot_id,
        task_id=task_id,
        task_details=task_details,
        history_rows=history_rows,
        thread_id=thread_id
    )


@gc_tool(
    action=dedent("""
        The action to be performed on Google Drive.  Possible actions are:
            LOGIN - Used to login in to Google Workspace with OAuth2.0.  Not implemented
            LIST - Get's list of files in a folder.  Same as DIRECTORY, DIR, GET FILES IN FOLDER
            SET_ROOT_FOLDER - Sets the root folder for the user on their drive
            GET_FILE_VERSION_NUM - Gets the version numbergiven a g_file id
            GET_COMMENTS - Gets the comments and replies for a file give a g_file_id
            ADD_COMMENT - Adds a comment to a file given a g_file_id
            ADD_REPLY_TO_COMMENT - Adds a reply to a comment given a g_file_id and a comment_id
            GET_SHEET - (Also can be READ_SHEET) - Gets the contents of a Google Sheet given a g_file_id
            EDIT_SHEET - (Also can be WRITE SHEET) - Edits a Google Sheet given a g_file_id and values.  Passing
                a cell range is optional
            GET_LINK_FROM_FILE_ID - Gets the url link to a file given a g_file_id
            GET_FILE_BY_NAME - Searches for a file by name and returns the file id
            SAVE_QUERY_RESULTS_TO_G_SHEET - Saves the results of a query to a Google Sheet
    """),
    g_folder_id="The unique identifier of a folder stored on Google Drive.",
    g_file_id="The unique identifier of a file stored on Google Drive.",
    g_sheet_cell="Cell in a Google Sheet to edit/update.",
    g_sheet_value="Value to update the cell in a Google Sheet or update a comment.",
    g_file_comment_id="The unique identifier of a comment stored on Google Drive.",
    g_file_name="The name of a file, files, folder, or folders stored on Google Drive.",
    g_sheet_query="Query string to run and save the results to a Google Sheet.",
    user="""The unique identifier of the process_id. MAKE SURE TO DOUBLE-CHECK THAT YOU ARE USING THE CORRECT test_process_id 
        ON UPDATES AND DELETES!  Required for CREATE, UPDATE, and DELETE.""",
    thread_id="THREAD_ID_IMPLICIT_FROM_CONTEXT",
    _group_tags_=[tool_belt_tools]
)
def _google_drive(
    action: str,
    g_folder_id: str,
    g_file_id: str,
    g_sheet_cell: str,
    g_sheet_value: str,
    g_file_comment_id: str,
    g_file_name: str,
    g_sheet_query: str,
    user: str,
    thread_id: str,
):
    """
    Performs certain actions on Google Drive, including logging in, listing files, setting the root folder,
    and getting the version number of a google file (g_file).

    Args:
        action (str): The action to perform on the Google Drive files. Supported actions are 'LIST' and 'DOWNLOAD'.

    Returns:
        dict: A dictionary containing the result of the action. E.g. for 'LIST', it includes the list of files in the Google Drive.
    """
    return ToolBelt().google_drive(
        action=action,
        g_folder_id=g_folder_id,
        g_file_id=g_file_id,
        g_sheet_cell=g_sheet_cell,
        g_sheet_value=g_sheet_value,
        g_file_comment_id=g_file_comment_id,
        g_file_name=g_file_name,
        g_sheet_query=g_sheet_query,
        user=user,
        thread_id=thread_id
    )
# ==================================================================================================================

def get_tools(
    which_tools: list[str],
    db_adapter,
    slack_adapter_local=None,
    include_slack: bool = True,
    tool_belt=None
    ) -> tuple[list, dict, dict]:
    """
    Retrieve a list of tools (function groups), available functions, and a mapping of functions to tools based on the specified tool names.

    Args:
        which_tools (list): A list of tool (function group) names to retrieve.
        db_adapter: The database adapter to use (some functions we methods of db_adapter).
        slack_adapter_local: The Slack adapter to use for Slack operations (optional).
        include_slack (bool): Whether to include Slack tools (default is True).
        tool_belt: An optional tool belt instance to use.

    Returns:
        tuple: A tuple containing three elements:
            - list of dicts: A list of function descriptions
            - dict: A dictionary mapping function names to their implementations (callable objects).
            - dict: A dictionary mapping tool (group) names to a list of function descriptors (dicts) for this tool (group)
    """
    func_descriptors = []
    available_functions_loaded = {} # map function_name (str)--> 'locator' (str|callable) ;
    # 'locator' can be a callable or string.
    # If a string, it gets dyanmically evaluated below to the actual callable object
    tool_to_func_descriptors_map = {} # map of tool name to list of function descriptors
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

        # Resolve 'old style' tool names
        # ----------------------------------
        if tool_name == "google_drive_tools":
            func_descriptors.extend(google_drive_functions)
            available_functions_loaded.update(google_drive_tools)
            tool_to_func_descriptors_map[tool_name] = google_drive_functions
        elif tool_name == "bot_dispatch_tools":
            func_descriptors.extend(BOT_DISPATCH_DESCRIPTIONS)
            available_functions_loaded.update(bot_dispatch_tools)
            tool_to_func_descriptors_map[tool_name] = BOT_DISPATCH_DESCRIPTIONS
        elif tool_name == "manage_tests_tools":
            func_descriptors.extend(manage_tests_functions)
            available_functions_loaded.update(manage_tests_tools)
            tool_to_func_descriptors_map[tool_name] = manage_tests_functions
        elif tool_name == "data_dev_tools":
            func_descriptors.extend(data_dev_tools_functions)
            available_functions_loaded.update(data_dev_tools)
        elif tool_name == "project_manager_tools" or tool_name == "todo_manager_tools":
            func_descriptors.extend(PROJECT_MANAGER_FUNCTIONS)
            available_functions_loaded.update(project_manager_tools)
            tool_to_func_descriptors_map[tool_name] = PROJECT_MANAGER_FUNCTIONS
        elif include_slack and tool_name == "slack_tools":
            func_descriptors.extend(slack_tools_descriptions)
            available_functions_loaded.update(slack_tools)
            tool_to_func_descriptors_map[tool_name] = slack_tools_descriptions
        elif tool_name == "harvester_tools":
            func_descriptors.extend(harvester_tools_functions)
            available_functions_loaded.update(harvester_tools_list)
            tool_to_func_descriptors_map[tool_name] = harvester_tools_functions
        elif tool_name == "make_baby_bot":
            func_descriptors.extend(MAKE_BABY_BOT_DESCRIPTIONS)
            available_functions_loaded.update(make_baby_bot_tools)
            tool_to_func_descriptors_map[tool_name] = MAKE_BABY_BOT_DESCRIPTIONS
        elif tool_name == "bot_dispatch":
            func_descriptors.extend(BOT_DISPATCH_DESCRIPTIONS)
            available_functions_loaded.update(bot_dispatch_tools)
            tool_to_func_descriptors_map[tool_name] = BOT_DISPATCH_DESCRIPTIONS
        elif tool_name == "image_tools":
            func_descriptors.extend(image_functions)
            available_functions_loaded.update(image_tools)
            tool_to_func_descriptors_map[tool_name] = image_functions
        elif tool_name == "autonomous_tools" or tool_name == "autonomous_functions":
            func_descriptors.extend(autonomous_functions)
            available_functions_loaded.update(autonomous_tools)
            tool_to_func_descriptors_map[tool_name] = autonomous_functions
        elif tool_name == "process_runner_tools":
            func_descriptors.extend(process_runner_functions)
            available_functions_loaded.update(process_runner_tools)
            tool_to_func_descriptors_map[tool_name] = process_runner_functions
        elif tool_name == "git_file_manager_tools":  # Add this section
            func_descriptors.extend(git_file_manager_functions)
            available_functions_loaded.update(git_file_manager_tools)
            tool_to_func_descriptors_map[tool_name] = git_file_manager_functions
        elif tool_name == "webpage_downloader":
            func_descriptors.extend(webpage_downloader_functions)
            available_functions_loaded.update(webpage_downloader_tools)
            tool_to_func_descriptors_map[tool_name] = webpage_downloader_functions
        else:
            registry = get_global_tools_registry()
            tool_funcs : List[Callable] = registry.get_tool_funcs_by_group(tool_name)
            if tool_funcs:
                descriptors : List(ToolFuncDescriptor) = [get_tool_func_descriptor(func) for func in tool_funcs]
                func_descriptors.extend([descriptor.to_llm_description_dict()
                                        for descriptor in descriptors])
                available_functions_loaded.update({get_tool_func_descriptor(func).name : func
                                                for func in tool_funcs})
                tool_to_func_descriptors_map[tool_name] = [descriptor.to_llm_description_dict()
                                                        for descriptor in descriptors]
            else:
                # Ultimately, fallback to try to load the function data dynamaically from a module named exactly like tool_name
                # ??? is this ever actually used ???
                try:
                    module_path = "generated_modules." + tool_name
                    desc_func = "TOOL_FUNCTION_DESCRIPTION_" + tool_name.upper()
                    functs_func = tool_name.lower() + "_action_function_mapping"
                    module = __import__(module_path, fromlist=[desc_func, functs_func])
                    # here's how to get the function for generated things even new ones...
                    func = [getattr(module, desc_func)]
                    func_descriptors.extend(func)
                    tool_to_func_descriptors_map[tool_name] = func
                    func_af = getattr(module, functs_func)
                    available_functions_loaded.update(func_af)
                except:
                    logger.warn(f"Functions for tool '{tool_name}' could not be found.")

    # Resolve 'old style' tool functions to actual callables
    available_functions = {}
    for name, function_handle in available_functions_loaded.items():
        if callable(function_handle):
            available_functions[name] = function_handle
        else:
            assert isinstance(function_handle, str)
            module_path, func_name = function_handle.rsplit(".", 1)
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
                # Dyanmic imports (e.g. module_path= 'bot_genesis.make_baby_bot')
                module = __import__(module_path, fromlist=[func_name])
                func = getattr(module, func_name)
                # logger.info("imported: ",func)
            available_functions[name] = func
    # Insert additional code here if needed

    # add user extended tools
    user_extended_tools_definitions, user_extended_functions = load_user_extended_tools(db_adapter, project_id=global_flags.project_id,
                                                                                        dataset_name=global_flags.genbot_internal_project_and_schema.split(".")[1])
    if user_extended_functions:
        func_descriptors.extend(user_extended_functions)
        available_functions_loaded.update(user_extended_tools_definitions)
        tool_to_func_descriptors_map[tool_name] = user_extended_functions

    return func_descriptors, available_functions, tool_to_func_descriptors_map
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

# holds the list of all data connection tool functions
# NOTE: Update this list when adding new data connection tools (TODO: automate this by scanning the module?)
_all_tool_belt_functions = (
    _manage_todos,
    _manage_processes,
    _process_scheduler,
    _google_drive,
    _manage_artifact,
    _manage_tests,
    _manage_notebook,
    _send_email,
    _download_webpage,
    _git_action,
    _delegate_work,
    _manage_project_assets,
    _get_project_todos,
    _manage_todo_dependencies,
    _get_todo_dependencies,
    _delegate_work,
    _get_webpage_content,
    _manage_projects
)


# Called from bot_os_tools.py to update the global list of data connection tool functions
def get_tool_belt_functions():
    return _all_tool_belt_functions
