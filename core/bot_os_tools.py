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

from   typing                   import Optional

from   bot_genesis.make_baby_bot \
                                import (MAKE_BABY_BOT_DESCRIPTIONS,
                                        get_bot_details, make_baby_bot_tools)


from connectors.database_tools import (
    autonomous_functions,
    autonomous_tools,
    image_functions,
    image_tools,
)

from   schema_explorer.harvester_tools \
                                import (harvester_tools_functions,
                                        harvester_tools_list)
from   slack.slack_tools        import slack_tools, slack_tools_descriptions

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
from .bot_os_dispatch_input_adapter import BotOsDispatchInputAdapter


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


# git action
@gc_tool(
    action=ToolFuncParamDescriptor(
        name="action",
        description=dedent(
            """The action to perform:
            - list_files: List all tracked files (optional: path)
            - read_file: Read file contents (requires: file_path)
            - write_file: Write content to file (requires: file_path, content; optional: commit_message)
            - generate_diff: Generate diff between contents (requires: old_content, new_content; optional: context_lines)
            - apply_diff: Apply a unified diff to a file (requires: file_path, diff_content; optional: commit_message)
            - commit: Commit changes (requires: message)
            - get_history: Get commit history (optional: file_path, max_count)
            - create_branch: Create new branch (requires: branch_name)
            - switch_branch: Switch to branch (requires: branch_name)
            - get_branch: Get current branch name
            - get_status: Get file status (optional: file_path)"""
        ),
        required=True,
        llm_type_desc=dict(
            type="string",
            enum=[
                "list_files",
                "read_file",
                "write_file",
                "generate_diff",
                "apply_diff",
                "commit",
                "get_history",
                "create_branch",
                "switch_branch",
                "get_branch",
                "get_status",
            ],
        ),
    ),
    file_path="Path to the file within the repository",
    content="Content to write to the file",
    commit_message="Message to use when committing changes",
    old_content="Original content for generating diff",
    new_content="New content for generating diff",
    diff_content="Unified diff content to apply to a file",
    branch_name="Name of the branch to create or switch to",
    path="Optional path filter for listing files",
    message="Message to use when committing changes",
    max_count="Maximum number of commits to return",
    context_lines="Number of context lines in generated diffs",
    bot_id=BOT_ID_IMPLICIT_FROM_CONTEXT,
    thread_id=THREAD_ID_IMPLICIT_FROM_CONTEXT,
    _group_tags_=[tool_belt_tools],
)
def _git_action(
        action: str,
        file_path: str = None,
        content: str = None,
        commit_message: str = None,
        old_content: str = None,
        new_content: str = None,
        diff_content: str = None,
        branch_name: str = None,
        path: str = None,
        message: str = None,
        max_count: int = None,
        context_lines: int = None,
        bot_id: str = None,
        thread_id: str = None,
        _group_tags_=[tool_belt_tools],
):
    return ToolBelt().git_action(
        action=action,
        file_path=file_path,
        content=content,
        commit_message=commit_message,
        old_content=old_content,
        new_content=new_content,
        diff_content=diff_content,
        branch_name=branch_name,
        path=path,
        message=message,
        max_count=max_count,
        context_lines=context_lines,
        bot_id=bot_id,
        thread_id=thread_id
    )

# run_process
@gc_tool(
    action="The action to perform: KICKOFF_PROCESS, GET_NEXT_STEP, END_PROCESS, TIME, or STOP_ALL_PROCESSES.  Either process_name or process_id must also be specified.",
    process_name="The name of the process to run",
    process_id="The id of the process to run (note: this is NOT the task_id or process_schedule_id)",
    previous_response="The previous response from the bot (for use with GET_NEXT_STEP)",
    concise_mode="Optional, to run in low-verbosity/concise mode. Default to False.",
    bot_id=BOT_ID_IMPLICIT_FROM_CONTEXT,
    thread_id=THREAD_ID_IMPLICIT_FROM_CONTEXT,
    _group_tags_=[tool_belt_tools],
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
        if tool_name == "bot_dispatch_tools":
            func_descriptors.extend(BOT_DISPATCH_DESCRIPTIONS)
            available_functions_loaded.update(bot_dispatch_tools)
            tool_to_func_descriptors_map[tool_name] = BOT_DISPATCH_DESCRIPTIONS
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
        # elif tool_name == "process_runner_tools":
        #     func_descriptors.extend(process_runner_functions)
        #     available_functions_loaded.update(process_runner_tools)
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
                    # here's how to get the function for generated things even new ones...x
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

    # add user extended tools
    user_extended_tools_definitions, user_extended_functions = load_user_extended_tools(db_adapter, project_id=global_flags.project_id,
                                                                                        dataset_name=global_flags.genbot_internal_project_and_schema.split(".")[1])
    if user_extended_functions:
        func_descriptors.extend(user_extended_functions)
        available_functions_loaded.update(user_extended_tools_definitions)
        tool_to_func_descriptors_map[tool_name] = user_extended_functions

    return func_descriptors, available_functions, tool_to_func_descriptors_map
    # logger.info("imported: ",func)


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


# holds the list of all tool_belt_tools functions
# NOTE: Update this list when adding new tool_belt_tools (TODO: automate this by scanning the module?)
_all_tool_belt_functions = (

)


# Called from bot_os_tools.py to update the global list of data connection tool functions
def get_tool_belt_functions():
    return _all_tool_belt_functions
