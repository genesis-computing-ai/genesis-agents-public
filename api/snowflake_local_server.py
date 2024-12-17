import os
from typing import Dict
import uuid
from connectors import get_global_db_connector
from core.bot_os_llm import LLMKeyHandler
from core.bot_os_server import BotOsServer
from apscheduler.schedulers.background import BackgroundScheduler
from demo.sessions_creator import create_sessions

from .genesis_base import GenesisBot, GenesisMetadataStore, GenesisServer, SnowflakeMetadataStore
from streamlit_gui.udf_proxy_bot_os_adapter import UDFBotOsInputAdapter
import core.global_flags as global_flags

class GenesisLocalSnowflakeServer(GenesisServer):
    def __init__(self, scope, sub_scope="app1", bot_list=None, fast_start=False):
        super().__init__(scope, sub_scope)
        self.set_global_flags(fast_start=fast_start)
        self.bot_id_to_udf_adapter_map: Dict[str, UDFBotOsInputAdapter] = {}
        if f"{scope}.{sub_scope}" != os.getenv("GENESIS_INTERNAL_DB_SCHEMA"):
            raise Exception(f"Scope {scope}.{sub_scope} does not match environment variable GENESIS_INTERNAL_DB_SCHEMA {os.getenv('GENESIS_INTERNAL_DB_SCHEMA')}")
        self.server = None
        self.bot_list = bot_list
        self.restart()

    def restart(self):
        if self.server is not None:
            self.server.shutdown()
        db_adapter = get_global_db_connector("Snowflake")
        llm_key_handler = LLMKeyHandler(db_adapter=db_adapter)
        _, llm_api_key_struct = llm_key_handler.get_llm_key_from_db()
        if llm_api_key_struct is not None and llm_api_key_struct.llm_key is not None:
            (
                sessions,
                api_app_id_to_session_map,
                self.bot_id_to_udf_adapter_map,
                bot_id_to_slack_adapter_map #SystemVariables.bot_id_to_slack_adapter_map,
            ) = create_sessions(
                db_adapter,
                None, # bot_id_to_udf_adapter_map,
                stream_mode=True,
                bot_list=[{"bot_id": bot_id} for bot_id in self.bot_list] if self.bot_list is not None else None
        )

        self.bot_id_to_slack_adapter_map = bot_id_to_slack_adapter_map # We need to save it for a graceful shutdown

        scheduler = BackgroundScheduler(
            {
                "apscheduler.job_defaults.max_instances": 100,
                "apscheduler.job_defaults.coalesce": True,
            }
        )
        self.server = BotOsServer(
            None,
            sessions=sessions,
            scheduler=scheduler,
            scheduler_seconds_interval=2,
            slack_active=False, #global_flags.slack_active,
            db_adapter=db_adapter,
            bot_id_to_udf_adapter_map = self.bot_id_to_udf_adapter_map,
            api_app_id_to_session_map = api_app_id_to_session_map,
            bot_id_to_slack_adapter_map = self.bot_id_to_slack_adapter_map,
        )
        BotOsServer.stream_mode = True
        scheduler.start() # Note: self.server.shutdown() shuts down the scheduler



    def set_global_flags(self, fast_start=False):
        genbot_internal_project_and_schema = os.getenv("GENESIS_INTERNAL_DB_SCHEMA", "None")
        if genbot_internal_project_and_schema == "None":
            print("ENV Variable GENESIS_INTERNAL_DB_SCHEMA is not set.")
        if genbot_internal_project_and_schema is not None:
            genbot_internal_project_and_schema = genbot_internal_project_and_schema.upper()
        db_schema = genbot_internal_project_and_schema.split(".")
        project_id = db_schema[0]
        global_flags.project_id = project_id
        dataset_name = db_schema[1]
        global_flags.genbot_internal_project_and_schema = genbot_internal_project_and_schema

        db_adapter = get_global_db_connector("Snowflake")
        if fast_start:
            print("Genesis API Fast Start-Skipping Metadata Update Checks")
        else:
            db_adapter.one_time_db_fixes()
            db_adapter.ensure_table_exists()
            db_adapter.create_google_sheets_creds()

    def get_metadata_store(self) -> GenesisMetadataStore:
        return SnowflakeMetadataStore(self.scope, self.sub_scope)

    def register_bot(self, bot: GenesisBot):
        return(self.server.make_baby_bot_wrapper(
            bot_id=bot.get("BOT_ID", None),
            bot_name=bot.get("BOT_NAME", None),
            bot_implementation=bot.get("BOT_IMPLEMENTATION", None),
            files=bot.get("FILES", None),
            available_tools=bot.get("AVAILABLE_TOOLS", None),
            bot_instructions=bot.get("BOT_INSTRUCTIONS", None)
        ))

    def add_message(self, bot_id, message, thread_id) -> str|dict: # returns request_id
        if not thread_id:
            thread_id = str(uuid.uuid4())
        request_id = self.bot_id_to_udf_adapter_map[bot_id].submit(message, thread_id, bot_id={})
        return {"request_id": request_id,
                "bot_id": bot_id,
                "thread_id": thread_id}
        #return f"Request submitted on thread {thread_id} . To get response use: get_response --bot_id {bot_id} --request_id {request_id}"

    def get_message(self, bot_id, request_id) -> str:
        return self.bot_id_to_udf_adapter_map[bot_id].lookup_udf(request_id)


    def run_tool(self, bot_id, tool_name, params):
        session = next((s for s in self.server.sessions if s.bot_id == bot_id), None)
        if session is None:
            raise ValueError(f"No session found for bot_id {bot_id}")

        if tool_name == 'run_snowpark_python':
            params['return_base64'] = True
            params['save_artifacts'] = False

        # Search for the tool in the assistant's available functions
        tool = session.available_functions.get(tool_name)
        if tool is None:
            # Try appending an underscore to the front of the tool name
            tool = session.available_functions.get(f"_{tool_name}")
        if tool is None:
            raise ValueError(f"Tool {tool_name} not found for bot {bot_id}")

        # Run the tool with the query
        try:
            tool_result = tool(**params)
            return {"success": True, "results": tool_result}
        except Exception as tool_error:
            return {"success": False, "message": f"Error running tool: {str(tool_error)}"}

    def get_all_tools(self, bot_id):
        session = next((s for s in self.server.sessions if s.bot_id == bot_id), None)
        if session is None:
            raise ValueError(f"No session found for bot_id {bot_id}")
        return [t['function']['name'] for t in session.tools]

    def get_tool(self, bot_id, tool_name):
        session = next((s for s in self.server.sessions if s.bot_id == bot_id), None)
        if session is None:
            raise ValueError(f"No session found for bot_id {bot_id}")
        # Search for the tool in the assistant's available tools
        tool = next((t for t in session.tools if t.get('function', {}).get('name') == tool_name or t.get('function', {}).get('name') == f'_{tool_name}'), None)
        if tool is None:
            raise ValueError(f"Tool {tool_name} not found for bot {bot_id}")
        return tool['function']


    def shutdown(self):
        # shuts down the server (including the apscheduler)
        self.server.shutdown()
        # If there were any slack adapters created, shut them down
        if self.bot_id_to_slack_adapter_map:
            for slack_adapter in self.bot_id_to_slack_adapter_map.values():
                slack_adapter.shutdown()


    def upload_file(self, file_path, file_name, contents):
        """
        Write file contents to a Snowflake stage location.
        Args:
            file_path: Full path to stage, e.g. '@my_stage/path/to/'
            file_name: Name of file to write
            contents: String contents to write to file
        Returns:
            True if file was written successfully, False otherwise
        """
        conn = get_global_db_connector("Snowflake").connection
        try:
            cursor = conn.cursor()

            # Write contents to temporary local file
            with open(f'./tmp/{file_name}', 'w') as f:
                f.write(contents)

            # Put file to stage
            put_cmd = f"PUT file://./tmp/{file_name} {file_path} AUTO_COMPRESS=FALSE;"
            cursor.execute(put_cmd)

            return True
        except Exception as e:
            print(f"Error writing file to stage: {e}")
            return False
        finally:
            cursor.close()

    def get_file_contents(self, file_path, file_name):
        """
        Retrieve file contents from a Snowflake stage location.
        Args:
            stage_path: Full path to file in stage, e.g. '@my_stage/path/to/file.txt'
        Returns:
            String contents of the file
        """
        conn = get_global_db_connector("Snowflake").connection
        try:
            cursor = conn.cursor()
            get_file_cmd = f"GET {file_path}{file_name} file://./tmp/;"
            res = cursor.execute(get_file_cmd)

            with open(f'./tmp/{file_name}', 'r') as f:
                contents = f.read()

            return contents
        except Exception as e:
            print(f"Error retrieving file from stage: {e}")
            return None
        finally:
            cursor.close()

    def remove_file(self, file_path, file_name):
        """
        Remove a file from a Snowflake stage location if it exists.
        Args:
            stage_path: Full path to stage, e.g. '@my_stage/path/to/'
            file_name: Name of file to remove
        Returns:
            True if file was removed successfully, False otherwise
        """
        conn = get_global_db_connector("Snowflake").connection
        try:
            cursor = conn.cursor()
            # Check if file exists first
            list_cmd = f"LIST {file_path}{file_name};"
            res = cursor.execute(list_cmd).fetchall()

            if len(res) > 0:
                # File exists, remove it
                remove_cmd = f"REMOVE {file_path}{file_name};"
                cursor.execute(remove_cmd)
                return True
            return False
        except Exception as e:
            print(f"Error removing file from stage: {e}")
            return False
        finally:
            cursor.close()
