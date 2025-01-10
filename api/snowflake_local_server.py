from   .genesis_base            import (GenesisBot, GenesisMetadataStore,
                                        GenesisServer, SnowflakeMetadataStore,
                                        SqliteMetadataStore,
                                        get_tool_func_descriptor,
                                        is_bot_client_tool)
from   connectors               import get_global_db_connector
from   demo.app.genesis_app     import genesis_app
from   demo.routes              import main_routes, udf_routes
from   flask                    import Flask
import json
import os
import requests
from   streamlit_gui.udf_proxy_bot_os_adapter \
                                import UDFBotOsInputAdapter
import threading
import time
from   typing                   import Dict
import uuid


LOCAL_FLASK_SERVER_IP = "127.0.0.1"
LOCAL_FLASK_SERVER_HOST = f"http://{LOCAL_FLASK_SERVER_IP}"
LOCAL_FLASK_SERVER_PORT = 8080
LOCAL_FLASK_SERVER_URL = f"{LOCAL_FLASK_SERVER_HOST}:{LOCAL_FLASK_SERVER_PORT}"

class GenesisLocalServer(GenesisServer):

    _BOT_OS_DIRECT_MODE = False
    "internal flag to control whether to call directly to the BotOsServer or via the Flask server, for methods that support both"

    def __init__(self, scope, sub_scope="app1", bot_list=None, fast_start=False):
        super().__init__(scope, sub_scope)

        if f"{scope}.{sub_scope}" != os.getenv("GENESIS_INTERNAL_DB_SCHEMA"):
             raise Exception(f"Scope {scope}.{sub_scope} does not match environment variable GENESIS_INTERNAL_DB_SCHEMA {os.getenv('GENESIS_INTERNAL_DB_SCHEMA')}")
        # self.server = None
        self.bot_list = bot_list

        #-----------------------------
        self.genesis_app = genesis_app # Note that genesis_app is a global singleton instance of GenesisApp; this pointer is for convenience and encapsulation
        self.genesis_app.set_internal_project_and_schema()
        self.genesis_app.setup_databse(fast_start=fast_start)
        self.genesis_app.set_llm_key_handler()

        self.flask_app = None
        self.flask_thread = None

        self.client_tool_func_map: Dict[str, callable] = {} # maps names of client tool functions to their callable implementations. See add_client_tool

        # start the server
        self.restart()


    def restart(self):

        bot_list_maps=[{"bot_id": bot_id} for bot_id in self.bot_list] if self.bot_list else None
        self.genesis_app.create_app_sessions(bot_list=bot_list_maps)
        self.genesis_app.start_server()
        # start the flask server only once (regards to the state of the BotOsServer)
        if self.flask_app is None:
            self._start_flask_app()


    def _start_flask_app(self):
        def run_flask(): # flask thread function
            # Monkey-patch flask.cli.show_server_banner to be a no-op function to avoid printing information to stdout
            try:
                import flask.cli
                if hasattr(flask.cli, 'show_server_banner'):
                    flask.cli.show_server_banner = lambda *args, **kwargs: None
            except ImportError:
                pass
            # Start the (lightweigth debug) Flask app on local host
            self.flask_app.run(host=LOCAL_FLASK_SERVER_IP, port=LOCAL_FLASK_SERVER_PORT, debug=False, use_reloader=False)


        assert self.flask_app is None and self.flask_thread is None
        flask_app = Flask(self.__class__.__name__)
        flask_app.register_blueprint(udf_routes)
        flask_app.register_blueprint(main_routes)


        self.flask_app = flask_app
        self.flask_thread = threading.Thread(target=run_flask)
        self.flask_thread.setDaemon(True) # this allows the main thread to exit without 'joining' the flask thread
        self.flask_thread.start()
        # Wait for the Flask app to actually start running before we return, to avoid race condition
        self._test_flask_app_is_ready(timeout=0.5)


    def _test_flask_app_is_ready(self, timeout):
        start_time = time.time()
        url = LOCAL_FLASK_SERVER_URL + "/healthcheck"
        while True:
            try:
                response = requests.get(url)
                if response.status_code == 200:
                    break
            except requests.ConnectionError:
                pass
            if time.time() - start_time > timeout:
                raise TimeoutError("Flask app did not start within the expected time.")
            time.sleep(0.1)


    def get_metadata_store(self) -> GenesisMetadataStore:
        if os.getenv("SQLITE_OVERRIDE", "false").lower() == "true":
            return SqliteMetadataStore(self.scope, self.sub_scope)
        else:
            return SnowflakeMetadataStore(self.scope, self.sub_scope)


    def register_bot(self, bot: GenesisBot):
        if self._BOT_OS_DIRECT_MODE:
            return self._register_bot_direct(bot)
        else:
            return self._register_bot_indirect(bot)


    def _register_bot_direct(self, bot: GenesisBot):
        return(self.server.make_baby_bot_wrapper(
            bot_id=bot.get("BOT_ID", None),
            bot_name=bot.get("BOT_NAME", None),
            bot_implementation=bot.get("BOT_IMPLEMENTATION", None),
            files=bot.get("FILES", None),
            available_tools=bot.get("AVAILABLE_TOOLS", None),
            bot_instructions=bot.get("BOT_INSTRUCTIONS", None)
        ))


    def _register_bot_indirect(self, bot: GenesisBot):
        url = LOCAL_FLASK_SERVER_URL + "/udf_proxy/create_baby_bot"
        headers = {"Content-Type": "application/json"}
        data = json.dumps({
            "data": {
                "bot_name": bot.get("BOT_NAME", None),
                "bot_implementation": bot.get("BOT_IMPLEMENTATION", None),
                "bot_id": bot.get("BOT_ID", None),
                "files": bot.get("FILES", None),
                "available_tools": bot.get("AVAILABLE_TOOLS", None),
                "bot_instructions": bot.get("BOT_INSTRUCTIONS", None)
            }
        })
        response = requests.post(url, headers=headers, data=data)
        return response.json()


    def add_message(self, bot_id, message, thread_id=None) -> dict: # returns a dict with request_id, bot_id, thread_id
        if not thread_id:
            thread_id = str(uuid.uuid4())
        if self._BOT_OS_DIRECT_MODE:
            return self._add_message_direct(bot_id, message, thread_id)
        else:
            return self._add_message_indirect(bot_id, message, thread_id)


    def _add_message_direct(self, bot_id, message, thread_id) -> dict: # returns request_id
        # Add the message directly to the BotOsServer's internal message queue, short-circuiting end-points (Flask) mechanism
        request_id = self.genesis_app.bot_id_to_udf_adapter_map[bot_id].submit(message, thread_id, bot_id={})
        return {"request_id": request_id,
                "bot_id": bot_id,
                "thread_id": thread_id}


    def _add_message_indirect(self, bot_id, message, thread_id) -> dict: # returns a dict with request_id, bot_id, thread_id
        # send the message through the end point.
        url = LOCAL_FLASK_SERVER_URL + "/udf_proxy/submit_udf"
        headers = {"Content-Type": "application/json"}
        data = json.dumps({"data": [[1, message, thread_id, json.dumps({"bot_id": bot_id})]]})

        response = requests.post(url, headers=headers, data=data)
        if response.status_code == 200:
            response_data = response.json()["data"][0][1]
            return {"request_id": response_data,
                    "bot_id": bot_id,
                    "thread_id": thread_id}
        else:
            raise Exception(f"Failed to submit message to UDF proxy: {response.text}")


    def get_message(self, bot_id, request_id) -> str:
        """
        Get a response message from the BotOsServer.
        Returns:
            The message, or None if no message is found.
        """
        if self._BOT_OS_DIRECT_MODE:
            msg = self._get_message_direct(bot_id, request_id)
        else:
            msg = self._get_message_indirect(bot_id, request_id)

        if msg is None:
            return None

        # check is this is a special action message
        try:
            action_msg = UDFBotOsInputAdapter.parse_action_msg(msg)
        except ValueError as e:
            pass # not an action message - regular chat response message
        else:
            if action_msg["action_type"] == "action_required":
                # LLM requesting us to call a client tool.
                # We expect all the following fields to be present in the action_msg:
                invocation_id = action_msg["invocation_id"]
                tool_func_name = action_msg["tool_func_name"]
                invocation_kwargs = action_msg["invocation_kwargs"]
                # invoke the tool and return the result
                try:
                    func_result = self._invoke_client_tool(tool_func_name, invocation_kwargs)
                except Exception as e:
                    func_result = f"Error invoking client tool: {str(e)}"
                # send the result back to the LLM
                result_msg = UDFBotOsInputAdapter.format_action_msg("action_result",
                                                                    invocation_id=invocation_id,
                                                                    func_result=func_result)
                self.add_message(bot_id, result_msg)
                msg = None # this is an internal message. Hide it from the client.
            else:
                # We do not recognize this action message.
                raise ValueError(f"Internal error:Unrecognized action message: {action_msg}")

        return msg


    def _get_message_direct(self, bot_id, request_id) -> str:
        # Get any pending message directly form the BotOsServer's internal message queues, short-circuiting end-points (Flask) mechanism
        return self.genesis_app.bot_id_to_udf_adapter_map[bot_id].lookup_udf(request_id)


    def _get_message_indirect(self, bot_id, request_id) -> str:
        # poll for responses through the end point.
        url = LOCAL_FLASK_SERVER_URL + "/udf_proxy/lookup_udf"
        headers = {"Content-Type": "application/json"}
        data = json.dumps({"data": [[1, request_id, bot_id]]})
        response = requests.post(url, headers=headers, data=data)
        if response.status_code == 200:
            response_data = response.json()["data"][0][1]
            if response_data.lower() != "not found":
                return response_data
        return None

    # deprecated tool handling logic
    #--------------------------------
    # def run_tool(self, bot_id, tool_name, params):
    #     session = next((s for s in self.genesis_app.server.sessions if s.bot_id == bot_id), None)
    #     if session is None:
    #         raise ValueError(f"No session found for bot_id {bot_id}")

    #     if tool_name == 'run_snowpark_python':
    #         params['return_base64'] = True
    #         params['save_artifacts'] = False

    #     # Search for the tool in the assistant's available functions
    #     tool = session.available_functions.get(tool_name)
    #     if tool is None:
    #         # Try appending an underscore to the front of the tool name
    #         tool = session.available_functions.get(f"_{tool_name}")
    #     if tool is None:
    #         raise ValueError(f"Tool {tool_name} not found for bot {bot_id}")

    #     # Run the tool with the query
    #     try:
    #         tool_result = tool(**params)
    #         return {"success": True, "results": tool_result}
    #     except Exception as tool_error:
    #         return {"success": False, "message": f"Error running tool: {str(tool_error)}"}

    # def get_all_tools(self, bot_id):
    #     session = next((s for s in self.server.sessions if s.bot_id == bot_id), None)
    #     if session is None:
    #         raise ValueError(f"No session found for bot_id {bot_id}")
    #     return [t['function']['name'] for t in session.tools]

    # def get_tool(self, bot_id, tool_name):
    #     session = next((s for s in self.server.sessions if s.bot_id == bot_id), None)
    #     if session is None:
    #         raise ValueError(f"No session found for bot_id {bot_id}")
    #     # Search for the tool in the assistant's available tools
    #     tool = next((t for t in session.tools if t.get('function', {}).get('name') == tool_name or t.get('function', {}).get('name') == f'_{tool_name}'), None)
    #     if tool is None:
    #         raise ValueError(f"Tool {tool_name} not found for bot {bot_id}")
    #     return tool['function']


    def _invoke_client_tool(self, tool_name:str, kwargs):
        """
        Invoke a client tool function by its name with the provided keyword arguments.
        Args:
            tool_name: Name of the client tool function to invoke
            **kwargs: Keyword arguments to pass to the client tool function
        Returns:
            The result of the client tool function invocation
        Raises:
            ValueError: If the tool_name is not found in the client_tool_func_map
        """
        tool_func = self.client_tool_func_map.get(tool_name)
        if tool_func is None:
            raise ValueError(f"Client tool function '{tool_name}' not found")
        res = tool_func(**kwargs)
        return res


    def shutdown(self):
        # shuts down the server (including the apscheduler)
        self.genesis_app.shutdown_server()



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
        conn = get_global_db_connector(self.db_source).connection
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
        conn = get_global_db_connector(self.db_source).connection
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
        conn = get_global_db_connector(self.db_source).connection
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


    def add_client_tool(self, bot_id, tool_func):
        # Validate that tool_func is a proper bot_tool function
        if not is_bot_client_tool(tool_func):
            raise ValueError("The provided tool_func is not a valid bot_tool function")
        # Extract the tool function descriptor
        tool_func_descriptor = get_tool_func_descriptor(tool_func)
        if tool_func_descriptor is None:
            raise ValueError("The provided tool_func does not have a valid ToolFuncDescriptor")

        # Prepare the payload for the endpoint
        url = LOCAL_FLASK_SERVER_URL + "/udf_proxy/add_client_tool"
        headers = {"Content-Type": "application/json"}
        payload = {
            "bot_id": bot_id,
            "tool_func_descriptor": tool_func_descriptor.to_json()
        }

        # Call the endpoint to add the client tool
        response = requests.post(url, headers=headers, json=payload)

        if response.status_code != 200:
            raise RuntimeError(f"Failed to add client tool: {response.text}")

        # Remember this function since we might be asked to call it later
        self.client_tool_func_map[tool_func_descriptor.name] = tool_func

        # respond
        resp = response.json()
        return resp
