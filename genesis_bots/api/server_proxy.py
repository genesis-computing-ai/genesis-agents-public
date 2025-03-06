from   genesis_bots.api.genesis_base \
                                import (GenesisBotConfig, RequestHandle,
                                        _ALL_BOTS_, get_tool_func_descriptor,
                                        is_bot_client_tool)
from   genesis_bots.core.bot_os_udf_proxy_input \
                                import UDFBotOsInputAdapter
from   genesis_bots.demo.app.genesis_app \
                                import DEFAULT_HTTP_ENDPOINT_PORT, genesis_app

import json
import requests
from   requests                 import Response

from   abc                      import ABC, abstractmethod
import collections
import socket
import sqlalchemy as sqla
import threading
import time
from   typing                   import Any, Dict, Union
import uuid
import logging

from  getpass                  import getpass

DEFAULT_GENESIS_DB = "GENESIS_BOTS"

class GenesisServerProxyBase(ABC):
    """
    GenesisServerProxyBase is an abstract base class that defines the interface for connecting to a Genesis server.
    Clients should not use this class directly, but rather use one of the concrete subclasses.

    Abtract Methods:
        _connect():
            Abstract method to connect to the server. Must be implemented by subclasses.

        _send_REST_request(op_name, endpoint_name, payload, content_type="application/json", extra_headers=None) -> Response:
            Abstract method to send a REST request. Must be implemented by subclasses.

    """
    def __init__(self):
        # maps names of client tool functions to a set of bot_ids
        self._client_tool_func_map: Dict[str, callable] = {} # maps function names to the tool functions (callable)
        self._client_tool_func_to_bots_map: Dict[str, Dict[str, set]] = {} # # maps function names to teh set of bots to which  was assigned
        self._is_connected = False
        self.genesis_app = None


    @abstractmethod
    def _connect(self):
        """Connect to the server. Must be implemented by subclasses."""
        raise NotImplementedError("method must be implemented by subclasses")


    @abstractmethod
    def _send_REST_request(self,
                          op_name, # POST, GET, etc
                          endpoint_name,
                          payload,
                          content_type="application/json",
                          extra_headers=None
                          ) -> Response:
        raise NotImplementedError("method must be implemented by subclasses")


    def connect(self):
        if not self._is_connected:
            try:
                 self._connect()
            except Exception as e:
                raise RuntimeError(f"Could not connect to the Genesis server: {str(e)}")
            self._is_connected = True
        return self._is_connected


    def register_bot(self, bot_config: Dict[str, Any] ):
        data = json.dumps({
            "data": {
                "bot_name": bot_config.get("BOT_NAME", None),
                "bot_implementation": bot_config.get("BOT_IMPLEMENTATION", None),
                "bot_id": bot_config.get("BOT_ID", None),
                "files": bot_config.get("FILES", None),
                "available_tools": bot_config.get("AVAILABLE_TOOLS", None),
                "bot_instructions": bot_config.get("BOT_INSTRUCTIONS", None)
            }
        })
        response = self._send_REST_request("post", "udf_proxy/create_baby_bot", data)
        return response.json()


    def list_available_bots(self) -> list[GenesisBotConfig]:
        data = json.dumps({"data": [[0]]})
        response = self._send_REST_request("post", "udf_proxy/list_available_bots", data)
        if response.status_code != 200:
            raise RuntimeError(f"Failed to list available bots: {response.text}")
        data = response.json()["data"][0][1]

        # ignore some attributes that are not needed by the client
        ignored_keys = ["api_app_id", "auth_state", "auth_url", "bot_avatar_image", "bot_intro_prompt", "bot_slack_user_id"]

        bot_configs = []
        for bot_data in data:
            for ikey in ignored_keys:
                bot_data.pop(ikey, None)
            bot_configs.append(GenesisBotConfig(**bot_data))
        return bot_configs


    def submit_message(self, bot_id, message, thread_id=None) -> RequestHandle: # returns a dict with keys: request_id, bot_id, thread_id
        # create a new unique thread_id if not provided
        if not thread_id:
            thread_id = str(uuid.uuid4())
        # send the message through the end point.
        data = json.dumps({"data": [[1, message, thread_id, json.dumps({"bot_id": bot_id})]]})
        response = self._send_REST_request("post", "udf_proxy/submit_udf", data)
        response_data = response.json()["data"][0][1]
        return RequestHandle(request_id=response_data, bot_id=bot_id, thread_id=thread_id)


    def _get_raw_message(self, bot_id, request_id) -> str:
        # poll for responses through the end point.
        data = json.dumps({"data": [[1, request_id, bot_id]]})
        response = self._send_REST_request("post", "udf_proxy/lookup_udf", data)
        if response.status_code == 200:
            response_data = response.json()["data"][0][1]
            if response_data.lower() != "not found":
                return response_data
        return None


    def get_message(self, bot_id, request_id) -> str:
        """
        Get a response message from the BotOsServer.
        Returns:
            The message, or None if no message is found.
        """
        msg = self._get_raw_message(bot_id, request_id)

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
                self.submit_message(bot_id, result_msg)
                msg = None # this is an internal message. Hide it from the client.
            else:
                # We do not recognize this action message.
                raise ValueError(f"Internal error:Unrecognized action message: {action_msg}")

        return msg


    def run_genesis_tool(self, tool_name, params, bot_id) -> Union[dict, list, str]:
        if not isinstance(params, (dict, collections.abc.Mapping)):
            raise ValueError("params must be a dictionary/mapping")
        data = json.dumps(dict(bot_id=bot_id, tool_name=tool_name, params=params))
        response = self._send_REST_request("post", "/realtime/genesis_tool", data)
        raw_response = response.json()
        if raw_response.get("success", False):
            return raw_response.get("results", {})
        else:
            raise RuntimeError(f"Failed to run Genesis tool {tool_name} for bot {bot_id}: {raw_response.get('message', 'Unknown error')}")


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
        tool_func = self._client_tool_func_map.get(tool_name)
        if tool_func is None:
            raise ValueError(f"Client tool function '{tool_name}' not found")
        res = tool_func(**kwargs)
        return res


    def register_client_tool(self, bot_id, tool_func, timeout_seconds):
        """
        Register a client tool function with the server for a specific bot.  The same tool function can be registered for multiple bots.
        """
        assert bot_id is not None and bot_id != _ALL_BOTS_, "Unsopported bot_id: {bot_id}"

        # Validate that tool_func is a proper bot_tool function
        if not is_bot_client_tool(tool_func):
            raise ValueError("The provided tool_func is not a valid bot_tool function")
        # Extract the tool function descriptor
        tool_func_descriptor = get_tool_func_descriptor(tool_func)
        if tool_func_descriptor is None:
            raise ValueError("The provided tool_func does not have a valid ToolFuncDescriptor")

        # Prepare the payload for the endpoint
        payload = {
            "bot_id": bot_id,
            "tool_func_descriptor": tool_func_descriptor.to_json(),
            "timeout_seconds": timeout_seconds
        }

        # Call the endpoint to add the client tool using _send_REST_request
        response = self._send_REST_request(
            "post",
            "udf_proxy/register_client_tool",
            json.dumps(payload)
        )

        if response.status_code != 200:
            raise RuntimeError(f"Failed to add client tool: {response.text}")

        # store the function for the specific bot_id
        self._client_tool_func_map[tool_func_descriptor.name] = tool_func
        if tool_func_descriptor.name not in self._client_tool_func_to_bots_map:
            self._client_tool_func_to_bots_map[tool_func_descriptor.name] = set()
        self._client_tool_func_to_bots_map[tool_func_descriptor.name].add(bot_id)


        # respond
        resp = response.json()
        return resp


    def unregister_client_tool(self, func_or_name, bot_id=_ALL_BOTS_):
        """
        Unregister a client tool function for a specific bot or all bots.
        """
        tool_name = func_or_name if isinstance(func_or_name, str) else func_or_name.__name__

        # Validate that tool_func is a proper bot_tool function if func_or_name is a callable
        if not isinstance(func_or_name, str) and not is_bot_client_tool(func_or_name):
            raise ValueError("The provided tool_func is not a valid bot_tool function")

        if tool_name not in self._client_tool_func_map:
            raise ValueError("Tool function not previosly registered")


        if bot_id != _ALL_BOTS_ and bot_id not in self.self.client_tool_func_to_bots_map[tool_name]:
            raise ValueError(f"Tool function '{tool_name}' not registered for bot_id '{bot_id}', nothing to do")

        # Prepare the payload for the endpoint
        payload = {
            "bot_id": bot_id,
            "tool_name": tool_name
        }

        # Call the endpoint to remove the client tool using _send_REST_request
        response = self._send_REST_request(
            "post",
            "udf_proxy/unregister_client_tool",
            json.dumps(payload)
        )

        if response.status_code != 200:
            raise RuntimeError(f"Failed to remove client tool: {response.text}")

        # Update the internal mapping
        if bot_id == _ALL_BOTS_:
            del self._client_tool_func_map[tool_name]
            del self._client_tool_func_to_bots_map[tool_name]
        else:
            self._client_tool_func_to_bots_map[tool_name].discard(bot_id)
            if not self._client_tool_func_to_bots_map[tool_name]:
                del self._client_tool_func_map[tool_name]


    def shutdown(self):
        # unregister any client tools we are trackign in seld from all bots
        for tool_name in list(self._client_tool_func_map.keys()):
            self.unregister_client_tool(tool_name, bot_id=_ALL_BOTS_)



class RESTGenesisServerProxy(GenesisServerProxyBase):
    """
    RESTGenesisServerProxy is a concrete subclass of GenesisServerProxyBase that connects to the Genesis server
    running as a REST service.
    """

    LOCAL_FLASK_SERVER_IP = "127.0.0.1"
    LOCAL_FLASK_SERVER_HOST = f"http://{LOCAL_FLASK_SERVER_IP}"
    LOCAL_FLASK_SERVER_PORT = DEFAULT_HTTP_ENDPOINT_PORT
    LOCAL_FLASK_SERVER_URL = f"{LOCAL_FLASK_SERVER_HOST}:{LOCAL_FLASK_SERVER_PORT}"

    def __init__(self,
                 server_url:str=LOCAL_FLASK_SERVER_URL,
                 _use_endpoint_router: bool=False # for testing only
                 ):
        super().__init__()
        self.server_url = server_url
        self._use_endpoint_router = _use_endpoint_router


    def _connect(self):
        timeout_sec = 1.0
        try:
            self._test_server_ready(timeout_sec=timeout_sec)
        except TimeoutError:
            raise RuntimeError(f"Could not connect to the Genesis server running on {self.server_url} [timed out after {timeout_sec} seconds]")


    def _send_REST_request(self,
                          op_name, # POST, GET, etc
                          endpoint_name,
                          payload,
                          content_type="application/json",
                          extra_headers=None
                          ) -> Response:
        # normalize the endpoint name
        if not endpoint_name.startswith('/'):
            endpoint_name = '/' + endpoint_name
        # use the endpoint router if requested
        if self._use_endpoint_router:
            return self._send_REST_request_via_endpoint_router(op_name, endpoint_name, payload, content_type, extra_headers)
        # otherwise, send the request directly to the endpoint
        url = self.server_url + endpoint_name
        headers_dict = {"Content-Type": content_type}
        if extra_headers:
            headers_dict.update(extra_headers)
        op_name = str(op_name).lower()
        assert op_name in ["post", "get", "put", "delete"]
        op_func = getattr(requests, op_name)
        response = op_func(url, headers=headers_dict, data=payload)
        if response.status_code != 200:
            raise Exception(f"Failed to submit message to endpoint {endpoint_name}: {response.text}")
        return response


    def _send_REST_request_via_endpoint_router(self,
                          op_name, # POST, GET, etc
                          endpoint_name,
                          payload,
                          content_type="application/json",
                          extra_headers=None
                          ) -> Response:

        # Pack the request details into a JSON object
        request_data = json.dumps({
            "endpoint_name": endpoint_name,
            "op_name": op_name,
            "headers": {"Content-Type": content_type, **(extra_headers or {})},
            "payload": payload
        })

        # Send the request to the endpoint router
        url = self.server_url + "/udf_proxy/endpoint_router"
        response = requests.post(url, headers={"Content-Type": "application/json"}, data=request_data)

        # Check the response status
        if response.status_code != 200:
            raise Exception(f"Failed to submit message to UDF proxy: {response.text}")

        return response


    def _test_server_ready(self, timeout_sec: float):
        start_time = time.time()
        endpoint_name = "/healthcheck"
        while True:
            try:
                response = self._send_REST_request("get", endpoint_name, None)
                if response.status_code == 200:
                    break
            except requests.ConnectionError:
                pass
            if time.time() - start_time > timeout_sec:
                raise TimeoutError(f"Failed to to reach healthcheck endpoint on the server. Timed out after {timeout_sec} seconds.")
            time.sleep(0.1)


class SPCSServerProxy(GenesisServerProxyBase):
    """
    SPCSServerProxy is a concrete subclass of GenesisServerProxyBase that connects to the Genesis server
    running as a Snowflake native app (SPCS).
    """

    def __init__(self,
                 connection_url: str, # a SQLAlchemy connection string
                 connect_args: Dict[str, str] = None, # optional connection arguments passed to SQLAlchemy create_engine,
                 genesis_db: str = DEFAULT_GENESIS_DB,
                 ):

        super().__init__()
        # validate the connection string
        try:
            self._connection_url = sqla.engine.url.make_url(connection_url)
        except Exception as e:
            raise ValueError(f"Invalid SQLAlchemy connection string: {str(e)}")

        self._engine = None
        self._genesis_db = genesis_db
        self._genesis_schema = "APP1"
        self._connect_args = connect_args or {}


    def _connect(self):
        # Create an engine and test the connection
        try:
            self._engine = sqla.create_engine(self._connection_url, connect_args=self._connect_args)
            with self._engine.connect() as conn:
                conn.execute(sqla.text("SELECT current_version()"))
        except Exception as e:
            raise RuntimeError(f"Failed to connect to the Snowflake database at {self._connection_url}: {str(e)}")


    def _send_REST_request(self,
                          op_name, # POST, GET, etc
                          endpoint_name,
                          payload,
                          content_type="application/json", # ignored, assumed to always be JSON
                          extra_headers=None # ignored
                          ) -> Response:
        if not endpoint_name.startswith('/'):
            endpoint_name = '/' + endpoint_name
        sql = f"select {self._genesis_db}.{self._genesis_schema}.ENDPOINT_ROUTER(:op_name, :endpoint_name, :payload)"
        try:
            with self._engine.connect() as conn:
                rowset = conn.execute(
                    sqla.text(sql),
                    {
                        "op_name": op_name,
                        "endpoint_name": endpoint_name,
                        "payload": payload
                    }
                )
                row = rowset.fetchone()
            if row is None:
                result = {} # empty response
            else:
                result = list(row)[0]
        except Exception as e:
            raise RuntimeError(f"Failed to execute the ENDPOINT_ROUTER UDF: {str(e)}")
        resp = requests.Response()
        resp._content = result.encode('utf-8')
        resp.status_code = 200
        resp.headers['Content-Type'] = 'application/json'
        return resp


class EmbeddedGenesisServerProxy(RESTGenesisServerProxy):
    """
    EmbeddedGenesisServerProxy is a specialization of RESTGenesisServerProxy that starts the Genesis server within the current process.
    """

    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(EmbeddedGenesisServerProxy, cls).__new__(cls)
        return cls._instance


    def __init__(self, fast_start=False, **kwargs):
        if hasattr(self, '_initialized') and self._initialized:
            raise RuntimeError("GenesisServerProxy should not be initialized more than once in the same process")
        if "server_url" in kwargs:
            raise ValueError(f"server_url param is not allowed to be provided to {self.__class__.__name__}")
        super().__init__(**kwargs)

        # Initialize a genesis_app (BotOsServer) and a flask_app accepting requests from the client in a dedicated thread
        self.genesis_app = genesis_app  # Note that genesis_app is a global singleton instance of GenesisApp;
                                        # this pointer is for convenience and encapsulation
        self.genesis_app.set_internal_project_and_schema()
        self.genesis_app.setup_database(fast_start=fast_start)
        self.genesis_app.set_llm_key_handler()

        self.flask_app = None
        self.flask_thread = None


    def _is_flask_running_locally(self, port):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            return s.connect_ex(('localhost', port)) == 0


    def _connect(self):
        if self._is_flask_running_locally(RESTGenesisServerProxy.LOCAL_FLASK_SERVER_PORT):
            raise RuntimeError(f"Port {RESTGenesisServerProxy.LOCAL_FLASK_SERVER_PORT} is already in use on local host. Cannot start Flask app.")
        self.genesis_app.create_app_sessions()
        self.genesis_app.start_server()
        # start the flask server only once (regards to the state of the BotOsServer)
        if self.flask_app is None:
            self._start_flask_app()
        # Wait for the Flask app to actually start running before we return, to avoid race condition
        self._test_server_ready(timeout_sec=0.5)


    def _start_flask_app(self):
        from   genesis_bots.demo.routes import main_routes, udf_routes  # lazy import to avoid unecessary dependencies
        from   flask                    import Flask
        def run_flask(): # flask thread function
            # Monkey-patch flask.cli.show_server_banner to be a no-op function to avoid printing information to stdout
            try:
                import flask.cli
                if hasattr(flask.cli, 'show_server_banner'):
                    flask.cli.show_server_banner = lambda *args, **kwargs: None
            except ImportError:
                pass
            # Start the (lightweight debug) Flask app on local host
            self.flask_app.run(host=RESTGenesisServerProxy.LOCAL_FLASK_SERVER_IP,
                               port=RESTGenesisServerProxy.LOCAL_FLASK_SERVER_PORT,
                               debug=False,
                               use_reloader=False)

        assert self.flask_app is None and self.flask_thread is None
        flask_app = Flask(self.__class__.__name__)
        flask_app.register_blueprint(udf_routes)
        flask_app.register_blueprint(main_routes)

        log = logging.getLogger('werkzeug')
        log.disabled = True


        self.flask_app = flask_app
        self.flask_thread = threading.Thread(target=run_flask)
        self.flask_thread.daemon = True # this allows the main thread to exit without 'joining' the flask thread
        self.flask_thread.start()


    def shutdown(self):
        super().shutdown()
        self.genesis_app.shutdown_server()




def _load_snowflake_private_key(filename: str, silent: bool=True) -> bytes:
    """
    Load a Snowflake private key from a PEM file.

    Args:
        filename (str): The path to the PEM file containing the private key.
        silent (bool): If True, suppresses print statements. Default is True.

    Returns:
        bytes: The private key in DER format.
    """
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import serialization
    if not silent:
        print(f"Loading Snowflake private key from {filename}")
    with open(filename, "rb") as finp:
        try:
            p_key= serialization.load_pem_private_key(finp.read(), password=None, backend=default_backend())
        except Exception as e:
            p_key= serialization.load_pem_private_key(finp.read(), password=getpass('Passphrase for private key: ').encode(), backend=default_backend())

    pkb = p_key.private_bytes(encoding=serialization.Encoding.DER, format=serialization.PrivateFormat.PKCS8,encryption_algorithm=serialization.NoEncryption())
    return pkb


def build_server_proxy(server_url: str, snowflake_conn_args: str|dict = None, genesis_db: str = DEFAULT_GENESIS_DB) -> GenesisServerProxyBase:
    """
    Build a server proxy based on the provided server URL and optional Snowflake connection arguments.

    Args:
        server_url (str): The URL of the server. It supports three types of URLS:
           1. HTTP(s) server URL (e.g. "http://localhost:8080"),
           2. "embedded" for running the Genesis BotOsServer inside the caller's process (used for testing and development only).
           3. Snowflake SQLAlchemy connection URL (e.g. "snowflake://user@account") that is passed to SqlAlchemy create_engine function.

        snowflake_conn_args (str|dict, optional): Additional connection arguments for a Snowflake connection if the server URL is a Snowflake connection URL.
        If a string, we assume it has the format key1=value1,key2=value2,... (no quotes) and parse it into a str->str dictionary.
        We pass it to SqlAlchemy.create_engine(server_url, connect_args=snowflake_conn_args).
        For convenience, if one of the keys in the dictionary is "private_key_file", we load the private key from the provited PEM file and add it to the arguments as "private_key".

    Returns:
        GenesisServerProxyBase: An instance of a server proxy based on the provided server URL and connection arguments.

    Raises:
        ValueError
    """
    if server_url.lower() == "embedded":
        return EmbeddedGenesisServerProxy()
    elif server_url.startswith('snowflake://'):
        if snowflake_conn_args is not None:
            args_format = "key1=value1,key2=value2;..."
            if isinstance(snowflake_conn_args, str):
                # parse the args from the string. Assume it has the format key1=value1,key2=value2;...
                try:
                    snowflake_conn_args = {k: v for k, v in [item.split('=') for item in snowflake_conn_args.split(',')]}
                except Exception as e:
                    raise ValueError(f"Invalid snowflake connection arguments format: {str(e)}. Expecting format {args_format}.")
            if not isinstance(snowflake_conn_args, dict):
                raise ValueError(f"Invalid snowflake connection arguments. Must be a dictionary or a string in the format {args_format}.")
            private_key_file = snowflake_conn_args.pop("private_key_file", None)
            if private_key_file:
                pkb = _load_snowflake_private_key(private_key_file)
                snowflake_conn_args["private_key"] = pkb
        return SPCSServerProxy(connection_url=server_url, connect_args=snowflake_conn_args, genesis_db=genesis_db)
    else:
        return RESTGenesisServerProxy(server_url=server_url)
