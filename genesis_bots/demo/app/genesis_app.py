import os
from genesis_bots.core.logging_config import logger
from genesis_bots.connectors import get_global_db_connector
from genesis_bots.core import global_flags
from genesis_bots.core.bot_os_llm import LLMKeyHandler
from genesis_bots.bot_genesis.make_baby_bot import  get_slack_config_tokens, test_slack_config_token
from genesis_bots.core.system_variables import SystemVariables
from genesis_bots.demo.sessions_creator import create_sessions
from apscheduler.schedulers.background import BackgroundScheduler
from genesis_bots.auto_ngrok.auto_ngrok import launch_ngrok_and_update_bots


DEFAULT_HTTP_ENDPOINT_PORT = 8080
DEFAULT_HTTPS_ENDPOINT_PORT = 8082
DEFAULT_STREAMLIT_APP_PORT = 8501

class GenesisApp:
    _instance = None

    def __new__(cls):
        """
        Implements the Singleton pattern for the GenesisApp class.

        This method ensures that only one instance of the GenesisApp class is created.
        If an instance already exists, it is reused; otherwise, a new instance is created.

        Returns:
            GenesisApp: The single instance of the GenesisApp class.
        """
        if cls._instance is None:
            cls._instance = super(GenesisApp, cls).__new__(cls)
            cls._instance.__init__()
        return cls._instance


    def __init__(self):
        """
        Initializes a new instance of the GenesisApp class.

        This constructor sets up initial values for various attributes which are
        used throughout the application for managing sessions, database connections,
        and server configurations.

        Attributes:
            project_id (str): The ID of the project being processed.
            dataset_name (str): The name of the dataset used in the application.
            db_adapter: The database adapter for connecting to the database.
            llm_api_key_struct: The structure to store LLM API key information.
            sessions: Holds the session information for the app.
            api_app_id_to_session_map: Maps API app IDs to session instances.
            bot_id_to_udf_adapter_map: Maps bot IDs to UDF adapter instances.
            bot_id_to_slack_adapter_map: Maps bot IDs to Slack adapter instances.
            server: Represents the server instance used by the application.
        """

        self.project_id = None
        self.dataset_name = None
        self.db_adapter = None
        self.llm_api_key_struct = None
        self.sessions = None
        self.api_app_id_to_session_map = None
        self.bot_id_to_udf_adapter_map = None
        self.bot_id_to_slack_adapter_map = None
        self.server = None

    def get_server(self):
        """
        Returns the server instance associated with this GenesisApp.

        Returns:
            The server instance or None if no server is running.
        """
        return self.server


    def generate_index_file(self):
        """
        Deletes the index size file if it exists, as it is only used when running the app locally
        and is expected to be deleted on each local run. This method is called by the constructor
        to setup initial values for the application.

        Attributes:
            index_size_file (str): The index size file path to be deleted.
        """
        index_file_path = './tmp/'
        index_size_file = os.path.join(index_file_path, 'index_size.txt')
        if os.path.exists(index_size_file):
            try:
                os.remove(index_size_file)
                logger.info(f"Deleted {index_size_file} (this is expected on local test runs)")
            except Exception as e:
                logger.info(f"Error deleting {index_size_file}: {e}")


    def set_internal_project_and_schema(self):
        """
        Sets the internal project and schema for the GenesisApp.

        This method is used to set the project ID and dataset name for the application
        by retrieving the GENESIS_INTERNAL_DB_SCHEMA environment variable and splitting
        it into project ID and dataset name.

        If the environment variable is not set, a log message is printed indicating this.

        Attributes:
            project_id (str): The ID of the project being processed.
            dataset_name (str): The name of the dataset used in the application.
        """
        genbot_internal_project_and_schema = os.getenv("GENESIS_INTERNAL_DB_SCHEMA", "None")
        if genbot_internal_project_and_schema == "None":
            logger.info("ENV Variable GENESIS_INTERNAL_DB_SCHEMA is not set.")
        if genbot_internal_project_and_schema is not None:
            genbot_internal_project_and_schema = genbot_internal_project_and_schema.upper()
        db_schema = genbot_internal_project_and_schema.split(".")
        project_id = db_schema[0]
        global_flags.project_id = project_id
        dataset_name = db_schema[1]
        global_flags.genbot_internal_project_and_schema = genbot_internal_project_and_schema
        self.project_id = project_id
        self.dataset_name = dataset_name


    def setup_databse(self, fast_start=None):
        """
        Configures the internal database for the GenesisApp.

        This method identifies the database source from environment variables and sets up a
        global database connector. If the application is not in fast start mode, it performs
        necessary one-time database fixes, ensures required tables exist, and configures Google
        Sheets credentials. It also updates the global flags to reflect the current database source.

        Args:
            fast_start (bool, optional): If True, skips certain setup steps for faster startup.

        Attributes:
            db_adapter: The database adapter instance for connecting to the database.
        """

        genesis_source = os.getenv("GENESIS_SOURCE", default="Snowflake")
        db_adapter = get_global_db_connector(genesis_source)

        test_mode =  os.getenv("TEST_MODE")
        if fast_start is None:
            # set fast_start based on TEST_MODE env var
            if test_mode is not None:
                test_mode = test_mode.lower()
                if test_mode in ["true", "1", "yes", "y", "on", "enable", "enabled", "active", "enabled"]:
                    logger.info(f"Env var TEST_MODE set to {test_mode} - setting FAST_START to True")
                    fast_start = True
                elif test_mode in ["false", "0", "no", "n", "off", "disable", "disabled", "inactive", "disabled"]:
                    logger.info(f"Env var TEST_MODE set to {test_mode} - setting FAST_START to False")
                    fast_start = False
                else:
                    logger.warning(f"Ignoring Env var TEST_MODE = {test_mode} - unrecognized value. Defaulting to False")
                    fast_start = False
            else:
            #    logger.info("TEST_MODE not defined in environment - setting FAST_START to False")
                fast_start = False
        else:
            if test_mode is not None:
                logger.warning(f"FAST_START set to {fast_start} by caller. Ignoring TEST_MODE = {test_mode}")

        if fast_start:
            logger.info("()()()()()()()()()()()()()")
            logger.info("FAST START - ensure table exists skipped")
            logger.info("()()()()()()()()()()()()()")
        else:
           # logger.info("NOT RUNNING FAST START - APPLYING ONE TIME DB FIXES AND CREATING TABLES")
            db_adapter.one_time_db_fixes()
            db_adapter.ensure_table_exists()
            db_adapter.create_google_sheets_creds()

        logger.info(f"---> CONNECTED TO DATABASE:: {genesis_source}")
        global_flags.source = genesis_source

        self.db_adapter = db_adapter


    def set_llm_key_handler(self):
        """
        Sets up the LLM key handler for the GenesisApp.

        This method initializes a LLM key handler and attempts to retrieve the active LLM key
        from the database. If the key is not found, it falls back to environment variables.
        The method also handles specific logic for different LLM types such as 'cortex' and 'openai'.

        Attributes:
            llm_api_key_struct: The structure to store LLM API key information.
        """
        llm_api_key_struct = None
        llm_key_handler = LLMKeyHandler(db_adapter=self.db_adapter)

        # set the system LLM type and key
        logger.info('Checking LLM_TOKENS for saved LLM Keys:')
        try:
            api_key_from_env, llm_api_key_struct = llm_key_handler.get_llm_key_from_db()
        except Exception as e:
            logger.error(f"Failed to get LLM key from database: {e}")
            llm_api_key_struct = None
        self.llm_api_key_struct = llm_api_key_struct


    def set_slack_config(self):
        """
        Sets the Slack configuration for the GenesisApp.

        Retrieves the Slack app config token and refresh token from the database, and
        sets the global flag `global_flags.slack_active` based on the result of
        `test_slack_config_token()`. If the token is expired, sets `global_flags.slack_active`
        to False.

        Attributes:
            global_flags.slack_active (bool): The flag indicating whether the Slack
                connector is active.
        """
        t, r = get_slack_config_tokens()
        global_flags.slack_active = test_slack_config_token()
        if global_flags.slack_active == 'token_expired':
            logger.info('Slack Config Token Expired')
            global_flags.slack_active = False

        logger.info(f"...Slack Connector Active Flag: {global_flags.slack_active}")


    def create_app_sessions(self, bot_list=None):
        """
        Creates the sessions for the GenesisApp.

        This method initializes the sessions for the GenesisApp by invoking the create_sessions()
        function with the necessary parameters such as the database adapter, LLM key structure,
        etc. It also updates the global flag `SystemVariables.bot_id_to_slack_adapter_map`
        and assigns the created session instances, the map of API app IDs to session instances,
        the map of bot IDs to UDF adapter instances, and the map of bot IDs to Slack adapter
        instances to the corresponding class attributes.

        Args:
            bot_list (list, optional): A list of bot configurations to create sessions for. Defaults to None.
        """
        db_adapter = self.db_adapter
        llm_api_key_struct = self.llm_api_key_struct
        if llm_api_key_struct is not None and llm_api_key_struct.llm_key is not None:
            (
                sessions,
                api_app_id_to_session_map,
                bot_id_to_udf_adapter_map,
                bot_id_to_slack_adapter_map,
            ) = create_sessions(
                db_adapter,
                self.bot_id_to_udf_adapter_map,
                stream_mode=True,
                bot_list=bot_list,
            )
            SystemVariables.bot_id_to_slack_adapter_map = bot_id_to_slack_adapter_map
            self.sessions = sessions
            self.api_app_id_to_session_map = api_app_id_to_session_map
            self.bot_id_to_udf_adapter_map = bot_id_to_udf_adapter_map
            self.bot_id_to_slack_adapter_map = bot_id_to_slack_adapter_map
        else:
            # wait to collect API key from Streamlit user, then make sessions later
            pass


    def start_server(self):
        """
        Creaters and starts the server instance for the GenesisApp.

        This method creates a BotOsServer instance with the provided database adapter,
        LLM key structure, sessions, API app ID to session map,
        bot ID to UDF adapter map, and bot ID to Slack adapter map. It also starts the
        BackgroundScheduler.

        Attributes:
            server (BotOsServer): The server instance.
            scheduler (BackgroundScheduler): The scheduler instance.
        """
        db_adapter = self.db_adapter
        llm_api_key_struct = self.llm_api_key_struct
        sessions = self.sessions
        api_app_id_to_session_map = self.api_app_id_to_session_map
        bot_id_to_udf_adapter_map = self.bot_id_to_udf_adapter_map

        # scheduler = BackgroundScheduler(executors={'default': ThreadPoolExecutor(max_workers=100)})
        scheduler = BackgroundScheduler(
            {
                "apscheduler.job_defaults.max_instances": 100,
                "apscheduler.job_defaults.coalesce": True,
            }
        )
        # Retrieve the number of currently running jobs in the scheduler
        # Code to clear any threads that are stuck or crashed from BackgroundScheduler

        server = None
        if llm_api_key_struct is not None and llm_api_key_struct.llm_key is not None:
            from genesis_bots.core.bot_os_server import BotOsServer

            BotOsServer.stream_mode = True
            server = BotOsServer(
                flask_app=None, sessions=sessions, scheduler=scheduler, scheduler_seconds_interval=1,
                slack_active=global_flags.slack_active,
                db_adapter=db_adapter,
                        bot_id_to_udf_adapter_map = bot_id_to_udf_adapter_map,
                        api_app_id_to_session_map = api_app_id_to_session_map,
                        bot_id_to_slack_adapter_map = SystemVariables.bot_id_to_slack_adapter_map,
            )
        self.server = server
        self.scheduler = scheduler
        self.scheduler.start()


    def is_server_running(self):
        return self.server is not None and self.scheduler is not None


    def shutdown_server(self):
        ''' shuts down the server (including the apscheduler) '''
        if not self.is_server_running():
            logger.info("Server is not running, nothing to shutdown")
            return

        self.server.shutdown()

        # If there were any slack adapters created, shut them down
        if self.bot_id_to_slack_adapter_map:
            for slack_adapter in self.bot_id_to_slack_adapter_map.values():
                slack_adapter.shutdown()

        self.server = None
        self.scheduler = None


    def run_ngrok(self):
        """
        Start ngrok and update the Slack app endpoint URLs if slack is active.

        Returns:
            bool: True if ngrok was successfully activated, False if not.
        """

        ngrok_active = launch_ngrok_and_update_bots(update_endpoints=global_flags.slack_active)


    def start_all(self):
        self.generate_index_file()
        self.set_internal_project_and_schema()
        self.setup_databse()
        self.set_llm_key_handler()
        self.set_slack_config()
        self.run_ngrok()
        self.create_app_sessions()
        self.start_server()


# singleton instance of app.
genesis_app = GenesisApp()



