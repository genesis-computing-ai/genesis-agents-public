import os
from enum import Enum
from genesis_bots.core.logging_config import logger

class BotLlmEngineEnum(Enum):
    '''
    Supported names of LLM model providers (engines)
    '''
    openai = "openai"
    cortex = "cortex"

class LLMKeyHandler:

    def __init__(self, db_adapter=None):
        self.llm_api_key = None
        self.api_key_from_env = False
        self.connection = None
        self.genesis_source = os.getenv('GENESIS_SOURCE',default="Snowflake")

        if db_adapter:
            self.db_adapter = db_adapter
        else:
            if self.genesis_source == 'BigQuery':
                self.connection = 'BigQuery'
            elif self.genesis_source == 'Sqlite':
                from genesis_bots.connectors.sqlite_connector import SqliteConnector  # avoid circular imports as this is a core module
                self.db_adapter = SqliteConnector(connection_name="Sqlite")
                self.connection = 'Sqlite'
            elif self.genesis_source == 'Snowflake':
                from genesis_bots.connectors.snowflake_connector.snowflake_connector import SnowflakeConnector  # avoid circular imports as this is a core module
                self.db_adapter = SnowflakeConnector(connection_name='Snowflake')
                self.connection = 'Snowflake'
            else:
                raise ValueError('Invalid Source')

    def get_llm_key_from_env(self):
        from genesis_bots.connectors.connector_helpers import llm_keys_and_types_struct # avoid ciscular imports as this is a core module
        self.default_llm_engine = BotLlmEngineEnum(os.getenv("BOT_OS_DEFAULT_LLM_ENGINE") or "cortex")

        api_key_from_env = False
        llm_api_key = None
        llm_type = self.default_llm_engine
        llm_endpoint = None
        # check for Openai Env Override
        if llm_type is BotLlmEngineEnum.openai:
            llm_api_key = os.getenv("OPENAI_API_KEY", None)
            llm_endpoint = os.getenv("AZURE_OPENAI_API_ENDPOINT", None)
            os.environ["CORTEX_MODE"] = "False"
        # elif self.default_llm_engine.lower() == "gemini":
        #     llm_api_key = os.getenv("GEMINI_API_KEY", None)
        #     llm_type = self.default_llm_engine
        elif llm_type is BotLlmEngineEnum.cortex or llm_api_key is None:
            if os.environ.get("CORTEX_AVAILABLE", 'False') in ['False', '']:
                cortex_available = self.db_adapter.check_cortex_available()
            else:
                cortex_available = True
            if cortex_available:
                llm_api_key = 'cortex_no_key_needed'
                llm_type = BotLlmEngineEnum.cortex
                os.environ["CORTEX_MODE"] = "True"
                os.environ["CORTEX_HARVESTER_MODEL"] = "llama3.1-405b"
                os.environ["CORTEX_EMBEDDING_MODEL"] = 'e5-base-v2'
            else:
                logger.info("cortex not availabe and no llm key set - use streamlit to add a llm key")
        else:
            logger.info("cortex not available and no llm key set - set key in streamlit")
            return False, llm_api_key, llm_type

        self.default_llm_engine = BotLlmEngineEnum(llm_type)

        if not llm_api_key:
            llm_api_key = None
            return False, llm_api_key, llm_type
        else:
            api_key_from_env = True
            logger.info(f"Default LLM set to {self.default_llm_engine} because ENV Var is present")

        try:
            #  insert key into db
            if llm_api_key:
                set_key_result = self.db_adapter.db_set_llm_key(llm_key=llm_api_key, llm_type=llm_type, llm_endpoint=llm_endpoint)
                logger.info(f"set llm key in database result: {set_key_result}")
        except Exception as e:
            logger.info(f"error updating llm key in database with error: {e}")

        return api_key_from_env, llm_keys_and_types_struct(llm_key=llm_api_key,
                                                           llm_type=llm_type.value, # this struct expects the llm type (engine) name
                                                           llm_endpoint=llm_endpoint)


    def get_llm_key_from_db(self, db_connector=None, i=-1):
        """
        Retrieve the LLM key from the database or environment variables.

        This function attempts to retrieve the active LLM key from the database.
        If the key is not found in the database, it falls back to environment variables. The function
        also handles specific logic for different LLM types such as 'cortex' and 'openai'.

        Args:
            db_connector (optional): An optional database connector. If not provided, the default
                                     database adapter will be used.
            i (int, optional): An optional index parameter, default is -1.

        Returns:
            tuple: A tuple containing a boolean indicating if the API key was retrieved from the environment,
                   and an LLM key structure (see e.g. db_get_active_llm_key)
        """
        # Logic (as of 2024-09-27)
        # 1. Check if a custom database connector is provided; otherwise, use the default adapter.
        # 2. Check if Cortex is available.
        # 3. Handle 'CORTEX_OVERRIDE' environment variable:
        #     - If set to 'true' and Cortex is available, set the necessary environment variables for Cortex.
        #     - If set to 'true' but Cortex is not available, print a message.
        # 4. Attempt to retrieve the active LLM key from the database.
        #     - If an error occurs, print the error and return.
        # 5. If an LLM key is found:
        #     - Set the appropriate environment variables based on the LLM type.
        #     - If the LLM type is 'cortex' and Cortex is available, set the necessary environment variables.
        #     - If Cortex is not available, print a message.
        # 6. If no LLM key is found, fall back to retrieving the key from environment variables.
        # 7. If the LLM type is 'cortex' but Cortex is not available, fall back to 'openai'.
        #     - Attempt to retrieve the OpenAI key from the environment or database.
        #     - If no OpenAI key is found, print a message.
        # 8. Set the default LLM engine environment variable.
        # 9. Return the API key retrieval status and the LLM key structure.

        import json

        if db_connector:
            db_adapter = db_connector
        else:
            db_adapter = self.db_adapter

        cortex_avail = db_adapter.check_cortex_available()

        if "CORTEX_OVERRIDE" in os.environ:
            if os.environ["CORTEX_OVERRIDE"].lower() == "true" and cortex_avail:
                os.environ["CORTEX_MODE"] = "True"
                os.environ["CORTEX_HARVESTER_MODEL"] = "llama3.1-405b"
                os.environ["CORTEX_EMBEDDING_MODEL"] = 'e5-base-v2'
                os.environ["BOT_OS_DEFAULT_LLM_ENGINE"] = 'cortex'
                self.default_llm_engine = BotLlmEngineEnum.cortex
                logger.info('&& CORTEX OVERRIDE FROM ENV VAR &&')
                return False, 'cortex_no_key_needed', "cortex"
            elif os.environ["CORTEX_OVERRIDE"] == "True" and not cortex_avail:
                logger.info("Cortex override set to True but Cortex is not available")


        try:
            llm_key_struct = db_adapter.db_get_active_llm_key()
        except Exception as e:
            logger.info(f"Error retrieving LLM key from database: {e}")
            return False, None, None

        if llm_key_struct.llm_key:
            if (llm_key_struct.llm_type.lower() == "openai"):
                os.environ["OPENAI_API_KEY"] = llm_key_struct.llm_key
                os.environ["AZURE_OPENAI_API_ENDPOINT"] = llm_key_struct.llm_endpoint
                os.environ["CORTEX_MODE"] = "False"
                if llm_key_struct.llm_endpoint:
                    if llm_key_struct.model_name is not None and llm_key_struct.model_name != '':
                        os.environ["OPENAI_MODEL_NAME"] = llm_key_struct.model_name
                    else:
                        os.environ["OPENAI_MODEL_NAME"] = 'gpt-4o'
                    if llm_key_struct.embedding_model_name is not None and llm_key_struct.embedding_model_name != '':
                        os.environ["OPENAI_HARVESTER_EMBEDDING_MODEL"] = llm_key_struct.embedding_model_name
                    else:
                        os.environ["OPENAI_HARVESTER_EMBEDDING_MODEL"] = 'text-embedding-3-large'
            # elif (llm_type.lower() == "reka"):
            #     os.environ["REKA_API_KEY"] = llm_key
            #     os.environ["CORTEX_MODE"] = "False"
            # elif (llm_type.lower() == "gemini"):
            #     os.environ["GEMINI_API_KEY"] = llm_key
            #     os.environ["CORTEX_MODE"] = "False"
            elif (llm_key_struct.llm_type.lower() == "cortex"):
                if os.environ.get("CORTEX_AVAILABLE", 'False') in ['False', '']:
                    cortex_available = db_adapter.check_cortex_available()
                else:
                    cortex_available = True
                if cortex_available:
                    self.default_llm_engine = BotLlmEngineEnum(llm_key_struct.llm_type)
                    llm_key_struct.llm_key = 'cortex_no_key_needed'
                    os.environ["CORTEX_MODE"] = "True"
                    os.environ["CORTEX_HARVESTER_MODEL"] = "llama3.1-405b"
                    os.environ["CORTEX_EMBEDDING_MODEL"] = 'e5-base-v2'
                else:
                    os.environ["CORTEX_MODE"] = "False"
                    logger.info("cortex not availabe and no llm key set")
            api_key_from_env = False
        else:
            try:
                api_key_from_env, llm_key_struct = self.get_llm_key_from_env()
            except:
                return None, llm_key_struct

        if llm_key_struct.llm_type.lower() == "cortex" and not cortex_avail:
            logger.info("Cortex is not available. Falling back to OpenAI.")
            llm_key_struct.llm_type = "openai"
            # Attempt to get OpenAI key if it exists

            # First, check if OPENAI_API_KEY is already set in the environment
            openai_key = os.environ.get("OPENAI_API_KEY", None)
            if openai_key ==  '':
                openai_key = None
            if openai_key is not None:
                api_key_from_env = True

            if not openai_key:
                # If not set in environment, try to get it from the database
                llm_info = db_adapter.get_llm_info()
                if llm_info["Success"]:
                    llm_data = json.loads(llm_info["Data"])
                    openai_key = next((item["llm_key"] for item in llm_data if item["llm_type"].lower() == "openai"), None)
                else:
                    logger.info(f"Error retrieving LLM info: {llm_info.get('Error')}")

            if openai_key:
                llm_key = openai_key
                os.environ["OPENAI_API_KEY"] = llm_key
            else:
                logger.info("No OpenAI key found in environment or database and cortex not available. LLM functionality may be limited.")
                llm_key = None

            os.environ["CORTEX_MODE"] = "False"

        os.environ["BOT_OS_DEFAULT_LLM_ENGINE"] = llm_key_struct.llm_type.lower()
        # self.set_llm_env_vars() # not implemented?
        return api_key_from_env, llm_key_struct

