import os
from connectors.database_connector import llm_keys_and_types_struct
from connectors.sqlite_connector import SqliteConnector
from connectors.snowflake_connector import SnowflakeConnector

class LLMKeyHandler:

    def __init__(self, db_adapter=None):
        self.llm_api_key = None
        self.api_key_from_env = False
        self.connection = None
        self.genesis_source = os.getenv('GENESIS_SOURCE',default="BigQuery")

        if db_adapter:
            self.db_adapter = db_adapter
        else:
            if self.genesis_source == 'BigQuery':
                self.connection = 'BigQuery'
            elif self.genesis_source == 'Sqlite':
                self.db_adapter = SqliteConnector(connection_name="Sqlite")
                self.connection = 'Sqlite'
            elif self.genesis_source == 'Snowflake':    
                self.db_adapter = SnowflakeConnector(connection_name='Snowflake')
                self.connection = 'Snowflake'
            else:
                raise ValueError('Invalid Source')

    def get_llm_key_from_env(self):

        self.default_llm_engine = os.getenv("BOT_OS_DEFAULT_LLM_ENGINE") or "cortex"
        api_key_from_env = False
        llm_api_key = None
        llm_type = self.default_llm_engine
        llm_endpoint = None
        # check for Openai Env Override
        if llm_type.lower() == "openai":
            llm_api_key = os.getenv("OPENAI_API_KEY", None)
            llm_endpoint = os.getenv("AZURE_OPENAI_API_ENDPOINT", None)
            os.environ["CORTEX_MODE"] = "False"
        # elif self.default_llm_engine.lower() == "gemini":
        #     llm_api_key = os.getenv("GEMINI_API_KEY", None)
        #     llm_type = self.default_llm_engine
        elif llm_type.lower() == "cortex" or llm_api_key is None:
            if os.environ.get("CORTEX_AVAILABLE", 'False') in ['False', '']:
                cortex_available = self.db_adapter.check_cortex_available()
            else:
                cortex_available = True
            if cortex_available:
                llm_api_key = 'cortex_no_key_needed'
                llm_type = 'cortex'
                os.environ["CORTEX_MODE"] = "True"
                os.environ["CORTEX_HARVESTER_MODEL"] = "reka-flash"
                os.environ["CORTEX_EMBEDDING_MODEL"] = 'e5-base-v2'
            else:
                print("cortex not availabe and no llm key set - use streamlit to add a llm key")
        else:
            print("cortex not available and no llm key set - set key in streamlit")
            return False, llm_api_key, llm_type

        self.default_llm_engine = llm_type

        if not llm_api_key:
            llm_api_key = None
            return False, llm_api_key, llm_type
        else:
            api_key_from_env = True
            print(f"Default LLM set to {self.default_llm_engine} because ENV Var is present")
    
        try:
            #  insert key into db
            if llm_api_key:
                set_key_result = self.db_adapter.db_set_llm_key(llm_key=llm_api_key, llm_type=llm_type)
                print(f"set llm key in database result: {set_key_result}")
        except Exception as e:
            print(f"error updating llm key in database with error: {e}")

        return api_key_from_env, llm_keys_and_types_struct(llm_api_key=llm_api_key, llm_type=llm_type, llm_endpoint=llm_endpoint)


    def get_llm_key_from_db(self, db_connector=None, i=-1):
        import json 

        if db_connector:
            db_adapter = db_connector
        else:
            db_adapter = self.db_adapter

        cortex_avail = db_adapter.check_cortex_available()

        if "CORTEX_OVERRIDE" in os.environ:
            if os.environ["CORTEX_OVERRIDE"].lower() == "true" and cortex_avail:
                os.environ["CORTEX_MODE"] = "True"
                os.environ["CORTEX_HARVESTER_MODEL"] = "reka-flash"
                os.environ["CORTEX_EMBEDDING_MODEL"] = 'e5-base-v2'
                os.environ["BOT_OS_DEFAULT_LLM_ENGINE"] = 'cortex' 
                self.default_llm_engine = "cortex"
                print('&& CORTEX OVERRIDE FROM ENV VAR &&')
                return False, 'cortex_no_key_needed', "cortex"
            elif os.environ["CORTEX_OVERRIDE"] == "True" and not cortex_avail:
                print("Cortex override set to True but Cortex is not available")


        try:
            llm_key_struct = db_adapter.db_get_active_llm_key()
        except Exception as e:
            print(f"Error retrieving LLM key from database: {e}")
            return False, None, None
        
        if llm_key_struct.llm_key:
            if (llm_key_struct.llm_type.lower() == "openai"):
                os.environ["OPENAI_API_KEY"] = llm_key_struct.llm_key
                os.environ["AZURE_OPENAI_API_ENDPOINT"] = llm_key_struct.llm_endpoint
                os.environ["CORTEX_MODE"] = "False"
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
                    self.default_llm_engine = llm_key_struct.llm_type
                    llm_key_struct.llm_key = 'cortex_no_key_needed'
                    os.environ["CORTEX_MODE"] = "True"
                    os.environ["CORTEX_HARVESTER_MODEL"] = "reka-flash"
                    os.environ["CORTEX_EMBEDDING_MODEL"] = 'e5-base-v2'
                else:
                    os.environ["CORTEX_MODE"] = "False"
                    print("cortex not availabe and no llm key set")
            api_key_from_env = False
        else:
            api_key_from_env, llm_key_struct = self.get_llm_key_from_env()

        if llm_key_struct.llm_type.lower() == "cortex" and not cortex_avail:
            print("Cortex is not available. Falling back to OpenAI.")
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
                    print(f"Error retrieving LLM info: {llm_info.get('Error')}")
            
            if openai_key:
                llm_key = openai_key
                os.environ["OPENAI_API_KEY"] = llm_key
            else:
                print("No OpenAI key found in environment or database and cortex not available. LLM functionality may be limited.")
                llm_key = None
            
            os.environ["CORTEX_MODE"] = "False"

        os.environ["BOT_OS_DEFAULT_LLM_ENGINE"] = llm_key_struct.llm_type.lower()
        self.set_llm_env_vars()
        return api_key_from_env, llm_key_struct
            
    def set_llm_env_vars(self):
        
        try:
            llm_keys_and_types_arr = self.db_adapter.db_get_llm_key()
            for llm_keys_and_types in llm_keys_and_types_arr:
                if llm_keys_and_types.llm_key:
                    if (llm_keys_and_types.llm_type.lower() == "openai"):
                        os.environ["OPENAI_API_KEY"] = llm_keys_and_types.llm_key
                        os.environ["AZURE_OPENAI_API_ENDPOINT"]= llm_keys_and_types.llm_endpoint
                    # elif (llm_type.lower() == "reka"):
                    #     os.environ["REKA_API_KEY"] = llm_key
                    # elif (llm_type.lower() == "gemini"):
                    #     os.environ["GEMINI_API_KEY"] = llm_key
                    elif (llm_keys_and_types.llm_type.lower() == "cortex"):
                        if os.environ.get("CORTEX_AVAILABLE", 'False') in ['False', '']:
                            cortex_available = self.db_adapter.check_cortex_available()
                        else:
                            cortex_available = True
                        if not cortex_available:
                            print("cortex not available ")
                            #TODO does this need to be handled?
        except Exception as e:
            print(f"Error setting LLM environment variables: {e}")

