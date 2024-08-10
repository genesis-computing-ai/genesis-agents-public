import os
import time
from connectors.sqlite_connector import SqliteConnector

class LLMKeyHandler:
    cortex_mode = os.getenv("CORTEX_MODE", "False")

    @classmethod
    def set_cortex_mode(cls, mode):
        cls.cortex_mode = mode  # Modify the class variable
        os.environ["CORTEX_MODE"] = str(mode)
        print(f"##### cortex_mode: {cls.cortex_mode} #####")

    def __init__(self):
        self.llm_api_key = None
        self.api_key_from_env = False
        self.connection = None
        self.genesis_source = os.getenv('GENESIS_SOURCE',default="BigQuery")

        if self.genesis_source == 'BigQuery':
            connection = 'BigQuery'
        elif self.genesis_source == 'Sqlite':
            # self.db_adapter = SqliteConnector(connection_name="Sqlite")
            connection = 'Sqlite'
        elif self.genesis_source == 'Snowflake':    
            # self.db_adapter = SnowflakeConnector(connection_name='Snowflake')
            connection = 'Snowflake'
        else:
            raise ValueError('Invalid Source')


    def check_cortex_available(self):
        from connectors.snowflake_connector import SnowflakeConnector
        db_adapter = SnowflakeConnector(connection_name=self.connection)

        os.environ["CORTEX_AVAILABLE"] = 'False'
        if os.getenv("CORTEX_VIA_COMPLETE",'False').lower() == '':
            os.environ["CORTEX_VIA_COMPLETE"] = 'False'

        if self.genesis_source == "Snowflake":
            try:
                cortex_test = db_adapter.test_cortex_via_rest()

                if cortex_test == True:
                    os.environ["CORTEX_AVAILABLE"] = 'True'
                    self.default_llm_engine = 'cortex'
                    LLMKeyHandler.set_cortex_mode('True')
                    self.llm_api_key = 'cortex_no_key_needed'
                    print('\nCortex LLM is Available via REST and successfully tested')
                    return True
                else:
                    LLMKeyHandler.set_cortex_mode('False')
                    return False
            except Exception as e:
                print('Cortex LLM Not available via REST, exception on test: ',e)

            if os.environ["CORTEX_AVAILABLE"] == 'False' or os.getenv("CORTEX_VIA_COMPLETE",'False').lower() == 'true':
                try:
                    cortex_test = db_adapter.test_cortex()

                    if cortex_test == True:
                        os.environ["CORTEX_AVAILABLE"] = 'True'
                        os.environ["CORTEX_VIA_COMPLETE"] = 'True'
                        LLMKeyHandler.set_cortex_mode('True')
                        self.default_llm_engine = 'cortex'
                        self.llm_api_key = 'cortex_no_key_needed'
                        print('Cortex LLM is Available via SQL COMPLETE() and successfully tested')
                        return True
                    else:
                        LLMKeyHandler.set_cortex_mode('False')
                        return False
                except Exception as e:
                    print('Cortex LLM Not available via SQL COMPLETE(), exception on test: ',e)
                    
   
    def get_llm_key_from_env(self):
        from connectors.snowflake_connector import SnowflakeConnector
        db_adapter = SnowflakeConnector(connection_name=self.connection)
        self.default_llm_engine = os.getenv("BOT_OS_DEFAULT_LLM_ENGINE", "openai")
        api_key_from_env = False
        llm_api_key = None

        # check for Openai Env Override
        if self.default_llm_engine.lower() == "openai":
            llm_api_key = os.getenv("OPENAI_API_KEY", None)
            if llm_api_key == "" or llm_api_key == None:
                llm_api_key = None
            else:
                api_key_from_env = True
                os.environ["CORTEX_MODE"] == "False"
                print('Default LLM set to OpenAI because ENV Var OPENAI_API_KEY is present')
        elif self.default_llm_engine.lower() == "reka":
            llm_api_key = os.getenv("REKA_API_KEY", None)
        elif self.default_llm_engine.lower() == "gemini":
            llm_api_key = os.getenv("GEMINI_API_KEY", None)
        elif self.default_llm_engine.lower() == "cortex" or self.default_llm_engine is None or llm_api_key is None:
            if os.environ.get("CORTEX_AVAILABLE", 'False') == 'False':
                cortex_available = self.check_cortex_available()
            else:
                cortex_available = True
            if cortex_available:
                llm_api_key = 'cortex_no_key_needed'
                os.environ["CORTEX_MODE"] = "True"
                os.environ["CORTEX_HARVESTER_MODEL"] = "reka-flash"
                os.environ["CORTEX_EMBEDDING_MODEL"] = 'e5-base-v2'
            else:
                print("cortex not availabe and no llm key set")
        else:
            print("cortex not availabe and no llm key set")
        
        try:
            #  insert key into db
            set_key_result = db_adapter.db_set_llm_key(llm_key=llm_api_key, llm_type=self.default_llm_engine)
        except Exception as e:
            print(f"error updating llm key in database with error: {e}")

        return api_key_from_env, llm_api_key


    def get_llm_key_from_db(self, db_connector=None):
        from connectors.snowflake_connector import SnowflakeConnector
        if db_connector:
            db_adapter = db_connector
        else:
            db_adapter = SnowflakeConnector(connection_name=self.connection)

        llm_key, llm_type = db_adapter.db_get_active_llm_key()

        if llm_key:
            if (llm_type.lower() == "openai"):
                os.environ["OPENAI_API_KEY"] = llm_key
                os.environ["CORTEX_MODE"] = "False"
            elif (llm_type.lower() == "reka"):
                os.environ["REKA_API_KEY"] = llm_key
                os.environ["CORTEX_MODE"] = "False"
            elif (llm_type.lower() == "gemini"):
                os.environ["GEMINI_API_KEY"] = llm_key
                os.environ["CORTEX_MODE"] = "False"
            elif (llm_type.lower() == "cortex"):
                if os.environ.get("CORTEX_AVAILABLE", 'False') == 'False':
                    cortex_available = self.check_cortex_available()
                else:
                    cortex_available = True
                if cortex_available:
                    os.environ["CORTEX_MODE"] = "True"
                    os.environ["CORTEX_HARVESTER_MODEL"] = "reka-flash"
                    os.environ["CORTEX_EMBEDDING_MODEL"] = 'e5-base-v2'
                else:
                    print("cortex not availabe and no llm key set")
            api_key_from_env = True
        else:
            api_key_from_env, llm_key = self.get_llm_key_from_env()
            api_key_from_env = True

        return api_key_from_env, llm_key
            
        
#TODO are u necessary?
    def check_llm_key(self, llm_keys_and_types):
        
        # llm_keys_and_types = []
        #if llm_api_key is None and genesis_source == "Snowflake":
        # if api_key_from_env == False and self.genesis_source == "Snowflake":
        #     print('Checking LLM_TOKENS for saved LLM Keys:')
        llm_api_key = None
        if llm_keys_and_types:
            for llm_key, llm_type in llm_keys_and_types:
                if llm_key and llm_type:
                    if llm_type.lower() == "openai":
                        os.environ["OPENAI_API_KEY"] = llm_key
                        LLMKeyHandler.set_cortex_mode('False')
                        print("Found OpenAI Key in LLM_TOKENS, setting env var OPENAI_API_KEY")
                    #elif llm_type.lower() == "reka":
                    #    os.environ["REKA_API_KEY"] = llm_key
                    elif llm_type.lower() == "gemini":
                        os.environ["GEMINI_API_KEY"] = llm_key
                        print("Found Gemini Key in LLM_TOKENS, setting env var GEMINI_API_KEY")
                    elif llm_key.lower() == 'cortex_no_key_needed':
                        llm_api_key = llm_key
                    if llm_api_key is None:
                        print(f"Cortex is not available, so setting default LLM API key: {llm_key} and type: {llm_type} found in LLM_TOKENS")
                        api_key_from_env = False
                        LLMKeyHandler.set_cortex_mode('False')
                        llm_api_key = llm_key
                        self.default_llm_engine = llm_type
                    break
        if llm_api_key is None and LLMKeyHandler.cortex_mode == 'False':
            # LLMKeyHandler.set_cortex_mode('False')
            print("===========")
            print("NOTE: Cortex not available and no LLM configured, Config via Streamlit to continue")
            print("===========")
            return None
        else:
            return llm_api_key
        
        if self.genesis_source == 'BigQuery' and not api_key_from_env:
            while True:
                print('!!!!! Loading LLM API Key from File No longer Supported -- Please provide via ENV VAR when using BigQuery Source')
                time.sleep(3)
