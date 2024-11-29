from abc import ABC
from pydantic import BaseModel
import sys
from pydantic import BaseModel
from typing import Dict, Any, Tuple, List
import os
import json
from datetime import datetime
from snowflake.connector import SnowflakeConnection


class BotGuardrails(BaseModel):
    def preprocess(self, message):
        pass
    def postprocess_response(self, response):
        pass

class BotKnowledgeGuardrails(BotGuardrails):
    def preprocess(self, message):
        return message
    def postprocess_response(self, response):
        return response

class GenesisBot(BaseModel):
    BOT_ID: str
    BOT_NAME: str
    #BOT_DESCRIPTION: str
    #TOOL_LIST: List[str]
    BOT_IMPLEMENTATION: str
    FILES: str # List[str]
    
    API_APP_ID: str
    AUTH_STATE: str
    AUTH_URL: str
    AVAILABLE_TOOLS: str
    BOT_AVATAR_IMAGE: str
    BOT_INSTRUCTIONS: str
    BOT_INTRO_PROMPT: str
    BOT_SLACK_USER_ID: str
    CLIENT_ID: str
    CLIENT_SECRET: str
    DATABASE_CREDENTIALS: str
    RUNNER_ID: str
    SLACK_ACTIVE: str
    SLACK_APP_LEVEL_KEY: str
    SLACK_APP_TOKEN: str
    SLACK_CHANNEL_ID: str
    SLACK_SIGNING_SECRET: str
    SLACK_USER_ALLOW: str
    TEAMS_ACTIVE: str
    TEAMS_APP_ID: str
    TEAMS_APP_PASSWORD: str
    TEAMS_APP_TENANT_ID: str
    TEAMS_APP_TYPE: str
    UDF_ACTIVE: str

    guardrails: BotKnowledgeGuardrails = BotKnowledgeGuardrails()

    def __str__(self):
        return f"GenesisBot(BOT_ID={self.BOT_ID}, BOT_NAME={self.BOT_NAME}, BOT_IMPLEMENTATION={self.BOT_IMPLEMENTATION})"

class GenesisServer(ABC):
    def __init__(self, scope):
        self.scope = scope
        self.bots = []
        self.adapters = []
    def add_bot(self, bot: GenesisBot):
        self.bots.append(bot)
    def add_adapter(self, adapter):
        self.adapters.append(adapter)
    def get_all_adapters(self):
        return self.adapters
    def run_tool(self, tool_name, tool_parameters):
        pass
    def add_message(self, bot_id, message, thread_id):
        pass
    def get_message(self, bot_id, thread_id):
        pass

class GenesisMetadataStore():#BaseModel):
    scope: str
    def __init__(self, scope):
        #super().__init__(scope=scope)
        self.scope = scope
    def insert_or_update_metadata(self, metadata_type: str, name: str, metadata: Dict[str, Any]):
        pass
    def get_metadata(self, metadata_type: str, name: str, name2: str = None) -> Dict[str, Any]:
        pass
    def get_all_metadata(self, metadata_type: str, name: str = None):
        pass

class LocalMetadataStore(GenesisMetadataStore):
    metadata_dir: str = "./metadata"  # Set a default value for metadata_dir
    def __init__(self, scope):
        super().__init__(scope)
    def insert_or_update_metadata(self, metadata_type: str, name: str, metadata: BaseModel):
        metadata_dict = metadata.model_dump()
        metadata_dict['type'] = metadata.__class__.__name__  # Store the class name for later instantiation
        if not os.path.exists(self.metadata_dir):
            os.makedirs(self.metadata_dir)
        metadata_type_dir = f"{self.metadata_dir}/{self.scope}/{metadata_type}"
        if not os.path.exists(metadata_type_dir):
            os.makedirs(metadata_type_dir)
        with open(f"{metadata_type_dir}/{name}.json", "w") as f:
            import json
            json.dump(metadata_dict, f)
    def get_metadata(self, metadata_type: str, name: str, name2: str = None) -> BaseModel:
        try:
            if name2:
                file_path = f"{self.metadata_dir}/{self.scope}/{metadata_type}/{name}/{name2}.json"
            else:
                file_path = f"{self.metadata_dir}/{self.scope}/{metadata_type}/{name}.json"
            with open(file_path, "r") as f:
                metadata_dict = json.load(f)
                # Dynamically instantiate the original object type
                metadata_class = getattr(sys.modules[__name__], metadata_dict['type'])
                return metadata_class(**metadata_dict)
        except FileNotFoundError:
            return None
    def get_all_metadata(self, metadata_type: str, name: str = None):
        metadata_type_dir = f"{self.metadata_dir}/{self.scope}/{metadata_type}"
        if not os.path.exists(metadata_type_dir):
            return []
        return [json.load(open(f"{metadata_type_dir}/{f.name}", "r")) for f in os.scandir(metadata_type_dir) if f.is_file()]

class SnowflakeMetadataStore(GenesisMetadataStore):
    metadata_type_mapping: Dict[str, Tuple[str, str]] = {
        "GenesisBot": ("BOT_SERVICING", "BOT_ID"),
        "GenesisProject": ("PROJECTS", "PROJECT_ID"),
        "GenesisProcess": ("PROCESSES", "PROCESS_ID"),
        "GenesisNote": ("NOTEBOOK", "NOTE_ID"),
        "GenesisKnowledge": ("KNOWLEDGE", "KNOWLEDGE_THREAD_ID"),
        "GenesisHarvestResults": ("HARVEST_RESULTS", "SOURCE_NAME"),
    }
    conn: SnowflakeConnection = None

    def __init__(self, scope, sub_scope="app1"):
        super().__init__(scope)
        self.sub_scope = sub_scope
        self.conn = SnowflakeConnection(
            account=os.getenv("SNOWFLAKE_ACCOUNT_OVERRIDE"),
            user=os.getenv("SNOWFLAKE_USER_OVERRIDE"),
            password=os.getenv("SNOWFLAKE_PASSWORD_OVERRIDE"),
            database=scope,
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE_OVERRIDE"),
            role=os.getenv("SNOWFLAKE_ROLE_OVERRIDE")
        )
    def insert_or_update_metadata(self, metadata_type: str, name: str, metadata: BaseModel):
        pass
    def get_metadata(self, metadata_type: str, name: str) -> BaseModel:
        cursor = self.conn.cursor()
        table_name, filter_column = self.metadata_type_mapping.get(metadata_type, (None, None))
        if not table_name:
            raise ValueError(f"Unknown metadata type: {metadata_type}")
        if self.sub_scope == "app1": # only necessary if connecting remotely
            query = f"SELECT * FROM {self.sub_scope}.{table_name} WHERE {filter_column} = \\\'{name}\\\'"
            cursor.execute("call core.run_arbitrary('%s')" % query) # TODO: don't need run_arbitrary if running locally
        else:
            query = f"SELECT * FROM {self.sub_scope}.{table_name} WHERE {filter_column} = '{name}'"
            cursor.execute(query)
        metadata_dict = cursor.fetchone()
        if metadata_dict:
            metadata_class = getattr(sys.modules[__name__], metadata_type)
            if self.sub_scope == "app1": # only necessary if connecting remotely
                metadata_dict = json.loads(metadata_dict[0])[0]
            else:
                metadata_dict = cursor.fetch_pandas_all().to_dict()
                metadata_dict = {k: v[0] if len(v) == 1 else v for k, v in metadata_dict.items()}
            metadata_dict = {k: v if v is not None else '' for k, v in metadata_dict.items()}
            return metadata_class(**metadata_dict)
        else:
            return None
    def get_all_metadata(self, metadata_type: str):
        cursor = self.conn.cursor()
        table_name, filter_column = self.metadata_type_mapping.get(metadata_type, (None, None))
        if not table_name:
            raise ValueError(f"Unknown metadata type: {metadata_type}")
        query = f"SELECT {filter_column} FROM {self.sub_scope}.{table_name}"
        if self.sub_scope == "app1": # only necessary if connecting remotely
            cursor.execute("call core.run_arbitrary('%s')" % query)
            metadata_list = cursor.fetchall()
            metadata_list = json.loads(metadata_list[0][0])
            metadata_list = [metadata[filter_column] for metadata in metadata_list]
        else:
            cursor.execute(query)
            metadata_list = cursor.fetchall()
            metadata_list = [metadata[0] for metadata in metadata_list]
        return metadata_list


class ToolDefinition(BaseModel):
    name: str
    description: str
    parameters: Dict[str, Any]  # JSON Schema

class GenesisProject(BaseModel):
    PROJECT_ID: str
    PROJECT_NAME: str
    DESCRIPTION: str
    CREATED_AT: str
    CURRENT_STATUS: str
    PROJECT_MANAGER_BOT_ID: str
    REQUESTED_BY_USER: str
    TARGET_COMPLETION_DATE: str
    UPDATED_AT: str

    # def toggle_item_completion(self, item_index: int):
    #     if item_index in self.project_items:
    #         name, completed = self.project_items[item_index]
    #         self.project_items[item_index] = (name, not completed)
    #     else:
    #         raise ValueError("Item index not found")


class GenesisLocalServer(GenesisServer):
    def __init__(self, scope):
        super().__init__(scope)
    def add_message(self, bot_id, message, thread_id):
        return "Thread_12345"
    def get_message(self, bot_id, thread_id):
        return "Message from thread_12345"


class GenesisSnowflakeServer(GenesisServer):
    def __init__(self, scope):
        super().__init__(scope)
        self.conn = SnowflakeConnection(
            account=os.getenv("SNOWFLAKE_ACCOUNT_OVERRIDE"),
            user=os.getenv("SNOWFLAKE_USER_OVERRIDE"),
            password=os.getenv("SNOWFLAKE_PASSWORD_OVERRIDE"),
            database=scope,
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE_OVERRIDE"),
            role=os.getenv("SNOWFLAKE_ROLE_OVERRIDE")
        )
        self.cursor = self.conn.cursor()
    def add_message(self, bot_id, message, thread_id):
        self.cursor.execute(f"select {self.scope}.app1.submit_udf('{message}', '{thread_id}', '{{\"bot_id\": \"{bot_id}\"}}')")
        return self.cursor.fetchone()[0]

    def get_message(self, bot_id, thread_id):
        self.cursor.execute(f"select {self.scope}.app1.lookup_udf('{thread_id}', '{bot_id}')")
        return self.cursor.fetchone()[0]

class GenesisProcess(BaseModel):
    process_id: str
    process_name: str
    bot_id: str
    process_steps: List[str]
    scheduled_next_run_time: datetime

class GenesisNote(BaseModel):
    note_id: str
    note_name: str
    bot_id: str
    note_type: str
    note_content: str
    note_params: str

class GenesisKnowledge(BaseModel):
    timestamp: datetime
    timestamp_ntz: datetime
    thread_id: str
    knowledge_thread_id: str
    primary_user: str
    bot_id: str
    last_timestamp: datetime
    thread_summary: str
    user_learning: str
    tool_learning: str
    data_learning: str
