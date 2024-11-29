import sys
from pydantic import BaseModel
from typing import Dict, Any, Tuple, List
import os
import json
from datetime import datetime
from snowflake.connector import SnowflakeConnection

from api.genesis_base import GenesisBot, GenesisServer
from api.snowflake_local_server import GenesisLocalSnowflakeServer

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
        query = f"SELECT * FROM {self.sub_scope}.{table_name} WHERE {filter_column} = \\\'{name}\\\'"
        cursor.execute("call core.run_arbitrary('%s')" % query)
        metadata_dict = cursor.fetchone()
        if metadata_dict:
            metadata_class = getattr(sys.modules[__name__], metadata_type)
            metadata_dict = json.loads(metadata_dict[0])[0]
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
        cursor.execute("call core.run_arbitrary('%s')" % query)
        metadata_list = cursor.fetchall()
        metadata_list = json.loads(metadata_list[0][0])
        metadata_list = [metadata[filter_column] for metadata in metadata_list]
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

class GenesisAPI:
    def __init__(self, server_type, scope, sub_scope="app1"):
        self.server_type = server_type
        self.scope = scope
        self.sub_scope = sub_scope
        if server_type == "local":
            self.metadata_store = LocalMetadataStore(scope)
            self.registered_server = GenesisLocalServer(scope)
        elif server_type == "local-snowflake":
            self.metadata_store = SnowflakeMetadataStore(scope, sub_scope)
            self.registered_server = GenesisLocalSnowflakeServer(scope, sub_scope)
        elif server_type == "remote-snowflake":
            self.metadata_store = SnowflakeMetadataStore(scope, sub_scope)
            self.registered_server = GenesisSnowflakeServer(scope)
        else:
            raise ValueError("Remote server not supported yet")
    def register_bot(self, bot: GenesisBot):
        self.metadata_store.insert_or_update_metadata("GenesisBot", bot.bot_id, bot)
    def get_bot(self, bot_id):
        bot = self.metadata_store.get_metadata("GenesisBot", bot_id)
        #if bot:
        #    bot.set_client(self)
        return bot
    def get_all_bots(self):
        return self.metadata_store.get_all_metadata("GenesisBot")
    
    def register_tool(self, tool: ToolDefinition):
        self.metadata_store.insert_or_update_metadata("tool", tool.name, tool)
    def get_tool(self, tool_name):
        return self.metadata_store.get_metadata("tool", tool_name)
    def get_all_tools(self):
        return self.metadata_store.get_all_metadata("tool")
    def run_tool(self, tool_name, tool_parameters):
        registered_server = self.get_registered_server()
        if registered_server:
            return registered_server.run_tool(tool_name, tool_parameters)
        else:
            raise ValueError("No server registered")
    
    def register_project(self, project: GenesisProject):
        self.metadata_store.insert_or_update_metadata("GenesisProject", project.project_id, project)
    def get_project(self, project_id) -> GenesisProject:
        return self.metadata_store.get_metadata("GenesisProject", project_id)
    def get_all_projects(self):
        return self.metadata_store.get_all_metadata("GenesisProject")
    
    def get_project_assets(self, project_id, asset_id):
        return self.metadata_store.get_metadata("project_assets", project_id, asset_id)
    def get_all_project_assets(self, project_id):
        return self.metadata_store.get_all_metadata("project_assets", project_id)

    def register_process(self, process: GenesisProcess):
        self.metadata_store.insert_or_update_metadata("GenesisProcess", process.process_id, process)
    def get_process(self, process_id) -> GenesisProcess:
        return self.metadata_store.get_metadata("GenesisProcess", process_id)
    def get_all_processes(self):
        return self.metadata_store.get_all_metadata("GenesisProcess")
    
    def register_note(self, note: GenesisNote):
        self.metadata_store.insert_or_update_metadata("GenesisNote", note.note_id, note)
    def get_note(self, note_id) -> GenesisNote:
        return self.metadata_store.get_metadata("GenesisNote", note_id)
    def get_all_notes(self):
        return self.metadata_store.get_all_metadata("GenesisNote")

    def get_harvest_results(self, source_name):
        return self.metadata_store.get_all_metadata("harvest_results", source_name)
    def get_all_harvest_results(self):
        return self.metadata_store.get_all_metadata("harvest_results")
    
    def register_knowledge(self, knowledge: GenesisKnowledge):
        self.metadata_store.insert_or_update_metadata("GenesisKnowledge", knowledge.knowledge_thread_id, knowledge)
    def get_knowledge(self, thread_id) -> GenesisKnowledge:
        return self.metadata_store.get_metadata("GenesisKnowledge", thread_id)
    def get_all_knowledge(self):
        return self.metadata_store.get_all_metadata("GenesisKnowledge")
    
    def add_message(self, bot_id, message=None, thread_id=None) -> str:
        return self.registered_server.add_message(bot_id, message=message, thread_id=thread_id)
    def get_response(self, bot_id, thread_id=None) -> str:
        return self.registered_server.get_message(bot_id, thread_id)

#client = GenesisAPI("local", scope="GENESIS_INTERNAL") # or "url of genesis server" with a valid API key
#client = GenesisAPI("snowflake", scope="GENESIS_BOTS_ALPHA") # or "url of genesis server" with a valid API key

#new_bot = GenesisBot(bot_name="my_bot", bot_description="my bot description", tool_list=["send_slack_message", "run_sql_query"], 
#                     bot_implementation="openai", docs=["doc1.txt", "doc2.txt"])
#client.register_bot(new_bot)
#my_bot = client.get_bot(bot_name="my_bot")
#assert new_bot.bot_description == my_bot.bot_description
#response = my_bot.add_message_and_wait(message="what is the weather in SF?")
#print(response)

    