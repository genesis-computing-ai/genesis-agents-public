from api.genesis_base import GenesisBot, GenesisLocalServer, GenesisProject, GenesisProcess, GenesisNote, GenesisKnowledge, LocalMetadataStore, SnowflakeMetadataStore, ToolDefinition
from api.snowflake_local_server import GenesisLocalSnowflakeServer
from api.snowflake_remote_server import GenesisSnowflakeServer

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
    def get_bot(self, bot_id) -> GenesisBot:
        return self.metadata_store.get_metadata("GenesisBot", bot_id)
    def get_all_bots(self) -> list[str]:
        return self.metadata_store.get_all_metadata("GenesisBot")['BOT_ID']
    
    def register_tool(self, tool: ToolDefinition):
        self.metadata_store.insert_or_update_metadata("tool", tool.name, tool)
    def get_tool(self, tool_name):
        return self.metadata_store.get_metadata("tool", tool_name)
    def get_all_tools(self):
        return self.metadata_store.get_all_metadata("tool")
    def run_tool(self, tool_name, tool_parameters: dict):
        registered_server = self.get_registered_server()
        if registered_server:
            return registered_server.run_tool(tool_name, tool_parameters)
        else:
            raise ValueError("No server registered")
    
    def register_project(self, project: GenesisProject):
        self.metadata_store.insert_or_update_metadata("GenesisProject", project.project_id, project)
    def get_project(self, project_id) -> GenesisProject:
        return self.metadata_store.get_metadata("GenesisProject", project_id)
    def get_all_projects(self) -> list[str]:
        return self.metadata_store.get_all_metadata("GenesisProject")['PROJECT_ID']
    
    def get_project_assets(self, project_id, asset_id):
        return self.metadata_store.get_metadata("project_assets", project_id, asset_id)
    def get_all_project_assets(self, project_id) -> list[str]:
        return self.metadata_store.get_all_metadata("project_assets", project_id)['ASSET_ID']

    def register_process(self, process: GenesisProcess):
        self.metadata_store.insert_or_update_metadata("GenesisProcess", process.process_id, process)
    def get_process(self, process_id) -> GenesisProcess:
        return self.metadata_store.get_metadata("GenesisProcess", process_id)
    def get_all_processes(self) -> list[str]:
        return self.metadata_store.get_all_metadata("GenesisProcess")['PROCESS_ID']
    
    def register_note(self, note: GenesisNote):
        self.metadata_store.insert_or_update_metadata("GenesisNote", note.note_id, note)
    def get_note(self, note_id) -> GenesisNote:
        return self.metadata_store.get_metadata("GenesisNote", note_id)
    def get_all_notes(self) -> list[str]:
        return self.metadata_store.get_all_metadata("GenesisNote")['NOTE_ID']

    def get_harvest_results(self, source_name):
        return self.metadata_store.get_all_metadata("harvest_results", source_name)
    def get_all_harvest_results(self) -> list[str]:
        return self.metadata_store.get_all_metadata("harvest_results")['SOURCE_NAME']
    
    def register_knowledge(self, knowledge: GenesisKnowledge):
        self.metadata_store.insert_or_update_metadata("GenesisKnowledge", knowledge.knowledge_thread_id, knowledge)
    def get_knowledge(self, thread_id) -> GenesisKnowledge:
        return self.metadata_store.get_metadata("GenesisKnowledge", thread_id)
    def get_all_knowledge(self) -> list[str]:
        return self.metadata_store.get_all_metadata("GenesisKnowledge")['KNOWLEDGE_THREAD_ID']
    
    def get_message_log(self, bot_id, thread_id=None, last_n=None):
        if last_n is None:
            last_n = 5
        return self.metadata_store.get_all_metadata("GenesisMessage", 
                                                    fields_to_return=["TIMESTAMP", "THREAD_ID", "MESSAGE_TYPE", "MESSAGE_PAYLOAD"], 
                                                    first_filter=bot_id, second_filter=thread_id, last_n=last_n)

    def add_message(self, bot_id, message:str, thread_id=None) -> dict:
        return self.registered_server.add_message(bot_id, message=message, thread_id=thread_id)
    def get_response(self, bot_id, request_id=None) -> str:
        return self.registered_server.get_message(bot_id, request_id)

#client = GenesisAPI("local", scope="GENESIS_INTERNAL") # or "url of genesis server" with a valid API key
#client = GenesisAPI("snowflake", scope="GENESIS_BOTS_ALPHA") # or "url of genesis server" with a valid API key

#new_bot = GenesisBot(bot_name="my_bot", bot_description="my bot description", tool_list=["send_slack_message", "run_sql_query"], 
#                     bot_implementation="openai", docs=["doc1.txt", "doc2.txt"])
#client.register_bot(new_bot)
#my_bot = client.get_bot(bot_name="my_bot")
#assert new_bot.bot_description == my_bot.bot_description
#response = my_bot.add_message_and_wait(message="what is the weather in SF?")
#print(response)

    