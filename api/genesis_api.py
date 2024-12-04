import json
import time
from api.genesis_base import GenesisBot, GenesisLocalServer, GenesisProject, GenesisProcess, GenesisNote, GenesisKnowledge, LocalMetadataStore, SnowflakeMetadataStore, ToolDefinition
from api.snowflake_local_server import GenesisLocalSnowflakeServer
from api.snowflake_remote_server import GenesisSnowflakeServer

class GenesisAPI:
    def __init__(self, server_type, scope, sub_scope="app1", bot_list=None):
        self.server_type = server_type
        self.scope = scope
        self.sub_scope = sub_scope
        self.server_type = server_type
        if server_type == "local":
            if bot_list is not None:
                raise ValueError("bot_list not supported for local server")
            self.metadata_store = LocalMetadataStore(scope)
            self.registered_server = GenesisLocalServer(scope)
        elif server_type == "local-snowflake":
            self.metadata_store = SnowflakeMetadataStore(scope, sub_scope)
            self.registered_server = GenesisLocalSnowflakeServer(scope, sub_scope, bot_list=bot_list)
        elif server_type == "remote-snowflake":
            if bot_list is not None:
                raise ValueError("bot_list not supported for remote server")
            self.metadata_store = SnowflakeMetadataStore(scope, sub_scope)
            self.registered_server = GenesisSnowflakeServer(scope)
        else:
            raise ValueError("Remote server not supported yet")
        
    def register_bot(self, bot: GenesisBot):

        if self.server_type == "local-snowflake":
            return(self.registered_server.server.make_baby_bot_wrapper(
                bot_id=bot.get("BOT_ID", None),
                bot_name=bot.get("BOT_NAME", None),
                bot_implementation=bot.get("BOT_IMPLEMENTATION", None),
                files=bot.get("FILES", None),
                available_tools=bot.get("AVAILABLE_TOOLS", None),
                bot_instructions=bot.get("BOT_INSTRUCTIONS", None)
            ))

        self.metadata_store.insert_or_update_metadata("GenesisBot", bot["BOT_ID"], bot)
        
    def get_bot(self, bot_id) -> GenesisBot:
        return self.metadata_store.get_metadata("GenesisBot", bot_id)
    def get_all_bots(self) -> list[str]:
        return self.metadata_store.get_all_metadata("GenesisBot")

    def register_tool(self, tool: ToolDefinition):
        raise NotImplementedError("register_tool not implemented")
    def get_tool(self, bot_id, tool_name) -> dict:
        return self.registered_server.get_tool(bot_id, tool_name)
    def get_all_tools(self, bot_id) -> list[str]:
        return self.registered_server.get_all_tools(bot_id)
    def run_tool(self, bot_id, tool_name, tool_parameters: dict):
        return self.registered_server.run_tool(bot_id, tool_name, tool_parameters)
    
    def register_project(self, project: GenesisProject):
        self.metadata_store.insert_or_update_metadata("GenesisProject", project.project_id, project)
    def get_project(self, project_id) -> GenesisProject:
        return self.metadata_store.get_metadata("GenesisProject", project_id)
    def get_all_projects(self) -> list[str]:
        return self.metadata_store.get_all_metadata("GenesisProject")
    
    def get_project_asset(self, asset_id):
        return self.metadata_store.get_metadata("GenesisProjectAsset", asset_id)
    def get_all_project_assets(self, project_id) -> list[str]:
        return [list(item.values())[0] for item in self.metadata_store.get_all_metadata("GenesisProjectAsset", project_id)]

    def register_process(self, process: GenesisProcess):
        self.metadata_store.insert_or_update_metadata("GenesisProcess", process.process_id, process)
    def get_process(self, process_id) -> GenesisProcess:
        return self.metadata_store.get_metadata("GenesisProcess", process_id)
    def get_all_processes(self) -> list[str]:
        return self.metadata_store.get_all_metadata("GenesisProcess")
    
    def register_note(self, note: GenesisNote):
        self.metadata_store.insert_or_update_metadata("GenesisNote", note.note_id, note)
    def get_note(self, note_id) -> GenesisNote:
        return self.metadata_store.get_metadata("GenesisNote", note_id)
    def get_all_notes(self) -> list[str]:
        return self.metadata_store.get_all_metadata("GenesisNote")

    def get_harvest_results(self, source_name):
        return self.metadata_store.get_all_metadata("harvest_results", source_name)
    def get_all_harvest_results(self) -> list[str]:
        return self.metadata_store.get_all_metadata("harvest_results")
    
    def register_knowledge(self, knowledge: GenesisKnowledge):
        self.metadata_store.insert_or_update_metadata("GenesisKnowledge", knowledge.knowledge_thread_id, knowledge)
    def get_knowledge(self, thread_id) -> GenesisKnowledge:
        return self.metadata_store.get_metadata("GenesisKnowledge", thread_id)
    def get_all_knowledge(self) -> list[str]:
        return self.metadata_store.get_all_metadata("GenesisKnowledge")
    
    def get_message_log(self, bot_id, thread_id=None, last_n=None):
        if last_n is None:
            last_n = 5
        return self.metadata_store.get_all_metadata("GenesisMessage", 
                                                    fields_to_return=["TIMESTAMP", "THREAD_ID", "MESSAGE_TYPE", "MESSAGE_PAYLOAD"], 
                                                    first_filter=bot_id, second_filter=thread_id, last_n=last_n)

    def add_message(self, bot_id, message:str, thread_id=None) -> dict:
        return self.registered_server.add_message(bot_id, message=message, thread_id=thread_id)
    def get_response(self, bot_id, request_id=None, timeout_seconds=None) -> str:
        time_start = time.time()
        while timeout_seconds is None or time.time() - time_start < timeout_seconds:
            response = self.registered_server.get_message(bot_id, request_id)
            if response is not None and not response.endswith("💬"):
                return response
            time.sleep(1)
        return None
    

    

    def shutdown(self):
        self.registered_server.shutdown()
