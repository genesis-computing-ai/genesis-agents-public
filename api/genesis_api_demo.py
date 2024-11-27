from abc import ABC, abstractmethod
from pydantic import BaseModel
from typing import Dict, Any, Tuple

class GenesisMetadataStore(BaseModel):
    def __init__(self, scope):
        self.scope = scope
    def insert_or_update_metadata(self, metadata_type: str, name: str, metadata: Dict[str, Any]):
        pass
    def get_metadata(self, metadata_type: str, name: str) -> Dict[str, Any]:
        pass

class LocalMetadataStore(GenesisMetadataStore):
    def __init__(self, scope):
        super().__init__(scope)
    def insert_or_update_metadata(self, metadata_type: str, name: str, metadata: BaseModel):
        metadata = metadata.model_dump()
        with open(f"{self.scope}/{metadata_type}/{name}.json", "w") as f:
            import json
            json.dump(metadata, f)
    def get_metadata(self, metadata_type: str, name: str) -> BaseModel:
        try:
            with open(f"{self.scope}/{metadata_type}/{name}.json", "r") as f:
                import json
                metadata_dict = json.load(f)
                return BaseModel(**metadata_dict)
        except FileNotFoundError:
            return None

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

class GenesisBot:
    def __init__(self, bot_name, bot_description, tool_list, bot_implementation, docs, guardrails=BotKnowledgeGuardrails()):
        self.bot_name = bot_name
        self.bot_description = bot_description
        self.tool_list = tool_list
        self.bot_implementation = bot_implementation
        self.docs = docs
        self.guardrails = guardrails

class ToolDefinition(BaseModel):
    name: str
    description: str
    parameters: Dict[str, Any]  # JSON Schema

class GenesisProject(BaseModel):
    project_id: str
    project_name: str
    project_description: str
    project_items: Dict[int, Tuple[str, bool]] = {}

    def toggle_item_completion(self, item_index: int):
        if item_index in self.project_items:
            name, completed = self.project_items[item_index]
            self.project_items[item_index] = (name, not completed)
        else:
            raise ValueError("Item index not found")

class GenesisAPI:
    def __init__(self, server_type, scope):
        self.server_type = server_type
        self.scope = scope
        self.metadata_store = LocalMetadataStore(scope)
    def register_bot(self, bot: GenesisBot):
        self.metadata_store.insert_or_update_metadata("bot", bot)
    def get_bot(self, bot_name):
        return self.metadata_store.get_metadata("bot", bot_name)
    def register_tool(self, tool: ToolDefinition):
        self.metadata_store.insert_or_update_metadata("tool", tool)
    def get_tool(self, tool_name):
        return self.metadata_store.get_metadata("tool", tool_name)
    def register_project(self, project: GenesisProject):
        self.metadata_store.insert_or_update_metadata("project", project)
    def get_project(self, project_id) -> GenesisProject:
        return self.metadata_store.get_metadata("project", project_id)

class GenesisServer(ABC):
    def __init__(self, client):
        self.client = client
        self.bots = []
        self.adapters = []

    def add_bot(self, bot: GenesisBot):
        self.bots.append(bot)

    def add_adapter(self, adapter):
        self.adapters.append(adapter)

    def run(self, message):
        pass

class GenesisSynchronousServer(GenesisServer):
    def __init__(self, client):
        super().__init__(client)

    def add_message_and_wait(self, message):
        return "Dummy response"



client = GenesisAPI("local", scope="GENESIS_INTERNAL") # or "url of genesis server" with a valid API key

new_bot = GenesisBot(client=client,bot_name="my_bot", bot_description="my bot description", tool_list=["slack_tools", "database_tools"], 
                     bot_implementation="openai", docs=[])
client.register_bot(new_bot)
my_bot = client.retrieve_bot(bot_name="my_bot")
assert new_bot.bot_description == my_bot.bot_description

server = GenesisSynchronousServer(client=client)
server.add_bot(my_bot)
server.add_adapter(GenesisLocalAdapter())
server.run()
response = server.add_message_and_wait(message="what is the weather in SF?")
print(response)