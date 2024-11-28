from abc import ABC, abstractmethod
import sys
from pydantic import BaseModel
from typing import Dict, Any, Tuple, List
import os
import json
import argparse
from datetime import datetime

class GenesisMetadataStore(BaseModel):
    scope: str
    def __init__(self, scope):
        super().__init__(scope=scope)
    def insert_or_update_metadata(self, metadata_type: str, name: str, metadata: Dict[str, Any]):
        pass
    def get_metadata(self, metadata_type: str, name: str) -> Dict[str, Any]:
        pass
    def get_all_metadata(self, metadata_type: str):
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
    def get_metadata(self, metadata_type: str, name: str) -> BaseModel:
        try:
            with open(f"{self.metadata_dir}/{self.scope}/{metadata_type}/{name}.json", "r") as f:
                metadata_dict = json.load(f)
                # Dynamically instantiate the original object type
                metadata_class = getattr(sys.modules[__name__], metadata_dict['type'])
                return metadata_class(**metadata_dict)
        except FileNotFoundError:
            return None
    def get_all_metadata(self, metadata_type: str):
        metadata_type_dir = f"{self.metadata_dir}/{self.scope}/{metadata_type}"
        if not os.path.exists(metadata_type_dir):
            return []
        return [f.name for f in os.scandir(metadata_type_dir) if f.is_file()]

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
    bot_name: str
    bot_description: str
    tool_list: List[str]
    bot_implementation: str
    docs: List[str]
    guardrails: BotKnowledgeGuardrails = BotKnowledgeGuardrails()
    client: Any = None
    def set_client(self, client):
        self.client = client
    def add_message_and_wait(self, message):
        return "Dummy response"
    
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

class GenesisIOAdapter(ABC):
    @abstractmethod
    def get_input(self):
        pass

    @abstractmethod
    def send_output(self, output):
        pass

class GenesisLocalAdapter(GenesisIOAdapter):
    def get_input(self):
        return input()

    def send_output(self, output):
        print(output)

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

class GenesisLocalServer(GenesisServer):
    def __init__(self, scope):
        super().__init__(scope)
        self.add_adapter(GenesisLocalAdapter())

    def add_message_and_wait(self, message):
        return "Dummy response"

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

class GenesisAPI:
    def __init__(self, server_type, scope):
        self.server_type = server_type
        self.scope = scope
        if server_type == "local":
            self.metadata_store = LocalMetadataStore(scope)
            self.registered_server = GenesisLocalServer(scope)
        else:
            raise ValueError("Remote server not supported yet")
    def register_bot(self, bot: GenesisBot):
        self.metadata_store.insert_or_update_metadata("bot", bot.bot_name, bot)
    def get_bot(self, bot_name):
        bot = self.metadata_store.get_metadata("bot", bot_name)
        bot.set_client(self)
        return bot
    def get_all_bots(self):
        return self.metadata_store.get_all_metadata("bot")
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
        self.metadata_store.insert_or_update_metadata("project", project.project_id, project)
    def get_project(self, project_id) -> GenesisProject:
        return self.metadata_store.get_metadata("project", project_id)
    def get_all_projects(self):
        return self.metadata_store.get_all_metadata("project")
    def register_process(self, process: GenesisProcess):
        self.metadata_store.insert_or_update_metadata("process", process.process_id, process)
    def get_process(self, process_id) -> GenesisProcess:
        return self.metadata_store.get_metadata("process", process_id)
    def get_all_processes(self):
        return self.metadata_store.get_all_metadata("process")
    def register_note(self, note: GenesisNote):
        self.metadata_store.insert_or_update_metadata("note", note.note_id, note)
    def get_note(self, note_id) -> GenesisNote:
        return self.metadata_store.get_metadata("note", note_id)
    def get_all_notes(self):
        return self.metadata_store.get_all_metadata("note")

def main():
    client = GenesisAPI("local", scope="GENESIS_INTERNAL") # or "url of genesis server" with a valid API key
    #new_bot = GenesisBot(bot_name="my_bot", bot_description="my bot description", tool_list=["send_slack_message", "run_sql_query"], 
    #                     bot_implementation="openai", docs=["doc1.txt", "doc2.txt"])
    #client.register_bot(new_bot)
    #my_bot = client.get_bot(bot_name="my_bot")
    #assert new_bot.bot_description == my_bot.bot_description
    #response = my_bot.add_message_and_wait(message="what is the weather in SF?")
    #print(response)

    parser = argparse.ArgumentParser(description='CLI for GenesisAPI')
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')

    # Register bot
    parser_register_bot = subparsers.add_parser('register_bot', help='Register a new bot')
    parser_register_bot.add_argument('--bot_name', required=True, help='Name of the bot')
    parser_register_bot.add_argument('--bot_description', required=True, help='Description of the bot')
    parser_register_bot.add_argument('--tool_list', required=True, nargs='+', help='List of tools for the bot')
    parser_register_bot.add_argument('--bot_implementation', required=True, help='Implementation of the bot')
    parser_register_bot.add_argument('--docs', required=True, nargs='+', help='Documentation for the bot')

    # Get bot
    parser_get_bot = subparsers.add_parser('get_bot', help='Get a bot by name')
    parser_get_bot.add_argument('--bot_name', required=True, help='Name of the bot')

    # Get all bots
    parser_get_all_bots = subparsers.add_parser('get_all_bots', help='Get all registered bots')

    # Run tool
    parser_run_tool = subparsers.add_parser('run_tool', help='Run a tool with given parameters')
    parser_run_tool.add_argument('--tool_name', required=True, help='Name of the tool')
    parser_run_tool.add_argument('--tool_parameters', required=True, help='Parameters for the tool')

    # Register project
    parser_register_project = subparsers.add_parser('register_project', help='Register a new project')
    parser_register_project.add_argument('--project_id', required=True, help='ID of the project')

    # Get project
    parser_get_project = subparsers.add_parser('get_project', help='Get a project by ID')
    parser_get_project.add_argument('--project_id', required=True, help='ID of the project')

    # Get all projects
    parser_get_all_projects = subparsers.add_parser('get_all_projects', help='Get all registered projects')

    # Register process
    parser_register_process = subparsers.add_parser('register_process', help='Register a new process')
    parser_register_process.add_argument('--process_id', required=True, help='ID of the process')

    # Get process
    parser_get_process = subparsers.add_parser('get_process', help='Get a process by ID')
    parser_get_process.add_argument('--process_id', required=True, help='ID of the process')

    # Get all processes
    parser_get_all_processes = subparsers.add_parser('get_all_processes', help='Get all registered processes')

    # Register notebook
    parser_register_notebook = subparsers.add_parser('register_notebook', help='Register a new notebook')
    parser_register_notebook.add_argument('--notebook_id', required=True, help='ID of the notebook')

    # Get notebook
    parser_get_notebook = subparsers.add_parser('get_notebook', help='Get a notebook by ID')
    parser_get_notebook.add_argument('--notebook_id', required=True, help='ID of the notebook')

    # Get all notebooks
    parser_get_all_notebooks = subparsers.add_parser('get_all_notebooks', help='Get all registered notebooks')

    while True:
        try:
            args = parser.parse_args(input("Enter a command: ").split())
        except SystemExit as e:
            if isinstance(e, SystemExit) and e.code == 2:  # Code 2 indicates --help was used
                parser.print_help()
                #sys.exit(2)
            else:
                print("Error:",e)
            continue
        if args.command == 'register_bot':
            new_bot = GenesisBot(bot_name=args.bot_name, bot_description=args.bot_description, tool_list=args.tool_list, bot_implementation=args.bot_implementation, docs=args.docs)
            client.register_bot(new_bot)
        elif args.command == 'get_bot':
            bot = client.get_bot(args.bot_name)
            print(bot)
        elif args.command == 'get_all_bots':
            bots = client.get_all_bots()
            print(bots)
        elif args.command == 'run_tool':
            response = client.run_tool(args.tool_name, args.tool_parameters)
            print(response)
        elif args.command == 'register_project':
            new_project = GenesisProject(project_id=args.project_id)
            client.register_project(new_project)
        elif args.command == 'get_project':
            project = client.get_project(args.project_id)
            print(project)
        elif args.command == 'get_all_projects':
            projects = client.get_all_projects()
            print(projects)
        elif args.command == 'register_process':
            new_process = GenesisProcess(process_id=args.process_id)
            client.register_process(new_process)
        elif args.command == 'get_process':
            process = client.get_process(args.process_id)
            print(process)
        elif args.command == 'get_all_processes':
            processes = client.get_all_processes()
            print(processes)
        elif args.command == 'register_notebook':
            new_notebook = GenesisNote(notebook_id=args.notebook_id)
            client.register_notebook(new_notebook)
        elif args.command == 'get_notebook':
            notebook = client.get_notebook(args.notebook_id)
            print(notebook)
        elif args.command == 'get_all_notebooks':
            notebooks = client.get_all_notebooks()
            print(notebooks)
        elif args.command is None:
            print("Invalid command. Type 'help' for available commands.")
        else:
            print("Unknown command. Type 'help' for available commands.")

if __name__ == "__main__":
    main()
