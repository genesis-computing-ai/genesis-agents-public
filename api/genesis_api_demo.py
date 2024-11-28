from abc import ABC, abstractmethod
import sys
from pydantic import BaseModel
from typing import Dict, Any, Tuple, List
import os
import json
import argparse
from datetime import datetime
from snowflake.connector import SnowflakeConnection

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
        #"ToolDefinition": ("tools", "name"),
        "GenesisProject": ("PROJECTS", "PROJECT_ID"),
        "GenesisProcess": ("PROCESSES", "PROCESS_ID"),
        "GenesisNote": ("NOTEBOOK", "NOTE_ID"),
        "GenesisKnowledge": ("KNOWLEDGE", "KNOWLEDGE_THREAD_ID"),
        "GenesisHarvestResults": ("HARVEST_RESULTS", "SOURCE_NAME"),
    }
    conn: SnowflakeConnection = None

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
    def insert_or_update_metadata(self, metadata_type: str, name: str, metadata: BaseModel):
        pass
    def get_metadata(self, metadata_type: str, name: str) -> BaseModel:
        cursor = self.conn.cursor()
        table_name, filter_column = self.metadata_type_mapping.get(metadata_type, (None, None))
        if not table_name:
            raise ValueError(f"Unknown metadata type: {metadata_type}")
        query = f"SELECT * FROM app1.{table_name} WHERE {filter_column} = \\\'{name}\\\'"
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
        query = f"SELECT {filter_column} FROM app1.{table_name}"
        cursor.execute("call core.run_arbitrary('%s')" % query)
        metadata_list = cursor.fetchall()
        metadata_list = json.loads(metadata_list[0][0])
        metadata_list = [metadata[filter_column] for metadata in metadata_list]
        return metadata_list

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
    def add_message(self, bot_id, message, thread_id):
        pass
    def get_message(self, bot_id, thread_id):
        pass

class GenesisLocalServer(GenesisServer):
    def __init__(self, scope):
        super().__init__(scope)
        self.add_adapter(GenesisLocalAdapter())
    def add_message(self, bot_id, message, thread_id):
        return "Thread_12345"
    def get_message(self, bot_id, thread_id):
        return "Message from thread_12345"

class GenesisSnowflakeServer(GenesisServer):
    def __init__(self, scope):
        super().__init__(scope)
        self.add_adapter(GenesisLocalAdapter())
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
    def __init__(self, server_type, scope):
        self.server_type = server_type
        self.scope = scope
        if server_type == "local":
            self.metadata_store = LocalMetadataStore(scope)
            self.registered_server = GenesisLocalServer(scope)
        elif server_type == "snowflake":
            self.metadata_store = SnowflakeMetadataStore(scope)
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

def main():
    #client = GenesisAPI("local", scope="GENESIS_INTERNAL") # or "url of genesis server" with a valid API key
    client = GenesisAPI("snowflake", scope="GENESIS_BOTS_ALPHA") # or "url of genesis server" with a valid API key

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
    parser_register_bot.add_argument('--bot_id', required=True, help='ID of the bot')
    parser_register_bot.add_argument('--bot_name', required=True, help='Name of the bot')
    parser_register_bot.add_argument('--bot_description', required=True, help='Description of the bot')
    parser_register_bot.add_argument('--tool_list', required=True, nargs='+', help='List of tools for the bot')
    parser_register_bot.add_argument('--bot_implementation', required=True, help='Implementation of the bot')
    parser_register_bot.add_argument('--docs', required=True, nargs='+', help='Documentation for the bot')

    # Get bot
    parser_get_bot = subparsers.add_parser('get_bot', help='Get a bot by ID')
    parser_get_bot.add_argument('--bot_id', required=True, help='ID of the bot')

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

    # Add message
    parser_add_message = subparsers.add_parser('add_message', help='Add a message to a bot')
    parser_add_message.add_argument('--bot_id', required=True, help='ID of the bot')
    parser_add_message.add_argument('--message', required=True, help='Message to add', nargs='+')
    parser_add_message.add_argument('--thread_id', required=False, help='Thread ID for the message')

    # Get response
    parser_get_response = subparsers.add_parser('get_response', help='Get a response from a bot')
    parser_get_response.add_argument('--bot_id', required=True, help='ID of the bot')
    parser_get_response.add_argument('--thread_id', required=False, help='Thread ID for the response')

    # Harvest Results
    parser_get_harvest_results = subparsers.add_parser('get_harvest_results', help='Get harvest results by source name')
    parser_get_harvest_results.add_argument('--source_name', required=True, help='Source name for the harvest results')

    # Get all harvest results
    parser_get_all_harvest_results = subparsers.add_parser('get_all_harvest_results', help='Get all harvest results')

    # Register knowledge
    parser_register_knowledge = subparsers.add_parser('register_knowledge', help='Register new knowledge')
    parser_register_knowledge.add_argument('--knowledge_thread_id', required=True, help='Thread ID of the knowledge')

    # Get knowledge
    parser_get_knowledge = subparsers.add_parser('get_knowledge', help='Get knowledge by thread ID')
    parser_get_knowledge.add_argument('--thread_id', required=True, help='Thread ID of the knowledge')

    # Get all knowledge
    parser_get_all_knowledge = subparsers.add_parser('get_all_knowledge', help='Get all registered knowledge')

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
            bot = client.get_bot(args.bot_id)
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
        elif args.command == 'add_message':
            response = client.add_message(args.bot_id, thread_id=args.thread_id, message=" ".join(args.message))
            print(response)
        elif args.command == 'get_response':
            response = client.get_response(args.bot_id, args.thread_id)
            print(response)
        elif args.command == 'get_harvest_results':
            harvest_results = client.get_harvest_results(args.source_name)
            print(harvest_results)
        elif args.command == 'get_all_harvest_results':
            all_harvest_results = client.get_all_harvest_results()
            print(all_harvest_results)
        elif args.command == 'register_knowledge':
            new_knowledge = GenesisKnowledge(knowledge_thread_id=args.knowledge_thread_id)
            client.register_knowledge(new_knowledge)
        elif args.command == 'get_knowledge':
            knowledge = client.get_knowledge(args.thread_id)
            print(knowledge)
        elif args.command == 'get_all_knowledge':
            all_knowledge = client.get_all_knowledge()
            print(all_knowledge)
        elif args.command is None:
            print("Invalid command. Type 'help' for available commands.")
        else:
            print("Unknown command. Type 'help' for available commands.")

if __name__ == "__main__":
    main()
