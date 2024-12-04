import argparse
from genesis_api import GenesisAPI, GenesisBot, GenesisProject, GenesisProcess, GenesisNote, GenesisKnowledge

def main():
    #client = GenesisAPI("local", scope="GENESIS_INTERNAL") 
    client = GenesisAPI("remote-snowflake", scope="GENESIS_BOTS_ALPHA") 
    #client = GenesisAPI("local-snowflake", scope="GENESIS_TEST", sub_scope="GENESIS_INTERNAL") 

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

    # Get project asset
    parser_get_project_asset = subparsers.add_parser('get_project_asset', help='Get a project asset by ID')
    parser_get_project_asset.add_argument('--asset_id', required=True, help='ID of the asset')

    # Get all project assets    
    parser_get_all_project_assets = subparsers.add_parser('get_all_project_assets', help='Get all assets for a project')
    parser_get_all_project_assets.add_argument('--project_id', required=True, help='ID of the project')

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
    parser_get_response.add_argument('--request_id', required=True, help='Request ID from add_messagefor the response')

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

    # Get message log
    parser_get_message_log = subparsers.add_parser('get_message_log', help='Get message log by bot ID and thread ID')
    parser_get_message_log.add_argument('--bot_id', required=True, help='ID of the bot')
    parser_get_message_log.add_argument('--thread_id', required=False, help='Thread ID for the message log')
    parser_get_message_log.add_argument('--last_n', required=False, help='Last N messages to return')

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
        elif args.command == 'get_project_asset':
            asset = client.get_project_asset(args.asset_id)
            print(asset)
        elif args.command == 'get_all_project_assets':
            assets = client.get_all_project_assets(args.project_id)
            print(assets)
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
            response = client.get_response(args.bot_id, args.request_id)
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
        elif args.command == 'get_message_log':
            message_log = client.get_message_log(args.bot_id, args.thread_id, int(args.last_n) if args.last_n else None)
            print(message_log)
        elif args.command is None:
            print("Invalid command. Type 'help' for available commands.")
        else:
            print("Unknown command. Type 'help' for available commands.")

if __name__ == "__main__":
    main()
