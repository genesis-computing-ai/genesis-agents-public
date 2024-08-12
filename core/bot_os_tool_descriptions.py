# From Justin on Slack 2:13 PM 6/18/2024
# I think the “test bot” is just one designated to run the test processes on other bots via scheduled tasks for the test bot
# The test bot will be told by a task that it’s time to run the “test Eliza daily” process for example
# Then it will call the process tool and say it wants to run that process and asks what to do first
# The tool will run the secondary LLM to generate the instruction for the test bot of “what to do next”
# The test bot will do that (for example ask Eliza to search the metadata for baseball) then report back to the tool what happened (for example it found 10 tables about baseball)
# Then the tool will call the secondary LLM with the process descriptions, what has happened so far , and the results of the most recent step, and ask the secondary LLM what it should do next.
# And so on until the secondary LLM decides the process is finished or in some kind of unrecoverable error state (edited)
# And the tool will log what happens on each step and whether it was successful or not
# So 3 llms at play here , one for the test bot, one for the target bot being tested (although other processes like “account reconciliation” won’t always involve another bot), and one for the secondary LLM
# Keeping the secondary LLM focused on adjudicating the step results and deciding what should be done next should keep everything on track
# But it will be mediated by the tool so it doesn’t need to talk directly to any of the bots which keeps it simpler

BOT_DISPATCH_DESCRIPTIONS = [
    {
        "type": "function",
        "function": {
            "name": "dispatch_to_bots",
            "description": 'Specify an arry of templated natual language tasks you want to execute in parallel to a set of bots like you. for example, "Who is the president of {{ country_name }}". Never use this tool for arrays with < 2 items.',
            "parameters": {
                "type": "object",
                "properties": {
                    "task_template": {
                        "type": "string",
                        "description": "Jinja template for the tasks you want to farm out to other bots",
                    },
                    "args_array": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": 'Arguments you want to fill in for each jinja template variable of the form [{"country_name": "france"}, {"country_name": "spain"}]',
                    },
                },
                "required": ["task_template", "args_array"],
                "bot_id": {
                    "type": "string",
                    "description": "The unique identifier for an existing bot you are aware of to dispatch the tasks to. Should be the bot_name dash a 6 letter alphanumeric random code, for example mybot-w73hxg. Pass None to dispatch to yourself.",
                },
            },
        },
    }
]
# "bot_id": {
#     "type": "string",
#     "description": "The unique identifier for an existing bot you are aware of to dispatch the tasks to. Should be the bot_name dash a 6 letter alphanumeric random code, for example mybot-w73hxg."
# }


bot_dispatch_tools = {"dispatch_to_bots": "core.bot_os_tools.dispatch_to_bots"}

process_runner_functions = [
    {
        "type": "function",
        "function": {
            "name": "_run_process",
            "description": "Manages processes run on other bots",
            "parameters": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "description": """
                        The action to perform on the task can be one of these: KICKOFF_PROCESS, GET_NEXT_STEP, GOTO_STEP
                        """,
                    },
                    "process_name": {
                        "type": "string",
                        "description": "The name the process to run",
                    },
                    "previous_response": {
                        "type": "string",
                        "description": "The previous response from the bot",
                    },
                    "goto_step": {
                        "type": "string",
                        "description": "Directs the process runner to update the program counter",
                    },
                },
            },
        },
    }
]

process_runner_tools = {"_run_process": "tool_belt.run_process"}

# Start of Generated Description
webpage_downloader_functions = [
    {
        "type": "function",
        "function": {
            "name": "_webpage_downloader",
            "description": "Downloads a webpage and returns its HTML content and hyperlinks in chunks, ensuring each chunk does not exceed 512KB. Allows specifying a chunk index to download specific parts of the beautified content. This tool is particularly useful for large and complex webpages and utilizes BeautifulSoup for parsing. It might require multiple sequential chunk downloads to capture the complete content relevant to the user's request.",
            "parameters": {
                "type": "object",
                "properties": {
                    "url": {
                        "type": "string",
                        "description": "The URL of the webpage to download.",
                    },
                    "chunk_index": {
                        "type": "integer",
                        "default": 0,
                        "description": "The specific chunk index to download, with each chunk being up to 512KB in size. Defaults to the first chunk (0) if not specified.",
                    },
                },
                "required": ["url"],
            },
        },
    }
]

# webpage_downloader_tools = {
#     "webpage_downloader": "generated_modules.webpage_downloader.download_webpage"
# }
# webpage_downloader_action_function_mapping = {
#     "webpage_downloader": "generated_modules.webpage_downloader.download_webpage"
# }

webpage_downloader_tools = {
    "_webpage_downloader": "tool_belt.download_webpage"
}
webpage_downloader_action_function_mapping = {
    "webpage_downloader": "tool_belt.download_webpage"
}


tools_data = [
    (
        "slack_tools",
        "Lookup slack users by name, and send direct messages in Slack",
    ),
    (
        "make_baby_bot",
        "Create, configure, and administer other bots programatically",
    ),
    # ('integrate_code', 'Create, test, and deploy new tools that bots can use'),
    (
        "webpage_downloader",
        "Access web pages on the internet and return their contents",
    ),
    (
        "database_tools",
        "Discover database metadata, find database tables, and run SQL queries on a database",
    ),
    (
        "harvester_tools",
        "Control the database harvester, add new databases to harvest, add schema inclusions and exclusions, see harvest status",
    ),
    (
        "snowflake_stage_tools",
        "Read, update, write, list, and delete from Snowflake Stages including Snowflake Semantic Models.",
    ),
    (
        "snowflake_semantic_tools",
        "Create and modify Snowflake Semantic Models",
    ),
    ("image_tools", "Tools to interpret visual images and pictures"),
    (
        "autonomous_tools",
        "Tools for bots to create and managed autonomous tasks",
    ),
    (
        "process_runner_tools",
        "Tools to run processes.",
    ),
    (
        "process_manager_tools",
        "Tools to create and manage processes.",
    ),
]
