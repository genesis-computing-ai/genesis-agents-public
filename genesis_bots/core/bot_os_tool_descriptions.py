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

# A global list of tools (function groups) and their descriptions.
# NOTE: This is used to populate the AVAILABLE_TOOLS table in the database.
_tools_data = [
    (
        "slack_tools",
        "Lookup slack users by name, and send direct messages in Slack",
    ),
    (
        "make_baby_bot",
        "Create, configure, and administer other bots programatically",
    ),
    (
        "harvester_tools",
        "Control the database harvester, add new databases to harvest, add schema inclusions and exclusions, see harvest status",
    ),
    (
        "process_runner_tools",
        "Tools to run processes.",
    ),
    (
        "notebook_manager_tools",
        "Tools to manage bot notebook.",
    ),
    (
        "data_dev_tools",
        "Tools for data development workflows including Jira integration",
    ),
]

process_runner_functions = [
    {
        "type": "function",
        "function": {
            "name": "_run_process",
            "description": "Run a process",
            "parameters": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "description": """
                        The action to perform: KICKOFF_PROCESS, GET_NEXT_STEP, END_PROCESS, TIME, or STOP_ALL_PROCESSES.  Either process_name or process_id must also be specified.
                        """,
                    },
                    "process_name": {
                        "type": "string",
                        "description": "The name of the process to run",
                    },
                    "process_id": {
                        "type": "string",
                        "description": "The id of the process to run (note: this is NOT the task_id or process_schedule_id)",
                    },
                    "previous_response": {
                        "type": "string",
                        "description": "The previous response from the bot (for use with GET_NEXT_STEP)",
                    },
                    "concise_mode": {
                        "type": "boolean",
                        "default": False,
                        "description": "Optional, to run in low-verbosity/concise mode. Default to False.",
                    },
                    "process_config": {
                        "type": "string", 
                        "description": "Optional configuration parameters for the process run in JSON format. Provide these only on the KICKOFF_PROCESS call."
                    },
                    #           "goto_step": {
                    #               "type": "string",
                    #               "description": "Directs the process runner to update the program counter",
                    #           },
                },
                "required": ["action"],
            },
        },
    },
]

process_runner_tools = {
    "_run_process": "tool_belt.run_process",
}


