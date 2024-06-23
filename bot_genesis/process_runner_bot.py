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
                        The action to perform: GET_ANSWER
                        """,
                    },
                    "bot_id": {
                        "type": "string",
                        "description": "The identifier of the test bot.",
                    },
                    "bot_name": {
                        "type": "string",
                        "description": "The name of the test bot.",
                    },
                    "bot_description": {
                        "type": "string",
                        "description": "The description of the test bot.",
                    },
                },
            },
        },
    }
]

process_runner_tools = {"_run_process": "db_adapter.run_process"}


def run_process(self, action, thread_id):  # MOVE OUT OF ADAPTER!
    print(f"Running processes Action: {action} | thread_id: {thread_id}")
    if action == "GET_ANSWER":
        print("The meaning of life has been discovered - 24!")
        return {
            "Success": True,
            "Message": "The meaning of life has been discovered - 24!",
        }
