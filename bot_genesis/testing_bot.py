testing_functions = [
    {
        "type": "function",
        "function": {
            "name": "_bot_tester",
            "description": "Manages tests for bots, including creating, updating, deleting, listing test scripts, test steps, and test schedules as well as running tests and returning results.",
            "parameters": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "description": """
                        The action to perform: CREATE TEST, UPDATE TEST, DELETE TEST, LIST TESTS, SHOW SCRIPT, ADD STEP, EDIT STEP, DELETE STEP, SET RUN SCHEDULE, LIST SCHEDULE, RUN SCHEDULE, \
                        RUN SCRIPT NOW, REPORT RESULTS, DELETE RESULTS, LIST RESULT SETS
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
                    "test_scripts": {
                        "type": "object",
                        "description": "A collection of scripts used to test bots",
                        "properties": {
                            "test_id": {
                                "type": "string",
                                "description": "The unique identifier of the test, create as bot_id_<random 6 character string>.",
                            },
                            "test_name": {
                                "type": "string",
                                "description": "The name of the test script.",
                            },
                            "tested_bot_id": {
                                "type": "string",
                                "description": "The id of the bot that is being tested",
                            },
                            "tested_bot_name": {
                                "type": "string",
                                "description": "The name of the bot that is being tested",
                            },
                            "scheduled_run_start_times": {
                                "type": "string",
                                "description": "A cron expression used to indicate scheduled runtimes for test",
                            },
                            "script": {
                                "type": "string",
                                "description": "Name of script csv file with steps to run test.  Includes looping, branching, and other logic.",
                            },
                        },
                        "required": [
                            "test_id",
                            "tested_bot_id",
                            "scheduled_run_start_times",
                            "steps",
                        ],
                    },
                    "test_results": {
                        "type": "object",
                        "description": "A collection of results from test runs",
                        "properties": {
                            "test_id": {
                                "type": "string",
                                "description": "The unique identifier of the task, create as bot_id_<random 6 character string>",
                            },
                            "test_results": {
                                "type": "string",
                                "description": "A list of tests, test steps, and results",
                            },
                            "required": [
                                # Not required for new test bot, will be added as a result of running tests
                            ],
                        },
                        "required": ["action", "bot_id", "test_scripts"],
                    },
                },
            },
        },
    }
]
