from   datetime                 import datetime
import json
import os
import random
import string
import threading
import time
from   typing                   import Callable, List


from   genesis_bots.core.bot_os_tools2 \
                                import (ToolFuncDescriptor,
                                        ToolFuncGroupLifetime,
                                        get_global_tools_registry,
                                        get_tool_func_descriptor)

# from   genesis_bots.llm.llm_openai.bot_os_openai import StreamingEventHandler



from   genesis_bots.bot_genesis.make_baby_bot \
                                import (MAKE_BABY_BOT_DESCRIPTIONS,
                                        make_baby_bot_tools)
from   genesis_bots.connectors.database_tools \
                                import (notebook_manager_functions,
                                        notebook_manager_tools)
from   jinja2                   import Template

from   genesis_bots.schema_explorer.harvester_tools \
                                import (harvester_tools_functions,
                                        harvester_tools_list)
from   genesis_bots.slack.slack_tools \
                                import slack_tools, slack_tools_descriptions



from   genesis_bots.core.bot_os_tool_descriptions \
                                import (process_runner_functions,
                                        process_runner_tools)

from   genesis_bots.connectors.snowflake_connector.snowflake_connector \
                                import SnowflakeConnector

# from connectors.snowflake_tools import (
#                                         snowflake_tools,
#                                         snowflake_functions,
#                                         )


from   genesis_bots.core.logging_config \
                                import logger

from   genesis_bots.core.tools.tool_helpers \
                                import (chat_completion, get_process_info,
                                        get_sys_email)


genesis_source = os.getenv("GENESIS_SOURCE", default="Snowflake")

# We use this URL to include the genesis logo in snowflake-generated emails.
# TODO: use a permanent URL under the genesiscomputing.ai domain
GENESIS_LOGO_URL = "https://i0.wp.com/genesiscomputing.ai/wp-content/uploads/2024/05/Genesis-Computing-Logo-White.png"

class ToolBelt:
    def __init__(self):
        self.counter = {}
        self.instructions = {}
        self.process_config = {}
        self.process_history = {}
        self.done = {}
        self.silent_mode = {}
        self.last_fail= {}
        self.fail_count = {}
        self.lock = threading.Lock()
        self.recurse_stack = []
        self.recurse_level = 1
        self.process_id = {}
        # self.include_code = False
        self.sys_default_email = get_sys_email()

        if os.getenv("SNOWFLAKE_METADATA", "False").lower() == "false":
            from genesis_bots.connectors import get_global_db_connector
            self.db_adapter = get_global_db_connector()
        else:
            self.db_adapter = SnowflakeConnector(connection_name="Snowflake")  # always use this for metadata

        # self.todos = ProjectManager(self.db_adapter)  # Initialize Todos instance
        # self.git_manager = GitFileManager()
        self.server = None  # Will be set later

    def set_server(self, server):
        """Set the server instance for this toolbelt"""
        self.server = server

    # ====== RUN PROCESSES START ==========================================================================================

    def set_process_cache(self, bot_id, thread_id, process_id):
        cache_dir = "./process_cache"
        os.makedirs(cache_dir, exist_ok=True)
        cache_file = os.path.join(cache_dir, f"{bot_id}_{thread_id}_{process_id}.json")

        cache_data = {
            "counter": self.counter.get(thread_id, {}).get(process_id),
            "last_fail": self.last_fail.get(thread_id, {}).get(process_id),
            "fail_count": self.fail_count.get(thread_id, {}).get(process_id),
            "instructions": self.instructions.get(thread_id, {}).get(process_id),
            "process_history": self.process_history.get(thread_id, {}).get(process_id),
            "done": self.done.get(thread_id, {}).get(process_id),
            "silent_mode":  self.silent_mode.get(thread_id, {}).get(process_id)
        }

        with open(cache_file, 'w') as f:
            json.dump(cache_data, f)

    def get_process_cache(self, bot_id, thread_id, process_id):
        cache_file = os.path.join("./process_cache", f"{bot_id}_{thread_id}_{process_id}.json")

        if os.path.exists(cache_file):
            with open(cache_file, 'r') as f:
                cache_data = json.load(f)

            with self.lock:
                if thread_id not in self.counter:
                    self.counter[thread_id] = {}
                self.counter[thread_id][process_id] = cache_data.get("counter")

                if thread_id not in self.last_fail:
                    self.last_fail[thread_id] = {}
                self.last_fail[thread_id][process_id] = cache_data.get("last_fail")

                if thread_id not in self.fail_count:
                    self.fail_count[thread_id] = {}
                self.fail_count[thread_id][process_id] = cache_data.get("fail_count")

                if thread_id not in self.instructions:
                    self.instructions[thread_id] = {}
                self.instructions[thread_id][process_id] = cache_data.get("instructions")

                if thread_id not in self.process_history:
                    self.process_history[thread_id] = {}
                self.process_history[thread_id][process_id] = cache_data.get("process_history")

                if thread_id not in self.done:
                    self.done[thread_id] = {}
                self.done[thread_id][process_id] = cache_data.get("done")

                if thread_id not in self.silent_mode:
                    self.silent_mode[thread_id] = {}
                self.silent_mode[thread_id][process_id] = cache_data.get("silent_mode", False)

            return True
        return False

    def clear_process_cache(self, bot_id, thread_id, process_id):
        cache_file = os.path.join("./process_cache", f"{bot_id}_{thread_id}_{process_id}.json")

        if os.path.exists(cache_file):
            os.remove(cache_file)
            return True
        return False

    def get_current_time_with_timezone(self):
        current_time = datetime.now().astimezone()
        return current_time.strftime("%Y-%m-%d %H:%M:%S %Z")

    def clear_process_registers_by_thread(self, thread_id):
        # Initialize thread-specific data structures if not already present
        with self.lock:
            if thread_id not in self.counter:
                self.counter[thread_id] = {}
            #   if thread_id not in self.process:
            #       self.process[thread_id] = {}
            if thread_id not in self.last_fail:
                self.last_fail[thread_id] = {}
            if thread_id not in self.fail_count:
                self.fail_count[thread_id] = {}
            if thread_id not in self.instructions:
                self.instructions[thread_id] = {}
            if thread_id not in self.process_history:
                self.process_history[thread_id] = {}
            if thread_id not in self.done:
                self.done[thread_id] = {}
            if thread_id not in self.silent_mode:
                self.silent_mode[thread_id] = {}
            if thread_id not in self.process_config:
                self.process_config[thread_id] = {}

    def clear_all_process_registers(self, thread_id):
        # Initialize thread-specific data structures if not already present
        with self.lock:
            self.counter[thread_id] = {}
            self.last_fail[thread_id]  = {}
            self.fail_count[thread_id]  = {}
            self.instructions[thread_id]  = {}
            self.process_history[thread_id]  = {}
            self.done[thread_id]  = {}
            self.silent_mode[thread_id]  = {}
            self.process_config[thread_id]  = {}

    def run_process(
        self,
        action,
        previous_response="",
        process_name="",
        process_id=None,
        process_config=None,
        thread_id=None,
        bot_id=None,
        concise_mode=False,
        bot_name=None
    ):
        #  logger.info(f"Running processes Action: {action} | process_id: {process_id or 'None'} | Thread ID: {thread_id or 'None'}")
        #         self.recurse_level = 0
        self.recurse_stack.append({thread_id: thread_id, process_id: process_id})

        if process_id is not None and process_id == '':
            process_id = None
        if process_name is not None and process_name == '':
            process_name = None

        if process_config is not None and action != "KICKOFF_PROCESS":
            return {
                "Success": False,
                "Error": "process_config should only be supplied when action is KICKOFF_PROCESS"
            }


        if action == "TIME":
            return {
                "current_system_time": datetime.now()
            }

        if bot_id is None:
            return {
                "Success": False,
                "Error": "Bot_id and either process_id or process_name are required parameters."
            }

        # Convert verbose to boolean if it's a string

        # Invert silent_mode if it's a boolean
        silent_mode = concise_mode
        verbose = False
        if isinstance(silent_mode, bool):
            verbose = not silent_mode

        if isinstance(silent_mode, str):
            if silent_mode.upper() == 'TRUE':
                silent_mode = True
                verbose = False
            else:
                silent_mode = False
                verbose = True

        # Ensure verbose is a boolean
        if not isinstance(silent_mode, bool):
            verbose = True

        # Check if both process_name and process_id are None
        if process_name is None and process_id is None:
            return {
                "Success": False,
                "Error": "Either process_name or process_id must be provided."
            }

        self.clear_process_registers_by_thread(thread_id)

        # Try to get process info from PROCESSES table
        process = get_process_info(bot_id, process_name=process_name, process_id=process_id)

        if len(process) == 0:
            # Get a list of processes for the bot
            processes = self.db_adapter.get_processes_list(bot_id)
            if processes is not None:
                process_list = ", ".join([p['process_name'] for p in processes['processes']])
                return_dict = {
                    "Success": False,
                    "Message": f"Process not found. Available processes are {process_list}.",
                    "Suggestion": "If one of the available processess is a very close match for what you're looking for, go ahead and run it."
                }
                if silent_mode is True:
                    return_dict["Reminder"] = "Remember to call the process in concise_mode as requested previously once you identify the right one"
                return return_dict
            else:
                return {
                    "Success": False,
                    "Message": f"Process not found. {bot_id} has no processes defined.",
                }
        process = process['Data']
        process_id = process['PROCESS_ID']
        process_name = process['PROCESS_NAME']
        process_config_default = process.get('PROCESS_CONFIG', '')
        if process_config_default is None:
            process_config_default = "None"
        if process_config is None:
            process['PROCESS_CONFIG'] = "None"
            process_config = "None"
        else:
            process['PROCESS_CONFIG'] = process_config

        if action == "KICKOFF_PROCESS":
            logger.info("Kickoff process.")

            with self.lock:
                self.counter[thread_id][process_id] = 1
                #       self.process[thread_id][process_id] = process
                self.last_fail[thread_id][process_id] = None
                self.fail_count[thread_id][process_id] = 0
                self.instructions[thread_id][process_id] = None
                self.process_config[thread_id][process_id] = process_config
                self.process_history[thread_id][process_id] = None
                self.done[thread_id][process_id] = False
                self.silent_mode[thread_id][process_id] = silent_mode
                self.process_id[thread_id] = process_id

            logger.info(
                f"Process {process_name} has been kicked off."
            )

            extract_instructions = f"""
            You will need to break the process instructions below up into individual steps and and return them one at a time.
            By the way the current system time is {datetime.now()}.
            By the way, the system default email address (SYS$DEFAULT_EMAIL) is {self.sys_default_email}.  If the instructions say to send an email
            to SYS$DEFAULT_EMAIL, replace it with {self.sys_default_email}.
            Start by returning the first step of the process instructions below.
            If there one or more {{dynamic parameters}} in the process instructions for the first step, replace them with the actual values from the below-provided process configuration.
            If there is a dynamic parameter needed that is not provided in the process configuration, return an error message and stop running the process.
            Simply return the first instruction on what needs to be done first without removing or changing any details, except for replacing dynamic parameters, if any.

            Also, if the instructions include a reference to note, don't look up the note contents, just pass on the note_id or note_name.
            The note contents will be unpacked by whatever tool is used depending on the type of note, either run_query if the note is of
            type sql or run_snowpark_sql if the note is of type python.

            If a step of the instructions says to run another process, return '>> RECURSE' and the process name or process id as the first step
            and then call _run_process with the action KICKOFF_PROCESS to get the first step of the next process to run.  Continue this process until
            you have completed all the steps.  If you are asked to run another process as part of this process, follow the same instructions.  Do this
            up to ten times.

            Process Instructions:
            {process['PROCESS_INSTRUCTIONS']}
            """

            if process['PROCESS_CONFIG'] != "None":
                extract_instructions += f"""

            Process configuration:
            {process['PROCESS_CONFIG']}.

            """

            first_step = chat_completion(extract_instructions, self.db_adapter, bot_id = bot_id, bot_name = '', thread_id=thread_id, process_id=process_id, process_name=process_name)

            # Check if the first step contains ">>RECURSE"
            if ">> RECURSE" in first_step or ">>RECURSE" in first_step:
                self.recurse_level += 1
                self.recurse_stack.append({thread_id: thread_id, process_id: process_id})
                # Extract the process name or ID
                process_to_run = first_step.split(">>RECURSE")[1].strip() if ">>RECURSE" in first_step else first_step.split(">> RECURSE")[1].strip()

                # Prepare the instruction for the bot to run the nested process
                first_step = f"""
                Use the _run_process tool to run the process '{process_to_run}' with the following parameters:
                - action: KICKOFF_PROCESS
                - process_name: {process_to_run}
                - bot_id: {bot_id}
                - silent_mode: {silent_mode}

                After the nested process completes, continue with the next step of this process.
                """

            with self.lock:
                self.process_history[thread_id][process_id] = "First step: "+ first_step + "\n"

                self.instructions[thread_id][process_id] = f"""
                Hey **@{process['BOT_ID']}**

                {first_step}

                Execute this instruction now and then pass your response to the _run_process tool as a parameter called previous_response and an action of GET_NEXT_STEP.
                Execute the instructions you were given without asking for permission.  Do not ever verify anything with the user, unless you need to get a specific input
                from the user to be able to continue the process.

                Also, if you are asked to run either sql or snowpark_python from a given note_id, make sure you examine the note_type field and use the appropriate tool for
                the note type.  Only pass the note_id, not the code itself, to the appropriate tool where the note will be handled.
                """
            if self.sys_default_email:
                self.instructions[thread_id][process_id] += f"""
                The system default email address (SYS$DEFAULT_EMAIL) is {self.sys_default_email}.  If you need to send an email, use this address.
                """

            if verbose:
                self.instructions[thread_id][process_id] += """
                    However DO generate text explaining what you are doing and showing interium outputs, etc. while you are running this and further steps to keep the user informed what is going on, preface these messages by 🔄 aka :arrows_counterclockwise:.
                    Oh, and mention to the user before you start running the process that they can send "stop" to you at any time to stop the running of the process, and if they want less verbose output next time they can run request to run the process in "concise mode".
                    And keep them informed while you are running the process about what you are up to, especially before you call various tools.
                    """
            else:
                self.instructions[thread_id][process_id] += """
                This process is being run in low verbosity mode. Do not directly repeat the first_step instructions to the user, just perform the steps as instructed.
                Also, if you are asked to run either sql or snowpark_python from a given note_id, make sure you examine the note_type field and use the appropriate tool for
                the note type.  Only pass the note_id, not the code itself, to the appropriate tool where the note will be handled.
                """
            self.instructions[thread_id][process_id] += f"""
            In your response back to _run_process, provide a DETAILED description of what you did, what result you achieved, and why you believe this to have successfully completed the step.
            Do not use your memory or any cache that you might have.  Do not simulate any user interaction or tools calls.  Do not ask for any user input unless instructed to do so.
            If you are told to run another process as part of this process, actually run it, and run it completely before returning the results to this parent process.
            By the way the current system time is {datetime.now()}.  You can call manage_process with
            action TIME to get updated time if you need it when running the process.

            Now, start by performing the FIRST_STEP indicated above.
            """
            self.instructions[thread_id][process_id] += "..... P.S. I KNOW YOU ARE IN SILENT MODE BUT ACTUALLY PERFORM THIS STEP NOW, YOU ARE NOT DONE YET!"

            self.instructions[thread_id][process_id] = "\n".join(
                line.lstrip() for line in self.instructions[thread_id][process_id].splitlines()
                )

            # Call set_process_cache to save the current state
            self.set_process_cache(bot_id, thread_id, process_id)
            #    logger.info(f'Process cached with bot_id: {bot_id}, thread_id: {thread_id}, process_id: {process_id}')

            return {"Success": True, "Instructions": self.instructions[thread_id][process_id], "process_id": process_id}

        elif action == "GET_NEXT_STEP":
            logger.info("Entered GET NEXT STEP")

            if thread_id not in self.counter and process_id not in self.counter[thread_id]:
                return {
                    "Success": False,
                    "Message": f"Error: GET_NEXT_STEP seems to have been run before KICKOFF_PROCESS. Please retry from KICKOFF_PROCESS."
                }

            # Load process cache
            if not self.get_process_cache(bot_id, thread_id, process_id):
                return {
                    "Success": False,
                    "Message": f"Error: Process cache for {process_id} couldn't be loaded. Please retry from KICKOFF_PROCESS."
                }
            # Print that the process cache has been loaded and the 3 params to get_process_cache
            logger.info(f"Process cache loaded with params: bot_id: {bot_id}, thread_id: {thread_id}, process_id: {process_id}")

            # Check if silent_mode is set for the thread and process
            verbose = True
            if thread_id in self.silent_mode and process_id in self.silent_mode[thread_id]:
                if self.silent_mode[thread_id][process_id]:
                    verbose = False

            with self.lock:
                if process_id not in self.process_history[thread_id]:
                    return {
                        "Success": False,
                        "Message": f"Error: Process {process_name} with id {process_id} couldn't be continued. Please retry once more from KICKOFF_PROCESS."
                    }

                if self.done[thread_id][process_id]:
                    self.last_fail[thread_id][process_id] = None
                    self.fail_count[thread_id][process_id] = None
                    return {
                        "Success": True,
                        "Message": f"Process {process_name} run complete.",
                    }

                if self.last_fail[thread_id][process_id] is not None:
                    check_response = f"""
                    A bot has retried a step of a process based on your prior feedback (shown below).  Also below is the previous question that the bot was
                    asked and the response the bot gave after re-trying to perform the task based on your feedback.  Review the response and determine if the
                    bot's response is now better in light of the instructions and the feedback you gave previously. You can accept the final results of the
                    previous step without asking to see the sql queries and results that led to the final conclusion.  Do not nitpick validity of actual data value
                    like names and similar.  Do not ask to see all the raw data that a query or other tool has generated. If you are very seriously concerned that the step
                    may still have not have been correctly perfomed, return a request to again re-run the step of the process by returning the text "**fail**"
                    followed by a DETAILED EXPLAINATION as to why it did not pass and what your concern is, and why its previous attempt to respond to your criticism
                    was not sufficient, and any suggestions you have on how to succeed on the next try. If the response looks correct, return only the text string
                    "**success**" (no explanation needed) to continue to the next step.  At this point its ok to give the bot the benefit of the doubt to avoid
                    going in circles.  By the way the current system time is {datetime.now()}.

                    Process Config: {self.process_config[thread_id][process_id]}

                    Full Process Instructions: {process['PROCESS_INSTRUCTIONS']}

                    Process History so far this run: {self.process_history[thread_id][process_id]}

                    Your previous guidance: {self.last_fail[thread_id][process_id]}

                    Bot's latest response: {previous_response}
                    """
                else:
                    check_response = f"""
                    Check the previous question that the bot was asked in the process history below and the response the bot gave after trying to perform the task.  Review the response and
                    determine if the bot's response was correct and makes sense given the instructions it was given.  You can accept the final results of the
                    previous step without asking to see the sql queries and results that led to the final conclusion.  You don't need to validate things like names or other
                    text values unless they seem wildly incorrect. You do not need to see the data that came out of a query the bot ran.

                    If you are very seriously concerned that the step may not have been correctly perfomed, return a request to re-run the step of the process again by returning the text "**fail**" followed by a
                    DETAILED EXPLAINATION as to why it did not pass and what your concern is, and any suggestions you have on how to succeed on the next try.
                    If the response seems like it is likely correct, return only the text string "**success**" (no explanation needed) to continue to the next step.  If the process is complete,
                    tell the process to stop running.  Remember, proceed under your own direction and do not ask the user for permission to proceed.

                    Remember, if you are asked to run either sql or snowpark_python from a given note_id, make sure you examine the note_type field and use the appropriate tool for
                    the note type.  Only pass the note_id, not the code itself, to the appropriate tool where the note will be handled.

                    Process Config:
                    {self.process_config[thread_id][process_id]}

                    Full process Instructions:
                    {process['PROCESS_INSTRUCTIONS']}

                    Process History so far this run:
                    {self.process_history[thread_id][process_id]}

                    Current system time:
                    {datetime.now()}

                    Bot's most recent response:
                    {previous_response}
                    """

            #     logger.info(f"\nSENT TO 2nd LLM:\n{check_response}\n")

            result = chat_completion(check_response, self.db_adapter, bot_id = bot_id, bot_name = '', thread_id=thread_id, process_id=process_id, process_name = process_name)

            with self.lock:
                self.process_history[thread_id][process_id] += "\nBots response: " + previous_response

            if not isinstance(result, str):
                self.set_process_cache(bot_id, thread_id, process_id)
                #         logger.info(f'Process cached with bot_id: {bot_id}, thread_id: {thread_id}, process_id: {process_id}')

                return {
                    "success": False,
                    "message": "Process failed: The checking function didn't return a string."
                }

            # logger.info("RUN 2nd LLM...")

            #        logger.info(f"\nRESULT FROM 2nd LLM: {result}\n")

            if "**fail**" in result.lower():
                with self.lock:
                    self.last_fail[thread_id][process_id] = result
                    self.fail_count[thread_id][process_id] += 1
                    self.process_history[thread_id][process_id] += "\nSupervisors concern: " + result
                if self.fail_count[thread_id][process_id] <= 5:
                    logger.info(f"\nStep {self.counter[thread_id][process_id]} failed. Fail count={self.fail_count[thread_id][process_id]} Trying again up to 5 times...\n")
                    self.set_process_cache(bot_id, thread_id, process_id)
                    #       logger.info(f'Process cached with bot_id: {bot_id}, thread_id: {thread_id}, process_id: {process_id}')

                    return_dict = {
                        "success": False,
                        "feedback_from_supervisor": result,
                        "current system time": {datetime.now()},
                        "recovery_step": f"Review the message above and submit a clarification, and/or try this Step {self.counter[thread_id][process_id]} again:\n{self.instructions[thread_id][process_id]}"
                    }
                    if verbose:
                        return_dict["additional_request"] = "Please also explain and summarize this feedback from the supervisor bot to the user so they know whats going on, and how you plan to rectify it."
                    else:
                        return_dict["shhh"] = "Remember you are running in slient, non-verbose mode. Limit your output as much as possible."

                    return return_dict

                else:
                    logger.info(f"\nStep {self.counter[thread_id][process_id]} failed. Fail count={self.fail_count[thread_id][process_id]} > 5 failures on this step, stopping process...\n")

                    with self.lock:
                        self.done[thread_id][process_id] = True
                    self.clear_process_cache(bot_id, thread_id, process_id)
                    try:
                        del self.counter[thread_id][process_id]
                    except:
                        pass
                    logger.info(f'Process cache cleared for bot_id: {bot_id}, thread_id: {thread_id}, process_id: {process_id}')

                    return {"success": "False", "message": f'The process {process_name} has failed due to > 5 repeated step completion failures.  Do not start this process again without user approval.'}

            with self.lock:
                self.last_fail[thread_id][process_id] = None
                self.fail_count[thread_id][process_id] = 0
                #          logger.info(f"\nThis step passed.  Moving to next step\n")
                self.counter[thread_id][process_id] += 1

            extract_instructions = f"""
            Extract the text for the next step from the process instructions and return it, using the section marked 'Process History' to see where you are in the process.
            Remember, the process instructions are a set of individual steps that need to be run in order.
            Return the text of the next step only, do not make any other comments or statements.
            By the way, the system default email address (SYS$DEFAULT_EMAIL) is {self.sys_default_email}.  If the instructions say to send an email
            to SYS$DEFAULT_EMAIL, replace it with {self.sys_default_email}.

            If a step of the instructions says to run another process, return '>>RECURSE' and the process name or process id as the first step
            and then call _run_process with the action KICKOFF_PROCESS to get the first step of the next process to run.  Continue this process until
            you have completed all the steps.  If you are asked to run another process as part of this process, follow the same instructions.  Do this
            up to ten times.

            If the process is complete, respond "**done**" with no other text.

            Process History: {self.process_history[thread_id][process_id]}

            Current system time: {datetime.now()}

            Process Configuration:
            {self.process_config[thread_id][process_id]}

            Process Instructions:

            {process['PROCESS_INSTRUCTIONS']}
            """

            #     logger.info(f"\nEXTRACT NEXT STEP:\n{extract_instructions}\n")

            #     logger.info("RUN 2nd LLM...")
            next_step = chat_completion(extract_instructions, self.db_adapter, bot_id = bot_id, bot_name = '', thread_id=thread_id, process_id=process_id, process_name=process_name)

            #      logger.info(f"\nRESULT (NEXT_STEP_): {next_step}\n")

            if next_step == '**done**' or next_step == '***done***' or next_step.strip().endswith('**done**'):
                with self.lock:
                    self.last_fail[thread_id][process_id] = None
                    self.fail_count[thread_id][process_id] = None
                    self.done[thread_id][process_id] = True
                # Clear the process cache when the process is complete
                self.clear_process_cache(bot_id, thread_id, process_id)
                logger.info(f'Process cache cleared for bot_id: {bot_id}, thread_id: {thread_id}, process_id: {process_id}')

                return {
                    "success": True,
                    "process_complete": True,
                    "message": f"Congratulations, the process {process_name} is complete.",
                    "proccess_success_step": True,
                    "reminder": f"If you were running this as a subprocess inside another process, be sure to continue the parent process."
                }

            #        logger.info(f"\n{next_step}\n")

            with self.lock:
                if ">> RECURSE" in next_step or ">>RECURSE" in next_step:
                    self.recurse_level += 1
                    # Extract the process name or ID
                    process_to_run = next_step.split(">>RECURSE")[1].strip() if ">>RECURSE" in next_step else next_step.split(">> RECURSE")[1].strip()

                    # Prepare the instruction for the bot to run the nested process
                    next_step = f"""
                    Use the _run_process tool to run the process '{process_to_run}' with the following parameters:
                    - action: KICKOFF_PROCESS
                    - process_name: {process_to_run}
                    - bot_id: {bot_id}
                    - silent_mode: {silent_mode}

                    After the nested process completes, continue with the next step of this process.
                    """

                    logger.info(f"RECURSE found.  Running process {process_to_run} on level {self.recurse_level}")

                    return {
                        "success": True,
                        "message": next_step,
                    }

                self.instructions[thread_id][process_id] = f"""
                Hey **@{process['BOT_ID']}**, here is the next step of the process.

                {next_step}

                If you are asked to run either sql or snowpark_python from a given note_id, make sure you examine the note_type field and use the appropriate tool for
                the note type.  Only pass the note_id, not the code itself, to the appropriate tool where the note will be handled.

                Execute these instructions now and then pass your response to the run_process tool as a parameter called previous_response and an action of GET_NEXT_STEP.
                If you are told to run another process in these instructions, actually run it using _run_process before calling GET_NEXT_STEP for this process, do not just pretend to run it.
                If need to terminate the process early, call with action of END_PROCESS.
                """
                if verbose:
                    self.instructions[thread_id][process_id] += """
                Tell the user what you are going to do in this step and showing interium outputs, etc. while you are running this and further steps to keep the user informed what is going on.
                For example if you are going to call a tool to perform this step, first tell the user what you're going to do.
                """
                else:
                    self.instructions[thread_id][process_id] += """
                This process is being run in low verbosity mode, so do not generate a lot of text while running this process. Just do whats required, call the right tools, etc.
                Also, it you are asked to run either sql or snowpark_python from a given note_id, make sure you examine the note_type field and use the appropriate tool for
                the note type.  Only pass the note_id, not the code itself, to the appropriate tool where the note will be handled.
                """
                self.instructions[thread_id][process_id] += f"""
                Don't stop to verify anything with the user unless specifically told to.
                By the way the current system time id: {datetime.now()}.
                In your response back to run_process, provide a detailed description of what you did, what result you achieved, and why you believe this to have successfully completed the step.
                """

            #     logger.info(f"\nEXTRACTED NEXT STEP: \n{self.instructions[thread_id][process_id]}\n")

            with self.lock:
                self.process_history[thread_id][process_id] += "\nNext step: " + next_step

            self.set_process_cache(bot_id, thread_id, process_id)
            logger.info(f'Process cached with bot_id: {bot_id}, thread_id: {thread_id}, process_id: {process_id}')

            return {
                "success": True,
                "message": self.instructions[thread_id][process_id],
            }

        elif action == "END_PROCESS":
            logger.info(f"Received END_PROCESS action for process {process_name} on level {self.recurse_level}")

            with self.lock:
                self.done[thread_id][process_id] = True

            self.clear_process_registers_by_thread(thread_id)

            self.process_id[thread_id] = None

            self.clear_process_cache(bot_id, thread_id, process_id)
            logger.info(f'Process cache cleared for bot_id: {bot_id}, thread_id: {thread_id}, process_id: {process_id}')

            self.recurse_level -= 1
            logger.info(f"Returning to recursion level {self.recurse_level}")

            return {"success": True, "message": f'The process {process_name} has finished.  You may now end the process.'}
        if action == 'STOP_ALL_PROCESSES':
            try:
                self.clear_all_process_registers(thread_id)
                return {
                    "Success": True,
                    "Message": "All processes stopped (?)"
                }
            except Exception as e:
                return {
                    "Success": False,
                    "Error": f"Failed to stop all processes: {e}"
                }
        else:
            logger.info("No action specified.")
            return {"success": False, "message": "No action specified."}

    # ====== RUN PROCESSES END ==========================================================================================

    # ====== NOTEBOOK START ==========================================================================================

    def get_notebook_list(self, bot_id="all"):
        db_adapter = self.db_adapter
        cursor = db_adapter.client.cursor()
        try:
            if bot_id == "all":
                list_query = f"SELECT created_at, bot_id, note_id, note_name, note_type, note_content, note_params FROM {db_adapter.schema}.NOTEBOOK" if db_adapter.schema else f"SELECT created_at, bot_id, note_id, note_name, note_type, note_content, note_params FROM NOTEBOOK"
                cursor.execute(list_query)
            else:
                list_query = f"SELECT created_at, bot_id, note_id, note_name, note_type, note_content, note_params FROM {db_adapter.schema}.NOTEBOOK WHERE upper(bot_id) = upper(%s)" if db_adapter.schema else f"SELECT created_at, bot_id, note_id, note_name, note_type, note_content, note_params FROM NOTEBOOK WHERE upper(bot_id) = upper(%s)"
                cursor.execute(list_query, (bot_id,))
            notes = cursor.fetchall()
            note_list = []
            for note in notes:
                note_dict = {
                    "timestamp": note[0],
                    "bot_id": note[1],
                    "note_id": note[2],
                    'note_name': note[3],
                    'note_type': note[4],
                    'note_content': note[5],
                    'note_params': note[6]
                }
                note_list.append(note_dict)
            return {"Success": True, "notes": note_list}
        except Exception as e:
            return {
                "Success": False,
                "Error": f"Failed to list notes for bot {bot_id}: {e}",
            }
        finally:
            cursor.close()

    def get_note_info(self, bot_id=None, note_id=None):
        db_adapter = self.db_adapter
        cursor = db_adapter.client.cursor()
        try:
            result = None

            if note_id is None or note_id == '':
                return {
                    "Success": False,
                    "Error": "Note_id must be provided and cannot be empty."
                }
            if note_id is not None and note_id != '':
                query = f"SELECT * FROM {db_adapter.schema}.NOTEBOOK WHERE bot_id LIKE %s AND note_id = %s" if db_adapter.schema else f"SELECT * FROM NOTEBOOK WHERE bot_id LIKE %s AND note_id = %s"
                cursor.execute(query, (f"%{bot_id}%", note_id))
                result = cursor.fetchone()

            if result:
                # Assuming the result is a tuple of values corresponding to the columns in the NOTEBOOK table
                # Convert the tuple to a dictionary with appropriate field names
                field_names = [desc[0] for desc in cursor.description]
                return {
                    "Success": True,
                    "Data": dict(zip(field_names, result)),
                    "Note": "Only use this information to help manage or update notes",
                    "Important!": "If a user has asked you to show these notes to them, output them verbatim, do not modify or summarize them."
                }
            else:
                return {}
        except Exception as e:
            return {}

    def manage_notebook(
        self, action, bot_id=None, note_id=None, note_name = None, note_content=None, note_params=None, thread_id=None, note_type=None, note_config = None
    ):
        """
        Manages notes in the NOTEBOOK table with actions to create, delete, or update a note.

        Args:
            action (str): The action to perform
            bot_id (str): The bot ID associated with the note.
            note_id (str): The note ID for the note to manage.
            note_content (str): The content of the note for create or update actions.
            note_params (str): The parameters for the note for create or update actions.

        Returns:
            dict: A dictionary with the result of the operation.
        """

        required_fields_create = [
            "note_id",
            "bot_id",
            "note_name",
            "note_type",
            "note_content",
        ]

        required_fields_update = [
            "note_id",
            "bot_id",
            "note_name",
            "note_content",
        ]

        if action not in ['CREATE','CREATE_CONFIRMED', 'UPDATE','UPDATE_CONFIRMED', 'DELETE', 'DELETE_CONFIRMED', 'LIST', 'TIME']:
            return {
                "Success": False,
                "Error": "Invalid action.  Manage Notebook tool only accepts actions of CREATE, CREATE_CONFIRMED, UPDATE, UPDATE_CONFIRMED, DELETE, LIST, or TIME."
            }

        try:
            if not self.done[thread_id][self.process_id[thread_id]]:
                return {
                    "Success": False,
                    "Error": "You cannot run the notebook manager from within a process.  Please run this tool outside of a process."
                }
        except KeyError as e:
            pass

        if action == "TIME":
            return {
                "current_system_time": datetime.now()
            }
        action = action.upper()

        db_adapter = self.db_adapter
        cursor = db_adapter.client.cursor()

        try:
            if action in ["UPDATE_NOTE_CONFIG", "CREATE_NOTE_CONFIG", "DELETE_NOTE_CONFIG"]:
                note_config = '' if action == "DELETE_NOTE_CONFIG" else note_config
                update_query = f"""
                    UPDATE {db_adapter.schema}.NOTEBOOK
                    SET NOTE_CONFIG = %(note_config)s
                    WHERE NOTE_ID = %(note_id)s
                """
                cursor.execute(
                    update_query,
                    {"note_config": note_config, "note_id": note_id},
                )
                db_adapter.client.commit()

                return {
                    "Success": True,
                    "Message": f"note_config updated or deleted",
                    "note_id": note_id,
                }

            if action == "CREATE" or action == "CREATE_CONFIRMED":
                # Check for dupe name
                sql = f"SELECT * FROM {db_adapter.schema}.NOTEBOOK WHERE bot_id = %s and note_id = %s"
                cursor.execute(sql, (bot_id, note_id))

                record = cursor.fetchone()

                if record:
                    return {
                        "Success": False,
                        "Error": f"Note with id {note_id} already exists for bot {bot_id}.  Please choose a different id."
                    }

            if action == "UPDATE" or action == 'UPDATE_CONFIRMED':
                # Check for dupe name
                sql = f"SELECT * FROM {db_adapter.schema}.NOTEBOOK WHERE bot_id = %s and note_id = %s"
                cursor.execute(sql, (bot_id, note_id))

                record = cursor.fetchone()

                if record and '_golden' in record[2]:
                    return {
                        "Success": False,
                        "Error": f"Note with id {note_id} is a system note and can not be updated.  Suggest making a copy with a new name."
                    }

            if (action == "CREATE" or action == "UPDATE") and note_type == 'process':
                # Send note_instructions to 2nd LLM to check it and format nicely if note type 'process'
                note_field_name = 'Note Content'
                confirm_notification_prefix = ''
                tidy_note_content = f"""
                Below is a note that has been submitted by a user.  Please review it to insure it is something
                that will make sense to the run_process tool.  If not, make changes so it is organized into clear
                steps.  Make sure that it is tidy, legible and properly formatted.

                Do not create multiple options for the instructions, as whatever you return will be used immediately.
                Return the updated and tidy instructions.  If there is an issue with the instructions, return an error message.

                If the note wants to send an email to a default email, or says to send an email but doesn't specify
                a recipient address, note that the SYS$DEFAULT_EMAIL is currently set to {self.sys_default_email}.
                Include the notation of SYS$DEFAULT_EMAIL in the instructions instead of the actual address, unless
                the instructions specify a different specific email address.

                The note is as follows:\n {note_content}
                """

                tidy_note_content= "\n".join(
                    line.lstrip() for line in tidy_note_content.splitlines()
                )

                note_content = chat_completion(tidy_note_content, db_adapter, bot_id = bot_id, bot_name = '', thread_id=thread_id, note_id=note_id)

            if action == "CREATE":
                return {
                    "Success": False,
                    "Fields": {"note_id": note_id, "note_name": note_name, "bot_id": bot_id, "note_type": note_type, "note content": note_content, "note_params:": note_params},
                    "Confirmation_Needed": "Please reconfirm the field values with the user, then call this function again with the action CREATE_CONFIRMED to actually create the note.  If the user does not want to create a note, allow code in the process instructions",
                    "Suggestion": "If possible, for a sql or python note, suggest to the user that we test the sql or python before making the note to make sure it works properly",
                    "Next Step": "If you're ready to create this note or the user has chosen not to create a note, call this function again with action CREATE_CONFIRMED instead of CREATE.  If the user chooses to allow code in the process, allow them to do so and include the code directly in the process."
                #    "Info": f"By the way the current system time is {datetime.now().strftime('%Y-%m-%d %H:%M:%S %Z')}",
                }

            if action == "UPDATE":
                return {
                    "Success": False,
                    "Fields": {"note_id": note_id, "note_name": note_name, "bot_id": bot_id, "note content": note_content, "note_param:": note_params},
                    "Confirmation_Needed": "Please reconfirm this content and all the other note field values with the user, then call this function again with the action UPDATE_CONFIRMED to actually update the note.  If the user does not want to update the note, allow code in the process instructions",
                    "Suggestion": "If possible, for a sql or python note, suggest to the user that we test the sql or python before making the note to make sure it works properly",
                    "Next Step": "If you're ready to update this note, call this function again with action UPDATE_CONFIRMED instead of UPDATE"
                #    "Info": f"By the way the current system time is {datetime.now().strftime('%Y-%m-%d %H:%M:%S %Z')}",
                }

        except Exception as e:
            return {"Success": False, "Error": f"Error connecting to LLM: {e}"}

        if action == "CREATE_CONFIRMED":
            action = "CREATE"
        if action == "UPDATE_CONFIRMED":
            action = "UPDATE"

        if action == "DELETE":
            return {
                "Success": False,
                "Confirmation_Needed": "Please reconfirm that you are deleting the correct note_id, and double check with the user they want to delete this note, then call this function again with the action DELETE_CONFIRMED to actually delete the note.  Call with LIST to double-check the note_id if you aren't sure that its right.",
            }

        if action == "DELETE_CONFIRMED":
            action = "DELETE"

        if action not in ["CREATE", "DELETE", "UPDATE", "LIST", "SHOW"]:
            return {"Success": False, "Error": "Invalid action specified. Should be CREATE, DELETE, UPDATE, LIST, or SHOW."}

        if action == "LIST":
            logger.info("Running get notebook list")
            return self.get_notebook_list(bot_id if bot_id is not None else "all")

        if action == "SHOW":
            logger.info("Running show notebook info")
            if bot_id is None:
                return {"Success": False, "Error": "bot_id is required for SHOW action"}
            if note_id is None:
                return {"Success": False, "Error": "note_id is required for SHOW action"}

            if note_id is not None:
                return self.get_note_info(bot_id=bot_id, note_id=note_id)
            else:
                note_id = note_content['note_id']
                return self.get_note_info(bot_id=bot_id, note_id=note_id)

        note_id_created = False
        if note_id is None:
            if action == "CREATE":
                note_id = f"{bot_id}_{''.join(random.choices(string.ascii_letters + string.digits, k=6))}"
                note_id_created = True
            else:
                return {"Success": False, "Error": f"Missing note_id field"}

        try:
            if action == "CREATE":
                insert_query = f"""
                    INSERT INTO {db_adapter.schema}.NOTEBOOK (
                        created_at, updated_at, note_id, bot_id, note_name, note_type, note_content, note_params
                    ) VALUES (
                        current_timestamp(), current_timestamp(), %(note_id)s, %(bot_id)s, %(note_name)s, %(note_type)s, %(note_content)s, %(note_params)s
                    )
                """ if db_adapter.schema else f"""
                    INSERT INTO NOTEBOOK (
                        created_at, updated_at, note_id, bot_id, note_name, note_type, note_content, note_params
                    ) VALUES (
                        current_timestamp(), current_timestamp(), %(note_id)s, %(bot_id)s, %(note_name)s, %(note_type)s, %(note_content)s, %(note_params)s
                    )
                """

                insert_query= "\n".join(
                    line.lstrip() for line in insert_query.splitlines()
                )
                # Generate 6 random alphanumeric characters
                if note_id_created == False:
                    random_suffix = "".join(
                    random.choices(string.ascii_letters + string.digits, k=6)
                     )
                    note_id_with_suffix = note_id + "_" + random_suffix
                else:
                    note_id_with_suffix = note_id
                cursor.execute(
                    insert_query,
                    {
                        "note_id": note_id_with_suffix,
                        "bot_id": bot_id,
                        "note_name": note_name,
                        "note_type": note_type,
                        "note_content": note_content,
                        "note_params": note_params,
                    },
                )

                db_adapter.client.commit()
                return {
                    "Success": True,
                    "Message": f"note successfully created.",
                    "Note Id": note_id_with_suffix,
                    "Suggestion": "Now that the note is created, remind the user of the note_id and offer to test it using the correct runner, either sql, snowpark_python, or process, depending on the type set in the note_type field, and if there are any issues you can later on UPDATE the note using manage_notes to clarify anything needed.  OFFER to test it, but don't just test it unless the user agrees.  ",
                }

            elif action == "DELETE":
                delete_query = f"""
                    DELETE FROM {db_adapter.schema}.NOTEBOOK
                    WHERE note_id = %s
                """ if db_adapter.schema else f"""
                    DELETE FROM NOTEBOOK
                    WHERE note_id = %s
                """
                cursor.execute(delete_query, (note_id))

                return {
                    "Success": True,
                    "Message": f"note deleted",
                    "note_id": note_id,
                }

            elif action == "UPDATE":
                update_query = f"""
                    UPDATE {db_adapter.schema}.NOTEBOOK
                    SET updated_at = CURRENT_TIMESTAMP, note_id=%s, bot_id=%s, note_name=%s, note_content=%s, note_params=%s, note_type=%s
                    WHERE note_id = %s
                """ if db_adapter.schema else """
                    UPDATE NOTEBOOK
                    SET updated_at = CURRENT_TIMESTAMP, note_id=%s, bot_id=%s, note_name=%s, note_content=%s, note_params=%s, note_type=%s
                    WHERE note_id = %s
                """
                cursor.execute(
                    update_query,
                    (note_id, bot_id, note_name, note_content, note_params, note_type, note_id)
                )
                db_adapter.client.commit()
                return {
                    "Success": True,
                    "Message": "note successfully updated",
                    "Note id": note_id,
                    "Suggestion": "Now that the note is updated, offer to test it using run_note, and if there are any issues you can later on UPDATE the note again using manage_notebook to clarify anything needed. OFFER to test it, but don't just test it unless the user agrees.",
                }
            return {"Success": True, "Message": f"note update or delete confirmed."}
        except Exception as e:
            return {"Success": False, "Error": str(e)}

        finally:
            cursor.close()

    def insert_notebook_history(
        self,
        note_id,
        work_done_summary,
        note_status,
        updated_note_learnings,
        report_message="",
        done_flag=False,
        needs_help_flag="N",
        note_clarity_comments="",
    ):
        """
        Inserts a row into the NOTEBOOK_HISTORY table.

        Args:
            note_id (str): The unique identifier for the note.
            work_done_summary (str): A summary of the work done.
            note_status (str): The status of the note.
            updated_note_learnings (str): Any new learnings from the note.
            report_message (str): The message to report about the note.
            done_flag (bool): Flag indicating if the note is done.
            needs_help_flag (bool): Flag indicating if help is needed.
            note_clarity_comments (str): Comments on the clarity of the note.
        """
        db_adapter = self.db_adapter
        insert_query = f"""
            INSERT INTO {db_adapter.schema}.NOTEBOOK_HISTORY (
                note_id, work_done_summary, note_status, updated_note_learnings,
                report_message, done_flag, needs_help_flag, note_clarity_comments
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s
            )
        """ if db_adapter.schema else f"""
            INSERT INTO NOTEBOOK_HISTORY (
                note_id, work_done_summary, note_status, updated_note_learnings,
                report_message, done_flag, needs_help_flag, note_clarity_comments
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s
            )
        """
        cursor = None
        try:
            cursor = db_adapter.client.cursor()
            cursor.execute(
                insert_query,
                (
                    note_id,
                    work_done_summary,
                    note_status,
                    updated_note_learnings,
                    report_message,
                    done_flag,
                    needs_help_flag,
                    note_clarity_comments,
                ),
            )
            db_adapter.client.commit()
            cursor.close()
            logger.info(
                f"Notebook history row inserted successfully for note_id: {note_id}"
            )
        except Exception as e:
            logger.info(f"An error occurred while inserting the notebook history row: {e}")
            if cursor is not None:
                cursor.close()

    # ====== NOTEBOOK END ==========================================================================================

# A collection of 'old style' tools information. These are tools that are not registered in the global tools registry
# (AKA new style tools).
# TODO: migrate all old style tools to the new style tools registry. Once we do that, we can refactor get_tools() below
#       to use the new style tools registry.
_old_style_tool_metadata = {
    "slack_tools": {
        "func_descriptors": slack_tools_descriptions,
        "funcname_to_locator": slack_tools
    },
    "harvester_tools": {
        "func_descriptors": harvester_tools_functions,
        "funcname_to_locator": harvester_tools_list
    },
    "make_baby_bot": {
        "func_descriptors": MAKE_BABY_BOT_DESCRIPTIONS,
        "funcname_to_locator": make_baby_bot_tools
    },
    "process_runner_tools": {
        "func_descriptors": process_runner_functions,
        "funcname_to_locator": process_runner_tools
    },
    "notebook_manager_tools": {
        "func_descriptors": notebook_manager_functions,
        "funcname_to_locator": notebook_manager_tools
    }
}


def get_all_tool_to_func_map(include_ephemeral_tools: bool = False) -> dict[str, list[str]]:
    """
    Retrieve metadata for all tools, optionally including ephemeral tools.

    Args:
        include_ephemeral_tools (bool): Whether to include ephemeral tools in the metadata.

    Returns:
        dict: A dictionary mapping tool (group) names to a list of the function names in this group.
    """
    all_tools_metadata = {}

    # Include old style tools (always non-ephemeral)
    for tool_group, metadata in _old_style_tool_metadata.items():
        func_names = [func_desc["function"]["name"] for func_desc in metadata["func_descriptors"]]
        all_tools_metadata[tool_group] = func_names

    # Include new style tools from the global tools registry
    reg = get_global_tools_registry()
    group_lifetime_incl_filter = None
    if not include_ephemeral_tools:
        group_lifetime_incl_filter = ToolFuncGroupLifetime.PERSISTENT
    all_tools_metadata.update(reg.get_tool_to_func_map(group_lifetime_incl_filter))

    return all_tools_metadata


def get_persistent_tools_descriptions() -> dict[str, str]:
    """
    Retrieves a map of all avaialble tool func group) names to their descriptions for tools that have a 'persistent' lifetime, sorted by tool name.

    Returns:
        dict: A dictionary where each key is the name of a tool (group) and the value is its description.
    """
    from genesis_bots.core.bot_os_tool_descriptions import _tools_data # holds the legacy tools (groups) descriptors
    tools_data = {name: description for name, description in _tools_data}

    registry = get_global_tools_registry()
    for group in registry.list_groups():
        if group.lifetime == ToolFuncGroupLifetime.PERSISTENT:
            tools_data[group.name] = group.description

    return dict(sorted(tools_data.items()))


def get_tools(
    which_tools: list[str],
    db_adapter = None, # used for 'old style' tools
    slack_adapter_local=None, # # used for 'old style' tools
    include_slack: bool = True,
    tool_belt=None # # used for 'old style' tools
    ) -> tuple[list, dict, dict]:
    """
    Retrieve a list of tools (function groups), available functions, and a mapping of functions to tools based on the specified tool names.

    This function combines information from 'old style' and 'new style' tools (those which are registered in the global tools registry).

    Args:
        which_tools (list): A list of tool (function group) names to retrieve.
        include_slack (bool): Whether to include Slack tools (default is True).

    Returns:
        tuple: A tuple containing three elements:
            - list of dicts: A list of function descriptions
            - dict: A dictionary mapping function names to their implementations (callable objects).
            - dict: A dictionary mapping tool (group) names to a list of function descriptors (dicts) for this tool (group)
    """
    func_descriptors = []
    available_functions_loaded = {} # map function_name (str)--> 'locator' (str|callable) ;
    # 'locator' can be a callable or string.
    # If a string, it gets dyanmically evaluated below to the actual callable object
    tool_to_func_descriptors_map = {} # map of tool name to list of function descriptors

    # if "autonomous_functions" in which_tools and "autonomous_tools" not in which_tools:
    #     which_tools = [
    #         tool if tool != "autonomous_functions" else "autonomous_tools"
    #         for tool in which_tools
    #     ]

    which_tools = list(which_tools)

    for tool in which_tools:
        try:
            tool_name = tool.get("tool_name")
        except:
            tool_name = tool

        # Canonicalize 'old style' tool names
        # ----------------------------------
        if tool_name == "bot_dispatch_tools" or tool_name == "bot_dispatch":
            tool_name = "delegate_work"

        if tool_name == "git_file_manager_tools":
            tool_name = "git_action"

        if tool_name == "data_dev_tools":
            tool_name = "jira_connector_tools" # FIXME

        # Skip loading slack tools if include_slack is False (backward compatible behavior) TODO: remove special case
        if tool_name == "slack_tools" and not include_slack:
            continue

        # Lookup tool in _old_style_tool_metadata
        global _old_style_tool_metadata
        if tool_name in _old_style_tool_metadata:
            tool_metadata = _old_style_tool_metadata[tool_name]
            func_descriptors.extend(tool_metadata["func_descriptors"])
            available_functions_loaded.update(tool_metadata["funcname_to_locator"])
            tool_to_func_descriptors_map[tool_name] = tool_metadata["func_descriptors"]
        else:
            # Resolve 'new style' tool functions
            # (from tool functions registry)
            # ----------------------------------
            registry = get_global_tools_registry()
            tool_funcs : List[Callable] = registry.get_tool_funcs_by_group(tool_name)
            if tool_funcs:
                descriptors : List[ToolFuncDescriptor] = [get_tool_func_descriptor(func) for func in tool_funcs]
                func_descriptors.extend([descriptor.to_llm_description_dict()
                                        for descriptor in descriptors])
                available_functions_loaded.update({get_tool_func_descriptor(func).name : func
                                                for func in tool_funcs})
                tool_to_func_descriptors_map[tool_name] = [descriptor.to_llm_description_dict()
                                                        for descriptor in descriptors]
            else:
                # Ultimately, fallback to try to load the function data dynamaically from a module named exactly like tool_name
                # ??? is this ever actually used ???
                try:
                    module_path = "generated_modules." + tool_name
                    desc_func = "TOOL_FUNCTION_DESCRIPTION_" + tool_name.upper()
                    functs_func = tool_name.lower() + "_action_function_mapping"
                    module = __import__(module_path, fromlist=[desc_func, functs_func])
                    # here's how to get the function for generated things even new ones...
                    func = [getattr(module, desc_func)]
                    func_descriptors.extend(func)
                    tool_to_func_descriptors_map[tool_name] = func
                    func_af = getattr(module, functs_func)
                    available_functions_loaded.update(func_af)
                except:
                    logger.warning(f"Functions for tool '{tool_name}' could not be found.")

    # Resolve 'old style' tool functions to actual callables
    available_functions = {}
    for name, function_handle in available_functions_loaded.items():
        if callable(function_handle):
            available_functions[name] = function_handle
        else:
            assert isinstance(function_handle, str)
            module_path, func_name = function_handle.rsplit(".", 1)
            if module_path in locals():
                # old style tools might be depending on the following object: tool_belt, db_adapter, slack_adapter_local
                module = locals()[module_path]
                try:
                    func = getattr(module, func_name)
                except:
                    func = module
                # logger.info("existing local: ",func)
            elif module_path in globals():
                module = globals()[module_path]
                try:
                    func = getattr(module, func_name)
                except:
                    func = module
                # logger.info("existing global: ",func)
            else:
                # Dyanmic imports (e.g. module_path= 'bot_genesis.make_baby_bot')
                module = __import__(module_path, fromlist=[func_name])
                func = getattr(module, func_name)
                # logger.info("imported: ",func)
            if func is not None:
                available_functions[name] = func
            else:
                logger.warning(f"Tool Function '{name}' could not be resolved to a callable from '{function_handle}'. It may be listed as a tool function, but not available for use.")

    return func_descriptors, available_functions, tool_to_func_descriptors_map


def dispatch_to_bots(task_template, args_array, dispatch_bot_id=None):
    """
    Dispatches a task to multiple bots, each instantiated by creating a new thread with a specific task.
    The task is created by filling in the task template with arguments from the args_array using Jinja templating.

    Args:
        task_template (str): A natural language task template using Jinja templating.
        args_array (list of dict): An array of dictionaries to plug into the task template for each bot.

    Returns:
        list: An array of responses.
    """
    from genesis_bots.core.bot_os_dispatch_input_adapter import BotOsDispatchInputAdapter
    if len(args_array) < 2:
        return "Error: args_array size must be at least 2."

    template = Template(task_template)
    adapter = BotOsDispatchInputAdapter(bot_id=dispatch_bot_id)

    for s_args in args_array:
        # Fill in the task template with the current arguments
        args = json.loads(s_args)
        task = template.render(**args)
        adapter.dispatch_task(task)

    while True:
        responses = adapter.check_tasks()
        if responses:
            logger.info(f"dispatch_to_bots - {responses}")
            return responses
        time.sleep(1)
