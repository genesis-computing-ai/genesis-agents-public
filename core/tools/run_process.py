from datetime import datetime
from core.logging_config import logger
from textwrap import dedent
import re
import os

from core.bot_os_tools2 import (
    BOT_ID_IMPLICIT_FROM_CONTEXT,
    THREAD_ID_IMPLICIT_FROM_CONTEXT,
    ToolFuncGroup,
    ToolFuncParamDescriptor,
    gc_tool,
)

from core.tools.tool_helpers import chat_completion, get_sys_email, clear_process_registers_by_thread, get_process_info

run_process_tools = ToolFuncGroup(
    name="run_process_tools",
    description=dedent(
    """
    Runs a process by name or ID, allowing bots to manage processes.

    Returns:
        dict: A dictionary containing the result of the operation.
    """
    ),
    lifetime="PERSISTENT",
)


# run_process
@gc_tool(
    action="The action to perform: KICKOFF_PROCESS, GET_NEXT_STEP, END_PROCESS, TIME, or STOP_ALL_PROCESSES.  Either process_name or process_id must also be specified.",
    process_name="The name of the process to run",
    process_id="The id of the process to run (note: this is NOT the task_id or process_schedule_id)",
    previous_response="The previous response from the bot (for use with GET_NEXT_STEP)",
    concise_mode="Optional, to run in low-verbosity/concise mode. Default to False.",
    bot_id=BOT_ID_IMPLICIT_FROM_CONTEXT,
    thread_id=THREAD_ID_IMPLICIT_FROM_CONTEXT,
    _group_tags_=[run_process_tools],
)
def run_process(
    action,
    previous_response="",
    process_name="",
    process_id=None,
    process_config=None,
    thread_id=None,
    bot_id=None,
    concise_mode=False,
    bot_name=None,
):
    #  logger.info(f"Running processes Action: {action} | process_id: {process_id or 'None'} | Thread ID: {thread_id or 'None'}")
    #         self.recurse_level = 0
    self.recurse_stack = {thread_id: thread_id, process_id: process_id}

    if process_id is not None and process_id == "":
        process_id = None
    if process_name is not None and process_name == "":
        process_name = None

    if action == "TIME":
        return {"current_system_time": datetime.now()}

    if bot_id is None:
        return {
            "Success": False,
            "Error": "Bot_id and either process_id or process_name are required parameters.",
        }

    # Convert verbose to boolean if it's a string

    # Invert silent_mode if it's a boolean
    silent_mode = concise_mode
    if isinstance(silent_mode, bool):
        verbose = not silent_mode

    if isinstance(silent_mode, str):
        if silent_mode.upper() == "TRUE":
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
            "Error": "Either process_name or process_id must be provided.",
        }

    sys_default_email = get_sys_email()

    clear_process_registers_by_thread(thread_id)

    # Try to get process info from PROCESSES table
    process = get_process_info(
        bot_id, process_name=process_name, process_id=process_id
    )

    if len(process) == 0:
        # Get a list of processes for the bot
        processes = self.db_adapter.get_processes_list(bot_id)
        if processes is not None:
            process_list = ", ".join(
                [p["process_name"] for p in processes["processes"]]
            )
            return_dict = {
                "Success": False,
                "Message": f"Process not found. Available processes are {process_list}.",
                "Suggestion": "If one of the available processess is a very close match for what you're looking for, go ahead and run it.",
            }
            if silent_mode is True:
                return_dict["Reminder"] = (
                    "Remember to call the process in concise_mode as requested previously once you identify the right one"
                )
            return return_dict
        else:
            return {
                "Success": False,
                "Message": f"Process not found. {bot_id} has no processes defined.",
            }
    process = process["Data"]
    process_id = process["PROCESS_ID"]
    process_name = process["PROCESS_NAME"]
    process_config = process.get("PROCESS_CONFIG", "")
    if process_config is None:
        process_config = "None"
        process["PROCESS_CONFIG"] = "None"

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

        logger.info(f"Process {process_name} has been kicked off.")

        extract_instructions = f"""
            You will need to break the process instructions below up into individual steps and and return them one at a time.
            By the way the current system time is {datetime.now()}.
            By the way, the system default email address (SYS$DEFAULT_EMAIL) is {self.sys_default_email}.  If the instructions say to send an email
            to SYS$DEFAULT_EMAIL, replace it with {self.sys_default_email}.
            Start by returning the first step of the process instructions below.
            Simply return the first instruction on what needs to be done first without removing or changing any details.

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

        if process["PROCESS_CONFIG"] != "None":
            extract_instructions += f"""

            Process configuration:
            {process['PROCESS_CONFIG']}.

            """

        first_step = chat_completion(
            extract_instructions,
            self.db_adapter,
            bot_id=bot_id,
            bot_name="",
            thread_id=thread_id,
            process_id=process_id,
            process_name=process_name,
        )

        # Check if the first step contains ">>RECURSE"
        if ">> RECURSE" in first_step or ">>RECURSE" in first_step:
            self.recurse_level += 1
            self.recurse_stack.append({thread_id: thread_id, process_id: process_id})
            # Extract the process name or ID
            process_to_run = (
                first_step.split(">>RECURSE")[1].strip()
                if ">>RECURSE" in first_step
                else first_step.split(">> RECURSE")[1].strip()
            )

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
            self.process_history[thread_id][process_id] = (
                "First step: " + first_step + "\n"
            )

            self.instructions[thread_id][
                process_id
            ] = f"""
                Hey **@{process['BOT_ID']}**

                {first_step}

                Execute this instruction now and then pass your response to the _run_process tool as a parameter called previous_response and an action of GET_NEXT_STEP.
                Execute the instructions you were given without asking for permission.  Do not ever verify anything with the user, unless you need to get a specific input
                from the user to be able to continue the process.

                Also, if you are asked to run either sql or snowpark_python from a given note_id, make sure you examine the note_type field and use the appropriate tool for
                the note type.  Only pass the note_id, not the code itself, to the appropriate tool where the note will be handled.
                """
        if self.sys_default_email:
            self.instructions[thread_id][
                process_id
            ] += f"""
                The system default email address (SYS$DEFAULT_EMAIL) is {self.sys_default_email}.  If you need to send an email, use this address.
                """

        if verbose:
            self.instructions[thread_id][
                process_id
            ] += """
                    However DO generate text explaining what you are doing and showing interium outputs, etc. while you are running this and further steps to keep the user informed what is going on, preface these messages by 🔄 aka :arrows_counterclockwise:.
                    Oh, and mention to the user before you start running the process that they can send "stop" to you at any time to stop the running of the process, and if they want less verbose output next time they can run request to run the process in "concise mode".
                    And keep them informed while you are running the process about what you are up to, especially before you call various tools.
                    """
        else:
            self.instructions[thread_id][
                process_id
            ] += """
                This process is being run in low verbosity mode. Do not directly repeat the first_step instructions to the user, just perform the steps as instructed.
                Also, if you are asked to run either sql or snowpark_python from a given note_id, make sure you examine the note_type field and use the appropriate tool for
                the note type.  Only pass the note_id, not the code itself, to the appropriate tool where the note will be handled.
                """
        self.instructions[thread_id][
            process_id
        ] += f"""
            In your response back to _run_process, provide a DETAILED description of what you did, what result you achieved, and why you believe this to have successfully completed the step.
            Do not use your memory or any cache that you might have.  Do not simulate any user interaction or tools calls.  Do not ask for any user input unless instructed to do so.
            If you are told to run another process as part of this process, actually run it, and run it completely before returning the results to this parent process.
            By the way the current system time is {datetime.now()}.  You can call manage_process with
            action TIME to get updated time if you need it when running the process.

            Now, start by performing the FIRST_STEP indicated above.
            """
        self.instructions[thread_id][
            process_id
        ] += "..... P.S. I KNOW YOU ARE IN SILENT MODE BUT ACTUALLY PERFORM THIS STEP NOW, YOU ARE NOT DONE YET!"

        self.instructions[thread_id][process_id] = "\n".join(
            line.lstrip()
            for line in self.instructions[thread_id][process_id].splitlines()
        )

        # Call set_process_cache to save the current state
        self.set_process_cache(bot_id, thread_id, process_id)
        #    logger.info(f'Process cached with bot_id: {bot_id}, thread_id: {thread_id}, process_id: {process_id}')

        return {
            "Success": True,
            "Instructions": self.instructions[thread_id][process_id],
            "process_id": process_id,
        }

    elif action == "GET_NEXT_STEP":
        logger.info("Entered GET NEXT STEP")

        if thread_id not in self.counter and process_id not in self.counter[thread_id]:
            return {
                "Success": False,
                "Message": f"Error: GET_NEXT_STEP seems to have been run before KICKOFF_PROCESS. Please retry from KICKOFF_PROCESS.",
            }

        # Load process cache
        if not self.get_process_cache(bot_id, thread_id, process_id):
            return {
                "Success": False,
                "Message": f"Error: Process cache for {process_id} couldn't be loaded. Please retry from KICKOFF_PROCESS.",
            }
        # Print that the process cache has been loaded and the 3 params to get_process_cache
        logger.info(
            f"Process cache loaded with params: bot_id: {bot_id}, thread_id: {thread_id}, process_id: {process_id}"
        )

        # Check if silent_mode is set for the thread and process
        verbose = True
        if thread_id in self.silent_mode and process_id in self.silent_mode[thread_id]:
            if self.silent_mode[thread_id][process_id]:
                verbose = False

        with self.lock:
            if process_id not in self.process_history[thread_id]:
                return {
                    "Success": False,
                    "Message": f"Error: Process {process_name} with id {process_id} couldn't be continued. Please retry once more from KICKOFF_PROCESS.",
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

        result = chat_completion(
            check_response,
            self.db_adapter,
            bot_id=bot_id,
            bot_name="",
            thread_id=thread_id,
            process_id=process_id,
            process_name=process_name,
        )

        with self.lock:
            self.process_history[thread_id][process_id] += (
                "\nBots response: " + previous_response
            )

        if not isinstance(result, str):
            self.set_process_cache(bot_id, thread_id, process_id)
            #         logger.info(f'Process cached with bot_id: {bot_id}, thread_id: {thread_id}, process_id: {process_id}')

            return {
                "success": False,
                "message": "Process failed: The checking function didn't return a string.",
            }

        # logger.info("RUN 2nd LLM...")

        #        logger.info(f"\nRESULT FROM 2nd LLM: {result}\n")

        if "**fail**" in result.lower():
            with self.lock:
                self.last_fail[thread_id][process_id] = result
                self.fail_count[thread_id][process_id] += 1
                self.process_history[thread_id][process_id] += (
                    "\nSupervisors concern: " + result
                )
            if self.fail_count[thread_id][process_id] <= 5:
                logger.info(
                    f"\nStep {self.counter[thread_id][process_id]} failed. Fail count={self.fail_count[thread_id][process_id]} Trying again up to 5 times...\n"
                )
                self.set_process_cache(bot_id, thread_id, process_id)
                #       logger.info(f'Process cached with bot_id: {bot_id}, thread_id: {thread_id}, process_id: {process_id}')

                return_dict = {
                    "success": False,
                    "feedback_from_supervisor": result,
                    "current system time": {datetime.now()},
                    "recovery_step": f"Review the message above and submit a clarification, and/or try this Step {self.counter[thread_id][process_id]} again:\n{self.instructions[thread_id][process_id]}",
                }
                if verbose:
                    return_dict["additional_request"] = (
                        "Please also explain and summarize this feedback from the supervisor bot to the user so they know whats going on, and how you plan to rectify it."
                    )
                else:
                    return_dict["shhh"] = (
                        "Remember you are running in slient, non-verbose mode. Limit your output as much as possible."
                    )

                return return_dict

            else:
                logger.info(
                    f"\nStep {self.counter[thread_id][process_id]} failed. Fail count={self.fail_count[thread_id][process_id]} > 5 failures on this step, stopping process...\n"
                )

                with self.lock:
                    self.done[thread_id][process_id] = True
                self.clear_process_cache(bot_id, thread_id, process_id)
                try:
                    del self.counter[thread_id][process_id]
                except:
                    pass
                logger.info(
                    f"Process cache cleared for bot_id: {bot_id}, thread_id: {thread_id}, process_id: {process_id}"
                )

                return {
                    "success": "False",
                    "message": f"The process {process_name} has failed due to > 5 repeated step completion failures.  Do not start this process again without user approval.",
                }

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
        next_step = self.chat_completion(
            extract_instructions,
            self.db_adapter,
            bot_id=bot_id,
            bot_name="",
            thread_id=thread_id,
            process_id=process_id,
            process_name=process_name,
        )

        #      logger.info(f"\nRESULT (NEXT_STEP_): {next_step}\n")

        if (
            next_step == "**done**"
            or next_step == "***done***"
            or next_step.strip().endswith("**done**")
        ):
            with self.lock:
                self.last_fail[thread_id][process_id] = None
                self.fail_count[thread_id][process_id] = None
                self.done[thread_id][process_id] = True
            # Clear the process cache when the process is complete
            self.clear_process_cache(bot_id, thread_id, process_id)
            logger.info(
                f"Process cache cleared for bot_id: {bot_id}, thread_id: {thread_id}, process_id: {process_id}"
            )

            return {
                "success": True,
                "process_complete": True,
                "message": f"Congratulations, the process {process_name} is complete.",
                "proccess_success_step": True,
                "reminder": f"If you were running this as a subprocess inside another process, be sure to continue the parent process.",
            }

        #        logger.info(f"\n{next_step}\n")

        with self.lock:
            if ">> RECURSE" in next_step or ">>RECURSE" in next_step:
                self.recurse_level += 1
                # Extract the process name or ID
                process_to_run = (
                    next_step.split(">>RECURSE")[1].strip()
                    if ">>RECURSE" in next_step
                    else next_step.split(">> RECURSE")[1].strip()
                )

                # Prepare the instruction for the bot to run the nested process
                next_step = f"""
                    Use the _run_process tool to run the process '{process_to_run}' with the following parameters:
                    - action: KICKOFF_PROCESS
                    - process_name: {process_to_run}
                    - bot_id: {bot_id}
                    - silent_mode: {silent_mode}

                    After the nested process completes, continue with the next step of this process.
                    """

                logger.info(
                    f"RECURSE found.  Running process {process_to_run} on level {self.recurse_level}"
                )

                return {
                    "success": True,
                    "message": next_step,
                }

            self.instructions[thread_id][
                process_id
            ] = f"""
                Hey **@{process['BOT_ID']}**, here is the next step of the process.

                {next_step}

                If you are asked to run either sql or snowpark_python from a given note_id, make sure you examine the note_type field and use the appropriate tool for
                the note type.  Only pass the note_id, not the code itself, to the appropriate tool where the note will be handled.

                Execute these instructions now and then pass your response to the run_process tool as a parameter called previous_response and an action of GET_NEXT_STEP.
                If you are told to run another process in these instructions, actually run it using _run_process before calling GET_NEXT_STEP for this process, do not just pretend to run it.
                If need to terminate the process early, call with action of END_PROCESS.
                """
            if verbose:
                self.instructions[thread_id][
                    process_id
                ] += """
                Tell the user what you are going to do in this step and showing interium outputs, etc. while you are running this and further steps to keep the user informed what is going on.
                For example if you are going to call a tool to perform this step, first tell the user what you're going to do.
                """
            else:
                self.instructions[thread_id][
                    process_id
                ] += """
                This process is being run in low verbosity mode, so do not generate a lot of text while running this process. Just do whats required, call the right tools, etc.
                Also, it you are asked to run either sql or snowpark_python from a given note_id, make sure you examine the note_type field and use the appropriate tool for
                the note type.  Only pass the note_id, not the code itself, to the appropriate tool where the note will be handled.
                """
            self.instructions[thread_id][
                process_id
            ] += f"""
                Don't stop to verify anything with the user unless specifically told to.
                By the way the current system time id: {datetime.now()}.
                In your response back to run_process, provide a detailed description of what you did, what result you achieved, and why you believe this to have successfully completed the step.
                """

        #     logger.info(f"\nEXTRACTED NEXT STEP: \n{self.instructions[thread_id][process_id]}\n")

        with self.lock:
            self.process_history[thread_id][process_id] += "\nNext step: " + next_step

        self.set_process_cache(bot_id, thread_id, process_id)
        logger.info(
            f"Process cached with bot_id: {bot_id}, thread_id: {thread_id}, process_id: {process_id}"
        )

        return {
            "success": True,
            "message": self.instructions[thread_id][process_id],
        }

    elif action == "END_PROCESS":
        logger.info(
            f"Received END_PROCESS action for process {process_name} on level {self.recurse_level}"
        )

        with self.lock:
            self.done[thread_id][process_id] = True

        clear_process_registers_by_thread(thread_id)

        self.process_id[thread_id] = None

        self.clear_process_cache(bot_id, thread_id, process_id)
        logger.info(
            f"Process cache cleared for bot_id: {bot_id}, thread_id: {thread_id}, process_id: {process_id}"
        )

        self.recurse_level -= 1
        logger.info(f"Returning to recursion level {self.recurse_level}")

        return {
            "success": True,
            "message": f"The process {process_name} has finished.  You may now end the process.",
        }
    if action == "STOP_ALL_PROCESSES":
        try:
            self.clear_all_process_registers(thread_id)
            return {"Success": True, "Message": "All processes stopped (?)"}
        except Exception as e:
            return {"Success": False, "Error": f"Failed to stop all processes: {e}"}
    else:
        logger.info("No action specified.")
        return {"success": False, "message": "No action specified."}

_run_process_functions = (run_process,)

# Called from bot_os_tools.py to update the global list of functions
def get_google_drive_tool_functions():
    return _run_process_functions
