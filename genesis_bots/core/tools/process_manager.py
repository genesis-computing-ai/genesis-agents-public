from genesis_bots.core.logging_config import logger
from datetime import datetime

from typing import Optional
from textwrap import dedent
import random
import string

from genesis_bots.core.bot_os_tools2 import (
    BOT_ID_IMPLICIT_FROM_CONTEXT,
    THREAD_ID_IMPLICIT_FROM_CONTEXT,
    ToolFuncGroup,
    ToolFuncParamDescriptor,
    gc_tool,
)

from genesis_bots.core.tools.tool_helpers import chat_completion, get_processes_list, get_process_info, get_sys_email

from genesis_bots.connectors import get_global_db_connector
db_adapter = get_global_db_connector()

process_manager_tools = ToolFuncGroup(
    name="process_manager_tools",
    description=dedent(
    """
    Manages schedules to automatically run processes on a schedule (sometimes called tasks), including creating, updating,
    and deleting schedules for processes.
    """
    ),
    lifetime="PERSISTENT",
)


# manage processes
@gc_tool(
    action=ToolFuncParamDescriptor(
        name="action",
        description="The action to perform on a process: CREATE, UPDATE, DELETE, CREATE_PROCESS_CONFIG, UPDATE_PROCESS_CONFIG, DELETE_PROCESS_CONFIG, HIDE_PROCESS, UNHIDE_PROCESS LIST returns a list of all processes, SHOW shows full instructions and details for a process, SHOW_CONFIG shows the configuration for a process, HIDE_PROCESS hides the process from the list of processes, UNHIDE_PROCESS unhides the process from the list of processes, or TIME to get current system time. If you are trying to deactivate a schedule for a task, use _process_scheduler instead, don't just DELETE the process",
        required=True,
        llm_type_desc=dict(
            type="string",
            enum=[
                "CREATE",
                "CREATE_CONFIRMED",
                "UPDATE",
                "UPDATE_CONFIRMED",
                "DELETE",
                "DELETE_CONFIRMED",
                "CREATE_PROCESS_CONFIG",
                "UPDATE_PROCESS_CONFIG",
                "DELETE_PROCESS_CONFIG",
                "HIDE_PROCESS",
                "UNHIDE_PROCESS",
                "LIST",
                "SHOW",
                "SHOW_CONFIG",
                "TIME",
            ],
        ),
    ),
    process_id=dedent(
        """The unique identifier of the process, create as bot_id_<random 6 character string>. MAKE SURE TO DOUBLE-CHECK THAT YOU ARE USING THE CORRECT process_id ON UPDATES AND DELETES!
            Required for CREATE, UPDATE, and DELETE."""
    ),
    process_name="The name of the process.  Required for SHOW.",
    process_instructions="DETAILED instructions for completing the process  Do NOT summarize or simplify instructions provided by a user.",
    process_config="Configuration string used by process when running.",
    hidden="If true, the process will not be shown in the list of processes.  This is used to create processes to test the bots functionality without showing them to the user.",
    allow_code="If true, the process will be allowed to include sql, python, or other source code directly in the process instructions.  This is not recommended, but is allowed for short code snippets or for testing purposes.  It preferred that code exists in notes",
    bot_id=BOT_ID_IMPLICIT_FROM_CONTEXT,
    thread_id=THREAD_ID_IMPLICIT_FROM_CONTEXT,
    _group_tags_=[process_manager_tools],
)
def manage_processes(
    action: str,
    bot_id: str = None,
    process_id: str = None,
    process_instructions: str = None,
    thread_id: str = None,
    process_name: str = None,
    process_config: str = None,
    hidden: bool = False,
    allow_code: bool = False,
) -> dict:
    """
    Manages processs in the PROCESSES table with actions to create, delete, update a process, or stop all processes

    Args:
        action (str): The action to perform
        bot_id (str): The bot ID associated with the process.
        process_id (str): The process ID for the process to manage.
        process_details (dict, optional): The details of the process for create or update actions.

    Returns:
        dict: A dictionary with the result of the operation.
    """

    process_details = {}
    if process_name:
        process_details['process_name'] = process_name
    if process_instructions:
        process_details['process_instructions'] = process_instructions
    if process_config:
        process_details['process_config'] = process_config
    if hidden:
        process_details['hidden'] = hidden
    if allow_code:
        process_details['allow_code'] = allow_code

    # If process_name is specified but not in process_details, add it to process_details
    # if process_name and process_details and 'process_name' not in process_details:
    #     process_details['process_name'] = process_name

    # # If process_name is specified but not in process_details, add it to process_details
    # if process_name and process_details==None:
    #     process_details = {}
    #     process_details['process_name'] = process_name

    required_fields_create = [
        "process_name",
        "process_instructions",
    ]

    required_fields_update = [
        "process_name",
        "process_instructions",
    ]

    action = action.upper()

    if action == "TIME":
        return {
            "current_system_time": datetime.now().astimezone()
        }
    cursor = db_adapter.client.cursor()

    try:
        if action == "HIDE_PROCESS":
            hide_query = f"""
                UPDATE {db_adapter.schema}.PROCESSES
                SET HIDDEN = True
                WHERE PROCESS_ID = %(process_id)s
            """
            cursor.execute(
                hide_query,
                {"process_id": process_id},
            )
            db_adapter.client.commit()

        if action == "UNHIDE_PROCESS":
            hide_query = f"""
                UPDATE {db_adapter.schema}.PROCESSES
                SET HIDDEN = False
                WHERE PROCESS_ID = %(process_id)s
            """
            cursor.execute(
                hide_query,
                {"process_id": process_id},
            )
            db_adapter.client.commit()

        if action in ["UPDATE_PROCESS_CONFIG", "CREATE_PROCESS_CONFIG", "DELETE_PROCESS_CONFIG"]:
            process_config = '' if action == "DELETE_PROCESS_CONFIG" else process_config
            update_query = f"""
                UPDATE {db_adapter.schema}.PROCESSES
                SET PROCESS_CONFIG = %(process_config)s
                WHERE PROCESS_ID = %(process_id)s
            """
            cursor.execute(
                update_query,
                {"process_config": process_config, "process_id": process_id},
            )
            db_adapter.client.commit()

            return {
                "Success": True,
                "Message": f"process_config updated or deleted",
                "process_id": process_id,
            }

        if action == "CREATE" or action == "CREATE_CONFIRMED":
            # Check for dupe name
            sql = f"SELECT process_id FROM {db_adapter.schema}.PROCESSES WHERE bot_id = %s and process_name = %s"
            cursor.execute(sql, (bot_id, process_details['process_name']))

            record = cursor.fetchone()

            if record:
                return {
                    "Success": False,
                    "Error": f"Process with name {process_details['process_name']} already exists, it's id is {record[0]}.  Please choose a different name.",
                    "existing_process_id": record[0],
                }

        if action == "UPDATE" or action == 'UPDATE_CONFIRMED':
            # Check for dupe name
            sql = f"SELECT * FROM {db_adapter.schema}.PROCESSES WHERE bot_id = %s and process_name = %s"
            cursor.execute(sql, (bot_id, process_details['process_name']))

            record = cursor.fetchone()

            if record and '_golden' in record[2]:  # process_id
                return {
                    "Success": False,
                    "Error": f"Process with name {process_details['process_name']} is a system process and can not be updated.  Suggest making a copy with a new name."
                }

        sys_default_email = get_sys_email()

        if action == "CREATE" or action == "UPDATE":
            # Check for dupe name
            # sql = f"SELECT * FROM {db_adapter.schema}.PROCESSES WHERE bot_id = %s and process_name = %s"
            # cursor.execute(sql, (bot_id, process_details['process_name']))

            # record = cursor.fetchone()

            # if record and '_golden' in record['process_id']:
            #     return {
            #         "Success": False,
            #         "Error": f"Process with name {process_details['process_name']}.  Please choose a different name."
            #     }

            check_for_code_instructions = f"""Please examine the text below and return only the word 'SQL' if the text contains
            actual SQL code, not a reference to SQL code, or only the word 'PYTHON' if the text contains actual Python code, not a reference to Python code.
            If the text contains both, return only 'SQL + PYTHON'.  Do not return any other verbage.  If the text contains
            neither, return only the word 'NO CODE':\n {process_details['process_instructions']}"""

            result = chat_completion(check_for_code_instructions, db_adapter, bot_id=bot_id, bot_name='')

            print(f"Result of check_for_code_instructions: {result}")

            # Send process_instructions to 2nd LLM to check it and format nicely
            tidy_process_instructions = f"""
            Below is a process that has been submitted by a user.  Please review it to insure it is something
            that will make sense to the run_process tool.  If not, make changes so it is organized into clear
            steps.  Make sure that it is tidy, legible and properly formatted.

            Do not create multiple options for the instructions, as whatever you return will be used immediately.
            Return the updated and tidy process.  If there is an issue with the process, return an error message."""

            if result != 'NO CODE':
                tidy_process_instructions += f"""
            Since the process contains either sql or snowpark_python code, you will need to ask the user if they want
            to allow code in the process.  If they do, go ahead and allow the code to remain in the process.
            If they do not, extract the code and create a new note with
            your manage_notebook tool, making sure to specify the note_type field as either 'sql or 'snowpark_python'.
            Then replace the code in the process with the note_id of the new note.  Do not
            include the note contents in the process, just include an instruction to run the note with the note_id."""

            tidy_process_instructions += f"""

            If the process wants to send an email to a default email, or says to send an email but doesn't specify
            a recipient address, note that the SYS$DEFAULT_EMAIL is currently set to {sys_default_email}.
            Include the notation of SYS$DEFAULT_EMAIL in the instructions instead of the actual address, unless
            the instructions specify a different specific email address.

            If one of the steps of the process involves scheduling this process to run on a schedule, remove that step,
            and instead include a note separate from the cleaned up process that the user should instead use _process_scheduler
            to schedule the process after it has been created.

            The process is as follows:\n {process_details['process_instructions']}
            """

            tidy_process_instructions = "\n".join(
                line.lstrip() for line in tidy_process_instructions.splitlines()
            )

            process_details['process_instructions'] = chat_completion(tidy_process_instructions, db_adapter, bot_id = bot_id, bot_name = '', thread_id=thread_id, process_id=process_id, process_name=process_name)

        if action == "CREATE":
            logger.info("Received CREATE action")
            return {
                "Success": False,
                "Cleaned up instructions": process_details['process_instructions'],
                "Confirmation_Needed": "I've run the process instructions through a cleanup step.  Please reconfirm these instructions and all the other process details with the user, then call this function again with the action CREATE_CONFIRMED to actually create the process.",
                "Next Step": "If you're ready to create this process call this function again with action CREATE_CONFIRMED instead of CREATE"
            #    "Info": f"By the way the current system time is {datetime.now().strftime('%Y-%m-%d %H:%M:%S %Z')}",
            }

        if action == "UPDATE":
            return {
                "Success": False,
                "Cleaned up instructions": process_details['process_instructions'],
                "Confirmation_Needed": "I've run the process instructions through a cleanup step.  Please reconfirm these instructions and all the other process details with the user, then call this function again with the action UPDATE_CONFIRMED to actually update the process.",
                "Next Step": "If you're ready to update this process call this function again with action UPDATE_CONFIRMED instead of UPDATE"
            #    "Info": f"By the way the current system time is {datetime.now().strftime('%Y-%m-%d %H:%M:%S %Z')}",
            }

    except Exception as e:
        return {"Success": False, "Error": f"Error in process_manager: {e}"}

    if action == "CREATE_CONFIRMED":
        action = "CREATE"
    if action == "UPDATE_CONFIRMED":
        action = "UPDATE"

    if action == "DELETE":
        return {
            "Success": False,
            "Confirmation_Needed": "Please reconfirm that you are deleting the correct process_ID, and double check with the user they want to delete this process, then call this function again with the action DELETE_CONFIRMED to actually delete the process.  Call with LIST to double-check the process_id if you aren't sure that its right.",
        }

    if action == "DELETE_CONFIRMED":
        action = "DELETE"

    if action not in ["CREATE", "DELETE", "UPDATE", "LIST", "SHOW"]:
        return {"Success": False, "Error": "Invalid action specified. Should be CREATE, DELETE, UPDATE, LIST, or SHOW."}

    if action == "LIST":
        logger.info("Running get processes list")
        return get_processes_list(bot_id if bot_id is not None else "all")

    if action == "SHOW":
        logger.info("Running show process info")
        if bot_id is None:
            return {"Success": False, "Error": "bot_id is required for SHOW action"}
        if process_id is None:
            if process_details is None or ('process_name' not in process_details and 'process_id' not in process_details):
                return {"Success": False, "Error": "Either process_name or process_id is required in process_details for SHOW action"}

        if process_id is not None or 'process_id' in process_details:

            return get_process_info(bot_id=bot_id, process_id=process_id)
        else:
            process_name = process_details['process_name']
            return get_process_info(bot_id=bot_id, process_name=process_name)

    process_id_created = False
    if process_id is None:
        if action == "CREATE":
            logger.info("CREATE action with no process_id")
            process_id = f"{bot_id}_{''.join(random.choices(string.ascii_letters + string.digits, k=6))}"
            process_id_created = True
        else:
            return {"Success": False, "Error": f"Missing process_id field"}

    if action in ["CREATE", "UPDATE"] and not process_details:
        return {
            "Success": False,
            "Error": "Process details must be provided for CREATE or UPDATE action.",
        }

    if action in ["CREATE"] and any(
        field not in process_details for field in required_fields_create
    ):
        logger.info("CREATE action missing fields - tell user")
        missing_fields = [
            field
            for field in required_fields_create
            if field not in process_details
        ]
        return {
            "Success": False,
            "Error": f"Missing required process details: {', '.join(missing_fields)}",
        }

    if action in ["UPDATE"] and any(
        field not in process_details for field in required_fields_update
    ):
        missing_fields = [
            field
            for field in required_fields_update
            if field not in process_details
        ]
        return {
            "Success": False,
            "Error": f"Missing required process details: {', '.join(missing_fields)}",
        }

    if bot_id is None:
        return {
            "Success": False,
            "Error": "The 'bot_id' field is required."
        }

    try:
        logger.info(f"Received CREATE action with{'' if db_adapter.schema else 'out' } db_adapter.schema")
        if action == "CREATE":
            insert_query = f"""
                INSERT INTO {db_adapter.schema}.PROCESSES (
                    created_at, updated_at, process_id, bot_id, process_name, process_instructions
                ) VALUES (
                    current_timestamp(), current_timestamp(), %(process_id)s, %(bot_id)s, %(process_name)s, %(process_instructions)s
                )
            """ if db_adapter.schema else f"""
                INSERT INTO PROCESSES (
                    created_at, updated_at, process_id, bot_id, process_name, process_instructions
                ) VALUES (
                    current_timestamp(),current_timestamp(), %(process_id)s, %(bot_id)s, %(process_name)s, %(process_instructions)s
                )
            """

            # Generate 6 random alphanumeric characters
            if process_id_created == False:
                random_suffix = "".join(
                random.choices(string.ascii_letters + string.digits, k=6)
                )
                process_id_with_suffix = process_id + "_" + random_suffix
            else:
                process_id_with_suffix = process_id
            cursor.execute(
                insert_query,
                {
                    **process_details,
                    "process_id": process_id_with_suffix,
                    "bot_id": bot_id,
                },
            )
            # Get process_name from process_details if available, otherwise set to "Unknown"
            process_name = process_details.get('process_name', "Unknown")
            db_adapter.client.commit()
            logger.info("Successfully CREATED process {process_name} with process_id: {process_id_with_suffix}")
            return {
                "Success": True,
                "Message": f"process successfully created.",
                "process_id": process_id_with_suffix,
                "process_name": process_name,
                "Suggestion": "Now that the process is created, remind the user of the process_id and process_name, and offer to test it using run_process, and if there are any issues you can later on UPDATE the process using manage_processes to clarify anything needed.  OFFER to test it, but don't just test it unless the user agrees.  Also OFFER to schedule it to run on a scheduled basis, using _process_scheduler if desired.",
                "Reminder": "If you are asked to test the process, use _run_process function to each step, don't skip ahead since you already know what the steps are, pretend you don't know what the process is and let run_process give you one step at a time!",
            }

        elif action == "DELETE":
            fetch_process_name_query = f"""
                SELECT process_name FROM {db_adapter.schema}.PROCESSES
                WHERE process_id = %s
                """ if db_adapter.schema else """
                SELECT process_name FROM PROCESSES
                WHERE process_id = %s
                """
            cursor.execute(fetch_process_name_query, (process_id,))
            result = cursor.fetchone()

            if result:
                process_name = result[0]
                delete_query = f"""
                    DELETE FROM {db_adapter.schema}.PROCESSES
                    WHERE process_id = %s
                """ if db_adapter.schema else f"""
                    DELETE FROM PROCESSES
                    WHERE process_id = %s
                """
                cursor.execute(delete_query, (process_id,))

                delete_task_queries = f"""
                    DELETE FROM {db_adapter.schema}.TASKS
                    WHERE task_name = %s
                """ if db_adapter.schema else """
                    DELETE FROM TASKS
                    WHERE task_name = %s
                """
                cursor.execute(delete_task_queries, (process_name,))
                db_adapter.client.commit()

                return {
                    "Success": True,
                    "Message": f"process deleted",
                    "process_id": process_id,
                }
            else:
                return {
                    "Success": False,
                    "Error": f"process with process_id {process_id} not found",
                }

        elif action == "UPDATE":
            update_query = f"""
                UPDATE {db_adapter.schema}.PROCESSES
                SET {', '.join([f"{key} = %({key})s" for key in process_details.keys()])},
                updated_at = current_timestamp()
                WHERE process_id = %(process_id)s
            """ if db_adapter.schema else f"""
                UPDATE PROCESSES
                SET {', '.join([f"{key} = %({key})s" for key in process_details.keys()])},
                updated_at = current_timestamp()
                WHERE process_id = %(process_id)s
            """
            cursor.execute(
                update_query,
                {**process_details, "process_id": process_id},
            )
            db_adapter.client.commit()
            return {
                "Success": True,
                "Message": f"process successfully updated",
                "process_id": process_id,
                "Suggestion": "Now that the process is updated, offer to test it using run_process, and if there are any issues you can later on UPDATE the process again using manage_processes to clarify anything needed.  OFFER to test it, but don't just test it unless the user agrees.",
                "Reminder": "If you are asked to test the process, use _run_process function to each step, don't skip ahead since you already know what the steps are, pretend you don't know what the process is and let run_process give you one step at a time!",
            }
        
        return {"Success": True, "Message": f"process update or delete confirmed."}
    except Exception as e:
        return {"Success": False, "Error": str(e)}

    finally:
        cursor.close()


def insert_process_history(
    process_id,
    work_done_summary,
    process_status,
    updated_process_learnings,
    report_message="",
    done_flag=False,
    needs_help_flag="N",
    process_clarity_comments="",
):
    """
    Inserts a row into the PROCESS_HISTORY table.

    Args:
        process_id (str): The unique identifier for the process.
        work_done_summary (str): A summary of the work done.
        process_status (str): The status of the process.
        updated_process_learnings (str): Any new learnings from the process.
        report_message (str): The message to report about the process.
        done_flag (bool): Flag indicating if the process is done.
        needs_help_flag (bool): Flag indicating if help is needed.
        process_clarity_comments (str): Comments on the clarity of the process.
    """
    insert_query = (
        f"""
                INSERT INTO {db_adapter.schema}.PROCESS_HISTORY (
                    process_id, work_done_summary, process_status, updated_process_learnings,
                    report_message, done_flag, needs_help_flag, process_clarity_comments
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s
                )
            """
        if db_adapter.schema
        else f"""
                INSERT INTO PROCESS_HISTORY (
                    process_id, work_done_summary, process_status, updated_process_learnings,
                    report_message, done_flag, needs_help_flag, process_clarity_comments
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s
                )
            """
    )
    try:
        cursor = db_adapter.client.cursor()
        cursor.execute(
            insert_query,
            (
                process_id,
                work_done_summary,
                process_status,
                updated_process_learnings,
                report_message,
                done_flag,
                needs_help_flag,
                process_clarity_comments,
            ),
        )
        db_adapter.client.commit()
        cursor.close()
        logger.info(
            f"Process history row inserted successfully for process_id: {process_id}"
        )
    except Exception as e:
        logger.info(f"An error occurred while inserting the process history row: {e}")
        if cursor is not None:
            cursor.close()


process_manage_functions = [manage_processes,]

# Called from bot_os_tools.py to update the global list of functions
def get_process_manager_functions():
    return process_manage_functions
