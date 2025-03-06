from datetime import datetime
from genesis_bots.core.logging_config import logger

from textwrap import dedent

from genesis_bots.core.bot_os_tools2 import (
    BOT_ID_IMPLICIT_FROM_CONTEXT,
    THREAD_ID_IMPLICIT_FROM_CONTEXT,
    ToolFuncGroup,
    ToolFuncParamDescriptor,
    gc_tool,
)

from genesis_bots.core.tools.tool_helpers import get_processes_list
from genesis_bots.core.bot_os_utils import datetime_to_string

from genesis_bots.connectors import get_global_db_connector
db_adapter = get_global_db_connector()

process_scheduler_tools = ToolFuncGroup(
    name="process_scheduler_tools",
    description=dedent("""Manages schedules to automatically run processes on a schedule (sometimes called tasks), including creating,
                       updating,and deleting schedules for processes."""),
    lifetime="PERSISTENT",
)


# process scheduler
@gc_tool(
    action=dedent(
        """The action to perform on the process schedule: CREATE, UPDATE, or DELETE.  Or LIST to get details on all
                      scheduled processes for a bot, or TIME to get current system time or HISTORY to get the history of a scheduled
                      process by task_id.  For history lookup task_id first using LIST.  To deactive a schedule without deleting it,
                      UPDATE it and set task_active to FALSE."""
    ),
    bot_id="BOT_ID_IMPLICIT_FROM_CONTEXT",
    task_id=dedent(
        """The unique identifier of the process schedule, create as bot_id_<random 6 character string>. MAKE SURE TO
                       DOUBLE-CHECK THAT YOU ARE USING THE CORRECT task_id, its REQUIRED ON CREATE, UPDATES AND DELETES! Note that this
                       is not the same as the process_id"""
    ),
    task_details=ToolFuncParamDescriptor(
        name="task_details",
        description="The properties of this object are the details of the process schedule for use when creating and updating.",
        llm_type_desc=dict(
            type="object",
            properties=dict(
                process_name=dict(
                    type="string",
                    description="The name of the process to run on a schedule. This must be a valid process name shown by _manage_processes LIST",
                ),
                primary_report_to_type=dict(
                    type="string",
                    description="Set to SLACK_USER",
                ),
                primary_report_to_id=dict(
                    type="string",
                    description="The Slack USER ID of the person who told you to create this schedule for a process.",
                ),
                next_check_ts=dict(
                    type="string",
                    description="The timestamp for the next run of the process 'YYYY-MM-DD HH:MM:SS'. Call action TIME to get current time and timezone. Make sure this time is in the future.",
                ),
                action_trigger_type=dict(
                    type="string",
                    description="Always set to TIMER",
                ),
                action_trigger_details=dict(
                    type="string",
                    description="""For TIMER, a description of when to call the task, eg every hour, Tuesdays at 9am, every morning.  Also be clear about whether the task should be called one time, or is recurring, and if recurring if it should recur forever or stop at some point.""",
                ),
            ),
        ),
        required=False,
    ),
    history_rows=ToolFuncParamDescriptor(
        name="history_rows",
        description="Number of history rows to retrieve.",
        required=False,
        llm_type_desc=dict(type="integer"),
    ),
    thread_id="THREAD_ID_IMPLICIT_FROM_CONTEXT",
    _group_tags_=[process_scheduler_tools],
)
def process_scheduler(
    action: str,
    bot_id: str,
    task_id: str = None,
    task_details: dict = None,
    thread_id: str = None,
    history_rows: int = 10,
) -> dict:
    """
    Manages tasks in the TASKS table with actions to create, delete, or update a task."""
    import random
    import string
    """
    Manages tasks in the TASKS table with actions to create, delete, or update a task.

    Args:
        action (str): The action to perform - 'CREATE', 'DELETE', or 'UPDATE'.
        bot_id (str): The bot ID associated with the task.
        task_id (str): The task ID for the task to manage.
        task_details (dict, optional): The details of the task for create or update actions.

    Returns:
        dict: A dictionary with the result of the operation.
    """

    #    logger.info("Reached process scheduler")

    if task_details and 'process_name' in task_details and 'task_name' not in task_details:
        task_details['task_name'] = task_details['process_name']
        del task_details['process_name']

    required_fields_create = [
        "task_name",
        "primary_report_to_type",
        "primary_report_to_id",
        "next_check_ts",
        "action_trigger_type",
        "action_trigger_details",
        "last_task_status",
        "task_learnings",
        "task_active",
    ]

    required_fields_update = ["task_active"]
    client = db_adapter.client
    cursor = client.cursor()
    if action == "HISTORY":
        if not task_id:
            return {
                "Success": False,
                "Error": "task_id is required for retrieving task history. You can get the task_id by calling this function with the 'LIST' action for the bot_id."
            }
        limit = history_rows
        history_query = f"""
            SELECT * FROM {db_adapter.schema}.TASK_HISTORY
            WHERE task_id = %s
            ORDER BY RUN_TIMESTAMP DESC
            LIMIT %s
            """
        try:
            cursor.execute(history_query, (task_id, limit))
            client.commit()
            history = cursor.fetchall()
            return {
                "Success": True,
                "Task History": history,
                "history_rows": limit
            }
        except Exception as e:
            return {
                "Success": False,
                "Error": e
            }

    if action == "TIME":
        return {
            "current_system_time": _get_current_time_with_timezone()
        }
    action = action.upper()

    if action == "CREATE":
        return {
            "Success": False,
            "Confirmation_Needed": "Please reconfirm all the scheduled process details with the user, then call this function again with the action CREATE_CONFIRMED to actually create the schedule for the process.   Make sure to be clear in the action_trigger_details field whether the process schedule is to be triggered one time, or if it is ongoing and recurring. Also make the next Next Check Timestamp is in the future, and aligns with when the user wants the task to run next",
            "Process Schedule Details": task_details,
            "Info": f"By the way the current system time is {_get_current_time_with_timezone()}",
        }
    if action == "CREATE_CONFIRMED":
        action = "CREATE"

    if action == "UPDATE":

        return {
            "Success": False,
            "Confirmation_Needed": "Please reconfirm all the updated process details with the user, then call this function again with the action UPDATE_CONFIRMED to actually update the schedule for the process.   Make sure to be clear in the action_trigger_details field whether the process schedule is to be triggered one time, or if it is ongoing and recurring. Also make the next Next Check Timestamp is in the future, and aligns with when the user wants the task to run next.",
            "Proposed Updated Process Schedule Details": task_details,
            "Info": f"By the way the current system time is {_get_current_time_with_timezone()}",
        }
    if action == "UPDATE_CONFIRMED":
        action = "UPDATE"

    if action == "DELETE":
        return {
            "Success": False,
            "Confirmation_Needed": "Please reconfirm that you are deleting the correct TASK_ID, and double check with the user they want to delete this schedule for the process, then call this function again with the action DELETE_CONFIRMED to actually delete the task.  Call with LIST to double-check the task_id if you aren't sure that its right.",
        }

    if action == "DELETE_CONFIRMED":
        action = "DELETE"

    if action not in ["CREATE", "DELETE", "UPDATE", "LIST"]:
        return {"Success": False, "Error": "Invalid action specified."}

    if action == "LIST":
        try:
            list_query = (
                f"SELECT * FROM {db_adapter.schema}.TASKS WHERE upper(bot_id) = upper(%s)"
            )
            cursor.execute(list_query, (bot_id,))
            tasks = cursor.fetchall()
            task_list = []
            for task in tasks:
                next_check = None
                if task[5] is not None:
                    next_check = datetime_to_string(task[5])
                task_dict = {
                    "task_id": task[0],
                    "bot_id": task[1],
                    "task_name": task[2],
                    "primary_report_to_type": task[3],
                    "primary_report_to_id": task[4],
                    "next_check_ts": next_check,
                    "action_trigger_type": task[6],
                    "action_trigger_details": task[7],
                    "process_name_to_run": task[8],
                    "reporting_instructions": task[9],
                    "last_task_status": task[10],
                    "task_learnings": task[11],
                    "task_active": task[12],
                }
                task_list.append(task_dict)
            return {"Success": True, "Scheduled Processes": task_list, "Note": "Don't take any immediate actions on this information unless instructed to by the user. Also note the task_id is the id of the schedule, not the id of the process to run."}
        except Exception as e:
            return {
                "Success": False,
                "Error": f"Failed to list tasks for bot {bot_id}: {e}",
            }

    if task_id is None:
        return {"Success": False, "Error": f"Missing task_id field"}

    if action in ["CREATE", "UPDATE"] and not task_details:
        return {
            "Success": False,
            "Error": "Task details must be provided for CREATE or UPDATE action.",
        }

    if action in ["CREATE"] and task_details and any(
        field not in task_details for field in required_fields_create
    ):
        missing_fields = [
            field for field in required_fields_create if field not in task_details
        ]
        return {
            "Success": False,
            "Error": f"Missing required task details: {', '.join(missing_fields)}",
        }

    if action in ["UPDATE"] and task_details and any(
        field not in task_details for field in required_fields_update
    ):
        missing_fields = [
            field for field in required_fields_update if field not in task_details
        ]
        return {
            "Success": False,
            "Error": f"Missing required task details: {', '.join(missing_fields)}",
        }

    # Check if the action is CREATE or UPDATE
    if action in ["CREATE", "UPDATE"] and task_details and "task_name" in task_details:
        # Check if the task_name is a valid process for the bot
        valid_processes = get_processes_list(bot_id=bot_id)
        if not valid_processes["Success"]:
            return {
                "Success": False,
                "Error": f"Failed to retrieve processes for bot {bot_id}: {valid_processes['Error']}",
            }

        if task_details["task_name"] not in [
            process["process_name"] for process in valid_processes["processes"]
        ]:
            return {
                "Success": False,
                "Error": f"Invalid task_name: {task_details.get('task_name')}. It must be one of the valid processes for this bot",
                "Valid_Processes": [process["process_name"] for process in valid_processes["processes"]],
            }

    # Convert timestamp from string in format 'YYYY-MM-DD HH:MM:SS' to a Snowflake-compatible timestamp
    if task_details is not None and task_details.get("task_active", False):
        try:
            formatted_next_check_ts = datetime.strptime(
                task_details["next_check_ts"], "%Y-%m-%d %H:%M:%S"
            )
        except ValueError as ve:
            return {
                "Success": False,
                "Error": f"Invalid timestamp format for 'next_check_ts'. Required format: 'YYYY-MM-DD HH:MM:SS' in system timezone. Error details: {ve}",
                "Info": f"Current system time in system timezone is {_get_current_time_with_timezone()}. Please note that the timezone should not be included in the submitted timestamp.",
            }
        if formatted_next_check_ts < datetime.now():
            return {
                "Success": False,
                "Error": "The 'next_check_ts' is in the past.",
                "Info": f"Current system time is {_get_current_time_with_timezone()}",
            }

    try:
        if action == "CREATE":
            insert_query = f"""
                INSERT INTO {db_adapter.schema}.TASKS (
                    task_id, bot_id, task_name, primary_report_to_type, primary_report_to_id,
                    next_check_ts, action_trigger_type, action_trigger_details, task_instructions,
                    reporting_instructions, last_task_status, task_learnings, task_active
                ) VALUES (
                    %(task_id)s, %(bot_id)s, %(task_name)s, %(primary_report_to_type)s, %(primary_report_to_id)s,
                    %(next_check_ts)s, %(action_trigger_type)s, %(action_trigger_details)s, null,
                    null, %(last_task_status)s, %(task_learnings)s, %(task_active)s
                )
            """

            # Generate 6 random alphanumeric characters
            random_suffix = "".join(
                random.choices(string.ascii_letters + string.digits, k=6)
            )
            task_id_with_suffix = task_id + "_" + random_suffix
            cursor.execute(
                insert_query,
                {**task_details, "task_id": task_id_with_suffix, "bot_id": bot_id},
            )
            client.commit()
            return {
                "Success": True,
                "Message": f"Task successfully created, next check scheduled for {task_details['next_check_ts']}",
            }

        elif action == "DELETE":
            delete_query = f"""
                DELETE FROM {db_adapter.schema}.TASKS
                WHERE task_id = %s AND bot_id = %s
            """
            cursor.execute(delete_query, (task_id, bot_id))
            client.commit()

        elif action == "UPDATE":
            if task_details['task_active'] == False:
                task_details['next_check_ts'] = None
            update_query = f"""
                UPDATE {db_adapter.schema}.TASKS
                SET {', '.join([f"{key} = %({key})s" for key in task_details.keys()])}
                WHERE task_id = %(task_id)s AND bot_id = %(bot_id)s
            """
            cursor.execute(
                update_query, {**task_details, "task_id": task_id, "bot_id": bot_id}
            )
            client.commit()

        return {"Success": True, "Message": f"Task update or delete confirmed."}
    except Exception as e:
        return {"Success": False, "Error": str(e)}

    finally:
        cursor.close()


def _get_current_time_with_timezone():
    current_time = datetime.now().astimezone()
    return current_time.strftime("%Y-%m-%d %H:%M:%S %Z")


process_scheduler_functions = [process_scheduler,]

# Called from bot_os_tools.py to update the global list of functions
def get_process_scheduler_functions():
    return process_scheduler_functions
