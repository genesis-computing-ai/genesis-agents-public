from genesis_bots.core.logging_config import logger
from datetime import datetime
import random
import string
from typing import Optional

from textwrap import dedent

from genesis_bots.core.bot_os_tools2 import (
    BOT_ID_IMPLICIT_FROM_CONTEXT,
    THREAD_ID_IMPLICIT_FROM_CONTEXT,
    ToolFuncGroup,
    ToolFuncParamDescriptor,
    gc_tool,
)

from genesis_bots.connectors import get_global_db_connector
db_adapter = get_global_db_connector()

manage_tests_tools = ToolFuncGroup(
    name="manage_tests_tools",
    description="Functions to execute Genesis-internal system tests",
    lifetime="PERSISTENT",
)

def _get_test_manager_list(bot_id="all"):
    cursor = db_adapter.client.cursor()
    try:
        if bot_id == "all":
            list_query = f"SELECT * FROM {db_adapter.schema}.test_manager order by test_priority" if db_adapter.schema else f"SELECT * FROM test_manager order by test_priority"
            cursor.execute(list_query)
        else:
            list_query = f"SELECT * FROM {db_adapter.schema}.test_manager WHERE upper(bot_id) = upper(%s) order by test_priority" if db_adapter.schema else f"SELECT * FROM test_manager WHERE upper(bot_id) = upper(%s) order by test_priority"
            cursor.execute(list_query, (bot_id,))
        test_processes = cursor.fetchall()
        test_process_list = []
        for test_process in test_processes:
            test_process_dict = {
                "bot_id": test_process[1],
                "test_process_id": test_process[2],
                'test_process_name': test_process[3],
                'test_type': test_process[4],
                'test_priority': test_process[5],
            }
            test_process_list.append(test_process_dict)
        return {"Success": True, "test_processs": test_process_list}
    except Exception as e:
        return {
            "Success": False,
            "Error": f"Failed to list test_processs for bot {bot_id}: {e}",
        }
    finally:
        cursor.close()


@gc_tool(
   action=ToolFuncParamDescriptor(
        name="action",
        description="Action to perform (CREATE, UPDATE, CHANGE_STATUS, LIST)",
        required=True,
        llm_type_desc=dict(type="string", enum=["CREATE", "UPDATE", "CHANGE_STATUS", "LIST"])
    ),
    test_process_id="""
            The unique identifier of the process_id. MAKE SURE TO DOUBLE-CHECK THAT YOU ARE USING THE CORRECT
            test_process_id ON UPDATES AND DELETES!  Required for CREATE, UPDATE, and DELETE.
            """,
    test_process_name="Human reable unique name for the test.",
    test_type="The type of table, either enabled or disabled.",
    test_priority="Determines the order in which the tests will run.  Lower numbers run first.",
    bot_id=BOT_ID_IMPLICIT_FROM_CONTEXT,
    thread_id=THREAD_ID_IMPLICIT_FROM_CONTEXT,
    _group_tags_=[manage_tests_tools],
)
def manage_tests(
    action: str,
    bot_id: str,
    test_process_id: str,
    test_process_name: str,
    test_type: str,
    test_priority: int = 1,
    thread_id: str = None,
):
    """
    Manages tests that will run when when the project is deployed, including adding, updating, listing and deleting tests from the list of tests to run when the
    project is deployed, allowing bots to manage tests. Remember that this is not used to create new processes.  Make sure that the user is specifically
    asking for a test to be added to the deploy sequence, have its priority weighting updated, or deleted. This tool is not used to run a test.
    If you are asked to run a tests, use the run process tool and pass the manage_process_id, do not use this tool.  If you aren't sure, ask the user to clarify.
    If you are asked to enable a test, set its test_type to enabled.  If you are asked to disable a test, set its test_type to disabled.
    """

    required_fields_add = [
        "test_process_id",
        "bot_id",
        "test_process_name",
    ]

    required_fields_update = [
        "test_process_id",
    ]

    if action not in ['ADD','ADD_CONFIRMED', 'UPDATE','UPDATE_CONFIRMED', 'DELETE', 'DELETE_CONFIRMED', 'LIST', 'TIME']:
        return {
            "Success": False,
            "Error": "Invalid action.  test manager tool only accepts actions of ADD, ADD_CONFIRMED, UPDATE, UPDATE_CONFIRMED, DELETE, LIST, or TIME."
        }

    cursor = db_adapter.client.cursor()

    if action == "TIME":
        return {
            "current_system_time": datetime.now()
        }

    if test_process_name is not None and test_process_id is None:
        cursor.execute(f"SELECT process_id FROM {db_adapter.schema}.PROCESSES WHERE process_name = %s", (test_process_name,))
        result = cursor.fetchone()
        if result:
            test_process_id = result[0]
        else:
            return {
                "Success": False,
                "Error": f"Process with name {test_process_name} not found."
            }

    action = action.upper()

    try:
        if action == "ADD" or action == "ADD_CONFIRMED":
            # Check for dupe name
            sql = f"SELECT * FROM {db_adapter.schema}.test_manager WHERE bot_id = %s and test_process_name = %s"
            cursor.execute(sql, (bot_id, test_process_name))

            record = cursor.fetchone()

            if record:
                return {
                    "Success": False,
                    "Error": f"test_process with id {test_process_name} is already included for bot {bot_id}."
                }

        if action == "UPDATE" or action == 'UPDATE_CONFIRMED':
            # Check for dupe name
            sql = f"SELECT * FROM {db_adapter.schema}.test_manager WHERE bot_id = %s and test_process_id = %s"
            cursor.execute(sql, (bot_id, test_process_name))

            record = cursor.fetchone()

            if record and '_golden' in record[2]:
                return {
                    "Success": False,
                    "Error": f"test_process with id {test_process_name} is a system test_process and can not be updated.  Suggest making a copy with a new name."
                }

        if action == "ADD":
            return {
                "Success": False,
                "Fields": {"test_process_id": test_process_id, "test_process_name": test_process_name, "bot_id": bot_id},
                "Confirmation_Needed": "Please reconfirm the field values with the user, then call this function again with the action CREATE_CONFIRMED to actually create the test_process.  If the user does not want to create a test_process, allow code in the process instructions",
                "Suggestion": "If possible, for a sql or python test_process, suggest to the user that we test the sql or python before making the test_process to make sure it works properly",
                "Next Step": "If you're ready to create this test_process or the user has chosen not to create a test_process, call this function again with action CREATE_CONFIRMED instead of CREATE.  If the user chooses to allow code in the process, allow them to do so and include the code directly in the process."
            #    "Info": f"By the way the current system time is {datetime.now().strftime('%Y-%m-%d %H:%M:%S %Z')}",
            }

        if action == "UPDATE":
            return {
                "Success": False,
                "Fields": {"test_process_id": test_process_id, "test_process_name": test_process_name, "bot_id": bot_id},
                "Confirmation_Needed": "Please reconfirm this content and all the other test_process field values with the user, then call this function again with the action UPDATE_CONFIRMED to actually update the test_process.  If the user does not want to update the test_process, allow code in the process instructions",
                "Suggestion": "If possible, for a sql or python test_process, suggest to the user that we test the sql or python before making the test_process to make sure it works properly",
                "Next Step": "If you're ready to update this test_process, call this function again with action UPDATE_CONFIRMED instead of UPDATE"
            #    "Info": f"By the way the current system time is {datetime.now().strftime('%Y-%m-%d %H:%M:%S %Z')}",
            }

    except Exception as e:
        return {"Success": False, "Error": f"Error connecting to LLM: {e}"}

    if action == "ADD_CONFIRMED":
        action = "ADD"
    if action == "UPDATE_CONFIRMED":
        action = "UPDATE"

    if action == "DELETE":
        return {
            "Success": False,
            "Confirmation_Needed": "Please reconfirm that you are deleting the correct test_process_id, and double check with the user they want to delete this test_process, then call this function again with the action DELETE_CONFIRMED to actually delete the test_process.  Call with LIST to double-check the test_process_id if you aren't sure that its right.",
        }

    if action == "DELETE_CONFIRMED":
        action = "DELETE"

    if action not in ["ADD", "DELETE", "UPDATE", "LIST", "SHOW"]:
        return {"Success": False, "Error": "Invalid action specified. Should be ADD, DELETE, UPDATE, LIST, or SHOW."}

    if action == "LIST":
        logger.info("Running get test_process list")
        return _get_test_manager_list(bot_id if bot_id is not None else "all")

    if action == "SHOW":
        logger.info("Running show test_process info")
        if bot_id is None:
            return {"Success": False, "Error": "bot_id is required for SHOW action"}
        if test_process_name is None:
            return {"Success": False, "Error": "process is required for SHOW action"}

    test_process_id_created = False
    if test_process_name is None:
        if action == "ADD":
            test_process_id_created = True
        else:
            return {"Success": False, "Error": f"Missing test_process_id field"}
    try:
        if action == "ADD":
            insert_query = f"""
                INSERT INTO {db_adapter.schema}.test_manager (
                    created_at, updated_at, test_process_id, bot_id
                ) VALUES (
                    current_timestamp(), current_timestamp(), %(test_process_id)s, %(bot_id)s
                )
            """ if db_adapter.schema else f"""
                INSERT INTO test_manager (
                    created_at, updated_at, test_process_id, bot_id
                ) VALUES (
                    current_timestamp(), current_timestamp(), %(test_process_id)s, %(bot_id)s
                )
            """

            insert_query= "\n".join(
                line.lstrip() for line in insert_query.splitlines()
            )
            # Generate 6 random alphanumeric characters
            if test_process_id_created == False:
                random_suffix = "".join(
                random.choices(string.ascii_letters + string.digits, k=6)
                )
                test_process_id_with_suffix = test_process_id + "_" + random_suffix
            else:
                test_process_id_with_suffix = test_process_id
            cursor.execute(
                insert_query,
                {
                    "test_process_id": test_process_id_with_suffix,
                    "bot_id": bot_id,
                    "test_process_name": test_process_name,
                    "test_priority": test_priority
                },
            )

            db_adapter.client.commit()
            return {
                "Success": True,
                "Message": f"test_process successfully created.",
                "test_process Id": test_process_id_with_suffix,
                "Suggestion": "Now that the test_process has been added to the test suite, OFFER to test it, but don't just test it unless the user agrees.  ",
            }

        elif action == "DELETE":
            delete_query = f"""
                DELETE FROM {db_adapter.schema}.test_manager
                WHERE test_process_id = %s
            """ if db_adapter.schema else f"""
                DELETE FROM test_process
                WHERE test_process_id = %s
            """
            cursor.execute(delete_query, (test_process_id))

            return {
                "Success": True,
                "Message": f"test_process deleted",
                "test_process_id": test_process_id,
            }

        elif action == "UPDATE":
            update_query = f"""
                UPDATE {db_adapter.schema}.test_manager
                SET updated_at = CURRENT_TIMESTAMP, test_process_id=%s, bot_iprocess=%s, test_process_content=%s
                WHERE test_process_id = %s
            """ if db_adapter.schema else """
                UPDATE test_process
                SET updated_at = CURRENT_TIMESTAMP, test_process_id=%s, bot_iprocess=%s, test_process_content=%s
                WHERE test_process_id = %s
            """
            cursor.execute(
                update_query,
                (test_process_id, bot_id, test_process_name,test_type, test_process_id)
            )
            db_adapter.client.commit()
            return {
                "Success": True,
                "Message": "test_process successfully updated",
                "test_process id": test_process_id,
            }
        return {"Success": True, "Message": f"test_process update or delete confirmed."}
    except Exception as e:
        return {"Success": False, "Error": str(e)}

    finally:
        cursor.close()


test_manager_functions = [manage_tests,]

# Called from bot_os_tools.py to update the global list of functions
def get_test_manager_functions():
    return test_manager_functions
