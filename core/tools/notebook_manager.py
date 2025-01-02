# from core.logging_config import logger
# from datetime import datetime
# import random
# import string

# from textwrap import dedent
# import re
# import os

# from core.bot_os_tools2 import (
#     BOT_ID_IMPLICIT_FROM_CONTEXT,
#     THREAD_ID_IMPLICIT_FROM_CONTEXT,
#     ToolFuncGroup,
#     ToolFuncParamDescriptor,
#     gc_tool,
# )

# from connectors import get_global_db_connector
# db_adapter = get_global_db_connector()

# from core.tools.tool_helpers import chat_completion, get_sys_email

# manage_notebook_tools = ToolFuncGroup(
#     name="manage_notebook_tools",
#     description="",
#     lifetime="PERSISTENT",
# )

# def _insert_notebook_history(
#     note_id,
#     work_done_summary,
#     note_status,
#     updated_note_learnings,
#     report_message="",
#     done_flag=False,
#     needs_help_flag="N",
#     note_clarity_comments="",
# ):
#     """
#     Inserts a row into the NOTEBOOK_HISTORY table.

#     Args:
#         note_id (str): The unique identifier for the note.
#         work_done_summary (str): A summary of the work done.
#         note_status (str): The status of the note.
#         updated_note_learnings (str): Any new learnings from the note.
#         report_message (str): The message to report about the note.
#         done_flag (bool): Flag indicating if the note is done.
#         needs_help_flag (bool): Flag indicating if help is needed.
#         note_clarity_comments (str): Comments on the clarity of the note.
#     """
#     insert_query = f"""
#         INSERT INTO {db_adapter.schema}.NOTEBOOK_HISTORY (
#             note_id, work_done_summary, note_status, updated_note_learnings,
#             report_message, done_flag, needs_help_flag, note_clarity_comments
#         ) VALUES (
#             %s, %s, %s, %s, %s, %s, %s, %s
#         )
#     """ if db_adapter.schema else f"""
#         INSERT INTO NOTEBOOK_HISTORY (
#             note_id, work_done_summary, note_status, updated_note_learnings,
#             report_message, done_flag, needs_help_flag, note_clarity_comments
#         ) VALUES (
#             %s, %s, %s, %s, %s, %s, %s, %s
#         )
#     """
#     try:
#         cursor = db_adapter.client.cursor()
#         cursor.execute(
#             insert_query,
#             (
#                 note_id,
#                 work_done_summary,
#                 note_status,
#                 updated_note_learnings,
#                 report_message,
#                 done_flag,
#                 needs_help_flag,
#                 note_clarity_comments,
#             ),
#         )
#         db_adapter.client.commit()
#         cursor.close()
#         logger.info(
#             f"Notebook history row inserted successfully for note_id: {note_id}"
#         )
#     except Exception as e:
#         logger.info(f"An error occurred while inserting the notebook history row: {e}")
#         if cursor is not None:
#             cursor.close()

# def _get_notebook_list(bot_id="all"):
#     cursor = db_adapter.client.cursor()
#     try:
#         if bot_id == "all":
#             list_query = f"SELECT * FROM {db_adapter.schema}.NOTEBOOK" if db_adapter.schema else f"SELECT note_id, bot_id FROM NOTEBOOK"
#             cursor.execute(list_query)
#         else:
#             list_query = f"SELECT * FROM {db_adapter.schema}.NOTEBOOK WHERE upper(bot_id) = upper(%s)" if db_adapter.schema else f"SELECT note_id, bot_id FROM NOTEBOOK WHERE upper(bot_id) = upper(%s)"
#             cursor.execute(list_query, (bot_id,))
#         notes = cursor.fetchall()
#         note_list = []
#         for note in notes:
#             note_dict = {
#                 "timestamp": note[0],
#                 "bot_id": note[1],
#                 "note_id": note[2],
#                 'note_name': note[3],
#                 'note_type': note[4],
#                 'note_content': note[5],
#                 'note_params': note[6]
#             }
#             note_list.append(note_dict)
#         return {"Success": True, "notes": note_list}
#     except Exception as e:
#         return {
#             "Success": False,
#             "Error": f"Failed to list notes for bot {bot_id}: {e}",
#         }
#     finally:
#         cursor.close()

# def _get_note_info(bot_id=None, note_id=None):
#     cursor = db_adapter.client.cursor()
#     try:
#         result = None

#         if note_id is None or note_id == '':
#             return {
#                 "Success": False,
#                 "Error": "Note_id must be provided and cannot be empty."
#             }
#         if note_id is not None and note_id != '':
#             query = f"SELECT * FROM {db_adapter.schema}.NOTEBOOK WHERE bot_id LIKE %s AND note_id = %s" if db_adapter.schema else f"SELECT * FROM NOTEBOOK WHERE bot_id LIKE %s AND note_id = %s"
#             cursor.execute(query, (f"%{bot_id}%", note_id))
#             result = cursor.fetchone()

#         if result:
#             # Assuming the result is a tuple of values corresponding to the columns in the NOTEBOOK table
#             # Convert the tuple to a dictionary with appropriate field names
#             field_names = [desc[0] for desc in cursor.description]
#             return {
#                 "Success": True,
#                 "Data": dict(zip(field_names, result)),
#                 "Note": "Only use this information to help manage or update notes",
#                 "Important!": "If a user has asked you to show these notes to them, output them verbatim, do not modify or summarize them."
#             }
#         else:
#             return {}
#     except Exception as e:
#         return {}

# def manage_notebook(
#     action, bot_id=None, note_id=None, note_name = None, note_content=None, note_params=None, thread_id=None, note_type=None, note_config = None
# ):
#     """
#     Manages notes in the NOTEBOOK table with actions to create, delete, or update a note.

#     Args:
#         action (str): The action to perform
#         bot_id (str): The bot ID associated with the note.
#         note_id (str): The note ID for the note to manage.
#         note_content (str): The content of the note for create or update actions.
#         note_params (str): The parameters for the note for create or update actions.

#     Returns:
#         dict: A dictionary with the result of the operation.
#     """

#     required_fields_create = [
#         "note_id",
#         "bot_id",
#         "note_name",
#         "note_content",
#     ]

#     required_fields_update = [
#         "note_id",
#         "bot_id",
#         "note_name",
#         "note_content",
#     ]

#     if action not in ['CREATE','CREATE_CONFIRMED', 'UPDATE','UPDATE_CONFIRMED', 'DELETE', 'DELETE_CONFIRMED', 'LIST', 'TIME']:
#         return {
#             "Success": False,
#             "Error": "Invalid action.  Manage Notebook tool only accepts actions of CREATE, CREATE_CONFIRMED, UPDATE, UPDATE_CONFIRMED, DELETE, LIST, or TIME."
#         }

#     try:
#         if not self.done[thread_id][self.process_id[thread_id]]:
#             return {
#                 "Success": False,
#                 "Error": "You cannot run the notebook manager from within a process.  Please run this tool outside of a process."
#             }
#     except KeyError as e:
#         pass

#     if action == "TIME":
#         return {
#             "current_system_time": datetime.now()
#         }
#     action = action.upper()

#     cursor = db_adapter.client.cursor()

#     try:
#         if action in ["UPDATE_NOTE_CONFIG", "CREATE_NOTE_CONFIG", "DELETE_NOTE_CONFIG"]:
#             note_config = '' if action == "DELETE_NOTE_CONFIG" else note_config
#             update_query = f"""
#                 UPDATE {db_adapter.schema}.NOTEBOOK
#                 SET NOTE_CONFIG = %(note_config)s
#                 WHERE NOTE_ID = %(note_id)s
#             """
#             cursor.execute(
#                 update_query,
#                 {"note_config": note_config, "note_id": note_id},
#             )
#             db_adapter.client.commit()

#             return {
#                 "Success": True,
#                 "Message": f"note_config updated or deleted",
#                 "note_id": note_id,
#             }

#         if action == "CREATE" or action == "CREATE_CONFIRMED":
#             # Check for dupe name
#             sql = f"SELECT * FROM {db_adapter.schema}.NOTEBOOK WHERE bot_id = %s and note_id = %s"
#             cursor.execute(sql, (bot_id, note_id))

#             record = cursor.fetchone()

#             if record:
#                 return {
#                     "Success": False,
#                     "Error": f"Note with id {note_id} already exists for bot {bot_id}.  Please choose a different id."
#                 }

#         if action == "UPDATE" or action == 'UPDATE_CONFIRMED':
#             # Check for dupe name
#             sql = f"SELECT * FROM {db_adapter.schema}.NOTEBOOK WHERE bot_id = %s and note_id = %s"
#             cursor.execute(sql, (bot_id, note_id))

#             record = cursor.fetchone()

#             if record and '_golden' in record[2]:
#                 return {
#                     "Success": False,
#                     "Error": f"Note with id {note_id} is a system note and can not be updated.  Suggest making a copy with a new name."
#                 }

#         if (action == "CREATE" or action == "UPDATE") and note_type == 'process':
#             # Send note_instructions to 2nd LLM to check it and format nicely if note type 'process'
#             note_field_name = 'Note Content'
#             confirm_notification_prefix = ''
#             tidy_note_content = f"""
#             Below is a note that has been submitted by a user.  Please review it to insure it is something
#             that will make sense to the run_process tool.  If not, make changes so it is organized into clear
#             steps.  Make sure that it is tidy, legible and properly formatted.

#             Do not create multiple options for the instructions, as whatever you return will be used immediately.
#             Return the updated and tidy instructions.  If there is an issue with the instructions, return an error message.

#             If the note wants to send an email to a default email, or says to send an email but doesn't specify
#             a recipient address, note that the SYS$DEFAULT_EMAIL is currently set to {get_sys_email()}.
#             Include the notation of SYS$DEFAULT_EMAIL in the instructions instead of the actual address, unless
#             the instructions specify a different specific email address.

#             The note is as follows:\n {note_content}
#             """

#             tidy_note_content= "\n".join(
#                 line.lstrip() for line in tidy_note_content.splitlines()
#             )

#             note_content = chat_completion(tidy_note_content, db_adapter, bot_id = bot_id, bot_name = '', thread_id=thread_id, note_id=note_id)

#         if action == "CREATE":
#             return {
#                 "Success": False,
#                 "Fields": {"note_id": note_id, "note_name": note_name, "bot_id": bot_id, "note content": note_content, "note_params:": note_params},
#                 "Confirmation_Needed": "Please reconfirm the field values with the user, then call this function again with the action CREATE_CONFIRMED to actually create the note.  If the user does not want to create a note, allow code in the process instructions",
#                 "Suggestion": "If possible, for a sql or python note, suggest to the user that we test the sql or python before making the note to make sure it works properly",
#                 "Next Step": "If you're ready to create this note or the user has chosen not to create a note, call this function again with action CREATE_CONFIRMED instead of CREATE.  If the user chooses to allow code in the process, allow them to do so and include the code directly in the process."
#             #    "Info": f"By the way the current system time is {datetime.now().strftime('%Y-%m-%d %H:%M:%S %Z')}",
#             }

#         if action == "UPDATE":
#             return {
#                 "Success": False,
#                 "Fields": {"note_id": note_id, "note_name": note_name, "bot_id": bot_id, "note content": note_content, "note_param:": note_params},
#                 "Confirmation_Needed": "Please reconfirm this content and all the other note field values with the user, then call this function again with the action UPDATE_CONFIRMED to actually update the note.  If the user does not want to update the note, allow code in the process instructions",
#                 "Suggestion": "If possible, for a sql or python note, suggest to the user that we test the sql or python before making the note to make sure it works properly",
#                 "Next Step": "If you're ready to update this note, call this function again with action UPDATE_CONFIRMED instead of UPDATE"
#             #    "Info": f"By the way the current system time is {datetime.now().strftime('%Y-%m-%d %H:%M:%S %Z')}",
#             }

#     except Exception as e:
#         return {"Success": False, "Error": f"Error connecting to LLM: {e}"}

#     if action == "CREATE_CONFIRMED":
#         action = "CREATE"
#     if action == "UPDATE_CONFIRMED":
#         action = "UPDATE"

#     if action == "DELETE":
#         return {
#             "Success": False,
#             "Confirmation_Needed": "Please reconfirm that you are deleting the correct note_id, and double check with the user they want to delete this note, then call this function again with the action DELETE_CONFIRMED to actually delete the note.  Call with LIST to double-check the note_id if you aren't sure that its right.",
#         }

#     if action == "DELETE_CONFIRMED":
#         action = "DELETE"

#     if action not in ["CREATE", "DELETE", "UPDATE", "LIST", "SHOW"]:
#         return {"Success": False, "Error": "Invalid action specified. Should be CREATE, DELETE, UPDATE, LIST, or SHOW."}

#     if action == "LIST":
#         logger.info("Running get notebook list")
#         return _get_notebook_list(bot_id if bot_id is not None else "all")

#     if action == "SHOW":
#         logger.info("Running show notebook info")
#         if bot_id is None:
#             return {"Success": False, "Error": "bot_id is required for SHOW action"}
#         if note_id is None:
#             return {"Success": False, "Error": "note_id is required for SHOW action"}

#         if note_id is not None:
#             return _get_note_info(bot_id=bot_id, note_id=note_id)
#         else:
#             note_name = note_content['note_id']
#             return _get_note_info(bot_id=bot_id, note_name=note_name)

#     note_id_created = False
#     if note_id is None:
#         if action == "CREATE":
#             note_id = f"{bot_id}_{''.join(random.choices(string.ascii_letters + string.digits, k=6))}"
#             note_id_created = True
#         else:
#             return {"Success": False, "Error": f"Missing note_id field"}

#     try:
#         if action == "CREATE":
#             insert_query = f"""
#                 INSERT INTO {db_adapter.schema}.NOTEBOOK (
#                     created_at, updated_at, note_id, bot_id, note_name, note_content, note_params
#                 ) VALUES (
#                     current_timestamp(), current_timestamp(), %(note_id)s, %(bot_id)s, %(note_name)s, %(note_content)s, %(note_params)s
#                 )
#             """ if db_adapter.schema else f"""
#                 INSERT INTO NOTEBOOK (
#                     created_at, updated_at, note_id, bot_id, note_name, note_content, note_params
#                 ) VALUES (
#                     current_timestamp(), current_timestamp(), %(note_id)s, %(bot_id)s, %(note_name)s, %(note_content)s, %(note_params)s
#                 )
#             """

#             insert_query= "\n".join(
#                 line.lstrip() for line in insert_query.splitlines()
#             )
#             # Generate 6 random alphanumeric characters
#             if note_id_created == False:
#                 random_suffix = "".join(
#                 random.choices(string.ascii_letters + string.digits, k=6)
#                 )
#                 note_id_with_suffix = note_id + "_" + random_suffix
#             else:
#                 note_id_with_suffix = note_id
#             cursor.execute(
#                 insert_query,
#                 {
#                     "note_id": note_id_with_suffix,
#                     "bot_id": bot_id,
#                     "note_name": note_name,
#                     "note_content": note_content,
#                     "note_params": note_params,
#                 },
#             )

#             db_adapter.client.commit()
#             return {
#                 "Success": True,
#                 "Message": f"note successfully created.",
#                 "Note Id": note_id_with_suffix,
#                 "Suggestion": "Now that the note is created, remind the user of the note_id and offer to test it using the correct runner, either sql, snowpark_python, or process, depending on the type set in the note_type field, and if there are any issues you can later on UPDATE the note using manage_notes to clarify anything needed.  OFFER to test it, but don't just test it unless the user agrees.  ",
#             }

#         elif action == "DELETE":
#             delete_query = f"""
#                 DELETE FROM {db_adapter.schema}.NOTEBOOK
#                 WHERE note_id = %s
#             """ if db_adapter.schema else f"""
#                 DELETE FROM NOTEBOOK
#                 WHERE note_id = %s
#             """
#             cursor.execute(delete_query, (note_id))

#             return {
#                 "Success": True,
#                 "Message": f"note deleted",
#                 "note_id": note_id,
#             }

#         elif action == "UPDATE":
#             update_query = f"""
#                 UPDATE {db_adapter.schema}.NOTEBOOK
#                 SET updated_at = CURRENT_TIMESTAMP, note_id=%s, bot_id=%s, note_name=%s, note_content=%s, note_params=%s, note_type=%s
#                 WHERE note_id = %s
#             """ if db_adapter.schema else """
#                 UPDATE NOTEBOOK
#                 SET updated_at = CURRENT_TIMESTAMP, note_id=%s, bot_id=%s, note_name=%s, note_content=%s, note_params=%s, note_type=%s
#                 WHERE note_id = %s
#             """
#             cursor.execute(
#                 update_query,
#                 (note_id, bot_id, note_name, note_content, note_params, note_type, note_id)
#             )
#             db_adapter.client.commit()
#             return {
#                 "Success": True,
#                 "Message": "note successfully updated",
#                 "Note id": note_id,
#                 "Suggestion": "Now that the note is updated, offer to test it using run_note, and if there are any issues you can later on UPDATE the note again using manage_notebook to clarify anything needed. OFFER to test it, but don't just test it unless the user agrees.",
#             }
#         return {"Success": True, "Message": f"note update or delete confirmed."}
#     except Exception as e:
#         return {"Success": False, "Error": str(e)}

#     finally:
#         cursor.close()

# notebook_manager_functions = (manage_notebook,)

# # Called from bot_os_tools.py to update the global list of functions
# def get_notebook_manager_functions():
#     return notebook_manager_functions
