from textwrap import dedent
import re
import os

from genesis_bots.core.bot_os_tools2 import (
    BOT_ID_IMPLICIT_FROM_CONTEXT,
    THREAD_ID_IMPLICIT_FROM_CONTEXT,
    ToolFuncGroup,
    ToolFuncParamDescriptor,
    gc_tool,
)

from genesis_bots.google_sheets.g_sheets import (
    add_g_file_comment,
    add_reply_to_g_file_comment,
    find_g_file_by_name,
    get_g_file_comments,
    get_g_file_version,
    get_g_file_web_link,
    get_g_folder_directory,
    read_g_sheet,
    write_g_sheet_cell,
)

from genesis_bots.connectors import get_global_db_connector
db_adapter = get_global_db_connector()


google_drive_tools = ToolFuncGroup(
    name="google_drive_tools",
    description="Performs certain actions on Google Drive, including logging in, listing files, setting the root folder,and getting the version number of a google file (g_file).",
    lifetime="PERSISTENT",
)


@gc_tool(
    action=dedent(
        """
        The action to be performed on Google Drive.  Possible actions are:
            LOGIN - Used to login in to Google Workspace with OAuth2.0.  Not implemented
            LIST - Get's list of files in a folder.  Same as DIRECTORY, DIR, GET FILES IN FOLDER
            SET_ROOT_FOLDER - Sets the root folder for the user on their drive
            GET_FILE_VERSION_NUM - Gets the version numbergiven a g_file id
            GET_COMMENTS - Gets the comments and replies for a file give a g_file_id.  Also includes the anchor tag which specifies the cell where the comment is located
            ADD_COMMENT - Adds a comment to a file given a g_file_id
            ADD_REPLY_TO_COMMENT - Adds a reply to a comment given a g_file_id and a comment_id.  Also includes the anchor tag which specifies the cell where the comment is located
            GET_SHEET - (Also can be READ_SHEET) - Gets the contents of a Google Sheet given a g_file_id
            EDIT_SHEET - (Also can be WRITE SHEET) - Edits a Google Sheet given a g_file_id and values.  Passing
                a cell range is optional
            GET_LINK_FROM_FILE_ID - Gets the url link to a file given a g_file_id
            GET_FILE_BY_NAME - Searches for a file by name and returns the file id
            SAVE_QUERY_RESULTS_TO_G_SHEET - Saves the results of a query to a Google Sheet
    """
    ),
    g_folder_id="The unique identifier of a folder stored on Google Drive.",
    g_file_id="The unique identifier of a file stored on Google Drive.",
    g_sheet_cell="Cell in a Google Sheet to edit/update.",
    g_sheet_value="Value to update the cell in a Google Sheet or update a comment.",
    g_file_comment_id="The unique identifier of a comment stored on Google Drive.",
    g_file_name="The name of a file, files, folder, or folders stored on Google Drive.",
    g_sheet_query="Query string to run and save the results to a Google Sheet.",
    g_sheet_anchor="The anchor tag which specifies the cell where the comment is located.",
    user="""The unique identifier of the process_id. MAKE SURE TO DOUBLE-CHECK THAT YOU ARE USING THE CORRECT test_process_id
        ON UPDATES AND DELETES!  Required for CREATE, UPDATE, and DELETE.""",
    thread_id="THREAD_ID_IMPLICIT_FROM_CONTEXT",
    _group_tags_=[google_drive_tools],
)
def google_drive(
    action: str,
    g_folder_id: str = None,
    g_file_id: str = None,
    g_sheet_cell: str = None,
    g_sheet_value: str = None,
    g_file_comment_id: str = None,
    g_file_name: str = None,
    g_sheet_query: str = None,
    g_sheet_anchor: str = None,
    user: str = None,
    thread_id: str = None,
) -> None:
    """
    A wrapper for LLMs to access/manage Google Drive files by performing specified actions such as listing or downloading files.

    Args:
        action (str): The action to perform on the Google Drive files. Supported actions are 'LIST' and 'DOWNLOAD'.

    Returns:
        dict: A dictionary containing the result of the action. E.g. for 'LIST', it includes the list of files in the Google Drive.
    """
    def column_to_number(letter: str) -> int:
        num = 0
        for char in letter:
            num = num * 26 + (ord(char.upper()) - ord('A') + 1)
        return num

    def number_to_column(num: int) -> str:
        result = ""
        while num > 0:
            num -= 1
            result = chr(num % 26 + 65) + result
            num //= 26
        return result

    def verify_single_cell(g_sheet_cell: str) -> str:
        pattern = r"^([a-zA-Z]{1,3})(\d{1,4})$"
        match = re.match(pattern, g_sheet_cell)
        if not match:
            raise ValueError("Invalid g_sheet_cell format. It should start with 1-3 letters followed by 1-4 numbers.")

        col, row = match.groups()
        # next_col = number_to_column(column_to_number(col) + 1)
        cell_range = f"{col}{row}" # :{next_col}{row}"

        return cell_range

    def verify_cell_range(g_sheet_cell):
        pattern = r"^([A-Z]{1,2})(\d+):([A-Z]{1,2})(\d+)$"
        match = re.match(pattern, g_sheet_cell)

        # Verify range is only one cell
        if not match:
            raise ValueError("Invalid g_sheet_cell format. It should be in the format 'A1:B1'.")

        # column_1, row_1, column_2, row_2 = match.groups()
        # column_1_int = column_to_number(column_1)
        # column_2_int = column_to_number(column_2)

        return True

    if action == "LIST":
        try:
            files = get_g_folder_directory(
                g_folder_id, None, user=db_adapter.user
            )
            if files is False:
                return {"Success": False, "Error": "Could not get files from Google Drive, missing credentials."}
            else:
                return {"Success": True, "files": files}
        except Exception as e:
            return {"Success": False, "Error": str(e)}

    elif action == "GET_FILE_BY_NAME":
        try:
            file_id = find_g_file_by_name(g_file_name, None, db_adapter.user)
            return {"Success": True, "id": file_id}
        except Exception as e:
            return {"Success": False, "Error": str(e)}

    elif action == "SET_ROOT_FOLDER":
        raise NotImplementedError

    elif action == "GET_LINK_FROM_FILE_ID":
        try:
            web_link = get_g_file_web_link(g_file_id, None, db_adapter.user)
            return {"Success": True, "web_link": web_link}
        except Exception as e:
            return {"Success": False, "Error": str(e)}

    elif action == "GET_FILE_VERSION_NUM":
        try:
            file_version_num = get_g_file_version(g_file_id, None, db_adapter.user)
        except Exception as e:
            return {"Success": False, "Error": str(e)}

        return {"Success": True, "file_version_num": file_version_num}

    elif action == "GET_COMMENTS":
        try:
            comments_and_replies = get_g_file_comments(db_adapter.user, g_file_id)
            return {"Success": True, "Comments & Replies": comments_and_replies}
        except Exception as e:
            return {"Success": False, "Error": str(e)}

    elif action == "ADD_COMMENT":
        try:
            result = add_g_file_comment(
                g_file_id, g_sheet_value, None, db_adapter.user
            )
            return {"Success": True, "Result": result}
        except Exception as e:
            return {"Success": False, "Error": str(e)}

    elif action == "ADD_REPLY_TO_COMMENT":
        try:
            result = add_reply_to_g_file_comment(
                g_file_id, g_file_comment_id, g_sheet_value, g_file_comment_id, None, db_adapter.user
            )
            return {"Success": True, "Result": result}
        except Exception as e:
            return {"Success": False, "Error": str(e)}

    # elif action == "GET_SHEET":
    #     cell_range = verify_single_cell(g_sheet_cell)
    #     try:
    #         value = read_g_sheet(g_file_id, cell_range, None, db_adapter.user)
    #         return {"Success": True, "value": value}
    #     except Exception as e:
    #         return {"Success": False, "Error": str(e)}

    elif action == "EDIT_SHEET":
        # cell_range = verify_single_cell(g_sheet_cell)

        print(
            f"\nG_sheet value to insert to cell {g_sheet_cell}: Value: {g_sheet_value}\n"
        )

        write_g_sheet_cell(
            g_file_id, g_sheet_cell, g_sheet_value, None, db_adapter.user
        )

        return {
            "Success": True,
            "Message": f"g_sheet value to insert to cell {g_sheet_cell}: Value: {g_sheet_value}",
        }

    elif action == "GET_SHEET" or action == "READ_SHEET":
        # cell_range = verify_single_cell(g_sheet_cell)
        try:
            value = read_g_sheet(
                g_file_id, g_sheet_cell, None, db_adapter.user
            )
            return {"Success": True, "value": value}
        except Exception as e:
            return {"Success": False, "Error": str(e)}

    elif action == "LOGIN":
        from google_auth_oauthlib.flow import Flow

        SCOPES = [
            "https://www.googleapis.com/auth/drive.file",
            "https://www.googleapis.com/auth/documents",
            "https://www.googleapis.com/auth/drive"
        ]

        redirect_url = f"{os.environ['NGROK_BASE_URL']}:8080/oauth"

        flow = Flow.from_client_secrets_file(
            f"credentials.json",
            scopes=SCOPES,
            redirect_uri = redirect_url #"http://127.0.0.1:8080/oauth",  # Your redirect URI
        )
        auth_url, _ = flow.authorization_url(prompt="consent")
        return {"Success": "True", "auth_url": f"<{auth_url}|View Document>"}

    elif action == "SAVE_QUERY_RESULTS_TO_G_SHEET":
        db_adapter.run_query(g_sheet_query, export_to_google_sheet = True)
        pass

    return {"Success": False, "Error": "Invalid action specified."}


google_drive_functions = (google_drive,)

# Called from bot_os_tools.py to update the global list of functions
def get_google_drive_tool_functions():
    return google_drive_functions
