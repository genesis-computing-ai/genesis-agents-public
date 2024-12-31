
def google_drive(
    self,
    action,
    g_folder_id=None,
    g_file_id=None,
    g_sheet_cell=None,
    g_sheet_value=None,
    g_file_comment_id=None,
    g_file_name=None,
    g_sheet_query=None,
    user=None,
    thread_id=None,
):
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
                g_folder_id, None, user=self.db_adapter.user
            )
            return {"Success": True, "files": files}
        except Exception as e:
            return {"Success": False, "Error": str(e)}

    elif action == "GET_FILE_BY_NAME":
        try:
            file_id = find_g_file_by_name(g_file_name, None, self.db_adapter.user)
            return {"Success": True, "id": file_id}
        except Exception as e:
            return {"Success": False, "Error": str(e)}

    elif action == "SET_ROOT_FOLDER":
        raise NotImplementedError

    elif action == "GET_LINK_FROM_FILE_ID":
        try:
            web_link = get_g_file_web_link(g_file_id, None, self.db_adapter.user)
            return {"Success": True, "web_link": web_link}
        except Exception as e:
            return {"Success": False, "Error": str(e)}

    elif action == "GET_FILE_VERSION_NUM":
        try:
            file_version_num = get_g_file_version(g_file_id, None, self)
        except Exception as e:
            return {"Success": False, "Error": str(e)}

        return {"Success": True, "file_version_num": file_version_num}

    elif action == "GET_COMMENTS":
        try:
            comments_and_replies = get_g_file_comments(self.db_adapter.user, g_file_id)
            return {"Success": True, "Comments & Replies": comments_and_replies}
        except Exception as e:
            return {"Success": False, "Error": str(e)}

    elif action == "ADD_COMMENT":
        try:
            result = add_g_file_comment(
                g_file_id, g_sheet_value, None, self.db_adapter.user
            )
            return {"Success": True, "Result": result}
        except Exception as e:
            return {"Success": False, "Error": str(e)}

    elif action == "ADD_REPLY_TO_COMMENT":
        try:
            result = add_reply_to_g_file_comment(
                g_file_id, g_file_comment_id, g_sheet_value, g_file_comment_id, None, self.db_adapter.user
            )
            return {"Success": True, "Result": result}
        except Exception as e:
            return {"Success": False, "Error": str(e)}

    # elif action == "GET_SHEET":
    #     cell_range = verify_single_cell(g_sheet_cell)
    #     try:
    #         value = read_g_sheet(g_file_id, cell_range, None, self.db_adapter.user)
    #         return {"Success": True, "value": value}
    #     except Exception as e:
    #         return {"Success": False, "Error": str(e)}

    elif action == "EDIT_SHEET":
        # cell_range = verify_single_cell(g_sheet_cell)

        print(
            f"\nG_sheet value to insert to cell {g_sheet_cell}: Value: {g_sheet_value}\n"
        )

        write_g_sheet_cell(
            g_file_id, g_sheet_cell, g_sheet_value, None, self.db_adapter.user
        )

        return {
            "Success": True,
            "Message": f"g_sheet value to insert to cell {g_sheet_cell}: Value: {g_sheet_value}",
        }

    elif action == "GET_SHEET" or action == "READ_SHEET":
        # cell_range = verify_single_cell(g_sheet_cell)
        try:
            value = read_g_sheet(
                g_file_id, g_sheet_cell, None, self.db_adapter.user
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
        self.db_adapter.run_query(g_sheet_query, export_to_google_sheet = True)
        pass

    return {"Success": False, "Error": "Invalid action specified."}