# Commented out section is for using OAuth instead of creating a service account

import os.path

from google.oauth2.service_account import Credentials
# from google.auth.transport.requests import Request
# from google.oauth2.credentials import Credentials
# from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload
from datetime import datetime
import mimetypes
import os


# If modifying these scopes, delete the file token.json.
SCOPES = [
    "https://www.googleapis.com/auth/drive.file",
    "https://www.googleapis.com/auth/documents",
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets"
]


# def upload_file_to_folder(path_to_file, parent_folder_id):
#     creds = Credentials.from_service_account_file(
#             SERVICE_ACCOUNT_FILE, scopes=SCOPES
#         )
#     service = build("drive", "v3", credentials=creds)

#     file_path = os.path(path_to_file)
#     filename = os.path.basename(file_path)
#     mime_type = mimetypes.guess_type(file_path)

#     file_metadata = {"name": filename}
#     if parent_folder_id:
#         file_metadata["parents"] = [parent_folder_id]

#     media = MediaFileUpload(file_path, mimetype=mime_type[0])
#     file = (
#         service.files().create(body=file_metadata, media_body=media, fields="id").execute()
#     )
#     print(f'File ID: "{file.get("id")}".')
#     return file.get("id")


def save_text_to_google_folder(
    self, shared_folder_id, file_name, text = "No text in file", creds=None
):
    if not text:
        text = "No text received in save_text_to_google_folder."

    if not creds:
        SERVICE_ACCOUNT_FILE = f"g-workspace-{self.user}.json"
        try:
            # Authenticate using the service account JSON file
            creds = Credentials.from_service_account_file(
                SERVICE_ACCOUNT_FILE, scopes=SCOPES
            )
        except Exception as e:
            print(f"Error loading credentials: {e}")
            return None

    docs_service = build("docs", "v1", credentials=creds)
    drive_service = build("drive", "v3", credentials=creds)

    # Check if a document with the same name already exists in the shared folder
    query = f"'{shared_folder_id}' in parents and name='{file_name}' and mimeType='application/vnd.google-apps.document'"
    response = (
        drive_service.files().list(q=query, fields="files(id, name)").execute()
    )
    files = response.get("files", [])

    if files:
        for file in files:
            print(
                f"Deleting existing file: {file.get('name')} (ID: {file.get('id')})"
            )
            docs_service.files().delete(fileId=file.get("id")).execute()

    # Create a new document
    if not file_name:
        file_name = "genesis_" + datetime.now().strftime("%m%d%Y_%H:%M:%S")

    body = {"title": file_name}
    doc = docs_service.documents().create(body=body).execute()
    print("Created document with title: {0}".format(doc.get("title")))
    doc_id = doc.get("documentId")
    print(f"Document ID: {doc_id}")

    # Move the document to shared folder
    if shared_folder_id:
        file = (
            drive_service.files()
            .update(
                fileId=doc_id,
                addParents=shared_folder_id,
                fields="id, parents",
            )
            .execute()
        )
        print(f"File moved to folder: {file} | Parent folder {file['parents'][0]}")

    # Verify the new document exists in Google Drive
    try:
        file_verify = (
            drive_service.files()
            .get(fileId=doc_id, fields="id, name, parents, webViewLink")
            .execute()
        )
        print(f"File store confirmed: {file_verify}")
    except:
        raise Exception("Error creating document in Google Drive")

    parent = (
        drive_service.files().get(fileId=shared_folder_id, fields="id, name").execute()
    )
    print(f"Parent folder name: {parent.get('name')} (ID: {parent.get('id')})")

    requests = [{"insertText": {"location": {"index": 1}, "text": text}}]

    result = (
        docs_service.documents()
        .batchUpdate(documentId=doc_id, body={"requests": requests})
        .execute()
    )

    print("Document content updated: ", result)

    return file_verify.get("webViewLink")


def create_folder_in_folder(folder_name, parent_folder_id, user):
    SERVICE_ACCOUNT_FILE = f'g-workspace-{user}.json'
    creds = Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES
        )
    service = build("drive", "v3", credentials=creds)

    file_metadata = {
        "name": folder_name,
        "parents": [parent_folder_id],
        "mimeType": "application/vnd.google-apps.folder",
    }

    file = service.files().create(body=file_metadata, fields="id").execute()

    print(f'Folder ID: {file.get("id")} | Folder name: {folder_name}')

    return file.get("id")


# def create_g_drive_folder(folder_name:str = "folder"):
#     try:
#         creds = Credentials.from_service_account_file(
#             SERVICE_ACCOUNT_FILE, scopes=SCOPES
#         )
#         service = build("drive", "v3", credentials=creds)

#         file_metadata = {
#             "name": folder_name,
#             "mimeType": "application/vnd.google-apps.folder",
#         }

#         file = service.files().create(body=file_metadata, fields="id").execute()
#         print(f'Folder ID: "{file.get("id")}".')
#         return file.get("id")

# except HttpError as err:
#     print(err)
#     return None


def export_to_google_docs(text: str = 'No text received.', shared_folder_id: str = None, user =None, file_name = None):
    """
    Creates new file in Google Docs named Genesis_mmddyyy_hh:mm:ss from text string
    """
    if not user:
        raise Exception("User not specified for google drive conventions.")

    SERVICE_ACCOUNT_FILE = f"g-workspace-{user}.json"
    try:
        # Authenticate using the service account JSON file
        creds = Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES
        )
        docs_service = build("docs", "v1", credentials=creds)
        drive_service = build("drive", "v3", credentials=creds)

        # Check if a document with the same name already exists in the shared folder
        query = f"'{shared_folder_id}' in parents and name='{file_name}' and mimeType='application/vnd.google-apps.document'"
        response = drive_service.files().list(q=query, fields="files(id, name)").execute()
        files = response.get("files", [])

        if files:
            for file in files:
                print(f"Deleting existing file: {file.get('name')} (ID: {file.get('id')})")
                drive_service.files().delete(fileId=file.get("id")).execute()

        # Create a new document
        if not file_name:
            file_name = "genesis_" + datetime.now().strftime("%m%d%Y_%H:%M:%S")

        body = {"title": file_name}
        doc = docs_service.documents().create(body=body).execute()
        print("Created document with title: {0}".format(doc.get("title")))
        doc_id = doc.get("documentId")
        print(f"Document ID: {doc_id}")

        # Move the document to shared folder
        if shared_folder_id:
            file = (
                drive_service.files()
                .update(
                    fileId=doc_id,
                    addParents=shared_folder_id,
                    fields="id, parents",
                )
                .execute()
            )
            print(f"File moved to folder: {file} | Parent folder {file['parents'][0]}")

        # Verify the new document exists in Google Drive
        try:
            file_verify = (
                drive_service.files()
                .get(fileId=doc_id, fields="id, name, parents, webViewLink")
                .execute()
            )
            print(f"File store confirmed: {file_verify}")
        except:
            raise Exception("Error creating document in Google Drive")

        parent = (
            drive_service.files().get(fileId=shared_folder_id, fields="id, name").execute()
        )
        print(f"Parent folder name: {parent.get('name')} (ID: {parent.get('id')})")

        if not text:
            text = 'No text received from Snowflake stage.'

        requests = [{"insertText": {"location": {"index": 1}, "text": text}}]

        result = (
            docs_service.documents()
            .batchUpdate(documentId=doc_id, body={"requests": requests})
            .execute()
        )

        print("Document content updated: ", result)

        return file_verify.get("webViewLink")

    except HttpError as err:
        print(err)
        return None

def create_google_sheet(self, shared_folder_id, title, data):
    """
    Creates a Google Sheet with the given title and table data and moves it
    from the service account to the shared folder.
    Loads pre-authorized user credentials from the environment.
    """
    if not self.user:
        raise Exception("User not specified for google drive conventions.")

    SERVICE_ACCOUNT_FILE = f"g-workspace-{self.user}.json"
    try:
        # Authenticate using the service account JSON file
        creds = Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES
        )
    except Exception as e:
        print(f"Error loading credentials: {e}")
        return None

    try:
        service = build("sheets", "v4", credentials=creds)
        drive_service = build("drive", "v3", credentials=creds)

        spreadsheet = {"properties": {"title": title}}
        spreadsheet = (
            service.spreadsheets()
            .create(body=spreadsheet, fields="spreadsheetId")
            .execute()
        )

        ss_id = spreadsheet.get("spreadsheetId")
        print(f"Spreadsheet ID: {ss_id}")
        keys = list(data[0].keys())
        columns = [keys]

        # Check for stage links
        stage_column_index = [i for i, key in enumerate(keys) if key.endswith("_STAGE_LINK")]
        stage_column_folder_names = [key.replace("_STAGE_LINK", "") for key in keys if key.endswith("_STAGE_LINK")]
        stage_column_folder_ids = []

        # Create folder top level folder
        top_level_folder_id = create_folder_in_folder(
            "genesis_" + datetime.now().strftime("%m%d%Y_%H:%M:%S"),
            shared_folder_id,
            self.user
        )

        if len(stage_column_folder_names) > 0:
            # Create sub-folders
            for stage_column_folder in stage_column_folder_names:
                stage_column_folder_ids.append(
                    create_folder_in_folder(
                        stage_column_folder,
                        top_level_folder_id,
                        self.user
                    )
                )

        for i, row in enumerate(data):
            row_values = list(row.values())
            for j, row_value in enumerate(row_values):
                if isinstance(row_value, datetime):
                    row_values[j] = row_value.strftime("%Y-%m-%d %H:%M:%S")
                elif len(stage_column_index) > 0 and j in stage_column_index and row_value:
                    # Create a file with contents from the stage link, move it to the shared folder, and get the webViewLink
                    parts = row_value.split(".")
                    path = parts[2].split("/")
                    stage = path[0]

                    file_contents = self.read_file_from_stage(
                        # self,
                        parts[0].replace('@',''),
                        parts[1],
                        stage,
                        "/".join(path[1:]) + '.' + parts[-1],
                        True,
                    )

                    # create text docs in sub-folder
                    filename = path[-1] + '.' + parts[-1]

                    print(f"Stage Col Index: {stage_column_index.index(j)}")
                    print(f"Stage folder ID: {stage_column_folder_ids[stage_column_index.index(j)]}")

                    stage_folder_id = stage_column_folder_ids[stage_column_index.index(j)]

                    webLink = save_text_to_google_folder(
                        self, stage_folder_id, filename, file_contents, creds
                    )

                    row_values[j] = webLink

            columns.append(row_values)

        spreadsheet = {"properties": {"title": title}}
        spreadsheet = (
            service.spreadsheets()
            .create(body=spreadsheet, fields="spreadsheetId")
            .execute()
        )

        ss_id = spreadsheet.get("spreadsheetId")

        width_10 = chr(65 + len(columns[0]) % 26)
        width_1 = chr(64 + len(columns[0]) // 26) if len(columns[0]) > 25 else ''
        width = width_10 + width_1
        range_name = f"Sheet1!A1:{width}{len(columns)}"
        print(f"\n\nRange name: {range_name} | {len(columns[0])} | {len(columns)}\n\n")
        body = {
                "values": columns
               }

        result = (
            service.spreadsheets()
            .values()
            .update(
                spreadsheetId=ss_id,
                range=range_name,
                valueInputOption='RAW',
                body=body,
            )
            .execute()
        )
        print(f"{result.get('updatedCells')} cells updated.")


        # Move the document to shared folder
        if top_level_folder_id:
            file = (
                drive_service.files()
                .update(
                    fileId=ss_id,
                    addParents=top_level_folder_id,
                    fields="id, webViewLink, parents",
                )
                .execute()
            )
            print(f"File moved to folder: {file} | Parent folder {file['parents'][0]}")

        return {"Success": True, "file_id": spreadsheet.get("spreadsheetId"), "webViewLink": file.get("webViewLink")}

    except HttpError as error:
        print(f"An error occurred: {error}")
        return error
