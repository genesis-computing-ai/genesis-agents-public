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
    "https://www.googleapis.com/auth/drive"
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


def create_g_drive_folder(folder_name:str = "folder"):
    try:
        creds = Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES
        )
        service = build("drive", "v3", credentials=creds)

        file_metadata = {
            "name": folder_name,
            "mimeType": "application/vnd.google-apps.folder",
        }

        file = service.files().create(body=file_metadata, fields="id").execute()
        print(f'Folder ID: "{file.get("id")}".')
        return file.get("id")

    except HttpError as err:
        print(err)
        return None

def output_to_google_docs(text: str = 'No text received.', shared_folder_id: str = None, user =None, file_name = None):
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


# def output_to_google_docs(text:str =None):
#     """
#     Creates new file in Google Docs named Genesis_mmddyyy_hh:mm:ss from text string
#     """
#     creds = None

#     # if os.path.exists("token.json"):
#     #     # creds = Credentials.from_authorized_user_file("token.json", SCOPES)

#     # # If there are no (valid) credentials available, let the user log in.
#     # if not creds or not creds.valid:
#     #     if creds and creds.expired and creds.refresh_token:
#     #         creds.refresh(Request())
#     #     else:
#     #         flow = InstalledAppFlow.from_client_secrets_file("./google_sheets/credentials.json", SCOPES)
#     #         creds = flow.run_local_server(port=0)
#     #     # Save the credentials for the next run
#     #     with open("token.json", "w") as token:
#     #         token.write(creds.to_json())

#     try:
#         creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
#         service = build("docs", "v1", credentials=creds)

#         title = "Genesis_" + datetime.now().strftime("%m%d%Y_%H:%M:%S")
#         body = {'title': title}
#         doc = service.documents().create(body=body).execute()
#         print('Created document with title: {0}'.format(doc.get('title')))

#         requests = [
#                 {
#                 'insertText': {
#                     'location': {
#                         'index': 1
#                     },
#                     'text': text
#                 }
#             }
#         ]

#         result = service.documents().batchUpdate(
#             documentId=doc["documentId"], body={'requests': requests}).execute()

#         print('Document content updated: ', result)

#         return title

#     except HttpError as err:
#         print(err)
#         return None
