import os.path

from google.oauth2.service_account import Credentials
# from google.auth.transport.requests import Request
# from google.oauth2.credentials import Credentials
# from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from datetime import datetime


# If modifying these scopes, delete the file token.json.
SCOPES = [
    "https://www.googleapis.com/auth/drive.file",
    "https://www.googleapis.com/auth/documents",
    "https://www.googleapis.com/auth/drive"
]

SERVICE_ACCOUNT_FILE = "./google_sheets/genesis-workspace-project-d094fd7d2562.json"


def output_to_google_docs(text:str =None):
    """
    Creates new file in Google Docs named Genesis_mmddyyy_hh:mm:ss from text string
    """
    creds = None

    # if os.path.exists("token.json"):
    #     # creds = Credentials.from_authorized_user_file("token.json", SCOPES)


    # # If there are no (valid) credentials available, let the user log in.
    # if not creds or not creds.valid:
    #     if creds and creds.expired and creds.refresh_token:
    #         creds.refresh(Request())
    #     else:
    #         flow = InstalledAppFlow.from_client_secrets_file("./google_sheets/credentials.json", SCOPES)
    #         creds = flow.run_local_server(port=0)
    #     # Save the credentials for the next run
    #     with open("token.json", "w") as token:
    #         token.write(creds.to_json())

    try:
        creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        service = build("docs", "v1", credentials=creds)

        title = "Genesis_" + datetime.now().strftime("%m%d%Y_%H:%M:%S")
        body = {'title': title}
        doc = service.documents().create(body=body).execute()
        print('Created document with title: {0}'.format(doc.get('title')))

        requests = [
                {
                'insertText': {
                    'location': {
                        'index': 1
                    },
                    'text': text
                }
            }
        ]

        result = service.documents().batchUpdate(
            documentId=doc["documentId"], body={'requests': requests}).execute()

        print('Document content updated: ', result)

        return title


    except HttpError as err:
        print(err)
        return None
