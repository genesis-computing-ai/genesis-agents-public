
from flask import Blueprint, request
import os
from google_auth_oauthlib.flow import Flow

auth_routes = Blueprint('auth_routes', __name__)

@auth_routes.get("/oauth")
def oauth2_callback():
    
    SCOPES = [
        "https://www.googleapis.com/auth/drive.file",
        "https://www.googleapis.com/auth/documents",
        "https://www.googleapis.com/auth/drive",
    ]
    user = os.getenv("USER")
    flow = Flow.from_client_secrets_file(
        "credentials.json".format(user),
        scopes=SCOPES,
        redirect_uri="http://127.0.0.1:8080/oauth",  # Your redirect URI
    )
    authorization_response = request.url
    flow.fetch_token(authorization_response=authorization_response)
    # Get credentials and save them
    credentials = flow.credentials
    with open(f"token-{user}.json", "w") as token_file:
        token_file.write(credentials.to_json())
    return "Authorization complete. You can close this window."