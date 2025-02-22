
from flask import Blueprint, request, session, redirect, url_for
import os
from google_auth_oauthlib.flow import Flow
import google.oauth2.credentials

session_state = None

SCOPES = [
    "https://www.googleapis.com/auth/drive.file",
    "https://www.googleapis.com/auth/documents",
    "https://www.googleapis.com/auth/drive",
]

oauth_routes = Blueprint('oauth_routes', __name__)

@oauth_routes.get("/endpoint_check")
def endpoint_check():
    return "Endpoint check successful!"

@oauth_routes.get("/google_drive_login")
def google_drive_login():
    os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'  # Only for development!

    user = os.getenv("USER")

    # Make sure this matches EXACTLY what's in Google Cloud Console
    redirect_uri = "https://blf4aam4-dshrnxx-genesis-dev-consumer.snowflakecomputing.app/oauth/oauth2"  # Changed from 127.0.0.1

    flow = Flow.from_client_secrets_file(
        "google_oauth_credentials.json".format(user),
        scopes=SCOPES,
        redirect_uri=redirect_uri,
    )

    authorization_url, state = flow.authorization_url(
        access_type='offline',
        include_granted_scopes='true',
        prompt='consent'
    )

    # Store the state so we can verify it in the callback
    session_state = state

    return redirect(authorization_url)

@oauth_routes.get("/oauth2")
def oauth2callback():
  # Specify the state when creating the flow in the callback so that it can
    # verified in the authorization server response.
    import json
    flow = Flow.from_client_secrets_file(
        "google_oauth_credentials.json", scopes=SCOPES, state=session_state)
    flow.redirect_uri = url_for('main_routes.oauth2callback', _external=True)

    # Use the authorization server's response to fetch the OAuth 2.0 tokens.
    authorization_response = request.url
    flow.fetch_token(authorization_response=authorization_response)

    credentials = flow.credentials

    credentials_dict = {"web":{
        'token': credentials.token,
        'refresh_token': credentials.refresh_token,
        'token_uri': credentials.token_uri,
        'client_id': credentials.client_id,
        'client_secret': credentials.client_secret,
        'scopes': credentials.scopes
    }}

    print(f"Credentials from OAUTH: {credentials_dict}")

    # session['credentials'] = credentials_dict

    # Check which scopes user granted
    # granted_scopes = credentials.scopes
    # session['features'] = granted_scopes

    creds_json = json.dumps(credentials_dict, indent=4)
    with open(f'g-workspace-credentials.json', 'w') as json_file:
        json_file.write(creds_json)
    return True
    return "Authorization successful! You may close this page now"