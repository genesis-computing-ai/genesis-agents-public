
from flask import Blueprint, request, session, redirect
import os
from google_auth_oauthlib.flow import Flow
import google.oauth2.credentials

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
    redirect_uri = "http://localhost:8080/oauth2"  # Changed from 127.0.0.1

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
    session['state'] = state

    return redirect(authorization_url)

@oauth_routes.get("/oauth2")
def oauth2callback():
  # Specify the state when creating the flow in the callback so that it can
    # verified in the authorization server response.
    state = session['state']

    flow = Flow.from_client_secrets_file(
        "google_oauth_credentials.json", scopes=SCOPES, state=state)
    flow.redirect_uri = url_for('main_routes.oauth2callback', _external=True)

    # Use the authorization server's response to fetch the OAuth 2.0 tokens.
    authorization_response = request.url
    flow.fetch_token(authorization_response=authorization_response)

    # Store credentials in the session.
    # ACTION ITEM: In a production app, you likely want to save these
    #              credentials in a persistent database instead.
    credentials = flow.credentials

    credentials_dict = {
        'token': credentials.token,
        'refresh_token': credentials.refresh_token,
        'token_uri': credentials.token_uri,
        'client_id': credentials.client_id,
        'client_secret': credentials.client_secret,
        'scopes': credentials.scopes
    }
    session['credentials'] = credentials_dict

    # Check which scopes user granted
    granted_scopes = credentials.scopes
    session['features'] = granted_scopes
    return "Authorization successful! You may close this page now"