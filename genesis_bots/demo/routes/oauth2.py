from flask import Blueprint, request, session, redirect, url_for
import os
from google_auth_oauthlib.flow import Flow
import google.oauth2.credentials
import json
from genesis_bots.core.logging_config import logger

# Remove this line as it overwrites Flask's session
# session = {}

SCOPES = [
    "https://www.googleapis.com/auth/drive.file",
    "https://www.googleapis.com/auth/documents",
    "https://www.googleapis.com/auth/drive",
]

oauth_routes = Blueprint('oauth_routes', __name__)

@oauth_routes.get("/google_drive_login")
def google_drive_login():
    os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'  # Only for development!

    # Use consistent redirect URI
    redirect_uri = url_for('oauth_routes.oauth2callback', _external=True)
    logger.info(f"Redirect URI for Google Drive Login: {redirect_uri}")

    flow = Flow.from_client_secrets_file(
        "google_oauth_credentials.json",
        scopes=SCOPES,
        redirect_uri=redirect_uri
    )

    authorization_url, state = flow.authorization_url(
        access_type='offline',
        include_granted_scopes='true',
        prompt='consent'
    )

    logger.info(f"session['oauth_state'] = State: {state}")
    session['oauth_state'] = state

    return redirect(authorization_url)

@oauth_routes.get("/oauth2")
def oauth2callback():
    os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'

    state = session.get('oauth_state', None)
    logger.info(f"State from session: {state}")
    if not state:
        return 'State not found in session', 400

    flow = Flow.from_client_secrets_file(
        "google_oauth_credentials.json",
        scopes=SCOPES,
        state=state
    )

    # Use same redirect URI as in login
    flow.redirect_uri = url_for('oauth_routes.oauth2callback', _external=True)
    logger.info(f'Flow redirect URI: {flow.redirect_uri}')

    try:
        authorization_response = request.url
        flow.fetch_token(authorization_response=authorization_response)
        credentials = flow.credentials

        credentials_dict = {
            "web": {
                'token': credentials.token,
                'refresh_token': credentials.refresh_token,
                'token_uri': credentials.token_uri,
                'client_id': credentials.client_id,
                'client_secret': credentials.client_secret,
                'scopes': credentials.scopes
            }
        }

        logger.info(f"Credentials from OAUTH: {credentials_dict}")
        session['credentials'] = credentials_dict

        with open('g-workspace-credentials.json', 'w') as json_file:
            json.dump(credentials_dict, json_file, indent=4)

        return "Authorization successful! You may close this page now"

    except Exception as e:
        logger.error(f"Authorization failed: {str(e)}")
        return f"Authorization failed: {str(e)}", 400