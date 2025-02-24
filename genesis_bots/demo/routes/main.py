import os
from genesis_bots.core.logging_config import logger
from flask import Blueprint, request, make_response, redirect, session, url_for
import requests
from google_auth_oauthlib.flow import Flow
import google.oauth2.credentials

SCOPES = [
    "https://www.googleapis.com/auth/drive.file",
    "https://www.googleapis.com/auth/documents",
    "https://www.googleapis.com/auth/drive",
]


main_routes = Blueprint('main_routes', __name__)
@main_routes.post("/api/messages")
def api_message():
    logger.info(f"Flask: /api/messages: {request.json}")

    msg_from = request.json["from"]["id"]
    conv_id = request.json["conversation"]["id"]
    msg_to = request.json["recipient"]["id"]
    text = request.json["text"]

    r = {
        "type": "message",
        "from": {
            "id": msg_to,
            "name": "Teams TestBot"
        },
        "conversation": {
            "id": conv_id,
            "name": "Convo1"
        },
        "recipient": {
                "id": msg_from,
                "name": "Megan Bowen"
            },
        "text": "My bot's reply",
        "replyToId": "1632474074231"
    }

    response = make_response(r)
    response.headers["Content-type"] = "application/json"
    return response



@main_routes.get("/healthcheck")
def readiness_probe():
    # logger.info("Flask: /healthcheck probe received")
    response = make_response({"data": "I'm ready! (from get /healthcheck:8080)"})
    response.headers['Content-type'] = 'application/json'
    return response


@main_routes.post("/echo")
def echo():
    """
    Main handler for input data sent by Snowflake.
    """
    message = request.json
    logger.debug(f"Received request: {message}")

    if message is None or not message["data"]:
        logger.info("Received empty message")
        return {}

    # input format:
    #   {"data": [
    #     [row_index, column_1_value, column_2_value, ...],
    #     ...
    #   ]}
    input_rows = message["data"]
    logger.info(f"Received {len(input_rows)} rows")

    # output format:
    #   {"data": [
    #     [row_index, column_1_value, column_2_value, ...}],
    #     ...
    #   ]}
    # output_rows = [[row[0], submit(row[1],row[2])] for row in input_rows]
    output_rows = [[row[0], "Hi there!"] for row in input_rows]
    logger.info(f"Produced {len(output_rows)} rows")

    response = make_response({"data": input_rows})
    response.headers["Content-type"] = "application/json"
    logger.debug(f"Sending response: {response.json}")
    return response

@main_routes.route("/zapier", methods=["POST"])
def zaiper_handler():
    try:
        api_key = request.args.get("api_key")
    except:
        return "Missing API Key"

    #  logger.info("Zapier: ", api_key)
    return {"Success": True, "Message": "Success"}

# @main_routes.get("/google_drive_login")
# def google_drive_login():
#     os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'  # Only for development!

#     user = os.getenv("USER")

#     # Make sure this matches EXACTLY what's in Google Cloud Console
#     redirect_uri = "http://localhost:8080/oauth2"  # Changed from 127.0.0.1

#     flow = Flow.from_client_secrets_file(
#         "google_oauth_credentials.json".format(user),
#         scopes=SCOPES,
#         redirect_uri=redirect_uri,
#     )

#     authorization_url, state = flow.authorization_url(
#         access_type='offline',
#         include_granted_scopes='true',
#         prompt='consent'
#     )

#     # Store the state so we can verify it in the callback
#     session['state'] = state

#     return redirect(authorization_url)

# @main_routes.get("/oauth2")
# def oauth2callback():
#   # Specify the state when creating the flow in the callback so that it can
#     # verified in the authorization server response.
#     state = session['state']

#     flow = Flow.from_client_secrets_file(
#         "google_oauth_credentials.json", scopes=SCOPES, state=state)
#     flow.redirect_uri = url_for('main_routes.oauth2callback', _external=True)

#     # Use the authorization server's response to fetch the OAuth 2.0 tokens.
#     authorization_response = request.url
#     flow.fetch_token(authorization_response=authorization_response)

#     # Store credentials in the session.
#     # ACTION ITEM: In a production app, you likely want to save these
#     #              credentials in a persistent database instead.
#     credentials = flow.credentials

#     credentials_dict = {
#         'token': credentials.token,
#         'refresh_token': credentials.refresh_token,
#         'token_uri': credentials.token_uri,
#         'client_id': credentials.client_id,
#         'client_secret': credentials.client_secret,
#         'scopes': credentials.scopes
#     }
#     session['credentials'] = credentials_dict

#     # Check which scopes user granted
#     granted_scopes = credentials.scopes
#     session['features'] = granted_scopes
#     return "Authorization successful! You may close this page now"
