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
    "https://www.googleapis.com/auth/spreadsheets"
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
