import os
from flask import Blueprint
from genesis_bots.core.logging_config import logger
from flask import request, make_response
import requests

main_routes = Blueprint('main_routes', __name__)

@main_routes.post("/api/messages")
def api_message():
    logger.info(f"Flask: /api/messages: {request.json()}")
    r = {
        "type": "message",
        "from": {
            "id": "28:c9e8c047-2a34-40a1-b28a-b162d5f5327c",
            "name": "Teams TestBot"
        },
        "conversation": {
            "id": "a:17I0kl8EkpE1O9PH5TWrzrLNwnWWcfrU7QZjKR0WSfOpzbfcAg2IaydGElSo10tVr4C7Fc6GtieTJX663WuJCc1uA83n4CSrHSgGBj5XNYLcVlJAs2ZX8DbYBPck201w-",
            "name": "Convo1"
        },
        "recipient": {
                "id": "29:1XJKJMvc5GBtc2JwZq0oj8tHZmzrQgFmB25ATiQWA85gQtHieVkKilBZ9XHoq9j7Zaqt7CZ-NJWi7me2kHTL3Bw",
                "name": "Megan Bowen"
            },
        "text": "My bot's reply",
        "replyToId": "1632474074231"
    }

    return r.json()



@main_routes.get("/healthcheck")
def readiness_probe():
    # logger.info("Flask: /healthcheck probe received")
    response = make_response({"data": "I'm ready! (from get /healthcheck:8080)"})
    response.headers['Content-type'] = 'application/json'
    return response

@main_routes.post("/healthcheck")
def readiness_probe_post():
    response = make_response({"data": "I'm ready! (from post /healthcheck:8080)"})
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