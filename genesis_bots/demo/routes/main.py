import os
from flask import Blueprint
from genesis_bots.core.logging_config import logger
from flask import request, make_response

main_routes = Blueprint('main_routes', __name__)


@main_routes.get("/healthcheck")
def readiness_probe():
    # logger.info("Flask: /healthcheck probe received")
    return "I'm ready!"

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

    response = make_response({"data": output_rows})
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