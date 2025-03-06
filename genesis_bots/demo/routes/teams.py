import os
from flask import Blueprint
from genesis_bots.core.logging_config import logger
from flask import request, make_response
import requests

teams_routes = Blueprint('teams_routes', __name__)

@teams_routes.get("/healthcheck")
def healthcheck_3978():
    logger.info("Flask: /healthcheck:3978 probe received")
    response = make_response({"data": "I'm ready on 3978!"})
    response.headers['Content-type'] = 'application/json'
    return response