from flask import Blueprint, request, session, redirect, url_for, render_template
import os
import json
from genesis_bots.core.logging_config import logger
import requests
# from utils import get_bot_details, get_metadata, set_metadata

projects_routes = Blueprint('projects_routes', __name__)

LOCAL_SERVER_URL = "http://localhost:8080/"

def get_metadata(metadata_type):
    url = LOCAL_SERVER_URL + "udf_proxy/get_metadata"
    headers = {"Content-Type": "application/json"}
    data = json.dumps({"data": [[0, metadata_type]]})
    response = requests.post(url, headers=headers, data=data)
    if response.status_code == 200:
        return response.json()["data"][0][1]
    else:
        raise Exception(f"Failed to get metadata: {response.text}")

@projects_routes.get("/dashboard")
def dashboard():
    temp_bot_id = 'Eve'
    results = get_metadata(f"list_projects {temp_bot_id}")
    return render_template("index.html", projects=results['projects'])

@projects_routes.get("/delete_callback")
def delete_callback():
    # When user clicks icon to delete a TODO item, this function is called
    pass