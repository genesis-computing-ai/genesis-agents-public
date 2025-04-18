from flask import Blueprint, request, session, redirect, url_for, render_template, send_from_directory
import os
import json
from genesis_bots.core.logging_config import logger
import requests
import time


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

def _get_bot_list():
    url = LOCAL_SERVER_URL + "udf_proxy/list_available_bots"
    headers = {"Content-Type": "application/json"}
    data = json.dumps({"data": [[0]]})
    response = requests.post(url, headers=headers, data=data)
    if response.status_code == 200:
        return response.json()["data"][0][1]
    else:
        raise Exception(f"Failed to get bot list: {response.text}")

@projects_routes.get("/dashboard")
def dashboard():
    bots = _get_bot_list()
    return render_template("index.html", bots=bots)

@projects_routes.get("/get_projects")
def get_projects():
    # list_projects {bot_id}
    pass

@projects_routes.get("/get_todos")
def get_todos():
    # list_todos {project_id}
    pass

@projects_routes.get("/delete_todo_callback")
def delete_callback():
    pass

@projects_routes.get('/react')
def react():
    return send_from_directory('../../../genesis-dashboard/build', 'index.html')