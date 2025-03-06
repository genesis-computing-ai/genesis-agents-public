from flask import Blueprint, request, session, redirect, url_for, render_template
import os
import json
from genesis_bots.core.logging_config import logger

projects_routes = Blueprint('projects_routes', __name__)

@projects_routes.get("/dashboard")
def dashboard():
    # return "HELLO"
    return render_template("index.html")

@projects_routes.get("/delete_callback")
def delete_callback():
    pass