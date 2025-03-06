import streamlit as st
import pandas as pd

from utils import (
    get_references, get_session, get_metadata, set_metadata, upgrade_services
)
from snowflake.connector import SnowflakeConnection
from .components import config_page_header

def projects_dashboard():
    config_page_header("Projects Dashboard")
