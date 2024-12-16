
import json
from   pathlib                  import Path
from typing import get_origin, get_args, List, Dict
from   smart_open               import open as smart_open
import tempfile
from   textwrap                 import dedent

from   dagster                  import DagsterRunStatus
from   dagster_graphql          import DagsterGraphQLClient
import gzip
import os
import requests
import shutil
import tempfile
import inspect

DAGSTER_DEPLOYMENT_URL = "https://genesis-computing.dagster.cloud/prod"  # Your deployment-scoped url

from   dagster                  import AssetKey, DagsterRunStatus
from   dagster_graphql          import (DagsterGraphQLClient,
                                        DagsterGraphQLClientError)
import requests

DAGSTER_HOSTNAME = "genesis-computing.dagster.cloud"
DAGSTER_CLOUD_ACCOUNT_URL = "https://" + DAGSTER_HOSTNAME
DAGSTER_DEPLOYMENT_NAME = "prod"
DAGSTER_CLOUD_DEPLOYMENT_URL = DAGSTER_CLOUD_ACCOUNT_URL + "/prod"

user_token = (  # a User Token generated from the Organization Settings page in Dagster+.
    #"user:fe14648d4712402d8b5cf924bb557305"
    "user:04ffd0ca65014180b5c55ddd917da333" # created on Nov 18th from https://genesis-computing.dagster.cloud/prod/org-settings/tokens
)

GRAPHQL_QUERIES_DIR = Path(__file__).parent / "graphql"

DEMO_PATH=Path("/home/dekela/repos/dagster_gc_proj/demo_20241120_input")

#TODO: move this to some utils?
def _download_web_file(url, local_filename, token):
    """
    Downloads a file from a specified URL and saves it locally.

    Args:
        url (str): The URL of the file to download.
        local_filename (str): The path where the downloaded file will be saved.
        token (str): The authorization token for accessing the URL.

    Raises:
        HTTPError: If the HTTP request returned an unsuccessful status code.
    """
    headers = {
        "Authorization": f"Bearer {token}"
    }
    # Send a GET request to the URL with headers
    with requests.get(url, headers=headers, stream=True) as r:
        r.raise_for_status()  # Raise an error for bad responses
        # Open a local file with write-binary mode
        with open(local_filename, 'wb') as f:
            # Write the content to the local file in chunks
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

#TODO: move this to some utils?
def _get_content_of_web_file(url, token):
    """
    Downloads a file from a given URL, checks if it is gzipped, and returns its content.

    If the file is in gzip format, it is unzipped before reading. The content of the file is then returned as a string.

    Args:
        url (str): The URL of the file to download.
        token (str): The authorization token required to access the URL.

    Returns:
        str: The content of the file, or an error message if processing fails.

    Raises:
        OSError: If there is an issue with file operations.
        ValueError: If the file is not in a valid format.
    """
    # Create a temporary directory
    with tempfile.TemporaryDirectory() as temp_dir:
        local_filename = f"{temp_dir}/debug_file.gz"

        # download the file
        _download_web_file(url, local_filename, token)

        # Check if the file is a gzipped file and unzip it if needed
        with open(local_filename, 'rb') as f:
            try:
                if gzip.GzipFile(fileobj=f).readable():  # Check for gzip format using gzip library
                    with gzip.open(local_filename, 'rb') as gz_file:
                        unzipped_filename = f"{temp_dir}/debug_file"
                        with open(unzipped_filename, 'wb') as out_file:
                            shutil.copyfileobj(gz_file, out_file)
                        file_to_read = unzipped_filename
                else:
                    file_to_read = local_filename

                # Return the content of the file
                with open(file_to_read, 'r') as content_file:
                    content = content_file.read()
                    return content
            except (OSError, ValueError) as e:
                return f"Error processing file: {e}"

# TODO: refactor the gc_tool_descriptor value we attach to each func be a ToolDescriptor class that can
#       validate the input, do a to_dict()
def gctool(**param_descriptions):
    '''
    """
    A decorator for a 'tool' function that attaches a `gc_tool_descriptor` property to the wrapped function that can 
    be used as the Geensis bot tool description:

    example: 
        @gctool(param1='this is param1',
                param2="note that param2 is optional")
        def foo(param1: int, param2: str = "genesis"):
            'This is the description of gfoo'
            pass

        pprint.pprint(foo.gc_tool_descriptor)
       @gctool(pa)



    Output:
        (param1: int, param2: str = 'genesis')
        {'param1': <Parameter "param1: int">, 'param2': <Parameter "param2: str = 'genesis'">}
        {'function': {'description': 'This is the description of gfoo',
                    'name': 'foo',
                    'parameters': {'properties': {'param1': {'description': 'this is param1',
                                                            'type': 'int'},
                                                    'param2': {'description': 'note that param2 is optional',
                                                            'type': 'string'}},
                                    'type': 'object',
                                    'required': ['param1']},
        'type': 'function'}
    """
    '''
    def decorator(func):
        sig = inspect.signature(func)
        if not func.__doc__:
            raise ValueError("Function must have a docstring")
        if func.__annotations__ is None and len(inspect.signature(func).parameters) > 0:
            raise ValueError("Function must have type annotations")

        def _python_type_to_llm_type(python_type):
            origin = get_origin(python_type)
            args = get_args(python_type)
            if origin in (list, List):
                return {'type': 'array', 'items': python_type_to_llm_type(args[0])} if args else {'type': 'array'}
            elif origin in(dict, Dict):
                return {
                    'type': 'object',
                    'properties': {arg: python_type_to_llm_type(args[i]) for i, arg in enumerate(args)}
                }
            elif python_type is int:
                return {'type': 'int'}
            elif python_type is str:
                return {'type': 'string'}
            elif python_type is float:
                return {'type': 'float'}
            elif python_type is bool:
                return {'type': 'boolean'}

            else:
                raise ValueError(f"Could not convert annotation type {python_type} to llm type")

        def _cleanup_docsting(s):
            s = dedent(s)
            s = "\n".join([line for line in s.split("\n") if line])
            return s

        params_desc_dict = {pname: dict(description=param_descriptions[pname]) | _python_type_to_llm_type(pattrs.annotation)
             for pname, pattrs in sig.parameters.items()}

        required_params = [pname for pname, pattrs in sig.parameters.items()
                           if pattrs.default is  pattrs.empty]

        # Construct the gc_tool_descriptor attribute
        func.gc_tool_descriptor = {
            "type": "function",
            "function": {
                "name": func.__name__,
                "description": _cleanup_docsting(func.__doc__),
                "parameters": {
                    "type": "object",
                    "properties": params_desc_dict,
                    "required": required_params,
                },

            }
        }
        return func

    return decorator

gctool.gc_tool_descriptor_attr_name = "gc_tool_descriptor"


def list_gctool_decorated_functions():
    """
    Lists all the functions defined in this module that are decorated with @gctool.

    Returns:
        list: A list of function names that are decorated with @gctool.
    """
    gctool_decorated_functions = []
    for name, obj in globals().items():
        if callable(obj) and hasattr(obj, gctool.gc_tool_descriptor_attr_name):
            gctool_decorated_functions.append(obj)

    return gctool_decorated_functions


def _load_demo_file(fn):
    log_file_path = DEMO_PATH / fn
    try:
        with open(log_file_path, 'r') as log_file:
            log_content = log_file.read()
        return log_content
    except Exception as e:
        return f"Unable to load content"


def run_dagster_graphql(graphql_query, variables=None):
    client = DagsterGraphQLClient(DAGSTER_HOSTNAME, use_https=True, headers={"Dagster-Cloud-Api-Token": user_token})
    try:
        result = client._execute(graphql_query, variables=variables)
        return result
    except DagsterGraphQLClientError as e:
        raise e


def run_dagster_graphql_file(filename, variables=None):
    try:
        with open(filename, "r") as file:
            graphql_query = file.read()
        return run_dagster_graphql(graphql_query, variables)
    except Exception as e:
        return f"Error reading or executing query from file: {e}"


@gctool(
    run_id="The run_id to fetch status for"
)
def get_dagster_run_status(run_id: str):
    '''
    Get the status of the Dagster run, given its run_id.
    '''
    client = DagsterGraphQLClient(DAGSTER_HOSTNAME, use_https=True, headers={"Dagster-Cloud-Api-Token": user_token})
    status: DagsterRunStatus = client.get_run_status(run_id)
    return status


@gctool(
    run_id="The run_id to fetch status for"
)
def get_dagster_run_debug_dump(run_id: str):
    '''
    Fetch full (long, detailed) debug information from the Dagster Cloud server about a specific run, given its run_id.
    This is equivanet of downloading a full debug file information from dasgter cloud.

    Returns a JSON string representing the result of a dagster GraphQL query.
    '''

    run_debug_file_url = f"{DAGSTER_CLOUD_DEPLOYMENT_URL}/download_debug/{run_id}"
    return _get_content_of_web_file(run_debug_file_url, user_token)


@gctool(
    asset_key='the asset key, using "/" as path separeator (e.g. foo/bar for asset key ["foo", "bar"])'
)
def get_dagster_asset_definition_and_overview(asset_key: str):
    '''
    Fetch rich information about the given asset, which includes the following:
     - latest materlization information (time, run_id)
     - Description
     - Raw SQL (for DBT-wrapped assets)
     - Schema of the asset (e.g. columns for table/view assets)
     - Metadata for the assets recognized by Dagster

    Returns a JSON string representing the result of a dagster GraphQL query.       
    '''
    if isinstance(asset_key, str):
        asset_key = AssetKey.from_user_string(asset_key)
    ak = AssetKey.from_coercible(asset_key)
    return run_dagster_graphql_file(GRAPHQL_QUERIES_DIR / "dagster_asset_definition_and_overview.graphql",
                                      dict(assetKey=ak.to_graphql_input()))

@gctool()
def get_dagster_asset_lineage_graph():
    '''
    Fetch asset lineage for the entire dagster repository.

    Returns a JSON string representing the result of a dagster GraphQL query.       
    '''
    return run_dagster_graphql_file(GRAPHQL_QUERIES_DIR / "dagster_asset_lineage_graph.graphql",
                                    {})

# demo only
def _get_dagster_dbt_asset_materialization_debug_log(run_id):
    # returns the dbt.log file from the dbt target/<asset><internal_run_id> - What chatgpt calls the "DBT debug log" file
    return _load_demo_file("hacker_news_dbt_assets-0d92786-5a3bad8.dbt.log")

# demo only
def _get_dagster_dbt_asset_materialization_run_results_log(run_id):
    # returns the run_results.json file for this dbt asset build
    return _load_demo_file("hacker_news_dbt_assets-0d92786-5a3bad8.run_results.json")

# demo only
def _get_dagster_run_logs(run_id):
    # returns the logs for this run - as I got from running GraphQL manually
    return _load_demo_file("hacker_news_dbt_assets-0d92786-5a3bad8.run_results.json")




# def _get_dagster_asset_definition_and_overview(asset_key):
#     if isinstance(asset_key, str):
#         asset_key = AssetKey.from_user_string(asset_key)
#     ak = AssetKey.from_coercible(asset_key)
#     return run_dagster_graphql_file(GRAPHQL_QUERIES_DIR / "dagster_asset_definition_and_overview.graphql",
#                                       dict(assetKey=ak.to_graphql_input()))


# def _get_dagster_project_asset_lineage():
#     # returns the lineage which I generated semi-manually (but there is a GraphQL API for that...)
#     return _load_demo_file("project_assets_lineage.json")


# def get_dagster_assets_info(info_type):
#     info_type = info_type.upper()
#     if info_type == "ASSET_LINEAGE":
#         return _get_dagster_project_asset_lineage()
#     else:
#         return "Invalid info_type. Valid options are: ['ASSET_LINEAGE']"


# def get_dagster_run_info(run_id, info_type):
#     info_type = info_type.upper()
#     if info_type == "DBT_RUN_DEBUG_LOG":
#         return _get_dagster_dbt_asset_materialization_debug_log(None)
#     if info_type == "DBT_RUN_RESULTS_LOG":
#         return _get_dagster_dbt_asset_materialization_run_results_log(None)
#     if info_type == "RUN_LOGS":
#         return _get_dagster_run_logs(None)
#     if info_type == "RUN_DEBUG_DUMP":
#         return _get_dagster_run_debug_dump(None)
#     else:
#         return "invalid info_type. Valid options are: ['DBT_RUN_DEBUG_LOG', 'DBT_RUN_RESULTS_LOG', 'RUN_LOGS']"



# # used in bot_os_toools.py
# dagster_tool_functions = [
#     {
#         "type": "function",
#         "function": {
#             "name": "get_dagster_run_info",
#             "description": "Get various information about the given dagster run, including basic information, logs, and dbt logs (for assets that are DBT models)",
#             "parameters": {
#                 "type": "object",
#                 "properties": {
#                     "run_id": {
#                         "type": "string",
#                         "description": "The full UUID of the Dagster run",
#                     },
#                     "info_type": {
#                         "type": "string",
#                         "description": "What type of information to fetch for this run_id. Valid options are as follows: \n"
#                                        " - RUN_DEBUG_DUMP: fetch comprehensive debug infromation about the given run_id, as dumped by the dagster CLI tool. \n"
#                                        " - DBT_RUN_DEBUG_LOG: fetch the content of the `dbt.log` file for the dbt build that updated this asset in this dagster run, including SQL statements. \n"
#                                        " - DBT_RUN_RESULTS_LOG: fetch the content of the `run_results.json` file for the dbt build that updated this asset in this dagster run. \n"
#                                        " - RUN_LOGS: fetch the dagster run logs for this run, which will list the status and output from all the run steps. "
#                     },
#                 },
#                 "required": ["run_id", "info_type"],
#             },
#         },
#     },
#     {
#         "type": "function",
#         "function": {
#             "name": "get_dagster_assets_info",
#             "description": "Get information about the assets in this dagster project, including asset description and lineage.",
#             "parameters": {
#                 "type": "object",
#                 "properties": {
#                     "info_type": {
#                         "type": "string",
#                         "description": "What type of information to fetch. Valid options are as follows: \n"
#                                        " - ASSET_LINEAGE: the qualified names of all dagster assets, including their dependencies and asset-level lineage \n"
#                     },
#                 },
#                 "required": ["info_type"],
#             },
#         },
#     },
#     {
#         "type": "function",
#         "function": {
#             "name": "run_dagster_gql_query",
#             "description": "Execute a GraphQL query against the Dagster API. This allows querying ANY information on the run itself. Follow the Dagster ",
#             "parameters": {
#                 "type": "object",
#                 "properties": {
#                     "graphql_query": {
#                         "type": "string",
#                         "description": "The GraphQL query to execute",
#                     },
#                     "variables": {
#                         "type": "object",
#                         "description": "Variables to pass to the GraphQL query in the form of a JSON object mapping query variable names to their values",
#                     },
#                 },
#                 "required": ["graphql_query"],
#             },
#         },
#     },
# ]

# used in bot_os_toools.py:
dagster_tool_functions = [getattr(func, gctool.gc_tool_descriptor_attr_name)
                          for func in list_gctool_decorated_functions()]

# used in bot_os_toools.py:
dagster_tools = {func.__name__: f"{__name__}.{func.__name__}" for func in list_gctool_decorated_functions()}


