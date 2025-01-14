
import gzip
import os
import shutil
import tempfile



from   dagster                  import AssetKey, DagsterRunStatus
from   dagster_graphql          import (DagsterGraphQLClient,
                                        DagsterGraphQLClientError)
from   pathlib                  import Path
import requests

from   genesis_bots.core.bot_os_tools2       import ToolFuncGroup, gc_tool

# relative location to the files with the graphql queries used by the tools
GRAPHQL_QUERIES_DIR = Path(__file__).parent / "graphql"


class DagsterConfig:
    '''
    Singleton class that holds the configuration attributes needed to connect to the Dagster Cloud instance
    '''
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DagsterConfig, cls).__new__(cls)
            cls._instance.__initialized = False
        return cls._instance


    def __init__(self):
        if self.__initialized:
            return
        # Mandatory
        self.DAGSTER_HOSTNAME = os.getenv("DAGSTER_HOSTNAME")
        self.DAGSTER_USER_TOKEN = os.getenv("DAGSTER_USER_TOKEN")

        assert bool(self.DAGSTER_HOSTNAME) & bool(self.DAGSTER_USER_TOKEN), "DAGSTER_HOSTNAME and DAGSTER_USER_TOKEN env vars must be set"

        # Optional
        self.DAGSTER_DEPLOYMENT_NAME = os.getenv("DAGSTER_DEPLOYMENT_NAME", "prod")
        # Derived
        self.DAGSTER_CLOUD_ACCOUNT_URL = f"https://{self.DAGSTER_HOSTNAME}"
        self.DAGSTER_CLOUD_DEPLOYMENT_URL = f"{self.DAGSTER_CLOUD_ACCOUNT_URL}/prod"
        self.__initialized = True


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


dagster_tools_tag = ToolFuncGroup(name="dagster_tools",
                                  description="Tools to interact with Dagster Cloud",
                                  lifetime="PERSISTENT")


# def list_gctool_decorated_functions():
#     """
#     Lists all the functions defined in this module that are decorated with @gctool.

#     Returns:
#         list: A list of function names that are decorated with @gctool.
#     """
#     gctool_decorated_functions = []
#     for name, obj in globals().items():
#         if callable(obj) and hasattr(obj, gc_tool.gc_tool_descriptor_attr_name):
#             gctool_decorated_functions.append(obj)

#     return gctool_decorated_functions


def run_dagster_graphql(graphql_query, variables=None):
    client = DagsterGraphQLClient(DagsterConfig().DAGSTER_HOSTNAME, use_https=True, headers={"Dagster-Cloud-Api-Token": DagsterConfig().DAGSTER_USER_TOKEN})
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


@gc_tool(
    run_id="The run_id to fetch status for",
    _group_tags_= [dagster_tools_tag]
)
def get_dagster_run_status(run_id: str):
    '''
    Get the status of the Dagster run, given its run_id.
    '''
    client = DagsterGraphQLClient(DagsterConfig().DAGSTER_HOSTNAME,
                                  use_https=True,
                                  headers={"Dagster-Cloud-Api-Token": DagsterConfig().DAGSTER_USER_TOKEN})
    status: DagsterRunStatus = client.get_run_status(run_id)
    return status


@gc_tool(
    run_id="The run_id to fetch status for",
    _group_tags_= [dagster_tools_tag]
)
def get_dagster_run_debug_dump(run_id: str):
    '''
    Fetch full (long, detailed) debug information from the Dagster Cloud server about a specific run, given its run_id.
    This is equivanet of downloading a full debug file information from dasgter cloud.

    Returns a JSON string representing the result of a dagster GraphQL query.
    '''

    run_debug_file_url = f"{DagsterConfig().DAGSTER_CLOUD_DEPLOYMENT_URL}/download_debug/{run_id}"
    return _get_content_of_web_file(run_debug_file_url, DagsterConfig().DAGSTER_USER_TOKEN)


@gc_tool(
    asset_key='the asset key, using "/" as path separeator (e.g. foo/bar for asset key ["foo", "bar"])',
    _group_tags_= [dagster_tools_tag]
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

@gc_tool(_group_tags_= [dagster_tools_tag])
def get_dagster_asset_lineage_graph():
    '''
    Fetch asset lineage for the entire dagster repository.

    Returns a JSON string representing the result of a dagster GraphQL query.
    '''
    return run_dagster_graphql_file(GRAPHQL_QUERIES_DIR / "dagster_asset_lineage_graph.graphql",
                                    {})

# holds the list of all dagster tool functions
# NOTE: Update this list when adding new dagster tools (TODO: automate this by scanning the module?)
_all_dagster_tool_functions = (get_dagster_run_status,
                               get_dagster_run_debug_dump,
                               get_dagster_asset_definition_and_overview,
                               get_dagster_asset_lineage_graph)


# Called from bot_os_tools.py to update the global list of dagster tool functions
def get_dagster_tool_functions():
    return _all_dagster_tool_functions


