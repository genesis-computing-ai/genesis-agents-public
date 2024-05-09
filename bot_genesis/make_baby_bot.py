# make_baby_bot.py
import logging
import os, json, requests, uuid
from connectors.bigquery_connector import BigQueryConnector
from connectors.snowflake_connector import SnowflakeConnector
from google.cloud import bigquery

from core.bot_os_corpus import URLListFileCorpus

# Set up a logger for the module
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.WARN, format='%(asctime)s - %(levelname)s - %(message)s')



genesis_source = os.getenv('GENESIS_SOURCE',default="BigQuery")

if genesis_source == 'BigQuery':
    credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS',default=".secrets/gcp.json")
    with open(credentials_path) as f:
        connection_info = json.load(f)
    # Initialize BigQuery client
    bb_db_connector = BigQueryConnector(connection_info,'BigQuery')
else:    # Initialize BigQuery client
    bb_db_connector = SnowflakeConnector(connection_name='Snowflake')


genbot_internal_project_and_schema = os.getenv('GENESIS_INTERNAL_DB_SCHEMA','None')
if  genbot_internal_project_and_schema is None:       
    genbot_internal_project_and_schema = os.getenv('ELSA_INTERNAL_DB_SCHEMA','None')
if genbot_internal_project_and_schema == 'None':
    # Todo remove, internal note 
    print("ENV Variable GENESIS_INTERNAL_DB_SCHEMA is not set.")
db_schema = genbot_internal_project_and_schema.split('.')
project_id = db_schema[0]
dataset_name = db_schema[1]
bot_servicing_table = os.getenv('BOT_SERVICING_TABLE', 'BOT_SERVICING')

def list_all_bots(runner_id=None):
    return bb_db_connector.db_list_all_bots(project_id=project_id, dataset_name=dataset_name, bot_servicing_table=bot_servicing_table, runner_id=runner_id, full=False)
 
def get_all_bots_full_details(runner_id):
    return bb_db_connector.db_list_all_bots(project_id=project_id, dataset_name=dataset_name, bot_servicing_table=bot_servicing_table, runner_id=runner_id, full=True)
 
def set_slack_config_tokens(slack_app_config_token, slack_app_config_refresh_token):
    #test

    try:
        t, r = rotate_slack_token(slack_app_config_token,slack_app_config_refresh_token)
    except:
        return('Error','Refresh token invalid')
    
    save_slack_config_tokens(t,r)
    return t,r


def save_slack_config_tokens(slack_app_config_token, slack_app_config_refresh_token):
    """
    Saves the slack app config token and refresh token for the given runner_id to BigQuery.

    Args:
        slack_app_config_token (str): The slack app config token to be saved.
        slack_app_config_refresh_token (str): The slack app config refresh token to be saved.
    """
    runner_id = os.getenv('RUNNER_ID', 'jl-local-runner')
    return bb_db_connector.db_save_slack_config_tokens(slack_app_config_token=slack_app_config_token, slack_app_config_refresh_token=slack_app_config_refresh_token, project_id=project_id, dataset_name=dataset_name)

def get_slack_config_tokens():
    """
    Retrieves the current slack access keys for the given runner_id from BigQuery.

    Returns:
        tuple: A tuple containing the slack app config token and the slack app config refresh token.
    """
    runner_id = os.getenv('RUNNER_ID', 'jl-local-runner')
    return bb_db_connector.db_get_slack_config_tokens(project_id=project_id, dataset_name=dataset_name)


def get_ngrok_auth_token():
    """
    Retrieves the ngrok authentication token, use domain flag, and domain for the given runner_id from BigQuery.

    Returns:
        tuple: A tuple containing the ngrok authentication token, use domain flag, and domain.
    """
    runner_id = os.getenv('RUNNER_ID', 'jl-local-runner')
    return bb_db_connector.db_get_ngrok_auth_token(project_id=project_id, dataset_name=dataset_name)

def set_ngrok_auth_token(ngrok_auth_token, ngrok_use_domain='N', ngrok_domain=''):
    """
    Updates the ngrok_tokens table with the provided ngrok authentication token, use domain flag, and domain.

    Args:
        ngrok_auth_token (str): The ngrok authentication token.
        ngrok_use_domain (str): Flag indicating whether to use a custom domain ('Y' or 'N').
        ngrok_domain (str): The custom domain to use if ngrok_use_domain is 'Y'.
    """
    runner_id = os.getenv('RUNNER_ID', 'jl-local-runner')
    return bb_db_connector.db_set_ngrok_auth_token(ngrok_auth_token=ngrok_auth_token, ngrok_use_domain=ngrok_use_domain, ngrok_domain=ngrok_domain, project_id=project_id, dataset_name=dataset_name)


def get_llm_key():
    """
    Retrieves the LLM key and type for the given runner_id from BigQuery.

    Returns:
        tuple: A tuple containing the LLM key and LLM type.
    """
    runner_id = os.getenv('RUNNER_ID', 'jl-local-runner')
    return bb_db_connector.db_get_llm_key(project_id=project_id, dataset_name=dataset_name)

def set_llm_key(llm_key, llm_type):
    """
    Updates the llm_key table with the provided LLM key and type.

    Args:
        llm_key (str): The LLM key.
        llm_type (str): The type of LLM (e.g., 'openai', 'reka').
    """
    runner_id = os.getenv('RUNNER_ID', 'jl-local-runner')
    return bb_db_connector.db_set_llm_key(llm_key=llm_key, llm_type=llm_type, project_id=project_id, dataset_name=dataset_name)



def generate_manifest_template(bot_id, bot_name, request_url, redirect_url):
    """
    Updates the bot manifest template with the provided parameters.

    Args:
        bot_id (str): The unique identifier for the bot.
        bot_name (str): The name of the bot.
        request_url (str): The URL to be set for event subscriptions.
        redirect_url (str): The URL to be set for OAuth redirect.

    Returns:
        dict: The updated manifest as a dictionary.
    """
    manifest_template = {
        "display_information": {
            "name": bot_name,
            "description": bot_id,
            "background_color": "#292129"
        },
        "features": {
            "app_home": {
                "home_tab_enabled": False,
                "messages_tab_enabled": True,
                "messages_tab_read_only_enabled": False
            },
            "bot_user": {
                "display_name": bot_name,
                "always_online": True
            }
        },
        "oauth_config": {
            "redirect_urls": [redirect_url],
            "scopes": {
                "bot": [
                    "channels:history",
                    "chat:write",
                    "files:read",
                    "files:write",
                    "im:history",
                    "im:read",
                    "im:write",
                    "im:write.invites",
                    "im:write.topic",
                    "mpim:history",
                    "mpim:read",
                    "mpim:write",
                    "mpim:write.invites",
                    "mpim:write.topic",
                    "users:read",
                    "users:read.email"
                ]
            }
        },
        "settings": {
            "event_subscriptions": {
                "request_url": request_url,
                "bot_events": [
                    "message.channels",
                    "message.im"
                ]
            },
            "org_deploy_enabled": False,
            "socket_mode_enabled": False,
            "token_rotation_enabled": False
        }
    }
    return manifest_template

def generate_manifest_template_socket(bot_id, bot_name, request_url, redirect_url):
    """
    Updates the bot manifest template with the provided parameters.

    Args:
        bot_id (str): The unique identifier for the bot.
        bot_name (str): The name of the bot.
        request_url (str): The URL to be set for event subscriptions.
        redirect_url (str): The URL to be set for OAuth redirect.

    Returns:
        dict: The updated manifest as a dictionary.
    """
    manifest_template = {    
       "display_information": {
            "name": bot_name,
            "description": bot_id,
            "background_color": "#292129"
        },
        "features": {
            "app_home": {
                "home_tab_enabled": False,
                "messages_tab_enabled": True,
                "messages_tab_read_only_enabled": False
            },
            "bot_user": {
                "display_name": bot_name,
                "always_online": True
            }
        },
        "oauth_config": {
            "redirect_urls": [redirect_url],
            "scopes": {
                "bot": [
                    "channels:history",
                    "chat:write",
                    "files:read",
                    "files:write",
                    "im:history",
                    "im:read",
                    "im:write",
                    "im:write.invites",
                    "im:write.topic",
                    "mpim:history",
                    "mpim:read",
                    "mpim:write",
                    "mpim:write.invites",
                    "mpim:write.topic",
                    "users:read",
                    "users:read.email",
                    "app_mentions:read",
                    "groups:history"
                ]
            }
        },
        "settings": {
            "event_subscriptions": {
                "bot_events": [
                    "app_mention",
                    "message.channels",
                    "message.groups",
                    "message.im",
                    "message.mpim"
                ]
            },
            "interactivity": {
                "is_enabled": True
            },
            "org_deploy_enabled": False,
            "socket_mode_enabled": True,
            "token_rotation_enabled": False
        }
    }
    return manifest_template

def rotate_slack_token(config_token, refresh_token):
    """
    Refreshes the Slack app configuration token using the provided refresh token.
    Parameters:
        config_token (str): The current configuration token for the Slack app.
        refresh_token (str): The refresh token for the Slack app.
    Returns:
        tuple: A tuple containing the new configuration token and refresh token.
    """
    # Endpoint for rotating the token
    rotate_url = 'https://slack.com/api/tooling.tokens.rotate'

    # Prepare headers for the POST request
    headers = {
        'Authorization': f'Bearer {refresh_token}',
        'Content-Type': 'application/x-www-form-urlencoded'
    }

    # Prepare the data payload for the POST request
    data = {
        'refresh_token': refresh_token
    }

    # Make a POST request to the Slack API to rotate the token
    response = requests.post(rotate_url, headers=headers, data=data)

    # Parse the response
    if response.status_code == 200:
        response_data = response.json()
        if response_data.get('ok'):
            # Extract the new tokens from the response
            new_config_token = response_data.get('token')
            new_refresh_token = response_data.get('refresh_token')
            save_slack_config_tokens(new_config_token, new_refresh_token)
            # Return the new tokens
            return new_config_token, new_refresh_token
        else:
            logger.error(f"Failed to rotate token: {response_data.get('error')}")
            return None, None 
    else:
        logger.error(f"Failed to rotate token, status code: {response.status_code}")
        return None, None 



def test_slack_config_token(config_token=None):
    """
    Calls the Slack API method apps.manifest.validate to validate the provided manifest using the config token.

    Args:
        config_token (str, optional): The Slack app config token. If not provided, it will be retrieved from the environment.

    Returns:
        dict: The result of the validation.
    """
    if config_token is None:
        config_token, refresh_token = get_slack_config_tokens()

    if not config_token:
        return False

    # Prepare the headers for the request
    headers = {
        "Authorization": f"Bearer {config_token}",
        "Content-Type": "application/json"
    }

    # Prepare the payload with the manifest
    manifest = generate_manifest_template('test', 'test', 'https://example.com', 'https://example.com')
    payload = {
        "manifest": json.dumps(manifest)
    }

    # Slack API endpoint for manifest validation
    validate_url = "https://slack.com/api/apps.manifest.validate"

    try:
        # Make the request to the Slack API
        response = requests.post(validate_url, headers=headers, json=payload)

        # Check if the response is successful
        if response.status_code == 200:
            response_data = response.json()
            if response_data.get("ok"):
                logger.info("Manifest validation successful.")
                return True
            else:
                if response_data.get('error') == 'token_expired':
                    return("token_expired")

                logger.error(f"Manifest validation failed with error: {response_data.get('error')}")
                return False
        else:
            logger.error(f"Manifest validation failed with status code: {response.status_code}")
            return False
    except Exception as e:
        logger.exception("Failed to validate manifest with Slack API.")
        return False



def create_slack_bot_with_manifest(token, manifest):
    """
    Calls the Slack API to create a new Slack bot with the provided manifest.

    Parameters:
        token (str): The OAuth token used for authentication.
        manifest (dict): The manifest configuration for the new Slack bot.

    Returns:
        dict: A dictionary containing the response data from the Slack API.
    """
    # Endpoint for creating a new Slack bot
    create_url = 'https://slack.com/api/apps.manifest.create'

    # Prepare headers for the POST request
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }

    # Make a POST request to the Slack API to create the bot with the manifest
    response = requests.post(create_url, headers=headers, json={'manifest': manifest})

    # Parse the response
    if response.status_code == 200:
        response_data = response.json()
        if response_data.get('ok'):
            # Return the response data if the request was successful
            return response_data
        else:
            raise Exception(f"Failed to create Slack bot: {response_data.get('error')}")
    else:
        raise Exception(f"Failed to create Slack bot, status code: {response.status_code}")

def insert_new_bot(api_app_id, bot_slack_user_id, bot_id, bot_name, bot_instructions, runner_id, slack_signing_secret, 
                   slack_channel_id, available_tools, auth_url, auth_state, client_id, client_secret, udf_active, 
                   slack_active, files, bot_implementation):
    """
    Inserts a new bot configuration into the BOT_SERVICING table.

    Args:
        api_app_id (str): The API application ID for the bot.
        bot_slack_user_id (str): The Slack user ID for the bot.
        bot_id (str): The unique identifier for the bot.
        bot_name (str): The name of the bot.
        bot_instructions (str): Instructions for the bot's operation.
        runner_id (str): The identifier for the runner that will manage this bot.
        slack_app_token (str): The Slack app token for the bot.
        slack_signing_secret (str): The Slack signing secret for the bot.
        slack_channel_id (str): The Slack channel ID where the bot will operate.
        tools (str): A list of tools the bot has access to.
        files (json-embedded list): A list of files to include with the bot.
        bot_implementation: openai or cortex or ...
    """

    return bb_db_connector.db_insert_new_bot(api_app_id, bot_slack_user_id, bot_id, bot_name, bot_instructions, runner_id, slack_signing_secret, 
                   slack_channel_id, available_tools, auth_url, auth_state, client_id, client_secret, udf_active, 
                   slack_active, files, bot_implementation, project_id, dataset_name, bot_servicing_table)

   

def add_new_tools_to_bot(bot_id, new_tools):
    """
    Adds new tools to an existing bot's available_tools list if they are not already present.

    Args:
        bot_id (str): The unique identifier for the bot.
        new_tools (list): A list of new tool names to add to the bot.

    Returns:
        dict: A dictionary containing the tools that were added and those that were already present.
    """
    # Retrieve the current available tools for the bot
    
    available_tools_list = bb_db_connector.db_get_available_tools(project_id=project_id, dataset_name=dataset_name)
    available_tool_names = [tool['tool_name'] for tool in available_tools_list]

    # Check if all new_tools are in the list of available tools
    invalid_tools = [tool for tool in new_tools if tool not in available_tool_names]
    if invalid_tools:
        return {"success": False, "error": f"The following tools are not available: {', '.join(invalid_tools)}. The available tools are {available_tool_names}."}
    
    bot_details = get_bot_details(bot_id)
    if not bot_details:
        logger.error(f"Bot with ID {bot_id} not found.")
        return {"success": False, "error": "Bot not found."}

    current_tools_str = bot_details.get('available_tools', '[]')
    current_tools = json.loads(current_tools_str) if current_tools_str else []

    # Determine which tools are new and which are already present
    new_tools_to_add = [tool for tool in new_tools if tool not in current_tools]
    already_present = [tool for tool in new_tools if tool in current_tools]

    # Update the available_tools in the database
    updated_tools = current_tools + new_tools_to_add
    updated_tools_str = json.dumps(updated_tools)

    return bb_db_connector.db_update_bot_tools(project_id=project_id,dataset_name=dataset_name,bot_servicing_table=bot_servicing_table, bot_id=bot_id, updated_tools_str=updated_tools_str, new_tools_to_add=new_tools_to_add, already_present=already_present, updated_tools=updated_tools)


def validate_potential_files(new_file_ids=None):

   
    if isinstance(new_file_ids, str) and new_file_ids.lower() == 'null' or isinstance(new_file_ids, str) and new_file_ids.lower() == '[null]':
        new_file_ids = []

    if new_file_ids == [] or new_file_ids is None:
        return {"success": True, "message": "No files attached"}

    # Remove the part before the last '/' in each file_id
    new_file_ids = [file_id.split('/')[-1] for file_id in new_file_ids]

    valid_extensions = {
            '.c': 'text/x-c',
            '.cs': 'text/x-csharp',
            '.cpp': 'text/x-c++',
            '.doc': 'application/msword',
            '.docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
            '.html': 'text/html',
            '.java': 'text/x-java',
            '.json': 'application/json',
            '.md': 'text/markdown',
            '.pdf': 'application/pdf',
            '.php': 'text/x-php',
            '.pptx': 'application/vnd.openxmlformats-officedocument.presentationml.presentation',
            '.py': 'text/x-python',
            '.rb': 'text/x-ruby',
            '.tex': 'text/x-tex',
            '.txt': 'text/plain',
            '.css': 'text/css',
            '.js': 'text/javascript',
            '.sh': 'application/x-sh',
            '.ts': 'application/typescript',
        }

    # Check if any file ID starts with 'file-' and return an error if found
    file_stage_prefix_error_ids = [file_id for file_id in new_file_ids if file_id.startswith('file-')]
    if file_stage_prefix_error_ids:
        error_message = f"Files with IDs starting with 'file-' detected. Please use _add_file_to_stage function to upload the file to the Internal Files Stage for bots at: {genbot_internal_project_and_schema}.BOT_FILES_STAGE, and ensure to give it a human-readable valid file name with an allowed extension from the following list: {', '.join(valid_extensions.keys())}."
        logger.error(error_message)
        return {"success": False, "error": error_message}

    invalid_file_ids = [file_id for file_id in new_file_ids if not any(file_id.endswith(ext) for ext in valid_extensions)]
    if invalid_file_ids:
        error_message = f"Invalid file extension(s) for file ID(s): {', '.join(invalid_file_ids)}. Allowed extensions are: {', '.join(valid_extensions.keys())}."
        logger.error(error_message)
        return {"success": False, "error": error_message}

    internal_stage =  f"{genbot_internal_project_and_schema}.BOT_FILES_STAGE"
    database, schema, stage_name = internal_stage.split('.')

    stage_contents = bb_db_connector.list_stage_contents(database=database, schema=schema, stage=stage_name)
    # Check if the file is in stage_contents
    stage_file_names = [file_info['name'].split('/')[-1] if '/' in file_info['name'] else file_info['name'] for file_info in stage_contents]
    missing_files = [file_id.split('/')[-1] for file_id in new_file_ids if file_id not in stage_file_names]
    if missing_files:
        #limited_stage_contents = stage_file_names[:50]
        #more_files_exist = len(stage_file_names) > 50
        error_message = f"The following files are not in the stage: {', '.join(missing_files)}. Use _add_file_to_stage function to upload the file to the Internal Files Stage for bots at: {genbot_internal_project_and_schema}.BOT_FILES_STAGE"
        logger.warn(error_message)
        return {"success": False, "error": error_message}
    # Proceed if all files are present in the stage
    return {"success": True, "message": "All files are valid"}

def add_bot_files(bot_id, new_file_ids):
    """
    Adds a new file ID to the existing files list for the bot and saves it to the database.

    Args:
        bot_id (str): The unique identifier for the bot.
        new_file_ids (array): The new file ID to add to the bot's files list.
    """

    if isinstance(new_file_ids, str) and new_file_ids.lower() == 'null':
        new_file_ids = []

    if new_file_ids is None:
        new_file_ids = []

    new_file_ids = [file_id.split('/')[-1] for file_id in new_file_ids]

    # Retrieve the current files for the bot
    bot_details = get_bot_details(bot_id)
    if not bot_details:
        logger.error(f"Bot with ID {bot_id} not found.")
        return {"success": False, "error": "Bot not found.  Check for the bot_id using the list_all_bots function."}

    v = validate_potential_files(new_file_ids=new_file_ids)
    if v.get("success",False) == False:
        return v

    current_files_str = bot_details.get('files', '[]')
    if current_files_str == 'null':
        current_files_str = '[]'
    if current_files_str == '""':
        current_files_str = []
    current_files = json.loads(current_files_str) if current_files_str else []

    # Add the new file IDs if they're not already present
    for new_file_id in new_file_ids:
        if new_file_id not in current_files:
            current_files.append(new_file_id)
    updated_files_str = json.dumps(current_files)

    return bb_db_connector.db_update_bot_files(project_id=project_id, dataset_name=dataset_name, bot_servicing_table=bot_servicing_table, bot_id=bot_id, updated_files_str=updated_files_str, current_files=current_files, new_file_ids=new_file_ids)
    

def update_bot_instructions(bot_id, new_instructions, confirmed=None, thread_id = None):

    """
    Updates the bot_instructions in the database for the specified bot_id for the current runner_id.

    Args:
        bot_id (str): The unique identifier for the bot.
        new_instructions (str): The new instructions for the bot.
    """
    runner_id = os.getenv('RUNNER_ID', 'jl-local-runner')

    bot_details = get_bot_details(bot_id)

    if bot_details is None:
        return {
            "success": False,
            "error": f"Invalid bot_id: {bot_id}. Use list_all_bots to find the correct bot_id."
        }

    if confirmed != 'CONFIRMED':
        current_instructions = bot_details.get('bot_instructions', '')
        return {
            "success": False,
            "message": f"Please confirm the change of instructions. Call this function again with new parameter confirmed=CONFIRMED to confirm this change.",
            "current_instructions": current_instructions,
            "new_instructions": new_instructions
        }

    return bb_db_connector.db_update_bot_instructions(project_id=project_id, dataset_name=dataset_name, bot_servicing_table=bot_servicing_table, bot_id=bot_id, instructions=new_instructions, runner_id=runner_id)


def test_slack_app_level_token(app_level_token):
    """
    Test the Slack App Level Token by first checking if the token is valid in general, 
    and then testing to see if it can be used for apps.connections.open.

    Args:
        app_level_token (str): The Slack App Level Token to be tested.

    Returns:
        dict: A dictionary with a success flag and a message or error depending on the outcome.
    """
    # Test if the token is valid in general by calling the auth.test method
    auth_test_url = "https://slack.com/api/auth.test"
    headers = {
        "Authorization": f"Bearer {app_level_token}"
    }
    auth_test_response = requests.post(auth_test_url, headers=headers)
    if auth_test_response.status_code == 200 and auth_test_response.json().get("ok"):
        # If the token is valid, test to see if it can be used for apps.connections.open
        connections_open_url = "https://slack.com/api/apps.connections.open"
        connections_open_response = requests.post(connections_open_url, headers=headers)
        if connections_open_response.status_code == 200 and connections_open_response.json().get("ok"):
            # The token is valid and can open a socket connection
            return {"success": True, "message": "The token is valid and can open a socket connection."}
        else:
            # The token is valid but cannot open a socket connection
            return {"success": False, "error": "The token is a Slack token but it's not an app-level token valid but cannot open a socket connection. Make sure you provide an App Level Token for this application with connection-write scope.  It should start with xapp-."}
    else:
        # The token is not valid
        return {"success": False, "error": "The token is invalid.  Make sure you provide an App Level Token with connection-write scope.  It should start with xapp-."}

def update_bot_details(bot_id, bot_slack_user_id, slack_app_token):
    return bb_db_connector.db_update_bot_details(bot_id, bot_slack_user_id, slack_app_token, project_id, dataset_name, bot_servicing_table)


def update_slack_app_level_key(bot_id, slack_app_level_key):
    """
    Wrapper function to update the Slack app level key for a specific bot after verifying it is a valid app level token.

    Args:
        bot_id (str): The unique identifier for the bot.
        slack_app_level_key (str): The new Slack app level key to set for the bot.
    """
    # First, test the Slack app level token to ensure it is valid
    token_test_result = test_slack_app_level_token(slack_app_level_key)
    bot_details = get_bot_details(bot_id)
    
    if not bot_details:
        return {"success": False, "error": "Bot details not found for bot_id: {}.  Try using list_all_bots to look it up.".format(bot_id)}
    auth_url = bot_details.get('auth_url', 'No auth_url available')
    
    if not token_test_result.get("success"):
        return token_test_result
    
    if bot_details.get('bot_slack_user_id') == 'Pending_APP_LEVEL_TOKEN':
        update_bot_details(bot_id=bot_id, bot_slack_user_id='Pending_OAuth', slack_app_token=bot_details.get('slack_app_token', None))

    try:
        # Call the function from the Snowflake connector to update the Slack app level key
        result = bb_db_connector.db_update_slack_app_level_key(project_id=project_id, dataset_name=dataset_name, bot_servicing_table=bot_servicing_table, bot_id=bot_id, slack_app_level_key=slack_app_level_key)
        if result.get("success"):
            if auth_url:
                add_to_string = ""
                if os.getenv('SNOWFLAKE_HOST',None) is not None:
                    add_to_string = " Tell the user they may need to log into both Slack and Snowflake to complete this process, and that if they are talking to you from within a Streamlit GUI they should cut and paste the link into a new browser tab versus clicking it directly. If they tell you later that they have any issues with this, tell them to contact their Slack and/or Snowflake administrator depending on which login prompt they got and couldn't pass."
                else:
                    add_to_string =  " Tell the user they may need to log into Slack to complete this process. Tell them that if they see an NGROK warning page, it's safe to press OK to continue.  If they tell you later that they have any issues with this, tell them to pass these instructions on to their Slack administrator."
                return {
                    "success": True,
                    "message": f"Now ask the user to use this authentication URL to complete the installation of the new app into their Slack workspace: {auth_url}.{add_to_string}"
                }
            else:
                return {
                    "success": True,
                    "message": "The Slack app level key has been updated successfully, but no authentication URL is available.  Ask the user to try removing and re-creating the bot to fix this."
                }
        else:
            return {
                "success": False,
                "error": "Failed to update the Slack app level key. Ask the user to try removing and re-creating the bot to fix this."
            }
        return result
    except Exception as e:
        logger.error(f"Failed to update Slack app level key for bot_id: {bot_id} with error: {e}")
        return {"success": False, "error": str(e)}


def update_existing_bot(api_app_id, bot_id, bot_slack_user_id, client_id, client_secret, slack_signing_secret, 
                        auth_url, auth_state, udf_active, slack_active, files, bot_implementation):
    files_json = json.dumps(files)
    return bb_db_connector.db_update_existing_bot(api_app_id, bot_id, bot_slack_user_id, client_id, client_secret, slack_signing_secret, 
                            auth_url, auth_state, udf_active, slack_active, files_json, bot_implementation, project_id, dataset_name, bot_servicing_table)




def get_bot_details(bot_id):
    """
    Retrieves the details of a bot based on the provided bot_id from the BOT_SERVICING table.

    Args:
        bot_id (str): The unique identifier for the bot.

    Returns:
        dict: A dictionary containing the bot details if found, otherwise None.
    """
    return bb_db_connector.db_get_bot_details(project_id=project_id, dataset_name=dataset_name, bot_servicing_table=bot_servicing_table, bot_id=bot_id)


def get_available_tools():
    return bb_db_connector.db_get_available_tools(project_id=project_id, dataset_name=dataset_name)


def add_or_update_available_tool(tool_name, tool_description):
    return bb_db_connector.db_add_or_update_available_tool(tool_name=tool_name, tool_description=tool_description, project_id=project_id, dataset_name=dataset_name)


def make_baby_bot(bot_id, bot_name, bot_instructions='You are a helpful bot.', available_tools=None, runner_id=None, slack_channel_id=None, confirmed=None, activate_slack='Y', 
                  files = "", bot_implementation = "openai",
                  update_existing=False):
    

    try:
        files_array = json.loads(files)
        if isinstance(files_array, list):
            files = files_array
    except Exception as e:
        pass

    if isinstance(files, str):
        files = files.split(',') if files else []

    if available_tools == []:
        available_tools = None

    try:
        logger.info(f"Creating {bot_id} named {bot_name}")

        v = validate_potential_files(new_file_ids=files)
        if v.get("success",False) == False:
            return v

        if not update_existing:
            available_tools_array = available_tools.split(',') if available_tools else []

            # Validate the formatting and parsing of available_tools
            parsed_tools_str = ','.join(available_tools_array)
            if not (parsed_tools_str == '' and available_tools == None):
                if parsed_tools_str != available_tools:
                    return(f"Tool call error: Available tools was not properly formatted, it should be either the name of a single tool like 'tool1' or a simple list of tools like 'tool1,tool2'")

                # Check for leading or trailing whitespace in the available tools
                for tool in available_tools_array:
                    if tool != tool.strip():
                        return ValueError(f"Tool call error: Tool '{tool}' has leading or trailing whitespace in available_tools. Please remove any extra spaces from your list.")

                # Retrieve the list of available tools from the database
                db_available_tools = get_available_tools()
                db_tool_names = [tool['tool_name'] for tool in db_available_tools]

                # Check if the provided available tools match the database tools
                if not all(tool in db_tool_names for tool in available_tools_array):
                    invalid_tools = [tool for tool in available_tools_array if tool not in db_tool_names]
                    error_message = f"Tool call error: The following tools you included in available_tools are not available or invalid: {', '.join(invalid_tools)}.  The tools you can include in available_tools are: {db_available_tools}.  The available_tools parameter should be either a single tool like 'tool1' or a simple list of tools like 'tool1,tool2' (with no single quotes in the actual paramater string you send)"
                    return(error_message)
        
            confirm=False
            if confirmed is not None:
                if confirmed.upper() == 'CONFIRMED':
                    confirm=True

            # validate files 


            if confirm == False:
                conf = f'NOTE BOT NOT YET CREATED--ACTION REQUIRED:\nYou are about to create a new bot with bot_it {bot_id} called {bot_name}.\nBot instructions are: {bot_instructions}\n'
                if runner_id:
                    conf += f'The server to run this bot on is {runner_id}.\n'
                if activate_slack == 'N':
                    conf += f'You have chosen to NOT activate this bot on Slack at this time.\n'
                if available_tools:
                    conf += f'The array of tools available to this bot is: {available_tools}\n'
                else:
                    conf += 'No tools will be made available to this bot.\n'
                if files is not None and files != []:
                    conf += f'The array of files available to this bot is: {files}\n'
                conf += "Please make sure you have validated all this with the user.  If you've already validated with the user, and ready to make the Bot, call this function again with the parameter confirmed=CONFIRMED"
                return(conf)
        
        slack_active = test_slack_config_token()
        if slack_active == 'token_expired':
            t, r = get_slack_config_tokens()
            tp, rp = rotate_slack_token(config_token=t, refresh_token=r)
            slack_active = test_slack_config_token()



        def get_udf_endpoint_url():
            alt_service_name = os.getenv('ALT_SERVICE_NAME',None)
            if alt_service_name:
                query1 = f"SHOW ENDPOINTS IN SERVICE {alt_service_name};"
            else:
                query1 = f"SHOW ENDPOINTS IN SERVICE {project_id}.{dataset_name}.GENESISAPP_SERVICE_SERVICE;"
            try:
                logger.warning(f"Running query to check endpoints: {query1}")
                results = bb_db_connector.run_query(query1)
                udf_endpoint_url = next((endpoint['ingress_url'] for endpoint in results if endpoint['name'] == 'udfendpoint'), None)
                return udf_endpoint_url
            except Exception as e:
                logger.warning(f"Failed to get UDF endpoint URL with error: {e}")
                return None

        ep = get_udf_endpoint_url()
        logger.warning(f'Endpoint for service: {ep}')

        if slack_active and activate_slack != 'N':       

            ngrok_base_url = os.getenv('NGROK_BASE_URL')
            if not ngrok_base_url and ep == None:
                raise ValueError("The NGROK_BASE_URL environment variable is missing or empty, and no Snowflake SCPS endpoing is available for routing activation requests.")

            if ep:
                request_url = f"{os.getenv('NGROK_BASE_URL')}/slack/events/{bot_id}"
                redirect_url = f"https://{ep}/slack/events/{bot_id}/install"
            else:
                request_url = f"{os.getenv('NGROK_BASE_URL')}/slack/events/{bot_id}"
                redirect_url = f"{os.getenv('NGROK_BASE_URL')}/slack/events/{bot_id}/install"


            #manifest = generate_manifest_template(bot_id, bot_name, request_url=request_url, redirect_url=redirect_url)
            manifest = generate_manifest_template_socket(bot_id, bot_name, request_url=request_url, redirect_url=redirect_url)

            slack_app_config_token, slack_app_config_refresh_token = get_slack_config_tokens()

            logger.warn(f'-->  Manifest: {manifest}')
            try:
                bot_create_result = create_slack_bot_with_manifest(slack_app_config_token,manifest)
            except Exception as e:
                logger.warn(f'Error on creating slackbot: {e}, Manifest: {manifest}')
                return {"success": False, "message": f'Error on creating slackbot: {e}'}

            app_id = bot_create_result.get('app_id')
            credentials = bot_create_result.get('credentials')
            client_id = credentials.get('client_id') if credentials else None
            client_secret = credentials.get('client_secret') if credentials else None
        #    bot_user_id = 'Pending_OAuth'
            bot_user_id = 'Pending_APP_LEVEL_TOKEN'
            #verification_token = credentials.get('verification_token') if credentials else None
            signing_secret = credentials.get('signing_secret') if credentials else None
            oauth_authorize_url = bot_create_result.get('oauth_authorize_url')
            auth_state = str(uuid.uuid4())
            oauth_authorize_url+="&state="+auth_state

            # TODO base this off whether slack has already been activated 
            udf_active = 'Y'
            slack_active = 'Y'

        else:
            udf_active = 'Y'
            slack_active = 'N'
            oauth_authorize_url = None
            auth_state = None
            oauth_authorize_url = None
            signing_secret = None
            bot_user_id = None
            client_secret = None
            client_id = None
            credentials = None
            app_id = None

        if runner_id == None:
            runner_id = os.getenv('RUNNER_ID','jl-local-runner')

        if update_existing:
            update_existing_bot(
                api_app_id=app_id,
                bot_slack_user_id=bot_user_id,
                client_id=client_id,
                client_secret=client_secret,
                bot_id=bot_id,
                slack_signing_secret=signing_secret,
                auth_url=oauth_authorize_url,
                auth_state=auth_state,
                udf_active=udf_active,
                slack_active=slack_active,
                files=files,
                bot_implementation=bot_implementation,
            )            
        else:
            insert_new_bot(
                api_app_id=app_id,
                bot_slack_user_id=bot_user_id,
                client_id=client_id,
                client_secret=client_secret,
                bot_id=bot_id,
                bot_name=bot_name,
                bot_instructions=bot_instructions,
                runner_id=runner_id,
                slack_signing_secret=signing_secret,
                slack_channel_id=slack_channel_id,
                available_tools=available_tools_array,
                auth_url=oauth_authorize_url,
                auth_state=auth_state,
                udf_active=udf_active,
                slack_active=slack_active,
                files=files,
                bot_implementation=bot_implementation,
            )

        #    "message": f"Created {bot_id} named {bot_name}.  Now ask the user to use this authentication URL to complete the installation of the new app into their Slack workspace: {oauth_authorize_url}",
        print(oauth_authorize_url)
        if slack_active == 'Y':
            print("temp_debug: create success ", bot_id, bot_name)
            return {"success": True, 
                    "Success": True,
                    "message": f"Created {bot_id} named {bot_name}. To complete the setup on Slack for this bot, tell the user there are two more steps, first is to go to: https://api.slack.com/apps/{app_id}/general Ask them to scroll to App Level Tokens, add a token called 'app_token' with scope 'connections-write', and provide the results back to this bot.  Then you, the bot, should call the update_app_level_key function to update the backend.  Once you and the user do that, I will give you an AUTH_URL for the user to click as the second step to complete the installation."
                    }
        
        else:
            return {"success": True, "message": f"Created {bot_id} named {bot_name}.  Tell the user that they can now press 'New Chat' and select this new bot from the drop down at the top of the screen."}


    except Exception as e:
        logger.exception("Failed to create new bot")
        return {"success": False, "error": f"Failed to create {bot_id} named {bot_name}"}


server_point = None
map_point = None 

def set_remove_pointers(server, map):
    global server_point
    global map_point
    server_point = server
    map_point = map


def _remove_bot(bot_id, thread_id=None, confirmed=None):

    """
    Removes a bot based on its bot_id. It deletes the bot from the database table and via the Slack API.

    Args:
        bot_id (str): The unique identifier for the bot to be removed.
        confirm (str, optional): Confirmation string to proceed with deletion. Defaults to None.
    """
    # Confirmation check
    # Retrieve bot details using the bot_id
    bot_details = get_bot_details(bot_id)
    if not bot_details:
        logger.error(f"Bot with ID {bot_id} not found.")
        return {"success": False, "error": "Bot not found."}
  
    if bot_details["bot_id"] == 'jl-local-eve-test-1' or bot_details["bot_id"] == 'jl-local-elsa-test-1':
        return {"success": False, "error": "Deleting local test Eve or Elsa not allowed."}

    expected_confirmation = f"!CONFIRM DELETE {bot_id}"
    if confirmed != expected_confirmation:
        bot_name = bot_details.get('bot_name', 'Unknown')
        return f"Confirmation required: this method will delete bot {bot_id} named {bot_name}. To complete the deletion call this function again with the 'confirmed' parameter set to '{expected_confirmation}'"

    # Proceed with deletion if confirmation is provided

    # Retrieve the session using the API App ID from the map
    api_app_id = bot_details.get('api_app_id')
    session = map_point.get(api_app_id)
   
    # If a session is found, attempt to remove it
    if session:
        server_point.remove_session(session)
        logger.info(f"Session {session} for bot with API App ID {api_app_id} has been removed.")
    else:
        logger.info(f"No session found for bot with API App ID {api_app_id} proceeding to delete from database and Slack.")
 
    bb_db_connector.db_delete_bot(project_id=project_id, dataset_name=dataset_name, bot_servicing_table=bot_servicing_table, bot_id=bot_id)

    # Rotate the Slack app configuration token before making the Slack API call
    slack_app_config_token, slack_app_config_refresh_token = get_slack_config_tokens()
    #slack_app_config_token, slack_app_config_refresh_token = rotate_slack_token(slack_app_config_token, slack_app_config_refresh_token)

    if bot_details["slack_active"]=='Y':
        app_id = bot_details.get('api_app_id')

        if app_id and slack_app_config_token:
            # Endpoint for deleting the bot via the Slack API
            delete_url = 'https://slack.com/api/apps.manifest.delete'

            # Prepare headers for the POST request
            headers = {
                'Authorization': f'Bearer {slack_app_config_token}',
                'Content-Type': 'application/json'
            }

            # Prepare the data payload for the POST request
            data = {
                'app_id': app_id
            }

            # Make a POST request to the Slack API to delete the bot
            response = requests.post(delete_url, headers=headers, json=data)

            # Parse the response
            if response.status_code == 200:
                response_data = response.json()
                if response_data.get('ok'):
                    logger.info(f"Successfully deleted bot with bot_id: {bot_id} via the Slack API.")
                    return(f"Successfully deleted bot with bot_id: {bot_id}.")
                else:
                    return(f"Removed, but could not find on Slack: {response_data.get('error')}")
            else:
                return(f"Removed, but failed to delete bot via Slack API, status code: {response.status_code}")
        else:
            return(f"Removed, but no app_id or Slack app configuration token found for bot_id: {bot_id}. Cannot delete bot via Slack API.")
    else:
        return(f"Successfully deleted bot with bot_id: {bot_id}.")

def update_bot_implementation(bot_id, bot_implementation):
    """
    Updates the bot_implementation field in the BOT_SERVICING table for a given bot_id.

    Args:
        bot_id (str): The unique identifier for the bot.
        bot_implementation (str): The new implementation type to be set for the bot (e.g., 'openai', 'cortex').
    """
    runner_id = os.getenv('RUNNER_ID', 'jl-local-runner')

    bot_config = get_bot_details(bot_id=bot_id)

    return bb_db_connector.db_update_bot_implementation(project_id=project_id, dataset_name=dataset_name, bot_servicing_table=bot_servicing_table, bot_id=bot_id, bot_implementation=bot_implementation, runner_id=runner_id)


MAKE_BABY_BOT_DESCRIPTIONS = [{
    "type": "function",
    "function": {
        "name": "make_baby_bot",
        "description": "Creates a new bot with the specified parameters and logs the creation event.  BE SURE TO RECONFIRM AND DOUBLE CHECK ALL THE PARAMETERS WITH THE END USER BEFORE RUNNING THIS TOOL!",
        "parameters": {
            "type": "object",
            "properties": {
                "bot_id": {
                    "type": "string",
                    "description": "The unique identifier for the bot.  Should be the bot_name dash a 6 letter alphanumeric random code, for example mybot-w73hxg. Generate this yourself dont ask the user for it."
                },
                "bot_name": {
                    "type": "string",
                    "description": "The name of the bot."
                },
                "bot_instructions": {
                    "type": "string",
                    "description": "Instructions for the bot's operation. Defaults to 'You are a helpful bot.'",
                    "default": "You are a helpful bot."
                },
                "available_tools": {
                    "type": "string",
                    "description": "A comma-separated list of tools the new bot shoulf have access to, if any.  Example of a valid string for this field: 'slack_tools,database_tools,webpage_downloader'.  Use the get_available_tools tool to get a list of the tools that can be referenced here. ",
                    "default": ""
                },
                "runner_id": {
                    "type": "string",
                    "description": "The identifier for the server that will serve this bot. Only set this if directed specifically by the user, otherwise don't include it."
                },
                "activate_slack": {
                    "type": "string",
                    "description": "Set to Y to activate the bot on Slack, if possible.  Set to N to specifically NOT activate on Slack.  Only set to N if specified by the user.  Default is Y."
                },
                "confirmed": {
                    "type": "string",
                    "description": "Use this only if instructed by a response from this bot.  DO NOT SET IT AS CONFIRMED UNTIL YOU HAVE GONE BACK AND DOUBLECHECKED ALL PARAMETERS WITH THE END USER IN YOUR MAIN THREAD."
                },
                "files": {
                    "type": "string",
                    "description": "a commma-separated list of files to be available to the bot, they must first be added to the Internal Bot File Stage"
                },
                "bot_implementation": {
                    "type": "string",
                    "description": "The implementation type for the bot. Examples include 'openai', 'cortex', or custom implementations.",
                },
            },
            "required": ["bot_id", "bot_name", "bot_instructions"]
        }
    }
}]

MAKE_BABY_BOT_DESCRIPTIONS.append({
    "type": "function",
    "function": {
        "name": "get_available_tools",
        "description": "Retrieves the list of tools that a bot can assign to baby bots when using make_baby_bot.",
    }
})

MAKE_BABY_BOT_DESCRIPTIONS.append({
    "type": "function",
    "function": {
        "name": "_remove_bot",
        "description": "Deletes a bot with the specified bot_id and cleans up any resources it was using.  USE THIS VERY CAREFULLY, AND DOUBLE-CHECK WITH THE USER THE DETAILS OF THE BOT YOU PLAN TO DELETE BEFORE CALLING THIS FUNCTION.",
        "parameters": {
            "type": "object",
            "properties": {
                "bot_id": {
                    "type": "string",
                    "description": "The unique identifier for the bot to be deleted.  BE SURE TO CONFIRM THIS WITH THE USER!  Use the list_all_bots tool to figure out the bot_id."
                },
                "confirmed": {
                    "type": "string",
                    "description": "Use this only if instructed by a response from this bot."
                }
            },
            "required": ["bot_id"]
        }
    }
})

MAKE_BABY_BOT_DESCRIPTIONS.append({
    "type": "function",
    "function": {
        "name": "list_all_bots",
        "description": "Lists all the bots being served by the system, including their bot_ids, slack_user_id, runner IDs, names, instructions, tools, auth_url, etc.  This is useful to find information about a bot, or to search for a particular bot.",
    }
})

MAKE_BABY_BOT_DESCRIPTIONS.append({
    "type": "function",
    "function": {
        "name": "add_new_tools_to_bot",
        "description": "Adds new tools to an existing bot's available_tools list if they are not already present. It is ok to use this to grant tools to yourself if directed.",
        "parameters": {
            "type": "object",
            "properties": {
                "bot_id": {
                    "type": "string",
                    "description": "The unique identifier for the bot.  Use list_all_bots function if you are unsure of the bot_id."
                },
                "new_tools": {
                    "type": "array",
                    "items": {
                        "type": "string"
                    },
                    "description": "A list of new tool names to add to the bot.  Use get_available_tools function to know whats available."
                }
            },
            "required": ["bot_id", "new_tools"]
        }
    }
})

MAKE_BABY_BOT_DESCRIPTIONS.append({
    "type": "function",
    "function": {
        "name": "update_bot_instructions",
        "description": "Updates the bot_instructions for the specified bot_id.",
        "parameters": {
            "type": "object",
            "properties": {
                "bot_id": {
                    "type": "string",
                    "description": "The unique identifier for the bot."
                },
                "new_instructions": {
                    "type": "string",
                    "description": "The new instructions for the bot."
                }
            },
            "required": ["bot_id", "new_instructions"]
        }
    }
})

MAKE_BABY_BOT_DESCRIPTIONS.append({
    "type": "function",
    "function": {
        "name": "add_bot_files",
        "description": "Adds to the files list for the specified bot_id by adding new files if they are not already present.",
        "parameters": {
            "type": "object",
            "properties": {
                "bot_id": {
                    "type": "string",
                    "description": "The unique identifier for the bot."
                },
                "new_file_ids": {
                    "type": "array",
                    "items": {
                        "type": "string"
                    },
                    "description": "A list of the filenames from the Internal File Stage for Bots to assign to the bot."
                }
            },
            "required": ["bot_id", "file_names"]
        }
    }
})

MAKE_BABY_BOT_DESCRIPTIONS.append({
    "type": "function",
    "function": {
        "name": "update_app_level_key",
        "description": "Updates the Slack app level key for a specific bot after verifying it is a valid app level token.",
        "parameters": {
            "type": "object",
            "properties": {
                "bot_id": {
                    "type": "string",
                    "description": "The unique identifier for the bot."
                },
                "slack_app_level_key": {
                    "type": "string",
                    "description": "The new Slack app level key to set for the bot."
                }
            },
            "required": ["bot_id", "slack_app_level_key"]
        }
    }
})

MAKE_BABY_BOT_DESCRIPTIONS.append({
    "type": "function",
    "function": {
        "name": "update_bot_implementation",
        "description": "Updates the implementation type for a specific bot, allowing for changes in how the bot operates or interacts with its environment.",
        "parameters": {
            "type": "object",
            "properties": {
                "bot_id": {
                    "type": "string",
                    "description": "The unique identifier for the bot."
                },
                "bot_implementation": {
                    "type": "string",
                    "description": "The new implementation type to be set for the bot. Valid options include 'openai', 'cortex'."
                }
            },
            "required": ["bot_id", "bot_implementation"]
        }
    }
})





# Add the new tool to the make_baby_bot_tools dictionary
make_baby_bot_tools = {"make_baby_bot": "bot_genesis.make_baby_bot.make_baby_bot"}
make_baby_bot_tools["get_available_tools"] = "bot_genesis.make_baby_bot.get_available_tools"
make_baby_bot_tools["_remove_bot"] = "bot_genesis.make_baby_bot._remove_bot"
make_baby_bot_tools["list_all_bots"] = "bot_genesis.make_baby_bot.list_all_bots"
make_baby_bot_tools["update_bot_instructions"] = "bot_genesis.make_baby_bot.update_bot_instructions"
make_baby_bot_tools["add_new_tools_to_bot"] = "bot_genesis.make_baby_bot.add_new_tools_to_bot"
make_baby_bot_tools["add_bot_files"] = "bot_genesis.make_baby_bot.add_bot_files"
make_baby_bot_tools["update_app_level_key"] = "bot_genesis.make_baby_bot.update_slack_app_level_key"
make_baby_bot_tools["update_bot_implementation"] = "bot_genesis.make_baby_bot.update_bot_implementation"

# internal functions

def update_bot_endpoints(new_base_url, runner_id=None):
    """
    Updates the endpoints for all Slack bots running on the specified runner_id with a new base URL.

    Args:
        new_base_url (str): The new base URL to update the bot endpoints with.
        runner_id (str, optional): The runner_id to filter the bots. Defaults to the RUNNER_ID environment variable.
    """
    def get_udf_endpoint_url():
        alt_service_name = os.getenv('ALT_SERVICE_NAME',None)
        if alt_service_name:
            query1 = f"SHOW ENDPOINTS IN SERVICE {alt_service_name};"
        else:
            query1 = f"SHOW ENDPOINTS IN SERVICE {project_id}.{dataset_name}.GENESISAPP_SERVICE_SERVICE;"
        try:
            logger.warning(f"Running query to check endpoints: {query1}")
            results = bb_db_connector.run_query(query1)
            udf_endpoint_url = next((endpoint['ingress_url'] for endpoint in results if endpoint['name'] == 'udfendpoint'), None)
            return udf_endpoint_url
        except Exception as e:
            logger.warning(f"Failed to get UDF endpoint URL with error: {e}")
            return None

    # Retrieve the Slack app configuration tokens
    slack_app_config_token, slack_app_config_refresh_token = get_slack_config_tokens()
    # Rotate the Slack app configuration token
    #slack_app_config_token, slack_app_config_refresh_token = rotate_slack_token(slack_app_config_token, slack_app_config_refresh_token)
    # Save the new Slack app configuration tokens
 #   save_slack_config_tokens(slack_app_config_token, slack_app_config_refresh_token)

    try:
        bots = bb_db_connector.db_get_slack_active_bots(runner_id=runner_id, project_id=project_id, dataset_name=dataset_name, bot_servicing_table=bot_servicing_table)
        for bot in bots:
            bot_id = bot.get('bot_id')
            api_app_id = bot.get('api_app_id')
            auth_url = bot.get('auth_url')
 
            ep = get_udf_endpoint_url()
            logger.warning(f'Endpoint for service: {ep}')

            if ep:
                request_url = f"{new_base_url}/slack/events/{bot_id}"
                redirect_url = f"https://{ep}/slack/events/{bot_id}/install"
            else:
                request_url = f"{new_base_url}/slack/events/{bot_id}"
                redirect_url = f"{new_base_url}/slack/events/{bot_id}/install"

            if api_app_id is not None:
                try:
                    update_slack_bot_endpoint(bot_id, api_app_id, request_url, redirect_url, slack_app_config_token)
                    logger.info(f"Updated endpoints for bot_id: {bot_id} with new base URL: {new_base_url}")
                except:
                    logger.warning(f"Could not update endpoints for bot_id: {bot_id} with new base URL: {new_base_url}")


            
    except Exception as e:
        logger.error(f"Failed to update bot endpoints with error: {e}")
        raise e

def update_slack_bot_endpoint(bot_id, api_app_id, request_url, redirect_url, slack_app_config_token):
    """
    Updates the Slack bot's endpoints.

    Args:
        bot_id (str): The unique identifier for the bot.
        request_url (str): The new request URL for the Slack bot.
        redirect_url (str): The new redirect URL for the Slack bot.
        slack_app_token (str): The Slack app token to authenticate the request.
    """
    # Headers for the Slack API request
    headers = {
        "Authorization": f"Bearer {slack_app_config_token}",
        "Content-Type": "application/json"
    }

    if api_app_id == None:
          logger.info(f"Endpoint not updated -- No api_app_id set for bot_id: {bot_id}")
          return

    # Retrieve the current manifest using the apps.manifest.export API
    export_url = "https://slack.com/api/apps.manifest.export"
    export_payload = {"app_id": api_app_id}
    export_response = requests.post(export_url, headers=headers, json=export_payload)

    # Check for a successful response from apps.manifest.export
    if export_response.status_code == 200 and export_response.json().get("ok"):
        manifest_content = export_response.json().get("manifest")
        logger.info(f"Successfully retrieved manifest for bot_id: {bot_id}")
    else:
        error_message = export_response.json().get("error", "Failed to retrieve manifest due to an unknown error.")
        logger.error(f"Failed to retrieve manifest for bot_id: {bot_id} with error: {error_message}")
        raise Exception(f"Slack API error: {error_message}")

    manifest_content['settings']['event_subscriptions']['request_url'] = request_url
    manifest_content['oauth_config']['redirect_urls'] = [redirect_url]

    update_url = "https://slack.com/api/apps.manifest.update"
    update_payload = {"app_id": api_app_id, }
    update_payload["manifest"] = manifest_content
    update_response = requests.post(update_url, headers=headers, json=update_payload)
    # Check for a successful response
    if update_response.status_code == 200 and update_response.json().get("ok"):
        logger.info(f"Successfully updated endpoints for bot_id: {bot_id}")
    else:
        error_message = update_response.json().get("error", "Failed to update endpoints due to an unknown error.")
        logger.error(f"Failed to update endpoints for bot_id: {bot_id} with error: {error_message}")
        raise Exception(f"Slack API error: {error_message}")


#update_bot_endpoints(new_base_url='https://9942-141-239-172-58.ngrok-free.app',runner_id='jl-local-runner')

