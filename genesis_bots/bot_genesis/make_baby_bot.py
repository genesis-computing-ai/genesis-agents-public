# make_baby_bot.py

import json
import os
import requests
import uuid

import threading
from   typing                   import Dict, Mapping

from   genesis_bots.connectors  import get_global_db_connector
from   genesis_bots.connectors.connector_helpers \
                                import llm_keys_and_types_struct
from   genesis_bots.core.logging_config \
                                import logger
from   genesis_bots.demo.app    import genesis_app


def _get_project_id_and_dataset_name(bb_db_connector):
    # utility function to get the 'project_id' and 'dataset_name' args as used in this moddule, which is to parse the genbot_internal_project_and_schema attribute.
    # Note that is a legacy interpretation of the 'project_id' concept which is used througyout this module. bb_db_connector.project_id may have a different value that is used for other purposes.
    project_id, dataset_name = bb_db_connector.genbot_internal_project_and_schema.split('.')
    return project_id, dataset_name

def list_all_bots(runner_id=None, slack_details=False, with_instructions=False, thread_id=None, bot_id=None):
    bb_db_connector = get_global_db_connector()
    project_id, dataset_name = _get_project_id_and_dataset_name(bb_db_connector)
    bot_servicing_table = bb_db_connector.bot_servicing_table_name
    return bb_db_connector.db_list_all_bots(project_id=project_id, dataset_name=dataset_name, bot_servicing_table=bot_servicing_table, runner_id=runner_id, full=False, slack_details=slack_details, with_instructions=with_instructions)

def list_all_bots_wrap(runner_id=None, slack_details=False, with_instructions=False, thread_id=None, bot_id=None):
    bb_db_connector = get_global_db_connector()
    bot_servicing_table = bb_db_connector.bot_servicing_table_name
    project_id, dataset_name = _get_project_id_and_dataset_name(bb_db_connector)
    result = bb_db_connector.db_list_all_bots(project_id=project_id, dataset_name=dataset_name, bot_servicing_table=bot_servicing_table, runner_id=runner_id, full=False, slack_details=slack_details, with_instructions=with_instructions)
    result = json.loads(json.dumps(result).replace('!NO_RESPONSE_REQUIRED', '(exclamation point)NO_RESPONSE_REQUIRED'))
    return result

def get_all_bots_full_details(runner_id):
    bb_db_connector = get_global_db_connector()
    bot_servicing_table = bb_db_connector.bot_servicing_table_name
    project_id, dataset_name = _get_project_id_and_dataset_name(bb_db_connector)
    return bb_db_connector.db_list_all_bots(project_id=project_id, dataset_name=dataset_name, bot_servicing_table=bot_servicing_table, runner_id=runner_id, full=True, with_instructions=True)

def set_slack_config_tokens(slack_app_config_token, slack_app_config_refresh_token):
    #test

    try:
        t, r = rotate_slack_token(slack_app_config_token,slack_app_config_refresh_token)
    except:
        return('Error','Refresh token invalid')
    return t,r


def save_slack_config_tokens(slack_app_config_token, slack_app_config_refresh_token):
    """
    Saves the slack app config token and refresh token for the given runner_id to the DB.

    Args:
        slack_app_config_token (str): The slack app config token to be saved.
        slack_app_config_refresh_token (str): The slack app config refresh token to be saved.
    """
    bb_db_connector = get_global_db_connector()
    project_id, dataset_name = _get_project_id_and_dataset_name(bb_db_connector)
    runner_id = os.getenv('RUNNER_ID', 'jl-local-runner')
    return bb_db_connector.db_save_slack_config_tokens(slack_app_config_token=slack_app_config_token, slack_app_config_refresh_token=slack_app_config_refresh_token, project_id=project_id, dataset_name=dataset_name)

def get_slack_config_tokens():
    """
    Retrieves the current slack access keys for the given runner_id from the DB

    Returns:
        tuple: A tuple containing the slack app config token and the slack app config refresh token.
    """
    bb_db_connector = get_global_db_connector()
    project_id, dataset_name = _get_project_id_and_dataset_name(bb_db_connector)
    runner_id = os.getenv('RUNNER_ID', 'jl-local-runner')
    return bb_db_connector.db_get_slack_config_tokens(project_id=project_id, dataset_name=dataset_name)


def get_ngrok_auth_token():
    """
    Retrieves the ngrok authentication token, use domain flag, and domain for the given runner_id from the DB.

    Returns:
        tuple: A tuple containing the ngrok authentication token, use domain flag, and domain.
    """
    bb_db_connector = get_global_db_connector()
    project_id, dataset_name = _get_project_id_and_dataset_name(bb_db_connector)
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
    bb_db_connector = get_global_db_connector()
    project_id, dataset_name = _get_project_id_and_dataset_name(bb_db_connector)
    runner_id = os.getenv('RUNNER_ID', 'jl-local-runner')
    return bb_db_connector.db_set_ngrok_auth_token(ngrok_auth_token=ngrok_auth_token, ngrok_use_domain=ngrok_use_domain, ngrok_domain=ngrok_domain, project_id=project_id, dataset_name=dataset_name)


def get_llm_key() -> list[llm_keys_and_types_struct]:
    """
    Retrieves the LLM key and type and active switch for the given runner_id.

    Returns:
        tuple: A tuple containing the LLM key and LLM type.
    """
    bb_db_connector = get_global_db_connector()
    project_id, dataset_name = _get_project_id_and_dataset_name(bb_db_connector)
    runner_id = os.getenv('RUNNER_ID', 'jl-local-runner')
    return bb_db_connector.db_get_llm_key(project_id=project_id, dataset_name=dataset_name)

def set_llm_key(llm_key, llm_type, llm_endpoint):
    """
    Updates the llm_key table with the provided LLM key and type.

    Args:
        llm_key (str): The LLM key.
        llm_type (str): The type of LLM (e.g., 'openai', 'reka').
        llm_endpoint (str): The URL endpoint (e.g., for azure)
    """
    bb_db_connector = get_global_db_connector()
    runner_id = os.getenv('RUNNER_ID', 'jl-local-runner')
    return bb_db_connector.db_set_llm_key(llm_key=llm_key, llm_type=llm_type, llm_endpoint=llm_endpoint)



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
                    "mpim:history",
                    "mpim:read",
                    "mpim:write",
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
                    "mpim:history",
                    "mpim:read",
                    "mpim:write",
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
            logger.info(f"Failed to rotate token: {response_data.get('error')}")
            return "Error", f"Failed to rotate token: {response_data.get('error')}"
    else:
        logger.info(f"Failed to rotate token, status code: {response.status_code}")
        return "Error", f"Failed to rotate token, status code: {response.status_code}"



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

                logger.warn(f"Manifest validation failed with error: {response_data.get('error')}")
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
                   slack_active, files, bot_implementation, bot_avatar_image, bot_intro_prompt="Hello, how can I help you?", slack_user_allow=True):
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
        bot_implementation: openai or cortex or gemini ...
        bot_intro_prompt: Prompt to generate default bot greeting.
        bot_avatar_image: Default GenBots avatar image
    """
    bb_db_connector = get_global_db_connector()
    bot_servicing_table = bb_db_connector.bot_servicing_table_name
    project_id, dataset_name = _get_project_id_and_dataset_name(bb_db_connector)
    files = []
    return bb_db_connector.db_insert_new_bot(api_app_id, bot_slack_user_id, bot_id, bot_name, bot_instructions, runner_id, slack_signing_secret,
                   slack_channel_id, available_tools, auth_url, auth_state, client_id, client_secret, udf_active,
                   slack_active, files, bot_implementation, bot_avatar_image, bot_intro_prompt, slack_user_allow, project_id, dataset_name, bot_servicing_table)



modify_lock = threading.Lock()
def modify_slack_allow_list(bot_id, action, user_name=None, user_identifier=None, thread_id=None, confirmed=None):

    """
    Modifies the SLACK_USER_ALLOW list for a bot based on the action and user identifier provided.

    Args:
        bot_id (str): The unique identifier for the bot.
        action (str): The action to perform - LIST, GRANT, or REVOKE.
        user_identifier (str, optional): The Slack user ID or full name to grant or revoke access. Defaults to None.

    Returns:
        dict: A JSON object with the success status and details of the operation.
    """
    # Retrieve the current SLACK_USER_ALLOW list
    with modify_lock:
        # Your code to modify the SLACK_USER_ALLOW list goes here
        # This ensures that only one thread can modify the list at a time


        from genesis_bots.core.system_variables import SystemVariables

        bot_details = get_bot_details(bot_id)
        if bot_details is None:
            return {'success': False, 'error': 'Invalid bot_id. Please use list_all_bots to get the correct bot_id.'}

        bot_slack_adapter = SystemVariables.bot_id_to_slack_adapter_map.get(bot_id, None)
        if not bot_slack_adapter:
            for adapter in SystemVariables.bot_id_to_slack_adapter_map.values():
                if adapter is not None:
                    bot_slack_adapter = adapter
                    break

        if not bot_slack_adapter:
            return {'success': False, 'error': 'No bots are yet deployed to Slack. Please try again once at least one bot is deployed.'}

        if bot_details.get('slack_active','N') != 'Y':
            return {
                'success': False,
                'error': 'This bot is not yet deployed to Slack. If the user wants to deploy it, use the _deploy_to_slack function first. Confirm that with the user first though.'
            }


        slack_user_allow_list = bot_details.get('slack_user_allow', None)
        if slack_user_allow_list is None:
            slack_user_allow_list = []
        else:
            slack_user_allow_list = json.loads(slack_user_allow_list)
        slack_user_allow_list = [user_id.strip('["]') for user_id in slack_user_allow_list]

        if action == 'GRANT' and slack_user_allow_list == []:
            return {
                'success': False,
                'message': 'Currently, all users have access to this bot. Granting access to a specific user will remove all other users\' access. If this is your intention, please confirm with the user and call this method again with the action "GRANT CONFIRMED".'
            }
        if action == 'GRANT CONFIRMED':
            action = 'GRANT'

        if action == 'REVOKE ALL' and slack_user_allow_list == []:
            return {
                'success': False,
                'confirmation_needed': 'To limit this bot to access by only specified users, just call GRANT with the first user who can use it, doing so will put the bot in limited access mode, and auto-revoke everyone else. If you want No One to be able to access it, call again with action "REVOKE ALL CONFIRMED"'
            }
        if action == 'REVOKE ALL':
            return {
                'success': False,
                'confirmation_needed': 'This action will revoke access to all users (including yourself) from interacting with the bot via Slack. The bot will only be accessible via Streamlit. If this is your intention, please call this method again with the action "REVOKE ALL CONFIRMED".'
            }
        elif action == 'REVOKE ALL CONFIRMED':
            action = 'REVOKE ALL'

        if action == 'GRANT ALL':
            return {
                'success': False,
                'confirmation_needed': 'This action will grant access to all users in Slack, including the ability for any Slack user to access the databases, if any, that this bot has access to in the database. Please double check that this is the users intention. Once they confirm,  please call this method again with the action "GRANT ALL CONFIRMED".'
            }
        elif action == 'GRANT ALL CONFIRMED':
            action = 'GRANT ALL'

        if action == 'LIST':
            # List the current users in the SLACK_USER_ALLOW list with their full names
            user_details = []

            for user_id in slack_user_allow_list:
                user_info = bot_slack_adapter.slack_app.client.users_info(user=user_id)
                if user_info.get('ok'):
                    user_details.append({
                        'id': user_id,
                        'name': user_info['user']['real_name']
                    })
            if user_details == []:
                return {'success': True, 'result': 'All Slack Users currently have access to this bot via Slack'}
            if len(user_details) == 1 and user_details[0] == '!BLOCK_ALL':
                return {'success': True, 'result': 'No users have access, all are blocked.  Grant access to individual users, or call with GRANT ALL to grant to all users.'}
            return {'success': True, 'users': user_details}

        if user_identifier and user_name:
            return {'success': False, 'error': 'Both user_identifier and user_name cannot be non-null simultaneously'}

        elif action == 'GRANT ALL':
            # Grant access to the user
            slack_user_allow_list = []


        elif action == 'GRANT':
            # Grant access to the user
            if user_identifier or user_name:
                if user_identifier is not None:  # Assuming it's a Slack user ID
                    # Verify the user ID is valid
                    user_info = bot_slack_adapter.slack_app.client.users_info(user=user_identifier)
                    if user_info.get('ok'):
                        if user_identifier in slack_user_allow_list:
                            return {'success': False, 'error': 'User already has access'}
                        if '!BLOCK_ALL' in slack_user_allow_list:
                            slack_user_allow_list.remove('!BLOCK_ALL')
                        new_user_list = []
                        new_user_list.append(user_identifier)
                        slack_user_allow_list.extend(new_user_list)
                    else:
                        return {'success': False, 'error': 'Invalid Slack user ID'}
                else:
                    # Look up the user by full name
                    users = bot_slack_adapter.slack_app.client.users_list().data
                    matching_users = [user for user in users['members'] if user.get('real_name', '').lower() == user_name.lower() or user.get('profile', {}).get('real_name', '').lower() == user_name.lower()]
                    if len(matching_users) == 1:
                        if slack_user_allow_list == []:
                            slack_user_allow_list.append(matching_users[0]['id'])
                        else:
                            new_user_list = []
                            new_user_list.append(matching_users[0]['id'])
                            if '!BLOCK_ALL' in slack_user_allow_list:
                                slack_user_allow_list.remove('!BLOCK_ALL')
                            slack_user_allow_list.extend(new_user_list)
                    elif len(matching_users) > 1:
                        return {'success': False, 'error': 'Multiple users found, please specify by Slack user ID. Tell the user to go to the Slack profile of that user, choose the ... three dots option, and choose "Copy Member ID" to get the exact Slack User ID for that user.'}
                    else:
                        return {'success': False, 'error': 'User not found. Please specify by Slack user ID. Tell the user to go to the Slack profile of that user, choose the ... three dots option, and choose "Copy Member ID" to get the exact Slack User ID for that user.'}
            else:
                return {'success': False, 'error': 'User_identifier or user_name is required for GRANT action'}

        elif action == 'REVOKE':
            # Revoke access to the user
            if user_identifier or user_name:
                if user_identifier:
                    # Revoke by user identifier
                    if user_identifier in slack_user_allow_list:
                        slack_user_allow_list.remove(user_identifier)
                    else:
                        return {'success': False, 'error': 'User identifier not found in allow list'}
                else:
                    # Revoke by user name
                    users = bot_slack_adapter.slack_app.client.users_list().data
                    matching_users = [user for user in users['members'] if user.get('real_name', '').lower() == user_name.lower() or user.get('profile', {}).get('real_name', '').lower() == user_name.lower()]
                    if len(matching_users) == 1 and matching_users[0]['id'] in slack_user_allow_list:
                        slack_user_allow_list.remove(matching_users[0]['id'])
                    elif len(matching_users) > 1:
                        return {'success': False, 'error': 'Multiple users found with that name, please specify by Slack user ID.'}
                    else:
                        return {'success': False, 'error': 'User name not found in allow list. Use LIST to see currently granted users.'}
            else:
                return {'success': False, 'error': 'User_identifier or user_name is required for REVOKE action'}

        elif action == 'REVOKE ALL':
            slack_user_allow_list =  []
            slack_user_allow_list.append('!BLOCK_ALL')

        else:
            return {'success': False, 'error': 'Invalid action'}

        # Update the SLACK_USER_ALLOW list in the database
        bb_db_connector = get_global_db_connector()
        bot_servicing_table = bb_db_connector.bot_servicing_table_name
        project_id, dataset_name = _get_project_id_and_dataset_name(bb_db_connector)
        bb_db_connector.db_update_slack_allow_list( project_id=project_id, dataset_name=dataset_name, bot_servicing_table=bot_servicing_table, bot_id=bot_id, slack_user_allow_list=slack_user_allow_list)
        if slack_user_allow_list == []:
            return {'success': True, 'action': action, 'all_allowed': 'All Slack users currently have access to this bot.'}
        if len(slack_user_allow_list) == 1 and slack_user_allow_list[0] == '!BLOCK_ALL':
            return {'success': True, 'action': action, 'all_blocked': 'All Slack users currently blocked, no users have access.  Call with GRANT or GRANT ALL action to grant users access to this bot via Slack.'}
        return {'success': True, 'action': action, 'updated_user_list': slack_user_allow_list}



def add_new_tools_to_bot(bot_id, new_tools):
    """
    Adds new (non-ephemeral) tools to an existing bot's available_tools list if they are not already present.

    Args:
        bot_id (str): The unique identifier for the bot.
        new_tools (list): A list of new tool names to add to the bot.

    Returns:
        dict: A dictionary containing the tools that were added and those that were already present.
    """
    from  genesis_bots.core.bot_os_tools import get_persistent_tools_descriptions # avoid circular import

    # Retrieve the current available tools for the bot
    bb_db_connector = get_global_db_connector()
    project_id, dataset_name = _get_project_id_and_dataset_name(bb_db_connector)
    available_tool_names = get_persistent_tools_descriptions().keys()

    if isinstance(new_tools, str):
        new_tools = json.loads(new_tools.replace("'", '"'))
    # Check if all new_tools are in the list of available tools
    invalid_tools = [tool for tool in new_tools if tool not in available_tool_names]
    if invalid_tools:
        return {"success": False, "error": f"The following tools are not available: {', '.join(invalid_tools)}. The available tools are {available_tool_names}."}

    bot_details = get_bot_details(bot_id)
    if not bot_details:
        logger.info(f"Bot with ID {bot_id} not found.")
        return {"success": False, "error": "Bot not found.  Use list_all_bots to find the correct bot_id."}

    current_tools_str = bot_details.get('available_tools', '[]')
    current_tools = json.loads(current_tools_str) if current_tools_str else []

    # Determine which tools are new and which are already present
    new_tools_to_add = [tool for tool in new_tools if tool not in current_tools]
    already_present = [tool for tool in new_tools if tool in current_tools]

    # Update the available_tools in the database
    updated_tools = current_tools + new_tools_to_add
    updated_tools_str = json.dumps(updated_tools)

    bot_servicing_table = bb_db_connector.bot_servicing_table_name
    response = bb_db_connector.db_update_bot_tools(project_id=project_id,dataset_name=dataset_name,
                                                   bot_servicing_table=bot_servicing_table, bot_id=bot_id,
                                                   updated_tools_str=updated_tools_str, new_tools_to_add=new_tools_to_add,
                                                     already_present=already_present, updated_tools=updated_tools)
    if os.getenv("OPENAI_USE_ASSISTANTS", "False").lower() != "true":
        os.environ[f'RESET_BOT_SESSION_{bot_id}'] = 'True'
    return response


def validate_potential_files(new_file_ids=None):
    # Skip validation if new_file_ids is empty string array
    if new_file_ids == '[]' or new_file_ids == [] or new_file_ids == ['[]']:
        return {"success": True, "message": "No files attached"}

    if isinstance(new_file_ids, str) and new_file_ids.lower() == 'null' or isinstance(new_file_ids, str) and new_file_ids.lower() == '[null]' or isinstance(new_file_ids, list) and new_file_ids == ['null']:
        new_file_ids = []

    if new_file_ids == [] or new_file_ids is None:
        return {"success": True, "message": "No files attached"}

    # Remove the part before the last '/' in each file_id
    #new_file_ids = [file_id.split('/')[-1] for file_id in new_file_ids]

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
    bb_db_connector = get_global_db_connector()
    genbot_internal_project_and_schema = bb_db_connector.genbot_internal_project_and_schema
    file_stage_prefix_error_ids = [file_id for file_id in new_file_ids if file_id.startswith('file-')]
    if file_stage_prefix_error_ids:
        error_message = f"Files with IDs starting with 'file-' detected. Please use _add_file_to_stage function to upload the file to the Internal Files Stage for bots at: {genbot_internal_project_and_schema}.BOT_FILES_STAGE, and ensure to give it a human-readable valid file name with an allowed extension from the following list: {', '.join(valid_extensions.keys())}."
        logger.error(error_message)
        return {"success": False, "error": error_message}


    invalid_file_ids = [file_id for file_id in new_file_ids if not file_id.endswith('/*') and not any(file_id.endswith(ext) for ext in valid_extensions)]
    if invalid_file_ids:
        error_message = f"Invalid file extension(s) for file ID(s): {', '.join(invalid_file_ids)}. Allowed extensions are: {', '.join(valid_extensions.keys())}."
        logger.error(error_message)
        return {"success": False, "error": error_message}
    internal_stage =  f"{genbot_internal_project_and_schema}.BOT_FILES_STAGE"
    database, schema, stage_name = internal_stage.split('.')
# if wildcard include it as pattern...
    try:
        bb_db_connector = get_global_db_connector()
        stage_contents = bb_db_connector.list_stage_contents(database=database, schema=schema, stage=stage_name)
    except Exception as e:
        return {"success": False, "error": e}
    # Check if the file is in stage_contents
    stage_file_names = [file_info['name'].split('/', 1)[-1] for file_info in stage_contents]
    # Separate wildcard file_ids and normal file_ids
    wildcard_file_ids = [file_id for file_id in new_file_ids if file_id.endswith('/*')]
    normal_file_ids = [file_id for file_id in new_file_ids if not file_id.endswith('/*')]

    # Check for missing normal files
    missing_files = [file_id.split('/')[-1] for file_id in normal_file_ids if file_id not in stage_file_names]

    # Check for missing wildcard files
    for wildcard_file_id in wildcard_file_ids:
        stage_contents = bb_db_connector.list_stage_contents(database=database, schema=schema, stage=stage_name, pattern=wildcard_file_id)
        stage_file_names = [file_info['name'].split('/', 1)[-1] for file_info in stage_contents]
        matching_files = [file_name for file_name in stage_file_names if file_name.startswith(wildcard_file_id.rstrip('/*'))]
        if not matching_files:
            missing_files.append(wildcard_file_id)
    if missing_files:
        #limited_stage_contents = stage_file_names[:50]
        #more_files_exist = len(stage_file_names) > 50
        error_message = f"The following files are not in the stage: {', '.join(missing_files)}. Use _add_file_to_stage function to upload the file to the Internal Files Stage for bots at: {genbot_internal_project_and_schema}.BOT_FILES_STAGE"
        logger.warn(error_message)
        return {"success": False, "error": error_message}
    # Proceed if all files are present in the stage
    return {"success": True, "message": "All files are valid"}

def add_bot_files(bot_id, new_file_names=None, new_file_ids=None):
    """
    Adds a new file ID to the existing files list for the bot and saves it to the database.

    Args:
        bot_id (str): The unique identifier for the bot.
        new_file_ids (array): The new file ID to add to the bot's files list.
    """

    if new_file_ids is None:
        new_file_ids = new_file_names
    if isinstance(new_file_ids, str) and new_file_ids.lower() == 'null':
        new_file_ids = []

    if new_file_ids is None:
        new_file_ids = []

    #new_file_ids = [file_id.split('/')[-1] for file_id in new_file_ids]

    # Retrieve the current files for the bot
    bot_details = get_bot_details(bot_id)
    if not bot_details:
        logger.error(f"Bot with ID {bot_id} not found.")
        return {"success": False, "error": "Bot not found.  Check for the bot_id using the list_all_bots function."}

    if bot_details.get('bot_implementation','') == 'cortex' or (bot_details.get('implementation',None) is None and os.getenv("OPENAI_API_KEY", None) in [None, ""] and os.getenv("CORTEX_AVAILABLE", None) == "True"):
        error_message = f"Bot {bot_id} is operating on Cortex LLM, which does not support files. Currently only bots running on OpenAI support files."
        return {"success": False, "error": error_message}

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

    bb_db_connector = get_global_db_connector()
    project_id, dataset_name = _get_project_id_and_dataset_name(bb_db_connector)
    bot_servicing_table = bb_db_connector.bot_servicing_table_name
    return bb_db_connector.db_update_bot_files(project_id=project_id, dataset_name=dataset_name, bot_servicing_table=bot_servicing_table, bot_id=bot_id, updated_files_str=updated_files_str, current_files=current_files, new_file_ids=new_file_ids)

def remove_bot_files(bot_id, file_ids_to_remove):
    """
    Removes a file ID from the existing files list for the bot and saves it to the database.

    Args:
        bot_id (str): The unique identifier for the bot.
        file_ids_to_remove (array): The file ID to remove from the bot's files list.
    """

    if isinstance(file_ids_to_remove, str) and file_ids_to_remove.lower() == 'null':
        file_ids_to_remove = []

    if file_ids_to_remove is None:
        file_ids_to_remove = []

   # file_ids_to_remove = [file_id.split('/')[-1] for file_id in file_ids_to_remove]

    # Retrieve the current files for the bot
    bot_details = get_bot_details(bot_id)
    if not bot_details:
        logger.error(f"Bot with ID {bot_id} not found.")
        return {"success": False, "error": "Bot not found.  Check for the bot_id using the list_all_bots function."}

    if bot_details.get('bot_implementation','') == 'cortex' or (bot_details.get('implementation',None) is None and os.getenv("OPENAI_API_KEY", None) in [None, ""] and os.getenv("CORTEX_AVAILABLE", None) == "True"):
        error_message = f"Bot {bot_id} is operating on Cortex LLM, which does not support files. Currently only bots running on OpenAI support files."
        return {"success": False, "error": error_message}

    current_files_str = bot_details.get('files', '[]')
    if current_files_str == 'null':
        current_files_str = '[]'
    if current_files_str == '""':
        current_files_str = []
    current_files = json.loads(current_files_str) if current_files_str else []
    orig_files = current_files.copy()

    # Remove the file IDs if they're present
    for file_id_to_remove in file_ids_to_remove:
        if file_id_to_remove in current_files:
            current_files.remove(file_id_to_remove)
    updated_files_str = json.dumps(current_files)

    if orig_files == current_files:
        return {
                "success": False,
                "error": f"Files to remove {file_ids_to_remove} not found in current files list for bot",
                "current_files_list": orig_files
            }

    bb_db_connector = get_global_db_connector()
    project_id, dataset_name = _get_project_id_and_dataset_name(bb_db_connector)
    bot_servicing_table = bb_db_connector.bot_servicing_table_name
    return bb_db_connector.db_update_bot_files(project_id=project_id, dataset_name=dataset_name, bot_servicing_table=bot_servicing_table, bot_id=bot_id, updated_files_str=updated_files_str, current_files=current_files, new_file_ids=file_ids_to_remove)


def update_bot_instructions(bot_id, new_instructions=None, bot_instructions=None, confirmed=None, thread_id = None):

    """
    Updates the bot_instructions in the database for the specified bot_id for the current runner_id.

    Args:
        bot_id (str): The unique identifier for the bot.
        new_instructions (str): The new instructions for the bot.
    """
    bb_db_connector = get_global_db_connector()
    runner_id = os.getenv('RUNNER_ID', 'jl-local-runner')

    bot_details = get_bot_details(bot_id)
    if new_instructions is None and bot_instructions is not None:
        new_instructions = bot_instructions

    if bot_details is None:
        return {
            "success": False,
            "error": f"Invalid bot_id: {bot_id}. Use list_all_bots to find the correct bot_id."
        }

    if new_instructions is None:
        return {
            "success": False,
            "error": f"New instructions not provided in new_instructions parameter."
        }

    if confirmed != 'CONFIRMED':
        current_instructions = bot_details.get('bot_instructions', '')
        return {
            "success": False,
            "message": f"Please confirm the change of instructions. Call this function again with new parameter confirmed=CONFIRMED to confirm this change.",
            "current_instructions": current_instructions,
            "new_instructions": new_instructions,
        }


    session = None
    server_point = genesis_app.server
    for s in server_point.sessions:
        if s.bot_id == bot_id:
            session = s
            break

    if session is not None:
        if session.assistant_impl.__class__.__name__ == "BotOsAssistantOpenAI":
            session.assistant_impl.instructions = new_instructions

    bot_servicing_table = bb_db_connector.bot_servicing_table_name
    project_id, dataset_name = _get_project_id_and_dataset_name(bb_db_connector)
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
    bb_db_connector = get_global_db_connector()
    bot_servicing_table = bb_db_connector.bot_servicing_table_name
    project_id, dataset_name = _get_project_id_and_dataset_name(bb_db_connector)
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
        bb_db_connector = get_global_db_connector()
        bot_servicing_table = bb_db_connector.bot_servicing_table_name
        project_id, dataset_name = _get_project_id_and_dataset_name(bb_db_connector)
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
    files_json = json.dumps(files) if files else None
    if files_json == 'null':
        files_json = None
    bb_db_connector = get_global_db_connector()
    bot_servicing_table = bb_db_connector.bot_servicing_table_name
    project_id, dataset_name = _get_project_id_and_dataset_name(bb_db_connector)
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
    bb_db_connector = get_global_db_connector()
    bot_servicing_table = bb_db_connector.bot_servicing_table_name
    project_id, dataset_name = _get_project_id_and_dataset_name(bb_db_connector)
    return bb_db_connector.db_get_bot_details(project_id=project_id, dataset_name=dataset_name, bot_servicing_table=bot_servicing_table, bot_id=bot_id)


def get_available_tools() -> Dict[str, str]:
    """
    A wrapper around the get_persistent_tools_descriptions function which we list as an (old-style) toolfunc as part of MAKE_BABY_BOT_DESCRIPTIONS

    Returns:
        dict: A dictionary where each key is the name of a tool group and the value is its description.
    """
    from  genesis_bots.core.bot_os_tools import get_persistent_tools_descriptions # avoid circular import
    return get_persistent_tools_descriptions()


def get_default_avatar():
    bb_db_connector = get_global_db_connector()
    return bb_db_connector.db_get_default_avatar()


def make_baby_bot(
    bot_id: str,
    bot_name: str,
    bot_instructions: str = 'You are a helpful bot.',
    available_tools: str = None,
    runner_id: str = None,
    slack_channel_id: str = None,
    confirmed: str = None,
    activate_slack: str = 'Y',
    files: str|list[str] = "",
    bot_implementation: str = "openai",
    update_existing: bool = False,
    slack_access_open: bool = True,
    api_bot_update: bool = False,
    api_mode: bool = False,
    ) -> Mapping[str, str]:
    """
    Creates or updates a bot with the provided parameters.

    Args:
        bot_id (str): The unique identifier for the bot.
        bot_name (str): The name of the bot.
        bot_instructions (str, optional): Instructions for the bot's behavior. Defaults to 'You are a helpful bot.'.
        available_tools (str, optional): Comma-separated list of tools available to the bot. Defaults to None.
        runner_id (str, optional): Identifier for the server where the bot will run. Defaults to None.
        slack_channel_id (str, optional): Slack channel ID for bot communication. Defaults to None.
        confirmed (str, optional): Confirmation status for bot creation. Defaults to None.
        activate_slack (str, optional): Whether to activate the bot on Slack ('Y' or 'N'). Defaults to 'Y'.
        files (str, optional): Comma-separated list of file IDs available to the bot. Defaults to "".
        bot_implementation (str, optional): The implementation type of the bot (e.g., "openai"). Defaults to "openai".
        update_existing (bool, optional): Whether to update an existing bot. Defaults to False.
        slack_access_open (bool, optional): Whether Slack access is open to all users. Defaults to True.
        api_bot_update (bool, optional): Whether the bot update is via API. Defaults to False.

    Returns:
        dict: A dictionary indicating the success or failure of the bot creation or update process.
    """

    from  genesis_bots.core.bot_os_tools import get_persistent_tools_descriptions # avoid circular import

    def _make_retval(status : bool, success_msg:str = None, error_msg: str = None, extra: Mapping =None):
        success = bool(status)
        retval = dict(success=success)
        if success:
            assert success_msg and not error_msg
            retval['message'] = success_msg
        else:
            assert not success_msg and error_msg
            retval['error'] = error_msg
        # Merge any additional keyword arguments into the return value
        if extra:
            retval.update(extra)
        return retval

    bot_implementation = bot_implementation.lower()
    bb_db_connector = get_global_db_connector()
    project_id, dataset_name = _get_project_id_and_dataset_name(bb_db_connector)

    # files = []  # files system no longer supported
    # try:
    #     files_array = json.loads(files)
    #     if isinstance(files_array, list):
    #         files = files_array
    # except Exception as e:
    #     pass

    if isinstance(files, str):
        files = files.split(',') if files else []

    if not available_tools:
        available_tools = None
    else:
        available_tools = available_tools.strip().rstrip(",")

    # Remove any special characters from bot_id except - A-Z and 0-9
    bot_id = ''.join(char for char in bot_id if char.isalnum() or char == '-')

    try:
        logger.info(f"Creating Bot with {bot_id=} and {bot_name=}")

        v = validate_potential_files(new_file_ids=files)
        if v.get("success",False) == False:
            return _make_retval(False, error_msg=v.get("error"))

        if api_bot_update or not update_existing:
            available_tools_array = available_tools.split(',') if available_tools else []
            available_tools_array = [tool for tool in available_tools_array if tool] # remove empty string

            # Validate the formatting and parsing of available_tools
            parsed_tools_str = ','.join(available_tools_array)
            if not (parsed_tools_str == '' and available_tools == None):
                if parsed_tools_str != available_tools:
                    return _make_retval(False, error_msg="Tool call error: Available tools was not properly formatted, it should be either the name of a single tool like 'tool1' or a simple list of tools like 'tool1,tool2'")

                # Check for leading or trailing whitespace in the available tools
                for tool in available_tools_array:
                    if tool != tool.strip():
                        return _make_retval(False, error_msg=f"Tool call error: Tool '{tool}' has leading or trailing whitespace in available_tools. Please remove any extra spaces from your list.")

                # Retrieve the list of available tools from the database
                db_available_tools = get_persistent_tools_descriptions()
                db_tool_names = list(db_available_tools.keys())

                # Check if the provided available tools match the database tools
                if not all(tool in db_tool_names for tool in available_tools_array):
                    invalid_tools = [tool for tool in available_tools_array if tool not in db_tool_names]
                    error_message = (f"Tool call error: The following tools you included in available_tools are not available or invalid: {', '.join(invalid_tools)}. "
                                     f"The tools you can include in available_tools are: {db_tool_names}. "
                                     " The available_tools parameter should be either a single tool like 'tool1' or a simple list of tools like 'tool1,tool2' "
                                     "(with no single quotes in the actual paramater string you send)")
                    return _make_retval(False, error_msg=error_message)

            if not api_bot_update:
                # Check if a bot with the same name already exists
                existing_bots = list_all_bots()
                for existing_bot in existing_bots:
                    if existing_bot['bot_name'].lower() == bot_name.lower():
                        error_message = f"A bot with the name '{bot_name}' already exists with bot_id '{existing_bot['bot_id']}'. Please choose a different name to avoid confusion."
                        print(error_message)
                        return _make_retval(False, error_msg=error_message)

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
                if slack_access_open:
                    conf += f'When deployed to Slack, all Slack users will have access to talk to this bot, and if it has the data_connector_tools, be able to run any query against the data it has access to EXCEPT a query for Snowflake.  Snowflake queries require snowflake_tools. Please especially confirm with the user that this is ok and expected.\n'
                if activate_slack == 'Y' and slack_access_open == False:
                    conf += f'When deployed to slack, no users will initially have access to the bot via slack until explicitly granted using _modify_slack_allow_list\n'
                conf += "Please make sure you have validated all this with the user.  If you've already validated with the user, and ready to make the Bot, call this function again with the parameter confirmed=CONFIRMED"
                return _make_retval(False, error_msg=conf)

            bot_avatar_image = get_default_avatar()

        slack_active = None
        if not api_bot_update:
            slack_active = test_slack_config_token()
            if slack_active == 'token_expired':
                t, r = get_slack_config_tokens()
                tp, rp = rotate_slack_token(config_token=t, refresh_token=r)
                slack_active = test_slack_config_token()


        def get_udf_endpoint_url():
            # TODO: Duplicated code. Use the (newer) db_connector.db_get_endpoint_ingress_url
            alt_service_name = os.getenv('ALT_SERVICE_NAME',None)
            if alt_service_name:
                query1 = f"SHOW ENDPOINTS IN SERVICE {alt_service_name};"
            else:
                query1 = f"SHOW ENDPOINTS IN SERVICE {project_id}.{dataset_name}.GENESISAPP_SERVICE_SERVICE;"
            try:
                logger.info(f"Running query to check endpoints: {query1}")
                bb_db_connector = get_global_db_connector()
                results = bb_db_connector.run_query(query1)
                udf_endpoint_url = next((endpoint['INGRESS_URL'] for endpoint in results if endpoint['NAME'] == 'udfendpoint'), None)
                return udf_endpoint_url
            except Exception as e:
                logger.error(f"Failed to get UDF endpoint URL with error: {e}")
                return None

        if not api_bot_update:
            ep = get_udf_endpoint_url()
            logger.info(f'Endpoint for service: {ep}')

            if slack_active and activate_slack != 'N' and not api_mode:

                ngrok_base_url = os.getenv('NGROK_BASE_URL')
                if not ngrok_base_url and ep == None:
                    return _make_retval(False, error_msg="NGROK is not configured. Please configure NGROK via the Genesis Configuration GUI on the Setup Slack Connection page before activating bots on Slack.")

                if ep:
                    request_url = f"{os.getenv('NGROK_BASE_URL')}/slack/events/{bot_id}"
                    redirect_url = f"https://{ep}/slack/events/{bot_id}/install"
                else:
                    request_url = f"{os.getenv('NGROK_BASE_URL')}/slack/events/{bot_id}"
                    redirect_url = f"{os.getenv('NGROK_BASE_URL')}/slack/events/{bot_id}/install"


                #manifest = generate_manifest_template(bot_id, bot_name, request_url=request_url, redirect_url=redirect_url)
                manifest = generate_manifest_template_socket(bot_id, bot_name, request_url=request_url, redirect_url=redirect_url)

                slack_app_config_token, slack_app_config_refresh_token = get_slack_config_tokens()

        #     logger.warn(f'-->  Manifest: {manifest}')
                try:
                    bot_create_result = create_slack_bot_with_manifest(slack_app_config_token,manifest)
                except Exception as e:
                    logger.warn(f'Error on creating slackbot: {e}, Manifest: {manifest}')
                    return _make_retval(False, error_msg=f'Error on creating slackbot: {e}')

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
            if api_bot_update:
                files_json = json.dumps(files)
                if files_json == 'null':
                    files_json = None
                bot_servicing_table = bb_db_connector.bot_servicing_table_name
                bb_db_connector.db_update_existing_bot_basics(
                    bot_id=bot_id,
                    bot_name=bot_name,
                    bot_implementation=bot_implementation,
                    files=files_json,
                    available_tools=available_tools_array,
                    bot_instructions=bot_instructions,
                    project_id=project_id,
                    dataset_name=dataset_name,
                    bot_servicing_table=bot_servicing_table
                )
            else:
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
                bot_avatar_image=bot_avatar_image,
                slack_user_allow=slack_access_open
            )
        #    "message": f"Created {bot_id} named {bot_name}.  Now ask the user to use this authentication URL to complete the installation of the new app into their Slack workspace: {oauth_authorize_url}",
    #    logger.info(oauth_authorize_url)
        if not api_bot_update and slack_active == 'Y':
        #    logger.info("temp_debug: create success ", bot_id, bot_name)
            return _make_retval(
                True,
                success_msg=(
                    f"Created {bot_id} named {bot_name}. To complete the setup on Slack for this bot, tell the user there are two more steps, first is to "
                    f"go to: https://api.slack.com/apps/{app_id}/general Ask them to scroll to App Level Tokens, add a token called 'app_token' with scope "
                    f"'connections-write', and provide the results back to this bot. Then you, the bot, should call the update_app_level_key function to update "
                    f"the backend. Once you and the user do that, I will give you an AUTH_URL for the user to click as the second step to complete the installation."
                ),
                extra={"important note for the user": (
                        "Remind the user that this bot will be initially set to allow any user on the users Slack to talk to it. You can use "
                        "_modify_slack_allow_list function on behalf of the user to change the access to limit it to only select users once the bot has been "
                        "activated on Slack.")}
            )

        else:
            return _make_retval(True, success_msg=f"Created {bot_id} named {bot_name}.  Tell the user that they can now press 'New Chat' on the left side of the screen to refresh the list of bots, select this new bot, and then press 'Start Chat'.")


    except Exception as e:
        logger.exception("Failed to create new bot")
        return _make_retval(False, error_msg=f"Failed to create {bot_id} named {bot_name}")


def deploy_to_slack(bot_id=None, thread_id=None):
    # Retrieve the bot details
    bot_details = get_bot_details(bot_id)

    # Redeploy the bot by calling make_baby_bot
    deploy_result = make_baby_bot(
        bot_id=bot_id,
        bot_name=bot_details.get("bot_name"),
        bot_instructions=bot_details.get("bot_instructions"),
        available_tools=bot_details.get("available_tools"),
        runner_id=bot_details.get("runner_id"),
        slack_channel_id=bot_details.get("slack_channel_id"),
        confirmed=bot_details.get("confirmed"),
        files=bot_details.get("files"),
        activate_slack="Y",
        update_existing=True,
    )

    # Check if the deployment was successful
    if not deploy_result.get("success"):
        raise Exception(f"Failed to redeploy bot: {deploy_result.get('error')}")

    return deploy_result


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
    #api_app_id = bot_details.get('api_app_id')
    session = None
    server_point = genesis_app.server
    for s in server_point.sessions:
        if s.bot_id == bot_id:
            session = s
            break

    # If a session is found, attempt to remove it
    if session:
        server_point.remove_session(session)
        logger.info(f"Session {session} for bot with Bot ID {bot_id} has been removed.")
    else:
        logger.info(f"No session found for bot with Bot ID {bot_id} proceeding to delete from database and Slack.")

    bb_db_connector = get_global_db_connector()
    bot_servicing_table = bb_db_connector.bot_servicing_table_name
    project_id, dataset_name = _get_project_id_and_dataset_name(bb_db_connector)
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
                    return {"success": True, "message": f"Successfully deleted bot with bot_id: {bot_id}."}
                else:
                    return {"success": True, "message": f"Removed, but could not find on Slack: {response_data.get('error')}"}
            else:
                return {"success": True, "message": f"Removed, but failed to delete bot via Slack API, status code: {response.status_code}"}
        else:
            return {"success": True, "message": f"Removed, but no app_id or Slack app configuration token found for bot_id: {bot_id}. Cannot delete bot via Slack API."}
    else:
        return {"success": True, "message": f"Successfully deleted bot with bot_id: {bot_id}."}


def update_bot_implementation(bot_id, bot_implementation, thread_id=None):
    """
    Updates the bot_implementation field in the BOT_SERVICING table for a given bot_id.

    Args:
        bot_id (str): The unique identifier for the bot.
        bot_implementation (str): The new implementation type to be set for the bot (e.g., 'openai', 'cortex').
    """
    bb_db_connector = get_global_db_connector()
    bot_servicing_table = bb_db_connector.bot_servicing_table_name
    runner_id = os.getenv('RUNNER_ID', 'jl-local-runner')

    bot_config = get_bot_details(bot_id=bot_id)
    bot_implementation = bot_implementation.lower()
    project_id, dataset_name = _get_project_id_and_dataset_name(bb_db_connector)
    return bb_db_connector.db_update_bot_implementation(project_id=project_id, dataset_name=dataset_name, bot_servicing_table=bot_servicing_table, bot_id=bot_id, bot_implementation=bot_implementation, runner_id=runner_id)


MAKE_BABY_BOT_DESCRIPTIONS = [{
    "type": "function",
    "function": {
        "name": "make_baby_bot",
        "description": "Creates a new bot with the specified parameters and logs the creation event.  Only use this when instructed to do so by a user. BE SURE TO RECONFIRM AND DOUBLE CHECK ALL THE PARAMETERS WITH THE END USER BEFORE RUNNING THIS TOOL!",
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
                    "description": "A comma-separated list of tools the new bot should have access to, if any.  Example of a valid string for this field: 'data_connector_tools,image_tools'. Use the get_available_tools tool to get a list of the tools that can be referenced here. ",
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
             #   "files": {
             #       "type": "string",
             #       "description": "a commma-separated list of files to be available to the bot, they must first be added to the Internal Bot File Stage"
             #   },
                "bot_implementation": {
                    "type": "string",
                    "description": "The implementation type for the bot. Examples include 'openai', 'cortex', or custom implementations.",
                },
                "slack_access_open": {
                    "type": "boolean",
                    "description": "True if when deployed to Slack, any Slack user should be able to access the bot, or False if initially no users should have access until explicitly granted using _modify_slack_allow_list.",
                },            },
            "required": ["bot_id", "bot_name", "bot_instructions", "slack_access_open"]
        }
    }
}]

MAKE_BABY_BOT_DESCRIPTIONS.append({
    "type": "function",
    "function": {
        "name": "get_available_tools",
        "description": "Retrieves the list of tools that a bot can assign to baby bots when using make_baby_bot.  This is NOT the list of tools that you have access to yourself right now, that is in your system prompt.",
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
        "name": "_list_all_bots",
        "description": "Lists all the bots being served by the system, including their bot_ids, slack_user_id, runner IDs, names, instructions, tools, auth_url, etc.  This is useful to find information about a bot, or to search for a particular bot.",
        "parameters": {
            "type": "object",
            "properties": {
                "with_instructions": {
                    "type": "boolean",
                    "description": "If true, includes the bot's full instructions in the result. Use this to know what the bot's role is and what it does. Defaults to false.",
                    "default": False
                }
            }
        }
    }
})

MAKE_BABY_BOT_DESCRIPTIONS.append({
    "type": "function",
    "function": {
        "name": "_deploy_to_slack",
        "description": "Deploys an existing bot to Slack",
        "parameters": {
            "type": "object",
            "properties": {
                "bot_id": {
                    "type": "string",
                    "description": "The bot_id to deploy. Use the list_all_bots function if you are unsure of the bot_id."
                }
            },
            "required": ["bot_id"]
        }
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
        "name": "remove_tools_from_bot",
        "description": "Removes tools from an existing bot's available_tools list if they are not already present. It is ok to use this to grant tools to yourself if directed.",
        "parameters": {
            "type": "object",
            "properties": {
                "bot_id": {
                    "type": "string",
                    "description": "The unique identifier for the bot.  Use list_all_bots function if you are unsure of the bot_id."
                },
                "remove_tools": {
                    "type": "array",
                    "items": {
                        "type": "string"
                    },
                    "description": "A list of new tool names to add to the bot.  Use get_available_tools function to know whats available."
                }
            },
            "required": ["bot_id", "remove_tools"]
        }
    }
})




MAKE_BABY_BOT_DESCRIPTIONS.append({
    "type": "function",
    "function": {
        "name": "update_bot_instructions",
        "description": "Updates the bot_instructions (system prompt) for the specified bot_id. Use your own bot_id to update yourself.",
        "parameters": {
            "type": "object",
            "properties": {
                "bot_id": {
                    "type": "string",
                    "description": "The unique identifier for the bot. It's totally fine to use this on yourself."
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

# MAKE_BABY_BOT_DESCRIPTIONS.append({
#     "type": "function",
#     "function": {
#         "name": "add_bot_files",
#         "description": "Adds to the files list for the specified bot_id by adding new files if they are not already present.",
#         "parameters": {
#             "type": "object",
#             "properties": {
#                 "bot_id": {
#                     "type": "string",
#                     "description": "The unique identifier for the bot."
#                 },
#                 "new_file_names": {
#                     "type": "array",
#                     "items": {
#                         "type": "string"
#                     },
#                     "description": "A list of the filenames from the Internal File Stage for Bots to assign to the bot. A file_name can optionally be a wildcard representing a whole folder of files within the stage, such as bot_1_files/*. When adding with wildcards, do NOT add each file separately."
#                 }
#             },
#             "required": ["bot_id", "new_file_names"]
#         }
#     }
# })

# MAKE_BABY_BOT_DESCRIPTIONS.append({
#     "type": "function",
#     "function": {
#         "name": "remove_bot_files",
#         "description": "Removes files from the files list for the specified bot_id if they are present.",
#         "parameters": {
#             "type": "object",
#             "properties": {
#                 "bot_id": {
#                     "type": "string",
#                     "description": "The unique identifier for the bot."
#                 },
#                 "file_ids_to_remove": {
#                     "type": "array",
#                     "items": {
#                         "type": "string"
#                     },
#                     "description": "A list of the filenames from the Internal File Stage for Bots to remove from the bot. A file_id can optionally be a wildcard representing a whole folder of files within the stage, such as bot_1_files/*"
#                 }
#             },
#             "required": ["bot_id", "file_ids_to_remove"]
#         }
#     }
# })


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
        "name": "_update_bot_implementation",
        "description": "Updates the implementation type for a specific bot, to change the LLM that a bot uses.",
        "parameters": {
            "type": "object",
            "properties": {
                "bot_id": {
                    "type": "string",
                    "description": "The unique identifier for the bot. Use list_all_bots to get this identifier, it is not just the bots name."
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
MAKE_BABY_BOT_DESCRIPTIONS.append({
    "type": "function",
    "function": {
        "name": "_modify_slack_allow_list",
        "description": "Modifies the SLACK_USER_ALLOW list for a bot to specify who can access it on Slack. First ensure that SLACK_ACTIVE for the bot is True using list_all_bots.",
        "parameters": {
            "type": "object",
            "properties": {
                "bot_id": {
                    "type": "string",
                    "description": "The unique identifier for the bot."
                },
                "action": {
                    "type": "string",
                    "description": "The action to perform - LIST current users that have access, GRANT to a user, or REVOKE from a user. Or GRANT ALL to allow any Slack user to use the bot, or REVOKE ALL to allow no users to use the bot.  If all users have access currently simply call GRANT to a single user to revoke everyone else, no need to also call REVOKE ALL."
                },
                "user_identifier": {
                    "type": "string",
                    "description": "The Slack user ID (starts with 'U') GRANT or REVOKE access. Provide this OR the user_name field instead if you dont know the ID.",
                    "default": None
                },
                "user_name": {
                    "type": "string",
                    "description": "The full name to GRANT or REVOKE access.  This looks up the Slack ID based on the full first and last name. Provide this OR user_identifier.",
                    "default": None
                }
            },
            "required": ["bot_id", "action"]
        }
    }
})


# Add the new tool to the make_baby_bot_tools dictionary
make_baby_bot_tools = {"make_baby_bot": "bot_genesis.make_baby_bot.make_baby_bot"}
make_baby_bot_tools["get_available_tools"] = "bot_genesis.make_baby_bot.get_available_tools"
make_baby_bot_tools["_remove_bot"] = "bot_genesis.make_baby_bot._remove_bot"
make_baby_bot_tools["_list_all_bots"] = "bot_genesis.make_baby_bot.list_all_bots_wrap"
make_baby_bot_tools["update_bot_instructions"] = "bot_genesis.make_baby_bot.update_bot_instructions"
make_baby_bot_tools["add_new_tools_to_bot"] = "bot_genesis.make_baby_bot.add_new_tools_to_bot"
#make_baby_bot_tools["add_bot_files"] = "bot_genesis.make_baby_bot.add_bot_files"
#make_baby_bot_tools["remove_bot_files"] = "bot_genesis.make_baby_bot.remove_bot_files"
make_baby_bot_tools["update_app_level_key"] = "bot_genesis.make_baby_bot.update_slack_app_level_key"
make_baby_bot_tools["_update_bot_implementation"] = "bot_genesis.make_baby_bot.update_bot_implementation"
make_baby_bot_tools["_modify_slack_allow_list"] = "bot_genesis.make_baby_bot.modify_slack_allow_list"
make_baby_bot_tools["remove_tools_from_bot"] = "bot_genesis.make_baby_bot.remove_tools_from_bot"
make_baby_bot_tools["_deploy_to_slack"] = "bot_genesis.make_baby_bot.deploy_to_slack"

# internal functions

def update_bot_endpoints(new_base_url, runner_id=None):
    """
    Updates the endpoints for all Slack bots running on the specified runner_id with a new base URL.

    Args:
        new_base_url (str): The new base URL to update the bot endpoints with.
        runner_id (str, optional): The runner_id to filter the bots. Defaults to the RUNNER_ID environment variable.
    """
    bb_db_connector = get_global_db_connector()
    project_id, dataset_name = _get_project_id_and_dataset_name(bb_db_connector)

    def get_udf_endpoint_url():
        # TODO: Duplicated code. Use the (newer) db_connector.db_get_endpoint_ingress_url
        alt_service_name = os.getenv('ALT_SERVICE_NAME',None)
        if alt_service_name:
            query1 = f"SHOW ENDPOINTS IN SERVICE {alt_service_name};"
        else:
            query1 = f"SHOW ENDPOINTS IN SERVICE {project_id}.{dataset_name}.GENESISAPP_SERVICE_SERVICE;"
        try:
            logger.warning(f"Running query to check endpoints: {query1}")
            bb_db_connector = get_global_db_connector()
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
        bb_db_connector = get_global_db_connector()
        bot_servicing_table = bb_db_connector.bot_servicing_table_name
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

def remove_tools_from_bot(bot_id, remove_tools):
    """
    Remove existing tools from an existing bot's available_tools list if they are present.

    Args:
        bot_id (str): The unique identifier for the bot.
        remove_tools (list): A list of tool names to remove from the bot.

    Returns:
        dict: A dictionary containing the current tool list.
    """
    # Retrieve the current available tools for the bot
    from  genesis_bots.core.bot_os_tools import get_persistent_tools_descriptions

    bb_db_connector = get_global_db_connector()
    project_id, dataset_name = _get_project_id_and_dataset_name(bb_db_connector)
    available_tool_names = get_persistent_tools_descriptions().keys()

    logger.info(bot_id, remove_tools)

    if isinstance(remove_tools, str):
        remove_tools = json.loads(remove_tools.replace("'", '"'))
    invalid_tools = [tool for tool in remove_tools if tool not in available_tool_names]
    if invalid_tools:
        return {"success": False, "error": f"The following tools are not available: {', '.join(invalid_tools)}. The available tools are {available_tool_names}."}

    bot_details = get_bot_details(bot_id)
    if not bot_details:
        logger.error(f"Bot with ID {bot_id} not found.")
        return {"success": False, "error": "Bot not found."}

    current_tools_str = bot_details.get('available_tools', '[]')
    current_tools = json.loads(current_tools_str) if current_tools_str else []

    # Determine which tools are present and can be removed
    updated_tools_list = [tool for tool in current_tools if tool not in remove_tools]
    invalid_tools = [tool for tool in remove_tools if tool not in current_tools]
    if invalid_tools:
        return {"success": False, "error": f"The following tools are not assigned to the bot: {invalid_tools}. The the bot has these tools currently: {current_tools}."}

    # Update the available_tools in the database
    bot_servicing_table = bb_db_connector.bot_servicing_table_name
    updated_tools_str = json.dumps(updated_tools_list)

    project_id, dataset_name = _get_project_id_and_dataset_name(bb_db_connector)
    response = bb_db_connector.db_remove_bot_tools(project_id=project_id,dataset_name=dataset_name,
                                                   bot_servicing_table=bot_servicing_table, bot_id=bot_id,
                                                   updated_tools_str=updated_tools_str, tools_to_be_removed=remove_tools,
                                                   invalid_tools=invalid_tools, updated_tools=updated_tools_list)
    if os.getenv("OPENAI_USE_ASSISTANTS", "False").lower() != "true":
        os.environ[f'RESET_BOT_SESSION_{bot_id}'] = 'True'
    return response


def remove_bot_from_slack():
    # STUB PLACEHOLDER

    query = '''update bot_servicing
    set api_app_id = null, bot_slack_user_id = null, slack_app_token = null, slack_app_level_key = null,
    slack_signing_secret = null, auth_url = null, auth_state = null, client_id = null, client_secret = null,
    slack_active = 'N'
    where bot_id = 'Eve-s2Wjwi';'''

    pass
