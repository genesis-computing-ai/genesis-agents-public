from snowflake.connector import connect, SnowflakeConnection

import os
import json
from genesis_bots.core.bot_os_llm import BotLlmEngineEnum
from genesis_bots.core.logging_config import logger

def db_insert_new_bot(
    self,
    api_app_id,
    bot_slack_user_id,
    bot_id,
    bot_name,
    bot_instructions,
    runner_id,
    slack_signing_secret,
    slack_channel_id,
    available_tools,
    auth_url,
    auth_state,
    client_id,
    client_secret,
    udf_active,
    slack_active,
    files,
    bot_implementation,
    bot_avatar_image,
    bot_intro_prompt,
    slack_user_allow,
    project_id,
    dataset_name,
    bot_servicing_table,
):
    """
    Inserts a new bot configuration into the BOT_SERVICING table.

    Args:
        api_app_id (str): The API application ID for the bot.
        bot_slack_user_id (str): The Slack user ID for the bot.
        bot_id (str): The unique identifier for the bot.
        bot_name (str): The name of the bot.
        bot_instructions (str): Instructions for the bot's operation.
        runner_id (str): The identifier for the runner that will manage this bot.
        slack_signing_secret (str): The Slack signing secret for the bot.
        slack_channel_id (str): The Slack channel ID where the bot will operate.
        available_tools (json): A JSON of tools the bot has access to.
        files (json): A JSON of files to include with the bot.
        bot_implementation (str): cortex or openai or ...
        bot_intro_prompt: Default prompt for a bot introductory greeting
        bot_avatar_image: Default GenBots avatar image
    """

    insert_query = f"""
        INSERT INTO {bot_servicing_table} (
            api_app_id, bot_slack_user_id, bot_id, bot_name, bot_instructions, runner_id,
            slack_signing_secret, slack_channel_id, available_tools, auth_url, auth_state, client_id, client_secret, udf_active, slack_active,
            files, bot_implementation, bot_intro_prompt, bot_avatar_image
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
    """

    available_tools_string = json.dumps(available_tools)
    files_string = json.dumps(files) if files else ''

    # validate certain params
    bot_implementation = BotLlmEngineEnum(bot_implementation).value if bot_implementation else None

    try:
        cursor = self.connection.cursor()
        cursor.execute(
            insert_query,
            (
                api_app_id,
                bot_slack_user_id,
                bot_id,
                bot_name,
                bot_instructions,
                runner_id,
                slack_signing_secret,
                slack_channel_id,
                available_tools_string,
                auth_url,
                auth_state,
                client_id,
                client_secret,
                udf_active,
                slack_active,
                files_string,
                bot_implementation,
                bot_intro_prompt,
                bot_avatar_image,
            ),
        )
        self.connection.commit()
        logger.info(f"Successfully inserted new bot configuration for bot_id: {bot_id}")

        if not slack_user_allow:
            if self.source_name.lower() == "snowflake":
                slack_user_allow_update_query = f"""
                    UPDATE {bot_servicing_table}
                    SET slack_user_allow = parse_json(%s)
                    WHERE upper(bot_id) = upper(%s)
                    """
            else:
                slack_user_allow_update_query = f"""
                    UPDATE {bot_servicing_table}
                    SET slack_user_allow = %s
                    WHERE upper(bot_id) = upper(%s)
                    """
            slack_user_allow_value = '["!BLOCK_ALL"]'
            try:
                cursor.execute(
                    slack_user_allow_update_query, (slack_user_allow_value, bot_id)
                )
                self.connection.commit()
                logger.info(
                    f"Updated slack_user_allow for bot_id: {bot_id} to block all users."
                )
            except Exception as e:
                logger.info(
                    f"Failed to update slack_user_allow for bot_id: {bot_id} with error: {e}"
                )
                raise e

    except Exception as e:
        logger.info(
            f"Failed to insert new bot configuration for bot_id: {bot_id} with error: {e}"
        )
        raise e

def db_update_bot_tools(
    self,
    project_id=None,
    dataset_name=None,
    bot_servicing_table=None,
    bot_id=None,
    updated_tools_str=None,
    new_tools_to_add=None,
    already_present=None,
    updated_tools=None,
):
    from ...core import global_flags
    # Query to update the available_tools in the database
    update_query = f"""
        UPDATE {bot_servicing_table}
        SET available_tools = %s
        WHERE upper(bot_id) = upper(%s)
    """

    # Execute the update query
    try:
        cursor = self.connection.cursor()
        cursor.execute(update_query, (updated_tools_str, bot_id))
        self.connection.commit()
        logger.info(f"Successfully updated available_tools for bot_id: {bot_id}")

        if "SNOWFLAKE_TOOLS" in updated_tools_str.upper():
            # TODO JD - Verify this change ^^
            workspace_schema_name = f"{global_flags.project_id}.{bot_id.replace(r'[^a-zA-Z0-9]', '_').replace('-', '_')}_WORKSPACE".upper()
            self.create_bot_workspace(workspace_schema_name)
            self.grant_all_bot_workspace(workspace_schema_name)
            # TODO add instructions?

        return {
            "success": True,
            "added": new_tools_to_add,
            "already_present": already_present,
            "all_bot_tools": updated_tools,
        }

    except Exception as e:
        logger.error(f"Failed to add new tools to bot_id: {bot_id} with error: {e}")
        return {"success": False, "error": str(e)}

def db_update_bot_files(
    self,
    project_id=None,
    dataset_name=None,
    bot_servicing_table=None,
    bot_id=None,
    updated_files_str=None,
    current_files=None,
    new_file_ids=None,
):
    # Query to update the files in the database
    update_query = f"""
        UPDATE {bot_servicing_table}
        SET files = %s
        WHERE upper(bot_id) = upper(%s)
    """
    # Execute the update query
    try:
        cursor = self.connection.cursor()
        cursor.execute(update_query, (updated_files_str, bot_id))
        self.connection.commit()
        logger.info(f"Successfully updated files for bot_id: {bot_id}")

        return {
            "success": True,
            "message": f"File IDs {json.dumps(new_file_ids)} added to or removed from bot_id: {bot_id}.",
            "current_files_list": current_files,
        }

    except Exception as e:
        logger.error(
            f"Failed to add or remove new file to bot_id: {bot_id} with error: {e}"
        )
        return {"success": False, "error": str(e)}

def db_update_slack_app_level_key(
    self, project_id, dataset_name, bot_servicing_table, bot_id, slack_app_level_key
):
    """
    Updates the SLACK_APP_LEVEL_KEY field in the BOT_SERVICING table for a given bot_id.

    Args:
        project_id (str): The project identifier.
        dataset_name (str): The dataset name.
        bot_servicing_table (str): The bot servicing table name.
        bot_id (str): The unique identifier for the bot.
        slack_app_level_key (str): The new Slack app level key to be set for the bot.

    Returns:
        dict: A dictionary with the result of the operation, indicating success or failure.
    """
    update_query = f"""
        UPDATE {bot_servicing_table}
        SET SLACK_APP_LEVEL_KEY = %s
        WHERE upper(bot_id) = upper(%s)
    """

    # Execute the update query
    try:
        cursor = self.connection.cursor()
        cursor.execute(update_query, (slack_app_level_key, bot_id))
        self.connection.commit()
        logger.info(
            f"Successfully updated SLACK_APP_LEVEL_KEY for bot_id: {bot_id}"
        )

        return {
            "success": True,
            "message": f"SLACK_APP_LEVEL_KEY updated for bot_id: {bot_id}.",
        }

    except Exception as e:
        logger.error(
            f"Failed to update SLACK_APP_LEVEL_KEY for bot_id: {bot_id} with error: {e}"
        )
        return {"success": False, "error": str(e)}

def db_update_bot_instructions(
    self,
    project_id,
    dataset_name,
    bot_servicing_table,
    bot_id,
    instructions,
    runner_id,
):

    # Query to update the bot instructions in the database
    update_query = f"""
        UPDATE {bot_servicing_table}
        SET bot_instructions = %s
        WHERE upper(bot_id) = upper(%s) AND runner_id = %s
    """

    # Execute the update query
    try:
        cursor = self.connection.cursor()
        cursor.execute(update_query, (instructions, bot_id, runner_id))
        self.connection.commit()
        logger.info(f"Successfully updated bot_instructions for bot_id: {bot_id}")
        bot_details = self.db_get_bot_details(
            project_id, dataset_name, bot_servicing_table, bot_id
        )

        return {
            "success": True,
            "Message": f"Successfully updated bot_instructions for bot_id: {bot_id}.",
            "new_instructions": instructions,
            "new_bot_details": bot_details,
        }

    except Exception as e:
        logger.error(
            f"Failed to update bot_instructions for bot_id: {bot_id} with error: {e}"
        )
        return {"success": False, "error": str(e)}

def db_update_bot_implementation(
    self,
    project_id,
    dataset_name,
    bot_servicing_table,
    bot_id,
    bot_implementation,
    runner_id,
    thread_id = None):
    """
    Updates the implementation type for a specific bot in the database.

    Args:
        project_id (str): The project ID where the bot servicing table is located.
        dataset_name (str): The dataset name where the bot servicing table is located.
        bot_servicing_table (str): The name of the table where bot details are stored.
        bot_id (str): The unique identifier for the bot.
        bot_implementation (str): The new implementation type to be set for the bot.
        runner_id (str): The runner ID associated with the bot.

    Returns:
        dict: A dictionary with the result of the operation, indicating success or failure.
    """

    # validate inputs
    bot_implementation = BotLlmEngineEnum(bot_implementation).value if bot_implementation else None

    # Query to update the bot implementation in the database
    update_query = f"""
        UPDATE {bot_servicing_table}
        SET bot_implementation = %s
        WHERE upper(bot_id) = upper(%s) AND runner_id = %s
    """

    # Check if bot_id is valid
    valid_bot_query = f"""
        SELECT COUNT(*)
        FROM {bot_servicing_table}
        WHERE upper(bot_id) = upper(%s)
    """
    try:
        cursor = self.connection.cursor()
        cursor.execute(valid_bot_query, (bot_id,))
        result = cursor.fetchone()
        if result[0] == 0:
            return {
                "success": False,
                "error": f"Invalid bot_id: {bot_id}. Please use list_all_bots to get the correct bot_id."
            }
    except Exception as e:
        logger.error(f"Error checking bot_id validity for bot_id: {bot_id} with error: {e}")
        return {"success": False, "error": str(e)}

    # Execute the update query
    try:
        cursor = self.connection.cursor()
        res = cursor.execute(update_query, (bot_implementation, bot_id, runner_id))
        self.connection.commit()
        result = cursor.fetchone()
        if result[0] == 0 and result[1] == 0:
            return {
                "success": False,
                "error": f"No bots found to update.  Possibly wrong bot_id. Please use list_all_bots to get the correct bot_id."
            }
        logger.info(f"Successfully updated bot_implementation for bot_id: {bot_id} to {bot_implementation}")

        # trigger the changed bot to reload its session
        os.environ[f'RESET_BOT_SESSION_{bot_id}'] = 'True'
        return {
            "success": True,
            "message": f"bot_implementation updated for bot_id: {bot_id}.",
        }

    except Exception as e:
        logger.error(
            f"Failed to update bot_implementation for bot_id: {bot_id} with error: {e}"
        )
        return {"success": False, "error": str(e)}

def db_update_slack_allow_list(
    self,
    project_id,
    dataset_name,
    bot_servicing_table,
    bot_id,
    slack_user_allow_list,
    thread_id=None,
):
    """
    Updates the SLACK_USER_ALLOW list for a bot in the database.

    Args:
        bot_id (str): The unique identifier for the bot.
        slack_user_allow_list (list): The updated list of Slack user IDs allowed for the bot.

    Returns:
        dict: A dictionary with the result of the operation, indicating success or failure.
    """

    # Query to update the SLACK_USER_ALLOW list in the database
    if self.source_name.lower() == "snowflake":
        update_query = f"""
            UPDATE {bot_servicing_table}
            SET slack_user_allow = parse_json(%s)
            WHERE upper(bot_id) = upper(%s)
            """
    else:
        update_query = f"""
            UPDATE {bot_servicing_table}
            SET slack_user_allow = %s
            WHERE upper(bot_id) = upper(%s)
            """

    # Convert the list to a format suitable for database storage (e.g., JSON string)
    slack_user_allow_list_str = json.dumps(slack_user_allow_list)
    if slack_user_allow_list == []:
        update_query = f"""
        UPDATE {bot_servicing_table}
        SET SLACK_USER_ALLOW = null
        WHERE upper(bot_id) = upper(%s)
           """

    # Execute the update query
    try:
        cursor = self.connection.cursor()
        if slack_user_allow_list != []:
            cursor.execute(update_query, (slack_user_allow_list_str, bot_id))
        else:
            cursor.execute(update_query, (bot_id))
        self.connection.commit()
        logger.info(
            f"Successfully updated SLACK_USER_ALLOW list for bot_id: {bot_id}"
        )

        return {
            "success": True,
            "message": f"SLACK_USER_ALLOW list updated for bot_id: {bot_id}.",
        }

    except Exception as e:
        logger.error(
            f"Failed to update SLACK_USER_ALLOW list for bot_id: {bot_id} with error: {e}"
        )
        return {"success": False, "error": str(e)}

def db_get_bot_access(self, bot_id):

    # Query to select bot access list
    select_query = f"""
        SELECT slack_user_allow
        FROM {self.bot_servicing_table_name}
        WHERE upper(bot_id) = upper(%s)
    """

    try:
        cursor = self.connection.cursor()
        cursor.execute(select_query, (bot_id,))
        result = cursor.fetchone()
        cursor.close()
        if result:
            # Assuming the result is a tuple, we convert it to a dictionary using the column names
            columns = [desc[0].lower() for desc in cursor.description]
            bot_details = dict(zip(columns, result))
            return bot_details
        else:
            logger.error(f"No details found for bot_id: {bot_id}")
            return None
    except Exception as e:
        logger.exception(
            f"Failed to retrieve details for bot_id: {bot_id} with error: {e}"
        )
        return None

def db_get_bot_details(self, project_id, dataset_name, bot_servicing_table, bot_id):
    """
    Retrieves the details of a bot based on the provided bot_id from the BOT_SERVICING table.

    Args:
        bot_id (str): The unique identifier for the bot.

    Returns:
        dict: A dictionary containing the bot details if found, otherwise None.
    """

    # Query to select the bot details
    select_query = f"""
        SELECT *
        FROM {bot_servicing_table}
        WHERE upper(bot_id) = upper(%s)
    """

    try:
        cursor = self.connection.cursor()
        # logger.info(select_query, bot_id)

        cursor.execute(select_query, (bot_id,))
        result = cursor.fetchone()
        cursor.close()
        if result:
            # Assuming the result is a tuple, we convert it to a dictionary using the column names
            columns = [desc[0].lower() for desc in cursor.description]
            bot_details = dict(zip(columns, result))
            return bot_details
        else:
            logger.info(f"No details found for bot_id: {bot_id} in {bot_servicing_table}")
            return None
    except Exception as e:
        logger.exception(
            f"Failed to retrieve details for bot_id: {bot_id} with error: {e}"
        )
        return None

def db_get_bot_database_creds(self, project_id, dataset_name, bot_servicing_table, bot_id):
    """
    Retrieves the database credentials for a bot based on the provided bot_id from the BOT_SERVICING table.

    Args:
        bot_id (str): The unique identifier for the bot.

    Returns:
        dict: A dictionary containing the bot details if found, otherwise None.
    """

    # Query to select the bot details
    select_query = f"""
        SELECT bot_id, database_credentials

                    FROM {bot_servicing_table}
        WHERE upper(bot_id) = upper(%s)
    """

    try:
        cursor = self.connection.cursor()
        # logger.info(select_query, bot_id)

        cursor.execute(select_query, (bot_id,))
        result = cursor.fetchone()
        cursor.close()
        if result:
            # Assuming the result is a tuple, we convert it to a dictionary using the column names
            columns = [desc[0].lower() for desc in cursor.description]
            bot_details = dict(zip(columns, result))
            return bot_details
        else:
            logger.error(f"No details found for bot_id: {bot_id}")
            return None
    except Exception as e:
        logger.exception(
            f"Failed to retrieve details for bot_id: {bot_id} with error: {e}"
        )
        return None

def db_update_existing_bot(
    self,
    api_app_id,
    bot_id,
    bot_slack_user_id,
    client_id,
    client_secret,
    slack_signing_secret,
    auth_url,
    auth_state,
    udf_active,
    slack_active,
    files,
    bot_implementation,
    project_id,
    dataset_name,
    bot_servicing_table,
):
    """
    Updates an existing bot configuration in the BOT_SERVICING table with new values for the provided parameters.

    Args:
        bot_id (str): The unique identifier for the bot.
        bot_slack_user_id (str): The Slack user ID for the bot.
        client_id (str): The client ID for the bot.
        client_secret (str): The client secret for the bot.
        slack_signing_secret (str): The Slack signing secret for the bot.
        auth_url (str): The authorization URL for the bot.
        auth_state (str): The authorization state for the bot.
        udf_active (str): Indicates if the UDF feature is active for the bot.
        slack_active (str): Indicates if the Slack feature is active for the bot.
        files (json-embedded list): A list of files to include with the bot.
        bot_implementation (str): openai or cortex or ...
    """
    # validate inputs
    bot_implementation = BotLlmEngineEnum(bot_implementation).value if bot_implementation else None

    update_query = f"""
        UPDATE {bot_servicing_table}
        SET API_APP_ID = %s, BOT_SLACK_USER_ID = %s, CLIENT_ID = %s, CLIENT_SECRET = %s,
            SLACK_SIGNING_SECRET = %s, AUTH_URL = %s, AUTH_STATE = %s,
            UDF_ACTIVE = %s, SLACK_ACTIVE = %s, FILES = %s, BOT_IMPLEMENTATION = %s
        WHERE upper(BOT_ID) = upper(%s)
    """

    try:
        self.client.cursor().execute(
            update_query,
            (
                api_app_id,
                bot_slack_user_id,
                client_id,
                client_secret,
                slack_signing_secret,
                auth_url,
                auth_state,
                udf_active,
                slack_active,
                files,
                bot_implementation,
                bot_id,
            ),
        )
        self.client.commit()
        logger.info(
            f"Successfully updated existing bot configuration for bot_id: {bot_id}"
        )
    except Exception as e:
        logger.info(
            f"Failed to update existing bot configuration for bot_id: {bot_id} with error: {e}"
        )
        raise e

def db_update_existing_bot_basics(
    self,
    bot_id,
    bot_name,
    bot_implementation,
    files,
    available_tools,
    bot_instructions,
    project_id,
    dataset_name,
    bot_servicing_table,
):
    """
    Updates basic bot configuration fields in the BOT_SERVICING table.

    Args:
        bot_id (str): The unique identifier for the bot.
        bot_name (str): The name of the bot.
        bot_implementation (str): openai or cortex or ...
        files (json-embedded list): A list of files to include with the bot.
        available_tools (list): List of tools available to the bot.
        bot_instructions (str): Instructions for the bot.
        project_id (str): The Snowflake project ID.
        dataset_name (str): The Snowflake dataset name.
        bot_servicing_table (str): The name of the bot servicing table.
    """
    # validate inputs
    bot_implementation = BotLlmEngineEnum(bot_implementation).value if bot_implementation else None

    available_tools_string = json.dumps(available_tools)
    files_string = json.dumps(files) if not isinstance(files, str) else files

    update_query = f"""
        UPDATE {bot_servicing_table}
        SET BOT_NAME = %s,
            BOT_IMPLEMENTATION = %s,
            FILES = %s,
            AVAILABLE_TOOLS = %s,
            BOT_INSTRUCTIONS = %s
        WHERE upper(BOT_ID) = upper(%s)
    """

    try:
        self.client.cursor().execute(
            update_query,
            (
                bot_name,
                bot_implementation,
                files_string,
                available_tools_string,
                bot_instructions,
                bot_id,
            ),
        )
        self.client.commit()
        logger.info(
            f"Successfully updated basic bot configuration for bot_id: {bot_id}"
        )
    except Exception as e:
        logger.info(
            f"Failed to update basic bot configuration for bot_id: {bot_id} with error: {e}"
        )
        raise e

def db_update_bot_details(
    self,
    bot_id,
    bot_slack_user_id,
    slack_app_token,
    project_id,
    dataset_name,
    bot_servicing_table,
):
    """
    Updates the BOT_SERVICING table with the new bot_slack_user_id and slack_app_token for the given bot_id.

    Args:
        bot_id (str): The unique identifier for the bot.
        bot_slack_user_id (str): The new Slack user ID for the bot.
        slack_app_token (str): The new Slack app token for the bot.
    """

    update_query = f"""
        UPDATE {bot_servicing_table}
        SET BOT_SLACK_USER_ID = %s, SLACK_APP_TOKEN = %s
        WHERE upper(BOT_ID) = upper(%s)
    """

    try:
        self.client.cursor().execute(
            update_query, (bot_slack_user_id, slack_app_token, bot_id)
        )
        self.client.commit()
        logger.info(
            f"Successfully updated bot servicing details for bot_id: {bot_id}"
        )
    except Exception as e:
        logger.error(
            f"Failed to update bot servicing details for bot_id: {bot_id} with error: {e}"
        )
        raise e


def db_delete_bot(self, project_id, dataset_name, bot_servicing_table, bot_id):
    """
    Deletes a bot from the bot_servicing table in Snowflake based on the bot_id.

    Args:
        project_id (str): The project identifier.
        dataset_name (str): The dataset name.
        bot_servicing_table (str): The bot servicing table name.
        bot_id (str): The bot identifier to delete.
    """

    # Query to delete the bot from the database table
    delete_query = f"""
        DELETE FROM {bot_servicing_table}
        WHERE upper(bot_id) = upper(%s)
    """

    # Execute the delete query
    try:
        cursor = self.client.cursor()
        cursor.execute(delete_query, (bot_id,))
        self.client.commit()
        logger.info(
            f"Successfully deleted bot with bot_id: {bot_id} from the database."
        )
    except Exception as e:
        logger.error(
            f"Failed to delete bot with bot_id: {bot_id} from the database with error: {e}"
        )
        raise e

def db_remove_bot_tools(
    self,
    project_id=None,
    dataset_name=None,
    bot_servicing_table=None,
    bot_id=None,
    updated_tools_str=None,
    tools_to_be_removed=None,
    invalid_tools=None,
    updated_tools=None,
):

    # Query to update the available_tools in the database
    update_query = f"""
            UPDATE {bot_servicing_table}
            SET available_tools = %s
            WHERE upper(bot_id) = upper(%s)
        """

    # Execute the update query
    try:
        cursor = self.connection.cursor()
        cursor.execute(update_query, (updated_tools_str, bot_id))
        self.connection.commit()
        logger.info(f"Successfully updated available_tools for bot_id: {bot_id}")

        return {
            "success": True,
            "removed": tools_to_be_removed,
            "invalid tools": invalid_tools,
            "all_bot_tools": updated_tools,
        }

    except Exception as e:
        logger.error(
            f"Failed to remove tools from bot_id: {bot_id} with error: {e}"
        )
        return {"success": False, "error": str(e)}

def db_list_all_bots(
    self,
    project_id,
    dataset_name,
    bot_servicing_table,
    runner_id=None,
    full=False,
    slack_details=False,
    with_instructions=False,
):
    """
    Returns a list of all the bots being served by the system, including their runner IDs, names, instructions, tools, etc.

    Returns:
        list: A list of dictionaries, each containing details of a bot.
    """
    # Get the database schema from environment variables

    # Convert with_instructions to boolean if it's a string
    if isinstance(with_instructions, str):
        with_instructions = with_instructions.lower() == 'true'

    if full:
        select_str = "api_app_id, bot_slack_user_id, bot_id, bot_name, bot_instructions, runner_id, slack_app_token, slack_app_level_key, slack_signing_secret, slack_channel_id, available_tools, udf_active, slack_active, \
            files, bot_implementation, bot_intro_prompt, bot_avatar_image, slack_user_allow, teams_active, teams_app_id, teams_app_password, teams_app_type, teams_app_tenant_id"
    else:
        if slack_details:
            select_str = "runner_id, bot_id, bot_name, bot_instructions, available_tools, bot_slack_user_id, api_app_id, auth_url, udf_active, slack_active, files, bot_implementation, bot_intro_prompt, slack_user_allow"
        else:
            select_str = "runner_id, bot_id, bot_name, bot_instructions, available_tools, bot_slack_user_id, api_app_id, auth_url, udf_active, slack_active, files, bot_implementation, bot_intro_prompt"
    if not with_instructions and not full:
        select_str = select_str.replace("bot_instructions, ", "")
    # Remove bot_instructions if not requested
    if not with_instructions and not full:
        select_str = select_str.replace("bot_instructions, ", "")
        select_str = select_str.replace(", bot_intro_prompt", "")

    # Use the bot_servicing_table name for bot_table
    bot_table = bot_servicing_table
    # Extract table name after last dot if dots are present
    if '.' in bot_table:
        bot_table = bot_table.split('.')[-1]

    # Query to select all bots from the BOT_SERVICING table
    if runner_id is None:
        select_query = f"""
        SELECT {select_str}
        FROM {project_id}.{dataset_name}.{bot_table}
        """
    else:
        select_query = f"""
        SELECT {select_str}
        FROM {project_id}.{dataset_name}.{bot_table}
        WHERE runner_id = '{runner_id}'
        """

    try:
        # Execute the query and fetch all bot records
        cursor = self.connection.cursor()
        cursor.execute(select_query)
        bots = cursor.fetchall()
        columns = [col[0].lower() for col in cursor.description]
        bot_list = [dict(zip(columns, bot)) for bot in bots]
        cursor.close()
        # logger.info(f"Retrieved list of all bots being served by the system.")
        return bot_list
    except Exception as e:
        logger.error(f"Failed to retrieve list of all bots with error: {e}")
        raise e

