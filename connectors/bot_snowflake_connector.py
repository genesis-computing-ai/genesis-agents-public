import json
import os
from connectors.snowflake_connector import SnowflakeConnector

# Global variable to hold the bot connection


def bot_credentials(bot_id):
    """
    This function returns a single bot connection to Snowflake.
    If the connection does not exist, it creates one.
    """
    try:
        connector = SnowflakeConnector("Snowflake")

        genbot_internal_project_and_schema = os.getenv('GENESIS_INTERNAL_DB_SCHEMA','None')
        if genbot_internal_project_and_schema == 'None':
            # Todo remove, internal note 
            print("ENV Variable GENESIS_INTERNAL_DB_SCHEMA is not set.")
        if genbot_internal_project_and_schema is not None:
            genbot_internal_project_and_schema = genbot_internal_project_and_schema.upper()
        db_schema = genbot_internal_project_and_schema.split('.')
        project_id = db_schema[0]
        dataset_name = db_schema[1]
        bot_servicing_table = os.getenv('BOT_SERVICING_TABLE', 'BOT_SERVICING')

        bot_config = connector.db_get_bot_database_creds(project_id=project_id, dataset_name=dataset_name, bot_servicing_table=bot_servicing_table, bot_id=bot_id)
        bot_database_creds = None
        if bot_config["database_credentials"]:
            bot_database_creds = bot_config["database_credentials"]

            # add snowflake connection
            # Extract individual elements from the JSON credentials
            bot_database_creds = json.loads(bot_database_creds)
    
    except Exception as e:
        print(f"Error getting bot credentials for {bot_config['bot_id']} : {str(e)}")
    return bot_database_creds
