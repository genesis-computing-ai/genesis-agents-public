import json
import os
from genesis_bots.connectors.snowflake_connector.snowflake_connector import SnowflakeConnector
from genesis_bots.connectors.sqlite_connector import SqliteConnector
from genesis_bots.core.logging_config import logger
genesis_source = os.getenv("GENESIS_SOURCE", default="Snowflake")


# Global variable to hold the bot connection
def bot_credentials(bot_id):
    """
    This function returns a single bot connection to Snowflake.
    If the connection does not exist, it creates one.
    """
    try:
        if genesis_source == 'Sqlite':
            connector = SqliteConnector("Sqlite")
        elif genesis_source == 'Snowflake':
            connector = SnowflakeConnector("Snowflake")
        else:
            raise ValueError('Invalid Source')

        genbot_internal_project_and_schema = os.getenv('GENESIS_INTERNAL_DB_SCHEMA','None')
        if genbot_internal_project_and_schema == 'None':
            # Todo remove, internal note
            logger.info("ENV Variable GENESIS_INTERNAL_DB_SCHEMA is not set.")
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
        logger.info(f"Error getting bot credentials for {bot_config['bot_id']} : {str(e)}")
    return bot_database_creds
