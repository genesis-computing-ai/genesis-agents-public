import os, json
from functools import lru_cache
# from .database_connector import DatabaseConnector

# TODO: make the importing of SnowflakeConnector, BigQueryConnector etc to be lazy, inside _get_global_db_connector_cached
# See Issue #79
# This is left here for backward compatibility

#from genesis_bots.connectors.bigquery_connector import BigQueryConnector
from genesis_bots.connectors.snowflake_connector.snowflake_connector import SnowflakeConnector
# from .bot_snowflake_connector import bot_credentials

@lru_cache(maxsize=None)
def _get_global_db_connector_cached(genesis_source_name, **kwargs):
    # internal helper function for get_global_db_connector
    if genesis_source_name == "BIGQUERY":
        raise NotImplementedError("BigQueryConnector is not implemented")
 #       try:
 #           connection_info = kwargs.pop('connection_info')
 #       except KeyError:
 #           raise ValueError(f"missing manadatory arg 'connection_info' for {genesis_source_name=}")
 #       connection_name = kwargs.pop('connection_name', "BigQuery")
 #       return BigQueryConnector(connection_info=connection_info, connection_name=connection_name, **kwargs)
    elif genesis_source_name == 'SQLITE':
        from genesis_bots.connectors.sqlite_connector import SqliteConnector
        connection_name = kwargs.pop('connection_name', "Sqlite")
        return SqliteConnector(connection_name=connection_name, **kwargs)
    elif genesis_source_name == 'SNOWFLAKE':
        connection_name = kwargs.pop('connection_name', "Snowflake")
        return SnowflakeConnector(connection_name=connection_name, **kwargs)
    else:
        raise ValueError(f"Invalid Source name '{genesis_source_name}'")

# Global dictionary to store Annoy index handlers per bot_id
_global_annoy_handlers = {}

def set_global_annoy_handler(bot_id: str, handler):
    """
    Sets a global Annoy index handler for a specific bot_id.

    Args:
        bot_id (str): The ID of the bot
        handler: The Annoy index handler to store
    """
    global _global_annoy_handlers
    _global_annoy_handlers[bot_id] = handler

def get_global_annoy_handler(bot_id: str):
    """
    Gets the global Annoy index handler for a specific bot_id.

    Args:
        bot_id (str): The ID of the bot

    Returns:
        The stored Annoy index handler for the bot_id, or None if not found
    """
    return _global_annoy_handlers.get(bot_id)



def get_global_db_connector(genesis_source_name=None, **kwargs):
    """
    Retrieves a global database connector based on the specified genesis source name.

    If no genesis source name is provided, the function attempts to resolve it from the
    environment variable 'GENESIS_SOURCE'. The function supports connectors for BigQuery,
    SQLite, and Snowflake. Additional connection parameters can be passed via kwargs.

    Args:
        genesis_source_name (str, optional): The name of the genesis source. Defaults to None.
        **kwargs: Additional keyword arguments for connection configuration.

    Returns:
        DatabaseConnector: An instance of the appropriate database connector.

    Raises:
        ValueError: If the genesis source name is invalid or cannot be resolved from the environment.
    """
    if genesis_source_name is None:
        assert not kwargs  # we do not allow kwargs without an explicit source name
        # Resolve the connector type and extra params (if any) from env
        genesis_source_name = os.getenv("GENESIS_SOURCE", default="Snowflake")
        if not genesis_source_name:
            raise ValueError("Cannot automatically resolve GENESIS_SOURCE from the environment")
        genesis_source_name = genesis_source_name.upper()
        if genesis_source_name == "BIGQUERY":
            raise NotImplementedError("BigQuery connector is not implemented")
    else:
        genesis_source_name = genesis_source_name.upper()
    return _get_global_db_connector_cached(genesis_source_name, **kwargs)
