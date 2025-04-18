import json
from genesis_bots.connectors.connector_helpers import llm_keys_and_types_struct

class SnowflakeConnectorBase:
    def __init__(self, connection_info=None, connection_name=None):
        self.connection_info = connection_info
        self.connection_name = connection_name

    def get_databases(self):
        raise NotImplementedError

    def get_schemas(self, database):
        raise NotImplementedError

    def get_tables(self, database, schema):
        raise NotImplementedError

    def get_columns(self, database, schema, table):
        raise NotImplementedError

    def get_table_ddl(self, schema_name, table_name):
        """
        Placeholder method to fetch the DDL statement for a table.
        This method should be overridden in subclasses for specific database types.

        :param schema_name: The name of the schema.
        :param table_name: The name of the table.
        :return: DDL statement as a string.
        """
        raise NotImplementedError("This method should be implemented by subclasses.")

    def get_sample_data(self, database, schema_name, table_name):
        """
        Placeholder method to fetch sample data from a table.
        This method should be overridden in subclasses for specific database types.

        :param schema_name: The name of the schema.
        :param table_name: The name of the table.
        :return: A list of dictionaries representing rows of sample data.
        """
        raise NotImplementedError("This method should be implemented by subclasses.")

    def run_query(self, query, max_rows):
        """
        Placeholder method to run a query.
        This method should be overridden in subclasses for specific database types.

        """
        raise NotImplementedError("This method should be implemented by subclasses.")


    def get_harvest_control_data_as_json(self, thread_id=None):
        """
        Placeholder method to retrieve harvest control data as JSON.
        This method should be overridden in subclasses for specific database types.
        
        :return: Harvest control data in JSON format.
        """
        raise NotImplementedError("This method should be implemented by subclasses.")


    def set_harvest_control_data(self, source_name, database_name, initial_crawl_complete=False, refresh_interval=1, schema_exclusions=None, schema_inclusions=None, status='Include', thread_id=None):
        """
        Placeholder method to set harvest control data.
        This method should be overridden in subclasses for specific database types.

        :param source_name: The source name for the harvest control data.
        :param database_name: The database name for the harvest control data.
        :param initial_crawl_complete: Flag indicating if the initial crawl is complete. Defaults to False.
        :param refresh_interval: The interval at which the data is refreshed. Defaults to 1.
        :param schema_exclusions: A list of schema names to exclude. Defaults to None.
        :param schema_inclusions: A list of schema names to include. Defaults to None.
        :param status: The status of the harvest control. Defaults to 'Include'.
        :return: None.
        """
        raise NotImplementedError("This method should be implemented by subclasses.")

    def remove_harvest_control_data(self, source_name, database_name, thread_id=None):
        """
        Placeholder method to remove harvest control data.
        This method should be overridden in subclasses for specific database types.

        :param source_name: The source name for the harvest control data to remove.
        :param database_name: The database name for the harvest control data to remove.
        :return: None.
        """
        raise NotImplementedError("This method should be implemented by subclasses.")

    def remove_metadata_for_database(self, source_name, database_name, thread_id=None):
        """
        Placeholder method to remove metadata for a database.
        This method should be overridden in subclasses for specific database types.

        :param source_name: The source name for the metadata to remove.
        :param database_name: The database name for the metadata to remove.
        :return: None.
        """
        raise NotImplementedError("This method should be implemented by subclasses.")

    def get_harvest_summary(self, thread_id=None):
        """
        Placeholder method to get a summary of the harvest.
        This method should be overridden in subclasses for specific database types.

        :return: A summary of the harvest as a string or structured data.
        """
        raise NotImplementedError("This method should be implemented by subclasses.")

    def run_python_code(self, code: str) -> str:
        """
        Executes a string of Python code and returns the output as a string.

        :param code: A string containing Python code to execute.
        :return: A string containing the output of the executed code.
        """
        raise NotImplementedError("This method should be implemented by subclasses.")

    def db_get_llm_key(self, project_id=None, dataset_name=None) -> llm_keys_and_types_struct:
        raise NotImplementedError("This method should be implemented by subclasses.")

    def db_get_active_llm_key(self) -> list[llm_keys_and_types_struct]:
        raise NotImplementedError("This method should be implemented by subclasses.")

    def db_get_user_extended_tools(self, project_id, dataset_name) -> list[dict]:
        raise NotImplementedError("This method should be implemented by subclasses.")
