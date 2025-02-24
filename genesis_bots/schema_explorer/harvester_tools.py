
import json
import os

harvester_tools_functions = [
    {
        "type": "function",
        "function": {
            "name": "_get_harvest_control_data",
            "description": "Retrieves all the data from the harvest control table and returns it.  Tells you what sources and databases are being harvested, and if any schemas are being specifically included or excluded.  You can also use get_visible_databases to suggest additional things to harvest.",
            "parameters": {}
        }
    },
    {
        "type": "function",
        "function": {
            "name": "_set_harvest_control_data",
            "description": "Inserts or updates a row in the harvest control table using MERGE statement with explicit parameters.  Offer to use get_harvest_control_data to know whats already being harvested, then you can use get_visible_databases and get_visible_schemas tools to help decide what databases and schemas to add or change.",
            "parameters": {
                "type": "object",
                "properties": {
                    "connection_id": {"type": "string", "description": "Connection id of the database connection.  Get it via _list_database_connections is not known. If Snowflake, use 'Snowflake' as the connection id."},
                    "database_name": {"type": "string", "description": "The database name for the harvest control data. (For bigquery, this is the project name). For MySQL, this is also known as the Schema name. For SQLite, use the same as the connection id. Offer to the get_databases function to help figure out whats available.  Make sure to set the case properly for the database, either all upper case, or mixed case as reported by get_databases"},
                    "initial_crawl_complete": {"type": "boolean", "description": "Flag indicating if the initial crawl is complete. Set to False to trigger an immediate crawl.", "default": False},
                    "refresh_interval": {"type": "integer", "description": "The interval at which the data is refreshed in minutes.  Use 5 minutes unless the user specifies otherwise."},
                    "schema_exclusions": {
                        "type": "array",
                        "items": {
                            "type": "string",
                            "description": "Name of schema to exclude. The get_schemas function can be used to know what schemas are in a database.  You should suggest excluding the INFORMATION_SCHEMA unless directed to include it, or unless only specfic schemas are being selected using schema_inclusions. This is not applicable to MySQL or SQLite."
                        },
                        "description": "A list of schema names to exclude.  Optional.",
                        "default": []
                    },
                    "schema_inclusions": {
                        "type": "array",
                        "items": {
                            "type": "string",
                            "description": "Name of schema to include. "
                        },
                        "description": "A list of schema names to include. (For bigquery, these are the datasets). Leave empty to include All schemas, which is the default. This is not applicable to MySQL or SQLite.",
                        "default": []
                    },
                    "status": {"type": "string", "description": "The status of the harvest control.", "default": "Include"}
                },
                "required": ["source_name", "database_name"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "_remove_harvest_control_data",
            "description": "Removes a row from the harvest control table based on the provided identifiers. Removing this row will stop crawling data for this database.  If the user also wants to remove previously-crawled data for this source, also call _remove_metadata_for_database",
            "parameters": {
                "type": "object",
                "properties": {
                    "source_name": {"type": "string", "description": "The source name for the harvest control data to remove."},
                    "database_name": {"type": "string", "description": "The database name for the harvest control data to remove."}
                },
                "required": ["source_name", "database_name"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "_remove_metadata_for_database",
            "description": "Removes harvester crawl results for a specific source and database from the metadata table.",
            "parameters": {
                "type": "object",
                "properties": {
                    "source_name": {"type": "string", "description": "The source name for the harvest control data to remove."},
                    "database_name": {"type": "string", "description": "The name of the database for which to remove metadata."}
                },
                "required": ["source_name","database_name"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "_get_harvest_summary",
            "description": "Retrieves a summary and statistics of the currently-harvested data.",
            "parameters": {}
        }
    },
]


harvester_tools_list = {
    "_set_harvest_control_data": "db_adapter.set_harvest_control_data", 
    "_remove_harvest_control_data": "db_adapter.remove_harvest_control_data",
    "_remove_metadata_for_database": "db_adapter.remove_metadata_for_database",
    "_get_harvest_summary": "db_adapter.get_harvest_summary",
    "_get_harvest_control_data": "db_adapter.get_harvest_control_data_as_json",
}
