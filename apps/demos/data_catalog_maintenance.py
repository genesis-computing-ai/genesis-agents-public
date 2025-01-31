"""
This script serves as a demonstration of how to interact with Genesis bots using the Genesis API.
It showcases the following functionalities:
1. Fetching and displaying infromation on avaialable baseball teams from a demo database, without explicitly specifying
   what table or query to use.
2. Asking the bot to calculate some stat like win/lose ratio based on input from the user.

See the command line options for more information on how to connect to a Genesis bot server.
"""

import argparse
from   functools                import lru_cache
from   genesis_bots.api         import (GenesisAPI, bot_client_tool,
                                        build_server_proxy)
from   genesis_bots.api.utils   import add_default_argparse_options
import json
import os
from   textwrap                 import dedent


BOT_ID = "Eve"


@lru_cache(maxsize=None)
def _load_demo_catalog_data() -> dict:
    current_dir = os.path.dirname(__file__)
    json_file_path = os.path.join(current_dir, 'demo_data', 'demo_baseball_catalog.json')

    with open(json_file_path, 'r') as file:
        catalog_data = json.load(file)

    return catalog_data


@bot_client_tool(
    schema="The schema of the asset to retrieve from the data catalog. ",
    asset_name=("The name of the asset to retrieve from the data catalog. "
                "This is the name of the table or a view view, etc. that you want to get information on." )
)
def get_catalog_entry(schema: str, asset_name: str) -> str:
    """
    Reads a catalog entry for the given asset name (table, view, etc).
    Returns a JSON object with the asset description, columns, and other relevant information.
    If the asset is not found, returns an empty JSON object.
    """
    data = _load_demo_catalog_data()
    try:
        return data[asset_name]
    except KeyError:
        return {}


@bot_client_tool(
    schema="The schema of the asset for which to apply the changes. ",
    asset_name="The name of the asset for which to apply the changes. ",
    action="The action to apply to the asset. See function description for details. ",
    action_args="The arguments specific to the action in the form of a JSON string. ",
    change_description="a Human-readable description of the reason for the change, for posterity. "
)
def apply_catalog_change(schema: str, asset_name: str, action: str, action_args: str, change_description: str) -> str:
    """
    Apply a change to the catalog for the given asset.
    the `action` can be one of the following:  
        * add_table:	        Adds a new table with columns and constraints.
        * remove_table: 	    Removes an existing table.
        * rename_table:	        Renames an existing table.
        * add_column:	        Adds a new column to an existing table.
        * remove_column:	    Removes an existing column from a table.
        * modify_column:	    Modifies a single column's attributes.
        * rename_column:	    Renames a single column.
        * add_constraint:	    Adds a constraint to table.
        * remove_constraint:	Removes an existing constraint.
    """
    print("\n" + "-"*80 + "\n")
    print("\nSUGGESTED CHANGE TO CATALOG:")
    print(f"ASSET: {schema}.{asset_name}")
    print(f"ACTION: {action}")
    print(f"ACTION ARGS: ")
    action_args_dict = json.loads(action_args)
    print(json.dumps(action_args_dict, indent=4))
    print(f"CHANGE DESCRIPTION: {change_description}")
    print("\n" + "-"*80 + "\n")



def parse_arguments():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    add_default_argparse_options(parser)
    return parser.parse_args()


def main():
    args = parse_arguments()
    server_proxy = build_server_proxy(args.server_url, args.snowflake_conn_args)
    with GenesisAPI(server_proxy=server_proxy) as client:
        client.register_client_tool(BOT_ID, get_catalog_entry)
        client.register_client_tool(BOT_ID, apply_catalog_change)
        msg = dedent('''
            Your task is to maintain our internal data catalog for our baseball dataset.
            Perfrom the followng steps:
            1. list all the data assets currently available in the baseball dataset.
            2. fetch the latest information on those assets from the data catalog (use the proper tool).
            3. For assets for which we have catalog entries, check if any information is missing or is out of date. Here are the guidelines to follow:
              a. No catalog entry should have a missing value. If it is missing, suggest a value to fill it  in based on the latest asset metadata or data.
              b. If a catalog entry does not correctly or fully describes the latest metadata or data, suggest a change to the catalog to update it.
            4. Suggest a series of actions to take on the catalog by calling the `apply_catalog_change` tool.
            ''')
        req = client.submit_message(BOT_ID, msg)
        response = client.get_response(BOT_ID, req.id)
        print(response)



if __name__ == "__main__":
    main()
