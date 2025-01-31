"""
This script demonstrates how to use the Genesis API to maintain a custom data catalog.
It showcases the following functionalities:
1. Harnesing the the built-in power of the Genesis Bots to understand data and metadata as well as basica data engineering concepts like data catalog maintenance.
2. Providing custom tools to the bots to perfrom client-side operations (maintaining the catalog).

See the command line options for more information on how to connect to a Genesis bot server.
"""

import argparse
from   functools                import lru_cache
from   genesis_bots.api         import (GenesisAPI, bot_client_tool,
                                        build_server_proxy)
from   genesis_bots.api.utils   import add_default_argparse_options
from   io                       import StringIO
import json
import os
from   textwrap                 import dedent
import uuid
import yaml

BOT_ID = "Eve"


@lru_cache(maxsize=None)
def _load_demo_catalog_data() -> dict:
    current_dir = os.path.dirname(__file__)
    yaml_file_path = os.path.join(current_dir, 'demo_data', 'demo_baseball_catalog.yaml')
    print(">>>>", "Loading catalog data from", yaml_file_path, flush=True)

    with open(yaml_file_path, 'r') as file:
        catalog_data = yaml.safe_load(file)

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


def print_separator():
    print("="*80)


# Global StringIO object to accumulate prints
suggested_changes = StringIO()

@bot_client_tool(
    asset_name="The name of the asset (table, view) for which to apply the changes. ",
    action="The action to apply to the asset. See function description for details. ",
    action_args="The arguments specific to the action in the form of a JSON string. ",
    change_description="a Human-readable description of the reason for the change, for posterity. "
)
def apply_catalog_change(asset_name: str, action: str, action_args: str, change_description: str) -> str:
    """
    Apply a change to the catalog for the given asset.
    
    the `action` can be one of the following:  
    * add_table: Adds a new table with columns and constraints. Change description should include a full new table catalog entry.
    * remove_table: Removes an existing table. 
    * rename_table: Renames an existing table.
    * add_column: Adds a new column to an existing table. 
    * remove_column: Removes an existing column from a table. 
    * modify_column: Modifies a single column's attributes. 
    * rename_column: Renames a single column. 
    * add_constraint: Adds a constraint to table. 
    * remove_constraint: Removes an existing constraint.
    """
    suggested_changes.write("\n" + "-"*80 + "\n")
    suggested_changes.write("\nSUGGESTED CHANGE TO CATALOG:\n")
    suggested_changes.write(f"ASSET: {asset_name}\n")
    suggested_changes.write(f"ACTION: {action}\n")
    suggested_changes.write("ACTION ARGS: \n")
    action_args_dict = json.loads(action_args)
    suggested_changes.write(json.dumps(action_args_dict, indent=4) + "\n")
    suggested_changes.write(f"CHANGE DESCRIPTION: {change_description}\n")
    suggested_changes.write("\n" + "-"*80 + "\n")



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

        global suggested_changes
        uu = uuid.uuid4()

        print_separator()
        print("Maintaining existing catalog assets entries...")
        print_separator()
        print()

        msg = dedent('''
            Your task is to maintain our internal data catalog for our baseball dataset.
            You are running this task in a non-interactive environment, DO NOT ask for confirmation or clarification in order to proceed.
            You should only make changes to the catalog if you are sure that the changes are correct and necessary. 
            Remember that the catalog is used primarily by humans for data exploration and reporting, so it should be as accurate and complete as possible.
            
            Perfrom the following steps:
            
            1. list all the data assets currently available in the baseball dataset.
            
            2. fetch the latest information on those assets from the data catalog (use the proper tool).
            
            3. ONLY for assets for which we already have catalog entries, check if any information is missing or is out of date. 
               Here are the guidelines to follow:
            
              a. No catalog entry should have a missing value (such as a null, empty value, etc). If it is missing, suggest a value to fill it  in based on the latest asset metadata or data.
              
              b. If a catalog entry does not correctly or fully describes the latest metadata or data, suggest a change to the catalog to update it.

              c. If a catalog entry is partial, incomplete, or otherwise incorrect, suggest a change to the catalog to update it.

              d. If a catalog entry is redundant, suggest a change to the catalog to remove it.

            4. Suggest a series of actions to existing catalog entries by calling the `apply_catalog_change` tool.
            
            ''')
        suggested_changes.truncate(0)
        req = client.submit_message(BOT_ID, msg, thread_id=uu)
        response = client.get_response(BOT_ID, req.request_id, print_stream=True)
        print("------------- SUGGESTED CATALOG CHANGES -------------")
        print(suggested_changes.getvalue())

        suggested_changes.truncate(0)
        print_separator()
        print("Checking for missing catalog entries...")
        print_separator()
        print()
        msg = dedent('''
            Your next task is to check for any tables that exist in the Basball database for which we do not have a catalog entry.
            
            Perfrom the following steps:
            
            1. find all the tables in the baseball dataset that do not have a catalog entry.

            2. Out of the tables in the previous step, find the one missing table with the largest number of columns.
            
            3. Use the apply_catalog_change tool to create a new entry for that table in the catalog. This attribute of this entry should follow the strcuture of the existing entries in the catalog.
            
            ''')
        req = client.submit_message(BOT_ID, msg, thread_id=uu)
        response = client.get_response(BOT_ID, req.request_id, print_stream=True)
        print("------------- SUGGESTED CATALOG CHANGES -------------")
        print(suggested_changes.getvalue())


if __name__ == "__main__":
    main()
