"""
This script demonstrates how to use the Genesis API to maintain a custom data catalog.
It showcases the following functionalities:
1. Harnesing the the built-in power of the Genesis Bots to understand data and metadata as well as basic data engineering concepts like data catalog maintenance, without the need to write any special code.
2. Providing custom tools to the bots to perfrom client-side operations for maintaining a custom data catalog.

See the command line args for more information.
"""

import argparse
from   genesis_bots.api         import (GenesisAPI, bot_client_tool,
                                        build_server_proxy)
from   genesis_bots.api.utils   import add_default_argparse_options
import json
import os
from   pathlib                  import Path
from   textwrap                 import dedent
import uuid
import yaml

import shutil

class SampleCatalog:
    '''
    A class implementing a simple data catalog for the demo. The initial catalog is loaded from the source YAML file that is foung in the demo_data directory, 
    into a staging directory with the suffix '.v0'.
    Every subseqent change made to the catalog is saved to a new file in the staging directory with the next version number (v1, v2, etc).
    This class is meant to be used as a singleton.
    '''

    CATALOG_FILENAME = 'demo_baseball_catalog.yaml'
    CATALOG_SCHEMA_FILENAME = 'demo_baseball_catalog.schema.json'

    def __init__(self, stage_dir: str):
        self.source_dir = Path(os.path.dirname(__file__)).parent/ "demo_data"
        self.stage_dir = Path(stage_dir)
        self.curr_version = 0
        self.curr_catalog_path = None


    def setup(self):
        # Ensure the stage directory exists
        self.stage_dir.mkdir(parents=True, exist_ok=True)
        print(f">>>> Using stage directory for catalog data: {self.stage_dir}")

        # Clear the content of the stage directory
        for item in self.stage_dir.iterdir():
            if item.is_dir():
                shutil.rmtree(item)
            else:
                item.unlink()

        # Copy the source catalog data to the stage directory as v0
        self.curr_catalog_path = self.stage_dir / (self.CATALOG_FILENAME + f".v{self.curr_version}")
        with open(self.source_dir/self.CATALOG_FILENAME, 'r') as src_yaml_file:
            catalog_data = yaml.safe_load(src_yaml_file)
            catalog_data = self._normalize_dict(catalog_data)
            with open(self.curr_catalog_path, 'w') as tgt_yaml_file:
                yaml.dump(catalog_data, tgt_yaml_file)


    def _normalize_dict(self, entry: dict) -> dict:
        # ensure the entry is a dictionary
        if not isinstance(entry, dict):
            raise ValueError(f"Catalog entry must be a dictionary, got: {type(entry)}")
        def sort_dict(d):
            if not isinstance(d, dict):
                return d
            return {k: sort_dict(v) for k, v in sorted(d.items())}

        entry = sort_dict(entry)
        return entry


    def load_catalog(self) -> dict:
        with open(self.curr_catalog_path, 'r') as yaml_file:
            catalog_data = yaml.safe_load(yaml_file)
        return self._normalize_dict(catalog_data)


    def save_catalog(self, catalog_data: dict):
        catalog_data = self._normalize_dict(catalog_data)
        self.curr_version += 1
        vfn = self.CATALOG_FILENAME + f".v{self.curr_version}"
        self.curr_catalog_path = self.stage_dir / vfn
        print(f">>>> Updating catalog... changes saved to {vfn}")

        with open(self.curr_catalog_path, 'w') as yaml_file:
            yaml.dump(catalog_data, yaml_file)


    def list_assets(self) -> list[str]:
        catalog_data = self.load_catalog()
        return sorted(catalog_data.keys())


    def get_asset_entry(self, asset_name: str) -> dict:
        catalog_data = self.load_catalog()
        try:
            return catalog_data[asset_name]
        except KeyError:
            return {}


    def update_asset_entry(self, asset_name: str, asset_entry: dict):
        catalog_data = self.load_catalog()
        catalog_data[asset_name] = asset_entry
        self.save_catalog(catalog_data)


    def get_catalog_handbook(self) -> str:
        with open(self.source_dir / self.CATALOG_SCHEMA_FILENAME, 'r') as json_file:
            catalog_schema = json.load(json_file)

        catalog_handbook = dedent(f'''
            We maintain a simple data catalog for our baseball dataset.
            The primary purpose of this catalog is to collect metadata about our Baseball dataset so that Sports data analysts have
            the most up to date metadata about our dataset, which improves accuracy, collaboration, end efficiency of their work.
            Our catalog maintains the following information about these data assets:
            
                * Table and view descriptions
                * Column definitions and descriptions for each table and view
                * Relationships between tables and views.

            The catalog is maintained as a hierarchical dataset and is modelled as nested dictionaries where the top-level dictionary maps asset names (table and view names) to their catalog entries.
            The schema of this catalog dictionary is the following JSON schema:
            
            ```
            {catalog_schema}
            ```
            
            To access and maintain the catalog we provide the following tools:
            
            * `get_catalog_entry`: Retrieves a catalog entry for the given asset name (table, view, etc).
            * `update_catalog_asset_entry`: Updates a catalog asset entry for the given asset name.
            
            When applying changes to the catalog, follow these guidelines:
            
            * Use the your metadata harvesting tools to fetch the latest metadata about the assets in order to 
            update the most accurate and up to date information in the catalog.
            
            * Strictly follow the catalog schema when adding, modifying, or removing catalog entries.

            * changes can be applied to one asset at a time - either to an entire table (with action_type as one of 'create', 'remove', 'update') 
            or to a single column within a single table (with action_type = 'update').

            ''')
        return  catalog_handbook


sample_catalog : SampleCatalog = None # Singleton instance initialized by main()


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
    assert sample_catalog, 'Internal error: Catalog not properly setup yet'
    return sample_catalog.get_asset_entry(asset_name)



@bot_client_tool(
    asset_name="The name of the asset (table, view) to update.",
    action_type="The action to apply to that key. Valid values are: 'create', 'remove', 'update'.",
    new_data=("Required for action_type 'create' and 'update': provide a dictionary containing the full new entry for that asset. "
              "For 'remove' actions, this value should be null."),
    change_description="A summary of the changes/additions you made to the catalog entry."
)
def update_catalog_asset_entry(asset_name: str, action_type: str, new_data: dict, change_description: str) -> str:
    """
    Update a catalog asset entry for the given asset name.
    """
    assert sample_catalog, 'Internal error: Catalog not properly setup yet'

    action = action_type.lower()
    valid_actions = ['create', 'remove', 'update']
    if action not in valid_actions:
        raise ValueError(f"Invalid action: {action}. Use one of the following: {valid_actions}")

    if action == 'create':
        if not new_data:
            raise ValueError("For 'create' action, new_data must be provided.")
        if asset_name in sample_catalog.list_assets():
            raise ValueError(f"Asset '{asset_name}' already exists in the catalog. ")
    elif action == 'remove':
        if new_data:
            raise ValueError("For 'remove' action, new_data must be null.")
        if asset_name not in sample_catalog.list_assets():
            raise ValueError(f"Asset '{asset_name}' does not exist in the catalog. ")
        new_data = None
    elif action == 'update':
        if not new_data:
            raise ValueError("For 'create' action, new_data must     be provided.")
        if asset_name not in sample_catalog.list_assets():
            raise ValueError(f"Asset '{asset_name}' does not exist in the catalog. ")
    else:
        raise ValueError(f"Invalid action: {action}. ")
    print("\n" + "."*20 + "\n")
    print(f"UPDATING CATALOG ENTRY FOR ASSET: '{asset_name}', ACTION: '{action_type}'")
    print(f"DESCRIPTION: {change_description}")
    sample_catalog.update_asset_entry(asset_name, new_data)
    print("\n" + "."*20 + "\n")


Bot_CatalogMaintainer_config = dedent(f'''
    BOT_ID: CatalogMaintainer
    AVAILABLE_TOOLS: ["data_connector_tools", "harvester_tools", "web_access_tools", "delegate_work"]
    BOT_AVATAR_IMAGE: null
    BOT_IMPLEMENTATION: openai
    BOT_INSTRUCTIONS: >
        You are a data engineer who is responsible for routinely maintaining the data catalog for our baseball dataset and making sure it is up to date with respect to the latest metadata and data.
        Remember that the catalog is used primarily by humans for data exploration and reporting, so it should be as accurate and complete as possible.

        You are running this task in a non-interactive environment, DO NOT ask for confirmation or clarification in order to proceed.
        You should only make changes to the catalog if you are sure that the changes are correct and necessary.    

    BOT_INTRO_PROMPT: null
    BOT_NAME: CatalogMaintainer
    DATABASE_CREDENTIALS: ''
    FILES: null
    RUNNER_ID: snowflake-1
    UDF_ACTIVE: Y
    ''')


def print_header(msg: str, line_type: str = "="):
    print(f"\n{line_type*80}\n{msg}\n{line_type*80}\n")


def parse_arguments():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    add_default_argparse_options(parser)
    default_stage_dir = os.path.join(os.getcwd(), f"sample_catalog_stage")
    parser.add_argument("--stage-dir", type=str,
                        default=default_stage_dir,
                        help=f"The directory to use for the data catalog stage. Defaults to {default_stage_dir}")
    return parser.parse_args()


def main():
    args = parse_arguments()
    server_proxy = build_server_proxy(args.server_url, args.snowflake_conn_args)

    # setup the staged catalog where the bots will apply changes
    global sample_catalog
    sample_catalog = SampleCatalog(args.stage_dir)
    sample_catalog.setup()

    # Load bot definitions and enrich with the specific catalog handbook
    bot_def = yaml.safe_load(Bot_CatalogMaintainer_config)
    bot_id = bot_def['BOT_ID']
    bot_def['BOT_INSTRUCTIONS'] = dedent(bot_def['BOT_INSTRUCTIONS']) + dedent(f'''
                                                                               
        Here is the catalog handbook. Follow these instructions when updating the catalog:
        
        {sample_catalog.get_catalog_handbook()}
        ''')

    with GenesisAPI(server_proxy=server_proxy) as client:
        # register the bot
        client.register_bot(bot_def)

        # assign the local tools to the bot
        client.register_client_tool(bot_id, get_catalog_entry)
        #client.register_client_tool(BOT_ID, apply_catalog_change)
        client.register_client_tool(bot_id, update_catalog_asset_entry)

        # ------------------------------------------------------------------------------------------------
        # Pass 1: loop through all assets currently in the catalog and make sure they are up to date
        # ------------------------------------------------------------------------------------------------
        existing_cat_assets = sample_catalog.list_assets()
        th_id = uuid.uuid4()
        for asset_name in existing_cat_assets:
            print_header(f"Making sure catalog entry is up to date for asset: `{asset_name}`")

            task_prompt = dedent(f'''
                Your task is to maintain the Baseball dataset catalog entry for the asset named: {asset_name}.
                
                ** Perfrom the following steps:
                
                1. Fetch the latest metadata for asset `{asset_name}` from the Baseball database. Use the _data_explorer tool function to fetch the latest metadata.
                        
                2. Fetch the latest catalog entry for asset `{asset_name}`.
                
                3. Compare the latest metadata with the latest catalog entry and determine if any updates are needed.
                    Check the following:
                    - The asset description in the catalog correctly desscribes the asset
                    - The set of columns for that asset agrees with the latest shcema of the asset. Use the latest table/view DDL from the metadata to detremine the latest full set of columns
                    - All columns attributes are complete and up to date.
                    - All table attributes are complete and up to date.
                    - DO NOT bother making changes to column descriptions unless the change improves the accuracy of the description.
                    - DO NOT bother making changes to table descriptions unless the change improves the accuracy of the description.

                Use one or more calls to the update the catalog entry, providing a human-readable description of the changes you made.
                
                ''')
            req = client.submit_message(bot_id, task_prompt, thread_id=th_id)
            response = client.get_response(bot_id, req.request_id, print_stream=True)

        # ------------------------------------------------------------------------------------------------
        # Pass 2: Add catalog entries for data assets that are missing from the catalog
        # ------------------------------------------------------------------------------------------------
        print_header("Checking for missing catalog entries...")
        msg = dedent('''
            Your next task is to check for any tables that exist in the Basball database for which we do not have a corresponding catalog entry.
            
            Perform the following steps:
            
            1. Search for all data assets (tables/views) in the Baseball database.

            2. Determine which assets have no corresponding catalog entry.

            2. Out of all the missing assets, find the one with with the largest number of columns.
            
            3. Create a new catalog entry for that single asset.
            
            ''')
        req = client.submit_message(bot_id, msg, thread_id=th_id)
        response = client.get_response(bot_id, req.request_id, print_stream=True)

        print("\n\n------------- DONE -------------")
        print(f"You can now inspect the catalog changes in the stage directory: {sample_catalog.stage_dir}\n\n")


if __name__ == "__main__":
    main()
