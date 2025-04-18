import argparse
from   textwrap                 import dedent

DEFAULT_SERVER_URL = "http://localhost:8080"

def add_default_argparse_options(parser: argparse.ArgumentParser):
    parser.add_argument('--server_url', '-u', type=str, required=False, default=DEFAULT_SERVER_URL,
                        help=dedent(f'''
                            Server URL for GenesisAPI. Defaults to {DEFAULT_SERVER_URL}
                            It supports three types of URLS: 
                            1. HTTP(s) server URL (e.g. "http://localhost:8080"), 
                            2. "embedded" for running the Genesis BotOsServer inside the caller's process (used for testing and development only).
                            3. Snowflake SQLAlchemy connection URL (e.g. "snowflake://user@account") that is passed to SqlAlchemy's create_engine function.
                               (see --snowflake_conn_args for additional connection arguments for Snowflake)
                            ''')
                        )

    parser.add_argument('--snowflake_conn_args', '-c', type=str, required=False,
                        help=dedent('''
                            Additional connection arguments for a Snowflake connection if the server URL is a Snowflake connection URL. 
                            Use the format key1=value1,key2=value2,... (no quotes).
                            To pass a private_key that is stored in a PEM file is "private_key_file", we load the private key from the provited PEM file and add it to the arguments as "private_key".        
                            ''')
                        )

    parser.add_argument('--genesis_db', '-d', type=str, required=False, default="GENESIS_BOTS",
                        help=dedent('''
                            The name of the Genesis database to use. Defaults to GENESIS_BOTS.
                            ''')
                        )
    