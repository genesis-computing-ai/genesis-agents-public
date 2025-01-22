import argparse

DEFAULT_SERVER_URL = "http://localhost:8080"

def add_default_argparse_options(parser: argparse.ArgumentParser):
    parser.add_argument('--server_url', '-u', type=str, required=False, default=DEFAULT_SERVER_URL, help=f'Server URL for GenesisAPI. Deaults to {DEFAULT_SERVER_URL}')
    parser.add_argument('--snowflake_conn_args', '-c', type=str, required=False,
                        help='Additional connection arguments for Snowflake if the provider server URL is a snowflake connection URL (othewise ignore)')