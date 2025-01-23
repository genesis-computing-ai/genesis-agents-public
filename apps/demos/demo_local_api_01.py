import argparse
from   genesis_bots.api         import GenesisAPI, build_server_proxy
from   genesis_bots.api.utils   import add_default_argparse_options

def parse_arguments():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="A simple CLI chat interface to Genesis bots")
    add_default_argparse_options(parser)
    return parser.parse_args()


def main():
    args = parse_arguments()
    server_proxy = build_server_proxy(args.server_url, args.snowflake_conn_args)
    with GenesisAPI(server_proxy=server_proxy) as client:
        print("-----------------------")
        msg = "hello"
        print(f"\n>>>> Sending '{msg}' to Eve")
        request = client.submit_message("Eve", msg)
        response = client.get_response("Eve", request["request_id"])
        print(f"\n>>>> Response from Eve: {response}")

        msg = "Run a query to get the current date from the database. Use an arbitrary database connetion. Show me the result as well as which database connection was used."
        request = client.submit_message("Eve", msg)
        response = client.get_response("Eve", request["request_id"])
        print("\n>>>>", response)


if __name__ == "__main__":
    main()
