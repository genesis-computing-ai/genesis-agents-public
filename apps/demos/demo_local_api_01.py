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
        all_bot_configs = client.list_available_bots()
        all_bot_ids = sorted([bot.bot_id for bot in all_bot_configs])
        print(f'Available bots: {", ".join(all_bot_ids)}')
        msg = "hello"
        print(f"\n>>>> Sending '{msg}' to Eve")
        request = client.submit_message("Eve", msg)
        response = client.get_response("Eve", request["request_id"])
        print(f"\n>>>> Response from Eve: {response}")

        msg = "Run a query to get the current date from the database. Use an arbitrary database connection. Show me the result as well as which database connection was used and what the query was."
        request = client.submit_message("Eve", msg)
        response = client.get_response("Eve", request["request_id"])
        print("\n>>>>", response)

        # Example of listing and reading files from the internal git repo using the 'raw' tool invocation method
        print("\n>>>> git_action: list_files")
        res = client.run_genesis_tool(tool_name="git_action", params={"action": "list_files"}, bot_id="Eve")
        print("\n>>>>", res)

        print("\n>>>> git_action: read_file")
        res = client.run_genesis_tool(tool_name="git_action", params={"action": "read_file", "file_path": "README.md"}, bot_id="Eve")
        print("\n>>>>", res)

        # example of listing/reading/writing git files
        print("\n>>>> gitfiles.list_files")
        res = client.gitfiles.list_files()
        print("\n>>>>", res)

        print("\n>>>> gitfiles.read(README.md)")
        res = client.gitfiles.read("README.md")
        print("\n>>>>", res)

        print("\n>>>> gitfiles.write(test1.txt, 'Hello, world!')")
        res = client.gitfiles.write("test1.txt", "Hello, world!")
        print("\n>>>>", res)

        print("\n>>>> gitfiles.read(test1.txt)")
        res = client.gitfiles.read("test1.txt")
        print("\n>>>>", res)

if __name__ == "__main__":
    main()
