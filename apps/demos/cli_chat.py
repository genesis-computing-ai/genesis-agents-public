import argparse
import os
os.environ['LOG_LEVEL'] = 'WARNING' # control logging from GenesisAPI
from genesis_bots.api import GenesisAPI, build_server_proxy
from genesis_bots.api.utils import add_default_argparse_options

# color constants
COLOR_YELLOW = "\033[93m"
COLOR_RED = "\033[91m"
COLOR_BLUE = "\033[94m"
COLOR_CYAN = "\033[96m"
COLOR_RESET = "\033[0m"


RESPONSE_TIMEOUT_SECONDS = 20.0

EXIT_MSG = "Exiting chat. Goodbye!"

def parse_arguments():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="A simple CLI chat interface to Genesis bots")
    add_default_argparse_options(parser)
    return parser.parse_args()


def main():
    args = parse_arguments()
    server_proxy = build_server_proxy(args.server_url, args.snowflake_conn_args)
    bot_id = None

    with GenesisAPI(server_proxy=server_proxy) as client:
        welcome_msg = "\nWelcome to the Genesis chat interface. Type '/quit' to exit."
        if bot_id is None:
            welcome_msg += "\nStart your first message with @<bot_id> to chat with that bot. Use it again to switch bots."
        else:
            welcome_msg += f"\nYou are chatting with bot {bot_id}. Start your message with @<bot_id> to switch bots."
        print(welcome_msg)
        print("-"*len(welcome_msg))
        while True:
            try:
                # Prompt user for input
                user_input = input(f"{COLOR_YELLOW}[You]: {COLOR_RESET}")
                user_input = user_input.strip()

                # Check for exit condition
                if user_input.lower() == '/quit':
                    print(EXIT_MSG)
                    break
                elif user_input.startswith('@'):
                    parts = user_input.split(maxsplit=1)
                    bot_id = parts[0][1:]  # Remove the '@' character
                    user_input = parts[1] if len(parts) > 1 else ''
                if not bot_id:
                    print(f"{COLOR_RED}ERROR: No bot id set in context. Start your message with @<bot_id>.{COLOR_RESET}")
                    continue
                if not user_input:
                    continue

                # Send message to the bot, wait for response
                try:
                    request = client.add_message(bot_id, user_input)
                except Exception as e:
                    print(f"{COLOR_RED}ERROR: {e}.{COLOR_RESET}")
                    continue
                response = client.get_response(request["bot_id"], request["request_id"], timeout_seconds=RESPONSE_TIMEOUT_SECONDS)
                if not response:
                    print(f"{COLOR_RED}ERROR: No response from bot {request['bot_id']} within {RESPONSE_TIMEOUT_SECONDS} seconds.{COLOR_RESET}")
                    continue

                # Print the bot's response
                print(f"{COLOR_BLUE}{request['bot_id']}:{COLOR_RESET} {COLOR_CYAN}{response}{COLOR_RESET}")

            except (EOFError, KeyboardInterrupt):
                print(EXIT_MSG)
                break

if __name__ == "__main__":
    main()
