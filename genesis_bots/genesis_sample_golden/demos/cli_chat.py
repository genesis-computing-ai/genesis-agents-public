import argparse
import os
from uuid import UUID, uuid4
os.environ['LOG_LEVEL'] = 'WARNING' # control logging from GenesisAPI
from genesis_bots.api import GenesisAPI, build_server_proxy
from genesis_bots.api.utils import add_default_argparse_options
from collections import defaultdict


# color constants
COLOR_YELLOW = "\033[93m"
COLOR_RED = "\033[91m"
COLOR_BLUE = "\033[94m"
COLOR_CYAN = "\033[96m"
COLOR_RESET = "\033[0m"
COLOR_GREEN = "\033[92m"


RESPONSE_TIMEOUT_SECONDS = 20.0

EXIT_MSG = "Exiting chat. Goodbye!"

def parse_arguments():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="A simple CLI chat interface to Genesis bots")
    add_default_argparse_options(parser)
    return parser.parse_args()


def get_available_bots(client: GenesisAPI) -> list[str]:
    all_bot_configs = client.list_available_bots()
    all_bot_ids = sorted([bot.bot_id for bot in all_bot_configs])
    return all_bot_ids


def main():
    args = parse_arguments()
    server_proxy = build_server_proxy(args.server_url, args.snowflake_conn_args)
    curr_bot_id:str = None
    bot_to_thread_map:dict[str, UUID] = defaultdict(lambda: uuid4())  # maps bot_id to thread_id

    with GenesisAPI(server_proxy=server_proxy) as client:
        welcome_msg = "\nWelcome to the Genesis chat interface. Type '/quit' to exit."
        if curr_bot_id is None:
            welcome_msg += "\nStart your first message with @<bot_id> to chat with that bot. Use it again to switch bots."
        else:
            welcome_msg += f"\nYou are chatting with bot {curr_bot_id}. Start your message with @<bot_id> to switch bots."
            bot_to_thread_map[curr_bot_id] = uuid4()
        print(welcome_msg)
        print("-"*len(welcome_msg))

        available_bots = None
        while True:
            try:
                # Prompt user for input
                user_promt = f"[You->{curr_bot_id}]" if curr_bot_id else "[You]"
                user_input = input(f"{COLOR_GREEN}{user_promt}: {COLOR_RESET}")
                user_input = user_input.strip()

                # Check for exit condition
                if user_input.lower() == '/quit':
                    print(EXIT_MSG)
                    break
                # Set/switch curr_bot_id if input starts with '@'
                if user_input.startswith('@'):
                    parts = user_input.split(maxsplit=1)
                    new_bot_id = parts[0][1:] # Remove the '@' character
                    user_input = parts[1] if len(parts) > 1 else ''
                    if curr_bot_id is None or new_bot_id.lower() != curr_bot_id.lower(): # force refresh if switching/new bot
                        available_bots = get_available_bots(client)
                        # match new bot_id to available bots (case insensitive)
                        curr_bot_id = None
                        for bot_id in available_bots:
                            if bot_id.lower() == new_bot_id.lower():
                                curr_bot_id = bot_id
                                break
                        if not curr_bot_id:
                            print(f"{COLOR_RED}ERROR: Invalid bot id '{new_bot_id}'. Available bots: {', '.join(available_bots)}.{COLOR_RESET}")
                            continue
                if available_bots is None:
                    available_bots = get_available_bots(client)
                if not curr_bot_id:
                    print(f"{COLOR_RED}Start your chat with a bot by using the '@<bot_id>' prefix. Available bots: {', '.join(available_bots)}.{COLOR_RESET}")
                    continue
                # Skip empty messages
                if not user_input:
                    continue

                # Send message to the bot, wait for response
                thread_id = bot_to_thread_map[curr_bot_id]
                try:
                    request = client.submit_message(curr_bot_id, user_input, thread_id=thread_id)
                except Exception as e:
                    print(f"{COLOR_RED}ERROR: {e}.{COLOR_RESET}")
                    continue
                response = client.get_response(request.bot_id, request.request_id, timeout_seconds=RESPONSE_TIMEOUT_SECONDS)
                if not response:
                    print(f"{COLOR_RED}ERROR: No response from bot {request.bot_id} within {RESPONSE_TIMEOUT_SECONDS} seconds.{COLOR_RESET}")
                    continue

                # Print the bot's response
                print(f"{COLOR_BLUE}[{request['bot_id']}]: {COLOR_RESET} {COLOR_CYAN}{response}{COLOR_RESET}")

            except (EOFError, KeyboardInterrupt):
                print(EXIT_MSG)
                break

if __name__ == "__main__":
    main()
