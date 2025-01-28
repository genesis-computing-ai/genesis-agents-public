"""
This script demonstares using the Genesis API to interacts with a bot that can invoke client-side tools (functions defined in this script)
using the @bot_client_tool decorator.
In this simple example we pretend we have a local databse of english phrases from which we ask the bot to fetch data and translate 
it to Swedish. We saved these translations in local memory and ask the bots to fetch those and translate them to back to English

See  --help for more information.
"""
import argparse
from   genesis_bots.api         import GenesisAPI, build_server_proxy, bot_client_tool
from   genesis_bots.api.utils   import add_default_argparse_options

saved_translated_phrases = "" # Our local "database" for translated phrases

@bot_client_tool(phrase_number=("phrase_number (int): The phrase number to retrieve from a list of phrases. "
                                 "There is no minimum or maximum value - modulo is used to wrap around the list." ))
def retrieve_english_phrase(phrase_number: int) -> str:
    """
    Retrieves a specific paragraph from a predefined list of english phrases

    Returns:
        str: The requested paragraph as a string.
    """
    phrases = [
        "The quick brown fox jumps over the lazy dog.",
        "She sells seashells by the seashore.",
        "How much wood would a woodchuck chuck if a woodchuck could chuck wood?",
        "A journey of a thousand miles begins with a single step.",
        "To be or not to be, that is the question.",
        "All that glitters is not gold.",
        "The early bird catches the worm.",
        "A picture is worth a thousand words.",
        "When in Rome, do as the Romans do.",
        "Actions speak louder than words."
    ]
    phrase = phrases[phrase_number % len(phrases)]
    return phrase

BOT_ID = "Eve"

@bot_client_tool()
def get_all_translated_phrases() -> str:
    """
    Retrurns a table showing english pharases and their translations to a foreign language. 
    The table has two two columns: 'Orig Phrase' and 'Translation'
    """
    global saved_translated_phrases
    return saved_translated_phrases


def parse_arguments():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description=__doc__)
    add_default_argparse_options(parser)
    return parser.parse_args()

def main():
    args = parse_arguments()
    server_proxy = build_server_proxy(args.server_url, args.snowflake_conn_args)
    with GenesisAPI(server_proxy=server_proxy) as client:
        # Start a conversation
        req_msg = f"Hello, {BOT_ID}. Can you translate phrases in English to Swedish? Show me an example."
        print(f"\n>>>> Requesting: {req_msg}")
        request = client.submit_message(BOT_ID, req_msg)
        response = client.get_response(BOT_ID, request.request_id)
        print("\n>>>>", response)

        # Give the bot a new client-side tool
        client.register_client_tool(BOT_ID, retrieve_english_phrase, timeout_seconds=2)

        req_msg = ("Use a tool to fetch 5 random famous English phrases and translate them to Swedish. " \
                "Return the result as a nicely formatted text table with two columns: 'Orig Phrase' and 'Translated Phrase'.")
        print(f"\n>>>> Requesting: {req_msg}")
        request = client.submit_message(BOT_ID, req_msg)
        response = client.get_response(BOT_ID, request.request_id)
        print("\n>>>>", response)
        global saved_translated_phrases
        saved_translated_phrases = response # save the response so that it can be served by the get_all_translated_phrases tool

        # Give the bot another client-side tool
        client.register_client_tool(BOT_ID, get_all_translated_phrases)

        req_msg = ("Fetch a table of previously translated phrases using the proper tool. "
                   "For each translated phrase, detect what foreign language it is in and translate it back to English. "
                   "Return a plain text table with 3 columns: 'Foreign phrase', 'Foreign language', 'English translation'")
        print(f"\n>>>> Requesting: {req_msg}")
        request = client.submit_message(BOT_ID, req_msg)
        response = client.get_response(BOT_ID, request.request_id)
        print("\n>>>>", response)


if __name__ == "__main__":
    main()
