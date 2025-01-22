import argparse
from   genesis_bots.api         import GenesisAPI, build_server_proxy, bot_client_tool
from   genesis_bots.api.utils   import add_default_argparse_options

saved_translated_phrases = [] # accumulator for translated phrases

@bot_client_tool(phrase_number="phrase_number (int): The phrase number to retrieve from a list of phrases. "
                   "There is no minimum or maximum value - modulo is used to wrap around the list." )
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


@bot_client_tool()
def get_all_translated_phrases() -> str:
    """
    Retrurns an table showing english pharases and their translations to a foreign language. The table has two two columns: 'Orig Phrase' and 'Translation'
    """
    global saved_translated_phrases
    return saved_translated_phrases


def parse_arguments():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="A simple CLI chat interface to Genesis bots")
    add_default_argparse_options(parser)
    return parser.parse_args()

def main():
    args = parse_arguments()
    server_proxy = build_server_proxy(args.server_url, args.snowflake_conn_args)
    with GenesisAPI(server_proxy=server_proxy) as client:
        req_msg = "hello, Can you translate phrases in English to Swedish? Show me an example."
        print(f"\n>>>> Requesting: {req_msg}")
        request = client.add_message("Janice", req_msg) # "Janice"
        response = client.get_response("Janice", request["request_id"])
        print("\n>>>>", response)

        client.register_client_tool("Janice", retrieve_english_phrase, timeout_seconds=2)

        req_msg = ("Use a tool to fetch 5 random famous English phrases and translate them to Swedish. " \
                "Return the result as a nicely formatted text table with two columns: 'Orig Phrase' and 'Translation'.")
        print(f"\n>>>> Requesting: {req_msg}")
        request = client.add_message("Janice", req_msg)
        response = client.get_response("Janice", request["request_id"])
        print("\n>>>>", response)
        global saved_translated_phrases
        saved_translated_phrases = response # save the response so that it can be served by the get_all_translated_phrases tool

        client.register_client_tool("Janice", get_all_translated_phrases)

        req_msg = ("Fetch a table of previously translated phrases using the proper tool. "
                                    "For each tranlsation of a phrase, detect its language and translate it to english. "
                                    "Return a plain text table with 4 columns: 'orig phrase', 'translation', langage of translation', and 'translted to english'.")
        print(f"\n>>>> Requesting: {req_msg}")
        request = client.add_message("Janice", req_msg)
        response = client.get_response("Janice", request["request_id"])
        print("\n>>>>", response)


if __name__ == "__main__":
    main()
