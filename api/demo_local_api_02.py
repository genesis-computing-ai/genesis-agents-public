import time
import os
from api.genesis_api import GenesisAPI
from api.genesis_base import bot_client_tool
from api.snowflake_local_server import GenesisLocalServer

from dotenv import load_dotenv
load_dotenv()  # load environment variables

scope, sub_scope = os.getenv("GENESIS_INTERNAL_DB_SCHEMA", "GENESIS_TEST.GENSIS_LOCAL").split(".")

saved_translated_phrases = None

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
    Retrurns an HTML table showing english pharases and their translations to a foreign language. The table has two two columns: 'Orig Phrase' and 'Translation'
    """
    return saved_translated_phrases


with GenesisAPI(server_type=GenesisLocalServer,
                scope=scope, sub_scope=sub_scope, fast_start=False) as client:


    bots = client.get_all_bots()
    print(f"\n>>> Found the following Bots registered with the client: {bots}")

    request = client.add_message("Janice", "hello, Can you translate phrases in English to Swedish? Show me an example.") # "Janice"
    response = client.get_response("Janice", request["request_id"])
    print("\n>>>>", response)

    client.add_client_tool("Janice", retrieve_english_phrase, timeout_seconds=2)

    request = client.add_message("Janice", "Use a tool to fetch 5 random famous English phrases and translate them to Swedish. "
                                           "Return the result as an HTML table with two columns: 'Orig Phrase' and 'Translation'.")
    response = client.get_response("Janice", request["request_id"])

    print("\n>>>>", response)
    saved_translated_phrases = response

    client.add_client_tool("Janice", get_all_translated_phrases)

    request = client.add_message("Janice", "Fetch a table of previously translated phrases using the proper tool. "
                                 "For each tranlsation of a phrase, detect its language and translate it to english. "
                                 "Return a plain text table with 4 columns: 'orig phrase', 'translation', langage of translation', and 'translted to english'.")
    response = client.get_response("Janice", request["request_id"])
    print("\n>>>>", response