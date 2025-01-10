import time
import os
from api.genesis_api import GenesisAPI
from api.genesis_base import bot_client_tool
from api.snowflake_local_server import GenesisLocalServer

from dotenv import load_dotenv
load_dotenv()  # load environment variables

scope, sub_scope = os.getenv("GENESIS_INTERNAL_DB_SCHEMA", "GENESIS_TEST.GENSIS_LOCAL").split(".")

@bot_client_tool(phrase_number="phrase_number (int): The phrase number to retrieve from a list of phrases. "
                   "There is no minimum or maximum value - modulo is used to wrap around the list." )
def retrieve_english_phrase(phrase_number: int) -> str:
    """
    Retrieves a specific paragraph from a predefined list of story paragraphs.
    
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
    return phrases[phrase_number % len(phrases)]


with GenesisAPI(server_type=GenesisLocalServer,
                scope=scope, sub_scope=sub_scope, fast_start=False) as client:


    bots = client.get_all_bots()
    print(f"\n>>> Found the following Bots registered with the client: {bots}")

    request = client.add_message("Janice", "hello, Can you translate phrases in English to Swedish? Show me an example.") # "Janice"
    response = client.get_response("Janice", request["request_id"])
    print("\n>>>>", response)

    client.add_client_tool("Janice", retrieve_english_phrase)

    request = client.add_message("Janice", "Use the tool 'retrieve_english_phrase' to fetch 5 random famous English phrases and translate them to Swedish. Show me the phrase and the translation side by side in a table.")
    response = client.get_response("Janice", request["request_id"])
    print("\n>>>>", response)
