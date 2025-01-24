"""
This script serves as a demonstration of how to interact with Genesis bots using the Genesis API.
It showcases the following functionalities:
1. Fetching and displaying infromation on avaialable baseball teams from a demo database, without explicitly specifying
   what table or query to use.
2. Asking the bot to calculate some stat like win/lose ratio based on input from the user.

See the command line options for more information on how to connect to a Genesis bot server.
"""

import argparse
from   genesis_bots.api         import GenesisAPI, build_server_proxy
from   genesis_bots.api.utils   import add_default_argparse_options

bot_id = "Eve"

def parse_arguments():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    add_default_argparse_options(parser)
    return parser.parse_args()

def print_teams_info(client):
    # Ask the bot 'Eve' to return a JSON table of all teams
    msg = (
        "Return a nicely formatted text table containing the lists of top 30 teams in our demo baseball database, "
        "ordered by the total number of games played. The table should contain the following columns: 'team_code', "
        "'team name'. 'total_games_played'. return ONLY the table with no additional text or comments."
    )
    request = client.submit_message(bot_id, msg)
    response = client.get_response(bot_id, request.request_id)
    print(response)

def get_team_win_lose_ratio(client, team_code, year=None):
    # Ask the bot 'Eve' to fetch the win/lose ratio for a given team and optional year
    msg = f"Fetch the win/lose ratio for the team with code '{team_code}'"
    if year:
        msg += f" for the year {year}."
    else:
        msg += " over all available data. "
    msg += (
        "If you have the infromation, return ONLY a text table containing the following information: total wins, "
        "total losses, win/lose ratio, and [year, if provided]. Also provide a proper header for the table. "
        "Do not add any additional text or comments. If you don't have the information, explain why you don't have "
        "the information and sugget a way to get the information."
    )
    request = client.submit_message(bot_id, msg)
    response = client.get_response(bot_id, request.request_id)
    return response

def main():
    args = parse_arguments()
    server_proxy = build_server_proxy(args.server_url, args.snowflake_conn_args)
    with GenesisAPI(server_proxy=server_proxy) as client:
        # Print the table with all the teams
        print("Fetching baseball teams information...")
        print_teams_info(client)

        while True:
            # Ask the user to enter a team code and an optional year
            team_code = input(
                ">> To show win/lose ratio for a specific team, enter a team code (or 'quit' to exit): "
            ).strip()
            if team_code.lower() in ['quit', 'exit']:
                break
            if not team_code:
                continue
            year = input(">> Enter a year (optional): ").strip()
            year = year if year else None

            # Fetch and print the win/lose ratio
            win_lose_ratio = get_team_win_lose_ratio(client, team_code, year)
            print(win_lose_ratio)

if __name__ == "__main__":
    main()
