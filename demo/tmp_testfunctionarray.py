

from slack.slack_tools import bind_slack_available_functions, slack_tools
from slack.slack_bot_os_adapter import SlackBotAdapter
import json, os

slack_adapter = SlackBotAdapter(token=os.getenv("SLACK_APP_TOKEN"), signing_secret=os.getenv("SLACK_APP_SIGNING_SECRET"), # type: ignore
                                channel_id=os.getenv("SLACK_CHANNEL"), # type: ignore
                                bot_user_id=os.getenv("SLACK_BOT_USERID")) # type: ignore



# Assuming bind_slack_available_functions returns a dictionary of functions
available_functions = bind_slack_available_functions(slack_adapter)

# Convert the functions to a dictionary of their names
function_names = {name: func.__name__ for name, func in available_functions.items()}

# Now you can convert this dictionary to a string
function_names_str = json.dumps(function_names)


print(function_names_str)


function_names_new = json.loads(function_names_str)

available_functions_new = {name: getattr(slack_tools, func_name) for name, func_name in function_names.items()}

print(available_functions_new)
