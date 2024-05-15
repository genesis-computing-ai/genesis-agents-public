import os

slack_tools_descriptions = [
    {
        "type": "function",
        "function": {
            "name": "_send_slack_direct_message",
            "description": "Send a direct message to a colleague on slack.  Only use this when directed to by a user.  DO NOT USE THIS TO RESPOND TO A REGULAR CHAT THREAD.",
            "parameters": {
                "type": "object",
                "properties": {
                    "slack_user_id": {"type": "string", "description": "The slack user id to send a message to. Use _lookup_slack_user_id first to confirm the Slack ID of the user. ONLY USE THIS TO REACH OUT TO USERS YOU'RE NOT CURRENTLY TALKING TO, NOT TO RESPOND TO GENERAL CHAT THREADS"},
                    "message": {"type": "string", "description": "The text of the message to send directly to the user that you are not already talking to in the conversation thread.  Include any links to local documents referencing ./downloaded_files or the openAI file id. Use this format to reference files: ![file description](./downloaded_files/thread_<threadid>/<file name>)"},
                },
                "required": ["slack_user_id", "message"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "_send_slack_channel_message",
            "description": "Send a message to a specified Slack channel. This can be used for broadcasting messages or updates to a group of users within a channel.",
            "parameters": {
                "type": "object",
                "properties": {
                    "channel_id": {"type": "string", "description": "The NAME of the channel to send the message to, e.g. #channel"},
                    "message": {"type": "string", "description": "The text of the message to be sent to the channel.  Include any links to local documents referencing ./downloaded_files or the openAI file id. Use this format to reference files: ![file description](./downloaded_files/thread_<threadid>/<file name>)"},
                },
                "required": ["channel_id", "message"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "_lookup_slack_user_id",
            "description": "Lookup the slack user id for a user based on their name.  Only use this if you don't know the slack_id of a user already, and only if you need to send that user a direct message or tag them in a message.",
            "parameters": {
                "type": "object",
                "properties": {
                    "user_name": {"type": "string", "description": "Name of the user to find the slack user id for."},
                },
                "required": ["user_name"]
            }
        }
    }
]                         

slack_tools = {"_send_slack_direct_message": "slack_adapter_local.send_slack_direct_message", 
               "_send_slack_channel_message": "slack_adapter_local.send_slack_channel_message",
               "_lookup_slack_user_id": "slack_adapter_local.lookup_slack_user_id"}

def bind_slack_available_functions(slack_adapter):
    return {"_send_slack_direct_message": slack_adapter.send_slack_direct_message,
            "_send_slack_channel_message": slack_adapter.send_slack_channel_message,
            "_lookup_slack_user_id": slack_adapter.lookup_slack_user_id }

