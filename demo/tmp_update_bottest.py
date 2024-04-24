

import requests
import json

# Your app configuration token with required scopes
token = 'xoxe.xoxp-1-Mi0yLTY1NTA2NTAyNjA0NDgtNjUzMDM0NDc0ODkxNi02Nzg3MzM0MTQyOTkzLTY4NjY2NDUwMzIxNDUtMjM3ZWU4ZGJjMjYxZDk3NTMxYzJjOTRmNmU2YjZkYWZiYzdjNDY1NDY0NGY4OTc1YjEwMzA0OGE3ODgyMDBlYw'

# Your app manifest encoded as a string
manifest = json.dumps({
    "display_information": {
        "name": "BabyElsaNewName",
        "description": "Elsa (Baby)",
        "background_color": "#292129"
    },
    "features": {
        "app_home": {
            "home_tab_enabled": True,
            "messages_tab_enabled": True,
            "messages_tab_read_only_enabled": False
        },
        "bot_user": {
            "display_name": "BabyElsaNewName",
            "always_online": True
        }
    },
    "oauth_config": {
        "scopes": {
            "bot": [
                "channels:history",
                "chat:write",
                "files:read",
                "files:write",
                "im:history",
                "im:read",
                "im:write",
                "im:write.invites",
                "im:write.topic",
                "mpim:history",
                "mpim:read",
                "mpim:write",
                "mpim:write.invites",
                "mpim:write.topic",
                "users:read",
                "users:read.email"
            ]
        }
    },
    "settings": {
        "event_subscriptions": {
            "request_url": "https://310d-141-239-172-58.ngrok-free.app/slack/events",
            "bot_events": [
                "message.channels",
                "message.im"
            ]
        },
        "org_deploy_enabled": False,
        "socket_mode_enabled": False,
        "token_rotation_enabled": False
    }
})

app_id = "A06QXAJ6LDU"

response = requests.post('https://slack.com/api/apps.manifest.update', headers={
    'Authorization': f'Bearer {token}',
    'Content-Type': 'application/json'
}, json={
    'app_id': app_id,
    'manifest': manifest
})

print(response.json())




# (.venv) justin@justins-mbp bot_os % /Users/justin/Documents/bot_os/.venv/bin/python /Users/justin/Documents/bot_os/demo/tmp_makebottest.py
# {'ok': True, 'app_id': 'A06QXAJ6LDU', 'credentials': {'client_id': '6550650260448.6847358224470', 'client_secret': '2bf9fd019c96a3ee34fb658bdcd7192d', 'verification_token': '74c06LccyoPcSDNnt8adzIPP', 'signing_secret': '5b9de84ff2285a24bcccdb0fc1403cc4'}, 'oauth_authorize_url': 'https://slack.com/oauth/v2/authorize?client_id=6550650260448.6847358224470&scope=channels:history,chat:write,files:read,files:write,im:history,im:read,im:write,im:write.invites,im:write.topic,mpim:history,mpim:read,mpim:write,mpim:write.invites,mpim:write.topic,users:read,users:read.email', 'warning': 'missing_charset', 'response_metadata': {'warnings': ['missing_charset']}}
