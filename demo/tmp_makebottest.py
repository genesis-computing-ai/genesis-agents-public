

import requests
import json

# Your app configuration token with required scopes
token = 'xoxe.xoxp-1-Mi0yLTY1NTA2NTAyNjA0NDgtNjUzMDM0NDc0ODkxNi02Nzg3MzM0MTQyOTkzLTY4NTA5NjU4ODg5ODItYzI5ZDAxMmEzZWYzY2RmMGE0ZmZkZDFmYjc0ZmY4MmE3NTRiZTMwMzUzNGI0M2RmNjM1ZDk4NDEzMmMyNDMxOA'
refresh_token = 'xoxe-1-My0xLTY1NTA2NTAyNjA0NDgtNjc4NzMzNDE0Mjk5My02ODQzMDM5NDk2MDM5LWFjYTRhZTk5Y2VjMmRhYTcxZThkNDdiYjhlNTcyZjgwNWMzZmRiZmNjNmYwZWExNjlmOTRkMTdmMTc5NWI4MGQ'

# Your app manifest encoded as a string
manifest = json.dumps({
    "display_information": {
        "name": "Eve-JL",
        "description": "Eve-JL",
        "background_color": "#292129"
    },
    "features": {
        "app_home": {
            "home_tab_enabled": True,
            "messages_tab_enabled": True,
            "messages_tab_read_only_enabled": False
        },
        "bot_user": {
            "display_name": "BabyElsa2",
            "always_online": True
        }
    },
    "oauth_config": {
        "redirect_urls": [
             "https://310d-141-239-172-58.ngrok-free.app/slack/events/Eve-JL/install" ],
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
            "request_url": "https://310d-141-239-172-58.ngrok-free.app/slack/events/Eve-JL",
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

response = requests.post('https://slack.com/api/apps.manifest.create', headers={
    'Authorization': f'Bearer {token}',
    'Content-Type': 'application/json'
}, json={
    'manifest': manifest
})

print(response.json())



# eve JL
#{'ok': True, 'app_id': 'A06R3A1S0LW', 'credentials': {'client_id': '6550650260448.6853341884710', 'client_secret': 'b6d9c33151d3ddb7704bd16d5a0442c1', 'verification_token': 'zFR9W6EoI6Inj6lQTXeVeycP', 'signing_secret': '4ef684e61111ea8f515baf0befd631ea'}, 'oauth_authorize_url': 'https://slack.com/oauth/v2/authorize?client_id=6550650260448.6853341884710&scope=channels:history,chat:write,files:read,files:write,im:history,im:read,im:write,im:write.invites,im:write.topic,mpim:history,mpim:read,mpim:write,mpim:write.invites,mpim:write.topic,users:read,users:read.email', 'warning': 'missing_charset', 'response_metadata': {'warnings': ['missing_charset']}}

# Eve-JL code:  6550650260448.6872656086881.e243e8c9b44a0bd4fafb630d7dbe5bde6983d3702a770068dd071fe7e5516c02 state 
token data:  {'ok': True, 'app_id': 'A06R3A1S0LW', 'authed_user': {'id': 'U06FLA4N0SY'}, 'scope': 'channels:history,chat:write,files:read,files:write,im:history,im:read,im:write,im:write.invites,im:write.topic,mpim:history,mpim:read,mpim:write,mpim:write.invites,mpim:write.topic,users:read,users:read.email', 'token_type': 'bot', 'access_token': 'xoxb-6550650260448-6860001078754-dWwiznPHUbyPgtBer5yZEAuR', 'bot_user_id': 'U06RA012AN6', 'team': {'id': 'T06G6K47ND6', 'name': 'Hello World'}, 'enterprise': None, 'is_enterprise_install': False}
