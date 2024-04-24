import requests


def send_email_via_webhook(to, subject, body):
    webhook_url = 'https://hooks.zapier.com/hooks/catch/18384838/3xpvsgp/'
    payload = {
        'to': to,
        'subject': subject,
        'body': body
    }
    
    response = requests.post(webhook_url, json=payload)
    return response.status_code, response.text


def test_send_email_via_webhook():
    test_to = 'justin.langseth@gmail.com'
    test_subject = 'test'
    test_body = 'This is a test email.'
    status_code, _ = send_email_via_webhook(test_to, test_subject, test_body)
    return status_code == 200

# Start of Generated Description
TOOL_FUNCTION_DESCRIPTION_SEND_EMAIL_VIA_WEBHOOK = {
    "type": "function",
    "function": {
        "name": "send_email_via_webhook.send_email_via_webhook",
        "description": "Sends an email via a specified Zapier webhook, accepting parameters for the recipient's email (to), the subject of the email (subject), and the body of the email (body).",
        "parameters": {
            "type": "object",
            "properties": {
                "to": {
                    "type": "string",
                    "description": "No description"
                },
                "subject": {
                    "type": "string",
                    "description": "No description"
                },
                "body": {
                    "type": "string",
                    "description": "No description"
                }
            },
            "required": [
                "to",
                "subject",
                "body"
            ]
        }
    }
}
# End of Generated Description

# Start of Generated Mapping
send_email_via_webhook_action_function_mapping = {'send_email_via_webhook.send_email_via_webhook': send_email_via_webhook}
# End of Generated Mapping
