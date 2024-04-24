import base64
import os
from openai import OpenAI
import requests

def encode_image(image_file):
    with open(image_file, "rb") as f:
        return base64.b64encode(f.read()).decode('utf-8')

# Define the function to analyze the image using OpenAI's Chat API for Vision
def vision_chat_analysis(image_data, query):#, detail='auto'):
    if image_data.startswith('http'):
        raise(Exception("need to pass uploaded file_id only for now"))
    else:
        image_payload = encode_image(image_data)

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {os.getenv('OPENAI_API_KEY')}"
    }

    payload = {
        "model": "gpt-4-vision-preview",
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": query
                    },
                    {
                        "type": "image_url",
                        "image_url": {
                            "url": f"data:image/jpeg;base64,{image_payload}"
                        }
                    }
                ]
            }
        ],
        "max_tokens": 300
    }

    response = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload)
    return response.text

# Placeholder test function for the 'vision_chat_analysis'
def test_vision_chat_analysis():
    return 'Test function present but not executed; replace with actual tests.'

# Start of Generated Description
TOOL_FUNCTION_DESCRIPTION_VISION_CHAT_ANALYSIS = {
    "type": "function",
    "function": {
        "name": "vision_chat_analysis--vision_chat_analysis",
        "description": "Analyzes images using OpenAI's Chat API for Vision, with the capability to set detail level if required.",
        "parameters": {
            "type": "object",
            "properties": {
                "image_data": {
                    "type": "string",
                    "description": "No description"
                },
                "query": {
                    "type": "string",
                    "description": "No description"
                },
            },
            "required": [
                "image_data",
                "query"
            ]
        }
    }
}
# End of Generated Description

# Start of Generated Mapping
vision_chat_analysis_action_function_mapping = {'vision_chat_analysis--vision_chat_analysis': vision_chat_analysis}
# End of Generated Mapping
