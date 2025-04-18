# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.

from botbuilder.core import ActivityHandler, TurnContext
from botbuilder.schema import ChannelAccount, ActivityTypes, Activity
import asyncio
from datetime import timedelta
import logging
import requests
import json
import sys
import os
import traceback
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Import the JWT functions from jwt_funcs.py
from jwt_funcs import _get_token, token_exchange, connect_to_spcs

logger = logging.getLogger(__name__)

# Read private key from file
def read_private_key():
    """
    Read the private key from the private_key.pem file
    
    Returns:
        str: The private key as a string
    """
    private_key_path = os.path.join(os.path.dirname(__file__), "private_key.pem")
    try:
        with open(private_key_path, "r") as key_file:
            return key_file.read()
    except Exception as e:
        logger.error("Failed to read private key file: %s", e)
        raise

# Get private key from file
PRIVATE_KEY = read_private_key()

global_token = None
global_url = None

conversation_history = {}
thread_id = None

class Args:
    def __init__(self):
        self.account = os.getenv("SNOWFLAKE_ACCOUNT")
        self.user = os.getenv("SNOWFLAKE_USER")
        self.role = os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN")
        self.private_key = PRIVATE_KEY  # Use the key directly instead of file path
        self.endpoint = os.getenv("SNOWFLAKE_ENDPOINT_ALPHA") if os.getenv("PLATFORM") == "ALPHA" else os.getenv("SNOWFLAKE_ENDPOINT_DEV") if os.getenv("PLATFORM") == "DEV" else os.getenv("NGROK_ENDPOINT")
        self.endpoint_path = os.getenv("ENDPOINT_PATH", "")
        self.lifetime = int(os.getenv("TOKEN_LIFETIME", "59"))
        self.renewal_delay = int(os.getenv("TOKEN_RENEWAL_DELAY", "54"))
        self.snowflake_account_url = os.getenv("SNOWFLAKE_ACCOUNT_URL", None)


def main():
    """
    Main function to run the bot
    """
    global global_token
    
    # Check if we're in NGROK mode and skip token generation if so
    platform = os.getenv("PLATFORM", "")
    if platform.upper() == "NGROK":
        logger.info("NGROK mode detected, skipping token generation")
        global_token = "dummy_token_for_ngrok"
    else:
        # Generate token for non-NGROK platforms
        login()
    
    resp = send_message("Hi")
    print(resp)

def login():
    """
    Login to Snowflake and connect to SPCS
    """
    global global_token
    
    # Check if we're in NGROK mode
    platform = os.getenv("PLATFORM", "")
    if platform.upper() == "NGROK":
        logger.info("NGROK mode detected, skipping token generation")
        # Return a dummy token for NGROK mode
        return "dummy_token_for_ngrok"
    
    try:
        # Initialize JWT generator with args from environment
        args = Args()
        generator = JWTGenerator(
            args.account, args.user, args.role, args.private_key, args.lifetime, args.renewal_delay
        )
        
        # Generate a new token
        token = generator.get_token()
        global_token = token
        return token
    except Exception as e:
        logger.error(f"Error generating token: {str(e)}")
        logger.error(f"Token generation error traceback: {traceback.format_exc()}")
        # Return a dummy token so we can continue in degraded mode
        return "error_generating_token"

def call_submit_udf(token, url, bot_id, row_data, conversation_id=None, thread_id=None, file=None):
    """
    Call the submit_udf endpoint with proper authentication

    Args:
        token: Snowflake JWT token
        url: Base SPCS URL
        bot_id: Bot ID to include in request
        row_data: Message text to process
        conversation_id: Optional conversation ID
        thread_id: Optional thread ID to associate with request
        file: Optional file data to include
    """
    try:
        logger.info(
            f"Enter call submit udf - url: {url} bot_id: {bot_id} row_data: {row_data} conversation_id: {conversation_id} thread_id: {thread_id}"
        )

        # Check if URL is None or empty and use a fallback
        if not url:
            # Try to get the endpoint from environment variables
            platform = os.getenv("PLATFORM", "NGROK")
            if platform == "ALPHA":
                url = os.getenv("SNOWFLAKE_ENDPOINT_ALPHA")
            elif platform == "DEV":
                url = os.getenv("SNOWFLAKE_ENDPOINT_DEV")
            else:
                url = os.getenv("NGROK_ENDPOINT")
            
            # If still None, log error and return
            if not url:
                logger.error("No valid URL found for Snowflake endpoint. Please check your environment variables.")
                return {"status_code": 500, "text": "No valid URL found", "headers": {}}
            
            # Ensure URL has a proper schema
            if not url.startswith("http://") and not url.startswith("https://"):
                # Use HTTP for localhost, HTTPS for everything else
                if "localhost" in url or "127.0.0.1" in url:
                    url = f"http://{url}"
                else:
                    url = f"https://{url}"
            
            logger.info(f"Using fallback URL: {url}")

        # Check if we're in NGROK mode and skip token authentication if so
        platform = os.getenv("PLATFORM", "")
        if platform.upper() == "NGROK":
            logger.info("NGROK mode detected, skipping token authentication")
            headers = {"Content-Type": "application/json"}
        else:
            # Use token authentication for non-NGROK platforms
            headers = {"Authorization": f"Snowflake Token=\"{token}\"", "Content-Type": "application/json"}

        # Format bot_id as JSON object - include runner_id to avoid NoneType error in server
        bot_details = {
            "bot_id": bot_id,
            "runner_id": "teams_adapter",  # Add runner_id to prevent NoneType error in server
            "platform": "teams"            # Add platform information
        }
        bot_id_json = json.dumps(bot_details)

        # Process file data if present
        file_json = None
        if file:
            try:
                # Log file structure for debugging
                logger.info(f"File data structure: {file}")

                # Make sure the file has a 'filename' key which the server expects
                if 'name' in file and 'filename' not in file:
                    file['filename'] = file['name']  # Add filename key which server expects
                elif 'filename' not in file:
                    file['filename'] = "attachment.dat"  # Default filename if none provided
                
                # Make sure the file has a 'content' key which the server expects
                if 'content' not in file:
                    file['content'] = ""  # Add empty content if missing
                
                # Convert file content to base64 if it exists and is not already encoded
                if file['content'] and not isinstance(file['content'], str):
                    import base64
                    # If content is bytes, encode to base64
                    file['content'] = base64.b64encode(file['content']).decode('utf-8')

                # Convert file data to JSON string
                file_json = json.dumps(file)
                logger.info(f"Processed file attachment: {file.get('filename', 'unnamed')} ({file.get('contentType', 'unknown type')})")
            except Exception as e:
                logger.error(f"Error processing file attachment: {str(e)}")
                logger.error(f"File processing error traceback: {traceback.format_exc()}")
                file_json = None

        # Create data payload with optional file attachment
        data = {
            "data": [
                [0, row_data, thread_id, bot_id_json, file_json]  # Match input_rows structure
            ]
        }

        logger.info(f"Submit UDF payload structure: {json.dumps(data, default=str)[:200]}...")

        submit_url = f"{url}/udf_proxy/submit_udf"
        
        try:
            # Increase timeout to 60 seconds to allow more time for the server to respond
            response = requests.post(submit_url, headers=headers, json=data, timeout=60)
            
            logger.info(f"Submit UDF status code: {response.status_code}")
            logger.info(f"Submit UDF response: {response.text}")
            
            # Check if response is HTML (which might indicate an authentication issue)
            is_html = False
            if response.text and response.text.strip().startswith(('<!DOCTYPE', '<!doctype', '<html', '<HTML')):
                is_html = True
                logger.error(f"Received HTML response instead of JSON: {response.text[:200]}")
                # Check if it's a login page, which indicates authentication issues
                if 'login' in response.text.lower() or 'sign in' in response.text.lower():
                    logger.error("HTML response appears to be a login page, authentication may have failed")
            
            # Return a standardized response dictionary
            return {
                "status_code": response.status_code,
                "text": response.text,
                "headers": dict(response.headers),
                "is_html": is_html
            }
        except requests.exceptions.RequestException as e:
            logger.error(f"Request exception in call_submit_udf: {str(e)}")
            return {
                "status_code": 500,
                "text": f"Request error: {str(e)}",
                "headers": {},
                "is_html": False
            }
    except Exception as e:
        logger.error(f"Error in call_submit_udf: {str(e)}")
        logger.error(f"call_submit_udf error traceback: {traceback.format_exc()}")
        # Create a standardized response dictionary with error status
        return {
            "status_code": 500,
            "text": f"Error: {str(e)}",
            "headers": {},
            "is_html": False
        }

def call_lookup_udf(token, url, bot_id, uuid):
    """
    Call the lookup_udf endpoint with proper authentication

    Args:
        token: Snowflake JWT token
        url: Base SPCS URL
        bot_id: Bot ID to include in request
        uuid: UUID of the request to look up
    """
    try:
        # Check if URL is None or empty and use a fallback
        if not url:
            # Try to get the endpoint from environment variables
            platform = os.getenv("PLATFORM", "NGROK")
            if platform == "ALPHA":
                url = os.getenv("SNOWFLAKE_ENDPOINT_ALPHA")
            elif platform == "DEV":
                url = os.getenv("SNOWFLAKE_ENDPOINT_DEV")
            else:
                url = os.getenv("NGROK_ENDPOINT")
            
            # If still None, log error and return
            if not url:
                logger.error("No valid URL found for Snowflake endpoint. Please check your environment variables.")
                return {"status_code": 500, "text": "No valid URL found", "headers": {}}
            
            # Ensure URL has a proper schema
            if not url.startswith("http://") and not url.startswith("https://"):
                # Use HTTP for localhost, HTTPS for everything else
                if "localhost" in url or "127.0.0.1" in url:
                    url = f"http://{url}"
                else:
                    url = f"https://{url}"

        # Check if we're in NGROK mode and skip token authentication if so
        platform = os.getenv("PLATFORM", "")
        if platform.upper() == "NGROK":
            headers = {"Content-Type": "application/json"}
        else:
            headers = {"Authorization": f"Snowflake Token=\"{token}\"", "Content-Type": "application/json"}

        # Format bot_id as JSON object - include runner_id to avoid NoneType error in server
        bot_details = {
            "bot_id": bot_id,
            "runner_id": "teams_adapter",  # Add runner_id to prevent NoneType error in server
            "platform": "teams"            # Add platform information
        }
        bot_id_json = json.dumps(bot_details)

        # Create data payload - Based on server error, it's looking for input_rows[0][2]
        # So we need to structure our data as [1, uuid, bot_id]
        data = {
            "data": [
                [1, uuid, bot_id]  # Server expects index 2 to exist, so use format [1, uuid, bot_id]
            ]
        }

        lookup_url = f"{url}/udf_proxy/lookup_udf"
        
        try:
            # Log the payload for debugging
            logger.info(f"Lookup UDF payload: {json.dumps(data)}")
            
            # Increase timeout to 60 seconds to allow more time for the server to respond
            response = requests.post(lookup_url, headers=headers, json=data, timeout=60)
            
            # Log the response status and content
            logger.info(f"Lookup UDF status code: {response.status_code}")
            logger.info(f"Lookup UDF response: {response.text[:200]}")
            
            # Check if response is HTML (which might indicate an authentication issue)
            is_html = False
            if response.text and response.text.strip().startswith(('<!DOCTYPE', '<!doctype', '<html', '<HTML')):
                is_html = True
                logger.error(f"Received HTML response instead of JSON: {response.text[:200]}")
                # Check if it's a login page, which indicates authentication issues
                if 'login' in response.text.lower() or 'sign in' in response.text.lower():
                    logger.error("HTML response appears to be a login page, authentication may have failed")
            
            # Return a standardized response dictionary
            return {
                "status_code": response.status_code,
                "text": response.text,
                "headers": dict(response.headers),
                "is_html": is_html
            }
        except requests.exceptions.RequestException as e:
            logger.error(f"Request exception in call_lookup_udf: {str(e)}")
            return {
                "status_code": 500,
                "text": f"Request error: {str(e)}",
                "headers": {},
                "is_html": False
            }
    except Exception as e:
        logger.error(f"Error in call_lookup_udf: {str(e)}")
        logger.error(f"call_lookup_udf error traceback: {traceback.format_exc()}")
        # Create a standardized response dictionary with error status
        return {
            "status_code": 500,
            "text": f"Error: {str(e)}",
            "headers": {},
            "is_html": False
        }

def send_message(message, conversation_id=None, attachments=None):
    """
    Interactive chat test function that sends messages to a bot and polls for responses

    Args:
        message: The message to send
        conversation_id: Optional conversation ID
        attachments: Optional list of file attachments
    """
    import uuid
    import time
    import requests
    import base64
    import traceback  # Add traceback module for detailed error logging

    global thread_id
    global global_token

    # Get bot ID from environment - use "Eve" as default since we know it exists in the database
    bot_id = os.getenv("BOT_ID", "Eve")
    logger.info(f"send_messages message: {message} | BOT_ID: {bot_id}")

    if thread_id is None:
        thread_id = str(uuid.uuid4())  # Generate thread ID for conversation
        logger.info(f"Created new thread_id: {thread_id}")

    logger.info(f"Submitting uuid: {thread_id}")

    conversation_context = message
    logger.info(f"Submitting message: {conversation_context}")

    # Log attachment information if present
    if attachments:
        logger.info(f"Submitting with {len(attachments)} attachments")
        for i, attachment in enumerate(attachments):
            # Use get() method for dictionary access to avoid attribute errors
            logger.info(f"Attachment {i+1}: {attachment.get('name', 'Unnamed')} - {attachment.get('contentType', 'Unknown type')}")
            # Debug: Log the full attachment object structure
            logger.info(f"Attachment {i+1} full structure: {attachment}")

    try:
        # Process attachments if present
        file_data = None
        if attachments and len(attachments) > 0:
            try:
                # For now, we'll just take the first attachment
                # In a production system, you might want to handle multiple attachments
                attachment = attachments[0]

                # Log the attachment type for debugging
                logger.info(f"Attachment type: {type(attachment)}")

                # Create a JSON representation of the attachment
                file_data = {
                    "name": attachment.get("name", "file"),
                    "contentType": attachment.get("contentType", "application/octet-stream"),
                    "contentUrl": attachment.get("contentUrl", "")
                }

                # Try to download content from contentUrl if available
                if file_data["contentUrl"]:
                    try:
                        logger.info(f"Downloading content from URL: {file_data['contentUrl']}")
                        response = requests.get(file_data["contentUrl"], timeout=30)
                        if response.status_code == 200:
                            # Encode binary content as base64
                            file_data["content"] = base64.b64encode(response.content).decode('utf-8')
                            logger.info(f"Successfully downloaded content from URL ({len(response.content)} bytes)")
                        else:
                            logger.error(f"Failed to download content from URL: {response.status_code}")
                    except Exception as e:
                        logger.error(f"Error downloading content from URL: {str(e)}")
                        logger.error(f"Traceback: {traceback.format_exc()}")
                # If no contentUrl or download failed, use the content from the attachment if available
                elif hasattr(attachment, "content") or (isinstance(attachment, dict) and "content" in attachment):
                    content = attachment.get("content") if isinstance(attachment, dict) else attachment.content
                    if content:
                        if isinstance(content, str):
                            file_data["content"] = content
                        else:
                            # Assume it's bytes and encode as base64
                            file_data["content"] = base64.b64encode(content).decode('utf-8')
                        logger.info(f"Using content from attachment ({len(content)} bytes/chars)")
            except Exception as e:
                logger.error(f"Error processing attachment: {str(e)}")
                logger.error(f"Attachment processing error traceback: {traceback.format_exc()}")
                file_data = None

        # Get the endpoint URL
        platform = os.getenv("PLATFORM", "")
        if platform.upper() == "NGROK":
            url = os.getenv("NGROK_ENDPOINT")
            if not url.startswith("http://") and not url.startswith("https://"):
                url = f"http://{url}" if "localhost" in url or "127.0.0.1" in url else f"https://{url}"
            logger.info(f"Using NGROK endpoint: {url}")
            token = "dummy_token_for_ngrok"  # Use dummy token for NGROK
        else:
            # For non-NGROK platforms, ensure we have a valid token
            if global_token is None:
                logger.info("No token available, generating a new one")
                global_token = login()
            token = global_token
            
            # Get the appropriate endpoint based on platform
            if platform.upper() == "ALPHA":
                url = os.getenv("SNOWFLAKE_ENDPOINT_ALPHA")
            elif platform.upper() == "DEV":
                url = os.getenv("SNOWFLAKE_ENDPOINT_DEV")
            else:
                url = None  # Will be handled by call_submit_udf
        
        # Submit message
        submit_response = call_submit_udf(
            token=token,
            url=url,
            bot_id=bot_id,
            conversation_id=conversation_id,
            row_data=conversation_context,
            thread_id=thread_id,
            file=file_data
        )

        # Check if submit_response is a dictionary (our standardized format)
        if isinstance(submit_response, dict):
            if submit_response.get("status_code", 500) != 200:
                logger.error(f"Failed to submit message: {submit_response.get('status_code', 500)} - {submit_response.get('text', '')[:200]}")
                
                # Check if this is a server error (500)
                if submit_response.get("status_code", 0) == 500:
                    # For 500 errors in NGROK mode, we'll try to provide a more helpful message
                    if platform.upper() == "NGROK":
                        logger.error("Server returned 500 error in NGROK mode. This could indicate an issue with the local server.")
                        logger.error("Make sure the local server is running and properly configured to handle requests.")
                        
                        # Check if the error is related to bot_id not found
                        if "No details found for bot_id" in submit_response.get('text', ''):
                            return "The bot ID is not found in the database. Try using a different bot ID like 'Eve' which exists in the database."
                        
                        return "The local server returned an error. Please check that it's running correctly and configured to handle requests from the Teams adapter."
                
                return f"I couldn't connect to the service (Status: {submit_response.get('status_code', 'unknown')}). Please try again."

            # Get UUID from response
            try:
                logger.info(f"Return from submit_response: {submit_response.get('text', '')}")
                response_text = submit_response.get('text', '{}')
                
                # Try to parse the response as JSON
                try:
                    response_json = json.loads(response_text)
                    logger.info(f"Parsed JSON response: {json.dumps(response_json)[:200]}")
                    
                    # Check if the response has the expected structure
                    if "data" in response_json and isinstance(response_json["data"], list) and len(response_json["data"]) > 0:
                        uuid_value = response_json["data"][0][1]
                        logger.info(f"UUID from response: {uuid_value}")
                    else:
                        logger.error(f"Response JSON doesn't have the expected structure: {json.dumps(response_json)[:200]}")
                        return "I received an unexpected response format from the service. Please try again."
                except json.JSONDecodeError:
                    logger.error(f"Failed to parse response as JSON: {response_text[:200]}")
                    return "I received a non-JSON response from the service. Please try again."
                
            except (KeyError, IndexError) as e:
                logger.error(f"Failed to parse UUID from response: {e}")
                logger.error(f"Response content: {submit_response.get('text', '')[:200]}")
                return "I received an invalid response from the service. Please try again."
        else:
            # Handle legacy response object format
            if hasattr(submit_response, 'status_code') and submit_response.status_code != 200:
                logger.error(f"Failed to submit message: {submit_response.status_code} - {submit_response.text[:200]}")
                return f"I couldn't connect to the service (Status: {submit_response.status_code}). Please try again."
            
            # Get UUID from response
            try:
                if hasattr(submit_response, 'json'):
                    response_json = submit_response.json()
                    logger.info(f"Return from submit_response: {json.dumps(response_json)[:200]}")
                    uuid_value = response_json["data"][0][1]
                else:
                    logger.error("Submit response has no json method")
                    return "I received an invalid response from the service. Please try again."
                logger.info(f"UUID from response: {uuid_value}")
            except (KeyError, IndexError, json.JSONDecodeError, AttributeError) as e:
                logger.error(f"Failed to parse UUID from response: {e}")
                return "I received an invalid response from the service. Please try again."

        # For NGROK mode with 500 error, we'll skip polling and return early
        if platform.upper() == "NGROK" and submit_response.get("status_code", 0) == 500:
            return "The local server returned an error. Please check the server logs for more details."

        # Poll for response
        max_retries = 12  # 60 seconds total (12 retries * 5 seconds)
        retry_interval = int(os.getenv("RETRY_INTERVAL", "5"))
        retry_count = 0
        while retry_count < max_retries:
            lookup_response = call_lookup_udf(
                token=token, url=url, bot_id=bot_id, uuid=uuid_value
            )

            # Check if lookup_response is a dictionary (our standardized format)
            if isinstance(lookup_response, dict):
                if lookup_response.get("status_code", 500) != 200:
                    logger.error(f"Failed to lookup response: {lookup_response.get('status_code', 500)} - {lookup_response.get('text', '')[:200]}")
                    retry_count += 1
                    time.sleep(retry_interval)
                    continue

                try:
                    # Check if we got an authentication error
                    if lookup_response.get("status_code", 0) == 401:
                        logger.error("Authentication error detected, refreshing token")
                        global_token = login()
                        return "I encountered an authentication error. Please try again."

                    response_text = lookup_response.get('text', '{}')
                    try:
                        response_json = json.loads(response_text)
                        logger.info(f"Lookup response JSON: {json.dumps(response_json)[:200]}")
                        
                        if "data" in response_json and isinstance(response_json["data"], list) and len(response_json["data"]) > 0:
                            response_data = response_json["data"][0][1]
                            if response_data != "not found" and response_data:
                                logger.info(f"BOT RESPONSE DATA: {response_data}")
                                return response_data
                        else:
                            logger.error(f"Lookup response JSON doesn't have the expected structure: {json.dumps(response_json)[:200]}")
                    except json.JSONDecodeError:
                        logger.error(f"Failed to parse lookup response as JSON: {response_text[:200]}")
                except (KeyError, IndexError) as e:
                    logger.error(f"Error parsing lookup response: {e}")
                    logger.error(f"Response content: {lookup_response.get('text', '')[:200]}")
            else:
                # Handle legacy response object format
                if hasattr(lookup_response, 'status_code') and lookup_response.status_code != 200:
                    logger.error(f"Failed to lookup response: {lookup_response.status_code} - {lookup_response.text[:200]}")
                    retry_count += 1
                    time.sleep(retry_interval)
                    continue
                
                try:
                    # Check if we got an authentication error
                    if hasattr(lookup_response, 'status_code') and lookup_response.status_code == 401:
                        logger.error("Authentication error detected, refreshing token")
                        global_token = login()
                        return "I encountered an authentication error. Please try again."
                    
                    if hasattr(lookup_response, 'json'):
                        response_json = lookup_response.json()
                        logger.info(f"Lookup response JSON: {json.dumps(response_json)[:200]}")
                        response_data = response_json["data"][0][1]
                        if response_data != "not found" and response_data:
                            logger.info(f"BOT RESPONSE DATA: {response_data}")
                            return response_data
                except (KeyError, IndexError, json.JSONDecodeError, AttributeError) as e:
                    logger.error(f"Error parsing lookup response: {e}")
                    if hasattr(lookup_response, 'text'):
                        logger.error(f"Response content: {lookup_response.text[:200]}")

            logger.info(f"Retry {retry_count + 1}/{max_retries}: Waiting for response...")
            retry_count += 1
            time.sleep(retry_interval)
        
        # If we're in NGROK mode, provide a more helpful message
        if platform.upper() == "NGROK":
            return "The local server didn't provide a response in time. Please check the server logs for more details."
        return "I didn't receive a response in time. Please try again."
    except Exception as e:
        logger.error(f"Error in send_message: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return f"I encountered an error: {str(e)}"


if __name__ == "__main__":
    main()


class EchoBot(ActivityHandler):
    # See https://aka.ms/about-bot-activity-message to learn more about the message and other activity types.

    def __init__(self):
        logger.info("=== Initializing EchoBot ===")
        super().__init__()
        # Initialize Snowflake connection
        try:
            logger.info("=== Calling main() to initialize Snowflake connection ===")
            main()
            logger.info("=== Snowflake connection initialized successfully ===")
        except Exception as e:
            logger.error(f"=== Error initializing bot: {str(e)} ===")
            import traceback
            logger.error(f"=== Traceback: {traceback.format_exc()} ===")

    async def on_turn(self, turn_context: TurnContext):
        logger.info("=== EchoBot.on_turn called ===")
        try:
            # Debug: Log turn_context attributes
            logger.info("=== TurnContext attributes: ===")
            for attr in dir(turn_context):
                if not attr.startswith('_'):
                    try:
                        value = getattr(turn_context, attr)
                        if attr == 'activity':
                            logger.info(f"  {attr}: Activity object")
                            # Log activity attributes
                            logger.info("=== Activity attributes: ===")
                            activity = turn_context.activity
                            # Helper function to safely get attributes from activity
                            def get_activity_attr(activity, attr, default='N/A'):
                                if isinstance(activity, dict):
                                    return activity.get(attr, default)
                                return getattr(activity, attr, default)

                            for act_attr in dir(activity):
                                if not act_attr.startswith('_'):
                                    try:
                                        act_value = get_activity_attr(activity, act_attr)
                                        logger.info(f"    {act_attr}: {act_value}")
                                    except Exception as act_attr_err:
                                        logger.info(f"    Error accessing {act_attr}: {str(act_attr_err)}")
                        else:
                            logger.info(f"  {attr}: {value}")
                    except Exception as attr_err:
                        logger.info(f"  Error accessing {attr}: {str(attr_err)}")

            logger.info("=== Calling super().on_turn ===")
            await super().on_turn(turn_context)
            logger.info("=== super().on_turn completed successfully ===")
        except Exception as e:
            logger.error(f"=== Error in on_turn: {str(e)} ===")
            import traceback
            logger.error(f"=== Traceback: {traceback.format_exc()} ===")
            raise

    async def on_message_activity(self, turn_context: TurnContext):
        logger.info("=== EchoBot.on_message_activity called ===")
        try:
            # Debug: Log activity object
            logger.info("=== Message Activity details: ===")
            if hasattr(turn_context, 'activity'):
                activity = turn_context.activity

                # Helper function to safely get attributes from activity
                def get_activity_attr(activity, attr, default='N/A'):
                    if isinstance(activity, dict):
                        return activity.get(attr, default)
                    return getattr(activity, attr, default)

                # Log activity details safely
                logger.info(f"  type: {get_activity_attr(activity, 'type')}")
                logger.info(f"  id: {get_activity_attr(activity, 'id')}")
                logger.info(f"  text: {get_activity_attr(activity, 'text')}")

                # Get channelId safely
                channel_id = get_activity_attr(activity, 'channelId')
                logger.info(f"  Channel ID: {channel_id}")

                # Check for attachments
                attachments = []
                if hasattr(activity, 'attachments'):
                    # Get attachments safely using get_activity_attr
                    activity_attachments = get_activity_attr(activity, 'attachments', [])
                    if activity_attachments:
                        logger.info(f"  Found {len(activity_attachments)} attachments")
                        for i, attachment in enumerate(activity_attachments):
                            # Get attachment details safely
                            attachment_name = get_activity_attr(attachment, "name", f"file_{i}")
                            content_type = get_activity_attr(attachment, "contentType", "application/octet-stream")
                            content_url = get_activity_attr(attachment, "contentUrl", "")

                            logger.info(f"  Attachment {i+1}: {attachment_name} - {content_type}")
                            logger.info(f"  Attachment {i+1} URL: {content_url}")

                            # Convert attachment to dictionary for easier handling
                            attachment_dict = {
                                "name": attachment_name,
                                "contentType": content_type,
                                "contentUrl": content_url,
                                "content": get_activity_attr(attachment, "content", "")
                            }
                            attachments.append(attachment_dict)

                # Extract message text safely
                message_text = ""
                if hasattr(turn_context.activity, 'text'):
                    message_text = get_activity_attr(turn_context.activity, 'text', "")

                # Send a typing indicator using a proper Activity object
                logger.info("=== Sending typing indicator ===")
                typing_activity = Activity(
                    type=ActivityTypes.typing,
                    channel_id=get_activity_attr(turn_context.activity, 'channelId'),
                    conversation=get_activity_attr(turn_context.activity, 'conversation'),
                    recipient=get_activity_attr(turn_context.activity, 'from'),
                    from_property=get_activity_attr(turn_context.activity, 'recipient')
                )
                await turn_context.send_activity(typing_activity)

                # If there's no text but there are attachments, create a default message
                if not message_text and attachments:
                    attachment_names = [att.get('name', 'file') for att in attachments]
                    message_text = f"[Attachment: {', '.join(attachment_names)}]"
                    logger.info(f"Created default message text for attachment: {message_text}")

                # Extract conversation ID safely
                conversation_id = None
                if hasattr(turn_context.activity, 'conversation') and turn_context.activity.conversation:
                    conversation_id = get_activity_attr(turn_context.activity.conversation, 'id')
                    logger.info(f"Extracted conversation ID: {conversation_id}")

                # Log activity information for debugging
                logger.info(f"=== Processing message: '{message_text}' ===")
                if attachments:
                    logger.info(f"=== With {len(attachments)} attachments ===")

                # Get the actual response from Snowflake
                logger.info("=== Calling send_message ===")
                snowflake_response = send_message(message_text, conversation_id, attachments if attachments else None)
                logger.info(f"=== send_message returned: {snowflake_response} ===")

                # Simulate streaming with multiple small messages
                if snowflake_response:
                    # Split the response into chunks if it's long
                    logger.info("=== Chunking response ===")
                    chunks = self._chunk_response(snowflake_response)
                    logger.info(f"=== Response split into {len(chunks)} chunks ===")

                    for i, chunk in enumerate(chunks):
                        # Add progress indicators for all but the last chunk
                        if i < len(chunks) - 1:
                            prefix = [" Processing...", " Analyzing...", " Retrieving data..."][i % 3]
                            logger.info(f"=== Sending chunk {i+1}/{len(chunks)} with prefix ===")
                            await turn_context.send_activity(f"{prefix} {chunk}")
                            await asyncio.sleep(1)  # Simulate delay
                        else:
                            # Send the final chunk without prefix
                            logger.info(f"=== Sending final chunk {i+1}/{len(chunks)} ===")
                            await turn_context.send_activity(chunk)
            else:
                logger.info("=== No response from Snowflake, sending error message ===")
                await turn_context.send_activity("I couldn't get a response from the service. Please try again.")

            logger.info("=== on_message_activity completed successfully ===")
        except Exception as e:
            logger.error(f"=== Error in on_message_activity: {str(e)} ===")
            import traceback
            logger.error(f"=== Traceback: {traceback.format_exc()} ===")
            await turn_context.send_activity(f"I encountered an error: {str(e)}")

    async def on_members_added_activity(
        self, members_added: ChannelAccount, turn_context: TurnContext
    ):
        try:
            # Helper function to safely get attributes from activity
            def get_activity_attr(obj, attr, default='N/A'):
                if isinstance(obj, dict):
                    return obj.get(attr, default)
                return getattr(obj, attr, default)

            for member_added in members_added:
                recipient_id = get_activity_attr(turn_context.activity.recipient, 'id')
                member_id = get_activity_attr(member_added, 'id')
                if member_id != recipient_id:
                    await turn_context.send_activity("Hello and welcome! I'm your Snowflake assistant. Ask me anything!")
        except Exception as e:
            logger.error(f"Error in on_members_added_activity: {str(e)}")
            await turn_context.send_activity(f"I encountered an error during welcome: {str(e)}")

    def _chunk_response(self, response, max_chunk_size=600):
        """Split a long response into smaller chunks for simulated streaming"""
        if not response or len(response) <= max_chunk_size:
            return [response]

        # For longer responses, try to split at sentence boundaries
        sentences = response.replace('. ', '.|').replace('! ', '!|').replace('? ', '?|').split('|')

        chunks = []
        current_chunk = ""

        for sentence in sentences:
            if len(current_chunk) + len(sentence) <= max_chunk_size:
                current_chunk += sentence + " "
            else:
                if current_chunk:
                    chunks.append(current_chunk.strip())
                current_chunk = sentence + " "

        if current_chunk:
            chunks.append(current_chunk.strip())

        return chunks
