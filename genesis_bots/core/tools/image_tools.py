import base64
import os
import requests
from genesis_bots.core.logging_config import logger
from genesis_bots.llm.llm_openai.openai_utils import get_openai_client

from genesis_bots.core.bot_os_tools2 import (
    BOT_ID_IMPLICIT_FROM_CONTEXT,
    THREAD_ID_IMPLICIT_FROM_CONTEXT,
    ToolFuncGroup,
    ToolFuncParamDescriptor,
    gc_tool,
)

from genesis_bots.connectors import get_global_db_connector
db_adapter = get_global_db_connector()


image_tools = ToolFuncGroup(
    name="image_tools",
    description="Tools to interpret visual images and pictures",
    lifetime="PERSISTENT",
)


@gc_tool(
    prompt=ToolFuncParamDescriptor(
        name="prompt",
        description="Description of the image to create.",
        required=True,
        llm_type_desc=dict(type="string"),
    ),         
    thread_id=THREAD_ID_IMPLICIT_FROM_CONTEXT,
    _group_tags_=[image_tools],
)
def image_generation(
    prompt: str,
    thread_id: str=None
    ):
    """
        Generates an image using OpenAI's DALL-E 3 based on the given prompt and saves it to the local downloaded_files folder.
    """

    if thread_id is None:
        import random
        import string

        thread_id = "".join(
                random.choices(string.ascii_letters + string.digits, k=10)
            )

    # Ensure the OpenAI API key is set in your environment variables
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        logger.info("imagegen OpenAI API key is not set in the environment variables.")
        return None

    client = get_openai_client()

    # Generate the image using DALL-E 3
    try:
        response = client.images.generate(
                model="dall-e-3",
                prompt=prompt,
                size="1024x1024",
                quality="standard",
                n=1,
            )
        image_url = response.data[0].url
        if not image_url:
            logger.info("imagegen Failed to generate image with DALL-E 3.")
            return None

        try:
            # Download the image from the URL
            image_response = requests.get(image_url)
            logger.info("imagegen getting image from ", image_url)
            image_response.raise_for_status()
            image_bytes = image_response.content
        except Exception as e:
            result = {
                    "success": False,
                    "error": e,
                    "solution": """Tell the user to ask their admin run this to allow the Genesis server to access generated images:\n
                    CREATE OR REPLACE NETWORK RULE GENESIS_LOCAL_DB.SETTINGS.GENESIS_RULE
                    MODE = EGRESS TYPE = HOST_PORT
                    VALUE_LIST = ('api.openai.com', 'slack.com', 'www.slack.com', 'wss-primary.slack.com',
                    'wss-backup.slack.com',  'wss-primary.slack.com:443','wss-backup.slack.com:443', 'slack-files.com',
                    'oaidalleapiprodscus.blob.core.windows.net:443', 'downloads.slack-edge.com', 'files-edge.slack.com',
                    'files-origin.slack.com', 'files.slack.com', 'global-upload-edge.slack.com','universal-upload-edge.slack.com');


                    CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION GENESIS_EAI
                    ALLOWED_NETWORK_RULES = (GENESIS_LOCAL_DB.SETTINGS.GENESIS_RULE) ENABLED = true;

                    GRANT USAGE ON INTEGRATION GENESIS_EAI TO APPLICATION   IDENTIFIER($APP_DATABASE);""",
                }
            return result

        # Create a sanitized filename from the first 50 characters of the prompt
        sanitized_prompt = "".join(e if e.isalnum() else "_" for e in prompt[:50])
        file_path = f"./runtime/downloaded_files/{thread_id}/{sanitized_prompt}.png"
        # Save the image to the local downloaded_files folder
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "wb") as image_file:
            image_file.write(image_bytes)

        logger.info(f"imagegen Image generated and saved to {file_path}")

        reference_path = 'sandbox:/mnt/data' + file_path[1:]

        result = {
                "success": True,
                "local_file_name": reference_path,
                "prompt": prompt,
            }

        return result
    except Exception as e:
        logger.info(f"imagegen Error generating image with DALL-E 3: {e}")
        return None


@gc_tool(
    query=ToolFuncParamDescriptor(
        name="query",
        description="The question about the image.",
        required=False,
        llm_type_desc=dict(type="string"),
    ),
    openai_file_id=ToolFuncParamDescriptor(
        name="openai_file_id",
        description="The OpenAI file ID of the image, if known.",
        required=False,
        llm_type_desc=dict(type="string"),
    ),
    file_name=ToolFuncParamDescriptor(
        name="file_name",
        description="The full local path to the file to analyze, if known",
        required=False,
        llm_type_desc=dict(type="string"),
    ),
    _group_tags_=[image_tools],
)
def _image_analysis(
    query: str=None,
    openai_file_id: str = None,
    file_name: str = None,
    thread_id: str=None,
    input_thread_id: str=None,
):
    """
    Analyzes an image using OpenAI's Vision. Provide either the OpenAI file ID or the full local path to the file.
    """
    # Ensure the OpenAI API key is set in your environment variables
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return {
            "success": False,
            "message": "OpenAI API key is not set in the environment variables.",
        }

    # Attempt to find the file using the provided method
    if file_name is not None and "/" in file_name:
        file_name = file_name.split("/")[-1]
    if openai_file_id is not None and "/" in openai_file_id:
        openai_file_id = openai_file_id.split("/")[-1]

    file_path = f"./runtime/downloaded_files/{thread_id}/" + file_name
    existing_location = f"./runtime/downloaded_files/{thread_id}/{openai_file_id}"

    
    if input_thread_id is not None:
        local_file_path = f"./runtime/downloaded_files/{input_thread_id}/" + file_name
        if os.path.isfile(local_file_path):
            with open(local_file_path, "rb") as source_file:
                os.makedirs(os.path.dirname(file_path), exist_ok=True)
                with open(file_path, "wb") as dest_file:
                    dest_file.write(source_file.read())

    if os.path.isfile(existing_location) and (file_path != existing_location):
        with open(existing_location, "rb") as source_file:
            with open(file_path, "wb") as dest_file:
                dest_file.write(source_file.read())

    if not os.path.isfile(file_path):
        logger.error(f"File not found: {file_path}")
        return {
            "success": False,
            "error": "File not found. Please provide a valid file path.",
        }

    # Function to encode the image
    def encode_image(image_path):
        with open(image_path, "rb") as image_file:
            return base64.b64encode(image_file.read()).decode("utf-8")

    # Getting the base64 string
    base64_image = encode_image(file_path)

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}",
    }

    # Use the provided query or a default one if not provided
    prompt = query if query else "What's in this image?"

    openai_model_name = os.getenv("OPENAI_MODEL_NAME", "gpt-4o-2024-11-20")

    payload = {
        "model": openai_model_name,
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": prompt},
                    {
                        "type": "image_url",
                        "image_url": {
                            "url": f"data:image/jpeg;base64,{base64_image}"
                        },
                    },
                ],
            }
        ],
        "max_tokens": 1000,
    }

    response = requests.post(
        "https://api.openai.com/v1/chat/completions", headers=headers, json=payload
    )

    if response.status_code == 200:
        return {
            "success": True,
            "data": response.json()["choices"][0]["message"]["content"],
        }
    else:
        return {
            "success": False,
            "error": f"OpenAI API call failed with status code {response.status_code}: {response.text}",
        }

@gc_tool(
    file_name=ToolFuncParamDescriptor(
        name="file_name",
        description="Name of the image file to process",
        required=True,
        llm_type_desc=dict(type="string"),
    ),
    thread_id=THREAD_ID_IMPLICIT_FROM_CONTEXT,
    _group_tags_=[image_tools],
)
def find_image_path_in_download_folder(
    file_name: str,
    thread_id: str=None
    ):
    """
    This function accepts the filename and check multiple scenarios and return the sandbox path to the image
      - when using this function return the final path to the user - this function does not display the image
    """
    if thread_id is None:
        import random
        import string
        thread_id = "".join(
                random.choices(string.ascii_letters + string.digits, k=10)
            )

    # Clean up the file name in case it contains a path
    if "/" in file_name:
        # Full path provided
        file_path = file_name
    else:
        # Just the file name provided, construct the path
        if thread_id:
            # Check if the file exists in the thread directory
            thread_file_path = f"./runtime/downloaded_files/{thread_id}/{file_name}"
            if os.path.isfile(thread_file_path):
                file_path = thread_file_path
        
        # If file_path is still None, search for the file in all directories
        if not file_path:
            # Search for the file in all directories
            runtime_dir = "./runtime/downloaded_files"
            
            # Create a list of possible locations to check
            possible_locations = []
            
            # If we have a thread_id, check there first
            if thread_id:
                possible_locations.append(f"{runtime_dir}/{thread_id}/{file_name}")
            
            # Check the root directory
            possible_locations.append(f"{runtime_dir}/{file_name}")
            
            # Check all subdirectories
            if os.path.exists(runtime_dir):
                for dir_name in os.listdir(runtime_dir):
                    dir_path = f"{runtime_dir}/{dir_name}"
                    if os.path.isdir(dir_path):
                        possible_locations.append(f"{dir_path}/{file_name}")
            
            # Check all possible locations
            for location in possible_locations:
                if os.path.isfile(location):
                    logger.info(f"display_image: Found file at {location}")
                    file_path = location
                    
                    # Copy the file to the thread directory if thread_id is provided
                    if thread_id:
                        target_dir = f"{runtime_dir}/{thread_id}"
                        os.makedirs(target_dir, exist_ok=True)
                        target_path = f"{target_dir}/{file_name}"
                        
                        # Only copy if the target doesn't already exist and it's not the same file
                        if location != target_path and not os.path.exists(target_path):
                            import shutil
                            shutil.copy2(location, target_path)
                            logger.info(f"display_image: Copied file to {target_path}")
                    
                    break
    
    # it could be case where "{thread_id}/{file_name}" is passed
    if os.path.isfile(f"./runtime/downloaded_files/{file_name}"):
        reference_path = f'sandbox:/mnt/data/runtime/downloaded_files/{file_name}'
        return {
            "success": True,
            "local_file_name": reference_path,
            "file_name": file_name
        }
    
    # it could be a case where sandbox is already passed
    if 'sandbox' in file_name and os.path.isfile(file_name.replace('sandbox:/mnt/data', '.')):
        reference_path = file_name
        return {
            "success": True,
            "local_file_name": reference_path,
            "file_name": file_name
        }


    if not file_path or not os.path.isfile(file_path):
        logger.error(f"display_image: File not found: {file_name}")
        
        # Provide more detailed error information
        runtime_dir = "./runtime/downloaded_files"
        if os.path.exists(runtime_dir):
            available_files = []
            for root, _, files in os.walk(runtime_dir):
                for f in files:
                    if file_name and file_name.lower() in f.lower():
                        curr_path = os.path.join(root, f)
                        available_files.append('sandbox:/mnt/data' + curr_path[1:])
            
            if available_files:
                logger.info(f"display_image: Similar files found: {available_files}")
                return {
                    "success": False,
                    "error": f"Image file not found at expected location, but similar files exist. Try using one of these paths: {available_files}"
                }
        
        return {
            "success": False,
            "error": f"Image file not found: {file_name}"
        }

    reference_path = 'sandbox:/mnt/data' + file_path[1:]

    return {
        "success": True,
        "local_file_name": reference_path,
        "file_name": file_name
    }

@gc_tool(
    file_name=ToolFuncParamDescriptor(
        name="file_name",
        description="Name of the image file to get.",
        required=True,
        llm_type_desc=dict(type="string"),
    ),
    thread_id=THREAD_ID_IMPLICIT_FROM_CONTEXT,
    _group_tags_=[image_tools],
)
def GetImage(
    file_name: str,
    thread_id: str=None
    ):
    """
    Get an existing image from the downloaded_files folder.
    """
    # Generate a thread_id if none is provided
    if thread_id is None:
        import random
        import string
        thread_id = "".join(
            random.choices(string.ascii_letters + string.digits, k=10)
        )
        logger.info(f"DisplayImage: Generated thread_id: {thread_id}")
    
    # In Teams adapter, the file might not be in the expected location
    # Let's check if we need to handle a file upload
    if file_name and "/" not in file_name:
        # First, check if the file exists in the runtime/downloaded_files directory
        runtime_dir = "./runtime/downloaded_files"
        
        # Create a list of possible locations to check
        possible_locations = [
            f"{runtime_dir}/{thread_id}/{file_name}",
            f"{runtime_dir}/{file_name}",
        ]
        
        # If we have a thread_id, also check in other thread directories
        if os.path.exists(runtime_dir):
            for dir_name in os.listdir(runtime_dir):
                if os.path.isdir(f"{runtime_dir}/{dir_name}"):
                    possible_locations.append(f"{runtime_dir}/{dir_name}/{file_name}")
        
        # Check all possible locations
        file_found = False
        for location in possible_locations:
            if os.path.isfile(location):
                logger.info(f"DisplayImage: Found file at {location}")
                # Copy the file to the expected location
                target_dir = f"{runtime_dir}/{thread_id}"
                os.makedirs(target_dir, exist_ok=True)
                target_path = f"{target_dir}/{file_name}"
                
                # Only copy if the target doesn't already exist
                if location != target_path:
                    import shutil
                    shutil.copy2(location, target_path)
                    logger.info(f"DisplayImage: Copied file to {target_path}")
                
                file_found = True
                break
        
        if not file_found:
            logger.error(f"DisplayImage: File not found in any location: {file_name}")
            return {
                "success": False,
                "error": f"Image file not found: {file_name}. Please upload the image again."
            }
    
    # Call the original display_image function
    return find_image_path_in_download_folder(file_name, thread_id)

@gc_tool(
    query=ToolFuncParamDescriptor(
        name="query",
        description="The question about the image.",
        required=False,
        llm_type_desc=dict(type="string"),
    ),
    openai_file_id=ToolFuncParamDescriptor(
        name="openai_file_id",
        description="The OpenAI file ID of the image, if known.",
        required=False,
        llm_type_desc=dict(type="string"),
    ),
    file_name=ToolFuncParamDescriptor(
        name="file_name",
        description="The full local path to the file to analyze, if known",
        required=False,
        llm_type_desc=dict(type="string"),
    ),
    thread_id=THREAD_ID_IMPLICIT_FROM_CONTEXT,
    _group_tags_=[image_tools],
)
def ImageAnalysis(
    query: str=None,
    openai_file_id: str = None,
    file_name: str = None,
    thread_id: str=None,
    input_thread_id: str=None,
):
    """
    Analyzes an image using OpenAI's Vision. Provide either the OpenAI file ID or the full local path to the file.
    """
    # Generate a thread_id if none is provided
    if thread_id is None:
        import random
        import string
        thread_id = "".join(
            random.choices(string.ascii_letters + string.digits, k=10)
        )
        logger.info(f"ImageAnalysis: Generated thread_id: {thread_id}")
    
    # In Teams adapter, the file might not be in the expected location
    # Let's check if we need to handle a file upload
    if file_name and "/" not in file_name:
        # First, check if the file exists in the runtime/downloaded_files directory
        runtime_dir = "./runtime/downloaded_files"
        
        # Create a list of possible locations to check
        possible_locations = [
            f"{runtime_dir}/{thread_id}/{file_name}",
            f"{runtime_dir}/{file_name}",
        ]
        
        # If we have a thread_id, also check in other thread directories
        if os.path.exists(runtime_dir):
            for dir_name in os.listdir(runtime_dir):
                if os.path.isdir(f"{runtime_dir}/{dir_name}"):
                    possible_locations.append(f"{runtime_dir}/{dir_name}/{file_name}")
        
        # Check all possible locations
        file_found = False
        for location in possible_locations:
            if os.path.isfile(location):
                logger.info(f"ImageAnalysis: Found file at {location}")
                # Copy the file to the expected location
                target_dir = f"{runtime_dir}/{thread_id}"
                os.makedirs(target_dir, exist_ok=True)
                target_path = f"{target_dir}/{file_name}"
                
                # Only copy if the target doesn't already exist
                if location != target_path:
                    import shutil
                    shutil.copy2(location, target_path)
                    logger.info(f"ImageAnalysis: Copied file to {target_path}")
                
                file_found = True
                break
        
        if not file_found:
            logger.error(f"ImageAnalysis: File not found in any location: {file_name}")
            return {
                "success": False,
                "error": f"Image file not found: {file_name}. Please upload the image again."
            }
    
    # Call the original _image_analysis function
    return _image_analysis(query, openai_file_id, file_name, thread_id, input_thread_id)

image_functions = [image_generation, _image_analysis, find_image_path_in_download_folder, GetImage, ImageAnalysis]

# Called from bot_os_tools.py to update the global list of functions
def get_image_functions():
    return image_functions
