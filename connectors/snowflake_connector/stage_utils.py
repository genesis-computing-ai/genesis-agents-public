import os
import random
import re
import string

import logging
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.WARN, format="%(asctime)s - %(levelname)s - %(message)s"
)

def add_file_to_stage(
    self,
    database: str = None,
    schema: str = None,
    stage: str = None,
    openai_file_id: str = None,
    file_name: str = None,
    file_content: str = None,
    thread_id=None,
):
    """
    Add a file to a Snowflake stage.

    Args:
        database (str): The name of the database.
        schema (str): The name of the schema.
        stage (str): The name of the stage.
        file_path (str): The local path to the file to be uploaded.
        file_format (str): The format of the file (default is 'CSV').

    Returns:
        dict: A dictionary with the result of the operation.
    """

    try:
        if file_content is None:
            file_name = file_name.replace("serverlocal:", "")
            if openai_file_id is not None:
                openai_file_id = openai_file_id.replace("serverlocal:", "")

            if file_name.startswith("file-"):
                return {
                    "success": False,
                    "error": "Please provide a human-readable file name in the file_name parameter, with a supported extension, not the OpenAI file ID. If unsure, ask the user what the file should be called.",
                }

            # allow files to have relative paths
            #     if '/' in file_name:
            #         file_name = file_name.split('/')[-1]
            if file_name.startswith("/"):
                file_name = file_name[1:]

            file_name = re.sub(r"[^\w\s\/\.-]", "", file_name.replace(" ", "_"))
            if openai_file_id is not None:
                if "/" in openai_file_id:
                    openai_file_id = openai_file_id.split("/")[-1]

            file_path = f"./downloaded_files/{thread_id}/" + file_name
            existing_location = f"./downloaded_files/{thread_id}/{openai_file_id}"

            if not os.path.exists(os.path.dirname(file_path)):
                os.makedirs(os.path.dirname(file_path))

            # Replace spaces with underscores and remove disallowed characters
            #  file_name = re.sub(r'[^\w\s-]', '', file_name.replace(' ', '_'))
            if os.path.isfile(existing_location) and (
                file_path != existing_location
            ):
                with open(existing_location, "rb") as source_file:
                    with open(file_path, "wb") as dest_file:
                        dest_file.write(source_file.read())

            if not os.path.isfile(file_path):

                logger.error(f"File not found: {file_path}")
                return {
                    "success": False,
                    "error": f"Needs user review: Please first save and RETURN THE FILE *AS A FILE* to the user for their review, and once confirmed by the user, call this function again referencing the SAME OPENAI_FILE_ID THAT YOU RETURNED TO THE USER to save it to stage.",
                }

        else:
            if thread_id is None:
                thread_id = "".join(
                    random.choices(string.ascii_letters + string.digits, k=6)
                )

        if file_content is not None:
            # Ensure the directory exists
            directory = f"./downloaded_files/{thread_id}"
            if not os.path.exists(directory):
                os.makedirs(directory)

            # Write the content to the file
            file_path = os.path.join(directory, file_name)
            with open(file_path, "w") as file:
                file.write(file_content)
    except Exception as e:
        return {"success": False, "error": str(e)}

    try:
        p = os.path.dirname(file_name) if "/" in file_name else None
        if p is not None:
            query = f'PUT file://{file_path} @"{database}"."{schema}"."{stage}"/{p} overwrite=TRUE AUTO_COMPRESS=FALSE'
        else:
            query = f'PUT file://{file_path} @"{database}"."{schema}"."{stage}" overwrite=TRUE AUTO_COMPRESS=FALSE'
        return self.run_query(query)
    except Exception as e:
        logger.error(f"Error adding file to stage: {e}")
        return {"success": False, "error": str(e)}

def read_file_from_stage(
    self,
    database: str,
    schema: str,
    stage: str,
    file_name: str,
    return_contents: bool = True,        
    is_binary: bool = False,
    for_bot=None,
    thread_id=None,
):
    """
    Read a file from a Snowflake stage.

    Args:
        database (str): The name of the database.
        schema (str): The name of the schema.
        stage (str): The name of the stage.
        file_name (str): The name of the file to be read.

    Returns:
        str: The contents of the file.
    """
    try:
        # Define the local directory to save the file
        if for_bot == None:
            for_bot = thread_id
        local_dir = os.path.join(".", "downloaded_files", for_bot)

        #        if '/' in file_name:
        #            file_name = file_name.split('/')[-1]

        if not os.path.isdir(local_dir):
            os.makedirs(local_dir)
        local_file_path = os.path.join(local_dir, file_name)
        target_dir = os.path.dirname(local_file_path)
        if not os.path.exists(target_dir):
            os.makedirs(target_dir)

        # Modify the GET command to include the local file path

        query = f'GET @"{database}"."{schema}"."{stage}"/{file_name} file://{target_dir}'
        ret = self.run_query(query)
        if isinstance(ret, dict) and "does not exist or not authorized" in ret.get(
            "Error", ""
        ):
            database = database.upper()
            schema = schema.upper()
            stage = stage.upper()
            query = f'GET @"{database}"."{schema}"."{stage}"/{file_name} file://{local_dir}'
            ret = self.run_query(query)

        if os.path.isfile(local_file_path):
            if return_contents:
                if is_binary:   
                    with open(local_file_path, "rb") as file:
                        return file.read()
                else:
                    with open(local_file_path, "r") as file:
                        return file.read()
            else:
                return file_name
        else:
            return f"The file {file_name} does not exist at stage path @{database}.{schema}.{stage}/{file_name}."
    except Exception as e:
        return {"success": False, "error": str(e)}

def update_file_in_stage(
    self,
    database: str = None,
    schema: str = None,
    stage: str = None,
    file_name: str = None,
    thread_id=None,
):
    """
    Update (replace) a file in a Snowflake stage.

    Args:
        database (str): The name of the database.
        schema (str): The name of the schema.
        stage (str): The name of the stage.
        file_path (str): The local path to the new file.
        file_name (str): The name of the file to be replaced.
        file_format (str): The format of the file (default is 'CSV').

    Returns:
        dict: A dictionary with the result of the operation.
    """
    try:
        from .snowflake_connector import SnowflakeConnector  # Move import here
        if "/" in file_name:
            file_name = file_name.split("/")[-1]

        file_path = f"./downloaded_files/{thread_id}/" + file_name

        if not os.path.isfile(file_path):

            logger.error(f"File not found: {file_path}")
            return {
                "success": False,
                "error": f"Local new version of file not found: {file_path}",
            }

        # First, remove the existing file
        remove_query = f"REMOVE @{database}.{schema}.{stage}/{file_name}"
        self.run_query(remove_query)
        # Then, add the new file

        add_query = f"PUT file://{file_path} @{database}.{schema}.{stage} AUTO_COMPRESS=FALSE"
        return self.run_query(add_query)
    except Exception as e:
        logger.error(f"Error updating file in stage: {e}")
        return {"success": False, "error": str(e)}

def delete_file_from_stage(
    self,
    database: str = None,
    schema: str = None,
    stage: str = None,
    file_name: str = None,
    thread_id=None,
):
    """
    Delete a file from a Snowflake stage.

    Args:
        database (str): The name of the database.
        schema (str): The name of the schema.
        stage (str): The name of the stage.
        file_name (str): The name of the file to be deleted.

    Returns:
        dict: A dictionary with the result of the operation.
    """
    if "/" in file_name:
        file_name = file_name.split("/")[-1]

    try:
        query = f"REMOVE @{database}.{schema}.{stage}/{file_name}"
        ret = self.run_query(query)
        if isinstance(ret, dict) and "does not exist or not authorized" in ret.get(
            "Error", ""
        ):
            database = database.upper()
            schema = schema.upper()
            stage = stage.upper()
            query = f'REMOVE @"{database}"."{schema}"."{stage}"/{file_name}'
            ret = self.run_query(query)

        return ret
    except Exception as e:
        logger.error(f"Error deleting file from stage: {e}")
        return {"success": False, "error": str(e)}
    
def test_stage_functions():
    # Create a test instance of SnowflakeConnector
    from .snowflake_connector import SnowflakeConnector 
    test_connector = SnowflakeConnector("Snowflake")

    # Call the list_stage method with the specified parameters
    stage_list = test_connector.list_stage_contents(
        database="GENESIS_TEST", schema="GENESIS_INTERNAL", stage="SEMANTIC_STAGE"
    )

    # Print the result
    print(stage_list)

    for file_info in stage_list:
        file_name = file_info["name"].split("/")[-1]  # Extract the file name
        file_size = file_info["size"]
        file_md5 = file_info["md5"]
        file_last_modified = file_info["last_modified"]
        print(f"Reading file: {file_name}")
        print(f"Size: {file_size} bytes")
        print(f"MD5: {file_md5}")
        print(f"Last Modified: {file_last_modified}")
        file_content = test_connector.read_file_from_stage(
            database="GENESIS_TEST",
            schema="GENESIS_INTERNAL",
            stage="SEMANTIC_STAGE",
            file_name=file_name,
            return_contents=True,
        )
        print(file_content)

        # Call the function to write 'tostage.txt' to the stage
    result = test_connector.add_file_to_stage(
        database="GENESIS_TEST",
        schema="GENESIS_INTERNAL",
        stage="SEMANTIC_STAGE",
        file_name="tostage.txt",
    )
    print(result)

    # Read the 'tostage.txt' file from the stage
    tostage_content = test_connector.read_file_from_stage(
        database="GENESIS_TEST",
        schema="GENESIS_INTERNAL",
        stage="SEMANTIC_STAGE",
        file_name="tostage.txt",
        return_contents=True,
    )
    print("Content of 'tostage.txt':")
    print(tostage_content)

    import random
    import string

    # Function to generate a random string of fixed length
    def random_string(length=10):
        letters = string.ascii_letters
        return "".join(random.choice(letters) for i in range(length))

    # Generate a random string
    random_str = random_string()

    # Append the random string to the 'tostage.txt' file
    with open("./stage_files/tostage.txt", "a") as file:
        file.write(f"{random_str}\n")

    print(f"Appended random string to 'tostage.txt': {random_str}")

    # Upload the updated 'tostage.txt' to the stage
    update_result = test_connector.update_file_in_stage(
        database="GENESIS_TEST",
        schema="GENESIS_INTERNAL",
        stage="SEMANTIC_STAGE",
        file_name="tostage.txt",
    )
    print(f"Update result for 'tostage.txt': {update_result}")

    # Read the 'tostage.txt' file from the stage
    new_version_filename = test_connector.read_file_from_stage(
        database="GENESIS_TEST",
        schema="GENESIS_INTERNAL",
        stage="SEMANTIC_STAGE",
        file_name="tostage.txt",
        return_contents=False,
    )

    # Load new_version_contents from the file returned by new_version_filename
    with open("./stage_files/" + new_version_filename, "r") as file:
        new_version_content = file.read()

    # Split the content into lines and check the last line for the random string
    lines = new_version_content.split("\n")
    if (
        lines[-2].strip() == random_str
    ):  # -2 because the last element is an empty string due to the trailing newline
        print("The last line in the new version contains the random string.")
    else:
        print("The second to last line is:", lines[-2])
        print("The last line is:", lines[-1])
        print("The last line in the new version does not contain the random string.")
    # Delete the 'tostage.txt' file from the stage
    delete_result = test_connector.delete_file_from_stage(
        database="GENESIS_TEST",
        schema="GENESIS_INTERNAL",
        stage="SEMANTIC_STAGE",
        file_name="tostage.txt",
    )
    print(f"Delete result for 'tostage.txt': {delete_result}")

    # Re-list the stage contents to confirm deletion of 'tostage.txt'
    stage_list_after_deletion = test_connector.list_stage_contents(
        database="GENESIS_TEST", schema="GENESIS_INTERNAL", stage="SEMANTIC_STAGE"
    )

    # Check if 'tostage.txt' is in the stage list after deletion
    file_names_after_deletion = [
        file_info["name"].split("/")[-1] for file_info in stage_list_after_deletion
    ]
    if "tostage.txt" not in file_names_after_deletion:
        print("'tostage.txt' has been successfully deleted from the stage.")
    else:
        print("Error: 'tostage.txt' is still present in the stage.")