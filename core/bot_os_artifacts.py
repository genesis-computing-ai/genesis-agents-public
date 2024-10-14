
# Notes:
# -------
# how I imagine a 'genertate an email with a chart' to work:
# * The bot procces to follow would look something like:
#    1) In current Snowflake account, run a SQL query to get the number of queries run daily in the last 10 days.
#    2) Using snowpark python, run  a python code to run the query above and build a timeseries chart from the result plotting the
#       query count against dates
#    3) Send me an email linking to the chart explaining what the chart is showing.
#
# * first, the bot needs to figure out the SQL query
# * then the bot will use the tool run_python_code (make sure it runs in snowpark not by the openai code tool!) to generte the image.
# * we will need to teach it to store the output as an artifact (with an ID) and not to 'downloaded files'.
# * Then, in order to wrap in an an HTML, the bot should get the external URL of that artifact, and use it in an <img> tag.
#
#   * Future: python codes can be 'artifacts' themsleves (with metadata describing entrypoints, test, etc) and can be executed later by a generic sproc
#   * the execution of the code creates another artifact - an image artifact. This 'competes' witht the Notebook mechanism whcih is also meant to
#     be used for stroign python, SQL, etc.
#   * ?? can we wrap the 'generate chart' workflow as its own tool and teach the bot about it?
#   * ?? Use the 'action' API as a wrapper over CRUD? seems to work better for Cortex? See example in ToolBelt.manage_notebook and notebook_manager_functions
# Resources:
#   Staging and URL access: https://docs.snowflake.com/en/user-guide/unstructured-intro
#

# TODO next: (2024-10-02): debug the CRUD code. I never tested it. Try to load a test file from ./downloaded_files/*.png to the stage.
#            Stick some code that tests it during the last stages of the initialization


from typing import Any, Dict, List, Optional, Union, IO
import os
import base64
import uuid
from enum import Enum
import json
from abc import ABC, abstractmethod  # Missing import for ABC and abstractmethod
from pathlib import Path
from uuid import uuid4
import shutil
import tempfile
import functools


# class ArtifactMetadata:
#     def __init__(self,
#                  mime_type: str,
#                  friendly_name: Optional[str] = None,
#                  description: Optional[str] = None,
#                  tags: Optional[List[str]] = None):
#         self._mime_type = mime_type
#         self._friendly_name = friendly_name
#         self._description = description
#         self._tags = tags

#     def to_dict(self) -> Dict[str, Any]:
#         return {
#             "mime_type": self._mime_type,
#             "friendly_name": self._friendly_name,
#             "description": self._description,
#             "tags": self._tags
#         }

#     @classmethod
#     def from_dict(cls, data: Dict[str, Any]) -> 'ArtifactMetadata':
#         # TODO: validate input
#         return cls(
#             mime_type=data.get("mime_type"),
#             friendly_name=data.get("friendly_name"),
#             description=data.get("description"),
#             tags=data.get("tags")
#         )

#     def to_json(self) -> str:
#         return json.dumps(self.to_dict())

#     @classmethod
#     def from_json(cls, json_str: str) -> 'ArtifactMetadata':
#         data = json.loads(json_str)
#         return cls.from_dict(data)


#     def __repr__(self) -> str:
#         return f"{self.__class__.__name__}({json.dumps(self.to_dict(), indent=4)})"


class ArtifactsStoreBase(ABC):
    '''
    Provides a CRUD interface for artifacts and their metadata
    '''

    def create_artifact(self, content: Any, metadata: dict) -> str:
        """
        Create an artifact by uploading content to the artifact store and associating it with metadata.

        Args:
            content (Any): The content to be uploaded as an artifact. Can be a string or a file-like object.
            metadata (dict-like): Metadata associated with the artifact, including name, description, and tags.

        Returns:
            str: A unique identifier for the created artifact.
        """
        raise NotImplementedError("This is a pure abstract method and must be implemented by subclasses.")

    def read_artifact(self, artifact_id: str) -> bytes:
        raise NotImplementedError("This is a pure abstract method and must be implemented by subclasses.")

    def list_artifacts(self) -> List[Dict[str, Any]]:
        raise NotImplementedError("This is a pure abstract method and must be implemented by subclasses.")


class SnowflakeStageArtifactsStore(ArtifactsStoreBase):

    STAGE_NAME = 'ARTIFACTS' # name of the stage used for artifacts tracking
    STAGE_ENCRYPTION_TYPE = 'SNOWFLAKE_SSE'
    METADATA_FILE_EXTRA_SUFFIX = ".metadata.json"
    METADATA_FILE_NAMED_FORMAT = 'artifact_meta_json'
    SIGNED_URL_MAX_EXPIRATION_SECS = 604800

    def __init__(self,
                 snowflake_connector # A SnowflakeConnector
                 ):
        # validate params
        from connectors import SnowflakeConnector # avoid circular imports
        assert isinstance(snowflake_connector, SnowflakeConnector)

        self._sfconn = snowflake_connector
        self._stage_qualified_name = f"{snowflake_connector.genbot_internal_project_and_schema}.{self.STAGE_NAME}"


    @property
    def stage_qualified_name(self) -> str:
        return self._stage_qualified_name



    def _get_sql_cursor(self, cursor=None):
        return cursor or self._sfconn.client.cursor()


    @functools.lru_cache(maxsize=1)
    def _get_metadata_named_format(self) -> str:
        """
        Returns the name of a the file format for the metadata files (which are JSON files).
        This method is memoized to ensure the file format is created only once per session.

        Returns:
            str: The fully qualified name of the JSON file format.
        """
        ff_name = f"{self._sfconn.genbot_internal_project_and_schema}.{self.METADATA_FILE_NAMED_FORMAT}"
        ddl = f"CREATE TEMP FILE FORMAT IF NOT EXISTS {ff_name} TYPE = 'json';"
        with self._get_sql_cursor() as cursor:
            cursor.execute(ddl)
            cursor.fetchone()
        return ff_name


    def _make_artifact_filename(self,
                                artifact_id,
                                orig_filename) -> str:
        '''
        Create a unique filename using the (unique) artifact_id. retain original suffix.
        '''
        file_extension = Path(orig_filename).suffix
        target_filename = Path(artifact_id).with_suffix(file_extension)
        return str(target_filename)

        # create a metadata file name by removing the original suffix (if any) and adding the special suffix
        metadata_filename = target_filename.with_suffix(self.METADATA_FILE_EXTRA_SUFFIX)


    def _get_metadata_filename(self, artifact_id):
        '''
        Get the filename that would match the artifact_id
        '''
        return str(Path(artifact_id).with_suffix(self.METADATA_FILE_EXTRA_SUFFIX))


    def does_storage_exist(self):
        stage_check_query = f"SHOW STAGES LIKE '{self.STAGE_NAME}' IN SCHEMA {self._sfconn.genbot_internal_project_and_schema};"
        with self._get_sql_cursor() as cursor:
            cursor.execute(stage_check_query)  # Corrected variable name
            return bool(cursor.fetchone())


    def create_storage_if_needed(self, replace_if_exists: bool = False) -> bool:
        """
        Ensures the existence of a Snowflake stage for artifact storage. If the stage already exists,
        it can optionally be replaced based on the `replace_if_exists` flag.

        Args:
            replace_if_exists (bool): If True, replaces the existing stage. Defaults to False.

        Returns:
            bool: True if the stage was created or replaced, False if the stage already existed and was not replaced.
        """
        stage_ddl_prefix = None
        if self.does_storage_exist():
            if replace_if_exists:
                print(f"Stage @{self._stage_qualified_name} already exists but {replace_if_exists=}. Will replace Stage")
                stage_ddl_prefix = "CREATE OR REPLACE STAGE"
            else:
                print(f"Stage @{self._stage_qualified_name} already exists. (NoOp  since {replace_if_exists=}")
                return False # exists and nothing to do
        else:
            stage_ddl_prefix = "CREATE STAGE IF NOT EXISTS"
        stage_ddl = stage_ddl_prefix + (f" {self._stage_qualified_name}"
                                        f" ENCRYPTION = (TYPE = '{self.STAGE_ENCRYPTION_TYPE}')"
                                        ##f" DIRECTORY = (ENABLE = TRUE)" # uncomment if you want to manage a DIRECTORY table
                                        " ;")

        # create (if needed, or replace)
        with self._get_sql_cursor() as cursor:
            cursor.execute(stage_ddl)
            self._sfconn.client.commit()
            print(f"Stage @{self._stage_qualified_name} created using '{stage_ddl_prefix.lower()}'")

        return True


    def create_artifact_from_file(self, file_path, metadata: dict):
        """
        Create an artifact from a file and its associated metadata.

        This method uploads a file and its metadata to a Snowflake stage, creating a unique artifact identifier.
        The file is first copied to a temporary directory to ensure it can be uploaded without renaming issues.

        Args:
            file_path (str or Path): The path to the file to be uploaded as an artifact.
            metadata (dict): A dictionary containing metadata for the artifact. Must include a 'mime_type' key.

        Returns:
            str: A unique identifier for the created artifact.

        Raises:
            ValueError: If 'mime_type' is not present in the metadata.
            PermissionError: If the file cannot be read due to permission issues.
        """
        # Validate input
        if 'mime_type' not in metadata:
            raise ValueError("Metadata must contain a 'mime_type' key.")

        # Check read permission on the file path
        file_path = Path(file_path)
        if not file_path.is_file() or not os.access(file_path, os.R_OK):
            raise PermissionError(f"Read permission denied for file: {file_path}")

        # Create a unique filename using uuid4. Retain original suffix if exists.
        artifact_id = str(uuid4())
        file_extension = file_path.suffix
        target_filename = self._make_artifact_filename(artifact_id, file_path)
        assert "basename" not in metadata
        metadata["basename"] = str(target_filename)
        metadata["orig_path"] = str(file_path)

        # Create a metadata file name by removing the original suffix (if any) and adding the special suffix
        metadata_filename = self._get_metadata_filename(artifact_id)

        # Create the files to upload in the /tmp directory first
        # (PUT command cannot rename the source file)
        with tempfile.TemporaryDirectory() as tmpdirname:
            temp_file_path = Path(tmpdirname) / target_filename
            temp_metadata_path = Path(tmpdirname) / metadata_filename
            shutil.copy(file_path, temp_file_path)

            # Serialize metadata to a JSON file
            with open(temp_metadata_path, 'w') as metadata_file:
                json.dump(metadata, metadata_file)

            # Load the file and metadata file to a Snowflake stage
            with self._get_sql_cursor() as cursor:
                # Note: we silently ignore an overwrite of the file with the same name, but UUIDs should be globally unique.
                # Using OVERWRITE=FALSE would mean some performance overhead to list the files first.
                query = f"PUT file://{temp_file_path} @{self._stage_qualified_name} AUTO_COMPRESS=FALSE"
                cursor.execute(query)

                # Load the metadata file to a Snowflake stage
                metadata_query = f"PUT file://{temp_metadata_path} @{self._stage_qualified_name} AUTO_COMPRESS=FALSE"
                cursor.execute(metadata_query)

                self._sfconn.client.commit()
        return artifact_id


    def create_artifact_from_content(self,
                                     content,
                                     metadata: dict,
                                     content_filename: str):
        """
        Create an artifact from the given content and metadata.

        This method writes the provided content to a temporary file and then
        creates an artifact from it. The content_filename parameter is used
        to specify the name to give the content as if it was a file name,
        which will later be presented to the user as part of the metadata.

        :param content: The content to be stored as an artifact.
        :param metadata: A dictionary containing metadata for the artifact. Must include 'mine_type' key.
        :param content_filename: The name to assign to the content, used as
                                 a file name in the metadata.
        :return: The unique identifier for the created artifact.
        :raises ValueError: If content_filename is not provided.
        """
        # Extract the suffix from content_filename
        suffix = Path(content_filename).suffix

        # Create a temporary file with the same suffix and write the content to it
        # We want to retain the suffix as it is retained in the  artifact filename
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as temp_file:
            temp_file.write(content)
            temp_file_path = temp_file.name

        try:
            # Call create_artifact_from_file with the temporary file
            artifact_id = self.create_artifact_from_file(temp_file_path, metadata)
        finally:
            # Ensure the temporary file is removed
            os.remove(temp_file_path)

        return artifact_id


    def get_artifact_metadata(self, artifact_id) -> dict:
        if artifact_id is None:
            raise ValueError("artifact_id cannot be None")

        metadata_filename = self._get_metadata_filename(artifact_id)
        file_format = self._get_metadata_named_format()
        query = f"SELECT $1 FROM @{self._stage_qualified_name}/{metadata_filename} (FILE_FORMAT => '{file_format}');"

        with self._get_sql_cursor() as cursor:
            cursor.execute(query)
            row = cursor.fetchone()
            if not row:
                raise ValueError(f"Invalid artifact_id {artifact_id}: Failed to fetch metadata")
            return json.loads(row[0])


    def get_signed_url_for_artifact(self, artifact_id):
        metadata = self.get_artifact_metadata(artifact_id)
        basename = metadata.get('basename')
        if not basename:
            raise ValueError(f"Corrupted metadata for {artifact_id}. Missing basename attribute")
        query = (f"SELECT GET_PRESIGNED_URL(@{self._stage_qualified_name},"
                 f" '{basename}', "
                 f" {self.SIGNED_URL_MAX_EXPIRATION_SECS});")
        with self._get_sql_cursor() as cursor:
            cursor.execute(query)
            row = cursor.fetchone()
            if not row:
                raise ValueError(f"Failed to create a signed URL for artifact {artifact_id}")
            return row[0]


    def read_artifact(self, artifact_id: str) -> bytes:
        pass


    def list_artifacts(self) -> List[Dict[str, Any]]:
        pass
