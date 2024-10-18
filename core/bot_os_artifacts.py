"""
bot_os_artifacts.py

This module provides an interface for managing artifacts and their metadata within a Snowflake environment. 
It defines an abstract base class `ArtifactsStoreBase` for CRUD operations on artifacts, and a concrete 
implementation `SnowflakeStageArtifactsStore` that utilizes Snowflake stages for storage.

Key Features:
    - Create, read, and list artifacts with associated metadata.
    - Store artifacts in Snowflake stages with optional encryption.
    - Generate signed URLs for secure access to artifacts.
    - Ensure storage existence and manage stage creation or replacement.

"""

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

ARTIFACT_ID_REGEX = r'[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}' # regrex for matching a valid artifact UUID

class ArtifactsStoreBase(ABC):
    '''
    Provides a Create+read interface for artifacts and their metadata
    '''

    METADATA_IN_REQUIRED_FIELDS = {'mime_type'}
    METADATA_OUT_EXPECTED_FIELDS = METADATA_IN_REQUIRED_FIELDS | {'basename', 'orig_path'}

    def create_artifact(self, content: Any, metadata: dict) -> str:
        raise NotImplementedError("This is a pure abstract method and must be implemented by subclasses.")

    def read_artifact(self, artifact_id: str, local_out_dir: str) -> str:
        raise NotImplementedError("This is a pure abstract method and must be implemented by subclasses.")

    def list_artifacts(self) -> List[Dict[str, Any]]:
        raise NotImplementedError("This is a pure abstract method and must be implemented by subclasses.")

    def create_artifact(self, content: Any, metadata: dict) -> str:
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
        Returns the name of a the file format used in the internal Storage for the metadata files (which are JSON files).
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
        if not self.METADATA_IN_REQUIRED_FIELDS.issubset(metadata.keys()):
            raise ValueError(f"Missing keys in metadata: {self.METADATA_IN_REQUIRED_FIELDS - metadata.keys()}")

        # Check read permission on the file path
        file_path = Path(file_path)
        if not file_path.is_file() or not os.access(file_path, os.R_OK):
            raise PermissionError(f"Read permission denied for file: {file_path}")

        # Create a unique filename using uuid4. Retain original suffix if exists.
        # Having a meaningful suffix is reduntant since we have the mime type in the metadaa
        # but having it helps with human maintenance (we know what the file contains) and
        # allows us to guess the content type without fetching the metadata (for performance reasons)
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
        :param metadata: A dictionary containing metadata for the artifact. Must include 'mime_type' key.
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


    @functools.lru_cache(maxsize=100)
    def get_artifact_metadata(self, artifact_id) -> dict:
        """
        Retrieve the metadata for a given artifact as a dict

        Args:
            artifact_id: The unique identifier for the artifact whose metadata is to be retrieved.

        Returns:
            A dictionary containing the metadata of the artifact.
            See METADATA_OUT_EXPECTED_FIELDS for a list of minimum expected fields.

        Raises:
            ValueError: If the metadata for this artifact_is is not found.
        """
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
        """
        Generate a signed URL for accessing an artifact from an external system. 

        Args:
            artifact_id: The unique identifier for the artifact.

        Returns:
            A signed URL string for accessing the artifact.

        Raises:
            ValueError: If the metadata from this artifac cannot be found or we failed to generate the signed URL.
        """
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


    def read_artifact(self, artifact_id: str, local_out_dir) -> str:
        """
        Retrieve an artifact from the Snowflake stage and save it to a local directory.

        Args:
            artifact_id (str): The unique identifier for the artifact to be retrieved.
            local_out_dir (str): The local directory path where the artifact will be saved.

        Returns:
            str: The basename of the file within the given sirectory.

        Raises:
            ValueError: If the artifact_id is None, or if the metadata is corrupted or missing.
        """
        if artifact_id is None:
            raise ValueError("artifact_id cannot be None")

        metadata = self.get_artifact_metadata(artifact_id)
        basename = metadata.get('basename')
        if not basename:
            raise ValueError(f"Corrupted metadata for {artifact_id}. Missing basename attribute")
        sql = f"GET @{self._stage_qualified_name}/{basename} file://{local_out_dir}"

        # Execute the GET command
        with self._get_sql_cursor() as cursor:
            cursor.execute(sql)
            row = cursor.fetchone()
            if not row:
                raise ValueError(f"Failed to retrieve artifact {artifact_id}")
            return row[0]


    def list_artifacts(self) -> List[Dict[str, Any]]:
        raise NotImplementedError()


def get_artifacts_store(db_adapter):
    from connectors import SnowflakeConnector # avoid circular imports
    if isinstance(db_adapter, SnowflakeConnector):
        return SnowflakeStageArtifactsStore(db_adapter)
    else:
        raise NotImplementedError(f"No artifacts store is implemented for {db_adapter}")