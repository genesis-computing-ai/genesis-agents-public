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
import re
from datetime import datetime, timezone
from textwrap import dedent
from genesis_bots.core.logging_config import logger
# The import statement seems to be incomplete and has a typo. Correcting it to import the sqlite3 module.


# Regex for matching valid artifcat UUIDs
ARTIFACT_ID_REGEX = r'[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}' # regrex for matching a valid artifact UUID

# Regex for matching valid artifact markdown of the format [description](artifact:/<uuid>). will match a triplet (see lookup_artifact_markdown)
ARTIFACT_MARKDOWN_REGEX_STRICT = r'(\[([^\]]+)\]\(artifact:/(' + ARTIFACT_ID_REGEX + r')\))'

# like ARTIFACT_MARKDOWN_REGEX_STRICT, but will also allow for the image markdown like ![...](...).
ARTIFACT_MARKDOWN_REGEX_NONSTRICT = r'(!?\[([^\]]+)\]\(artifact:/(' + ARTIFACT_ID_REGEX + r')\))'


def _validate_artifact_id_format(aid):
    if isinstance(aid, str) and re.match(ARTIFACT_ID_REGEX, aid):
        return True
    return False

def lookup_artifact_markdown(txt, strict=False):
    """
    Searches for artifact markdown patterns within the given text.

    This function identifies and extracts pseudo-URL markdown patterns that reference artifacts.
    The patterns can be either strict or non-strict, depending on the 'strict' parameter.

    Args:
        txt (str): The text to search for artifact markdown patterns.
        strict (bool, optional): If True, uses a strict pattern that matches only standard markdown.
                                 If False, allows for image markdown patterns as well. Defaults to False.

    Returns:
        list: A list of matched markdown as 3-tuples: (full-markdown, description, UUID)
    """
    pattern = ARTIFACT_MARKDOWN_REGEX_STRICT if strict else ARTIFACT_MARKDOWN_REGEX_NONSTRICT
    return re.findall(pattern, txt)


class ArtifactsStoreBase(ABC):
    '''
    Provides a Create+read interface for artifacts and their metadata
    '''
    # Metadata fields user MUST provide when creating a new artifact
    METADATA_IN_REQUIRED_FIELDS = {'mime_type',         # mime type of the artifact
                                   'bot_id',            # Id of bot which created this artifact
                                   'title_filename',    # A filename to use as a meaningful name for this artifact
                                   'func_name',         # the name of the function whcih created this artifact
                                   }

    # Metadata fields user MAY provide when creating a new artifact
    METADATA_IN_OPTIONAL_FIELDS =  {'short_description',# A short description of this artifact
                                    'thread_context',   # A (long) description of the context that lead to the generation of this artifact.
                                                        # This is used for 'priming' LLM's context to allow exploring this artificat in a new chat thread.
                                   }

    # Additional Metadata fields user SHOULD expect to find when fetching an artifact
    METADATA_OUT_EXPECTED_FIELDS = METADATA_IN_REQUIRED_FIELDS | {'basename',           # the basename of the file, as it is stored on the stage
                                                                  'orig_path',          # the path of the original file loaded into the stage
                                                                  'creation_timestamp', # Time at which the item was created, in UTC, iso format.
                                                                  }


    def create_artifact(self, content: Any, metadata: dict) -> str:
        raise NotImplementedError("This is a pure abstract method and must be implemented by subclasses.")

    def read_artifact(self, artifact_id: str, local_out_dir: str) -> str:
        raise NotImplementedError("This is a pure abstract method and must be implemented by subclasses.")

    def list_artifacts(self) -> List[Dict[str, Any]]:
        raise NotImplementedError("This is a pure abstract method and must be implemented by subclasses.")

    def create_artifact(self, content: Any, metadata: dict) -> str:
        raise NotImplementedError("This is a pure abstract method and must be implemented by subclasses.")

    def delete_artifacts(self, artifac_ids: List[str]) -> bool:
        raise NotImplementedError("This is a pure abstract method and must be implemented by subclasses.")


class SnowflakeStageArtifactsStore(ArtifactsStoreBase):

    STAGE_NAME = 'ARTIFACTS' # name of the stage used for artifacts tracking
    STAGE_ENCRYPTION_TYPE = 'SNOWFLAKE_SSE'
    METADATA_FILE_EXTRA_SUFFIX = ".metadata.json"
    METADATA_FILE_NAMED_FORMAT = 'artifact_meta_json'
    SIGNED_URL_MAX_EXPIRATION_SECS = 604800
    SIGNED_URL_WRAPPER_SPROC_NAME = 'GET_PRESIGNED_URL_WRAPPER'

    def __init__(self,
                 snowflake_connector # A SnowflakeConnector
                 ):
        # validate params
        from genesis_bots.connectors import SnowflakeConnector # avoid circular imports
        assert isinstance(snowflake_connector, SnowflakeConnector)

        self._sfconn = snowflake_connector
        self._stage_qualified_name = f"{snowflake_connector.genbot_internal_project_and_schema}.{self.STAGE_NAME}"
        self._signed_url_sproc_name = f"{snowflake_connector.genbot_internal_project_and_schema}.{self.SIGNED_URL_WRAPPER_SPROC_NAME}"


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
        cursor = self._get_sql_cursor()
        if type(cursor).__name__ == "SQLiteCursorWrapper":
            return False
        with cursor:
            cursor.execute(stage_check_query)  # Corrected variable name
            return bool(cursor.fetchone())


    def setup_db_objects(self, replace_if_exists: bool = False) -> bool:
        """
        Ensures the existence of a Snowflake stage and other supporting objects for artifact storage. If the objects already exists,
        they can optionally be replaced based on the `replace_if_exists` flag.

        Args:
            replace_if_exists (bool): If True, replaces the existing stage and other objects. Defaults to False
        """
        # Create new stage?
        stage_ddl_prefix = None
        if self.does_storage_exist():
            if replace_if_exists:
                logger.info(f"Stage @{self._stage_qualified_name} already exists but {replace_if_exists=}. Will replace Stage")
                stage_ddl_prefix = "CREATE OR REPLACE STAGE"
            else:
                logger.info(f"Stage @{self._stage_qualified_name} already exists. (No-op)")
        else:
            stage_ddl_prefix = "CREATE STAGE IF NOT EXISTS"
        if stage_ddl_prefix:
            stage_ddl = stage_ddl_prefix + (f" {self._stage_qualified_name}"
                                            f" ENCRYPTION = (TYPE = '{self.STAGE_ENCRYPTION_TYPE}')"
                                            ##f" DIRECTORY = (ENABLE = TRUE)" # uncomment if you want to manage a DIRECTORY table
                                            " ;")
            with self._get_sql_cursor() as cursor:
                cursor.execute(stage_ddl)
                self._sfconn.client.commit()
                logger.info(f"Stage @{self._stage_qualified_name} created using '{stage_ddl_prefix.lower()}'")

        # reate a wrapper sproce for GET_PRESIGNED_URL.
        # We always create or replace to ensure latest version - this is a statless function).
        # We need a wrapper that runs inside a snowflake host and not a native app host.
        # See https://github.com/genesis-gh-jlangseth/genesis/issues/98
        # We use a sproc and not a siple SQL function because snowflake expects the stage to be qualified
        # with @ and will not allow creating it dynamically.
        sproc_ddl = dedent(f'''
            CREATE OR REPLACE PROCEDURE {self._signed_url_sproc_name} (
                STAGE_NAME STRING,
                FILE_PATH STRING,
                EXPIRATION FLOAT
            )
            RETURNS STRING
            LANGUAGE JAVASCRIPT
            EXECUTE AS OWNER
            AS
            $$
                // Construct the SQL command for GET_PRESIGNED_URL
                // Create the full reference to the stage using concatenation
                var stage_reference = '@' + STAGE_NAME; // Prefix with '@' to form the stage reference
                var sql_command = `SELECT GET_PRESIGNED_URL(${{stage_reference}}, '${{FILE_PATH}}', ${{EXPIRATION}}) AS presigned_url`;
                // Create the Snowflake statement and execute it
                var statement1 = snowflake.createStatement({{sqlText: sql_command}});
                var result_set = statement1.execute();
                // Retrieve the presigned URL from the result set
                if (result_set.next()) {{
                    return result_set.getColumnValue("PRESIGNED_URL");
                }} else {{
                    return null;
                }}
            $$;
            ''')
        with self._get_sql_cursor() as cursor:
            cursor.execute(sproc_ddl)
            self._sfconn.client.commit()
            logger.info(f"Created PROCEDURE {self._signed_url_sproc_name}")


    def create_artifact_from_file(self, file_path, metadata: dict):
        """
        Create an artifact from a file and its associated metadata.

        This method uploads a file and its metadata to a Snowflake stage, creating a unique artifact identifier.
        The file is first copied to a temporary directory to ensure it can be uploaded without renaming issues.

        Args:
            file_path (str or Path): The path to the file to be uploaded as an artifact.
            metadata (dict): A dictionary containing metadata for the artifact. See METADATA_IN_REQUIRED_FIELDS for list of manadory fields.

        Returns:
            str: A unique identifier for the created artifact.

        Raises:
            ValueError: If 'mime_type' is not present in the metadata.
            PermissionError: If the file cannot be read due to permission issues.
        """
        metadata = metadata.copy() # work with a copy as we are modifying it below

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
        assert "creation_timestamp" not in metadata
        metadata["basename"] = str(target_filename)
        metadata["orig_path"] = str(file_path)
        metadata["creation_timestamp"] = datetime.now(timezone.utc).isoformat()

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
        logger.info(f"New artifact created {artifact_id=}, by {metadata['func_name']} with title='{metadata['title_filename']}' ")
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
            if isinstance(content, str):
                content = content.encode('utf-8')  # Convert string to bytes (tmp file is opened in w+b mode by default)
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
        if not _validate_artifact_id_format(artifact_id):
            raise ValueError(f"invalid articat id: {artifact_id}")

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
            ValueError: If the metadata for this artifact cannot be found or we failed to generate the signed URL.
        """
        if not _validate_artifact_id_format(artifact_id):
            raise ValueError(f"Invalid artifact id: {artifact_id}")

        metadata = self.get_artifact_metadata(artifact_id)
        basename = metadata.get('basename')
        if not basename:
            raise ValueError(f"Corrupted metadata for {artifact_id}. Missing basename attribute")

        query = f"CALL {self._signed_url_sproc_name}('{self._stage_qualified_name}', '{basename}', {self.SIGNED_URL_MAX_EXPIRATION_SECS});"
        with self._get_sql_cursor() as cursor:
            cursor.execute(query)
            row = cursor.fetchone()
            if not row:
                raise ValueError(f"Failed to create a signed URL for artifact {artifact_id}")

            result = row[0]
            logger.info(f"Generated PRESIGNED_URL for artifact_id={artifact_id} using query={query}. Result: {result}")
            return result


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
        if not _validate_artifact_id_format(artifact_id):
            raise ValueError(f"invalid articat id: {artifact_id}")

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


    def delete_artifacts(self, artifac_ids: List[str]) -> List[str]:
        """
        Deletes artifacts from the Snowflake stage based on the provided list of artifact IDs.

        Args:
            artifac_ids (List[str]): A list of unique artifact identifiers to be deleted.

        Returns:
            List[str]: A list of artifact IDs that were successfully deleted. E.g. if the list is empty, no artifacts were deleted.

        Raises:
            ValueError: If any artifact ID has invalid format, or the deletion execition failed.
        """
        # TODO: support special fletring for deletion e.g. 'ALL' for deleting all artifacts, or a filter based on some metadata predicate.
        # validate input
        if not artifac_ids:
            raise ValueError("The list of artifact IDs is empty.")
        for aid in artifac_ids:
            if not _validate_artifact_id_format(aid):
                raise ValueError(f"Invalid artifact ID format: {aid}")

        # delete one by one
        succeeded = []
        for aid in artifac_ids:
            with self._get_sql_cursor() as cursor:
                # Remove all files that begin with the aid name (Snowflake treats the path as a prefix filter)
                # this will remove both the data file and metadata file
                sql = f"REMOVE @{self._stage_qualified_name}/{aid}"
                cursor.execute(sql)
                row_count = cursor.rowcount
                if row_count > 0:
                    logger.info(f"Deleted artifcat {aid}")
                    succeeded.append(aid)

        # Clear the cache for get_artifact_metadata after deletion
        if len(succeeded) > 0:
            self.get_artifact_metadata.cache_clear()
        return succeeded


    def get_llm_artifact_ref_instructions(self, artifact_id: str) -> str:
        """
        Generate instructions for referencing an artifact in LLM responses.

        Args:
            artifact_id (str): The unique identifier of the artifact.

        Returns:
            str: Instructions for referencing the artifact in both text/plain and text/html formats.
        """
        metadata = self.get_artifact_metadata(artifact_id)
        title = metadata['title_filename']
        mime_type = metadata.get('mime_type', '')

        # Simple check if this is an image
        is_image = mime_type and mime_type.startswith('image/')

        # Use image markdown for images, regular markdown for other files
        markdown = f"!{'' if is_image else ''}[{title}](artifact:/{artifact_id})"

        return f"Here is a markdown syntax you (assistant) can use to {'render' if is_image else 'reference'} this artifact when responding to the user: {markdown}. Strictly follow this markdown syntax. Note that this markdown cannot be used by the user. DO NOT suggest to the user to use this markdown."


def get_artifacts_store(db_adapter):
    from genesis_bots.connectors import SnowflakeConnector # avoid circular imports
    if isinstance(db_adapter, SnowflakeConnector):
        return SnowflakeStageArtifactsStore(db_adapter)
    else:
        raise NotImplementedError(f"No artifacts store is implemented for {db_adapter}")


