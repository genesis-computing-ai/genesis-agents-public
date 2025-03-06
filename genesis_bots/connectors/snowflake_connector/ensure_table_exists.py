from   datetime                 import datetime
import glob
import json
import os
import pandas as pd
import pytz
import random
import string
from   textwrap                 import dedent
import yaml

from   genesis_bots             import core
from   genesis_bots.connectors.sqlite_adapter \
                                import SQLiteAdapter

from   genesis_bots.core.bot_os_defaults \
                                import (BASE_EVE_BOT_AVAILABLE_TOOLS_SNOWFLAKE,
                                        BASE_EVE_BOT_INSTRUCTIONS,
                                        ELIZA_INTRO_PROMPT, EVE_INTRO_PROMPT,
                                        JANICE_INTRO_PROMPT,
                                        JANICE_JANITOR_INSTRUCTIONS,
                                        STUART_INTRO_PROMPT)

from   genesis_bots.core.logging_config \
                                import logger

def one_time_db_fixes(self):
   
    bots_table_check_query = f"SHOW TABLES LIKE 'BOT_SERVICING' IN SCHEMA {self.schema};"
    cursor = self.client.cursor()
    cursor.execute(bots_table_check_query)

    if cursor.fetchone():
        # Fetch all existing bots
        fetch_bots_query = f"SELECT BOT_NAME, AVAILABLE_TOOLS FROM {self.schema}.BOT_SERVICING;"
        cursor.execute(fetch_bots_query)
        bots = cursor.fetchall()

        for bot in bots:
            bot_name, tools = bot
            if tools:
                tools_list = json.loads(tools)
                update = False
                #     if 'notebook_manager_tools' not in tools_list:
                #         tools_list.append('notebook_manager_tools')
                #         update = True

                if "autonomous_tools" in tools_list:
                    logger.info("Found autonomous_tools in tools list")
                    tools_list.remove("autonomous_tools")
                    update = True

                if "todo_manager_tools" in tools_list:
                    tools_list.remove("todo_manager_tools")
                    if "project_manager_tools" not in tools_list:
                        tools_list.append('project_manager_tools')

                if 'database_tools' in tools_list:
                    logger.info("Found database_tools in tools list")
                    tools_list.remove('database_tools')
                    if "snowflake_tools" not in tools_list:
                        tools_list.append('snowflake_tools')
                    if "data_connector_tools" not in tools_list:
                        tools_list.append("data_connector_tools")
                    update = True

                if "snowflake_stage_tools" in tools_list:
                    logger.info("Found snowflake_stage_tools in tools list")
                    tools_list.remove("snowflake_stage_tools")
                    if "snowflake_tools" not in tools_list:
                        tools_list.append("snowflake_tools")
                    update = True

                if update:
                    updated_tools = json.dumps(tools_list)
                    # updated_tools = ','.join(tools_list)
                    update_query = f"""
                    UPDATE {self.schema}.BOT_SERVICING
                    SET AVAILABLE_TOOLS = %s
                    WHERE BOT_NAME = %s
                    """
                    cursor.execute(update_query, (updated_tools, bot_name))


        self.client.commit()
    # logger.info("Added notebook_manager_tools to all existing bots.")
    else:
        logger.info("BOTS table does not exist. Skipping tool addition.")
    
    if cursor is not None:
        cursor.close()

    return

def ensure_table_exists(self):
    from genesis_bots.core import bot_os_tool_descriptions
    from genesis_bots.core.bot_os_artifacts import get_artifacts_store

    # >>> BEGIN helper functions
    # -------------------------------
    def _fetch_all_schema_tables():
        """
        A helper function that fetches all table names in self.schema and returns them as a set.
        This set is used for testing table existence below.
        """
        retval = set()
        all_schema_tables_query = f"SHOW TABLES IN SCHEMA {self.schema};"
        with self.client.cursor() as cursor:
            try:
                cursor.execute(all_schema_tables_query)
                retval = {row[1] for row in cursor.fetchall()}  # Assuming the table name is in the second column
                logger.debug(f"Tables in schema {self.schema} (sorted): {sorted(retval)}")
            except Exception as e:
                logger.error(f"An error occurred while retrieving tables in schema {self.schema}: {e}")
        return retval

    sqlite = isinstance(self.client, SQLiteAdapter)
    if not sqlite:
        all_schema_tables = _fetch_all_schema_tables()
    else:
        all_schema_tables = None

    def _check_table_exists(tbl_name):
        ''' return true if tbl_name (unqualified) exists in self.schema '''
        sqlite = isinstance(self.client, SQLiteAdapter)
        if sqlite:
            with self.client.cursor() as cursor:
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (tbl_name,))
                return cursor.fetchone() is not None
        return tbl_name in all_schema_tables

    def _create_table_if_not_exist(tbl_name, ddl, raise_on_failure=False):
        """
        Checks if a table exists in the specified schema and creates it if it does not exist.

        This function uses the provided DDL statement to create the table if it is not already present in the schema.

        Parameters:
        tbl_name (str): The name of the table to check and potentially create.
        ddl (str): The Data Definition Language (DDL) statement to execute if the table does not exist.
        raise_on_failure (bool): If True, re-raises exceptions encountered during table creation; otherwise, logs the error.

        Returns:
        bool: True if the table was created, False if it already exists or an error occurred.

        Logs:
        - Info: Indicates whether the table was created successfully or already exists.
        - Error: Logs any exceptions that occur during the table creation process.
        """
        if not _check_table_exists(tbl_name):
            try:
                with self.client.cursor() as cursor:
                    cursor.execute(ddl)
                    self.client.commit()
                logger.info(f"Table {tbl_name} created successfully.")
                return True
            except Exception as e:
                if raise_on_failure:
                    raise
                logger.error(f"An error occurred while creating table {tbl_name}: {e}")
                return False
        else:
            logger.info(f"Table {tbl_name} already exists.")
            return False

    # <<< END  helper functions
    # -------------------------------

    # Get the current timestamp
    current_timestamp = self.get_current_time_with_timezone()
    # Format the timestamp as a string
    timestamp_str = current_timestamp

    # Create or replace the bots_active table with the current timestamp
    create_bots_active_table_query = f"""CREATE OR REPLACE TABLE {self.schema}.bots_active ("{timestamp_str}" STRING); """
    try:
        with self.client.cursor() as cursor:
            cursor.execute(create_bots_active_table_query)
            self.client.commit()
        logger.info(f"Table {self.schema}.bots_active created or replaced successfully with timestamp: {timestamp_str}")
    except Exception as e:
        logger.error(f"An error occurred while creating or replacing the bots_active table: {e}")

    # CUST_DB_CONNECTIONS to trigger its ensure_table_exists
    from genesis_bots.connectors.data_connector import DatabaseConnector
    db_connector = DatabaseConnector()

    # EXT_SERVICE_CONFIG
    # ---------------------
    create_external_service_config_table_ddl = f"""
        CREATE OR REPLACE TABLE {self.schema}.EXT_SERVICE_CONFIG (
            ext_service_name VARCHAR NOT NULL,
            parameter VARCHAR NOT NULL,
            value VARCHAR NOT NULL,
            user VARCHAR,
            created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    _create_table_if_not_exist('EXT_SERVICE_CONFIG', create_external_service_config_table_ddl, raise_on_failure=True)

    # G_DRIVE_FILE_VERSION
    # ---------------------
    # TODO: refactor to use _create_table_if_not_exist
    g_drive_file_version_table_check_query = (
        f"SHOW TABLES LIKE 'G_DRIVE_FILE_VERSION' IN SCHEMA {self.schema};"
    )
    try:
        cursor = self.client.cursor()
        cursor.execute(g_drive_file_version_table_check_query)

    except Exception as e:
        logger.error(f"Unable to execute 'SHOW TABLES' query: {e}\nQuery attempted: {g_drive_file_version_table_check_query}")
        raise Exception(
            f"Unable to execute 'SHOW TABLES' query: {e}\nQuery attempted: {g_drive_file_version_table_check_query}"
        )
    try:
        if not cursor.fetchone():
            create_g_drive_file_version_table_ddl = f"""
            CREATE OR REPLACE TABLE {self.schema}.G_DRIVE_FILE_VERSION (
                g_file_id VARCHAR NOT NULL,
                g_file_name VARCHAR NOT NULL,
                g_file_type VARCHAR NOT NULL,
                g_file_parent_id VARCHAR,
                g_file_size VARCHAR,
                g_file_version VARCHAR,
                created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            cursor = self.client.cursor()
            cursor.execute(create_g_drive_file_version_table_ddl)
            self.client.commit()
            logger.info(f"Table {self.schema}.G_DRIVE_FILE_VERSION created as Table successfully.")
        else:
            logger.info(f"Table {self.schema}.G_DRIVE_FILE_VERSION already exists.")
    except Exception as e:
        logger.error(
            f"An error occurred while checking or creating the G_DRIVE_FILE_VERSION table: {e}"
        )
    finally:
        if cursor is not None:
            cursor.close()

    # LLM_RESULTS
    # ---------------------
    # TODO: refactor to use _create_table_if_not_exist
    llm_results_table_check_query = (
        f"SHOW TABLES LIKE 'LLM_RESULTS' IN SCHEMA {self.schema};"
    )
    try:
        cursor = self.client.cursor()
        cursor.execute(llm_results_table_check_query)

    except Exception as e:
        logger.error(f"Unable to execute 'SHOW TABLES' query: {e}\nQuery attempted: {llm_results_table_check_query}")
        raise Exception(f"Unable to execute 'SHOW TABLES' query: {e}\nQuery attempted: {llm_results_table_check_query}")
    try:
        if not cursor.fetchone():
            create_llm_results_table_ddl = f"""
            CREATE OR REPLACE HYBRID TABLE {self.schema}.LLM_RESULTS (
                uu VARCHAR(40) PRIMARY KEY,
                message VARCHAR NOT NULL,
                created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX uu_idx (uu)
            );
            """
            cursor = self.client.cursor()
            cursor.execute(create_llm_results_table_ddl)
            self.client.commit()
            logger.info(f"Table {self.schema}.LLM_RESULTS created as Hybrid Table successfully.")
        else:
            logger.info(f"Table {self.schema}.LLM_RESULTS already exists.")
    except Exception as e:
        try:
            logger.error("Falling back to create non-hybrid table for LLM_RESULTS")
            create_llm_results_table_ddl = f"""
            CREATE OR REPLACE TABLE {self.schema}.LLM_RESULTS (
                uu VARCHAR(40) PRIMARY KEY,
                message VARCHAR NOT NULL,
                created TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            _create_table_if_not_exist('LLM_RESULTS', create_llm_results_table_ddl)
        except Exception as e:
            logger.error(f"An error occurred while creating the non-hybrid LLM_RESULTS table: {e}")
            pass

    # TASKS
    # ---------------------
    _create_table_if_not_exist(
        'TASKS',
        f"""
        CREATE TABLE {self.schema}.TASKS (
            task_id VARCHAR(255),
            bot_id VARCHAR(255),
            task_name VARCHAR(255),
            primary_report_to_type VARCHAR(50),
            primary_report_to_id VARCHAR(255),
            next_check_ts TIMESTAMP,
            action_trigger_type VARCHAR(50),
            action_trigger_details VARCHAR(1000),
            task_instructions TEXT,
            reporting_instructions TEXT,
            last_task_status VARCHAR(255),
            task_learnings TEXT,
            task_active BOOLEAN
        );
        """
    )

    # TASK_HISTORY
    # ---------------------
    create_task_history_table_ddl = f"""
    CREATE TABLE {self.schema}.TASK_HISTORY (
        task_id VARCHAR(255),
        run_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
        work_done_summary TEXT,
        task_status TEXT,
        updated_task_learnings TEXT,
        report_message TEXT,
        done_flag BOOLEAN,
        needs_help_flag BOOLEAN,
        task_clarity_comments TEXT
    );
    """
    _create_table_if_not_exist('TASK_HISTORY', create_task_history_table_ddl)

    # SEMANTIC_MODELS_DEV
    # ---------------------
    # TODO: refactor to use _create_table_if_not_exist
    semantic_stage_check_query = (
        f"SHOW STAGES LIKE 'SEMANTIC_MODELS_DEV' IN SCHEMA {self.schema};"
    )
    try:
        cursor = self.client.cursor()
        cursor.execute(semantic_stage_check_query)
        if not cursor.fetchone():
            semantic_stage_ddl = f"""
            CREATE STAGE {self.schema}.SEMANTIC_MODELS_DEV
            ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');
            """
            cursor.execute(semantic_stage_ddl)
            self.client.commit()
            logger.info(f"Stage {self.schema}.SEMANTIC_MODELS_DEV created.")
        else:
            logger.info(f"Stage {self.schema}.SEMANTIC_MODELS_DEV already exists.")
    except Exception as e:
        logger.error(f"An error occurred while checking or creating stage SEMANTIC_MODELS_DEV: {e}" )
    finally:
        if cursor is not None:
            cursor.close()

    # SEMANTIC_MODELS
    # ---------------------
    # TODO: refactor to use _create_table_if_not_exist
    semantic_stage_check_query = (
        f"SHOW STAGES LIKE 'SEMANTIC_MODELS' IN SCHEMA {self.schema};"
    )
    try:
        cursor = self.client.cursor()
        cursor.execute(semantic_stage_check_query)
        if not cursor.fetchone():
            semantic_stage_ddl = f"""
            CREATE STAGE {self.schema}.SEMANTIC_MODELS
            ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');
            """
            cursor.execute(semantic_stage_ddl)
            self.client.commit()
            logger.info(f"Stage {self.schema}.SEMANTIC_MODELS created.")
        else:
            logger.info(f"Stage {self.schema}.SEMANTIC_MODELS already exists.")
    except Exception as e:
        logger.error(f"An error occurred while checking or creating stage SEMANTIC_MODELS: {e}")
    finally:
        if cursor is not None:
            cursor.close()

    # SET_BOT_APP_LEVEL_KEY
    # ---------------------
    # TODO: refactor to use _create_table_if_not_exist
    udf_check_query = (
        f"SHOW USER FUNCTIONS LIKE 'SET_BOT_APP_LEVEL_KEY' IN SCHEMA {self.schema};"
    )
    try:
        cursor = self.client.cursor()
        cursor.execute(udf_check_query)
        if not cursor.fetchone():
            udf_creation_ddl = f"""
            CREATE OR REPLACE FUNCTION {self.schema}.set_bot_app_level_key (bot_id VARCHAR, slack_app_level_key VARCHAR)
            RETURNS VARCHAR
            SERVICE={self.schema}.GENESISAPP_SERVICE_SERVICE
            ENDPOINT=udfendpoint AS '/udf_proxy/set_bot_app_level_key';
            """
            cursor.execute(udf_creation_ddl)
            self.client.commit()
            logger.info(f"UDF set_bot_app_level_key created in schema {self.schema}.")
        else:
            logger.info(
                f"UDF set_bot_app_level_key already exists in schema {self.schema}."
            )
    except Exception as e:
        if os.getenv("SNOWFLAKE_METADATA", "false").lower() == "true":
            logger.info(
                f"UDF not created in {self.schema} {e}.  This is expected in Local mode when running against Snowflake-based metadata."
            )

    # BOT_FILES_STAGE
    # ---------------------
    # TODO: refactor to use _create_table_if_not_exist
    bot_files_stage_check_query = f"SHOW STAGES LIKE 'BOT_FILES_STAGE' IN SCHEMA {self.genbot_internal_project_and_schema};"
    try:
        cursor = self.client.cursor()
        cursor.execute(bot_files_stage_check_query)
        if not cursor.fetchone():
            bot_files_stage_ddl = f"""
            CREATE OR REPLACE STAGE {self.genbot_internal_project_and_schema}.BOT_FILES_STAGE
            ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');
            """
            cursor.execute(bot_files_stage_ddl)
            self.client.commit()
            logger.info(
                f"Stage {self.genbot_internal_project_and_schema}.BOT_FILES_STAGE created."
            )
        else:
            logger.info(
                f"Stage {self.genbot_internal_project_and_schema}.BOT_FILES_STAGE already exists."
            )
    except Exception as e:
        logger.error(f"An error occurred while checking or creating stage BOT_FILES_STAGE: {e}")
    finally:
        if cursor is not None:
            cursor.close()

    # LLM_TOKENS
    # ---------------------
    # TODO: refactor to use _create_table_if_not_exist
    try:
        runner_id = os.getenv("RUNNER_ID", "jl-local-runner")
        cursor = self.client.cursor()
        if not _check_table_exists('LLM_TOKENS'):
            llm_config_table_ddl = f"""
            CREATE OR REPLACE TABLE {self.genbot_internal_project_and_schema}.LLM_TOKENS (
                RUNNER_ID VARCHAR(16777216),
                LLM_KEY VARCHAR(16777216),
                LLM_TYPE VARCHAR(16777216),
                ACTIVE BOOLEAN,
                LLM_ENDPOINT VARCHAR(16777216),
                MODEL_NAME VARCHAR(16777216),
                EMBEDDING_MODEL_NAME VARCHAR(16777216)
            );
            """
            cursor.execute(llm_config_table_ddl)
            self.client.commit()
            logger.info(f"Table {self.genbot_internal_project_and_schema}.LLM_TOKENS created.")

            # Insert a row with the current runner_id and cortex as the active LLM key and type

            insert_initial_row_query = f"""
                INSERT INTO {self.genbot_internal_project_and_schema}.LLM_TOKENS
                SELECT
                    %s AS RUNNER_ID,
                    %s AS LLM_KEY,
                    %s AS LLM_TYPE,
                    %s AS ACTIVE,
                    NULL AS LLM_ENDPOINT,
                    NULL AS MODEL_NAME,
                    NULL AS EMBEDDING_MODEL_NAME;
            """

            # if a new install, set cortex to default LLM if available
            test_cortex_available = self.check_cortex_available()
            if test_cortex_available == True:
                cursor.execute(insert_initial_row_query, (runner_id,'cortex_no_key_needed', 'cortex', True))
            else:
                cursor.execute(insert_initial_row_query, (runner_id,None,None,False))
            self.client.commit()
            logger.info(f"Inserted initial row into {self.genbot_internal_project_and_schema}.LLM_TOKENS with runner_id: {runner_id}")
        else:
            logger.info(f"Table {self.genbot_internal_project_and_schema}.LLM_TOKENS already exists.")
            check_query = f"DESCRIBE TABLE {self.genbot_internal_project_and_schema}.LLM_TOKENS;"
            try:
                cursor.execute(check_query)
                columns = [col[0] for col in cursor.fetchall()]

                if "ACTIVE" not in columns:
                    cortex_active = False
                    alter_table_query = f"ALTER TABLE {self.genbot_internal_project_and_schema}.LLM_TOKENS ADD COLUMN ACTIVE BOOLEAN;"
                    cursor.execute(alter_table_query)
                    self.client.commit()
                    logger.info(
                        f"Column 'ACTIVE' added to table {self.genbot_internal_project_and_schema}.LLM_TOKENS."
                    )
                    update_query = f"UPDATE {self.genbot_internal_project_and_schema}.LLM_TOKENS SET ACTIVE=TRUE WHERE lower(LLM_TYPE)='openai'"
                    cursor.execute(update_query)
                    self.client.commit()

                if "MODEL_NAME" not in columns:
                    alter_table_query = f"ALTER TABLE {self.genbot_internal_project_and_schema}.LLM_TOKENS ADD COLUMN MODEL_NAME VARCHAR(16777216), EMBEDDING_MODEL_NAME VARCHAR(16777216);"
                    cursor.execute(alter_table_query)
                    self.client.commit()
                    logger.info(
                        f"Columns 'MODEL_NAME' and 'EMBEDDING_MODEL_NAME' added to table {self.genbot_internal_project_and_schema}.LLM_TOKENS."
                    )

                # update case to lower for llm_type. Can remove after release_202410b.
                update_case_query = f"""UPDATE {self.genbot_internal_project_and_schema}.LLM_TOKENS SET LLM_TYPE = LOWER(LLM_TYPE)"""
                cursor.execute(update_case_query)
                self.client.commit()

                select_active_llm_query = f"""SELECT LLM_TYPE FROM {self.genbot_internal_project_and_schema}.LLM_TOKENS WHERE ACTIVE = TRUE;"""
                cursor.execute(select_active_llm_query)
                active_llm = cursor.fetchone()

                if active_llm is None:
                    test_cortex_available = self.check_cortex_available()
                    if test_cortex_available:
                        active_llm = 'cortex'
                if active_llm == 'cortex':
                    cortex_active = True
                else:
                    cortex_active = False

                merge_cortex_row_query = f"""
                    MERGE INTO {self.genbot_internal_project_and_schema}.LLM_TOKENS AS target
                    USING (SELECT %s AS RUNNER_ID, %s AS LLM_KEY, %s AS LLM_TYPE, %s AS ACTIVE) AS source
                    ON target.LLM_TYPE = source.LLM_TYPE
                    WHEN MATCHED THEN
                        UPDATE SET
                            RUNNER_ID = source.RUNNER_ID,
                            LLM_KEY = source.LLM_KEY,
                            ACTIVE = source.ACTIVE
                    WHEN NOT MATCHED THEN
                        INSERT (RUNNER_ID, LLM_KEY, LLM_TYPE, ACTIVE, LLM_ENDPOINT)
                        VALUES (source.RUNNER_ID, source.LLM_KEY, source.LLM_TYPE, source.ACTIVE, null);
                """

                # if a new install, set cortex to default LLM if available
                test_cortex_available = self.check_cortex_available()
                if test_cortex_available == True:
                    cursor.execute(merge_cortex_row_query, (runner_id,'cortex_no_key_needed', 'cortex', cortex_active,))
                    # else:
                    #     cursor.execute(insert_initial_row_query, (runner_id,None,None,False,))
                    self.client.commit()
                    logger.info(f"Merged cortex row into {self.genbot_internal_project_and_schema}.LLM_TOKENS with runner_id: {runner_id}")

            except Exception as e:
                logger.error(
                    f"An error occurred while checking or altering table {self.genbot_internal_project_and_schema}.LLM_TOKENS to add ACTIVE column: {e}"
                )
            #               logger.info(f"Table {self.schema}.LLM_TOKENS already exists.")
    except Exception as e:
        logger.error(f"An error occurred while checking or creating table LLM_TOKENS: {e}")
    finally:
        if cursor is not None:
            cursor.close()

    # LLM_TOKENS
    # ---------------------
    # TODO: refactor to use _create_table_if_not_exist
    # Check if LLM_ENDPOINT column exists in LLM_TOKENS table
    check_llm_endpoint_query = f"DESCRIBE TABLE {self.genbot_internal_project_and_schema}.LLM_TOKENS;"
    try:
        cursor = self.client.cursor()
        cursor.execute(check_llm_endpoint_query)
        columns = [col[0] for col in cursor.fetchall()]

        if "LLM_ENDPOINT" not in columns:
            # Add LLM_ENDPOINT column if it doesn't exist
            alter_table_query = f"ALTER TABLE {self.genbot_internal_project_and_schema}.LLM_TOKENS ADD COLUMN LLM_ENDPOINT VARCHAR(16777216);"
            cursor.execute(alter_table_query)
            self.client.commit()
            logger.info(
                f"Column 'LLM_ENDPOINT' added to table {self.genbot_internal_project_and_schema}.LLM_TOKENS."
            )
    except Exception as e:
        logger.error(
            f"An error occurred while checking or altering table {self.genbot_internal_project_and_schema}.LLM_TOKENS to add LLM_ENDPOINT column: {e}"
        )
    finally:
        if cursor is not None:
            cursor.close()

    # SLACK_APP_CONFIG_TOKENS,
    # & self.slack_tokens_table_name
    # & EAI_CONFIG
    # & CUSTOM_ENDPOINTS
    # -------------------------
    # TODO: refactor to use _create_table_if_not_exist

    slack_tokens_table_check_query = (
        f"SHOW TABLES LIKE 'SLACK_APP_CONFIG_TOKENS' IN SCHEMA {self.schema};"
    )
    try:
        cursor = self.client.cursor()
        cursor.execute(slack_tokens_table_check_query)
        if not cursor.fetchone():
            slack_tokens_table_ddl = f"""
            CREATE OR REPLACE TABLE {self.slack_tokens_table_name} (
                RUNNER_ID VARCHAR(16777216),
                SLACK_APP_CONFIG_TOKEN VARCHAR(16777216),
                SLACK_APP_CONFIG_REFRESH_TOKEN VARCHAR(16777216)
            );
            """
            cursor.execute(slack_tokens_table_ddl)
            self.client.commit()
            logger.info(f"Table {self.slack_tokens_table_name} created.")

            # Insert a row with the current runner_id and NULL values for the tokens
            runner_id = os.getenv("RUNNER_ID", "jl-local-runner")
            insert_initial_row_query = f"""
            INSERT INTO {self.slack_tokens_table_name} (RUNNER_ID, SLACK_APP_CONFIG_TOKEN, SLACK_APP_CONFIG_REFRESH_TOKEN)
            VALUES (%s, NULL, NULL);
            """
            cursor.execute(insert_initial_row_query, (runner_id,))
            self.client.commit()
            logger.info(
                f"Inserted initial row into {self.slack_tokens_table_name} with runner_id: {runner_id}"
            )
        else:
            logger.info(
                f"Table {self.slack_tokens_table_name} already exists."
            )  # SLACK_APP_CONFIG_TOKENS
    except Exception as e:
        logger.error(
            f"An error occurred while checking or creating table {self.slack_tokens_table_name}: {e}"
        )
    finally:
        if cursor is not None:
            cursor.close()

        try:
            # eai_config_table_check_query = f"SHOW TABLES LIKE EAI_CONFIG in schema {self.schema};"
            cursor = self.client.cursor()
            # cursor.execute(eai_config_table_check_query)
            if not _check_table_exists('EAI_CONFIG'):
                eai_config_table_ddl = f"""
                CREATE OR REPLACE TABLE {self.genbot_internal_project_and_schema}.EAI_CONFIG (
                    EAI_TYPE VARCHAR(16777216),
                    EAI_NAME VARCHAR(16777216)
                );
                """
                cursor.execute(eai_config_table_ddl)
                self.client.commit()
                logger.info(f"Table EAI_CONFIG created.")

            else:
                logger.info(
                    f"Table EAI_CONFIG already exists."
                )

            if self.source_name == 'Snowflake':

                # ensure eai_config table matches EAI assigned to services
                get_eai_from_services_query = f" SHOW SERVICES IN APPLICATION {self.project_id}"
                cursor.execute(get_eai_from_services_query)
                # self.client.commit()
                get_eai_from_services_query = f""" select DISTINCT REPLACE(rtrim(ltrim("external_access_integrations",'['),']'),'"','') EAI_LIST from TABLE(RESULT_SCAN(LAST_QUERY_ID()));"""
                cursor.execute(get_eai_from_services_query)
                self.client.commit()
                eai_list = cursor.fetchone()

                values_clause = " UNION ALL ".join([f"""SELECT $${eai}$$ AS EAI_NAME, CHARINDEX('AZURE_OPENAI',$${eai}$$),
                                    iff(charindex('CONSUMER',$${eai}$$)>0,'CONSUMER',
                                        IFF(CHARINDEX('AZURE_OPENAI',$${eai}$$)>0,'AZURE_OPENAI',
                                            IFF(CHARINDEX('SLACK',$${eai}$$)>0,'SLACK',
                                                IFF(CHARINDEX('OPENAI',$${eai}$$)>0,'OPENAI','CUSTOM'))))  AS EAI_TYPE""" for eai in eai_list if eai is not None])

                # Create the full merge statement
                if values_clause:
                    merge_statement = dedent(f"""
                    MERGE INTO {self.genbot_internal_project_and_schema}.EAI_CONFIG AS tgt
                    USING (
                    {values_clause}
                    ) AS src
                    ON tgt.EAI_TYPE = src.eai_type
                    WHEN NOT MATCHED THEN
                    INSERT (eai_type, eai_name)
                    VALUES (src.eai_type, src.eai_name);
                    """)
                    # logger.info(f"######DEBUG###### {merge_statement}")
                    cursor.execute(merge_statement)
                    self.client.commit()
                    logger.info(
                        f"Updated EAI_CONFIG table from services"
                    )
        except Exception as e:
            if not hasattr(e, 'msg') or "Object found is of type 'DATABASE', not specified type 'APPLICATION'" not in e.msg:
                logger.error(
                    f"An error occurred while checking or creating table EAI_CONFIG: {e}"
                )
        finally:
            if cursor is not None:
                cursor.close()

        try:
            cursor = self.client.cursor()
            if not _check_table_exists('CUSTOM_ENDPOINTS'):
                endpoints_table_ddl = dedent(f"""
                CREATE OR REPLACE TABLE {self.genbot_internal_project_and_schema}.CUSTOM_ENDPOINTS (
                    GROUP_NAME VARCHAR(16777216),
                    ENDPOINT VARCHAR(16777216),
                    TYPE VARCHAR(16777216)
                );
                """)
                cursor.execute(endpoints_table_ddl)
                self.client.commit()
                logger.info(f"Table CUSTOM_ENDPOINTS created.")

            else:
                logger.info(
                    f"Table CUSTOM_ENDPOINTS already exists."
                )

                check_endpoint_columns = f"DESCRIBE TABLE {self.genbot_internal_project_and_schema}.CUSTOM_ENDPOINTS;"
                try:
                    cursor = self.client.cursor()
                    cursor.execute(check_endpoint_columns)
                    columns = [col[0] for col in cursor.fetchall()]

                    if "GROUP_NAME" not in columns:
                        # Add LLM_ENDPOINT column if it doesn't exist
                        alter_table_query = f"ALTER TABLE {self.genbot_internal_project_and_schema}.CUSTOM_ENDPOINTS ADD COLUMN GROUP_NAME VARCHAR(16777216);"
                        cursor.execute(alter_table_query)
                        self.client.commit()
                        logger.info(
                            f"Column 'GROUP_NAME' added to table {self.genbot_internal_project_and_schema}.CUSTOM_ENDPOINTS."
                        )
                except Exception as e:
                    logger.error(
                        f"An error occurred while checking or altering table {self.genbot_internal_project_and_schema}.CUSTOM_ENDPOINTS to add GROUP_NAME column: {e}"
                    )
                finally:
                    if cursor is not None:
                        cursor.close()

        except Exception as e:
            logger.error(
                f"An error occurred while checking or creating table CUSTOM_ENDPOINTS: {e}"
            )
        finally:
            if cursor is not None:
                cursor.close()

    # =====================================================================
    # NOTE: If using SQLite adapter, skip this section as SQLiteAdapter has
    # its own version of table creation and Eve bot initialization in
    # connectors/sqlite_adapter.py _ensure_bot_servicing_table()
    # =====================================================================

    # BOT_SERVICING
    # --------------------------
    try:
        cursor = self.client.cursor()
        if not _check_table_exists('BOT_SERVICING'):
            bot_servicing_table_ddl = f"""
            CREATE OR REPLACE TABLE {self.bot_servicing_table_name} (
                API_APP_ID VARCHAR(16777216),
                BOT_SLACK_USER_ID VARCHAR(16777216),
                BOT_ID VARCHAR(16777216),
                BOT_NAME VARCHAR(16777216),
                BOT_INSTRUCTIONS VARCHAR(16777216),
                AVAILABLE_TOOLS VARCHAR(16777216),
                RUNNER_ID VARCHAR(16777216),
                SLACK_APP_TOKEN VARCHAR(16777216),
                SLACK_APP_LEVEL_KEY VARCHAR(16777216),
                SLACK_SIGNING_SECRET VARCHAR(16777216),
                SLACK_CHANNEL_ID VARCHAR(16777216),
                TEAMS_APP_ID VARCHAR(16777216),
                TEAMS_APP_PASSWORD VARCHAR(16777216),
                TEAMS_APP_TYPE VARCHAR(16777216),
                TEAMS_APP_TENANT_ID VARCHAR(16777216),
                AUTH_URL VARCHAR(16777216),
                AUTH_STATE VARCHAR(16777216),
                CLIENT_ID VARCHAR(16777216),
                CLIENT_SECRET VARCHAR(16777216),
                UDF_ACTIVE VARCHAR(16777216),
                SLACK_ACTIVE VARCHAR(16777216),
                TEAMS_ACTIVE VARCHAR(16777216),
                FILES VARCHAR(16777216),
                BOT_IMPLEMENTATION VARCHAR(16777216),
                BOT_INTRO_PROMPT VARCHAR(16777216),
                BOT_AVATAR_IMAGE VARCHAR(16777216),
                SLACK_USER_ALLOW  ARRAY,
                DATABASE_CREDENTIALS VARIANT
            );
            """
            cursor.execute(bot_servicing_table_ddl)
            self.client.commit()
            logger.info(f"Table {self.bot_servicing_table_name} created.")

            # Insert a row with specified values and NULL for the rest
            runner_id = os.getenv("RUNNER_ID", "jl-local-runner")
            bot_id = "Eve"
            #                bot_id += "".join(
            #                    random.choices(string.ascii_letters + string.digits, k=6)
            #                )
            bot_name = "Eve"
            bot_instructions = BASE_EVE_BOT_INSTRUCTIONS
            available_tools = BASE_EVE_BOT_AVAILABLE_TOOLS_SNOWFLAKE
            udf_active = "Y"
            slack_active = "N"
            bot_intro_prompt = EVE_INTRO_PROMPT

            insert_initial_row_query = f"""
            MERGE INTO {self.bot_servicing_table_name} AS target
            USING (SELECT %s AS BOT_ID, %s AS RUNNER_ID, %s AS BOT_NAME, %s AS BOT_INSTRUCTIONS,
                            %s AS AVAILABLE_TOOLS, %s AS UDF_ACTIVE, %s AS SLACK_ACTIVE, %s AS BOT_INTRO_PROMPT) AS source
            ON target.BOT_ID = source.BOT_ID
            WHEN MATCHED THEN
                UPDATE SET
                    RUNNER_ID = source.RUNNER_ID,
                    BOT_NAME = source.BOT_NAME,
                    BOT_INSTRUCTIONS = source.BOT_INSTRUCTIONS,
                    AVAILABLE_TOOLS = source.AVAILABLE_TOOLS,
                    UDF_ACTIVE = source.UDF_ACTIVE,
                    SLACK_ACTIVE = source.SLACK_ACTIVE,
                    BOT_INTRO_PROMPT = source.BOT_INTRO_PROMPT
            WHEN NOT MATCHED THEN
                INSERT (BOT_ID, RUNNER_ID, BOT_NAME, BOT_INSTRUCTIONS, AVAILABLE_TOOLS, UDF_ACTIVE, SLACK_ACTIVE, BOT_INTRO_PROMPT)
                VALUES (source.BOT_ID, source.RUNNER_ID, source.BOT_NAME, source.BOT_INSTRUCTIONS,
                        source.AVAILABLE_TOOLS, source.UDF_ACTIVE, source.SLACK_ACTIVE, source.BOT_INTRO_PROMPT);
            """
            cursor.execute(
                insert_initial_row_query,
                (
                    bot_id,
                    runner_id,
                    bot_name,
                    bot_instructions,
                    available_tools,
                    udf_active,
                    slack_active,
                    bot_intro_prompt,
                ),
            )
            self.client.commit()
            logger.info(
                f"Inserted initial Eve row into {self.bot_servicing_table_name} with runner_id: {runner_id}"
            )

        else:
            # Check if the 'ddl_short' column exists in the metadata table

            # update_query = f"""
            # UPDATE {self.bot_servicing_table_name}
            # SET AVAILABLE_TOOLS = REPLACE(REPLACE(AVAILABLE_TOOLS, 'vision_chat_analysis', 'image_tools'),)
            # WHERE AVAILABLE_TOOLS LIKE '%vision_chat_analysis%'
            # """
            # cursor.execute(update_query)
            # self.client.commit()
            # logger.info(
            #     f"Updated 'vision_chat_analysis' to 'image_analysis' in AVAILABLE_TOOLS where applicable in {self.bot_servicing_table_name}."
            # )

            check_query = f"DESCRIBE TABLE {self.bot_servicing_table_name};"
            try:
                cursor.execute(check_query)
                columns = [col[0] for col in cursor.fetchall()]
                if "SLACK_APP_LEVEL_KEY" not in columns:
                    alter_table_query = f"ALTER TABLE {self.bot_servicing_table_name} ADD COLUMN SLACK_APP_LEVEL_KEY STRING;"
                    cursor.execute(alter_table_query)
                    self.client.commit()
                    logger.info(
                        f"Column 'SLACK_APP_LEVEL_KEY' added to table {self.bot_servicing_table_name}."
                    )
                if "BOT_IMPLEMENTATION" not in columns:
                    alter_table_query = f"ALTER TABLE {self.bot_servicing_table_name} ADD COLUMN BOT_IMPLEMENTATION STRING;"
                    cursor.execute(alter_table_query)
                    self.client.commit()
                    logger.info(
                        f"Column 'BOT_IMPLEMENTATION' added to table {self.bot_servicing_table_name}."
                    )
                if "BOT_INTRO" in columns:
                    alter_table_query = f"ALTER TABLE {self.bot_servicing_table_name} DROP COLUMN BOT_INTRO;"
                    cursor.execute(alter_table_query)
                    self.client.commit()
                    logger.info(
                        f"Column 'BOT_INTRO' dropped from table {self.bot_servicing_table_name}."
                    )
                if "BOT_INTRO_PROMPT" not in columns:
                    alter_table_query = f"ALTER TABLE {self.bot_servicing_table_name} ADD COLUMN BOT_INTRO_PROMPT STRING;"
                    cursor.execute(alter_table_query)
                    self.client.commit()
                    logger.info(
                        f"Column 'BOT_INTRO_PROMPT' added to table {self.bot_servicing_table_name}."
                    )
                    insert_initial_intros_query = f"""UPDATE {self.bot_servicing_table_name} b SET BOT_INTRO_PROMPT = a.BOT_INTRO_PROMPT
                    FROM (
                        SELECT BOT_NAME, BOT_INTRO_PROMPT
                        FROM (
                            SELECT 'EVE' BOT_NAME, $${EVE_INTRO_PROMPT}$$ BOT_INTRO_PROMPT
                            UNION
                            SELECT 'ELIZA' BOT_NAME, $${ELIZA_INTRO_PROMPT}$$ BOT_INTRO_PROMPT
                            UNION
                            SELECT 'JANICE' BOT_NAME, $${JANICE_INTRO_PROMPT}$$ BOT_INTRO_PROMPT
                            UNION
                            SELECT 'STUART' BOT_NAME, $${STUART_INTRO_PROMPT}$$ BOT_INTRO_PROMPT
                        ) ) a
                    WHERE upper(a.BOT_NAME) = upper(b.BOT_NAME)"""
                    cursor.execute(insert_initial_intros_query)
                    self.client.commit()
                    logger.info(
                        f"Initial 'BOT_INTRO_PROMPT' data inserted into table {self.bot_servicing_table_name}."
                    )
                if "BOT_AVATAR_IMAGE" not in columns:
                    alter_table_query = f"ALTER TABLE {self.bot_servicing_table_name} ADD COLUMN BOT_AVATAR_IMAGE VARCHAR(16777216);"
                    cursor.execute(alter_table_query)
                    self.client.commit()
                    logger.info(
                        f"Column 'BOT_AVATAR_IMAGE' added to table {self.bot_servicing_table_name}."
                    )
                if "SLACK_USER_ALLOW" not in columns:
                    alter_table_query = f"ALTER TABLE {self.bot_servicing_table_name} ADD COLUMN SLACK_USER_ALLOW ARRAY;"
                    cursor.execute(alter_table_query)
                    self.client.commit()
                    logger.info(
                        f"Column 'SLACK_USER_ALLOW' added to table {self.bot_servicing_table_name}."
                    )
                if "DATABASE_CREDENTIALS" not in columns:
                    alter_table_query = f"ALTER TABLE {self.bot_servicing_table_name} ADD COLUMN DATABASE_CREDENTIALS VARIANT;"
                    cursor.execute(alter_table_query)
                    self.client.commit()
                    logger.info(
                        f"Column 'DATABASE_CREDENTIALS' added to table {self.bot_servicing_table_name}."
                    )

            except Exception as e:
                logger.error(
                    f"An error occurred while checking or altering table {self.bot_servicing_table_name} to add BOT_IMPLEMENTATION column: {e}"
                )
            # except Exception as e:
            #     logger.info(
            #         f"An error occurred while checking or altering table {metadata_table_id}: {e}"
            #     )
            logger.info(f"Table {self.bot_servicing_table_name} already exists.")
        # update bot servicing table bot avatars from shared images table
        insert_images_query = f"""UPDATE {self.bot_servicing_table_name} b SET BOT_AVATAR_IMAGE = a.ENCODED_IMAGE_DATA
        FROM (
                SELECT P.ENCODED_IMAGE_DATA, P.BOT_NAME
                FROM {self.images_table_name} P
                WHERE UPPER(P.BOT_NAME) = 'DEFAULT'
            ) a """
        cursor.execute(insert_images_query)
        self.client.commit()
        logger.info(
            f"Initial 'BOT_AVATAR_IMAGE' data inserted into table {self.bot_servicing_table_name}."
        )
    except Exception as e:
        logger.error(
            f"An error occurred while checking or creating table {self.bot_servicing_table_name}: {e}"
        )
    finally:
        if cursor is not None:
            cursor.close()

    # check if Janice exists in BOT_SERVCING table

    if self.source_name == 'Snowflake':
        cursor = self.client.cursor()
        check_janice_query = f"SELECT * FROM {self.bot_servicing_table_name} WHERE BOT_ID = 'Janice';"
        cursor.execute(check_janice_query)
        result = cursor.fetchone()

        # If not, run this query to insert Janice
        if result is None:
            runner_id = os.getenv("RUNNER_ID", "jl-local-runner")
            bot_id = "Janice"
            #                bot_id += "".join(
            #                    random.choices(string.ascii_letters + string.digits, k=6)
            #                )
            bot_name = "Janice"
            bot_instructions = JANICE_JANITOR_INSTRUCTIONS
            available_tools = '["slack_tools", "database_tools", "snowflake_tools", "image_tools", "process_manager_tools", "process_runner_tools", "process_scheduler_tools", "notebook_manager_tools", "artifact_manager_tools"]'
            udf_active = "Y"
            slack_active = "N"
            bot_intro_prompt = JANICE_INTRO_PROMPT

            insert_initial_row_query = f"""
            MERGE INTO {self.bot_servicing_table_name} AS target
            USING (SELECT %s AS BOT_ID, %s AS RUNNER_ID, %s AS BOT_NAME, %s AS BOT_INSTRUCTIONS,
                            %s AS AVAILABLE_TOOLS, %s AS UDF_ACTIVE, %s AS SLACK_ACTIVE, %s AS BOT_INTRO_PROMPT) AS source
            ON target.BOT_ID = source.BOT_ID
            WHEN MATCHED THEN
                UPDATE SET
                    RUNNER_ID = source.RUNNER_ID,
                    BOT_NAME = source.BOT_NAME,
                    BOT_INSTRUCTIONS = source.BOT_INSTRUCTIONS,
                    AVAILABLE_TOOLS = source.AVAILABLE_TOOLS,
                    UDF_ACTIVE = source.UDF_ACTIVE,
                    SLACK_ACTIVE = source.SLACK_ACTIVE,
                    BOT_INTRO_PROMPT = source.BOT_INTRO_PROMPT
            WHEN NOT MATCHED THEN
                INSERT (BOT_ID, RUNNER_ID, BOT_NAME, BOT_INSTRUCTIONS, AVAILABLE_TOOLS, UDF_ACTIVE, SLACK_ACTIVE, BOT_INTRO_PROMPT)
                VALUES (source.BOT_ID, source.RUNNER_ID, source.BOT_NAME, source.BOT_INSTRUCTIONS,
                        source.AVAILABLE_TOOLS, source.UDF_ACTIVE, source.SLACK_ACTIVE, source.BOT_INTRO_PROMPT);
            """
            cursor.execute(
                insert_initial_row_query,
                (
                    bot_id,
                    runner_id,
                    bot_name,
                    bot_instructions,
                    available_tools,
                    udf_active,
                    slack_active,
                    bot_intro_prompt,
                ),
            )
            self.client.commit()
            logger.info(f"Inserted initial Janice row into {self.bot_servicing_table_name} with runner_id: {runner_id}"
            )

    # NGROK_TOKENS
    # -------------------
    try:
        cursor = self.client.cursor()
        if not _check_table_exists('NGROK_TOKENS'):
            ngrok_tokens_table_ddl = f"""
            CREATE OR REPLACE TABLE {self.ngrok_tokens_table_name} (
                RUNNER_ID VARCHAR(16777216),
                NGROK_AUTH_TOKEN VARCHAR(16777216),
                NGROK_USE_DOMAIN VARCHAR(16777216),
                NGROK_DOMAIN VARCHAR(16777216)
            );
            """
            cursor.execute(ngrok_tokens_table_ddl)
            self.client.commit()
            logger.info(f"Table {self.ngrok_tokens_table_name} created.")

            # Insert a row with the current runner_id and NULL values for the tokens and domain, 'N' for use_domain
            runner_id = os.getenv("RUNNER_ID", "jl-local-runner")
            insert_initial_row_query = f"""
            INSERT INTO {self.ngrok_tokens_table_name} (RUNNER_ID, NGROK_AUTH_TOKEN, NGROK_USE_DOMAIN, NGROK_DOMAIN)
            VALUES (%s, NULL, 'N', NULL);
            """
            cursor.execute(insert_initial_row_query, (runner_id,))
            self.client.commit()
            logger.info(
                f"Inserted initial row into {self.ngrok_tokens_table_name} with runner_id: {runner_id}"
            )
        else:
            logger.info(f"Table {self.ngrok_tokens_table_name} already exists.")
    except Exception as e:
        logger.error(
            f"An error occurred while checking or creating table {self.ngrok_tokens_table_name}: {e}"
        )
    finally:
        if cursor is not None:
            cursor.close()

    # # AVAILABLE_TOOLS
    # # -----------------
    # try:
    #     cursor = self.client.cursor()
    #     if os.getenv('TASK_TEST_MODE', 'False').lower() != 'true': # skip if TEST_MODE
    #         available_tools_table_ddl = f"""
    #         CREATE OR REPLACE TABLE {self.available_tools_table_name} (
    #             TOOL_NAME VARCHAR(16777216),
    #             TOOL_DESCRIPTION VARCHAR(16777216)
    #         );
    #         """
    #         cursor.execute(available_tools_table_ddl)
    #         self.client.commit()
    #         logger.info(
    #             f"Table {self.available_tools_table_name} (re)created, this is expected on every run."
    #         )

    #         tools_data = core.bot_os_tool_descriptions.get_persistent_tools_descriptions()

    #         insert_tools_query = f"""
    #         INSERT INTO {self.available_tools_table_name} (TOOL_NAME, TOOL_DESCRIPTION)
    #         VALUES (%s, %s);
    #         """
    #         for tool_name, tool_description in tools_data:
    #             cursor.execute(insert_tools_query, (tool_name, tool_description))
    #             logger.info(f"Inserting {tool_name} ({tool_description}) to available tools ")
    #         self.client.commit()
    #         logger.info(f"Inserted initial rows into {self.available_tools_table_name}")
    #     else:
    #         logger.info(f"Table {self.available_tools_table_name} already exists.")
    # except Exception as e:
    #     logger.error(
    #         f"An error occurred while checking or creating table {self.available_tools_table_name}: {e}"
    #     )
    # finally:
    #     if cursor is not None:
    #         cursor.close()

    # # Check if the 'snowflake_semantic_tools' row exists in the available_tables and insert if not present
    # check_snowflake_semantic_tools_query = f"SELECT COUNT(*) FROM {self.available_tools_table_name} WHERE TOOL_NAME = 'snowflake_semantic_tools';"
    # try:
    #     cursor = self.client.cursor()
    #     cursor.execute(check_snowflake_semantic_tools_query)
    #     if cursor.fetchone()[0] == 0:
    #         insert_snowflake_semantic_tools_query = f"""
    #         INSERT INTO {self.available_tools_table_name} (TOOL_NAME, TOOL_DESCRIPTION)
    #         VALUES ('snowflake_semantic_tools', 'Create and modify Snowflake Semantic Models');
    #         """
    #         cursor.execute(insert_snowflake_semantic_tools_query)
    #         self.client.commit()
    #         logger.info("Inserted 'snowflake_semantic_tools' into available_tools table.")
    # except Exception as e:
    #     logger.info(
    #         f"An error occurred while inserting 'snowflake_semantic_tools' into available_tools table: {e}"
    #     )
    # finally:
    #     if cursor is not None:
    #         cursor.close()

    # MESSAGE_LOG (CHAT HISTORY)
    # -----------------------------
    chat_history_table_id = self.message_log_table_name

    # Check if the chat history table exists
    try:
        cursor = self.client.cursor()
        if not _check_table_exists('MESSAGE_LOG'):
            chat_history_table_ddl = f"""
            CREATE TABLE {self.message_log_table_name} (
                timestamp TIMESTAMP NOT NULL,
                bot_id STRING NOT NULL,
                bot_name STRING NOT NULL,
                thread_id STRING,
                message_type STRING NOT NULL,
                message_payload STRING,
                message_metadata STRING,
                tokens_in INTEGER,
                tokens_out INTEGER,
                files STRING,
                channel_type STRING,
                channel_name STRING,
                primary_user STRING,
                task_id STRING
            );
            """
            cursor.execute(chat_history_table_ddl)
            self.client.commit()
            logger.info(f"Table {self.message_log_table_name} created.")
        else:
            check_query = f"DESCRIBE TABLE {chat_history_table_id};"
            try:
                cursor.execute(check_query)
                columns = [col[0] for col in cursor.fetchall()]
                for col in [
                    "FILES",
                    "CHANNEL_TYPE",
                    "CHANNEL_NAME",
                    "PRIMARY_USER",
                    "TASK_ID",
                ]:
                    if col not in columns:
                        alter_table_query = f"ALTER TABLE {chat_history_table_id} ADD COLUMN {col} STRING;"
                        cursor.execute(alter_table_query)
                        self.client.commit()
                        logger.info(
                            f"Column '{col}' added to table {chat_history_table_id}."
                        )
            except Exception as e:
                logger.error("Error adding column FILES to MESSAGE_LOG: ", e)
            logger.info(f"Table {self.message_log_table_name} already exists.")
    except Exception as e:
        logger.error(
            f"An error occurred while checking or creating table {self.message_log_table_name}: {e}"
        )

    # KNOWLEDGE
    # --------------------
    knowledge_table_check_query = (
        f"SHOW TABLES LIKE 'KNOWLEDGE' IN SCHEMA {self.schema};"
    )
    # Check if the chat knowledge table exists
    try:
        cursor = self.client.cursor()
        cursor.execute(knowledge_table_check_query)
        if not cursor.fetchone():
            knowledge_table_ddl = f"""
            CREATE TABLE {self.knowledge_table_name} (
                timestamp TIMESTAMP NOT NULL,
                thread_id STRING NOT NULL,
                knowledge_thread_id STRING NOT NULL,
                primary_user STRING,
                bot_id STRING,
                last_timestamp TIMESTAMP NOT NULL,
                thread_summary STRING,
                user_learning STRING,
                tool_learning STRING,
                data_learning STRING
            );
            """
            cursor.execute(knowledge_table_ddl)
            self.client.commit()
            logger.info(f"Table {self.knowledge_table_name} created.")
        else:
            check_query = f"DESCRIBE TABLE {self.knowledge_table_name};"
            logger.info(f"Table {self.knowledge_table_name} already exists.")
    except Exception as e:
        logger.error(
            f"An error occurred while checking or creating table {self.knowledge_table_name}: {e}"
        )

    # NOTEBOOK
    # -------------------
    create_bot_notebook_table_ddl = f"""
    CREATE OR REPLACE TABLE {self.schema}.NOTEBOOK (
        CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UPDATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        BOT_ID VARCHAR(16777216),
        NOTE_ID VARCHAR(16777216),
        NOTE_NAME VARCHAR(16777216),
        NOTE_TYPE VARCHAR(16777216),
        NOTE_CONTENT VARCHAR(16777216),
        NOTE_PARAMS VARCHAR(16777216)
    );
    """

    created = _create_table_if_not_exist('NOTEBOOK', create_bot_notebook_table_ddl)
    if not created:
        upgrade_timestamp_columns(self, 'NOTEBOOK')

    with self.client.cursor() as cursor:
        load_default_notes(self, cursor)

    # NOTEBOOK_HISTORY
    # ----------------------
    notebook_history_table_ddl = f"""
    CREATE OR REPLACE TABLE {self.schema}.NOTEBOOK_HISTORY (
        timestamp TIMESTAMP NOT NULL,
        note_id STRING,
        work_done_summary STRING,
        note_status STRING,
        updated_note_learnings STRING,
        report_message STRING,
        done_flag BOOLEAN,
        needs_help_flag BOOLEAN,
        note_clarity_comments STRING
    );
    """
    _create_table_if_not_exist('NOTEBOOK_HISTORY', notebook_history_table_ddl)

    # PROCESSES TABLE
    processes_table_check_query = (
        f"SHOW TABLES LIKE 'PROCESSES' IN SCHEMA {self.schema};"
    )

    try:
        cursor = self.client.cursor()
        cursor.execute(processes_table_check_query)
        if not cursor.fetchone():
            create_process_table_ddl = f"""
            CREATE TABLE {self.schema}.PROCESSES (
                CREATED_AT TIMESTAMP_NTZ(9) NOT NULL,
                UPDATED_AT TIMESTAMP_NTZ(9) NOT NULL,
                PROCESS_ID VARCHAR(16777216) NOT NULL PRIMARY KEY,
                BOT_ID VARCHAR(16777216),
                PROCESS_NAME VARCHAR(16777216) NOT NULL,
                PROCESS_INSTRUCTIONS VARCHAR(16777216),
                PROCESS_DESCRIPTION VARCHAR(16777216),
                NOTE_ID VARCHAR(16777216),
                PROCESS_CONFIG VARCHAR(16777216),
                HIDDEN BOOLEAN
            );
            """
            cursor.execute(create_process_table_ddl)
            self.client.commit()
            logger.info(f"Table {self.schema}.PROCESSES created successfully.")
        else:
            logger.info(f"Table {self.schema}.PROCESSES exists.")
            upgrade_timestamp_columns(self, 'PROCESSES')

    except Exception as e:
        logger.error(
            f"An error occurred while checking or creating the PROCESSES table: {e}"
        )

    # Check if PROCESS_CONFIG column exists in PROCESSES table
    try:
        cursor = self.client.cursor()
        describe_table_query = f"DESCRIBE TABLE {self.schema}.PROCESSES;"
        cursor.execute(describe_table_query)
        table_description = cursor.fetchall()

        process_config_exists = any(row[0].upper() == 'PROCESS_CONFIG' for row in table_description)
        note_id_exists = any(row[0].upper() == 'NOTE_ID' for row in table_description)
        # process_instructions_exists = any(row[0].upper() == 'PROCESS_INSTRUCTIONS' for row in table_description)
        hidden_exists = any(row[0].upper() == 'HIDDEN' for row in table_description)
        desc_exists = any(row[0].upper() == 'PROCESS_DESCRIPTION' for row in table_description)

        if not process_config_exists or not note_id_exists or not hidden_exists or not desc_exists:
            add_column_query = f"ALTER TABLE {self.schema}.PROCESSES "

            columns_to_add = []
            if not process_config_exists:
                columns_to_add.append(" PROCESS_CONFIG VARCHAR(16777216)")
            if not note_id_exists:
                columns_to_add.append(" NOTE_ID VARCHAR(16777216)")
            if not hidden_exists:
                columns_to_add.append(" HIDDEN BOOLEAN")
            if not desc_exists:
                columns_to_add.append(" PROCESS_DESCRIPTION VARCHAR(16777216)")
            add_column_query += "ADD COLUMN "
            add_column_query += ", ".join(columns_to_add)

            cursor.execute(add_column_query)
            self.client.commit()

            if not process_config_exists:
                logger.info("PROCESS_CONFIG column added to PROCESSES table.")
            if not note_id_exists:
                logger.info("NOTE_ID column added to PROCESSES table.")
            if not hidden_exists:
                logger.info("HIDDEN column added to PROCESSES table.")
            if not desc_exists:
                logger.info("PROCESS_DESCRIPTION column added to PROCESSES table.")
        else:
            logger.info("PROCESS_CONFIG column already exists in PROCESSES table.")
    except Exception as e:
        logger.error(f"An error occurred while checking or adding PROCESS_CONFIG column: {e}")

    load_default_processes_and_notebook(self, cursor)

    # PROCESS_HISTORY
    # ----------------------
    process_history_table_ddl = f"""
    CREATE TABLE {self.process_history_table_name} (
        timestamp TIMESTAMP NOT NULL,
        process_id STRING NOT NULL,
        work_done_summary STRING,
        process_status STRING,
        updated_process_learnings STRING,
        report_message STRING,
        done_flag BOOLEAN,
        needs_help_flag BOOLEAN,
        process_clarity_comments STRING
    );
    """
    _create_table_if_not_exist('PROCESS_HISTORY', process_history_table_ddl)

    # TOOL_KNOWLEDGE
    # ------------------
    user_bot_table_ddl = f"""
    CREATE TABLE IF NOT EXISTS {self.tool_knowledge_table_name} (
        timestamp TIMESTAMP NOT NULL,
        last_timestamp TIMESTAMP NOT NULL,
        bot_id STRING NOT NULL,
        tool STRING NOT NULL,
        summary STRING NOT NULL
    );
    """
    _create_table_if_not_exist('TOOL_KNOWLEDGE', user_bot_table_ddl)

    # PROC_KNOWLEDGE
    # ------------------
    user_bot_table_ddl = f"""
    CREATE TABLE IF NOT EXISTS {self.proc_knowledge_table_name} (
        timestamp TIMESTAMP NOT NULL,
        last_timestamp TIMESTAMP NOT NULL,
        bot_id STRING NOT NULL,
        process STRING NOT NULL,
        summary STRING NOT NULL
    );
    """
    _create_table_if_not_exist('PROC_KNOWLEDGE', user_bot_table_ddl)

    # DATA_KNOWLEDGE
    # ------------------
    user_bot_table_ddl = f"""
    CREATE TABLE IF NOT EXISTS {self.data_knowledge_table_name} (
        timestamp TIMESTAMP NOT NULL,
        last_timestamp TIMESTAMP NOT NULL,
        bot_id STRING NOT NULL,
        dataset STRING NOT NULL,
        summary STRING NOT NULL
    );
    """
    _create_table_if_not_exist('DATA_KNOWLEDGE', user_bot_table_ddl)

    # USER_BOT
    # --------------------
    user_bot_table_ddl = f"""
    CREATE TABLE IF NOT EXISTS {self.user_bot_table_name} (
        timestamp TIMESTAMP NOT NULL,
        primary_user STRING,
        bot_id STRING,
        user_learning STRING,
        tool_learning STRING,
        data_learning STRING
    );
    """
    _create_table_if_not_exist('USER_BOT', user_bot_table_ddl)

    # TEST_MANAGER
    # ------------------
    # Create test_manager table if it doesn't exist
    bot_test_manager_table_check_query = f"SHOW TABLES LIKE 'test_manager' IN SCHEMA {self.schema};"
    cursor = self.client.cursor()
    cursor.execute(bot_test_manager_table_check_query)

    if not cursor.fetchone():
        create_bot_test_manager_table_ddl = f"""
        CREATE OR REPLACE TABLE {self.schema}.TEST_MANAGER (
            CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UPDATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            BOT_ID VARCHAR(16777216),
            TEST_PROCESS_ID VARCHAR(16777216),
            TEST_PROCESS_NAME VARCHAR(16777216),
            TEST_PRIORITY INTEGER
        );
        """
        cursor.execute(create_bot_test_manager_table_ddl)
        self.client.commit()
        logger.info(f"Table {self.schema}.test_manager created successfully.")
    else:
        logger.info(f"Table {self.schema}.test_manager already exists.")
        upgrade_timestamp_columns(self, 'test_manager')

    # HARVEST CONTROL TABLE
    hc_table_id = self.genbot_internal_harvest_control_table
    hc_table_check_query = (
        f"SHOW TABLES LIKE '{hc_table_id.upper()}' IN SCHEMA {self.schema};"
    )

    # HARVEST CONTROL (self.harvest_control_table_name)
    # --------------------------------
    try:
        cursor.execute(hc_table_check_query)
        if not cursor.fetchone():
            hc_table_id = self.harvest_control_table_name
            hc_table_ddl = f"""
            CREATE TABLE {hc_table_id} (
                source_name STRING NOT NULL,
                database_name STRING NOT NULL,
                schema_inclusions ARRAY,
                schema_exclusions ARRAY,
                status STRING NOT NULL,
                refresh_interval INTEGER NOT NULL,
                initial_crawl_complete BOOLEAN NOT NULL
            );
            """
            cursor.execute(hc_table_ddl)
            self.client.commit()
            logger.info(f"Table {hc_table_id} created.")
        else:
            logger.info(f"Table {hc_table_id} already exists.")
    except Exception as e:
        logger.error(
            f"An error occurred while checking or creating table {hc_table_id}: {e}"
        )

    # METADATA TABLE FOR HARVESTER RESULTS
    # ---------------------------------------
    metadata_table_id = self.genbot_internal_harvest_table
    metadata_table_check_query = (
        f"SHOW TABLES LIKE '{metadata_table_id.upper()}' IN SCHEMA {self.schema};"
    )

    # Check if the metadata table exists
    try:
        cursor.execute(metadata_table_check_query)
        if not cursor.fetchone():
            metadata_table_id = self.metadata_table_name
            metadata_table_ddl = f"""
            CREATE TABLE {metadata_table_id} (
                source_name STRING NOT NULL,
                qualified_table_name STRING NOT NULL,
                database_name STRING NOT NULL,
                memory_uuid STRING NOT NULL,
                schema_name STRING NOT NULL,
                table_name STRING NOT NULL,
                complete_description STRING NOT NULL,
                ddl STRING NOT NULL,
                ddl_short STRING,
                ddl_hash STRING NOT NULL,
                summary STRING NOT NULL,
                sample_data_text STRING NOT NULL,
                last_crawled_timestamp TIMESTAMP NOT NULL,
                crawl_status STRING NOT NULL,
                role_used_for_crawl STRING NOT NULL,
                embedding ARRAY,
                embedding_native ARRAY
            );
            """
            cursor.execute(metadata_table_ddl)
            self.client.commit()
            logger.info(f"Table {metadata_table_id} created.")

            try:
                insert_initial_metadata_query = f"""
                INSERT INTO {metadata_table_id} (SOURCE_NAME, QUALIFIED_TABLE_NAME, DATABASE_NAME, MEMORY_UUID, SCHEMA_NAME, TABLE_NAME, COMPLETE_DESCRIPTION, DDL, DDL_SHORT, DDL_HASH, SUMMARY, SAMPLE_DATA_TEXT, LAST_CRAWLED_TIMESTAMP, CRAWL_STATUS, ROLE_USED_FOR_CRAWL)
                SELECT SOURCE_NAME, replace(QUALIFIED_TABLE_NAME,'APP_NAME', CURRENT_DATABASE()) QUALIFIED_TABLE_NAME,  CURRENT_DATABASE() DATABASE_NAME, MEMORY_UUID, SCHEMA_NAME, TABLE_NAME, REPLACE(COMPLETE_DESCRIPTION,'APP_NAME', CURRENT_DATABASE()) COMPLETE_DESCRIPTION, REPLACE(DDL,'APP_NAME', CURRENT_DATABASE()) DDL, REPLACE(DDL_SHORT,'APP_NAME', CURRENT_DATABASE()) DDL_SHORT, 'SHARED_VIEW' DDL_HASH, REPLACE(SUMMARY,'APP_NAME', CURRENT_DATABASE()) SUMMARY, SAMPLE_DATA_TEXT, LAST_CRAWLED_TIMESTAMP, CRAWL_STATUS, ROLE_USED_FOR_CRAWL
                FROM APP_SHARE.HARVEST_RESULTS WHERE SCHEMA_NAME IN ('BASEBALL','FORMULA_1') AND DATABASE_NAME = 'APP_NAME'
                """
                cursor.execute(insert_initial_metadata_query)
                self.client.commit()
                logger.info(f"Inserted initial rows into {metadata_table_id}")
            except Exception as e:
                logger.error(
                    f"Initial rows from APP_SHARE.HARVEST_RESULTS NOT ADDED into {metadata_table_id} due to erorr {e}"
                )

        else:
            # Check if the 'ddl_short' column exists in the metadata table
            metadata_col_check_query = f"DESCRIBE TABLE {self.metadata_table_name};"
            try:
                cursor.execute(metadata_col_check_query)
                columns = [col[0] for col in cursor.fetchall()]
                if "DDL_SHORT" not in columns:
                    alter_table_query = f"ALTER TABLE {self.metadata_table_name} ADD COLUMN ddl_short STRING;"
                    cursor.execute(alter_table_query)
                    self.client.commit()
                    logger.info(f"Column 'ddl_short' added to table {metadata_table_id}.")
            except Exception as e:
                logger.error(
                    f"An error occurred while checking or altering table {metadata_table_id}: {e}"
                )
            # Check if the 'embedding_native' column exists in the metadata table
            try:
                if "EMBEDDING_NATIVE" not in columns:
                    alter_table_query = f"ALTER TABLE {self.metadata_table_name} ADD COLUMN embedding_native ARRAY;"
                    cursor.execute(alter_table_query)
                    self.client.commit()
                    logger.info(f"Column 'embedding_native' added to table {metadata_table_id}.")
            except Exception as e:
                logger.error(
                    f"An error occurred while checking or altering table {metadata_table_id}: {e}"
                )
            logger.info(f"Table {metadata_table_id} already exists.")
    except Exception as e:
        logger.error(
            f"An error occurred while checking or creating table {metadata_table_id}: {e}"
        )

    # ARTIFACTS STORE
    # --------------------
    # Setup artifact storage
    af = get_artifacts_store(self)
    af.setup_db_objects(replace_if_exists=False)

    # run python code stored procedure
    proc_name = 'execute_snowpark_code'
    stored_proc_ddl =   f"""CREATE OR REPLACE PROCEDURE {self.schema}.{proc_name}( code STRING )
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'pandas' )
HANDLER = 'run'
AS
$$
import snowflake.snowpark as snowpark
import re, importlib
import pandas as pd

def run(session: snowpark.Session, code: str) -> str:
    # Normalize line endings
    code = code.replace('\\\\r\\\\n', '\\n').replace('\\r', '\\n')

    # Find all import statements, including 'from ... import ...'
    import_statements = re.findall(r'^\\s*(import\\s+.*|from\\s+.*\\s+import\\s+.*)$', code, re.MULTILINE)

    # Additional regex to find 'from ... import ... as ...' statements
    import_statements += re.findall(r'^from\\s+(\\S+)\\s+import\\s+(\\S+)\\s+as\\s+(\\S+)', code, re.MULTILINE)

    global_vars = globals().copy()

    # Handle imports
    for import_statement in import_statements:
        try:
            exec(import_statement, global_vars)
        except ImportError as e:
            return f"Error: Unable to import - {{str(e)}}"

    local_vars = {{}}
    local_vars["session"] = local_vars["session"] = session

    try:
        # Remove import statements from the code before execution
        code_without_imports = re.sub(r'^\\s*(import\\s+.*|from\\s+.*\\s+import\\s+.*)$', '', code, flags=re.MULTILINE)
        exec(code_without_imports, global_vars, local_vars)

        if 'result' in local_vars:
            return local_vars['result']
        else:
            return "Error: 'result' is not defined in the executed code"
    except Exception as e:
        return f"Error: {{str(e)}}"
$$
"""
    try:
        cursor.execute(stored_proc_ddl)
        self.client.commit()
        logger.info(f"Stored procedure {self.schema}.execute_snowpark_code created.")
    except Exception as e:
        logger.error(f"An error occurred while creating stored procedure {self.schema}.execute_snowpark_code: {e}")


def get_processes_list(self, bot_id="all"):
    cursor = self.client.cursor()
    try:
        if bot_id == "all":
            list_query = f"SELECT process_id, bot_id, process_name FROM {self.schema}.PROCESSES"
            cursor.execute(list_query)
        else:
            list_query = f"SELECT process_id, bot_id, process_name FROM {self.schema}.PROCESSES WHERE upper(bot_id) = upper(%s)"
            cursor.execute(list_query, (bot_id,))
        processs = cursor.fetchall()
        process_list = []
        for process in processs:
            process_dict = {
                "process_id": process[0],
                "bot_id": process[1],
                "process_name": process[2],
            }
            process_list.append(process_dict)
        return {"Success": True, "processes": process_list}
    except Exception as e:
        return {
            "Success": False,
            "Error": f"Failed to list processs for bot {bot_id}: {e}",
        }
    finally:
        cursor.close()

def get_process_info(self, bot_id, process_name):
    cursor = self.client.cursor()
    try:
        query = f"SELECT * FROM {self.schema}.PROCESSES WHERE bot_id like %s AND process_name LIKE %s"
        cursor.execute(query, (f"%{bot_id}%", f"%{process_name}%",))
        result = cursor.fetchone()
        if result:
            # Assuming the result is a tuple of values corresponding to the columns in the PROCESSES table
            # Convert the tuple to a dictionary with appropriate field names
            field_names = [desc[0] for desc in cursor.description]
            return dict(zip(field_names, result))
        else:
            return {}
    except Exception as e:
        return {}


def insert_process_history(
    self,
    process_id,
    work_done_summary,
    process_status,
    updated_process_learnings,
    report_message="",
    done_flag=False,
    needs_help_flag="N",
    process_clarity_comments="",
):
    """
    Inserts a row into the PROCESS_HISTORY table.

    Args:
        process_id (str): The unique identifier for the process.
        work_done_summary (str): A summary of the work done.
        process_status (str): The status of the process.
        updated_process_learnings (str): Any new learnings from the process.
        report_message (str): The message to report about the process.
        done_flag (bool): Flag indicating if the process is done.
        needs_help_flag (bool): Flag indicating if help is needed.
        process_clarity_comments (str): Comments on the clarity of the process.
    """
    insert_query = f"""
        INSERT INTO {self.schema}.PROCESS_HISTORY (
            process_id, work_done_summary, process_status, updated_process_learnings,
            report_message, done_flag, needs_help_flag, process_clarity_comments
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s
        )
    """
    try:
        cursor = self.client.cursor()
        cursor.execute(
            insert_query,
            (
                process_id,
                work_done_summary,
                process_status,
                updated_process_learnings,
                report_message,
                done_flag,
                needs_help_flag,
                process_clarity_comments,
            ),
        )
        self.client.commit()
        cursor.close()
        logger.info(
            f"Process history row inserted successfully for process_id: {process_id}"
        )
    except Exception as e:
        logger.error(f"An error occurred while inserting the process history row: {e}")
        if cursor is not None:
            cursor.close()

def make_date_tz_aware(date, tz='UTC'):
    """
    Makes a date object timezone-aware.

    Args:
        date (datetime): The date to make timezone-aware.
        tz (str): The timezone to use.

    Returns:
        datetime: The date string with timezone information.
    """
    if type(date) is not str and date is not None and not pd.isna(date):
        # Ensure row['CREATED_AT'] is timezone-aware
        if date.tzinfo is None:
            date = date.tz_localize(pytz.timezone(tz))
        else:
            date = date.astimezone(pytz.timezone(tz))
        date_str = date.strftime('%Y-%m-%d %H:%M:%S')
    else:
        date_str = None

    return date_str

def load_default_processes_and_notebook(self, cursor):
        # Check both possible locations for the golden defaults
        folder_paths = [
            'genesis_bots/golden_defaults/golden_processes',
            '.genesis_bots/golden_defaults/golden_processes'
        ]
        self.process_data = pd.DataFrame()

        # Try each path until we find files
        files = []
        for folder_path in folder_paths:
            files = glob.glob(os.path.join(folder_path, '*.yaml'))
            if files:
                logger.info(f"Found process files in {folder_path}")
                break

        if not files:
            logger.info("No files found in golden_defaults/golden_processes")
            return

        for filename in files:
            with open(filename, 'r') as file:
                yaml_data = yaml.safe_load(file)

            data = pd.DataFrame.from_dict(yaml_data, orient='index')
            data.reset_index(inplace=True)
            data.rename(columns={'index': 'PROCESS_ID'}, inplace=True)

            self.process_defaults = pd.concat([self.process_data, data], ignore_index=True)

        # Ensure TIMESTAMP column is timezone-aware
        self.process_defaults['TIMESTAMP'] = pd.to_datetime(self.process_defaults['TIMESTAMP'], format='ISO8601', utc=True)

        updated_process = False

        for _, process_default in self.process_defaults.iterrows():
            process_id = process_default['PROCESS_ID']

            timestamp_str = make_date_tz_aware(process_default['TIMESTAMP'])

            query = f"SELECT * FROM {self.schema}.PROCESSES WHERE PROCESS_ID = %s"
            cursor.execute(query, (process_id,))
            result = cursor.fetchone()
            # process_columns = [desc[0] for desc in cursor.description if desc[0] != 'CREATED_AT']
            process_columns = [desc[0] for desc in cursor.description]

            updated_process = False
            process_found = False
            if result is not None:
                process_found = True
                db_timestamp = result[process_columns.index('UPDATED_AT')] if len(result) > 0 else None

                # Ensure db_timestamp is timezone-aware
                if isinstance(db_timestamp, str):
                    db_timestamp = pd.to_datetime(db_timestamp, utc=True)
                if db_timestamp is None or db_timestamp == '':
                    db_timestamp = datetime.now(pytz.UTC)
                elif db_timestamp.tzinfo is None:
                    db_timestamp = db_timestamp.replace(tzinfo=pytz.UTC)

                if process_default['PROCESS_ID'] == process_id and db_timestamp < process_default['TIMESTAMP']:
                    # Remove old process
                    query = f"DELETE FROM {self.schema}.PROCESSES WHERE PROCESS_ID = %s"
                    cursor.execute(query, (process_id,))
                    updated_process = True
                elif result[process_columns.index('PROCESS_ID')] == process_id:
                    continue

            if process_found == False or (process_found==True and updated_process==True):
                placeholders = ', '.join(['%s'] * len(process_columns))

                insert_values = []
                for key in process_columns:
                    if key.lower() == 'process_id':
                        insert_values.append(process_id)
                    elif key.lower() == 'timestamp' or key.lower() == 'updated_at' or key.lower() == 'created_at':
                        insert_values.append(timestamp_str)
                    elif key.lower() == 'process_instructions':
                        # Note - remove this line and uncomment below
                        insert_values.append(process_default['PROCESS_INSTRUCTIONS'])

                        # Check to see if the process_instructions are already in a note in the NOTEBOOK table
                        check_exist_query = f"SELECT * FROM {self.schema}.NOTEBOOK WHERE bot_id = %s AND note_content = %s"
                        cursor.execute(check_exist_query, (process_default['BOT_ID'], process_default['PROCESS_INSTRUCTIONS']))
                        result = cursor.fetchone()

                        if False and result is None:
                            # Use this code to insert the process_instructions into the NOTEBOOK table
                            characters = string.ascii_letters + string.digits
                            process_default['NOTE_ID'] = process_default['BOT_ID'] + '_' + ''.join(random.choice(characters) for i in range(10))
                            note_type = 'process'
                            insert_query = f"""
                                INSERT INTO {self.schema}.NOTEBOOK (bot_id, note_content, note_type, note_id)
                                VALUES (%s, %s, %s, %s)
                            """
                            cursor.execute(insert_query, (process_default['BOT_ID'], process_default['PROCESS_INSTRUCTIONS'], note_type, process_default['NOTE_ID']))
                            self.client.commit()

                            insert_values.append(process_default['NOTE_ID'])
                            logger.info(f"Note_id {process_default['NOTE_ID']} inserted successfully for process {process_id}")
                    elif key.lower() == 'hidden':
                        insert_values.append(False)
                    else:
                        val = process_default.get(key, '') if process_default.get(key, '') is not None else ''
                        if pd.isna(val):
                            val = ''
                        insert_values.append(val)

                insert_query = f"INSERT INTO {self.schema}.PROCESSES ({', '.join(process_columns)}) VALUES ({placeholders})"
                cursor.execute(insert_query, insert_values)
                if updated_process:
                    logger.info(f"Process {process_id} updated successfully.")
                    updated_process = False
                else:
                    logger.info(f"Process {process_id} inserted successfully.")
            else:
                logger.info(f"Process {process_id} already in PROCESSES and it is up to date.")
        cursor.close()

def upgrade_timestamp_columns(self, table_name):
    try:
        cursor = self.client.cursor()
        check_for_old_timestamp_columns_query = f"DESCRIBE TABLE {self.schema}.{table_name};"
        cursor.execute(check_for_old_timestamp_columns_query)
        columns = [col[0] for col in cursor.fetchall()]

        if "CREATED_AT" not in columns and "UPDATED_AT" not in columns:
            alter_table_query = f"ALTER TABLE {self.schema}.{table_name} ADD COLUMN \"CREATED_AT\" TIMESTAMP, \"UPDATED_AT\" TIMESTAMP;"
            cursor.execute(alter_table_query)
            self.client.commit()
            logger.info(f"Table {table_name} updated with new columns.")

        if "TIMESTAMP" in columns:
            # Copy contents of TIMESTAMP to CREATED_AT
            copy_timestamp_to_created_at_query = f"""
            UPDATE {self.schema}.{table_name}
            SET CREATED_AT = TIMESTAMP, UPDATED_AT = TIMESTAMP
            WHERE CREATED_AT IS NULL;
            """

            cursor.execute(copy_timestamp_to_created_at_query)
            self.client.commit()

            # Drop TIMESTAMP column
            drop_timestamp_query = f"ALTER TABLE {self.schema}.{table_name} DROP COLUMN TIMESTAMP;"
            cursor.execute(drop_timestamp_query)
            self.client.commit()
            logger.info(f"TIMESTAMP column dropped from {table_name}.")

    except Exception as e:
        logger.error(f"An error occurred while checking or adding new timestamp columns: {e}")

    finally:
        cursor.close()

    return

def load_default_notes(self, cursor):
    logger.info("load_default_notes")
    # Check both possible locations for the golden defaults
    folder_paths = [
        'genesis_bots/golden_defaults/golden_notes',
        '.genesis_bots/golden_defaults/golden_notes'
    ]
    notes_data = pd.DataFrame()

    # Try each path until we find files
    files = []
    for folder_path in folder_paths:
        files = glob.glob(os.path.join(folder_path, '*.yaml'))
        if files:
            logger.info(f"Found note files in {folder_path}")
            break

    if not files:
        logger.info("No files found in golden_defaults/golden_notes")
        return

    for filename in files:
        with open(filename, 'r') as file:
            yaml_data = yaml.safe_load(file)

        data = pd.DataFrame.from_dict(yaml_data, orient='index')
        data.reset_index(inplace=True)
        data.rename(columns={'index': 'NOTE_ID'}, inplace=True)

        note_defaults = pd.concat([notes_data, data], ignore_index=True)

    # Ensure TIMESTAMP column is timezone-aware
    note_defaults['TIMESTAMP'] = pd.to_datetime(note_defaults['TIMESTAMP'], format='ISO8601', utc=True)

    updated_note = False

    for _, note_default in note_defaults.iterrows():
        note_id = note_default['NOTE_ID']
        timestamp_str = make_date_tz_aware(note_default['TIMESTAMP'])

        query = f"SELECT * FROM {self.schema}.NOTEBOOK WHERE NOTE_ID = %s"
        cursor.execute(query, (note_id,))
        result = cursor.fetchone()
        notebook_columns = [desc[0] for desc in cursor.description]

        # ONE-TIME FIX - MAKE SURE TABLE HAS CREATED_AT AND UPDATED_AT COLUMNS
        upgrade_timestamp_columns(self, 'NOTEBOOK')

        updated_note = False
        note_found = False
        if result is not None:
            note_found = True
            timestamp_index = notebook_columns.index('UPDATED_AT') if 'UPDATED_AT' in notebook_columns else None
            db_timestamp = result[timestamp_index] if len(result) > 0 else None

            # Ensure db_timestamp is timezone-aware
            if isinstance(db_timestamp, str):
                db_timestamp = pd.to_datetime(db_timestamp, utc=True)
            if db_timestamp is None:
                db_timestamp = datetime.now(pytz.UTC)
            elif db_timestamp.tzinfo is None:
                db_timestamp = db_timestamp.replace(tzinfo=pytz.UTC)

            if result[notebook_columns.index('NOTE_ID')] == note_id and db_timestamp < note_default['TIMESTAMP']:
                # Remove old process
                query = f"DELETE FROM {self.schema}.NOTEBOOK WHERE NOTE_ID = %s"
                cursor.execute(query, (note_id,))
                updated_note = True
            elif result[notebook_columns.index('NOTE_ID')] == note_id:
                continue

        placeholders = ', '.join(['%s'] * len(notebook_columns))

        insert_values = []
        for key in notebook_columns:
            if key == 'NOTE_ID':
                insert_values.append(note_id)
            elif key.lower() == 'updated_at' or key.lower() == 'created_at':
                insert_values.append(timestamp_str)
            else:
                val = note_default.get(key, '') if note_default.get(key, '') is not None else ''
                if pd.isna(val):
                    val = ''
                insert_values.append(val)
        insert_query = f"INSERT INTO {self.schema}.NOTEBOOK ({', '.join(notebook_columns)}) VALUES ({placeholders})"
        cursor.execute(insert_query, insert_values)
        if updated_note:
            logger.info(f"Note {note_id} updated successfully.")
            updated_note = False
        else:
            logger.info(f"Note {note_id} inserted successfully.")
    cursor.close()
