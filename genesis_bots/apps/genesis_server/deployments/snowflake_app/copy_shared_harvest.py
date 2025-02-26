import os
import sys
import json
from snowflake.connector import connect
import uuid
from datetime import datetime
from ..genesis_bots.core.logging_config import logger

def _create_connection_target():
    account = os.getenv('SNOWFLAKE_ACCOUNT_OVERRIDE_E')
    user = os.getenv('SNOWFLAKE_USER_OVERRIDE_E')
    host = 'fm01908.us-east-2.aws.snowflakecomputing.com'
# east1-    host = 'vyb73862.us-east-1.snowflakecomputing.com'
    password = os.getenv('SNOWFLAKE_PASSWORD_OVERRIDE_E')
    database = os.getenv('SNOWFLAKE_DATABASE_OVERRIDE_E', None)
    warehouse = os.getenv('SNOWFLAKE_WAREHOUSE_OVERRIDE_E', None)
    role = os.getenv('SNOWFLAKE_ROLE_OVERRIDE_E', None)

    return connect(
        user=user,
        password=password,
        account=account,
        warehouse=warehouse,
        host=host,
        database=database,
        role=role
    )



def _create_connection_source():
    account = os.getenv('SNOWFLAKE_ACCOUNT_OVERRIDE')
    user = os.getenv('SNOWFLAKE_USER_OVERRIDE')
    password = os.getenv('SNOWFLAKE_PASSWORD_OVERRIDE')
    database = os.getenv('SNOWFLAKE_DATABASE_OVERRIDE', None)
    warehouse = os.getenv('SNOWFLAKE_WAREHOUSE_OVERRIDE', None)
    role = os.getenv('SNOWFLAKE_ROLE_OVERRIDE', None)

    return connect(
        user=user,
        password=password,
        account=account,
        warehouse=warehouse,
        database=database,
        role=role
    )

def insert_table_summary(self, database_name, schema_name, table_name, ddl, ddl_short, summary, sample_data_text, complete_description="", crawl_status="Completed", role_used_for_crawl="Default", embedding=None):

        qualified_table_name = f'"{database_name}"."{schema_name}"."{table_name}"'
        memory_uuid = str(uuid.uuid4())
        last_crawled_timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat(" ")
        ddl_hash = self.sha256_hash_hex_string(ddl)

        # Assuming role_used_for_crawl is stored in self.connection_info["client_email"]
        role_used_for_crawl = self.role

        # Convert embedding list to string format if not None
        embedding_str = ','.join(str(e) for e in embedding) if embedding is not None else None

        # Construct the MERGE SQL statement with placeholders for parameters
        merge_sql = f"""
        MERGE INTO {self.metadata_table_name} USING (
            SELECT
                %(source_name)s AS source_name,
                %(qualified_table_name)s AS qualified_table_name,
                %(memory_uuid)s AS memory_uuid,
                %(database_name)s AS database_name,
                %(schema_name)s AS schema_name,
                %(table_name)s AS table_name,
                %(complete_description)s AS complete_description,
                %(ddl)s AS ddl,
                %(ddl_short)s AS ddl_short,
                %(ddl_hash)s AS ddl_hash,
                %(summary)s AS summary,
                %(sample_data_text)s AS sample_data_text,
                %(last_crawled_timestamp)s AS last_crawled_timestamp,
                %(crawl_status)s AS crawl_status,
                %(role_used_for_crawl)s AS role_used_for_crawl,
                %(embedding)s AS embedding
        ) AS new_data
        ON {self.metadata_table_name}.qualified_table_name = new_data.qualified_table_name
        WHEN MATCHED THEN UPDATE SET
            source_name = new_data.source_name,
            memory_uuid = new_data.memory_uuid,
            database_name = new_data.database_name,
            schema_name = new_data.schema_name,
            table_name = new_data.table_name,
            complete_description = new_data.complete_description,
            ddl = new_data.ddl,
            ddl_short = new_data.ddl_short,
            ddl_hash = new_data.ddl_hash,
            summary = new_data.summary,
            sample_data_text = new_data.sample_data_text,
            last_crawled_timestamp = TO_TIMESTAMP_NTZ(new_data.last_crawled_timestamp),
            crawl_status = new_data.crawl_status,
            role_used_for_crawl = new_data.role_used_for_crawl,
            embedding = ARRAY_CONSTRUCT(new_data.embedding)
        WHEN NOT MATCHED THEN INSERT (
            source_name, qualified_table_name, memory_uuid, database_name, schema_name, table_name,
            complete_description, ddl, ddl_short, ddl_hash, summary, sample_data_text, last_crawled_timestamp,
            crawl_status, role_used_for_crawl, embedding
        ) VALUES (
            new_data.source_name, new_data.qualified_table_name, new_data.memory_uuid, new_data.database_name,
            new_data.schema_name, new_data.table_name, new_data.complete_description, new_data.ddl, new_data.ddl_short,
            new_data.ddl_hash, new_data.summary, new_data.sample_data_text, TO_TIMESTAMP_NTZ(new_data.last_crawled_timestamp),
            new_data.crawl_status, new_data.role_used_for_crawl, ARRAY_CONSTRUCT(new_data.embedding)
        );
        """

        # Set up the query parameters
        query_params = {
            'source_name': self.source_name,
            'qualified_table_name': qualified_table_name,
            'memory_uuid': memory_uuid,
            'database_name': database_name,
            'schema_name': schema_name,
            'table_name': table_name,
            'complete_description': complete_description,
            'ddl': ddl,
            'ddl_short': ddl_short,
            'ddl_hash': ddl_hash,
            'summary': summary,
            'sample_data_text': sample_data_text,
            'last_crawled_timestamp': last_crawled_timestamp,
            'crawl_status': crawl_status,
            'role_used_for_crawl': role_used_for_crawl,
            'embedding': embedding_str
        }

        for param, value in query_params.items():
            #logger.info(f'{param}: {value}')
            if value is None:
               # logger.info(f'{param} is null')
                query_params[param] = 'NULL'

        # Execute the MERGE statement with parameters
        try:
            #logger.info("merge sql: ",merge_sql)
            cursor = self.client.cursor()
            cursor.execute(merge_sql, query_params)
            self.client.commit()
        except Exception as e:
            logger.info(f"An error occurred while executing the MERGE statement: {e}")
        finally:
            if cursor is not None:
                cursor.close()


if __name__ == "__main__":
    # Open a connection to source Snowflake
    source_conn = _create_connection_source()

    # Open a connection to target Snowflake
    target_conn = _create_connection_target()

    source_cursor = source_conn.cursor()

    # Check if the table SHARED_HARVEST_MASTER.public.harvest_results exists in the source Snowflake    source_cursor = source_conn.cursor()
    try:
        source_cursor.execute("SHOW TABLES LIKE 'HARVEST_RESULTS' IN SCHEMA GENESISAPP_MASTER.HARVEST_SHARE")
        result = source_cursor.fetchone()
        if result:
            logger.info("Table GENESISAPP_MASTER.HARVEST_SHARE.harvest_results exists in the source Snowflake.")
        else:
            logger.info("Table GENESISAPP_MASTER.HARVEST_SHARE.harvest_results does not exist in the source Snowflake.")
    except Exception as e:
        logger.info(f"An error occurred while checking for the table: {e}")
        source_conn.close()
        target_conn.close()
        raise e
    finally:
        source_cursor.close()

    target_cursor = target_conn.cursor()
    try:
        # Check if the database exists on the target, if not, create it
        target_cursor.execute("CREATE DATABASE IF NOT EXISTS GENESISAPP_MASTER")
        target_conn.commit()
        logger.info("Database GENESISAPP_MASTER ensured on target Snowflake.")

        # Check if the schema exists on the target, if not, create it
        target_cursor.execute("CREATE SCHEMA IF NOT EXISTS GENESISAPP_MASTER.HARVEST_SHARE")
        target_conn.commit()
        logger.info("Schema GENESISAPP_MASTER.HARVEST_SHARE ensured on target Snowflake.")
    except Exception as e:
        logger.info(f"An error occurred while ensuring the database and schema on the target: {e}")
        source_conn.close()
        target_conn.close()
        raise e
    finally:
        target_cursor.close()

    target_cursor = target_conn.cursor()
    try:
        # Check if the table harvest_results exists on the target, if not, create it, if yes recreate it
        target_cursor.execute("""
            CREATE OR REPLACE TABLE  GENESISAPP_MASTER.HARVEST_SHARE.HARVEST_RESULTS (
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
                embedding ARRAY
            );
        """)
        target_conn.commit()
        logger.info("Table GENESISAPP_MASTER.HARVEST_SHARE.HARVEST_RESULTS ensured on target Snowflake.")
    except Exception as e:
        logger.info(f"An error occurred while ensuring the table GENESISAPP_MASTER.HARVEST_SHARE.HARVEST_RESULTS on the target: {e}")
        source_conn.close()
        target_conn.close()
        raise e



    source_cursor = source_conn.cursor()
    target_cursor = target_conn.cursor()
    try:
        source_cursor.execute("SELECT * FROM GENESISAPP_MASTER.HARVEST_SHARE.HARVEST_RESULTS")
        rows = source_cursor.fetchall()
        i = 0
        for row in rows:
            i = i + 1
            logger.info(i, row[1])
            # Convert the last element in the row from string to array if it's not None
            if row[-1] is not None and isinstance(row[-1], str):
                embedding_array = row[-1].split(',') if row[-1] else []
                row = row[:-1] + (embedding_array,)

            try:
                # Assuming the last element in the row is the embedding in string format
                embedding_str = row[-1]
                # Fix the first row of the array if it starts with '[\n "'
                if embedding_str[0].startswith('[\n  "'):
                    embedding_str[0] = embedding_str[0].replace('[\n  "', '\'')
                # Attempt to convert the string representation of the embedding into an actual list of floats
                embedding = embedding_str
            except json.JSONDecodeError:
                try:
                    # If the first attempt fails, try a different slicing approach
                    embedding = json.loads('[' + embedding_str[5:-10] + ']')
                except json.JSONDecodeError:
                    # If both attempts fail, log an error and set the embedding to an empty list
                    logger.info(f"Cannot load array from Snowflake for row: {row}")
                    embedding = []
            # Replace the last element in the row with the actual list of floats

            embedding_str_last = embedding_str[:1]
            embedding_str_new = ','.join(str(e) for e in embedding_str) if embedding is not None else None

            a = row[15]
            row[15][3071] = row[15][3071][:-3]

            emb = [','.join(str(e) for e in row[15])]
            emb[0] = emb[0].replace('\'','')

            # Prepare the data for insertion using a dictionary to match the placeholders in the SQL query
            insert_data = {
                'source_name': row[0],
                'qualified_table_name': row[1],
                'database_name': row[2],
                'memory_uuid': row[3],
                'schema_name': row[4],
                'table_name': row[5],
                'complete_description': row[6],
                'ddl': row[7],
                'ddl_short': row[8],
                'ddl_hash': row[9],
                'summary': row[10],
                'sample_data_text': row[11],
                'last_crawled_timestamp': row[12],
                'crawl_status': row[13],
                'role_used_for_crawl': row[14],
                'embedding': emb if emb is not None else None
            }

            # Construct the MERGE SQL statement with placeholders for parameters
            merge_sql = f"""
            MERGE INTO GENESISAPP_MASTER.HARVEST_SHARE.HARVEST_RESULTS USING (
                SELECT
                    %(source_name)s AS source_name,
                    %(qualified_table_name)s AS qualified_table_name,
                    %(memory_uuid)s AS memory_uuid,
                    %(database_name)s AS database_name,
                    %(schema_name)s AS schema_name,
                    %(table_name)s AS table_name,
                    %(complete_description)s AS complete_description,
                    %(ddl)s AS ddl,
                    %(ddl_short)s AS ddl_short,
                    %(ddl_hash)s AS ddl_hash,
                    %(summary)s AS summary,
                    %(sample_data_text)s AS sample_data_text,
                    %(last_crawled_timestamp)s AS last_crawled_timestamp,
                    %(crawl_status)s AS crawl_status,
                    %(role_used_for_crawl)s AS role_used_for_crawl,
                    %(embedding)s AS embedding
            ) AS new_data
            ON GENESISAPP_MASTER.HARVEST_SHARE.HARVEST_RESULTS.qualified_table_name = new_data.qualified_table_name
            WHEN MATCHED THEN UPDATE SET
                source_name = new_data.source_name,
                memory_uuid = new_data.memory_uuid,
                database_name = new_data.database_name,
                schema_name = new_data.schema_name,
                table_name = new_data.table_name,
                complete_description = new_data.complete_description,
                ddl = new_data.ddl,
                ddl_short = new_data.ddl_short,
                ddl_hash = new_data.ddl_hash,
                summary = new_data.summary,
                sample_data_text = new_data.sample_data_text,
                last_crawled_timestamp = new_data.last_crawled_timestamp,
                crawl_status = new_data.crawl_status,
                role_used_for_crawl = new_data.role_used_for_crawl,
                embedding = ARRAY_CONSTRUCT(new_data.embedding)
            WHEN NOT MATCHED THEN INSERT (
                source_name, qualified_table_name, memory_uuid, database_name, schema_name, table_name,
                complete_description, ddl, ddl_short, ddl_hash, summary, sample_data_text, last_crawled_timestamp,
                crawl_status, role_used_for_crawl, embedding
            ) VALUES (
                new_data.source_name, new_data.qualified_table_name, new_data.memory_uuid, new_data.database_name,
                new_data.schema_name, new_data.table_name, new_data.complete_description, new_data.ddl, new_data.ddl_short,
                new_data.ddl_hash, new_data.summary, new_data.sample_data_text, new_data.last_crawled_timestamp,
                new_data.crawl_status, new_data.role_used_for_crawl, ARRAY_CONSTRUCT(new_data.embedding)
            );
            """

            # Execute the MERGE statement with the prepared data
            target_cursor.execute(merge_sql, insert_data)
        target_conn.commit()
        logger.info("Data copied from source to target for GENESISAPP_MASTER.HARVEST_SHARE.HARVEST_RESULTS.")

        # Check hashaggs of both tables to make sure they are identical
        try:
            source_hashagg_sql = "SELECT HASH_AGG(*) FROM GENESISAPP_MASTER.HARVEST_SHARE.HARVEST_RESULTS;"
            target_hashagg_sql = "SELECT HASH_AGG(*) FROM GENESISAPP_MASTER.HARVEST_SHARE.HARVEST_RESULTS;"  # Replace TARGET_TABLE_NAME with the actual target table name

            source_cursor.execute(source_hashagg_sql)
            source_hashagg = source_cursor.fetchone()[0]

            target_cursor.execute(target_hashagg_sql)
            target_hashagg = target_cursor.fetchone()[0]

            if source_hashagg == target_hashagg:
                logger.info("Hash aggregates of both tables are identical.")
            else:
                logger.info("Hash aggregates of both tables differ.")
                # Additional logic can be added here if action is needed when hash aggregates do not match
        except Exception as e:
            logger.info(f"An error occurred while comparing hash aggregates: {e}")
            raise e


    except Exception as e:
        logger.info(f"An error occurred while copying data: {e}")
        source_conn.close()
        target_conn.close()
        raise e
    finally:
        source_cursor.close()
        target_cursor.close()
