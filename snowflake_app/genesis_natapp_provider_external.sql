

-- ########## BEGIN ENVIRONMENT  ######################################


SET APP_OWNER_ROLE = 'ACCOUNTADMIN';
SET APP_WAREHOUSE = 'XSMALL';
SET APP_DISTRIBUTION = 'EXTERNAL';

-- ########## END   ENVIRONMENT  ######################################



USE ROLE ACCOUNTADMIN;
USE WAREHOUSE XSMALL;



-- ########## BEGIN INITIALIZATION  ######################################


--DROP DATABASE IF EXISTS GENESISAPP_APP_PKG_EXT ;
--drop database genesisapp_master;


CREATE DATABASE IF NOT EXISTS GENESISAPP_MASTER;
USE DATABASE GENESISAPP_MASTER;
CREATE SCHEMA IF NOT EXISTS CODE_SCHEMA;
USE SCHEMA CODE_SCHEMA;
CREATE IMAGE REPOSITORY IF NOT EXISTS SERVICE_REPO;




USE DATABASE GENESISAPP_APP_PKG_EXT;
CREATE SCHEMA IF NOT EXISTS CODE_SCHEMA;
CREATE STAGE IF NOT EXISTS APP_CODE_STAGE;


-- ##########  END INITIALIZATION   ######################################


--
-- STOP HERE AND UPLOAD ALL REQUIRED CONTAINERS INTO THE IMAGE REPO
--
show image repositories;

-- ########## UTILITY FUNCTIONS  #########################################
USE SCHEMA GENESISAPP_APP_PKG_EXT.CODE_SCHEMA;
use warehouse xsmall;


CREATE OR REPLACE PROCEDURE PUT_TO_STAGE_SUBDIR(STAGE VARCHAR,SUBDIR VARCHAR,FILENAME VARCHAR, CONTENT VARCHAR)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION=3.8
PACKAGES=('snowflake-snowpark-python')
HANDLER='put_to_stage'
AS $$
import io
import os


def put_to_stage(session, stage, subdir, filename, content):
   local_path = '/tmp'
   local_file = os.path.join(local_path, filename)
   f = open(local_file, "w")
   f.write(content)
   f.close()
   session.file.put(local_file, '@'+stage+'/'+subdir, auto_compress=False, overwrite=True)
   return "saved file "+filename+" in stage "+stage
$$;


CREATE OR REPLACE PROCEDURE GET_FROM_STAGE_SUBDIR(STAGE VARCHAR,SUBDIR VARCHAR, FILENAME VARCHAR)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION=3.8
PACKAGES=('snowflake-snowpark-python')
HANDLER='get_from_stage'
AS $$
import io
import os
from pathlib import Path


def get_from_stage(session, stage, subdir, filename):
   local_path = '/tmp'
   local_file = os.path.join(local_path, filename)
   session.file.get('@'+stage+'/'+subdir+'/'+filename, local_path)
   content=Path(local_file).read_text()
   return content
$$;


CREATE OR REPLACE PROCEDURE PUT_TO_STAGE(STAGE VARCHAR,FILENAME VARCHAR, CONTENT VARCHAR)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION=3.8
PACKAGES=('snowflake-snowpark-python')
HANDLER='put_to_stage'
AS $$
import io
import os


def put_to_stage(session, stage, filename, content):
   local_path = '/tmp'
   local_file = os.path.join(local_path, filename)
   f = open(local_file, "w")
   f.write(content)
   f.close()
   session.file.put(local_file, '@'+stage, auto_compress=False, overwrite=True)
   return "saved file "+filename+" in stage "+stage
$$;


--
-- Python stored procedure to return the content of a file in a stage
--
CREATE OR REPLACE PROCEDURE GET_FROM_STAGE(STAGE VARCHAR,FILENAME VARCHAR)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION=3.8
PACKAGES=('snowflake-snowpark-python')
HANDLER='get_from_stage'
AS $$
import io
import os
from pathlib import Path


def get_from_stage(session, stage, filename):
   local_path = '/tmp'
   local_file = os.path.join(local_path, filename)
   session.file.get('@'+stage+'/'+filename, local_path)
   content=Path(local_file).read_text()
   return content
$$;


-- ########## END UTILITY FUNCTIONS  #####################################


-- ########## DATA SHARING  ##############################################

-- dynamically generate shared views and grants for spider_data
CREATE OR REPLACE PROCEDURE CODE_SCHEMA.GENERATE_SHARED_VIEWS(SCHEMA_NAME VARCHAR, APP_PKG_NAME VARCHAR)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
  result STRING DEFAULT '';
  create_view_query STRING;
  grant_query STRING;
  table_name STRING;
  table_catalog STRING;
  table_schema STRING;
  new_schema_name STRING;
  app_pkg STRING;
  select_statement STRING;
BEGIN
  new_schema_name := SCHEMA_NAME;
  app_pkg := APP_PKG_NAME;

  EXECUTE IMMEDIATE 'CREATE SCHEMA IF NOT EXISTS ' || app_pkg || '.' || new_schema_name;
  EXECUTE IMMEDIATE 'GRANT USAGE ON SCHEMA ' || app_pkg || '.' || new_schema_name || ' TO SHARE IN APPLICATION PACKAGE ' || app_pkg;
  
  LET table_cursor CURSOR FOR SELECT TABLE_NAME, TABLE_CATALOG, TABLE_SCHEMA FROM SPIDER_DATA.INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = ?;
  OPEN table_cursor USING(new_schema_name);
  
  FOR table_record IN table_cursor DO
    table_name := table_record.table_name;
    table_catalog := table_record.table_catalog;
    table_schema := table_record.table_schema;
    create_view_query := 'CREATE OR REPLACE VIEW ' || app_pkg || '.' || table_schema || '.' || table_name || ' AS SELECT * FROM ' || table_catalog || '.' || table_schema || '.' || table_name || ';';
    grant_query := 'GRANT SELECT ON VIEW ' || app_pkg || '.' || table_schema || '.' || table_name || ' TO SHARE IN APPLICATION PACKAGE ' || app_pkg;
    EXECUTE IMMEDIATE create_view_query;
    EXECUTE IMMEDIATE grant_query;
    result := result || 'Executed: ' || grant_query || CHAR(10) || 'Executed: ' || create_view_query || CHAR(10);
  END FOR;
  RETURN result;
END;
$$;


-- Call the procedure to generate shared views and grants
CALL CODE_SCHEMA.GENERATE_SHARED_VIEWS('BASEBALL', CURRENT_DATABASE());
USE SCHEMA GENESISAPP_APP_PKG_EXT.CODE_SCHEMA;
CALL CODE_SCHEMA.GENERATE_SHARED_VIEWS('FORMULA_1', CURRENT_DATABASE());
USE SCHEMA GENESISAPP_APP_PKG_EXT.CODE_SCHEMA;

CREATE OR REPLACE PROCEDURE CODE_SCHEMA.SHARE_TO_APP_PKG(APP_PKG_NAME STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
-- Grant reference usage on additional databases
  EXECUTE IMMEDIATE 'GRANT REFERENCE_USAGE ON DATABASE SPIDER_DATA TO SHARE IN APPLICATION PACKAGE ' || APP_PKG_NAME || ';';
  EXECUTE IMMEDIATE 'GRANT REFERENCE_USAGE ON DATABASE GENESISAPP_MASTER TO SHARE IN APPLICATION PACKAGE ' || APP_PKG_NAME || ';';

-- Share harvest records from provider to app package (to be removed)
  EXECUTE IMMEDIATE 'CREATE OR REPLACE SCHEMA ' || APP_PKG_NAME || '.SHARED_HARVEST';
  EXECUTE IMMEDIATE 'GRANT USAGE ON SCHEMA ' || APP_PKG_NAME || '.SHARED_HARVEST TO SHARE IN APPLICATION PACKAGE ' || APP_PKG_NAME;
  EXECUTE IMMEDIATE 'CREATE OR REPLACE VIEW ' || APP_PKG_NAME || '.SHARED_HARVEST.HARVEST_RESULTS AS SELECT * FROM GENESISAPP_MASTER.HARVEST_SHARE.HARVEST_RESULTS_SHARED';
  EXECUTE IMMEDIATE 'GRANT SELECT ON VIEW ' || APP_PKG_NAME || '.SHARED_HARVEST.HARVEST_RESULTS TO SHARE IN APPLICATION PACKAGE ' || APP_PKG_NAME;

 -- Share data from provider to app package
  EXECUTE IMMEDIATE 'CREATE OR REPLACE SCHEMA ' || APP_PKG_NAME || '.APP_SHARE';
  EXECUTE IMMEDIATE 'GRANT USAGE ON SCHEMA ' || APP_PKG_NAME || '.APP_SHARE TO SHARE IN APPLICATION PACKAGE ' || APP_PKG_NAME;

 -- Share harvest data
  EXECUTE IMMEDIATE 'CREATE OR REPLACE VIEW ' || APP_PKG_NAME || '.APP_SHARE.HARVEST_RESULTS AS SELECT * FROM GENESISAPP_MASTER.HARVEST_SHARE.HARVEST_RESULTS_SHARED';
  EXECUTE IMMEDIATE 'GRANT SELECT ON VIEW ' || APP_PKG_NAME || '.APP_SHARE.HARVEST_RESULTS TO SHARE IN APPLICATION PACKAGE ' || APP_PKG_NAME; 
 -- Share image data
  EXECUTE IMMEDIATE 'CREATE OR REPLACE VIEW ' || APP_PKG_NAME || '.APP_SHARE.IMAGES AS SELECT ID, IMAGE_NAME, BOT_NAME, ENCODED_IMAGE_DATA, IMAGE_DESC FROM GENESISAPP_MASTER.APP_SHARE.IMAGES_SHARED';
  EXECUTE IMMEDIATE 'GRANT SELECT ON VIEW ' || APP_PKG_NAME || '.APP_SHARE.IMAGES TO SHARE IN APPLICATION PACKAGE ' || APP_PKG_NAME;

  RETURN 'Created and shared harvest results and images views';
END;
$$;


-- Call procedure to create the shared view for harvest results
CALL CODE_SCHEMA.SHARE_TO_APP_PKG(CURRENT_DATABASE());
USE SCHEMA GENESISAPP_APP_PKG_EXT.CODE_SCHEMA;
-- Call the procedure to generate shared views and grants
CALL CODE_SCHEMA.GENERATE_SHARED_VIEWS('BASEBALL', CURRENT_DATABASE());
USE SCHEMA GENESISAPP_APP_PKG_EXT.CODE_SCHEMA;
CALL CODE_SCHEMA.GENERATE_SHARED_VIEWS('FORMULA_1', CURRENT_DATABASE());
USE SCHEMA GENESISAPP_APP_PKG_EXT.CODE_SCHEMA;



-- ########## END DATA SHARING  ##########################################



-- ########## SCRIPTS CONTENT  ###########################################
USE SCHEMA GENESISAPP_APP_PKG_EXT.CODE_SCHEMA;
CREATE OR REPLACE TABLE SCRIPT (NAME VARCHAR, VALUE VARCHAR);
DELETE FROM SCRIPT;




INSERT INTO SCRIPT (NAME , VALUE)
VALUES ('MANIFEST',
$$
manifest_version: 1 # required
artifacts:
 setup_script: setup_script.sql
 readme: readme.md
 container_services:
   images:
   - /genesisapp_master/code_schema/service_repo/genesis_app:latest  
 extension_code: true
 default_streamlit: core.sis_launch
configuration:
  trace_level: OFF
  log_level: DEBUG
privileges:
  - BIND SERVICE ENDPOINT:
      description: "Allow access to application endpoints"
  - IMPORTED PRIVILEGES ON SNOWFLAKE DB:
      description: "to use of CORTEX LLM functions"
$$)
;
--privileges:
--  - IMPORTED PRIVILEGES ON SNOWFLAKE DB:
--      description: "to see table metadata of granted tables"


--privileges:
--  - BIND SERVICE ENDPOINT:
--      description: "a service can serve requests from public endpoint"



use schema GENESISAPP_APP_PKG_EXT.code_schema;
-- delete from script;

INSERT INTO SCRIPT (NAME , VALUE)
VALUES ('SIS_ENV',
$$
name: sis_launch
channels:
 - snowflake
dependencies:
 - streamlit=1.26.0
 - pandas
 - snowflake-snowpark-python
$$)
;


INSERT INTO SCRIPT (NAME , VALUE)
VALUES ('SIS_APP',
$$
#Get from Cursor
$$)
;


CREATE OR REPLACE TEMPORARY TABLE script_tmp AS SELECT 'README' NAME,REGEXP_REPLACE($$

# Title
Genesis Bots are AI-powered workers that can perform jobs for your company.

## Permissions
In the setup guide, you'll be asked to grant additional privileges from your account.

Once you install Genesis, you will be directed to a Streamlit app, which will walk you through running commands
in your Snowflake account to grant the application access to the following resources:

1. A Snowflake Virtual Warehouse to power Snowflake queries run by Genesis
2. A Snowflake Compute Pool to run the Genesis Server containers
3. A Network Rule and External Access Integration, to allow Genesis to access the following external endpoints:
    - OpenAI API
    - Slack
4. Optionally, access to any of your existing Databases, Schemas, and Tables you'd like to use with Genesis.

### Account level privileges

`BIND SERVICE ENDPOINT` on **ACCOUNT**
To allow Genesis to open two endpoints, one for Slack to authorize new Apps via OAuth, and one for inbound
access to the Streamlit Genesis GUI

`IMPORTED PRIVILEGES` ON **SNOWFLAKE DB**
To allow use of Snowflake CORTEX LLM functions

### Privileges to objects
`USAGE` on **COMPUTE POOL**
To run the Genesis Server containers in Snowpark Conrainer Services

`USAGE` on **WAREHOUSE**
For Genesis to run queries on Snowflake

`USAGE` on **EXTERNAL ACCESS INTEGRATION**
To allow Genesis to access external OpenAI and Slack API endpoints

`USAGE` on **DATABASES, SCHEMAS**
To optionally allow Genesis to work with some of your data

`SELECT` on **TABLES, VIEWS**
To optionally allow Genesis to work with some of your data

---

## Object creation
In the setup guide, you'll be asked to create the following object(s) in your account. 

`WAREHOUSE`XSMALL
For Genesis to use to run queries on Snowflake

`COMPUTE POOL`GENESIS_POOL
For Genesis to use to run its Genesis Server containers

`DATABASE`GENESIS_LOCAL_DB
To store the network rule

`SCHEMA`GENESIS_LOCAL_DB.SETTINGS
To store the network rule

`NETWORK RULE`GENESIS_LOCAL_DB.SETTINGS.GENESIS_RULE
To allow Genesis to access to required external APIs (OpenAI and Slack)

`EXTERNAL ACCESS INTEGRATION`GENESIS_EAI
To allow Genesis to access to required external APIs (OpenAI and Slack)


---

## Setup code

-- Note: Please use the default Streamlit App for a full walkthrough of these steps

-- use a role with sufficient privileges for the

use role ACCOUNTADMIN;

-- set the name of the installed application and warehouse to use

set APP_DATABASE = 'GENESIS_BOTS_ALPHA';
set APP_WAREHOUSE = 'XSMALL';  -- ok to use an existing warehouse

-- create the warehouse if needed

CREATE WAREHOUSE IF NOT EXISTS IDENTIFIER($APP_WAREHOUSE)
 MIN_CLUSTER_COUNT=1 MAX_CLUSTER_COUNT=1
 WAREHOUSE_SIZE=XSMALL AUTO_RESUME = TRUE AUTO_SUSPEND = 60;

-- allow Genesis to use the warehouse

GRANT USAGE ON WAREHOUSE  IDENTIFIER($APP_WAREHOUSE) TO APPLICATION  IDENTIFIER($APP_DATABASE);

-- remove an existing pool, if you've installed this app before

DROP COMPUTE POOL IF EXISTS GENESIS_POOL;

-- create the compute pool and associate it to this application

CREATE COMPUTE POOL IF NOT EXISTS GENESIS_POOL FOR APPLICATION IDENTIFIER($APP_DATABASE)
 MIN_NODES=1 MAX_NODES=1 INSTANCE_FAMILY='CPU_X64_XS' AUTO_SUSPEND_SECS=3600 INITIALLY_SUSPENDED=FALSE;

-- give Genesis the right to use the compute pool

GRANT USAGE ON COMPUTE POOL GENESIS_POOL TO APPLICATION  IDENTIFIER($APP_DATABASE);

-- create a local database to store the network rule (you can change these to an existing database and schema if you like)

CREATE DATABASE IF NOT EXISTS GENESIS_LOCAL_DB; 
CREATE SCHEMA IF NOT EXISTS GENESIS_LOCAL_DB.SETTINGS;

-- create a network rule that allows Genesis Server to access OpenAI's API, and optionally Slack 

-- create a network rule that allows Genesis Server to access OpenAI's API, and optionally Slack API and Azure Blob (for image generation) 
CREATE OR REPLACE NETWORK RULE GENESIS_LOCAL_DB.SETTINGS.GENESIS_RULE
 MODE = EGRESS TYPE = HOST_PORT
VALUE_LIST = ('api.openai.com', 'slack.com', 'www.slack.com', 'wss-primary.slack.com',
'wss-backup.slack.com',  'wss-primary.slack.com:443','wss-backup.slack.com:443','www.genesiscomputing.ai',
'oaidalleapiprodscus.blob.core.windows.net:443', 'downloads.slack-edge.com', 'files-edge.slack.com',
'files-origin.slack.com', 'files.slack.com', 'global-upload-edge.slack.com','universal-upload-edge.slack.com');

-- create an external access integration that surfaces the above network rule

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION GENESIS_EAI
   ALLOWED_NETWORK_RULES = (GENESIS_LOCAL_DB.SETTINGS.GENESIS_RULE) ENABLED = true;

-- Allows Slack to access the Genesis server to approve new Genesis Slack Applications

GRANT BIND SERVICE ENDPOINT ON ACCOUNT TO APPLICATION  IDENTIFIER($APP_DATABASE); 

-- grant Genesis Server the ability to use this external access integration

GRANT USAGE ON INTEGRATION GENESIS_EAI TO APPLICATION   IDENTIFIER($APP_DATABASE);

## Setup instructions

Please use the default Streamlit provided with this native application for a fully-guided setup experience.

## Usage Snippets

Please use the default Streamlit to interact with the Genesis application.

$$,':::','$$') VALUE;

-- delete from script;
INSERT INTO SCRIPT SELECT * FROM SCRIPT_TMP;


CREATE OR REPLACE TEMPORARY TABLE script_tmp AS SELECT 'SETUP' NAME,REGEXP_REPLACE($$
CREATE OR ALTER VERSIONED SCHEMA APP;


CREATE OR REPLACE TABLE APP.YAML (name varchar, value varchar);

INSERT INTO APP.YAML (NAME , VALUE)
VALUES ('GENESISAPP_SERVICE_SERVICE',
:::
    spec:
      containers:
      - name: genesis
        image: /genesisapp_master/code_schema/service_repo/genesis_app:latest
        env:
            RUNNER_ID: snowflake-1
            GENESIS_INTERNAL_DB_SCHEMA: {{app_db_sch}}
            GENESIS_SOURCE: Snowflake
            SNOWFLAKE_SECURE: FALSE
            OPENAI_HARVESTER_EMBEDDING_MODEL: text-embedding-3-large
        readinessProbe:
          port: 8080
          path: /healthcheck
      endpoints:
      - name: udfendpoint
        port: 8080
        public: true
      logExporters:
        eventTableConfig:
          logLevel: INFO
:::)
;

INSERT INTO APP.YAML (NAME , VALUE)
VALUES ('GENESISAPP_HARVESTER_SERVICE',
:::
    spec:
      containers:
      - name: genesis-harvester
        image: /genesisapp_master/code_schema/service_repo/genesis_app:latest
        env:
            GENESIS_MODE: HARVESTER
            AUTO_HARVEST: TRUE
            OPENAI_HARVESTER_EMBEDDING_MODEL: text-embedding-3-large
            HARVESTER_REFRESH_SECONDS: 120
            RUNNER_ID: snowflake-1
            SNOWFLAKE_SECURE: FALSE
            GENESIS_INTERNAL_DB_SCHEMA: {{app_db_sch}}
            GENESIS_SOURCE: Snowflake
      endpoints:
      - name: udfendpoint
        port: 8080
        public: false
      logExporters:
        eventTableConfig:
          logLevel: INFO
:::)
;

INSERT INTO APP.YAML (NAME , VALUE)
VALUES ('GENESISAPP_TASK_SERVICE',
:::
    spec:
      containers:
      - name: genesis-task-server
        image: /genesisapp_master/code_schema/service_repo/genesis_app:latest
        env:
            GENESIS_MODE: TASK_SERVER
            AUTO_HARVEST: TRUE
            OPENAI_HARVESTER_EMBEDDING_MODEL: text-embedding-3-large
            HARVESTER_REFRESH_SECONDS: 120
            RUNNER_ID: snowflake-1
            SNOWFLAKE_SECURE: FALSE
            GENESIS_INTERNAL_DB_SCHEMA: {{app_db_sch}}
            GENESIS_SOURCE: Snowflake
      endpoints:
      - name: udfendpoint
        port: 8080
        public: false
      logExporters:
        eventTableConfig:
          logLevel: INFO
:::)
;

CREATE OR REPLACE PROCEDURE APP.UPGRADE_APP(INSTANCE_NAME VARCHAR, SERVICE_NAME VARCHAR, UPDATE_HARVEST_METADATA BOOLEAN, APP_NAME VARCHAR, EAI_NAME VARCHAR, C_POOL_NAME VARCHAR)
RETURNS VARCHAR NOT NULL
LANGUAGE SQL
AS
:::
DECLARE
    schema_exists BOOLEAN;
    harvest_schema_exists BOOLEAN;
    harvest_excluded BOOLEAN;

BEGIN
    LET WAREHOUSE_NAME := 'NONE';
    
    show warehouses;
    SELECT "name" into WAREHOUSE_NAME FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) limit 1; 

    SELECT COUNT(*) > 0 INTO :schema_exists
    FROM INFORMATION_SCHEMA.SCHEMATA
    WHERE SCHEMA_NAME = :INSTANCE_NAME;

    IF (:schema_exists) then

    REVOKE USAGE ON FUNCTION APP1.deploy_bot(varchar) FROM APPLICATION ROLE APP_PUBLIC;
    
    DROP FUNCTION IF EXISTS APP1.configure_ngrok_token(varchar, varchar, varchar);
    
    REVOKE USAGE ON FUNCTION APP1.configure_slack_app_token(varchar, varchar) FROM APPLICATION ROLE APP_PUBLIC;
    
    REVOKE USAGE ON FUNCTION APP1.configure_llm(varchar, varchar) FROM APPLICATION ROLE APP_PUBLIC;
    
    REVOKE USAGE ON FUNCTION APP1.submit_udf(varchar, varchar, varchar) FROM APPLICATION ROLE APP_PUBLIC;
    
    REVOKE USAGE ON FUNCTION APP1.lookup_udf(varchar, varchar) FROM APPLICATION ROLE APP_PUBLIC;
    
    REVOKE USAGE ON FUNCTION APP1.get_slack_endpoints() FROM APPLICATION ROLE APP_PUBLIC;
    
    REVOKE USAGE ON FUNCTION APP1.list_available_bots() FROM APPLICATION ROLE APP_PUBLIC;
    
    DROP FUNCTION IF EXISTS APP1.get_ngrok_tokens();
    
    REVOKE USAGE ON FUNCTION APP1.get_metadata(varchar) FROM APPLICATION ROLE APP_PUBLIC;
    
      LET spec VARCHAR := (
            SELECT REGEXP_REPLACE(VALUE
              ,'{{app_db_sch}}',lower(current_database())||'.'||lower(:INSTANCE_NAME)) AS VALUE
            FROM APP.YAML WHERE NAME=:SERVICE_NAME);
      EXECUTE IMMEDIATE
        'ALTER SERVICE IF EXISTS '|| :INSTANCE_NAME ||'.'|| :SERVICE_NAME ||
        ' FROM SPECIFICATION  '||chr(36)||chr(36)||'\n'|| :spec ||'\n'||chr(36)||chr(36) ||
        ' ';

      if (WAREHOUSE_NAME is not NULL)
      THEN
            EXECUTE IMMEDIATE
           'CREATE SERVICE IF NOT EXISTS '|| :INSTANCE_NAME ||'.'|| :SERVICE_NAME ||
           ' IN COMPUTE POOL  '|| :C_POOL_NAME ||
           ' FROM SPECIFICATION  '||chr(36)||chr(36)||'\n'|| :spec ||'\n'||chr(36)||chr(36) ||
           ' QUERY_WAREHOUSE = '||:WAREHOUSE_NAME||
           ' EXTERNAL_ACCESS_INTEGRATIONS = ('||:EAI_NAME||')';
         EXECUTE IMMEDIATE
           'GRANT USAGE ON SERVICE '|| :INSTANCE_NAME ||'.'|| :SERVICE_NAME ||' TO APPLICATION ROLE APP_PUBLIC';
         EXECUTE IMMEDIATE
           'GRANT MONITOR ON SERVICE '|| :INSTANCE_NAME ||'.'|| :SERVICE_NAME || ' TO APPLICATION ROLE APP_PUBLIC';
        END IF;

      IF (UPDATE_HARVEST_METADATA) THEN
        -- Check if the APP1.HARVEST_RESULTS table exists and then delete specific rows from harvest_data
        SELECT COUNT(*) > 0 INTO :harvest_schema_exists FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = :INSTANCE_NAME AND TABLE_NAME = 'HARVEST_RESULTS';

        SELECT IFF(COUNT(*)>0, 1, 0) INTO :harvest_excluded
        FROM APP1.HARVEST_CONTROL
        WHERE DATABASE_NAME = :APP_NAME
            AND (ARRAY_CONTAINS('BASEBALL'::variant, SCHEMA_EXCLUSIONS) OR ARRAY_CONTAINS('FORMULA_1'::variant, SCHEMA_EXCLUSIONS)) ;

        IF  (:harvest_schema_exists AND NOT :harvest_excluded) THEN
          EXECUTE IMMEDIATE 'DELETE FROM APP1.HARVEST_RESULTS WHERE DATABASE_NAME = ''' || :APP_NAME || ''' AND SCHEMA_NAME IN (''BASEBALL'', ''FORMULA_1'')';
          EXECUTE IMMEDIATE 'INSERT INTO APP1.HARVEST_RESULTS (SOURCE_NAME, QUALIFIED_TABLE_NAME, DATABASE_NAME, MEMORY_UUID, SCHEMA_NAME, TABLE_NAME, COMPLETE_DESCRIPTION, DDL, DDL_SHORT, DDL_HASH, SUMMARY, SAMPLE_DATA_TEXT, LAST_CRAWLED_TIMESTAMP, CRAWL_STATUS, ROLE_USED_FOR_CRAWL, EMBEDDING)
                              SELECT SOURCE_NAME, replace(QUALIFIED_TABLE_NAME,''APP_NAME'',''' || :APP_NAME || ''') QUALIFIED_TABLE_NAME, ''' || :APP_NAME || ''' DATABASE_NAME, MEMORY_UUID, SCHEMA_NAME, TABLE_NAME, REPLACE(COMPLETE_DESCRIPTION,''APP_NAME'',''' || :APP_NAME || ''') COMPLETE_DESCRIPTION, REPLACE(DDL,''APP_NAME'',''' || :APP_NAME || ''') DDL, REPLACE(DDL_SHORT,''APP_NAME'',''' || :APP_NAME || ''') DDL_SHORT, ''SHARED_VIEW'' DDL_HASH, REPLACE(SUMMARY,''APP_NAME'',''' || :APP_NAME || ''') SUMMARY, SAMPLE_DATA_TEXT, LAST_CRAWLED_TIMESTAMP, CRAWL_STATUS, ROLE_USED_FOR_CRAWL, EMBEDDING 
 FROM SHARED_HARVEST.HARVEST_RESULTS WHERE DATABASE_NAME = ''APP_NAME'' AND SCHEMA_NAME IN (''BASEBALL'', ''FORMULA_1'')';
        END IF;      
      END IF;
    END IF;

END;
:::
;



CREATE OR REPLACE PROCEDURE APP.WAIT_FOR_STARTUP(INSTANCE_NAME VARCHAR, SERVICE_NAME VARCHAR, MAX_WAIT INTEGER)
RETURNS VARCHAR NOT NULL
LANGUAGE SQL
AS
:::
DECLARE
 SERVICE_STATUS VARCHAR DEFAULT 'READY';
 WAIT INTEGER DEFAULT 0;
 result VARCHAR DEFAULT '';
 C1 CURSOR FOR
   select
     v.value:containerName::varchar container_name
     ,v.value:status::varchar status
     ,v.value:message::varchar message
   from (select parse_json(system$get_service_status(?))) t,
   lateral flatten(input => t.$1) v
   order by container_name;
 SERVICE_START_EXCEPTION EXCEPTION (-20002, 'Failed to start Service. ');
BEGIN
 REPEAT
   LET name VARCHAR := INSTANCE_NAME||'.'||SERVICE_NAME;
   OPEN c1 USING (:name);
   service_status := 'READY';
   FOR record IN c1 DO
     IF ((service_status = 'READY') AND (record.status != 'READY')) THEN
        service_status := record.status;
        result := result || '\n' ||lpad(wait,5)||' '|| record.container_name || ' ' || record.status;
     END IF;
   END FOR;
   CLOSE c1;
   wait := wait + 1;
   SELECT SYSTEM$WAIT(1);
 UNTIL ((service_status = 'READY') OR (service_status = 'FAILED' ) OR ((:max_wait-wait) <= 0))          
 END REPEAT;
 IF (service_status != 'READY') THEN
   RAISE SERVICE_START_EXCEPTION;
 END IF;
 RETURN result || '\n' || service_status;
END;
:::
;


CREATE OR REPLACE PROCEDURE APP.CREATE_SERVER_SERVICE(INSTANCE_NAME VARCHAR,SERVICE_NAME VARCHAR, POOL_NAME VARCHAR, EAI_NAME VARCHAR , WAREHOUSE_NAME VARCHAR, APP_DATABASE VARCHAR)
RETURNS VARCHAR NOT NULL
LANGUAGE SQL
AS
:::
    BEGIN
 LET spec VARCHAR := (
      SELECT REGEXP_REPLACE(VALUE
        ,'{{app_db_sch}}',lower(:APP_DATABASE)||'.'||lower(:INSTANCE_NAME)) AS VALUE
      FROM APP.YAML WHERE NAME=:SERVICE_NAME);
 EXECUTE IMMEDIATE
   'CREATE SERVICE '|| :INSTANCE_NAME ||'.'|| :SERVICE_NAME ||
   ' IN COMPUTE POOL  '|| :POOL_NAME ||
   ' FROM SPECIFICATION  '||chr(36)||chr(36)||'\n'|| :spec ||'\n'||chr(36)||chr(36) ||
   ' QUERY_WAREHOUSE = '||:WAREHOUSE_NAME||
   ' EXTERNAL_ACCESS_INTEGRATIONS = ('||:EAI_NAME||')';
 EXECUTE IMMEDIATE
   'GRANT USAGE ON SERVICE '|| :INSTANCE_NAME ||'.'|| :SERVICE_NAME ||' TO APPLICATION ROLE APP_PUBLIC';
 EXECUTE IMMEDIATE
   'GRANT MONITOR ON SERVICE '|| :INSTANCE_NAME ||'.'|| :SERVICE_NAME || ' TO APPLICATION ROLE APP_PUBLIC';

  
 EXECUTE IMMEDIATE
   'CREATE or replace FUNCTION '|| :INSTANCE_NAME ||'.submit_udf (INPUT_TEXT VARCHAR, THREAD_ID VARCHAR, BOT_ID VARCHAR)  RETURNS varchar SERVICE='|| :INSTANCE_NAME ||'.'|| :SERVICE_NAME ||' ENDPOINT=udfendpoint AS '||chr(39)||'/udf_proxy/submit_udf'||chr(39);

  
 EXECUTE IMMEDIATE
   'CREATE or replace FUNCTION '|| :INSTANCE_NAME ||'.lookup_udf (UU VARCHAR, BOT_ID VARCHAR)  RETURNS varchar SERVICE='|| :INSTANCE_NAME ||'.'|| :SERVICE_NAME ||' ENDPOINT=udfendpoint AS '||chr(39)||'/udf_proxy/lookup_udf'||chr(39);

  
 EXECUTE IMMEDIATE
   'CREATE or replace FUNCTION '|| :INSTANCE_NAME ||'.get_slack_endpoints ()  RETURNS varchar SERVICE='|| :INSTANCE_NAME ||'.'|| :SERVICE_NAME ||' ENDPOINT=udfendpoint AS '||chr(39)||'/udf_proxy/get_slack_tokens'||chr(39);


 EXECUTE IMMEDIATE
   'CREATE or replace FUNCTION '|| :INSTANCE_NAME ||'.list_available_bots ()  RETURNS varchar SERVICE='|| :INSTANCE_NAME ||'.'|| :SERVICE_NAME ||' ENDPOINT=udfendpoint AS '||chr(39)||'/udf_proxy/list_available_bots'||chr(39);

 --EXECUTE IMMEDIATE
 --  'CREATE or replace FUNCTION '|| :INSTANCE_NAME ||'.get_ngrok_tokens ()  RETURNS varchar SERVICE='|| :INSTANCE_NAME ||'.'|| :SERVICE_NAME ||' ENDPOINT=udfendpoint AS '||chr(39)||'/udf_proxy/get_ngrok_tokens'||chr(39);

 EXECUTE IMMEDIATE
   'CREATE or replace FUNCTION '|| :INSTANCE_NAME ||'.get_metadata (metadata_type varchar)  RETURNS varchar SERVICE='|| :INSTANCE_NAME ||'.'|| :SERVICE_NAME ||' ENDPOINT=udfendpoint AS '||chr(39)||'/udf_proxy/get_metadata'||chr(39);
 
 EXECUTE IMMEDIATE
   'CREATE or replace FUNCTION '|| :INSTANCE_NAME ||'.configure_llm (llm_type varchar, api_key varchar)  RETURNS varchar SERVICE='|| :INSTANCE_NAME ||'.'|| :SERVICE_NAME ||' ENDPOINT=udfendpoint AS '||chr(39)||'/udf_proxy/configure_llm'||chr(39);

  EXECUTE IMMEDIATE
   'CREATE or replace FUNCTION '|| :INSTANCE_NAME ||'.configure_slack_app_token (token varchar, refresh varchar)  RETURNS varchar SERVICE='|| :INSTANCE_NAME ||'.'|| :SERVICE_NAME ||' ENDPOINT=udfendpoint AS '||chr(39)||'/udf_proxy/configure_slack_app_token'||chr(39);

     EXECUTE IMMEDIATE
   'CREATE or replace FUNCTION '|| :INSTANCE_NAME ||'.configure_ngrok_token (auth_token varchar, use_domain varchar, domain varchar)  RETURNS varchar SERVICE='|| :INSTANCE_NAME ||'.'|| :SERVICE_NAME ||' ENDPOINT=udfendpoint AS '||chr(39)||'/udf_proxy/configure_ngrok_token'||chr(39);

     EXECUTE IMMEDIATE
   'CREATE or replace FUNCTION '|| :INSTANCE_NAME ||'.deploy_bot (bot_id varchar)  RETURNS varchar SERVICE='|| :INSTANCE_NAME ||'.'|| :SERVICE_NAME ||' ENDPOINT=udfendpoint AS '||chr(39)||'/udf_proxy/deploy_bot'||chr(39);
  
-- EXECUTE IMMEDIATE
--   'GRANT USAGE ON FUNCTION '|| :INSTANCE_NAME ||'.deploy_bot ( varchar )  TO APPLICATION ROLE APP_PUBLIC';

 
--EXECUTE IMMEDIATE
--   'GRANT USAGE ON FUNCTION '|| :INSTANCE_NAME ||'.configure_ngrok_token ( varchar, varchar, varchar)  TO APPLICATION ROLE APP_PUBLIC';

 -- EXECUTE IMMEDIATE
 --  'GRANT USAGE ON FUNCTION '|| :INSTANCE_NAME ||'.configure_slack_app_token ( varchar, varchar)  TO APPLICATION ROLE APP_PUBLIC';

 --EXECUTE IMMEDIATE
--   'GRANT USAGE ON FUNCTION '|| :INSTANCE_NAME ||'.configure_llm ( varchar, varchar)  TO APPLICATION ROLE APP_PUBLIC';
-- EXECUTE IMMEDIATE
--   'GRANT USAGE ON FUNCTION '|| :INSTANCE_NAME ||'.submit_udf ( varchar, varchar, varchar)  TO APPLICATION ROLE APP_PUBLIC';
-- EXECUTE IMMEDIATE
--   'GRANT USAGE ON FUNCTION '|| :INSTANCE_NAME ||'.lookup_udf ( varchar, varchar)  TO APPLICATION ROLE APP_PUBLIC';
-- EXECUTE IMMEDIATE
--   'GRANT USAGE ON FUNCTION '|| :INSTANCE_NAME ||'.get_slack_endpoints ( )  TO APPLICATION ROLE APP_PUBLIC';
-- EXECUTE IMMEDIATE
--   'GRANT USAGE ON FUNCTION '|| :INSTANCE_NAME ||'.list_available_bots ( )  TO APPLICATION ROLE APP_PUBLIC';
 --EXECUTE IMMEDIATE
 --  'GRANT USAGE ON FUNCTION '|| :INSTANCE_NAME ||'.get_ngrok_tokens ( )  TO APPLICATION ROLE APP_PUBLIC';
 --EXECUTE IMMEDIATE
 --  'GRANT USAGE ON FUNCTION '|| :INSTANCE_NAME ||'.get_metadata (varchar )  TO APPLICATION ROLE APP_PUBLIC';


 RETURN 'service created';
END
:::
;




CREATE OR REPLACE PROCEDURE APP.CREATE_HARVESTER_SERVICE(INSTANCE_NAME VARCHAR,SERVICE_NAME VARCHAR, POOL_NAME VARCHAR, EAI_NAME VARCHAR , WAREHOUSE_NAME VARCHAR, APP_DATABASE VARCHAR)
RETURNS VARCHAR NOT NULL
LANGUAGE SQL
AS
:::
    BEGIN
 LET spec VARCHAR := (
      SELECT REGEXP_REPLACE(VALUE
        ,'{{app_db_sch}}',lower(:APP_DATABASE)||'.'||lower(:INSTANCE_NAME)) AS VALUE
      FROM APP.YAML WHERE NAME=:SERVICE_NAME);
 EXECUTE IMMEDIATE
   'CREATE SERVICE '|| :INSTANCE_NAME ||'.'|| :SERVICE_NAME ||
   ' IN COMPUTE POOL  '|| :POOL_NAME ||
   ' FROM SPECIFICATION  '||chr(36)||chr(36)||'\n'|| :spec ||'\n'||chr(36)||chr(36) ||
   ' QUERY_WAREHOUSE = '||:WAREHOUSE_NAME||
   ' EXTERNAL_ACCESS_INTEGRATIONS = ('||:EAI_NAME||')';
 EXECUTE IMMEDIATE
   'GRANT USAGE ON SERVICE '|| :INSTANCE_NAME ||'.'|| :SERVICE_NAME ||' TO APPLICATION ROLE APP_PUBLIC';
 EXECUTE IMMEDIATE
   'GRANT MONITOR ON SERVICE '|| :INSTANCE_NAME ||'.'|| :SERVICE_NAME || ' TO APPLICATION ROLE APP_PUBLIC';

 RETURN 'service created';
END
:::
;

CREATE OR REPLACE PROCEDURE APP.CREATE_TASK_SERVICE(INSTANCE_NAME VARCHAR,SERVICE_NAME VARCHAR, POOL_NAME VARCHAR, EAI_NAME VARCHAR , WAREHOUSE_NAME VARCHAR, APP_DATABASE VARCHAR)
RETURNS VARCHAR NOT NULL
LANGUAGE SQL
AS
:::
    BEGIN
 LET spec VARCHAR := (
      SELECT REGEXP_REPLACE(VALUE
        ,'{{app_db_sch}}',lower(:APP_DATABASE)||'.'||lower(:INSTANCE_NAME)) AS VALUE
      FROM APP.YAML WHERE NAME=:SERVICE_NAME);
 EXECUTE IMMEDIATE
   'CREATE SERVICE '|| :INSTANCE_NAME ||'.'|| :SERVICE_NAME ||
   ' IN COMPUTE POOL  '|| :POOL_NAME ||
   ' FROM SPECIFICATION  '||chr(36)||chr(36)||'\n'|| :spec ||'\n'||chr(36)||chr(36) ||
   ' QUERY_WAREHOUSE = '||:WAREHOUSE_NAME||
   ' EXTERNAL_ACCESS_INTEGRATIONS = ('||:EAI_NAME||')';
 EXECUTE IMMEDIATE
   'GRANT USAGE ON SERVICE '|| :INSTANCE_NAME ||'.'|| :SERVICE_NAME ||' TO APPLICATION ROLE APP_PUBLIC';
 EXECUTE IMMEDIATE
   'GRANT MONITOR ON SERVICE '|| :INSTANCE_NAME ||'.'|| :SERVICE_NAME || ' TO APPLICATION ROLE APP_PUBLIC';

 RETURN 'service created';
END
:::
;

CREATE APPLICATION ROLE IF NOT EXISTS APP_PUBLIC;
CREATE OR ALTER VERSIONED SCHEMA CORE;
GRANT USAGE ON SCHEMA CORE TO APPLICATION ROLE APP_PUBLIC;


CREATE OR REPLACE PROCEDURE CORE.INITIALIZE_APP_INSTANCE( INSTANCE_NAME VARCHAR, POOL_NAME VARCHAR, EAI_NAME VARCHAR, APP_WAREHOUSE VARCHAR)
RETURNS STRING
LANGUAGE SQL
AS
:::
DECLARE
    v_current_database STRING;
BEGIN
  SELECT CURRENT_DATABASE() INTO :v_current_database;

  EXECUTE IMMEDIATE 'CREATE SCHEMA '||:INSTANCE_NAME;
  EXECUTE IMMEDIATE 'GRANT USAGE ON SCHEMA '||:INSTANCE_NAME||' TO APPLICATION ROLE APP_PUBLIC';

  EXECUTE IMMEDIATE 'CREATE STAGE IF NOT EXISTS '||:INSTANCE_NAME||'.'||'WORKSPACE DIRECTORY = ( ENABLE = true ) ENCRYPTION = (TYPE = '||CHR(39)||'SNOWFLAKE_SSE'||chr(39)||')';
  EXECUTE IMMEDIATE 'GRANT READ ON STAGE '||:INSTANCE_NAME||'.'||'WORKSPACE TO APPLICATION ROLE APP_PUBLIC';

  CALL APP.CREATE_SERVER_SERVICE(:INSTANCE_NAME,'GENESISAPP_SERVICE_SERVICE',:POOL_NAME, :EAI_NAME, :APP_WAREHOUSE, :v_current_database);
  CALL APP.CREATE_HARVESTER_SERVICE(:INSTANCE_NAME,'GENESISAPP_HARVESTER_SERVICE',:POOL_NAME, :EAI_NAME, :APP_WAREHOUSE, :v_current_database);
  CALL APP.CREATE_TASK_SERVICE(:INSTANCE_NAME,'GENESISAPP_TASK_SERVICE',:POOL_NAME, :EAI_NAME, :APP_WAREHOUSE, :v_current_database);
  CALL APP.WAIT_FOR_STARTUP(:INSTANCE_NAME,'GENESISAPP_SERVICE_SERVICE',600);
  
  RETURN :v_current_database||'.'||:INSTANCE_NAME||'.GENESISAPP_SERVICE_SERVICE';
  
END
:::
;


GRANT USAGE ON PROCEDURE CORE.INITIALIZE_APP_INSTANCE(VARCHAR, VARCHAR, VARCHAR, VARCHAR) TO  APPLICATION ROLE APP_PUBLIC;



CREATE OR REPLACE PROCEDURE CORE.GET_APP_ENDPOINT(INSTANCE_NAME VARCHAR)
RETURNS TABLE(VARCHAR, INTEGER, VARCHAR, VARCHAR, VARCHAR  )
LANGUAGE SQL
AS
:::
BEGIN
 EXECUTE IMMEDIATE 'create or replace table '||:INSTANCE_NAME||'.ENDPOINT (name varchar, port integer, protocol varchar, ingress_enabled varchar, ingress_url varchar)';
 LET stmt VARCHAR := 'SELECT "name" AS SERVICE_NAME, "schema_name" AS SCHEMA_NAME FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))';
 LET RS0 RESULTSET := (EXECUTE IMMEDIATE 'SHOW SERVICES IN SCHEMA '||:INSTANCE_NAME);
 LET RS1 RESULTSET := (EXECUTE IMMEDIATE :stmt);
 LET C1 CURSOR FOR RS1;
 FOR REC IN C1 DO
   LET RS2 RESULTSET := (EXECUTE IMMEDIATE 'SHOW ENDPOINTS IN SERVICE '||rec.schema_name||'.'||rec.service_name);
   EXECUTE IMMEDIATE 'INSERT INTO '||:INSTANCE_NAME||'.ENDPOINT SELECT "name","port","protocol","ingress_enabled","ingress_url" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))';
 END FOR;
 LET RS3 RESULTSET := (EXECUTE IMMEDIATE 'SELECT name, port, protocol, ingress_enabled, ingress_url FROM '||:INSTANCE_NAME||'.ENDPOINT');
 RETURN TABLE(RS3); 
END;
:::
;
GRANT USAGE ON PROCEDURE CORE.GET_APP_ENDPOINT(VARCHAR) TO  APPLICATION ROLE APP_PUBLIC;


CREATE OR REPLACE PROCEDURE CORE.START_APP_INSTANCE(INSTANCE_NAME VARCHAR, POOL_NAME VARCHAR, EAI_NAME VARCHAR, APP_WAREHOUSE VARCHAR)
RETURNS TABLE(SERVICE_NAME VARCHAR,CONTAINER_NAME VARCHAR,STATUS VARCHAR, MESSAGE VARCHAR)
LANGUAGE SQL
AS
:::
BEGIN
 LET x INTEGER := 0;
 LET stmt VARCHAR := 'SELECT "name" as SERVICE_NAME, "schema_name" AS SCHEMA_NAME FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))';
 EXECUTE IMMEDIATE 'SHOW SERVICES IN SCHEMA ' ||:INSTANCE_NAME;
 LET RS1 RESULTSET := (EXECUTE IMMEDIATE :stmt);
 LET c1 CURSOR FOR RS1;
 FOR rec IN c1 DO
   EXECUTE IMMEDIATE 'ALTER SERVICE IF EXISTS '||rec.schema_name||'.'||rec.service_name||' resume';
   EXECUTE IMMEDIATE 'CALL APP.WAIT_FOR_STARTUP(\''||rec.schema_name||'\',\''||rec.service_name||'\',300)';
   x := x + 1;
 END FOR;

 IF (x < 3) THEN
   CALL APP.RECREATE_APP_INSTANCE(:INSTANCE_NAME, :POOL_NAME, :EAI_NAME, :APP_WAREHOUSE);
 END IF;

 LET RS3 RESULTSET := (CALL CORE.LIST_APP_INSTANCE(:INSTANCE_NAME));
 RETURN TABLE(RS3);
END;
:::
;
GRANT USAGE ON PROCEDURE CORE.START_APP_INSTANCE(VARCHAR,VARCHAR,VARCHAR,VARCHAR) TO  APPLICATION ROLE APP_PUBLIC;


CREATE OR REPLACE PROCEDURE CORE.STOP_APP_INSTANCE(INSTANCE_NAME VARCHAR)
RETURNS TABLE(SERVICE_NAME VARCHAR,CONTAINER_NAME VARCHAR,STATUS VARCHAR, MESSAGE VARCHAR)
LANGUAGE SQL
AS
:::
BEGIN
 LET stmt VARCHAR := 'SELECT "name" as SERVICE_NAME, "schema_name" AS SCHEMA_NAME FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))';
 EXECUTE IMMEDIATE 'SHOW SERVICES IN SCHEMA ' ||:INSTANCE_NAME;
 LET RS1 RESULTSET := (EXECUTE IMMEDIATE :stmt);
 LET c1 CURSOR FOR RS1;
 FOR rec IN c1 DO
   EXECUTE IMMEDIATE 'ALTER SERVICE IF EXISTS '||rec.schema_name||'.'||rec.service_name||' suspend';
 END FOR;
 LET RS3 RESULTSET := (CALL CORE.LIST_APP_INSTANCE(:INSTANCE_NAME));
 RETURN TABLE(RS3);
END;
:::
;
GRANT USAGE ON PROCEDURE CORE.STOP_APP_INSTANCE(VARCHAR) TO  APPLICATION ROLE APP_PUBLIC;


CREATE OR REPLACE PROCEDURE CORE.DROP_APP_INSTANCE(INSTANCE_NAME VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
AS
:::
BEGIN
 LET stmt VARCHAR := 'SELECT "name" as SERVICE_NAME, "schema_name" AS SCHEMA_NAME FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))';
 EXECUTE IMMEDIATE 'SHOW SERVICES IN SCHEMA ' ||:INSTANCE_NAME;
 LET RS1 RESULTSET := (EXECUTE IMMEDIATE :stmt);
 LET c1 CURSOR FOR RS1;
 FOR rec IN c1 DO
   EXECUTE IMMEDIATE 'DROP SERVICE IF EXISTS '||rec.schema_name||'.'||rec.service_name;
 END FOR;
 DROP SCHEMA IDENTIFIER(:INSTANCE_NAME);
 RETURN 'The instance with name '||:INSTANCE_NAME||' has been dropped';
END;
:::
;
GRANT USAGE ON PROCEDURE CORE.DROP_APP_INSTANCE(VARCHAR) TO APPLICATION ROLE APP_PUBLIC;


CREATE OR REPLACE PROCEDURE CORE.RESTART_APP_INSTANCE(INSTANCE_NAME VARCHAR)
RETURNS TABLE(SERVICE_NAME VARCHAR,CONTAINER_NAME VARCHAR,STATUS VARCHAR, MESSAGE VARCHAR)
LANGUAGE SQL
AS
:::
BEGIN
 LET stmt VARCHAR := 'SELECT "name" as SERVICE_NAME, "schema_name" AS SCHEMA_NAME FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))';
 EXECUTE IMMEDIATE 'SHOW SERVICES IN SCHEMA ' ||:INSTANCE_NAME;
 LET RS1 RESULTSET := (EXECUTE IMMEDIATE :stmt);
 LET c1 CURSOR FOR RS1;
 FOR rec IN c1 DO
   EXECUTE IMMEDIATE 'ALTER SERVICE IF EXISTS '||rec.schema_name||'.'||rec.service_name||' suspend';
   SELECT SYSTEM$WAIT(5);   
   EXECUTE IMMEDIATE 'ALTER SERVICE IF EXISTS '||rec.schema_name||'.'||rec.service_name||' resume';
   EXECUTE IMMEDIATE 'CALL APP.WAIT_FOR_STARTUP(\''||rec.schema_name||'\',\''||rec.service_name||'\',300)';
 END FOR;
 LET RS3 RESULTSET := (CALL CORE.LIST_APP_INSTANCE(:INSTANCE_NAME));
 RETURN TABLE(RS3);
END;
:::
;
GRANT USAGE ON PROCEDURE CORE.RESTART_APP_INSTANCE(VARCHAR) TO APPLICATION ROLE APP_PUBLIC;

CREATE OR REPLACE PROCEDURE APP.RECREATE_APP_INSTANCE( INSTANCE_NAME VARCHAR, POOL_NAME VARCHAR, EAI_NAME VARCHAR, APP_WAREHOUSE VARCHAR)
RETURNS STRING
LANGUAGE SQL
AS
:::
DECLARE
    v_current_database STRING;
BEGIN
  SELECT CURRENT_DATABASE() INTO :v_current_database;

  CALL APP.CREATE_SERVER_SERVICE(:INSTANCE_NAME,'GENESISAPP_SERVICE_SERVICE',:POOL_NAME, :EAI_NAME, :APP_WAREHOUSE, :v_current_database);
  CALL APP.CREATE_HARVESTER_SERVICE(:INSTANCE_NAME,'GENESISAPP_HARVESTER_SERVICE',:POOL_NAME, :EAI_NAME, :APP_WAREHOUSE, :v_current_database);
  CALL APP.CREATE_TASK_SERVICE(:INSTANCE_NAME,'GENESISAPP_TASK_SERVICE',:POOL_NAME, :EAI_NAME, :APP_WAREHOUSE, :v_current_database);
  CALL APP.WAIT_FOR_STARTUP(:INSTANCE_NAME,'GENESISAPP_SERVICE_SERVICE',600);
  
  RETURN :v_current_database||'.'||:INSTANCE_NAME||'.GENESISAPP_SERVICE_SERVICE';
  
END
:::
;


-- upgrades service service plus will update harvest_results shared metadata
CALL APP.UPGRADE_APP('APP1','GENESISAPP_SERVICE_SERVICE', TRUE, CURRENT_DATABASE(),'GENESIS_EAI','GENESIS_POOL');
-- upgrades harvester services, but will not update harvest results again
CALL APP.UPGRADE_APP('APP1','GENESISAPP_HARVESTER_SERVICE', FALSE, CURRENT_DATABASE(),'GENESIS_EAI','GENESIS_POOL');
-- upgrades or creates if needed task automation services, but will not update harvest results again
CALL APP.UPGRADE_APP('APP1','GENESISAPP_TASK_SERVICE', FALSE, CURRENT_DATABASE(),'GENESIS_EAI','GENESIS_POOL');


CREATE OR REPLACE PROCEDURE CORE.TEST_BILLING_EVENT()
RETURNS VARCHAR
LANGUAGE SQL
AS
:::
BEGIN
 EXECUTE IMMEDIATE 'SELECT SYSTEM$CREATE_BILLING_EVENT(\'TEST_BILL_EVENT\',\'\',CURRENT_TIMESTAMP(),CURRENT_TIMESTAMP(),10,\'\',\'\')';
 RETURN 'BILLED'; 
END;
:::
;


-- GRANT USAGE ON PROCEDURE CORE.TEST_BILLING_EVENT() TO  APPLICATION ROLE APP_PUBLIC;



CREATE OR REPLACE PROCEDURE CORE.LIST_APP_INSTANCE(INSTANCE_NAME VARCHAR)
RETURNS TABLE(SERVICE_NAME VARCHAR,CONTAINER_NAME VARCHAR,STATUS VARCHAR, MESSAGE VARCHAR)
LANGUAGE SQL
AS
:::
BEGIN
 EXECUTE IMMEDIATE 'create or replace table '||:INSTANCE_NAME||'.CONTAINER (service_name varchar, container_name varchar, status varchar, message varchar)';
 LET stmt VARCHAR := 'SELECT "name" AS SERVICE_NAME, "schema_name" AS SCHEMA_NAME FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))';
 LET RS0 RESULTSET := (EXECUTE IMMEDIATE 'SHOW SERVICES IN SCHEMA '||:INSTANCE_NAME);
 LET RS1 RESULTSET := (EXECUTE IMMEDIATE :stmt);
 LET C1 CURSOR FOR RS1;
 FOR REC IN C1 DO
   EXECUTE IMMEDIATE 'INSERT INTO '||:INSTANCE_NAME||'.CONTAINER '||
                     '  SELECT \''||rec.schema_name||'.'||rec.service_name||'\'::varchar service_name'||
                     '         , value:containerName::varchar container_name, value:status::varchar status, value:message::varchar message '||
                     '  FROM TABLE(FLATTEN(PARSE_JSON(SYSTEM$GET_SERVICE_STATUS(\''||rec.schema_name||'.'||rec.service_name||'\'))))'; 
 END FOR;
 LET RS3 RESULTSET := (EXECUTE IMMEDIATE 'SELECT service_name, container_name, status, message FROM '||:INSTANCE_NAME||'.CONTAINER');
 RETURN TABLE(RS3); 
END;
:::
;

GRANT USAGE ON PROCEDURE CORE.LIST_APP_INSTANCE(VARCHAR) TO APPLICATION ROLE APP_PUBLIC;


CREATE OR REPLACE PROCEDURE CORE.GET_POOLS()
RETURNS TABLE(NAME VARCHAR, STATE VARCHAR)
LANGUAGE SQL
AS
:::
BEGIN
 LET stmt VARCHAR := 'SELECT NAME, STATE FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))';
 EXECUTE IMMEDIATE 'SHOW COMPUTE POOLS';
 LET RS1 RESULTSET := (EXECUTE IMMEDIATE :stmt);
 RETURN TABLE(RS1);
END;
:::
;
GRANT USAGE ON PROCEDURE CORE.GET_POOLS() TO APPLICATION ROLE APP_PUBLIC;



CREATE OR REPLACE STREAMLIT CORE.SIS_LAUNCH
    FROM '/code_artifacts/streamlit'
    MAIN_FILE = '/sis_launch.py';
 

GRANT USAGE ON STREAMLIT CORE.SIS_LAUNCH TO APPLICATION ROLE app_public;

CREATE OR REPLACE PROCEDURE CORE.RUN_ARBITRARY(sql_query VARCHAR)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
:::
    // Prepare a statement using the provided SQL query
    var statement = snowflake.createStatement({sqlText: SQL_QUERY});
    
    // Execute the statement
    var result_set = statement.execute();
    
    // Initialize an array to hold each row's data
    var rows = [];
    
    // Iterate over each row in the result set
    while (result_set.next()) {
        // Initialize an object to store the current row's data
        var row = {};
        
        // Iterate over each column in the current row
        for (var colIdx = 1; colIdx <= result_set.getColumnCount(); colIdx++) {
            // Get the column name and value
            var columnName = result_set.getColumnName(colIdx);
            var columnValue = result_set.getColumnValue(colIdx);
            
            // Add the column name and value to the current row's object
            row[columnName] = columnValue;
        }
        
        // Add the current row's object to the rows array
        rows.push(row);
    }
    
    // Convert the rows array to a JSON string
    var jsonResult = JSON.stringify(rows);
    
    // Return the JSON string
    // Note: Snowflake automatically converts the returned string to a VARIANT (JSON) data type
    return JSON.parse(jsonResult);
:::;


 
-- GRANT USAGE ON PROCEDURE CORE.RUN_ARBITRARY(VARCHAR) TO APPLICATION ROLE app_public;




$$,':::','$$') VALUE;


USE SCHEMA GENESISAPP_APP_PKG_EXT.CODE_SCHEMA;
-- DELETE FROM SCRIPT;
INSERT INTO SCRIPT SELECT * FROM SCRIPT_TMP;

--delete from script;
-- INSERT INTO SCRIPT SELECT * FROM SCRIPT_TMP;


-- ########## SCRIPTS CONTENT  ###########################################


-- ########## BEGIN REPO PERMISSIONS  ####################################


USE SCHEMA GENESISAPP_APP_PKG_EXT.CODE_SCHEMA;


-- ########## END REPO PERMISSIONS  ######################################


-- ########## BEGIN UPLOAD FILES TO APP STAGE ############################


rm @app_code_stage;


CALL CODE_SCHEMA.PUT_TO_STAGE('APP_CODE_STAGE','manifest.yml',(SELECT VALUE FROM CODE_SCHEMA.SCRIPT WHERE NAME = 'MANIFEST'));
CALL CODE_SCHEMA.GET_FROM_STAGE('APP_CODE_STAGE','manifest.yml');
CALL CODE_SCHEMA.PUT_TO_STAGE('APP_CODE_STAGE','setup_script.sql', (SELECT VALUE FROM CODE_SCHEMA.SCRIPT WHERE NAME = 'SETUP'));
CALL CODE_SCHEMA.GET_FROM_STAGE('APP_CODE_STAGE','setup_script.sql');
CALL CODE_SCHEMA.PUT_TO_STAGE('APP_CODE_STAGE','readme.md', (SELECT VALUE FROM CODE_SCHEMA.SCRIPT WHERE NAME = 'README'));
CALL CODE_SCHEMA.GET_FROM_STAGE('APP_CODE_STAGE','readme.md');
CALL CODE_SCHEMA.PUT_TO_STAGE_SUBDIR('APP_CODE_STAGE','code_artifacts/streamlit','sis_launch.py', (SELECT VALUE FROM CODE_SCHEMA.SCRIPT WHERE NAME = 'SIS_APP'));
CALL CODE_SCHEMA.GET_FROM_STAGE_SUBDIR('APP_CODE_STAGE','code_artifacts/streamlit','sis_launch.py');
CALL CODE_SCHEMA.PUT_TO_STAGE_SUBDIR('APP_CODE_STAGE','code_artifacts/streamlit','environment.yml', (SELECT VALUE FROM CODE_SCHEMA.SCRIPT WHERE NAME = 'SIS_ENV'));
CALL CODE_SCHEMA.GET_FROM_STAGE_SUBDIR('APP_CODE_STAGE','code_artifacts/streamlit','environment.yml');


ls @APP_CODE_STAGE;


-- ########## END UPLOAD FILES TO APP STAGE ##############################


-- ########## BEGIN CREATE RELEASE / PATCH  ##############################


BEGIN
LET rs0 RESULTSET := (EXECUTE IMMEDIATE 'ALTER APPLICATION PACKAGE GENESISAPP_APP_PKG_EXT ADD VERSION V0_1 USING @GENESISAPP_APP_PKG_EXT.CODE_SCHEMA.APP_CODE_STAGE');
RETURN TABLE(rs0);
EXCEPTION
 WHEN OTHER THEN
   LET rs1 RESULTSET := (EXECUTE IMMEDIATE 'ALTER APPLICATION PACKAGE GENESISAPP_APP_PKG_EXT ADD PATCH FOR VERSION V0_1 USING @GENESISAPP_APP_PKG_EXT.CODE_SCHEMA.APP_CODE_STAGE');
   RETURN TABLE(rs1);
END;
;


-- ########## END CREATE RELEASE / PATCH  ################################




// provider test

select current_role();

DROP APPLICATION IF EXISTS GENESIS_BOTS_ALPHA;
show applications;

SET APP_DATABASE='GENESIS_BOTS_ALPHA';

CREATE APPLICATION GENESIS_BOTS_ALPHA FROM APPLICATION PACKAGE GENESISAPP_APP_PKG_EXT USING VERSION V0_1;

call GENESISAPP_APP.core.get_eai();
// to get streamlit up and running

// none?

// settings

set APP_DATABASE = 'GENESIS_BOTS_ALPHA';
use database IDENTIFIER($APP_DATABASE);
set APP_INSTANCE='APP1'; -- Do not change
set APP_COMPUTE_POOL='APP_COMPUTE_POOL'||$APP_INSTANCE;
set APP_INSTANCE_FAMILY='CPU_X64_XS';
set APP_LOCAL_DB='APP_LOCAL_DB'; -- For now, do not change, Secrets are hard-wired to this Database in YAML
set APP_LOCAL_SCHEMA=$APP_LOCAL_DB||'.'||'EGRESS'; -- For now, do not change, Secrets are hard-wired to this Schema in YAML
set APP_LOCAL_EGRESS_RULE=$APP_LOCAL_SCHEMA||'.'||'APP_RULE';
set APP_LOCAL_EAI = $APP_DATABASE||'_EAI';
set APP_WAREHOUSE = 'XSMALL'; -- change to an existing Warehouse if desired

// compute pool

DROP COMPUTE POOL IF EXISTS IDENTIFIER($APP_COMPUTE_POOL);
CREATE COMPUTE POOL IF NOT EXISTS IDENTIFIER($APP_COMPUTE_POOL) FOR APPLICATION IDENTIFIER($APP_DATABASE)
 MIN_NODES=1
 MAX_NODES=1
 INSTANCE_FAMILY='CPU_X64_XS';
show compute pools;

// network egress for openai, ngrok, slack
 
CREATE OR REPLACE NETWORK RULE IDENTIFIER($APP_LOCAL_EGRESS_RULE)
 MODE = EGRESS
 TYPE = HOST_PORT
VALUE_LIST = ('api.openai.com', 'slack.com', 'www.slack.com', 'wss-primary.slack.com',
'wss-backup.slack.com',  'wss-primary.slack.com:443','wss-backup.slack.com:443');

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION IDENTIFIER($APP_LOCAL_EAI)
   ALLOWED_NETWORK_RULES = (APP_LOCAL_DB.EGRESS.APP_RULE)  -- update from above if necessary
   ENABLED = true;

// grants

GRANT USAGE ON DATABASE IDENTIFIER($APP_LOCAL_DB) TO APPLICATION IDENTIFIER($APP_DATABASE);
GRANT USAGE ON SCHEMA IDENTIFIER($APP_LOCAL_SCHEMA) TO APPLICATION IDENTIFIER($APP_DATABASE);
GRANT USAGE ON INTEGRATION IDENTIFIER($APP_LOCAL_EAI) TO APPLICATION   IDENTIFIER($APP_DATABASE);
GRANT USAGE ON COMPUTE POOL  GENESIS_TEST_POOL TO APPLICATION  IDENTIFIER($APP_DATABASE);
GRANT BIND SERVICE ENDPOINT ON ACCOUNT TO APPLICATION  IDENTIFIER($APP_DATABASE);
GRANT USAGE ON WAREHOUSE  IDENTIFIER($APP_WAREHOUSE) TO APPLICATION  IDENTIFIER($APP_DATABASE);

// start

USE DATABASE IDENTIFIER($APP_DATABASE);
CALL CORE.INITIALIZE_APP_INSTANCE($APP_INSTANCE,'GENESIS_TEST_POOL',$APP_LOCAL_EAI,$APP_WAREHOUSE);
CALL CORE.DROP_APP_INSTANCE($APP_INSTANCE);

// check service

USE DATABASE IDENTIFIER($APP_DATABASE);
SELECT SYSTEM$GET_SERVICE_STATUS('GENESISAPP_SERVICE_SERVICE');
SELECT SYSTEM$GET_SERVICE_LOGS('GENESISAPP_SERVICE_SERVICE',0,'genesis',1000);



/////////////

   
/// consumer instructions

d
SELECT CURRENT_ROLE(); -- Use the same role that installed the Application
-- USE ROLE ACCOUNTADMIN; -- Use the same role that installed the Application
-- USE DATABASE CEB_TEST;


set OPENAI_API_KEY = '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';
set NGROK_AUTHTOKEN = '2ce4bWGvzt5lBCDn6c2WsymnVSr_3m7QssHXhUHLi1BVCguRN';

set APP_DATABASE = 'GENESISAPP_APP';
use database IDENTIFIER($APP_DATABASE);
set APP_INSTANCE='APP1'; -- Do not change
set APP_COMPUTE_POOL='APP_COMPUTE_POOL'||$APP_INSTANCE;
set APP_INSTANCE_FAMILY='CPU_X64_XS';
set APP_LOCAL_DB='APP_LOCAL_DB'; -- For now, do not change, Secrets are hard-wired to this Database in YAML
set APP_LOCAL_SCHEMA=$APP_LOCAL_DB||'.'||'EGRESS'; -- For now, do not change, Secrets are hard-wired to this Schema in YAML
set APP_LOCAL_EGRESS_RULE=$APP_LOCAL_SCHEMA||'.'||'APP_RULE';
set OPENAI_SECRET_NAME=$APP_LOCAL_SCHEMA||'.'||'OPENAI_API_KEY';  -- Do not change
set NGROK_SECRET_NAME=$APP_LOCAL_SCHEMA||'.'||'NGROK_AUTHTOKEN';  -- Do not change
set APP_LOCAL_EAI = $APP_DATABASE||'_EAI';
set EXAMPLE_DATA_DB = 'MY_DATA';
set EXAMPLE_DATA_SCHEMA=$EXAMPLE_DATA_DB||'.'||'EXAMPLE';
set EXAMPLE_DATA_TABLE=$EXAMPLE_DATA_SCHEMA||'.'||'CUSTOMERS';
set APP_WAREHOUSE = 'XSMALL'; -- change to an existing Warehouse if desired


CREATE WAREHOUSE IF NOT EXISTS IDENTIFIER($APP_WAREHOUSE)
 WITH WAREHOUSE_SIZE = 'XSMALL'
 AUTO_SUSPEND = 60
 AUTO_RESUME = TRUE
 INITIALLY_SUSPENDED = TRUE;


USE WAREHOUSE IDENTIFIER($APP_WAREHOUSE);




DROP COMPUTE POOL IF EXISTS IDENTIFIER($APP_COMPUTE_POOL);
CREATE COMPUTE POOL IF NOT EXISTS IDENTIFIER($APP_COMPUTE_POOL) FOR APPLICATION IDENTIFIER($APP_DATABASE)
 MIN_NODES=1
 MAX_NODES=1
 INSTANCE_FAMILY='CPU_X64_XS';

describe compute pool IDENTIFIER($APP_COMPUTE_POOL);

CREATE DATABASE IF NOT EXISTS IDENTIFIER($APP_LOCAL_DB);
CREATE SCHEMA IF NOT EXISTS IDENTIFIER($APP_LOCAL_SCHEMA);

CREATE OR REPLACE NETWORK RULE IDENTIFIER($APP_LOCAL_EGRESS_RULE)
 MODE = EGRESS
 TYPE = HOST_PORT
  VALUE_LIST = ('api.openai.com', 'connect.ngrok-agent.com:443', 'slack.com', 'api.slack.com')
  
//('0.0.0.0:443','0.0.0.0:80');

  
CREATE OR REPLACE SECRET IDENTIFIER($OPENAI_SECRET_NAME)
 TYPE = GENERIC_STRING
 SECRET_STRING = $OPENAI_API_KEY;


CREATE OR REPLACE SECRET IDENTIFIER($NGROK_SECRET_NAME)
 TYPE = GENERIC_STRING
 SECRET_STRING = $NGROK_AUTHTOKEN;


SELECT $APP_LOCAL_EGRESS_RULE, $OPENAI_SECRET_NAME, $NGROK_SECRET_NAME; -- update below CREATE statement if necessary
 CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION IDENTIFIER($APP_LOCAL_EAI)
   ALLOWED_NETWORK_RULES = (APP_LOCAL_DB.EGRESS.APP_RULE, APP_LOCAL_DB.EGRESS.APP_INGRES_RULE)  -- update from above if necessary
   ALLOWED_AUTHENTICATION_SECRETS = (APP_LOCAL_DB.EGRESS.OPENAI_API_KEY, APP_LOCAL_DB.EGRESS.NGROK_AUTHTOKEN) -- update from above if necessary
   ENABLED = true;



CREATE DATABASE IF NOT EXISTS IDENTIFIER($EXAMPLE_DATA_DB);
CREATE SCHEMA IF NOT EXISTS IDENTIFIER($EXAMPLE_DATA_SCHEMA);



CREATE OR REPLACE TABLE IDENTIFIER($EXAMPLE_DATA_TABLE) (
   customer_id INTEGER,
   first_name VARCHAR,
   last_name VARCHAR,
   email VARCHAR,
   signup_date DATE,
   is_active BOOLEAN,
   city VARCHAR,
   state VARCHAR,
   customer_segment VARCHAR
);


SELECT $APP_WAREHOUSE;
USE WAREHOUSE IDENTIFIER($APP_WAREHOUSE);


INSERT INTO IDENTIFIER($EXAMPLE_DATA_TABLE) (customer_id, first_name, last_name, email, signup_date, is_active, city, state, customer_segment) VALUES
(1, 'John', 'Doe', 'john.doe@example.com', '2023-01-01', TRUE, 'New York', 'NY', 'Premium'),
(2, 'Jane', 'Doe', 'jane.doe@example.com', '2023-02-01', TRUE, 'Los Angeles', 'CA', 'Standard'),
(3, 'Jim', 'Beam', 'jim.beam@example.com', '2023-03-01', FALSE, 'Chicago', 'IL', 'Standard'),
(4, 'Jack', 'Daniels', 'jack.daniels@example.com', '2023-01-15', TRUE, 'Houston', 'TX', 'Premium'),
(5, 'Jill', 'Hill', 'jill.hill@example.com', '2023-02-15', FALSE, 'Phoenix', 'AZ', 'Standard');


GRANT USAGE ON DATABASE IDENTIFIER($EXAMPLE_DATA_DB) TO APPLICATION IDENTIFIER($APP_DATABASE);
GRANT USAGE ON SCHEMA IDENTIFIER($EXAMPLE_DATA_SCHEMA) TO APPLICATION IDENTIFIER($APP_DATABASE);
GRANT SELECT ON ALL TABLES IN SCHEMA IDENTIFIER($EXAMPLE_DATA_SCHEMA) TO APPLICATION IDENTIFIER($APP_DATABASE);
GRANT SELECT ON ALL VIEWS IN SCHEMA IDENTIFIER($EXAMPLE_DATA_SCHEMA) TO APPLICATION IDENTIFIER($APP_DATABASE);
GRANT USAGE ON DATABASE IDENTIFIER($APP_LOCAL_DB) TO APPLICATION IDENTIFIER($APP_DATABASE);
GRANT USAGE ON SCHEMA IDENTIFIER($APP_LOCAL_SCHEMA) TO APPLICATION IDENTIFIER($APP_DATABASE);
GRANT USAGE ON INTEGRATION IDENTIFIER($APP_LOCAL_EAI) TO APPLICATION   IDENTIFIER($APP_DATABASE);
GRANT READ ON SECRET  IDENTIFIER($OPENAI_SECRET_NAME) TO APPLICATION  IDENTIFIER($APP_DATABASE);
GRANT READ ON SECRET IDENTIFIER($NGROK_SECRET_NAME) TO APPLICATION   IDENTIFIER($APP_DATABASE);
GRANT USAGE ON COMPUTE POOL  IDENTIFIER($APP_COMPUTE_POOL) TO APPLICATION  IDENTIFIER($APP_DATABASE);
GRANT BIND SERVICE ENDPOINT ON ACCOUNT TO ROLE ACCOUNTADMIN WITH GRANT OPTION;
GRANT BIND SERVICE ENDPOINT ON ACCOUNT TO APPLICATION  IDENTIFIER($APP_DATABASE);
GRANT USAGE ON WAREHOUSE  IDENTIFIER($APP_WAREHOUSE) TO APPLICATION  IDENTIFIER($APP_DATABASE);

--set TMP_INTERNAL_DB = 'GENESIS_TEST';
--set TMP_INTERNAL_SCH = 'GENESIS_TEST.GENESIS_INTERNAL';
--GRANT USAGE ON DATABASE IDENTIFIER($TMP_INTERNAL_DB) TO APPLICATION IDENTIFIER($APP_DATABASE);
--GRANT USAGE ON SCHEMA IDENTIFIER($TMP_INTERNAL_SCH) TO APPLICATION IDENTIFIER($APP_DATABASE);
--GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA IDENTIFIER($TMP_INTERNAL_SCH) TO APPLICATION IDENTIFIER($APP_DATABASE);
--GRANT SELECT, INSERT, UPDATE, DELETE ON ALL VIEWS IN SCHEMA IDENTIFIER($TMP_INTERNAL_SCH) TO APPLICATION IDENTIFIER($APP_DATABASE);
set TMP_SPIDER_DB = 'SPIDER_DATA';
set TMP_INTERNAL_SCH = 'SPIDER_DATA.GENESIS_INTERNAL';
GRANT USAGE ON DATABASE IDENTIFIER($TMP_INTERNAL_DB) TO APPLICATION IDENTIFIER($APP_DATABASE);
GRANT USAGE ON SCHEMA IDENTIFIER($TMP_INTERNAL_SCH) TO APPLICATION IDENTIFIER($APP_DATABASE);
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA IDENTIFIER($TMP_INTERNAL_SCH) TO APPLICATION IDENTIFIER($APP_DATABASE);
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL VIEWS IN SCHEMA IDENTIFIER($TMP_INTERNAL_SCH) TO APPLICATION IDENTIFIER($APP_DATABASE);

use schema IDENTIFIER($APP_LOCAL_SCHEMA);


CREATE OR REPLACE PROCEDURE grant_select_on_database_to_app(database_name STRING, APP_NAME STRING)
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    var connection = snowflake.createStatement({
        sqlText: `SELECT SCHEMA_NAME FROM ${DATABASE_NAME}.INFORMATION_SCHEMA.SCHEMATA`
    });
    var result = connection.execute();
    
    while (result.next()) {
        var schemaName = result.getColumnValue(1);
        if (schemaName === 'INFORMATION_SCHEMA') {
            continue;
        }
        var sqlCommands = [
            `GRANT USAGE ON DATABASE ${DATABASE_NAME} TO APPLICATION ${APP_NAME}`,
            `GRANT USAGE ON SCHEMA ${DATABASE_NAME}.${schemaName} TO APPLICATION ${APP_NAME}`,
            `GRANT SELECT ON ALL TABLES IN SCHEMA ${DATABASE_NAME}.${schemaName} TO APPLICATION ${APP_NAME}`,
            `GRANT SELECT ON ALL VIEWS IN SCHEMA ${DATABASE_NAME}.${schemaName} TO APPLICATION ${APP_NAME}`,
        ];
        
        for (var i = 0; i < sqlCommands.length; i++) {
            try {
                var stmt = snowflake.createStatement({sqlText: sqlCommands[i]});
                stmt.execute();
            } catch(err) {
                // Return error message if any command fails
                return `Error executing command: ${sqlCommands[i]} - ${err.message}`;
            }
        }
    }
    
    return "Successfully granted USAGE and SELECT on all schemas, tables, and views in database " + DATABASE_NAME + " to application " + APP_NAME;
$$;

 
call grant  _select_on_database_to_app('SPIDER_DATA',$APP_DATABASE);

revoke usage on database spider_data from application identifier($APP_DATABASE);


select current_role();



USE DATABASE IDENTIFIER($APP_DATABASE);


CALL CORE.INITIALIZE_APP_INSTANCE($APP_INSTANCE,$APP_COMPUTE_POOL,$APP_LOCAL_EAI,$APP_WAREHOUSE);
--CALL CORE.INITIALIZE_APP_INSTANCE_TEST($APP_INSTANCE,$APP_COMPUTE_POOL,$APP_LOCAL_EAI,$APP_WAREHOUSE);

-- call core.start_app_instance($APP_INSTANCE);
-- call core.stop_app_instance($APP_INSTANCE);
-- call core.drop_app_instance($APP_INSTANCE);
-- call core.list_app_instance($APP_INSTANCE);
-- call core.restart_app_instance($APP_INSTANCE);
-- call core.get_app_endpoint($APP_INSTANCE);

show services;
show compute pools;
drop compute pool GENESIS_TEST_POOL;

use role accountadmin;
show compute pools;

select current_version();


DROP COMPUTE POOL IDENTIFIER($APP_COMPUTE_POOL);
ALTER COMPUTE POOL GENESIS_TEST_POOL STOP ALL;

drop compute pool APP_COMPUTE_POOLAPP1;

select app1.get_slack_endpoints();

select current_schema();

CREATE or replace FUNCTION app_local_db.public.get_slack_endpoints ()
  RETURNS varchar
  SERVICE=genesisapp_app.app1.GENESISAPP_SERVICE_SERVICE
  ENDPOINT=udfendpoint
  AS '/udf_proxy/get_slack_tokens';

CREATE or replace FUNCTION app_test_schema.get_slack_endpoints ()
  RETURNS varchar
  SERVICE=genesis_server
  ENDPOINT=udfendpoint
  AS '/udf_proxy/get_slack_tokens';

CREATE or replace FUNCTION app_test_schema.list_available_bots()
  RETURNS varchar
  SERVICE=genesis_server
  ENDPOINT=udfendpoint
  AS '/udf_proxy/list_available_bots';

select submit_udf('hi how are you?','111','jl-local-eve-test-1');
select lookup_udf('6c04a3b6-ccc3-417a-b9c1-cb9c6b6dff40','jl-local-eve-test-1');


CREATE or replace FUNCTION app_test_schema.get_ngrok_tokens()
  RETURNS varchar
  SERVICE=genesis_server
  ENDPOINT=udfendpoint
  AS '/udf_proxy/get_ngrok_tokens';


CREATE or replace FUNCTION app_test_schema.get_metadata(metadata_type varchar)
  RETURNS varchar
  SERVICE=genesis_server
  ENDPOINT=udfendpoint
  AS '/udf_proxy/get_metadata';
  
CREATE or replace FUNCTION g_healthcheck ()
  RETURNS varchar
  SERVICE=genesis_server
  ENDPOINT=udfendpoint
  AS '/healthcheck';

select get_slack_endpoints();


select SYSTEM$ALLOWLIST();

use schema app1;
show tables;

describe service GENESISAPP_SERVICE_SERVICE;
SELECT SYSTEM$GET_SERVICE_STATUS('GENESISAPP_SERVICE_SERVICE');
SELECT SYSTEM$GET_SERVICE_LOGS('GENESISAPP_SERVICE_SERVICE',0,'genesis',1000);



////
// manual install (like cybersyn ai utilities)
////


DROP APPLICATION IF EXISTS GENESIS_BOTS;


SET APP_DATABASE='GENESISAPP_APP';


CREATE APPLICATION GENESIS_BOTS FROM APPLICATION PACKAGE GENESISAPP_APP_PKG_EXT;


create or replace database genesisapp_local_db;


CREATE OR REPLACE NETWORK RULE genesisapp_local_db.public.GENESISAPP_RULE
 MODE = EGRESS
 TYPE = HOST_PORT
   VALUE_LIST = ('0.0.0.0:443','0.0.0.0:80');
// VALUE_LIST = ('api.openai.com', 'connect.ngrok-agent.com:443');


CREATE OR REPLACE SECRET genesisapp_local_db.public.OPENAI_API_KEY
 TYPE = GENERIC_STRING
 SECRET_STRING = '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';


CREATE OR REPLACE SECRET genesisapp_local_db.public.NGROK_AUTHTOKEN
 TYPE = GENERIC_STRING
 SECRET_STRING = '2ce4bWGvzt5lBCDn6c2WsymnVSr_3m7QssHXhUHLi1BVCguRN';


CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION GENESISAPP_EAI
   ALLOWED_NETWORK_RULES = (genesisapp_local_db.public.GENESISAPP_RULE)
   ALLOWED_AUTHENTICATION_SECRETS = (genesisapp_local_db.public.OPENAI_API_KEY, genesisapp_local_db.public.NGROK_AUTHTOKEN)
   ENABLED = true;


GRANT USAGE ON DATABASE genesisapp_local_db TO APPLICATION GENESISAPP_APP;
GRANT USAGE ON SCHEMA genesisapp_local_db.public TO APPLICATION GENESISAPP_APP;
GRANT USAGE ON INTEGRATION GENESISAPP_EAI TO APPLICATION GENESISAPP_APP;
GRANT READ ON SECRET genesisapp_local_db.public.OPENAI_API_KEY TO APPLICATION GENESISAPP_APP;
GRANT READ ON SECRET genesisapp_local_db.public.NGROK_AUTHTOKEN TO APPLICATION GENESISAPP_APP;
--GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO APPLICATION GENESISAPP_APP;


GRANT SELECT ON ALL TABLES IN SCHEMA genesisapp_local_db.public TO APPLICATION GENESISAPP_APP;
GRANT SELECT ON ALL VIEWS IN SCHEMA genesisapp_local_db.public TO APPLICATION GENESISAPP_APP;

GRANT USAGE ON WAREHOUSE APP_WH TO APPLICATION GENESISAPP_APP;
--GRANT BIND SERVICE ENDPOINT ON ACCOUNT TO APPLICATION GENESISAPP_APP;


create table genesisapp_local_db.public.tables_t as select * from information_schema.tables;
create table genesisapp_local_db.public.test as select 'hi' as hello;
create view genesisapp_local_db.public.tables_v as select * from genesisapp_local_db.information_schema.tables;
select table_catalog, table_schema, table_name from genesisapp_local_db.public.tables_v;
GRANT SELECT ON ALL TABLES IN SCHEMA genesisapp_local_db.public TO APPLICATION GENESISAPP_APP;
GRANT SELECT ON ALL VIEWS IN SCHEMA genesisapp_local_db.public TO APPLICATION GENESISAPP_APP;


grant usage on database JUSTIN to application genesisapp_app;
grant usage on schema JUSTIN.public to application genesisapp_app;
GRANT SELECT ON ALL TABLES IN SCHEMA JUSTIN.public TO APPLICATION GENESISAPP_APP;
GRANT SELECT ON ALL VIEWS IN SCHEMA JUSTIN.public TO APPLICATION GENESISAPP_APP;




show databases;
use role accountadmin;


// grant another DB, see if it sees it






use role test_role_2;
grant role test_role_2 to user justin;
select * from genesisapp_local_db.tables_v;
use database genesisapp_local_db;
use schema public;
show views;
select * from tables_v;
use role accountadmin;
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO APPLICATION GENESISAPP_APP;


grant usage on warehouse app_wh to role test_role_2;
use role accountadmin;
grant role test_role_2 to application genesisapp_app;


select table_name from genesisapp_local_db.information_schema.tables;




create materialized view genesisapp_local_db.public.tables_mv as
select 1 as one;


create secure view genesisapp_local_db.public.tables_sv as select * from information_schema.tables;
create table genesisapp_local_db.public.tables_t as select * from information_schema.tables;


create table genesisapp_local_db.public.test as select 'hi' as hello;


select * from genesisapp_local_db.public.test;


select get_ddl('table','genesisapp_local_db.public.test');
describe table genesisapp_local_db.public.tables_t;


// SQL show databases;
// SQL select get_ddl(\'table\',\'genesisapp_local_db.public.test\');
// SQL describe table genesisapp_local_db.public.tables_t;




select table_schema, table_name from genesisapp_local_db.public.tables_t;
create secure view genesisapp_local_db.public.tables_sv as select * from information_schema.tables;
show databases;




select count(*) from genesisapp_local_db.public.tables_t;
select count(*) from genesisapp_local_db.information_schema.tables;






select table_name from genesisapp_local_db.information_schema.tables;


select 'hello' HI;




use database genesisapp_app;
use role accountadmin;
show compute pools;



call genesisapp_app.core.drop_app_instance('APP1');
show compute pools;


// next read secret inside app setup for the container start



-- ########## BEGIN CREATE/PATCH TEST APP   ##############################
DECLARE
 APP_DATABASE := 'GENESISAPP_APP';
 APP_COMPUTE_POOL VARCHAR DEFAULT $APP_COMPUTE_POOL;
 APP_INSTANCE VARCHAR DEFAULT 'APP1';


 APP_LOCAL_DB := (:APP_DATABASE||'_LOCAL_DB')::VARCHAR;
 APP_LOCAL_SCHEMA := (:APP_LOCAL_DB||'.'||'EGRESS')::VARCHAR;
 APP_LOCAL_EGRESS_RULE := (:APP_LOCAL_SCHEMA||'.'||'APP_RULE')::VARCHAR;
 APP_LOCAL_EAI := (:APP_DATABASE||'_EAI')::VARCHAR;
BEGIN
 BEGIN
   CREATE APPLICATION GENESISAPP_APP FROM APPLICATION PACKAGE GENESISAPP_APP_PKG_EXT USING VERSION V0_1;
 EXCEPTION
   WHEN OTHER THEN
     BEGIN
       ALTER APPLICATION GENESISAPP_APP UPGRADE USING VERSION V0_1;
       BEGIN
         CALL GENESISAPP_APP.CORE.DROP_APP_INSTANCE(:APP_INSTANCE);
       EXCEPTION
         WHEN OTHER THEN
           NULL;
       END;
     EXCEPTION
       WHEN OTHER THEN
         DROP APPLICATION IF EXISTS GENESISAPP_APP;
         CREATE APPLICATION GENESISAPP_APP FROM APPLICATION PACKAGE GENESISAPP_APP_PKG_EXT USING VERSION V0_1;
     END;
 END;


 CREATE DATABASE IF NOT EXISTS IDENTIFIER(:APP_LOCAL_DB);
 CREATE SCHEMA IF NOT EXISTS IDENTIFIER(:APP_LOCAL_SCHEMA);
    
 CREATE NETWORK RULE IF NOT EXISTS IDENTIFIER(:APP_LOCAL_EGRESS_RULE)
   TYPE = 'HOST_PORT'
   MODE= 'EGRESS'
   VALUE_LIST = ('0.0.0.0:443','0.0.0.0:80');
  
 CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION IDENTIFIER(:APP_LOCAL_EAI)
   ALLOWED_NETWORK_RULES = (GENESISAPP_APP_LOCAL_DB.EGRESS.APP_RULE)
   ENABLED = true;


 GRANT USAGE ON DATABASE IDENTIFIER(:APP_LOCAL_DB) TO APPLICATION IDENTIFIER(:APP_DATABASE);
 GRANT USAGE ON SCHEMA IDENTIFIER(:APP_LOCAL_SCHEMA) TO APPLICATION IDENTIFIER(:APP_DATABASE);
 GRANT USAGE ON NETWORK RULE IDENTIFIER(:APP_LOCAL_EGRESS_RULE) TO APPLICATION IDENTIFIER(:APP_DATABASE);


 GRANT USAGE ON INTEGRATION IDENTIFIER(:APP_LOCAL_EAI) TO APPLICATION IDENTIFIER(:APP_DATABASE);
 GRANT USAGE ON COMPUTE POOL IDENTIFIER(:APP_COMPUTE_POOL) TO APPLICATION IDENTIFIER(:APP_DATABASE);
 GRANT BIND SERVICE ENDPOINT ON ACCOUNT TO APPLICATION IDENTIFIER(:APP_DATABASE);


 GRANT USAGE ON COMPUTE POOL IDENTIFIER(:APP_COMPUTE_POOL) TO APPLICATION IDENTIFIER(:APP_DATABASE);


 USE DATABASE IDENTIFIER(:APP_DATABASE);
 LET RS1 RESULTSET := (CALL GENESISAPP_APP.CORE.INITIALIZE_APP_INSTANCE(:APP_INSTANCE,:APP_COMPUTE_POOL, :APP_LOCAL_EAI)); 
 RETURN TABLE(rs1);
END;


use database genesisapp_app;


drop application genesisapp_app;




select genesisapp_app.app1.submit_udf('hi', '123');
select genesisapp_app.app1.response_udf('10d12f42-fa6c-4807-9948-91a6ae8b9986');


call genesisapp_app.CORE.TEST_BILLING_EVENT();




call genesisapp_app.core.start_app_instance('APP1');


DESCRIBE SERVICE GENESISAPP_APP.APP1.GENESISAPP_SERVICE_SERVICE;
SELECT SYSTEM$GET_SERVICE_LOGS('GENESISAPP_APP.APP1.GENESISAPP_SERVICE_SERVICE',0,'echo',100);
SHOW ENDPOINTS IN SERVICE GENESISAPP_APP.APP1.GENESISAPP_SERVICE_SERVICE;


show streamlits;
desc streamlit genesisapp_app.core.SIS_LAUNCH;


// edcyofwr-sfengineering-ss-lprpr-test1.snowflakecomputing.app


call genesisapp_app.core.stop_app_instance('APP1');
call genesisapp_app.core.drop_app_instance('APP1');
call genesisapp_app.core.restart_app_instance('APP1');
-- call core.list_app_instance('APP1');
call genesisapp_app.core.get_app_endpoint('APP1');


-- ########## END CREATE TEST APP   ######################################


-- ##### BEGIN CREATE/PATCH TEST APP (DO NOT REBUILD THE APP)  ###########


DECLARE
 APP_INSTANCE VARCHAR DEFAULT 'APP1';
BEGIN
 ALTER APPLICATION GENESISAPP_APP UPGRADE USING VERSION V0_1;
 CALL GENESISAPP_APP.CORE.RESTART_APP_INSTANCE(:APP_INSTANCE);
 LET rs1 RESULTSET := (CALL GENESISAPP_APP.CORE.GET_APP_ENDPOINT(:APP_INSTANCE));
 RETURN TABLE(rs1);
END;



ALTER APPLICATION GENESISAPP_APP UPGRADE USING VERSION V0_1;
call genesisapp_app.core.restart_app_instance('APP1');


-- ########## END CREATE TEST APP   ######################################




-- ########## BEGIN PUBLISH   ############################################


show versions in application package GENESISAPP_APP_PKG_EXT;

CREATE APPLICATION GENESIS_BOTS FROM APPLICATION PACKAGE GENESISAPP_APP_PKG_EXT USING version V0_1 patch 32;

call GENESIS_BOTS.CORE.DROP_APP_INSTANCE('APP1');

call GENESIS_BOTS.CORE.INITIALIZE_APP_INSTANCE('APP1','GENESIS_POOL','GENESIS_EAI','XSMALL');

// v132 -> .32

ALTER APPLICATION PACKAGE GENESISAPP_APP_PKG_EXT
  SET DISTRIBUTION = $APP_DISTRIBUTION;

select $APP_DISTRIBUTION;
  

DECLARE
 max_patch VARCHAR;
BEGIN
 show versions in application package GENESISAPP_APP_PKG_EXT;
 select max("patch") INTO :max_patch FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) where "version" = 'V0_1';
 LET rs RESULTSET := (EXECUTE IMMEDIATE 'ALTER APPLICATION PACKAGE GENESISAPP_APP_PKG_EXT SET DEFAULT RELEASE DIRECTIVE VERSION = V0_1 PATCH = '||:max_patch);
 RETURN TABLE(rs);
END;


-- ########## END PUBLISH   ##############################################


