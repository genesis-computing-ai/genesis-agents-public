-- To get started use a text editor and replace the following tags (incl. < and >)
--   <APP_NAME> to your application name (use case sensitive change). The app name can be anything you like (assuming it's a valid identifier)
--   <app_name> to your application name (use case sensitive change; This name must be lower case and the same name as above)
--   <SERVICE_NAME> to your service name
--   <INSTANCE_FAMILY> to your instance_family
--   <ROLE_NAME> to your role name (note: the role creation is commented out by default. You will need role ACCOUTADMIN to run it or you need to ask somebody with ACCOUNTADMIN to create and asign the roles; ; using ACCOUNTADMIN to be the application owner is discouraged for security concerns)
--   <USER_NAME> to your role name (note: the role creation is commented out by default. You will need role ACCOUTADMIN to run it or you need to ask somebody with ACCOUNTADMIN to create the roles; using ACCOUNTADMIN to be the application owner is discouraged for security concerns; create either both roles on provider & consumer, or just the respective role)
--   <WAREHOUSE> to your warehouse
--   <DISTRIBUTION> to ['INTERNAL'|'EXTERNAL']. While developing the app, use 'INTERNAL' since it bypasses scanning of the containers (which can be time consuming)
--   <container:tag> to your container:tag
--   By default, several sections will not be executed. Uncomment section if needed
--      Create Role
--      Create Warehouse / Compute pool
--      Publish the App
--   run the first section INITIALIZATION only (!!!) This means to execute the code until you see END INITIALIZATION. This will create the structure (incl. the repo called SERVICE_REPO) so you can upload your containers
--   Update the service yaml with your service yaml
--   Upload your container to <your repo url>/<app_name>_app_pkg/code_schema/image_repo
--   Run the full script again. If the script run successfully, you will have an instance of you application called APP1. Otherwise, fix the errors.

-- ########## BEGIN ROLES (OPTIONAL)  ######################################
/*

USE ROLE ACCOUNTADMIN;

CREATE ROLE <ROLE_NAME>_PROVIDER_ROLE;
GRANT CREATE DATABASE ON ACCOUNT TO ROLE <ROLE_NAME>_PROVIDER_ROLE;
GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE <ROLE_NAME>_PROVIDER_ROLE;
GRANT CREATE COMPUTE POOL ON ACCOUNT TO ROLE <ROLE_NAME>_PROVIDER_ROLE WITH GRANT OPTION;
GRANT CREATE INTEGRATION ON ACCOUNT TO ROLE <ROLE_NAME>_PROVIDER_ROLE;
GRANT CREATE APPLICATION PACKAGE ON ACCOUNT TO ROLE <ROLE_NAME>_PROVIDER_ROLE;
GRANT CREATE APPLICATION  ON ACCOUNT TO ROLE <ROLE_NAME>_PROVIDER_ROLE ;
GRANT CREATE DATA EXCHANGE LISTING  ON ACCOUNT TO ROLE <ROLE_NAME>_PROVIDER_ROLE;
GRANT IMPORT SHARE ON ACCOUNT TO ROLE <ROLE_NAME>_PROVIDER_ROLE;
GRANT CREATE SHARE ON ACCOUNT TO ROLE <ROLE_NAME>_PROVIDER_ROLE;
GRANT MANAGE EVENT SHARING ON ACCOUNT TO ROLE <ROLE_NAME>_PROVIDER_ROLE;
GRANT CREATE DATA EXCHANGE LISTING ON ACCOUNT TO  ROLE <ROLE_NAME>_PROVIDER_ROLE;
GRANT BIND SERVICE ENDPOINT ON ACCOUNT TO ROLE <ROLE_NAME>_PROVIDER_ROLE WITH GRANT OPTION;

GRANT ROLE <ROLE_NAME>_PROVIDER_ROLE to USER <USER_NAME>;

CREATE ROLE <ROLE_NAME>_CONSUMER_ROLE;
GRANT CREATE DATABASE ON ACCOUNT TO ROLE <ROLE_NAME>_CONSUMER_ROLE;
GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE <ROLE_NAME>_CONSUMER_ROLE;
GRANT CREATE COMPUTE POOL ON ACCOUNT TO ROLE <ROLE_NAME>_CONSUMER_ROLE WITH GRANT OPTION;
GRANT CREATE INTEGRATION ON ACCOUNT TO ROLE <ROLE_NAME>_CONSUMER_ROLE;
GRANT CREATE APPLICATION  ON ACCOUNT TO ROLE <ROLE_NAME>_CONSUMER_ROLE ;
GRANT IMPORT SHARE ON ACCOUNT TO ROLE <ROLE_NAME>_CONSUMER_ROLE;
GRANT CREATE SHARE ON ACCOUNT TO ROLE <ROLE_NAME>_CONSUMER_ROLE;
GRANT MANAGE EVENT SHARING ON ACCOUNT TO ROLE <ROLE_NAME>_CONSUMER_ROLE;
GRANT BIND SERVICE ENDPOINT ON ACCOUNT TO ROLE <ROLE_NAME>_CONSUMER_ROLE WITH GRANT OPTION;

GRANT ROLE <ROLE_NAME>_CONSUMER_ROLE to USER <USER_NAME>;

*/
-- ########## END ROLES (OPTIONAL)  ######################################


-- ########## BEGIN ENVIRONMENT  ######################################

SET APP_OWNER_ROLE = '<ROLE_NAME>_PROVIDER_ROLE';
SET APP_WAREHOUSE = '<WAREHOUSE>'||'_WAREHOUSE';
SET APP_DISTRIBUTION = '<DISTRIBUTION>';

-- ########## END   ENVIRONMENT  ######################################

-- ########## BEGIN INITIALIZATION  ######################################

USE ROLE identifier($APP_OWNER_ROLE);

-- DROP DATABASE IF EXISTS <APP_NAME>_APP_PKG ;

/*
CREATE WAREHOUSE IF NOT EXISTS IDENTIFIER($APP_WAREHOUSE)
  MIN_CLUSTER_COUNT=1
  MAX_CLUSTER_COUNT=1
  WAREHOUSE_SIZE=XSMALL
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = FALSE
  AUTO_SUSPEND = 60;

CREATE COMPUTE POOL IF NOT EXISTS IDENTIFIER($APP_COMPUTE_POOL)
  MIN_NODES=1
  MAX_NODES=1
  INSTANCE_FAMILY=<INSTANCE_FAMILY>
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = FALSE
  AUTO_SUSPEND_SECS = 600;
*/

USE WAREHOUSE identifier($APP_WAREHOUSE);

CREATE DATABASE IF NOT EXISTS <APP_NAME>_MASTER;
USE DATABASE <APP_NAME>_MASTER;
CREATE SCHEMA IF NOT EXISTS CODE_SCHEMA;
USE SCHEMA CODE_SCHEMA;
CREATE IMAGE REPOSITORY IF NOT EXISTS SERVICE_REPO;

CREATE APPLICATION PACKAGE IF NOT EXISTS <APP_NAME>_APP_PKG;

USE DATABASE <APP_NAME>_APP_PKG;
CREATE SCHEMA IF NOT EXISTS CODE_SCHEMA;
CREATE STAGE IF NOT EXISTS APP_CODE_STAGE;

USE DATABASE <APP_NAME>_MASTER;
SHOW IMAGE REPOSITORIES IN SCHEMA CODE_SCHEMA;

-- ##########  END INITIALIZATION   ######################################

--
-- STOP HERE AND UPLOAD ALL REQUIRED CONTAINERS INTO THE IMAGE REPO
--

-- ########## UTILITY FUNCTIONS  #########################################
USE SCHEMA <APP_NAME>_APP_PKG.CODE_SCHEMA;

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

-- ########## SCRIPTS CONTENT  ###########################################
USE SCHEMA <APP_NAME>_APP_PKG.CODE_SCHEMA;
CREATE OR REPLACE TABLE SCRIPT (NAME VARCHAR, VALUE VARCHAR);
DELETE FROM SCRIPT;

INSERT INTO SCRIPT (NAME , VALUE)
VALUES ('MANIFEST',
$$
manifest_version: 1

artifacts:
  setup_script: setup_script.sql
  readme: readme.md
  container_services:
    images:
    - /<app_name>_master/code_schema/service_repo/<container:tag>
  default_streamlit: core.configure

configuration:
    grant_callback: app_admin.activate

lifecycle_callbacks:
    version_initializer: app_admin.version_init

privileges:
  - BIND SERVICE ENDPOINT:
      description: "a service can serve requests from public endpoint"
  - CREATE COMPUTE POOL:
      description: "Enable appplication to create its own compute pool(s)"

references:
  - ALL_ACCESS_EAI:
      label: "All Access Integration"
      description: "EAI for Egress from NA+SPCS"
      privileges: [USAGE]
      object_type: EXTERNAL_ACCESS_INTEGRATION
      register_callback: app_admin.register_single_callback
      configuration_callback: app_admin.get_configuration
      required_at_setup: true
$$)
;

INSERT INTO SCRIPT (NAME , VALUE)
VALUES ('README',
$$
# SPCS Demo Template

```
set APP_INSTANCE='<NAME>';
set APP_DATABASE=current_database();

-- call core.initialize_app_instance($APP_INSTANCE);
-- call core.start_app_instance($APP_INSTANCE);
-- call core.stop_app_instance($APP_INSTANCE);
-- call core.drop_app_instance($APP_INSTANCE);
-- call core.list_app_instance($APP_INSTANCE);
-- call core.restart_app_instance($APP_INSTANCE);
-- call core.get_app_endpoint($APP_INSTANCE);
-- call core.app_cmd('<cmd>');
```
$$);

INSERT INTO SCRIPT (NAME , VALUE)
VALUES ('APP_CONFIG_UI',
$$
import streamlit as st
from streamlit.logger import get_logger

LOGGER = get_logger(__name__)

def run():

    st.set_page_config(
        page_title="Hello",
        layout="wide"
    )

    st.markdown("""
    # Welcome to SPCS Demo App!
    1. Initialization: Use the Initialization tab to initialize (create) your app.
    1. Maintenance: Use the maitenance tab to start operations like creating or dropping an application instance.
    1. Debug: Use the debug tab to run any sql command within the context of the native app.
    """)

if __name__ == "__main__":
    run()
$$);

INSERT INTO SCRIPT (NAME , VALUE)
VALUES ('APP_INIT',
$$
import streamlit as st
import time
from streamlit.logger import get_logger
from snowflake.snowpark.context import get_active_session

LOGGER = get_logger(__name__)

session = get_active_session()

st.write("Enter your instance name in the input field below and hit *enter*")

option = st.selectbox(
     'Pick an operation',
     ('',
      'initialize_app_instance',
     ))

instance_name=st.text_input("Application Instance Name")

if instance_name and option:
    with st.spinner('Running the command ...'):
        time.sleep(1)
        try:
            df=session.sql("""call core.%s('%s')""" % (option,instance_name)).collect()
            st.success('Command %s for Instance %s has completed successfully' % (option,instance_name))
            st.dataframe(df)
        except Exception as e:
            st.error(str(e))

$$);

INSERT INTO SCRIPT (NAME , VALUE)
VALUES ('APP_MAINTENANCE',
$$
import streamlit as st
import time
from streamlit.logger import get_logger
from snowflake.snowpark.context import get_active_session

LOGGER = get_logger(__name__)

session = get_active_session()

st.write("Enter your instance name in the input field below and hit *enter*")

option = st.selectbox(
     'Pick an operation',
     ('',
      'start_app_instance',
      'stop_app_instance',
      'drop_app_instance',
      'list_app_instance',
      'restart_app_instance',
      'get_app_endpoint',
     ))

instance_name=st.text_input("Application Instance Name")

if instance_name and option:
    with st.spinner('Running the command ...'):
        time.sleep(1)
        try:
            df=session.sql("""call core.%s('%s')""" % (option,instance_name)).collect()
            st.success('Command %s for Instance %s has completed successfully' % (option,instance_name))
            st.dataframe(df)
        except Exception as e:
            st.error(str(e))

$$);

INSERT INTO SCRIPT (NAME , VALUE)
VALUES ('APP_DEBUG',
$$
import streamlit as st
import time
from streamlit.logger import get_logger
from snowflake.snowpark.context import get_active_session

LOGGER = get_logger(__name__)

session = get_active_session()

option = st.selectbox(
     'Pick an operation',
     ('',
      'app_cmd',
     ))

cmd=st.text_input("Command to run inside the Native App")

if cmd and option:
    with st.spinner('Running the command ...'):
        time.sleep(1)
        try:
            df=session.sql("""call core.%s('%s')""" % (option,cmd)).collect()
            st.success('Command %s has completed successfully' % (cmd))
            st.json(df)
        except Exception as e:
            st.error(str(e))

$$);

CREATE OR REPLACE TEMPORARY TABLE script_tmp AS SELECT 'SETUP' NAME,REGEXP_REPLACE($$
CREATE OR ALTER VERSIONED SCHEMA APP;

CREATE OR REPLACE TABLE APP.YAML (name varchar, value varchar);

INSERT INTO APP.YAML (NAME , VALUE)
VALUES ('<SERVICE_NAME>_SERVICE',
:::
spec:
  container:
  - name: demo
    image: /<app_name>_master/code_schema/service_repo/<container:tag>
    args:
    - {{ttyd_port}}
    - {{streamlit_port}}
    volumeMounts:
    - name: workspace
      mountPath: /home/snowpark/notebooks/workspace
  endpoint:
  - name: ttyd
    port: {{ttyd_port}}
    public: true
  - name: streamlit
    port: {{streamlit_port}}
    public: true
  volume:
  - name: workspace
    source: "@{{instance_name}}.workspace"
    gid: 1000
    uid: 1000
:::);

CREATE OR REPLACE PROCEDURE APP.CREATE_COMPUTE_POOL( POOL_NAME VARCHAR)
RETURNS STRING
LANGUAGE SQL
AS
:::
  BEGIN
    CREATE COMPUTE POOL IF NOT EXISTS IDENTIFIER(:POOL_NAME)
      MIN_NODES=1
      MAX_NODES=1
      INSTANCE_FAMILY=<INSTANCE_FAMILY>
      AUTO_RESUME = TRUE
      INITIALLY_SUSPENDED = FALSE
      AUTO_SUSPEND_SECS = 60;

    LET stmt VARCHAR := 'GRANT OPERATE ON COMPUTE POOL '||:POOL_NAME||' TO APPLICATION ROLE APP_PUBLIC';
    EXECUTE IMMEDIATE stmt;

    RETURN 'compute pool creation complete';
  END
:::
;

CREATE OR REPLACE PROCEDURE APP.EXPAND_YAML(INSTANCE_NAME VARCHAR,SERVICE_NAME VARCHAR )
RETURNS VARCHAR NOT NULL
LANGUAGE SQL
AS
:::
BEGIN
  LET spec VARCHAR := (SELECT REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(VALUE
         ,'{{instance_name}}',lower(:INSTANCE_NAME))
         ,'{{ttyd_port}}','1234')
         ,'{{streamlit_port}}','1235') AS VALUE
    FROM APP.YAML WHERE NAME=:SERVICE_NAME);

  RETURN :spec;
END
:::
;

CREATE OR REPLACE PROCEDURE APP.CREATE_SERVICE(INSTANCE_NAME VARCHAR,SERVICE_NAME VARCHAR, POOL_NAME VARCHAR, EAI_NAME VARCHAR )
RETURNS VARCHAR NOT NULL
LANGUAGE SQL
AS
:::
DECLARE
  spec VARCHAR := '';
BEGIN
  CALL APP.EXPAND_YAML(:INSTANCE_NAME, :SERVICE_NAME);
  SELECT $1 INTO :spec FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

  EXECUTE IMMEDIATE
    'CREATE SERVICE '|| :INSTANCE_NAME ||'.'|| :SERVICE_NAME ||
    ' IN COMPUTE POOL  '|| :POOL_NAME ||
    ' FROM SPECIFICATION '||chr(36)||chr(36)||'\n'|| :spec ||'\n'||chr(36)||chr(36) ||
    ' EXTERNAL_ACCESS_INTEGRATIONS = ('||:EAI_NAME||')'
  ;

  EXECUTE IMMEDIATE
    'GRANT USAGE ON SERVICE '|| :INSTANCE_NAME ||'.'|| :SERVICE_NAME ||' TO APPLICATION ROLE APP_PUBLIC';
  EXECUTE IMMEDIATE
    'GRANT MONITOR ON SERVICE '|| :INSTANCE_NAME ||'.'|| :SERVICE_NAME || ' TO APPLICATION ROLE APP_PUBLIC';
  RETURN 'service created';
END
:::
;

CREATE OR REPLACE PROCEDURE APP.SERVICE_UPGRADE()
RETURNS VARCHAR NOT NULL
LANGUAGE SQL
AS
:::
BEGIN
  LET stmt0 VARCHAR := 'SELECT "name" AS INSTANCE_NAME FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) WHERE NOT "name" in (\'INFORMATION_SCHEMA\',\'APP\', \'CORE\',\'V1\')';
  EXECUTE IMMEDIATE 'SHOW SCHEMAS';
  LET rs0 RESULTSET := (EXECUTE IMMEDIATE :stmt0);
  LET c0 CURSOR FOR rs0;
  FOR rec0 IN c0 DO
    LET stmt1 VARCHAR := 'SELECT "name" as SERVICE_NAME FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))';
    EXECUTE IMMEDIATE 'SHOW SERVICES IN SCHEMA ' ||rec0.instance_name;
    LET rs1 RESULTSET := (EXECUTE IMMEDIATE :stmt1);
    LET c1 CURSOR FOR rs1;
    FOR rec1 IN c1 DO
      LET stmt2 VARCHAR := 'CALL APP.ALTER_SERVICE(\''||rec0.instance_name||'\',\''||rec1.service_name||'\')';
      EXECUTE IMMEDIATE stmt2;
    END FOR;
  END FOR;
  RETURN 'upgrade complete';
END;
:::
;

CREATE OR REPLACE PROCEDURE APP.STREAMLIT_UPGRADE()
RETURNS VARCHAR NOT NULL
LANGUAGE SQL
AS
:::
BEGIN
  CREATE OR REPLACE STREAMLIT CORE.CONFIGURE
     FROM '/code_artifacts/streamlit'
     MAIN_FILE = '/APP_Configuration_UI.py';
  GRANT USAGE ON STREAMLIT CORE.CONFIGURE TO APPLICATION ROLE app_public;
  RETURN 'streamlit upgrade complete';
END;
:::
;

CREATE OR REPLACE PROCEDURE APP.ALTER_SERVICE(INSTANCE_NAME VARCHAR, SERVICE_NAME VARCHAR)
RETURNS VARCHAR NOT NULL
LANGUAGE SQL
AS
:::
DECLARE
  spec VARCHAR := '';
BEGIN

  CALL APP.EXPAND_YAML(:INSTANCE_NAME, :SERVICE_NAME);
  SELECT $1 INTO :spec FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

  EXECUTE IMMEDIATE
     'ALTER SERVICE '|| :INSTANCE_NAME ||'.'|| :SERVICE_NAME ||
     ' FROM SPECIFICATION '||chr(36)||chr(36)||'\n'|| :spec ||'\n'||chr(36)||chr(36)
  ;
  RETURN 'service updated';
END;
:::
;

CREATE OR REPLACE PROCEDURE APP.WAIT_FOR_STARTUP(INSTANCE_NAME VARCHAR, SERVICE_NAME VARCHAR, MAX_WAIT INTEGER)
RETURNS VARCHAR NOT NULL
LANGUAGE SQL
AS
:::
  BEGIN
    LET stmt VARCHAR := 'SELECT SYSTEM$GET_SERVICE_STATUS(\''||:INSTANCE_NAME||'.'||:SERVICE_NAME||'\','||:MAX_WAIT||')';
    EXECUTE IMMEDIATE stmt;
    RETURN stmt;
  END;
:::
;

CREATE OR ALTER VERSIONED SCHEMA APP_ADMIN;
CREATE APPLICATION ROLE IF NOT EXISTS APP_ADMIN;
GRANT USAGE ON SCHEMA APP_ADMIN TO APPLICATION ROLE APP_ADMIN;

CREATE OR REPLACE PROCEDURE APP_ADMIN.REGISTER_SINGLE_CALLBACK(ref_name STRING, operation STRING, ref_or_alias STRING)
 RETURNS STRING
 LANGUAGE SQL
 AS
 :::
   BEGIN
      CASE (operation)
         WHEN 'ADD' THEN
            SELECT system$set_reference(:ref_name, :ref_or_alias);
         WHEN 'REMOVE' THEN
            SELECT system$remove_reference(:ref_name);
         WHEN 'CLEAR' THEN
            SELECT system$remove_reference(:ref_name);
         ELSE
            RETURN 'Unknown operation: ' || operation;
      END CASE;
      RETURN 'Operation ' || operation || ' succeeds.';
   END;
 :::
 ;
GRANT USAGE ON PROCEDURE APP_ADMIN.REGISTER_SINGLE_CALLBACK(STRING,STRING,STRING) TO APPLICATION ROLE APP_ADMIN;

CREATE OR REPLACE PROCEDURE APP_ADMIN.GET_CONFIGURATION(ref_name STRING)
RETURNS STRING
LANGUAGE SQL
AS
:::
BEGIN
  CASE (UPPER(ref_name))
      WHEN 'ALL_ACCESS_EAI' THEN
          RETURN OBJECT_CONSTRUCT(
              'type', 'CONFIGURATION',
              'payload', OBJECT_CONSTRUCT(
                  'host_ports', ARRAY_CONSTRUCT('0.0.0.0:443','0.0.0.0:80'),
                  'allowed_secrets', 'NONE')
          )::STRING;
      ELSE
          RETURN '';
  END CASE;
END;
:::
;
GRANT USAGE ON PROCEDURE APP_ADMIN.GET_CONFIGURATION(STRING) TO APPLICATION ROLE APP_ADMIN;


CREATE OR REPLACE PROCEDURE APP_ADMIN.VERSION_INIT()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS OWNER
AS
:::
  BEGIN
    CALL APP.SERVICE_UPGRADE();
    CALL APP.STREAMLIT_UPGRADE();
    RETURN 'version init complete';
  END
:::
;
GRANT USAGE ON PROCEDURE APP_ADMIN.VERSION_INIT() TO APPLICATION ROLE APP_ADMIN;

CREATE OR REPLACE PROCEDURE APP_ADMIN.ACTIVATE(privileges array)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS OWNER
AS
:::
  BEGIN
    RETURN 'Activation complete';
  END
:::
;
GRANT USAGE ON PROCEDURE APP_ADMIN.ACTIVATE(array) TO APPLICATION ROLE APP_ADMIN;

CREATE OR ALTER VERSIONED SCHEMA CORE;
CREATE APPLICATION ROLE IF NOT EXISTS APP_PUBLIC;
GRANT USAGE ON SCHEMA CORE TO APPLICATION ROLE APP_PUBLIC;

CREATE OR REPLACE PROCEDURE CORE.INITIALIZE_APP_INSTANCE( INSTANCE_NAME VARCHAR)
RETURNS TABLE(VARCHAR, INTEGER, VARCHAR, VARCHAR, VARCHAR  )
LANGUAGE SQL
AS
:::
BEGIN
  EXECUTE IMMEDIATE 'CREATE SCHEMA '||:INSTANCE_NAME;
  EXECUTE IMMEDIATE 'GRANT USAGE ON SCHEMA '||:INSTANCE_NAME||' TO APPLICATION ROLE APP_PUBLIC';

  EXECUTE IMMEDIATE 'CREATE STAGE IF NOT EXISTS '||:INSTANCE_NAME||'.'||'WORKSPACE DIRECTORY = ( ENABLE = true ) ENCRYPTION = (TYPE = '||CHR(39)||'SNOWFLAKE_SSE'||chr(39)||')';
  EXECUTE IMMEDIATE 'GRANT READ ON STAGE '||:INSTANCE_NAME||'.'||'WORKSPACE TO APPLICATION ROLE APP_PUBLIC';

  LET compute_pool_name VARCHAR := :INSTANCE_NAME||'_<APP_NAME>_COMPUTE_POOL';
  CALL APP.CREATE_COMPUTE_POOL(:compute_pool_name);

  CALL APP.CREATE_SERVICE(:INSTANCE_NAME,'<SERVICE_NAME>_SERVICE',:compute_pool_name, 'REFERENCE(\'ALL_ACCESS_EAI\')');
  CALL APP.WAIT_FOR_STARTUP(:INSTANCE_NAME,'<SERVICE_NAME>_SERVICE',600);

  EXECUTE IMMEDIATE 'GRANT SERVICE ROLE '||:INSTANCE_NAME||'.<SERVICE_NAME>_SERVICE!ALL_ENDPOINTS_USAGE TO APPLICATION ROLE APP_PUBLIC';
  EXECUTE IMMEDIATE 'GRANT SERVICE ROLE '||:INSTANCE_NAME||'.<SERVICE_NAME>_SERVICE!ALL_ENDPOINTS_USAGE TO APPLICATION ROLE APP_ADMIN';

  --
  -- this is needed because get_service_status does not wait until the public endpoint is ready
  --
  SELECT SYSTEM$WAIT(120);
  --
  -- this is needed because get_service_status does not wait until the public endpoint is ready
  --


  LET RS1 RESULTSET := (CALL CORE.GET_APP_ENDPOINT(:INSTANCE_NAME));
  RETURN TABLE(rs1);
END
:::
;

GRANT USAGE ON PROCEDURE CORE.INITIALIZE_APP_INSTANCE(VARCHAR) TO  APPLICATION ROLE APP_PUBLIC;

CREATE OR REPLACE PROCEDURE CORE.GET_APP_ENDPOINT(INSTANCE_NAME VARCHAR)
RETURNS TABLE(VARCHAR, INTEGER, VARCHAR, VARCHAR, VARCHAR  )
LANGUAGE SQL
AS
:::
BEGIN
  EXECUTE IMMEDIATE 'create or replace table '||:INSTANCE_NAME||'.ENDPOINT (name varchar, port integer, protocol varchar, is_public varchar, ingress_url varchar)';
  LET stmt VARCHAR := 'SELECT "name" AS SERVICE_NAME, "schema_name" AS SCHEMA_NAME FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))';
  LET RS0 RESULTSET := (EXECUTE IMMEDIATE 'SHOW SERVICES IN SCHEMA '||:INSTANCE_NAME);
  LET RS1 RESULTSET := (EXECUTE IMMEDIATE :stmt);
  LET C1 CURSOR FOR RS1;
  FOR REC IN C1 DO
    LET RS2 RESULTSET := (EXECUTE IMMEDIATE 'SHOW ENDPOINTS IN SERVICE '||rec.schema_name||'.'||rec.service_name);
    EXECUTE IMMEDIATE 'INSERT INTO '||:INSTANCE_NAME||'.ENDPOINT SELECT "name","port","protocol","is_public","ingress_url" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))';
  END FOR;
  LET RS3 RESULTSET := (EXECUTE IMMEDIATE 'SELECT name, port, protocol, is_public, ingress_url FROM '||:INSTANCE_NAME||'.ENDPOINT');
  RETURN TABLE(RS3);
END;
:::
;
GRANT USAGE ON PROCEDURE CORE.GET_APP_ENDPOINT(VARCHAR) TO  APPLICATION ROLE APP_PUBLIC;

CREATE OR REPLACE PROCEDURE CORE.START_APP_INSTANCE(INSTANCE_NAME VARCHAR)
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
    EXECUTE IMMEDIATE 'ALTER SERVICE IF EXISTS '||rec.schema_name||'.'||rec.service_name||' resume';
    EXECUTE IMMEDIATE 'CALL APP.WAIT_FOR_STARTUP(\''||rec.schema_name||'\',\''||rec.service_name||'\',300)';
  END FOR;
  LET RS3 RESULTSET := (CALL CORE.LIST_APP_INSTANCE(:INSTANCE_NAME));
  RETURN TABLE(RS3);
END;
:::
;
GRANT USAGE ON PROCEDURE CORE.START_APP_INSTANCE(VARCHAR) TO  APPLICATION ROLE APP_PUBLIC;

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
  EXECUTE IMMEDIATE 'DROP COMPUTE POOL '||:INSTANCE_NAME||'_<APP_NAME>_COMPUTE_POOL';
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
GRANT USAGE ON PROCEDURE CORE.LIST_APP_INSTANCE(VARCHAR) TO  APPLICATION ROLE APP_PUBLIC;

CREATE OR REPLACE PROCEDURE CORE.SCALE_COMPUTE_POOL( INSTANCE_NAME VARCHAR, MIN_NODES INTEGER, MAX_NODES INTEGER)
RETURNS VARCHAR
LANGUAGE SQL
AS
:::
BEGIN
  LET stmt VARCHAR := 'ALTER COMPUTE POOL '||:INSTANCE_NAME||'_COMPUTE_POOL'||' SET MIN_NODES='||:MIN_NODES||' MAX_NODES='||:MAX_NODES;
  EXECUTE IMMEDIATE :stmt;
  RETURN 'Compute pool '||:INSTANCE_NAME||'_COMPUTE_POOL'||': MIN_NODES='||:MIN_NODES||' MAX_NODES='||:MAX_NODES;
END;
:::
;
GRANT USAGE ON PROCEDURE CORE.SCALE_COMPUTE_POOL(VARCHAR,INTEGER,INTEGER) TO APPLICATION ROLE APP_PUBLIC;

CREATE OR REPLACE PROCEDURE CORE.APP_CMD( CMD VARCHAR)
RETURNS TABLE(VARIANT)
LANGUAGE SQL
AS
:::
BEGIN
  LET RS0 RESULTSET := (EXECUTE IMMEDIATE :CMD);
  LET RS1 RESULTSET := (EXECUTE IMMEDIATE 'SELECT PARSE_JSON((\'[\'||LISTAGG(OBJECT_CONSTRUCT( * )::varchar,\',\')||\']\')::VARIANT) RESULT FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))');
  RETURN TABLE(RS1);
END;
:::
;
GRANT USAGE ON PROCEDURE CORE.APP_CMD(VARCHAR) TO APPLICATION ROLE APP_PUBLIC;

$$,':::','$$') VALUE;

INSERT INTO SCRIPT SELECT * FROM SCRIPT_TMP;

-- ########## SCRIPTS CONTENT  ###########################################



-- ########## BEGIN REPO PERMISSIONS  ####################################

USE SCHEMA <APP_NAME>_APP_PKG.CODE_SCHEMA;

-- ########## END REPO PERMISSIONS  ######################################

-- ########## BEGIN UPLOAD FILES TO APP STAGE ############################

rm @app_code_stage;

CALL CODE_SCHEMA.PUT_TO_STAGE('APP_CODE_STAGE','manifest.yml',(SELECT VALUE FROM CODE_SCHEMA.SCRIPT WHERE NAME = 'MANIFEST'));
CALL CODE_SCHEMA.GET_FROM_STAGE('APP_CODE_STAGE','manifest.yml');
CALL CODE_SCHEMA.PUT_TO_STAGE('APP_CODE_STAGE','setup_script.sql', (SELECT VALUE FROM CODE_SCHEMA.SCRIPT WHERE NAME = 'SETUP'));
CALL CODE_SCHEMA.GET_FROM_STAGE('APP_CODE_STAGE','setup_script.sql');
CALL CODE_SCHEMA.PUT_TO_STAGE('APP_CODE_STAGE','readme.md', (SELECT VALUE FROM CODE_SCHEMA.SCRIPT WHERE NAME = 'README'));
CALL CODE_SCHEMA.GET_FROM_STAGE('APP_CODE_STAGE','readme.md');
CALL CODE_SCHEMA.PUT_TO_STAGE('APP_CODE_STAGE/code_artifacts/streamlit','APP_Configuration_UI.py', (SELECT VALUE FROM CODE_SCHEMA.SCRIPT WHERE NAME = 'APP_CONFIG_UI'));
CALL CODE_SCHEMA.GET_FROM_STAGE('APP_CODE_STAGE/code_artifacts/streamlit','APP_Configuration_UI.py');
CALL CODE_SCHEMA.PUT_TO_STAGE('APP_CODE_STAGE/code_artifacts/streamlit/pages','APP_Initialization.py', (SELECT VALUE FROM CODE_SCHEMA.SCRIPT WHERE NAME = 'APP_INIT'));
CALL CODE_SCHEMA.GET_FROM_STAGE('APP_CODE_STAGE/code_artifacts/streamlit/pages','APP_Initialization.py');
CALL CODE_SCHEMA.PUT_TO_STAGE('APP_CODE_STAGE/code_artifacts/streamlit/pages','APP_Maintenance.py', (SELECT VALUE FROM CODE_SCHEMA.SCRIPT WHERE NAME = 'APP_MAINTENANCE'));
CALL CODE_SCHEMA.GET_FROM_STAGE('APP_CODE_STAGE/code_artifacts/streamlit/pages','APP_Maintenance.py');
CALL CODE_SCHEMA.PUT_TO_STAGE('APP_CODE_STAGE/code_artifacts/streamlit/pages','APP_Debug.py', (SELECT VALUE FROM CODE_SCHEMA.SCRIPT WHERE NAME = 'APP_DEBUG'));
CALL CODE_SCHEMA.GET_FROM_STAGE('APP_CODE_STAGE/code_artifacts/streamlit/pages','APP_Debug.py');

-- ########## END UPLOAD FILES TO APP STAGE ##############################

-- ########## BEGIN CREATE RELEASE / PATCH  ##############################

BEGIN
 LET rs0 RESULTSET := (EXECUTE IMMEDIATE 'ALTER APPLICATION PACKAGE <APP_NAME>_APP_PKG ADD VERSION V0_1 USING @<APP_NAME>_APP_PKG.CODE_SCHEMA.APP_CODE_STAGE');
 RETURN TABLE(rs0);
EXCEPTION
  WHEN OTHER THEN
    LET rs1 RESULTSET := (EXECUTE IMMEDIATE 'ALTER APPLICATION PACKAGE <APP_NAME>_APP_PKG ADD PATCH FOR VERSION V0_1 USING @<APP_NAME>_APP_PKG.CODE_SCHEMA.APP_CODE_STAGE');
    RETURN TABLE(rs1);
END;
;

-- ########## END CREATE RELEASE / PATCH  ################################

-- ########## BEGIN CREATE/PATCH TEST APP   ##############################
DECLARE
  APP_DATABASE := '<APP_NAME>_APP';

  APP_LOCAL_DB := (:APP_DATABASE||'_LOCAL_DB')::VARCHAR;
  APP_LOCAL_SCHEMA := (:APP_LOCAL_DB||'.'||'EGRESS')::VARCHAR;
  APP_LOCAL_EGRESS_RULE := (:APP_LOCAL_SCHEMA||'.'||'APP_RULE')::VARCHAR;
  APP_LOCAL_EAI := (:APP_DATABASE||'_EAI')::VARCHAR;
BEGIN
  BEGIN
    CREATE APPLICATION <APP_NAME>_APP FROM APPLICATION PACKAGE <APP_NAME>_APP_PKG USING VERSION V0_1;
  EXCEPTION
    WHEN OTHER THEN
      BEGIN
        ALTER APPLICATION <APP_NAME>_APP UPGRADE USING VERSION V0_1;
      EXCEPTION
        WHEN OTHER THEN
          DROP APPLICATION IF EXISTS <APP_NAME>_APP;
          CREATE APPLICATION <APP_NAME>_APP FROM APPLICATION PACKAGE <APP_NAME>_APP_PKG USING VERSION V0_1;
      END;
  END;

  CREATE DATABASE IF NOT EXISTS IDENTIFIER(:APP_LOCAL_DB);
  CREATE SCHEMA IF NOT EXISTS IDENTIFIER(:APP_LOCAL_SCHEMA);

  RETURN 'Application '||:APP_DATABASE||' ready.';
END;
-- call core.initialize_app_instance($APP_INSTANCE);
-- call core.start_app_instance($APP_INSTANCE);
-- call core.stop_app_instance($APP_INSTANCE);
-- call core.drop_app_instance($APP_INSTANCE);
-- call core.list_app_instance($APP_INSTANCE);
-- call core.restart_app_instance($APP_INSTANCE);
-- call core.get_app_endpoint($APP_INSTANCE);
-- call core.app_cmd('<cmd>');
-- ########## END CREATE TEST APP   ######################################


-- ########## BEGIN PUBLISH   ############################################
/*
ALTER APPLICATION PACKAGE <APP_NAME>_APP_PKG
   SET DISTRIBUTION = $APP_DISTRIBUTION;

DECLARE
  max_patch VARCHAR;
BEGIN
  show versions in application package <APP_NAME>_APP_PKG;
  select max("patch") INTO :max_patch FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) where "version" = 'V0_1';
  LET rs RESULTSET := (EXECUTE IMMEDIATE 'ALTER APPLICATION PACKAGE <APP_NAME>_APP_PKG SET DEFAULT RELEASE DIRECTIVE VERSION = V0_1 PATCH = '||:max_patch);
  RETURN TABLE(rs);
END;
*/
-- ########## END PUBLISH   ##############################################