

USE ROLE <authorized role>;

CREATE ROLE APP_OWNER_ROLE;

grant usage, operate on warehouse xsmall to role app_owner_role;
GRANT BIND SERVICE ENDPOINT ON ACCOUNT TO ROLE app_owner_role;

set TMP_INTERNAL_DB = 'GENESIS_TEST';
set TMP_INTERNAL_SCH = 'GENESIS_TEST.GENESIS_INTERNAL';
GRANT USAGE ON DATABASE IDENTIFIER($TMP_INTERNAL_DB) TO role app_owner_role;
GRANT USAGE ON SCHEMA IDENTIFIER($TMP_INTERNAL_SCH) TO role app_owner_role;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA IDENTIFIER($TMP_INTERNAL_SCH) TO role app_owner_role;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL VIEWS IN SCHEMA IDENTIFIER($TMP_INTERNAL_SCH) TO role app_owner_role;

use role <authorized role>;

delete from GENESIS_TEST.GENESIS_INTERNAL.LLM_TOKENS;

select * from GENESIS_TEST.GENESIS_INTERNAL.bot_Servicing;
update GENESIS_TEST.GENESIS_INTERNAL.bot_Servicing
set available_tools = '["slack_tools", "make_baby_bot", "integrate_code", "data_connector_tools", "snowflake_tools"]'
where bot_id = 'jl-local-eve-test-1';

select * from GENESIS_TEST.GENESIS_INTERNAL.harvest_control;

select * from GENESIS_TEST.GENESIS_NEW_1.bot_servicing;



CREATE DATABASE IF NOT EXISTS spcs_test;

GRANT OWNERSHIP ON DATABASE spcs_test TO ROLE app_owner_role COPY CURRENT GRANTS;

CREATE SECURITY INTEGRATION IF NOT EXISTS snowservices_ingress_oauth
  TYPE=oauth
  OAUTH_CLIENT=snowservices_ingress
  ENABLED=true;

GRANT BIND SERVICE ENDPOINT ON ACCOUNT TO ROLE test_role;

CREATE COMPUTE POOL genesis_test_pool
  MIN_NODES = 1
  MAX_NODES = 1
  INSTANCE_FAMILY = CPU_X64_XS AUTO_SUSPEND_SECS=3600 INITIALLY_SUSPENDED=FALSE;

use role <authorized role>;

alter compute pool genesis_test_pool suspend;
show compute pools;


GRANT USAGE, MONITOR ON COMPUTE POOL genesis_test_pool TO ROLE APP_OWNER_ROLE;

GRANT ROLE APP_OWNER_ROLE TO USER JUSTIN;

show compute pools;

USE ROLE APP_OWNER_ROLE;
USE DATABASE spcs_test;
USE WAREHOUSE xsmall;

CREATE SCHEMA IF NOT EXISTS app_test_schema;
GRANT ALL ON SCHEMA app_test_schema to role <authorized role>;

CREATE IMAGE REPOSITORY IF NOT EXISTS app_test_repository;
CREATE STAGE IF NOT EXISTS app_test_stage
  DIRECTORY = ( ENABLE = true );

show image repositories;

// dshrnxx-genesis.registry.snowflakecomputing.com/spcs_test/app_test_schema/app_test_repository

use role <authorized role>;
show compute pools;
drop compute pool GENESIS_TEST_POOL;
ALTER COMPUTE POOL GENESIS_TEST_POOL STOP ALL;

// needs outbound to slack api as well

CREATE or replace NETWORK RULE spcs_test.app_test_schema.allow_openai_ngrok
  TYPE = 'HOST_PORT'
  MODE= 'EGRESS'
  VALUE_LIST = ('api.openai.com', 'connect.ngrok-agent.com:443', 'slack.com', 'www.slack.com', 'mmb84124.snowflakecomputing.com:443');

 CREATE or replace NETWORK RULE spcs_test.app_test_schema.open_rule
 TYPE = 'HOST_PORT'
  MODE= 'EGRESS'
  VALUE_LIST = ('0.0.0.0:443','0.0.0.0:80');

  use role <authorized role>;

  show warehouses;

CREATE OR REPLACE NETWORK RULE spcs_test.app_test_schema.GENESIS_RULE
 MODE = EGRESS TYPE = HOST_PORT
  VALUE_LIST = ('api.openai.com', 'connect.ngrok-agent.com:443', 'slack.com', 'www.slack.com', 'mmb84124.snowflakecomputing.com:443', 'wss-primary.slack.com',
'wss-backup.slack.com',  'wss-primary.slack.com:443','wss-backup.slack.com:443');


CREATE or replace  EXTERNAL ACCESS INTEGRATION allow_openai_ngrok_genesis
  ALLOWED_NETWORK_RULES = (spcs_test.app_test_schema.GENESIS_RULE)
  ENABLED = true;

CREATE or replace  EXTERNAL ACCESS INTEGRATION allow_openai_ngrok
  ALLOWED_NETWORK_RULES = (spcs_test.app_test_schema.allow_openai_ngrok)
  ENABLED = true;

use role <authorized role>;

GRANT USAGE ON INTEGRATION allow_openai_ngrok TO ROLE app_owner_role;
GRANT USAGE ON INTEGRATION allow_openai_ngrok_genesis TO ROLE app_owner_role;
show integrations;

use role app_owner_role;


show functions;

select current_warehouse();
use role <authorized role>;
grant all on database genesis_test to role app_owner_role;
grant all on schema GENESIS_TEST.GENESIS_NEW2 to role app_owner_role;

create or replace schema GENESIS_TEST.GENESIS_NEW2;
use role app_owner_role;

create or replace schema GENESIS_TEST.GENESIS_NEW3;


show compute pools;
show image repositories;
use role app_owner_role;

use schema spcs_test.app_test_schema;

drop service spcs_test.app_test_schema.genesis_server;
show services;

select * from GENESIS_TEST.GENESIS_INTERNAL.HARVEST_RESULTS;
use role <authorized role>;

CREATE or replace NETWORK RULE genesis_test.public.snowflake_egress_access
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('MMB84124.snowflakecomputing.com');

CREATE or replace EXTERNAL ACCESS INTEGRATION snowflake_egress_access_integration
  ALLOWED_NETWORK_RULES = (genesis_test.public.snowflake_egress_access)
  ENABLED = true;

grant usage on network rule genesis_test.public.snowflake_egress_access to role app_owner_role;
grant usage on  INTEGRATION snowflake_egress_access_integration to role app_owner_role;

describe integration ALLOW_OPENAI_NGROK_GENESIS;

use role app_owner_role;

select current_account();
drop service spcs_test.app_test_schema.genesis_server;

CREATE SERVICE spcs_test.app_test_schema.genesis_server
  IN COMPUTE POOL genesis_test_pool
  FROM SPECIFICATION $$
    spec:
      containers:
      - name: genesis
        image: dshrnxx-genesis.registry.snowflakecomputing.com/spcs_test/app_test_schema/app_test_repository/genesis_app:latest
        env:
            OPENAI_API_KEY:
            OPENAI_HARVESTER_EMBEDDING_MODEL: text-embedding-3-large
            OPENAI_HARVESTER_MODEL: gpt-4-1106-preview
            HARVESTER_REFRESH_SECONDS: 20
            SNOWFLAKE_SECURE: FALSE
            RUNNER_ID: snowflake-1
            GENESIS_INTERNAL_DB_SCHEMA: GENESIS_TEST.GENESIS_NEW_1
            GENESIS_SOURCE: Snowflake
            SNOWFLAKE_ACCOUNT_OVERRIDE: mmb84124
            SNOWFLAKE_USER_OVERRIDE: GENESIS_RUNNER_JL
            SNOWFLAKE_PASSWORD_OVERRIDE: Gen12349esisBotTest3837
            SNOWFLAKE_DATABASE_OVERRIDE: GENESIS_TEST
            SNOWFLAKE_WAREHOUSE_OVERRIDE: XSMALL
            SNOWFLAKE_ROLE_OVERRIDE: <authorized role>
            ALT_SERVICE_NAME: spcs_test.app_test_schema.genesis_server
        readinessProbe:
          port: 8080
          path: /healthcheck
      endpoints:
      - name: genesisui
        port: 8501
        public: true
      - name: udfendpoint
        port: 8080
        public: true
      $$
   QUERY_WAREHOUSE = 'XSMALL'
   EXTERNAL_ACCESS_INTEGRATIONS = (ALLOW_OPENAI_NGROK_GENESIS)
   MIN_INSTANCES=1
   MAX_INSTANCES=1;

show compute pools;


use role app_owner_role;
use role <authorized role>;
grant all on schema genesis_test.genesis_new_1 to role app_owner_role;
grant all on all tables in schema genesis_test.genesis_new_1 to role app_owner_role;

select * from genesis_test.genesis_new_1.ngrok_tokens;
update genesis_test.genesis_new_1.ngrok_tokens set ngrok_auth_token = null,  ngrok_use_domain='N', ngrok_domain = null;

select * from genesis_test.genesis_new_1.bot_servicing;
select * from genesis_test.genesis_new_1.message_log order by timestamp desc;

update genesis_test.genesis_new_1.bot_servicing  set bot_slack_user_id = null, api_app_id = null, SLACK_ACTIVE = 'N', slack_app_token=null, slack_signing_secret = null, slack_channel_id = null, auth_state = null, client_id= null, client_secret = null, auth_url = null where bot_id = 'Eve-1';


describe service spcs_test.app_test_schema.genesis_server;

ALTER SERVICE  spcs_test.app_test_schema.genesis_server resume;

ALTER SERVICE  spcs_test.app_test_schema.genesis_server
  FROM SPECIFICATION $$
    spec:
      containers:
      - name: genesis
        image: dshrnxx-genesis.registry.snowflakecomputing.com/spcs_test/app_test_schema/app_test_repository/genesis_app:latest
        env:
            OPENAI_API_KEY:
            OPENAI_MODEL_NAME: gpt-4-1106-preview
            OPENAI_HARVESTER_EMBEDDING_MODEL: text-embedding-3-large
            OPENAI_HARVESTER_MODEL: gpt-4-1106-preview
            HARVESTER_REFRESH_SECONDS: 20
            SNOWFLAKE_SECURE: FALSE
            RUNNER_ID: snowflake-1
            GENESIS_INTERNAL_DB_SCHEMA: GENESIS_TEST.GENESIS_NEW_1
            GENESIS_SOURCE: Snowflake
            ALT_SERVICE_NAME: spcs_test.app_test_schema.genesis_server
        readinessProbe:
          port: 8080
          path: /healthcheck
      endpoints:
      - name: genesisui
        port: 8501
        public: true
      - name: udfendpoint
        port: 8080
        public: true
      $$;

/*
            SNOWFLAKE_ACCOUNT_OVERRIDE: mmb84124
            SNOWFLAKE_USER_OVERRIDE: GENESIS_RUNNER_JL
            SNOWFLAKE_PASSWORD_OVERRIDE: Gen12349esisBotTest3837
            SNOWFLAKE_DATABASE_OVERRIDE: GENESIS_TEST
            SNOWFLAKE_WAREHOUSE_OVERRIDE: XSMALL
            SNOWFLAKE_ROLE_OVERRIDE: <authorized role>
*/

use schema GENESIS_TEST.GENESIS_NEW_B;
show tables;
select current_role();
use role <authorized role>;
grant all on all tables in schema GENESIS_TEST.GENESIS_NEW_B to role app_owner_role;
use role app_owner_role;
select * from GENESIS_TEST.GENESIS_NEW_B.BOT_SERVICING;
update GENESIS_TEST.GENESIS_NEW_B.BOT_SERVICING set AVAILABLE_TOOLS = null;

select * from GENESIS_TEST.GENESIS_NEW_1.BOT_SERVICING;


use role <authorized role>;
use role app_owner_role;

show tables in schema genesis_test.genesis_new3;

show services;

CREATE OR REPLACE SCHEMA genesis_test.GENESIS_NEW_B;
GRANT ALL ON SCHEMA genesis_test.GENESIS_NEW_B TO ROLE <authorized role>;
grant all on database genesis_test to role <authorized role>;

SELECT CURRENT_ROLE();
use schema genesis_test.GENESIS_NEW_B;
show tables;


describe service spcs_test.app_test_schema.genesis_server;

use role <authorized role>;
use role app_owner_role;

use schema genesis_test.genesis_internal;
show stages;
ls @genesis_test.genesis_internal.SEMANTIC_STAGE;


CREATE STAGE genesis_test.genesis_internal.semantic_stage
  ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');

grant all on stage genesis_test.genesis_internal.semantic_stage to role app_owner_role;

use role app_owner_role;

select current_account();
select current_region();


SELECT SYSTEM$GET_SERVICE_STATUS('spcs_test.app_test_schema.genesis_server');
SELECT SYSTEM$GET_SERVICE_LOGS('spcs_test.app_test_schema.genesis_server',0,'chattest',300);
SHOW ENDPOINTS IN SERVICE spcs_test.app_test_schema.genesis_server;

select * from genesis_test.genesis_new_1.bot_servicing;

select * from genesis_test.genesis_internal.message_log order by timestamp desc;
select * from genesis_test.genesis_internal.harvest_results;

use role app_owner_role;
use role <authorized role>;

show stages;


call PUT_TO_STAGE('genesis_test.genesis_internal.semantic_stage','revenue.yaml',$$
name: revenue
tables:
  - name: DAILY_REVENUE
    description: '  ' # <FILL-OUT>
    base_table:
      database: REVENUE_TIME_SERIES
      schema: PUBLIC
      table: DAILY_REVENUE
    filters:
      - name: '  ' # <FILL-OUT>
        synonyms:
          - '  ' # <FILL-OUT>
        description: '  ' # <FILL-OUT>
        expr: '  ' # <FILL-OUT>
    time_dimensions:
      - name: DATE
        synonyms:
          - '  ' # <FILL-OUT>
        description: '  ' # <FILL-OUT>
        expr: DATE
        data_type: DATE
        sample_values:
          - '2023-01-01'
          - '2023-01-02'
          - '2023-01-03'
          - '2023-01-04'
          - '2023-01-05'
          - '2023-01-06'
          - '2023-01-07'
          - '2023-01-08'
          - '2023-01-09'
          - '2023-01-10'
    measures:
      - name: REVENUE
        synonyms:
          - '  ' # <FILL-OUT>
        description: '  ' # <FILL-OUT>
        expr: REVENUE
        data_type: NUMBER
        sample_values:
          - '15488.14'
          - '17151.89'
          - '16027.63'
          - '14236.55'
          - '16458.94'
          - '14375.87'
          - '18917.73'
          - '19636.63'
          - '13834.42'
          - '17917.25'
      - name: COGS
        synonyms:
          - '  ' # <FILL-OUT>
        description: '  ' # <FILL-OUT>
        expr: COGS
        data_type: NUMBER
        sample_values:
          - '7615.84'
          - '9344.93'
          - '7690.14'
          - '8972.82'
          - '7659.33'
          - '8886.58'
          - '6692.60'
          - '10430.40'
          - '10352.67'
          - '11510.98'
      - name: FORECASTED_REVENUE
        synonyms:
          - '  ' # <FILL-OUT>
        description: '  ' # <FILL-OUT>
        expr: FORECASTED_REVENUE
        data_type: NUMBER
        sample_values:
          - '15491.43'
          - '16727.86'
          - '15594.61'
          - '14710.08'
          - '14225.07'
          - '17057.19'
          - '13735.69'
          - '19009.97'
          - '18133.96'
          - '12893.25'
  - name: DAILY_REVENUE_BY_PRODUCT
    description: '  ' # <FILL-OUT>
    base_table:
      database: REVENUE_TIME_SERIES
      schema: PUBLIC
      table: DAILY_REVENUE_BY_PRODUCT
    filters:
      - name: '  ' # <FILL-OUT>
        synonyms:
          - '  ' # <FILL-OUT>
        description: '  ' # <FILL-OUT>
        expr: '  ' # <FILL-OUT>
    dimensions:
      - name: PRODUCT_LINE
        synonyms:
          - '  ' # <FILL-OUT>
        description: '  ' # <FILL-OUT>
        expr: PRODUCT_LINE
        data_type: TEXT
        sample_values:
          - Electronics
          - Clothing
          - Home Appliances
          - Toys
          - Books
    time_dimensions:
      - name: DATE
        synonyms:
          - '  ' # <FILL-OUT>
        description: '  ' # <FILL-OUT>
        expr: DATE
        data_type: DATE
        sample_values:
          - '2023-01-01'
          - '2023-01-02'
          - '2023-01-03'
          - '2023-01-04'
          - '2023-01-05'
          - '2023-01-06'
          - '2023-01-07'
          - '2023-01-08'
          - '2023-01-09'
          - '2023-01-10'
    measures:
      - name: REVENUE
        synonyms:
          - '  ' # <FILL-OUT>
        description: '  ' # <FILL-OUT>
        expr: REVENUE
        data_type: NUMBER
        sample_values:
          - '3333.22'
          - '40.31'
          - '7994.96'
          - '2236.36'
          - '648.75'
          - '571.50'
          - '3255.95'
          - '7777.86'
          - '6913.93'
          - '4897.83'
      - name: COGS
        synonyms:
          - '  ' # <FILL-OUT>
        description: '  ' # <FILL-OUT>
        expr: COGS
        data_type: NUMBER
        sample_values:
          - '2708.71'
          - '2168.92'
          - '2024.65'
          - '6.24'
          - '707.32'
          - '1223.21'
          - '3430.02'
          - '1578.78'
          - '688.16'
          - '5511.62'
      - name: FORECASTED_REVENUE
        synonyms:
          - '  ' # <FILL-OUT>
        description: '  ' # <FILL-OUT>
        expr: FORECASTED_REVENUE
        data_type: NUMBER
        sample_values:
          - '3420.39'
          - '4885.30'
          - '2206.83'
          - '1062.44'
          - '3916.47'
          - '4219.60'
          - '3589.23'
          - '5879.25'
          - '926.10'
          - '2113.68'
  - name: DAILY_REVENUE_BY_REGION
    description: '  ' # <FILL-OUT>
    base_table:
      database: REVENUE_TIME_SERIES
      schema: PUBLIC
      table: DAILY_REVENUE_BY_REGION
    filters:
      - name: '  ' # <FILL-OUT>
        synonyms:
          - '  ' # <FILL-OUT>
        description: '  ' # <FILL-OUT>
        expr: '  ' # <FILL-OUT>
    dimensions:
      - name: SALES_REGION
        synonyms:
          - '  ' # <FILL-OUT>
        description: '  ' # <FILL-OUT>
        expr: SALES_REGION
        data_type: TEXT
        sample_values:
          - North America
          - Europe
          - Asia
          - South America
          - Africa
    time_dimensions:
      - name: DATE
        synonyms:
          - '  ' # <FILL-OUT>
        description: '  ' # <FILL-OUT>
        expr: DATE
        data_type: DATE
        sample_values:
          - '2023-01-01'
          - '2023-01-02'
          - '2023-01-03'
          - '2023-01-04'
          - '2023-01-05'
          - '2023-01-06'
          - '2023-01-07'
          - '2023-01-08'
          - '2023-01-09'
          - '2023-01-10'
    measures:
      - name: REVENUE
        synonyms:
          - '  ' # <FILL-OUT>
        description: '  ' # <FILL-OUT>
        expr: REVENUE
        data_type: NUMBER
        sample_values:
          - '9456.72'
          - '2001.17'
          - '2377.24'
          - '765.82'
          - '1393.20'
          - '4606.57'
          - '1020.62'
          - '6141.34'
          - '3990.16'
          - '2019.54'
      - name: COGS
        synonyms:
          - '  ' # <FILL-OUT>
        description: '  ' # <FILL-OUT>
        expr: COGS
        data_type: NUMBER
        sample_values:
          - '118.26'
          - '2427.49'
          - '2794.91'
          - '2201.47'
          - '4840.11'
          - '1913.75'
          - '620.68'
          - '422.79'
          - '2298.06'
          - '1562.07'
      - name: FORECASTED_REVENUE
        synonyms:
          - '  ' # <FILL-OUT>
        description: '  ' # <FILL-OUT>
        expr: FORECASTED_REVENUE
        data_type: NUMBER
        sample_values:
          - '2636.70'
          - '1078.47'
          - '1783.86'
          - '9393.56'
          - '598.84'
          - '1384.24'
          - '356.68'
          - '7029.74'
          - '4293.92'
          - '3663.28'
$$);

use role <authorized role>;

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

grant usage on procedure PUT_TO_STAGE(varchar, varchar, varchar) to role app_owner_role;
grant usage on procedure GET_FROM_STAGE( varchar, varchar) to role app_owner_role;






describe function app_test_schema.lookup_udf (varchar, varchar);

show functions;

use database spcs_test;
use schema spcs_test.app_test_schema;

create or replace function app_test_schema.submit_udf(INPUT_TEXT VARCHAR, THREAD_ID VARCHAR, BOT_ID VARCHAR)
returns varchar
service=genesis_server
endpoint=udfendpoint
as '/udf_proxy/submit_udf';

create or replace function app_test_schema.lookup_udf(UU VARCHAR, BOT_ID VARCHAR)
returns varchar
service=genesis_server
endpoint=udfendpoint
as '/udf_proxy/lookup_udf';

CREATE or replace FUNCTION app_test_schema.get_slack_endpoints ()
  RETURNS varchar
  SERVICE=genesis_server
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

  CREATE or replace FUNCTION app_test_schema.CONFIGURE_NGROK_TOKEN(a varchar, b varchar, c varchar)
  RETURNS varchar
  SERVICE=genesis_server
  ENDPOINT=udfendpoint
  AS '/udf_proxy/configure_ngrok_token';



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

select g_healthcheck();

CREATE or replace FUNCTION g_submit_udf (prompt varchar, thread varchar)
  RETURNS varchar
  SERVICE=genesis_server
  ENDPOINT=udfendpoint
  AS '/echo';

CREATE or replace FUNCTION response_udf (request_id varchar)
  RETURNS varchar
  SERVICE=echo_service
  ENDPOINT=udfendpoint
  AS '/lookup';

select get_slack_endpoints();

grant usage on function app_test_schema.list_available_bots() to role <authorized role>;

show databases;

select * from genesis_test.genesis_internal.slack_app_config_tokens;

update genesis_test.genesis_internal.slack_app_config_tokens set slack_app_config_refresh_token = '<token>'
where runner_id = 'jl-local-runner';

/// simple test for udf
drop service echo_service;

show services;

CREATE SERVICE echo_service
  IN COMPUTE POOL genesis_test_pool
  FROM SPECIFICATION $$
    spec:
      containers:
      - name: echo
        image: dshrnxx-genesis.registry.snowflakecomputing.com/spcs_test/app_test_schema/app_test_repository/natapp:latest
        env:
          SERVER_PORT: 8000
          CHARACTER_NAME: Bob
          OPENAI_API_KEY:
          NGROK_AUTHTOKEN:
          SNOWFLAKE_HOST_OVERRIDE: "mmb84124.prod3.us-west-2.aws.snowflakecomputing.com"
          SNOWFLAKE_PORT_OVERRIDE: 443
          SNOWFLAKE_SCHEMA_OVERRIDE: APP_TEST_SCHEMA
          SNOWFLAKE_ACCOUNT_OVERRIDE: mmb84124
          SNOWFLAKE_USER_OVERRIDE: GENESIS_RUNNER_JL
          SNOWFLAKE_PASSWORD_OVERRIDE:
          SNOWFLAKE_DATABASE_OVERRIDE: GENESIS_TEST
          SNOWFLAKE_WAREHOUSE_OVERRIDE: XSMALL
          SNOWFLAKE_ROLE_OVERRIDE: APP_OWNER_ROLE
        readinessProbe:
          port: 8000
          path: /healthcheck
      endpoints:
      - name: udfendpoint
        port: 8000
        public: true
      - name: chatapp
        port: 8501
        public: true
      $$
   EXTERNAL_ACCESS_INTEGRATIONS = (ALLOW_OPENAI_NGROK)
   MIN_INSTANCES=1
   MAX_INSTANCES=1;

SELECT SYSTEM$GET_SERVICE_STATUS('echo_service');
SELECT SYSTEM$GET_SERVICE_LOGS('echo_service',0,'echo',1000);


CREATE or replace FUNCTION submit_udf (prompt varchar, thread varchar)
  RETURNS varchar
  SERVICE=echo_service
  ENDPOINT=udfendpoint
  AS '/echo';

CREATE or replace FUNCTION response_udf (request_id varchar)
  RETURNS varchar
  SERVICE=echo_service
  ENDPOINT=udfendpoint
  AS '/lookup';

select submit_udf('DATABASES','111');

select response_udf('ae8d681d-d109-4860-8482-adfe64aa51f8');








show compute pools;
select current_user();

// echo test

use role <authorized role>;

grant usage on database chattest_master to role ...;
grant usage on schema chattest_master.code_schema to role ...;

grant read on image repository chattest_master.code_schema.service_repo to role partner_apps_owner_role;

use database chattest_master;
show compute pools;

use role ...;
use database justin;



CREATE COMPUTE POOL genesis_test_pool
  MIN_NODES = 1
  MAX_NODES = 1
  INSTANCE_FAMILY = CPU_X64_XS;

  drop service echo_service;
create schema core;

// stuff below into app

//account
select current_account();
select current_warehouse


CREATE SERVICE core.echo_service
  IN COMPUTE POOL genesis_test_pool
  FROM SPECIFICATION $$
    spec:
      containers:
      - name: echo
        image: .../chattest_master/code_schema/service_repo/chatapp
        env:
          SERVER_PORT: 8000
          CHARACTER_NAME: Bob
          OPENAI_API_KEY: "sk-..."
          NGROK_AUTHTOKEN: "..."
          SNOWFLAKE_HOST: "....prod3.us-west-2.aws.snowflakecomputing.com"
          SNOWFLAKE_PORT: 443
          SNOWFLAKE_ACCOUNT: ...
          SNOWFLAKE_WAREHOUSE: ...
          SNOWFLAKE_DATABASE: JUSTIN
          SNOWFLAKE_SCHEMA: CORE
        readinessProbe:
          port: 8000
          path: /healthcheck
      endpoints:
      - name: udfendpoint
        port: 8000
        public: true
      - name: chatapp
        port: 8501
        public: true
      $$
   EXTERNAL_ACCESS_INTEGRATIONS = (ALLOW_ALL_RULE_JL_NEW)
   MIN_INSTANCES=1
   MAX_INSTANCES=1;

drop service core.echo_service_min;

CREATE SERVICE core.echo_service_min
  IN COMPUTE POOL genesis_test_pool
  FROM SPECIFICATION $$
    spec:
      containers:
      - name: echo
        image: sfengineering-ss-lprpr-test1.registry.snowflakecomputing.com/chattest_master/code_schema/service_repo/chatapp
        env:
          SERVER_PORT: 8000
          CHARACTER_NAME: Bob
          OPENAI_API_KEY: "sk-..."
          NGROK_AUTHTOKEN: "..."
        readinessProbe:
          port: 8000
          path: /healthcheck
      endpoints:
      - name: udfendpoint
        port: 8000
        public: true
      - name: chatapp
        port: 8501
        public: true
      $$
   EXTERNAL_ACCESS_INTEGRATIONS = (ALLOW_ALL_RULE_JL_NEW)
   MIN_INSTANCES=1
   MAX_INSTANCES=1;

CREATE or replace FUNCTION core.submit_udf (prompt varchar, thread varchar)
  RETURNS varchar
  SERVICE=core.echo_service_min
  ENDPOINT=udfendpoint
  AS '/echo';

CREATE or replace FUNCTION core.response_udf (request_id varchar)
  RETURNS varchar
  SERVICE=core.echo_service_min
  ENDPOINT=udfendpoint
  AS '/lookup';


DESCRIBE SERVICE echo_service_min;
SELECT SYSTEM$GET_SERVICE_STATUS('echo_service_min');

SELECT SYSTEM$GET_SERVICE_LOGS('echo_service_min',0,'echo',100);

SELECT submit_udf('test!');

select response_udf('....-9c03-4ff2-97d7-66a2bd91761f');

SHOW ENDPOINTS IN SERVICE echo_service;