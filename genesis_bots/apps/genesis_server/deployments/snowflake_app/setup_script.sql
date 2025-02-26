

CREATE OR ALTER VERSIONED SCHEMA CORE;

CREATE OR REPLACE STREAMLIT CORE.GENESIS
    FROM '/code_artifacts/streamlit'
    MAIN_FILE = '/Genesis.py';

CREATE APPLICATION ROLE IF NOT EXISTS APP_PUBLIC;
GRANT USAGE ON SCHEMA CORE TO APPLICATION ROLE APP_PUBLIC;

CREATE OR ALTER VERSIONED SCHEMA APP;


CREATE OR REPLACE TABLE APP.YAML (name varchar, value varchar);

INSERT INTO APP.YAML (NAME , VALUE)
VALUES ('GENESISAPP_SERVICE_SERVICE',
$$
    spec:
      containers:
      - name: genesis
        image: /genesisapp_master/code_schema/service_repo/genesis_app:latest
        volumeMounts:
        - name: botgit
          mountPath: /opt/bot_git
        env:
            RUNNER_ID: snowflake-1
            SNOWFLAKE_METADATA: TRUE
            SPCS_MODE: TRUE
            GENESIS_INTERNAL_DB_SCHEMA: {{app_db_sch}}
            GENESIS_SOURCE: Snowflake
            SNOWFLAKE_SECURE: FALSE
            LAUNCH_GUI: FALSE
            INTERNAL_HARVESTER_ENABLED: FALSE
            OPENAI_HARVESTER_EMBEDDING_MODEL: text-embedding-3-large
            OPENAI_MODEL_NAME: gpt-4o-2024-11-20
            OPENAI_MODEL_SUPERVISOR: gpt-4o
            OPENAI_O1_OVERRIDE_MODEL: o1-preview
            O1_OVERRIDE_BOT: NO_OVERRIDE_BOT
            OPENAI_FAST_MODEL_NAME: gpt-4o-mini
            CORTEX_MODEL: llama3.1-405b
            CORTEX_PREMIERE_MODEL: claude-3-5-sonnet
            CORTEX_FAST_MODEL_NAME: llama3.1-70b
            USE_KNOWLEDGE: TRUE
            LAST_K_KNOWLEGE: 0
            OPENAI_USE_ASSISTANTS: FALSE
            GIT_PATH: /opt/bot_git
            LOG_LEVEL: INFO
        readinessProbe:
            port: 8080
            path: /healthcheck
      volumes:
      - name: botgit
        source: "@app1.bot_git"
      endpoints:
      - name: udfendpoint
        port: 8080
        public: true
      - name: streamlit
        port: 8501
        public: true
      - name: streamlitdatacubes
        port: 8502
        public: true
      - name: debuggenesis
        port: 1234
        public: true
      - name: voicedemo
        port: 3000
        public: true
      - name: voicerelay
        port: 8081
        public: true
      - name: teamsendpoint
        port: 3978
        public: true
      logExporters:
        eventTableConfig:
          logLevel: INFO
    serviceRoles:
    - name: GENESISAPP_SERVICE_SERVICE_ROLE
      endpoints:
      - teamsendpoint
      - udfendpoint
      - streamlit
      - streamlitdatacubes
      - debuggenesis
      - voicedemo
      - voicerelay
$$)
;

INSERT INTO APP.YAML (NAME , VALUE)
VALUES ('GENESISAPP_HARVESTER_SERVICE',
$$
    spec:
      containers:
      - name: genesis-harvester
        image: /genesisapp_master/code_schema/service_repo/genesis_app:latest
        env:
            GENESIS_MODE: HARVESTER
            AUTO_HARVEST: TRUE
            SNOWFLAKE_METADATA: TRUE
            SPCS_MODE: TRUE
            OPENAI_HARVESTER_EMBEDDING_MODEL: text-embedding-3-large
            CORTEX_MODEL: llama3.1-405b
            CORTEX_PREMIERE_MODEL: claude-3-5-sonnet
            CORTEX_FAST_MODEL_NAME: llama3.1-70b
            OPENAI_MODEL_NAME: gpt-4o-2024-11-20
            OPENAI_MODEL_SUPERVISOR: gpt-4o
            OPENAI_O1_OVERRIDE_MODEL: o1-preview
            OPENAI_FAST_MODEL_NAME: gpt-4o-mini
            HARVESTER_REFRESH_SECONDS: 120
            RUNNER_ID: snowflake-1
            SNOWFLAKE_SECURE: FALSE
            GENESIS_INTERNAL_DB_SCHEMA: {{app_db_sch}}
            GENESIS_SOURCE: Snowflake
            LOG_LEVEL: INFO
      endpoints:
      - name: udfendpoint
        port: 8080
        public: false
      logExporters:
        eventTableConfig:
          logLevel: INFO
    serviceRoles:
    - name: GENESISAPP_HARVESTER_SERVICE_ROLE
      endpoints:
      - udfendpoint
$$)
;

INSERT INTO APP.YAML (NAME , VALUE)
VALUES ('GENESISAPP_KNOWLEDGE_SERVICE',
$$
    spec:
      containers:
      - name: genesis-knowledge
        image: /genesisapp_master/code_schema/service_repo/genesis_app:latest
        env:
            GENESIS_MODE: KNOWLEDGE
            KNOWLEDGE_REFRESH_SECONDS: 120
            SPCS_MODE: TRUE
            RUNNER_ID: snowflake-1
            CORTEX_MODEL: llama3.1-405b
            CORTEX_PREMIERE_MODEL: claude-3-5-sonnet
            CORTEX_FAST_MODEL_NAME: llama3.1-70b
            SNOWFLAKE_METADATA: TRUE
            OPENAI_MODEL_NAME: gpt-4o-2024-11-20
            OPENAI_MODEL_SUPERVISOR: gpt-4o
            OPENAI_O1_OVERRIDE_MODEL: o1-preview
            OPENAI_FAST_MODEL_NAME: gpt-4o-mini
            GENESIS_INTERNAL_DB_SCHEMA: {{app_db_sch}}
            GENESIS_SOURCE: Snowflake
            LOG_LEVEL: INFO
      endpoints:
      - name: udfendpoint
        port: 8080
        public: false
      logExporters:
        eventTableConfig:
          logLevel: INFO
    serviceRoles:
    - name: GENESISAPP_KNOWLEDGE_SERVICE_ROLE
      endpoints:
      - udfendpoint
$$)
;

INSERT INTO APP.YAML (NAME , VALUE)
VALUES ('GENESISAPP_TASK_SERVICE',
$$
    spec:
      containers:
      - name: genesis-task-server
        image: /genesisapp_master/code_schema/service_repo/genesis_app:latest
        volumeMounts:
        - name: botgit
          mountPath: /opt/bot_git
        env:
            GENESIS_MODE: TASK_SERVER
            AUTO_HARVEST: TRUE
            SNOWFLAKE_METADATA: TRUE
            SPCS_MODE: TRUE
            INTERNAL_HARVESTER_ENABLED: FALSE
            OPENAI_HARVESTER_EMBEDDING_MODEL: text-embedding-3-large
            CORTEX_MODEL: llama3.1-405b
            CORTEX_PREMIERE_MODEL: claude-3-5-sonnet
            CORTEX_FAST_MODEL_NAME: llama3.1-70b
            OPENAI_MODEL_NAME: gpt-4o-2024-11-20
            OPENAI_MODEL_SUPERVISOR: gpt-4o
            OPENAI_O1_OVERRIDE_MODEL: o1-preview
            OPENAI_FAST_MODEL_NAME: gpt-4o-mini
            RUNNER_ID: snowflake-1
            SNOWFLAKE_SECURE: FALSE
            GENESIS_INTERNAL_DB_SCHEMA: {{app_db_sch}}
            GENESIS_SOURCE: Snowflake
            OPENAI_USE_ASSISTANTS: FALSE
            GIT_PATH: /opt/bot_git
            LOG_LEVEL: INFO
      volumes:
      - name: botgit
        source: "@app1.bot_git"
      endpoints:
      - name: udfendpoint
        port: 8080
        public: false
      logExporters:
        eventTableConfig:
          logLevel: INFO
    serviceRoles:
    - name: GENESISAPP_TASK_SERVICE_ROLE
      endpoints:
      - udfendpoint
$$)
;


 CREATE OR REPLACE PROCEDURE CORE.GET_EAI_LIST(INSTANCE_NAME STRING)
  RETURNS STRING
  LANGUAGE SQL
  AS $$
  DECLARE
    eai_services_list STRING := '';
    services_exist STRING := '';
    eai_stored_list STRING := '';
    eai_list STRING := '';
    combined_list ARRAY;
    deduplicated_list ARRAY;
  BEGIN
    BEGIN
        EXECUTE IMMEDIATE 'SHOW SERVICES IN SCHEMA ' || :INSTANCE_NAME;
        SELECT "name" INTO services_exist FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) LIMIT 1;
    EXCEPTION
      WHEN STATEMENT_ERROR THEN
       services_exist := '';
    END;
    IF (services_exist IS NOT NULL AND services_exist <> '') THEN

        EXECUTE IMMEDIATE 'SHOW SERVICES IN SCHEMA ' || :INSTANCE_NAME;
        SELECT DISTINCT UPPER(REPLACE(RTRIM(LTRIM("external_access_integrations", '['), ']'), '"', '')) INTO eai_services_list
        FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) LIMIT 1;

    ELSE
        eai_list := '';
    END IF;

    BEGIN
        SELECT UPPER(LISTAGG(TRIM(EAI_NAME), ',')) INTO eai_stored_list
        FROM APP1.EAI_CONFIG;
    EXCEPTION
      WHEN STATEMENT_ERROR THEN
       eai_stored_list := '';
    END;

    IF (eai_stored_list IS NOT NULL AND eai_stored_list <> '') THEN

        IF (services_exist IS NULL OR services_exist = '') THEN
            eai_list := LTRIM(RTRIM(TRIM(ARRAY_TO_STRING(array_distinct(ARRAY_CAT(SPLIT(eai_services_list, ','), SPLIT(eai_stored_list, ','))), ',')), ','), ',');
        ELSE
            eai_list := LTRIM(RTRIM(TRIM(ARRAY_TO_STRING(array_distinct(SPLIT(eai_stored_list, ',')), ',')), ','), ',');
        END IF;

    ELSEIF (services_exist IS NOT NULL AND services_exist <> '') THEN
        eai_list := LTRIM(RTRIM(TRIM(ARRAY_TO_STRING(array_distinct(SPLIT(eai_services_list, ',')), ',')), ','), ',');
    ELSE
        eai_list := '';
    END IF;

    eai_list := RTRIM(TRIM(eai_list),',');

    RETURN eai_list;
  END;
  $$;

 CREATE OR REPLACE PROCEDURE core.grant_callback(privileges array)
  RETURNS STRING
  LANGUAGE SQL
  AS $$
  DECLARE
    wh_name STRING;
  BEGIN
   IF (ARRAY_CONTAINS('CREATE COMPUTE POOL'::VARIANT, privileges)) THEN
    BEGIN

      //create compute pool
      CREATE COMPUTE POOL IF NOT EXISTS GENESIS_DEMO_POOL
      MIN_NODES=1 MAX_NODES=1 INSTANCE_FAMILY='CPU_X64_S' AUTO_SUSPEND_SECS=3600 INITIALLY_SUSPENDED=FALSE;

      GRANT OPERATE ON COMPUTE POOL GENESIS_DEMO_POOL TO APPLICATION ROLE APP_PUBLIC;

    EXCEPTION
      WHEN STATEMENT_ERROR THEN
        RETURN 'ERROR ON GRANT';
    END;
   END IF;
   IF (ARRAY_CONTAINS('CREATE WAREHOUSE'::VARIANT, privileges)) THEN
      BEGIN
          LET stmt VARCHAR := 'SELECT "name" AS WH_NAME, "owner" AS WH_OWNER, CURRENT_DATABASE() AS CURRENT_DB FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) WHERE "name" = ''APP_XSMALL'' LIMIT 1';
          EXECUTE IMMEDIATE 'SHOW WAREHOUSES';
          LET RS1 RESULTSET := (EXECUTE IMMEDIATE :stmt);
          LET c1 CURSOR FOR RS1;

          FOR rec IN c1 DO
            IF (rec.WH_OWNER = rec.CURRENT_DB) THEN
              wh_name := 'APP_XSMALL';
            ELSE
              wh_name := 'APP_XSMALL_1';
            END IF;
           END FOR;
      EXCEPTION
        WHEN STATEMENT_ERROR THEN
          wh_name := 'APP_XSMALL';
      END;

      EXECUTE IMMEDIATE 'CREATE WAREHOUSE IF NOT EXISTS ' || :wh_name || ' MIN_CLUSTER_COUNT=1 MAX_CLUSTER_COUNT=1 ' ||
      ' WAREHOUSE_SIZE=XSMALL AUTO_RESUME = TRUE AUTO_SUSPEND = 60';

      EXECUTE IMMEDIATE 'GRANT USAGE, OPERATE ON WAREHOUSE ' || :wh_name || ' TO APPLICATION ROLE APP_PUBLIC';

      CALL CORE.INITIALIZE_APP_INSTANCE('APP1','GENESIS_DEMO_POOL',:wh_name);

   END IF;
   RETURN 'DONE';
 END;
 $$;

GRANT USAGE ON PROCEDURE core.grant_callback(array) TO APPLICATION ROLE app_public;


CREATE OR REPLACE PROCEDURE CORE.REGISTER_SINGLE_REFERENCE(ref_name STRING, operation STRING, ref_or_alias STRING)
  RETURNS STRING
  LANGUAGE SQL
  AS $$
    BEGIN
      CASE (operation)
        WHEN 'ADD' THEN
          SELECT SYSTEM$SET_REFERENCE(:ref_name, :ref_or_alias);
        WHEN 'REMOVE' THEN
          SELECT SYSTEM$REMOVE_REFERENCE(:ref_name, :ref_or_alias);
        WHEN 'CLEAR' THEN
          SELECT SYSTEM$REMOVE_ALL_REFERENCES(:ref_name);
      ELSE
        RETURN 'unknown operation: ' || operation;
      END CASE;
      RETURN NULL;
    END;
  $$;

GRANT USAGE ON PROCEDURE CORE.REGISTER_SINGLE_REFERENCE(STRING, STRING, STRING) TO APPLICATION ROLE APP_PUBLIC;


CREATE OR REPLACE PROCEDURE core.get_config_for_ref(ref_name STRING)
    RETURNS STRING
    LANGUAGE SQL
    AS
    $$
    DECLARE
      azure_ep VARCHAR;
      jira_ep VARCHAR;
      custom_ep VARCHAR;
      ports VARCHAR;
    BEGIN
      CASE (ref_name)
        WHEN 'SLACK_EXTERNAL_ACCESS' THEN
          RETURN '{
            "type": "CONFIGURATION",
            "payload":{
              "host_ports":["slack.com", "www.slack.com", "wss-primary.slack.com", "wss-backup.slack.com", "wss-primary.slack.com", "wss-backup.slack.com", "slack-files.com", "downloads.slack-edge.com", "files-edge.slack.com", "files-origin.slack.com", "files.slack.com", "global-upload-edge.slack.com", "universal-upload-edge.slack.com"],
              "allowed_secrets": "NONE"}}';
        WHEN 'GOOGLE_EXTERNAL_ACCESS' THEN
          RETURN '{
            "type": "CONFIGURATION",
            "payload":{
              "host_ports":["accounts.google.com","oauth2.googleapis.com","www.googleapis.com","googleapis.com","sheets.googleapis.com"],
              "allowed_secrets": "NONE"}}';
        WHEN 'JIRA_EXTERNAL_ACCESS' THEN
          SELECT VALUE || '.atlassian.net' INTO jira_ep
          FROM APP1.EXT_SERVICE_CONFIG
          WHERE UPPER(EXT_SERVICE_NAME) = 'JIRA' AND UPPER(PARAMETER) = 'SITE_NAME';

          IF (jira_ep = '.atlassian.net') THEN
              ports := '"www.atlassian.net"';
          ELSE
              ports := '"www.atlassian.net", "' || :jira_ep || '"';
          END IF;
          RETURN '{
            "type": "CONFIGURATION",
            "payload":{
              "host_ports":[' || ports || '],
              "allowed_secrets": "NONE"}}';
        WHEN 'SERPER_EXTERNAL_ACCESS' THEN
          RETURN '{
            "type": "CONFIGURATION",
            "payload":{
              "host_ports":["google.serper.dev", "scrape.serper.dev"],
              "allowed_secrets": "NONE"}}';
        WHEN 'GITHUB_EXTERNAL_ACCESS' THEN
          RETURN '{
            "type": "CONFIGURATION",
            "payload":{
              "host_ports":["api.github.com", "github.com"],
              "allowed_secrets": "NONE"}}';
        WHEN 'OPENAI_EXTERNAL_ACCESS' THEN
          RETURN '{
            "type": "CONFIGURATION",
            "payload":{
              "host_ports":["api.openai.com", "oaidalleapiprodscus.blob.core.windows.net"],
              "allowed_secrets": "NONE"}}';
        WHEN 'AZURE_OPENAI_EXTERNAL_ACCESS' THEN
          SELECT ENDPOINT || '.openai.azure.com' INTO azure_ep
          FROM APP1.CUSTOM_ENDPOINTS WHERE TYPE = 'AZURE';

          IF (azure_ep = '.openai.azure.com') THEN
              ports := '"openai.azure.com"';
          ELSE
              ports := '"openai.azure.com", "' || :azure_ep || '"';
          END IF;
          RETURN '{
            "type": "CONFIGURATION",
            "payload":{
              "host_ports":[' || ports || '],
              "allowed_secrets": "NONE"}}';

        WHEN 'CUSTOM_EXTERNAL_ACCESS' THEN
          SELECT LISTAGG('"' || ENDPOINT || '"', ',') INTO custom_ep
          FROM APP1.CUSTOM_ENDPOINTS
          WHERE TYPE = 'CUSTOM';

          IF (LEN(custom_ep) > 0) THEN
            RETURN '{
              "type": "CONFIGURATION",
              "payload":{
                "host_ports":[' || custom_ep || '],
                "allowed_secrets": "NONE"}}';
          ELSE
              RETURN '';
          END IF;

      END CASE;
  RETURN '';
  END;
  $$;

GRANT USAGE ON PROCEDURE core.get_config_for_ref(string) TO APPLICATION ROLE APP_PUBLIC;

CREATE OR REPLACE PROCEDURE core.get_config_for_secret(ref_name STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
    BEGIN
      IF (ref_name = 'PRIVATE_KEY_SECRET') THEN
          -- EXECUTE IMMEDIATE 'CALL CORE.UPGRADE_SERVICES()';
          RETURN 'SUCCESS';
      END IF;
    RETURN '';
  END;
  $$;

GRANT USAGE ON PROCEDURE core.get_config_for_secret(string) TO APPLICATION ROLE APP_PUBLIC;



CREATE OR REPLACE PROCEDURE APP.UPGRADE_APP(INSTANCE_NAME VARCHAR, SERVICE_NAME VARCHAR, UPDATE_HARVEST_METADATA BOOLEAN, APP_NAME VARCHAR, C_POOL_NAME VARCHAR, WAREHOUSE_NAME VARCHAR)
RETURNS VARCHAR NOT NULL
LANGUAGE SQL
AS
$$
DECLARE
    schema_exists BOOLEAN;
    harvest_schema_exists BOOLEAN;
    harvest_excluded BOOLEAN;
    WH_NAME STRING;
    EAI_LIST STRING;
    key_secret BOOLEAN;
BEGIN
    IF (WAREHOUSE_NAME IS NULL) THEN
      WH_NAME := 'APP_XSMALL';
    ELSE
      WH_NAME := WAREHOUSE_NAME;
    END IF;

    SELECT COUNT(*) > 0 INTO :schema_exists
    FROM INFORMATION_SCHEMA.SCHEMATA
    WHERE SCHEMA_NAME = :INSTANCE_NAME;

    IF (:schema_exists) then
      EXECUTE IMMEDIATE 'CREATE STAGE IF NOT EXISTS '||:INSTANCE_NAME||'.'||'BOT_GIT DIRECTORY = ( ENABLE = true ) ENCRYPTION = (TYPE = '||CHR(39)||'SNOWFLAKE_SSE'||chr(39)||')';
      EXECUTE IMMEDIATE 'GRANT READ, WRITE ON STAGE '||:INSTANCE_NAME||'.'||'BOT_GIT TO APPLICATION ROLE APP_PUBLIC';

      REVOKE USAGE ON FUNCTION APP1.deploy_bot(varchar) FROM APPLICATION ROLE APP_PUBLIC;

      DROP FUNCTION IF EXISTS APP1.configure_ngrok_token(varchar, varchar, varchar);

      REVOKE USAGE ON FUNCTION APP1.configure_slack_app_token(varchar, varchar) FROM APPLICATION ROLE APP_PUBLIC;

      REVOKE USAGE ON FUNCTION APP1.configure_llm(varchar, varchar) FROM APPLICATION ROLE APP_PUBLIC;

      -- REVOKE USAGE ON FUNCTION APP1.submit_udf(varchar, varchar, varchar) FROM APPLICATION ROLE APP_PUBLIC;

      -- REVOKE USAGE ON FUNCTION APP1.lookup_udf(varchar, varchar) FROM APPLICATION ROLE APP_PUBLIC;

      REVOKE USAGE ON FUNCTION APP1.get_slack_endpoints() FROM APPLICATION ROLE APP_PUBLIC;

      REVOKE USAGE ON FUNCTION APP1.list_available_bots() FROM APPLICATION ROLE APP_PUBLIC;

      DROP FUNCTION IF EXISTS APP1.get_ngrok_tokens();

      REVOKE USAGE ON FUNCTION APP1.get_metadata(varchar) FROM APPLICATION ROLE APP_PUBLIC;

      EXECUTE IMMEDIATE
        'CREATE FUNCTION if not exists '|| :INSTANCE_NAME ||'.set_metadata (metadata_type varchar)  RETURNS varchar SERVICE='|| :INSTANCE_NAME ||'.'|| :SERVICE_NAME ||' ENDPOINT=udfendpoint AS '||chr(39)||'/udf_proxy/set_metadata'||chr(39);

      EXECUTE IMMEDIATE
        'CREATE FUNCTION if not exists '|| :INSTANCE_NAME ||'.endpoint_router (op_name varchar, endpoint_name varchar, payload varchar)  RETURNS varchar SERVICE='|| :INSTANCE_NAME ||'.'|| :SERVICE_NAME ||' ENDPOINT=udfendpoint AS '||chr(39)||'/udf_proxy/endpoint_router'||chr(39);

      EXECUTE IMMEDIATE
        'GRANT USAGE ON FUNCTION '|| :INSTANCE_NAME ||'.endpoint_router ( varchar, varchar, varchar)  TO APPLICATION ROLE APP_PUBLIC';
      -- REVOKE USAGE ON FUNCTION APP1.get_artifact(varchar) FROM APPLICATION ROLE APP_PUBLIC;

      BEGIN
          SELECT
            CASE
                WHEN ARRAY_SIZE(PARSE_JSON(SYSTEM$GET_ALL_REFERENCES('private_key_secret'))) > 0
                THEN TRUE
                ELSE FALSE
            END AS has_references INTO key_secret;
      EXCEPTION
        WHEN STATEMENT_ERROR THEN
        key_secret := FALSE;
      END;

      IF (key_secret = TRUE and SERVICE_NAME = 'GENESISAPP_SERVICE_SERVICE') THEN

        // Add logic to check references for private_key_secret existence.
        LET spec VARCHAR := (
              SELECT REGEXP_REPLACE(
                REGEXP_REPLACE(VALUE
                          ,'{{app_db_sch}}',lower(current_database())||'.'||lower(:INSTANCE_NAME)),
                '(\\s+image:\\s.+)',
                '\\1\n        secrets:\n        - snowflakeSecret:\n            objectReference: \'private_key_secret\''
            ) AS updated_spec
            FROM APP.YAML
            WHERE NAME=:SERVICE_NAME and VALUE LIKE '%image:%');

          EXECUTE IMMEDIATE
          'ALTER SERVICE IF EXISTS '|| :INSTANCE_NAME ||'.'|| :SERVICE_NAME ||
          ' FROM SPECIFICATION  '||chr(36)||chr(36)||'\n'|| :spec ||'\n'||chr(36)||chr(36) ||
          ' ';
      ELSE
        LET spec VARCHAR := (
              SELECT REGEXP_REPLACE(VALUE
                ,'{{app_db_sch}}',lower(current_database())||'.'||lower(:INSTANCE_NAME)) AS VALUE
              FROM APP.YAML WHERE NAME=:SERVICE_NAME);

          EXECUTE IMMEDIATE
          'ALTER SERVICE IF EXISTS '|| :INSTANCE_NAME ||'.'|| :SERVICE_NAME ||
          ' FROM SPECIFICATION  '||chr(36)||chr(36)||'\n'|| :spec ||'\n'||chr(36)||chr(36) ||
          ' ';
      END IF;



      CALL CORE.GET_EAI_LIST(:INSTANCE_NAME) INTO :EAI_LIST;

      LET x INTEGER := 0;
      LET stmt VARCHAR := 'SELECT "name" as SERVICE_NAME, "schema_name" AS SCHEMA_NAME FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))';
      EXECUTE IMMEDIATE 'SHOW SERVICES IN SCHEMA ' ||:INSTANCE_NAME;
      LET RS1 RESULTSET := (EXECUTE IMMEDIATE :stmt);
      LET c1 CURSOR FOR RS1;
      FOR rec IN c1 DO
          IF (LEN(EAI_LIST) > 0) THEN
              EXECUTE IMMEDIATE
                'ALTER SERVICE IF EXISTS '|| :INSTANCE_NAME ||'.'|| :SERVICE_NAME ||
                ' SET ' ||
                ' QUERY_WAREHOUSE = '||:WH_NAME||
                ' EXTERNAL_ACCESS_INTEGRATIONS = (' || :EAI_LIST || ')';
          ELSE
            EXECUTE IMMEDIATE
              'ALTER SERVICE IF EXISTS '|| :INSTANCE_NAME ||'.'|| :SERVICE_NAME ||
              ' SET ' ||
              ' QUERY_WAREHOUSE = '||:WH_NAME;
          END IF;

        x := x + 1;
      END FOR;

      IF (x < 4) THEN
        CALL APP.RECREATE_APP_INSTANCE(:INSTANCE_NAME, :C_POOL_NAME, :WH_NAME);
      END IF;


      EXECUTE IMMEDIATE
          'GRANT USAGE ON SERVICE '|| :INSTANCE_NAME ||'.'|| :SERVICE_NAME ||' TO APPLICATION ROLE APP_PUBLIC';
      EXECUTE IMMEDIATE
          'GRANT MONITOR ON SERVICE '|| :INSTANCE_NAME ||'.'|| :SERVICE_NAME || ' TO APPLICATION ROLE APP_PUBLIC';
      EXECUTE IMMEDIATE
          'GRANT SERVICE ROLE '|| :INSTANCE_NAME ||'.'|| :SERVICE_NAME || '!' || :SERVICE_NAME || '_ROLE TO APPLICATION ROLE APP_PUBLIC';

        IF (UPDATE_HARVEST_METADATA) THEN
          -- Check if the APP1.HARVEST_RESULTS table exists and then delete specific rows from harvest_data
          SELECT COUNT(*) > 0 INTO :harvest_schema_exists FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = :INSTANCE_NAME AND TABLE_NAME = 'HARVEST_RESULTS';

          SELECT IFF(COUNT(*)>0, 1, 0) INTO :harvest_excluded
          FROM APP1.HARVEST_CONTROL
          WHERE DATABASE_NAME = :APP_NAME
              AND (ARRAY_CONTAINS('BASEBALL'::variant, SCHEMA_EXCLUSIONS) OR ARRAY_CONTAINS('FORMULA_1'::variant, SCHEMA_EXCLUSIONS)) ;

          IF  (:harvest_schema_exists AND NOT :harvest_excluded) THEN


            EXECUTE IMMEDIATE '
            MERGE INTO APP1.HARVEST_RESULTS AS target
            USING (
                SELECT
                    SOURCE_NAME,
                    REPLACE(QUALIFIED_TABLE_NAME, ''APP_NAME'', ''' || :APP_NAME || ''') AS QUALIFIED_TABLE_NAME,
                    ''' || :APP_NAME || ''' AS DATABASE_NAME,
                    MEMORY_UUID,
                    SCHEMA_NAME,
                    TABLE_NAME,
                    REPLACE(COMPLETE_DESCRIPTION, ''APP_NAME'', ''' || :APP_NAME || ''') AS COMPLETE_DESCRIPTION,
                    REPLACE(DDL, ''APP_NAME'', ''' || :APP_NAME || ''') AS DDL,
                    REPLACE(DDL_SHORT, ''APP_NAME'', ''' || :APP_NAME || ''') AS DDL_SHORT,
                    ''SHARED_VIEW'' AS DDL_HASH,
                    REPLACE(SUMMARY, ''APP_NAME'', ''' || :APP_NAME || ''') AS SUMMARY,
                    SAMPLE_DATA_TEXT,
                    LAST_CRAWLED_TIMESTAMP,
                    CRAWL_STATUS,
                    ROLE_USED_FOR_CRAWL
                FROM SHARED_HARVEST.HARVEST_RESULTS
                WHERE DATABASE_NAME = ''APP_NAME''
                AND SCHEMA_NAME IN (''BASEBALL'', ''FORMULA_1'')
            ) AS source
            ON target.QUALIFIED_TABLE_NAME = source.QUALIFIED_TABLE_NAME AND target.DDL = source.DDL
            WHEN MATCHED THEN
                UPDATE SET
                    target.SOURCE_NAME = source.SOURCE_NAME,
                    target.MEMORY_UUID = source.MEMORY_UUID,
                    target.SCHEMA_NAME = source.SCHEMA_NAME,
                    target.TABLE_NAME = source.TABLE_NAME,
                    target.COMPLETE_DESCRIPTION = source.COMPLETE_DESCRIPTION,
                    target.DDL = source.DDL,
                    target.DDL_SHORT = source.DDL_SHORT,
                    target.DDL_HASH = source.DDL_HASH,
                    target.SUMMARY = source.SUMMARY,
                    target.SAMPLE_DATA_TEXT = source.SAMPLE_DATA_TEXT,
                    target.LAST_CRAWLED_TIMESTAMP = source.LAST_CRAWLED_TIMESTAMP,
                    target.CRAWL_STATUS = source.CRAWL_STATUS,
                    target.ROLE_USED_FOR_CRAWL = source.ROLE_USED_FOR_CRAWL
            WHEN NOT MATCHED THEN
                INSERT (SOURCE_NAME, QUALIFIED_TABLE_NAME, DATABASE_NAME, MEMORY_UUID, SCHEMA_NAME, TABLE_NAME, COMPLETE_DESCRIPTION, DDL, DDL_SHORT, DDL_HASH, SUMMARY, SAMPLE_DATA_TEXT, LAST_CRAWLED_TIMESTAMP, CRAWL_STATUS, ROLE_USED_FOR_CRAWL)
                VALUES (
                    source.SOURCE_NAME,
                    source.QUALIFIED_TABLE_NAME,
                    source.DATABASE_NAME,
                    source.MEMORY_UUID,
                    source.SCHEMA_NAME,
                    source.TABLE_NAME,
                    source.COMPLETE_DESCRIPTION,
                    source.DDL,
                    source.DDL_SHORT,
                    source.DDL_HASH,
                    source.SUMMARY,
                    source.SAMPLE_DATA_TEXT,
                    source.LAST_CRAWLED_TIMESTAMP,
                    source.CRAWL_STATUS,
                    source.ROLE_USED_FOR_CRAWL
                );
            ';


--           EXECUTE IMMEDIATE 'DELETE FROM APP1.HARVEST_RESULTS WHERE DATABASE_NAME = ''' || :APP_NAME || ''' AND SCHEMA_NAME IN (''BASEBALL'', ''FORMULA_1'')';
--           EXECUTE IMMEDIATE 'INSERT INTO APP1.HARVEST_RESULTS (SOURCE_NAME, QUALIFIED_TABLE_NAME, DATABASE_NAME, MEMORY_UUID, SCHEMA_NAME, TABLE_NAME, COMPLETE_DESCRIPTION, DDL, DDL_SHORT, DDL_HASH, SUMMARY, SAMPLE_DATA_TEXT, LAST_CRAWLED_TIMESTAMP, CRAWL_STATUS, ROLE_USED_FOR_CRAWL)
--                               SELECT SOURCE_NAME, replace(QUALIFIED_TABLE_NAME,''APP_NAME'',''' || :APP_NAME || ''') QUALIFIED_TABLE_NAME, ''' || :APP_NAME || ''' DATABASE_NAME, MEMORY_UUID, SCHEMA_NAME, TABLE_NAME, REPLACE(COMPLETE_DESCRIPTION,''APP_NAME'',''' || :APP_NAME || ''') COMPLETE_DESCRIPTION, REPLACE(DDL,''APP_NAME'',''' || :APP_NAME || ''') DDL, REPLACE(DDL_SHORT,''APP_NAME'',''' || :APP_NAME || ''') DDL_SHORT, ''SHARED_VIEW'' DDL_HASH, REPLACE(SUMMARY,''APP_NAME'',''' || :APP_NAME || ''') SUMMARY, SAMPLE_DATA_TEXT, LAST_CRAWLED_TIMESTAMP, CRAWL_STATUS, ROLE_USED_FOR_CRAWL
--  FROM SHARED_HARVEST.HARVEST_RESULTS WHERE DATABASE_NAME = ''APP_NAME'' AND SCHEMA_NAME IN (''BASEBALL'', ''FORMULA_1'')';
        END IF;
      END IF;
    END IF;

END;
$$
;



CREATE OR REPLACE PROCEDURE APP.WAIT_FOR_STARTUP(INSTANCE_NAME VARCHAR, SERVICE_NAME VARCHAR, MAX_WAIT INTEGER)
RETURNS VARCHAR NOT NULL
LANGUAGE SQL
AS
$$
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
$$
;


CREATE OR REPLACE PROCEDURE APP.CREATE_SERVER_SERVICE(INSTANCE_NAME VARCHAR,SERVICE_NAME VARCHAR, POOL_NAME VARCHAR, WAREHOUSE_NAME VARCHAR, APP_DATABASE VARCHAR)
RETURNS VARCHAR NOT NULL
LANGUAGE SQL
AS
$$
  DECLARE
    EAI_LIST STRING;
  BEGIN
 LET spec VARCHAR := (
      SELECT REGEXP_REPLACE(VALUE
        ,'{{app_db_sch}}',lower(:APP_DATABASE)||'.'||lower(:INSTANCE_NAME)) AS VALUE
      FROM APP.YAML WHERE NAME=:SERVICE_NAME);

  CALL CORE.GET_EAI_LIST(:INSTANCE_NAME) INTO :EAI_LIST;

  IF (LEN(EAI_LIST) > 0) THEN
    EXECUTE IMMEDIATE
      'CREATE SERVICE IF NOT EXISTS '|| :INSTANCE_NAME ||'.'|| :SERVICE_NAME ||
      ' IN COMPUTE POOL  '|| :POOL_NAME ||
      ' FROM SPECIFICATION  '||chr(36)||chr(36)||'\n'|| :spec ||'\n'||chr(36)||chr(36) ||
      ' EXTERNAL_ACCESS_INTEGRATIONS = (' || :EAI_LIST || ')' ||
      ' QUERY_WAREHOUSE = '||:WAREHOUSE_NAME;
  ELSE
    EXECUTE IMMEDIATE
      'CREATE SERVICE IF NOT EXISTS '|| :INSTANCE_NAME ||'.'|| :SERVICE_NAME ||
      ' IN COMPUTE POOL  '|| :POOL_NAME ||
      ' FROM SPECIFICATION  '||chr(36)||chr(36)||'\n'|| :spec ||'\n'||chr(36)||chr(36) ||
      ' QUERY_WAREHOUSE = '||:WAREHOUSE_NAME;
  END IF;

 EXECUTE IMMEDIATE
   'GRANT USAGE ON SERVICE '|| :INSTANCE_NAME ||'.'|| :SERVICE_NAME ||' TO APPLICATION ROLE APP_PUBLIC';
 EXECUTE IMMEDIATE
   'GRANT MONITOR ON SERVICE '|| :INSTANCE_NAME ||'.'|| :SERVICE_NAME || ' TO APPLICATION ROLE APP_PUBLIC';
 EXECUTE IMMEDIATE
   'GRANT SERVICE ROLE '|| :INSTANCE_NAME ||'.'|| :SERVICE_NAME || '!' || :SERVICE_NAME || '_ROLE TO APPLICATION ROLE APP_PUBLIC';

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
   'CREATE or replace FUNCTION '|| :INSTANCE_NAME ||'.set_metadata (metadata_type varchar)  RETURNS varchar SERVICE='|| :INSTANCE_NAME ||'.'|| :SERVICE_NAME ||' ENDPOINT=udfendpoint AS '||chr(39)||'/udf_proxy/set_metadata'||chr(39);

 EXECUTE IMMEDIATE
   'CREATE or replace FUNCTION '|| :INSTANCE_NAME ||'.get_artifact (artifact_id varchar)  RETURNS varchar SERVICE='|| :INSTANCE_NAME ||'.'|| :SERVICE_NAME ||' ENDPOINT=udfendpoint AS '||chr(39)||'/udf_proxy/get_artifact'||chr(39);


 EXECUTE IMMEDIATE
   'CREATE or replace FUNCTION '|| :INSTANCE_NAME ||'.configure_llm (llm_type varchar, api_key varchar)  RETURNS varchar SERVICE='|| :INSTANCE_NAME ||'.'|| :SERVICE_NAME ||' ENDPOINT=udfendpoint AS '||chr(39)||'/udf_proxy/configure_llm'||chr(39);

  EXECUTE IMMEDIATE
   'CREATE or replace FUNCTION '|| :INSTANCE_NAME ||'.configure_slack_app_token (token varchar, refresh varchar)  RETURNS varchar SERVICE='|| :INSTANCE_NAME ||'.'|| :SERVICE_NAME ||' ENDPOINT=udfendpoint AS '||chr(39)||'/udf_proxy/configure_slack_app_token'||chr(39);

     EXECUTE IMMEDIATE
   'CREATE or replace FUNCTION '|| :INSTANCE_NAME ||'.configure_ngrok_token (auth_token varchar, use_domain varchar, domain varchar)  RETURNS varchar SERVICE='|| :INSTANCE_NAME ||'.'|| :SERVICE_NAME ||' ENDPOINT=udfendpoint AS '||chr(39)||'/udf_proxy/configure_ngrok_token'||chr(39);

     EXECUTE IMMEDIATE
   'CREATE or replace FUNCTION '|| :INSTANCE_NAME ||'.deploy_bot (bot_id varchar)  RETURNS varchar SERVICE='|| :INSTANCE_NAME ||'.'|| :SERVICE_NAME ||' ENDPOINT=udfendpoint AS '||chr(39)||'/udf_proxy/deploy_bot'||chr(39);


     EXECUTE IMMEDIATE
   'CREATE or replace FUNCTION '|| :INSTANCE_NAME ||'.endpoint_router (op_name varchar, endpoint_name varchar, payload varchar)  RETURNS varchar SERVICE='|| :INSTANCE_NAME ||'.'|| :SERVICE_NAME ||' ENDPOINT=udfendpoint AS '||chr(39)||'/udf_proxy/endpoint_router'||chr(39);

-- EXECUTE IMMEDIATE
--   'GRANT USAGE ON FUNCTION '|| :INSTANCE_NAME ||'.deploy_bot ( varchar )  TO APPLICATION ROLE APP_PUBLIC';


--EXECUTE IMMEDIATE
--   'GRANT USAGE ON FUNCTION '|| :INSTANCE_NAME ||'.configure_ngrok_token ( varchar, varchar, varchar)  TO APPLICATION ROLE APP_PUBLIC';

 -- EXECUTE IMMEDIATE
 --  'GRANT USAGE ON FUNCTION '|| :INSTANCE_NAME ||'.configure_slack_app_token ( varchar, varchar)  TO APPLICATION ROLE APP_PUBLIC';

 --EXECUTE IMMEDIATE
--   'GRANT USAGE ON FUNCTION '|| :INSTANCE_NAME ||'.configure_llm ( varchar, varchar)  TO APPLICATION ROLE APP_PUBLIC';
EXECUTE IMMEDIATE
   'GRANT USAGE ON FUNCTION '|| :INSTANCE_NAME ||'.submit_udf ( varchar, varchar, varchar)  TO APPLICATION ROLE APP_PUBLIC';
EXECUTE IMMEDIATE
   'GRANT USAGE ON FUNCTION '|| :INSTANCE_NAME ||'.lookup_udf ( varchar, varchar)  TO APPLICATION ROLE APP_PUBLIC';
EXECUTE IMMEDIATE
   'GRANT USAGE ON FUNCTION '|| :INSTANCE_NAME ||'.endpoint_router ( varchar, varchar, varchar)  TO APPLICATION ROLE APP_PUBLIC';
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
$$
;




CREATE OR REPLACE PROCEDURE APP.CREATE_HARVESTER_SERVICE(INSTANCE_NAME VARCHAR,SERVICE_NAME VARCHAR, POOL_NAME VARCHAR, WAREHOUSE_NAME VARCHAR, APP_DATABASE VARCHAR)
RETURNS VARCHAR NOT NULL
LANGUAGE SQL
AS
$$
  DECLARE
    EAI_LIST STRING;
  BEGIN
 LET spec VARCHAR := (
      SELECT REGEXP_REPLACE(VALUE
        ,'{{app_db_sch}}',lower(:APP_DATABASE)||'.'||lower(:INSTANCE_NAME)) AS VALUE
      FROM APP.YAML WHERE NAME=:SERVICE_NAME);

  CALL CORE.GET_EAI_LIST(:INSTANCE_NAME) INTO :EAI_LIST;

  IF (LEN(EAI_LIST) > 0) THEN
    EXECUTE IMMEDIATE
      'CREATE SERVICE IF NOT EXISTS '|| :INSTANCE_NAME ||'.'|| :SERVICE_NAME ||
      ' IN COMPUTE POOL  '|| :POOL_NAME ||
      ' FROM SPECIFICATION  '||chr(36)||chr(36)||'\n'|| :spec ||'\n'||chr(36)||chr(36) ||
      ' EXTERNAL_ACCESS_INTEGRATIONS = (' || :EAI_LIST || ')' ||
      ' QUERY_WAREHOUSE = '||:WAREHOUSE_NAME;
  ELSE
    EXECUTE IMMEDIATE
      'CREATE SERVICE IF NOT EXISTS '|| :INSTANCE_NAME ||'.'|| :SERVICE_NAME ||
      ' IN COMPUTE POOL  '|| :POOL_NAME ||
      ' FROM SPECIFICATION  '||chr(36)||chr(36)||'\n'|| :spec ||'\n'||chr(36)||chr(36) ||
      ' QUERY_WAREHOUSE = '||:WAREHOUSE_NAME;
  END IF;

 EXECUTE IMMEDIATE
   'GRANT USAGE ON SERVICE '|| :INSTANCE_NAME ||'.'|| :SERVICE_NAME ||' TO APPLICATION ROLE APP_PUBLIC';
 EXECUTE IMMEDIATE
   'GRANT MONITOR ON SERVICE '|| :INSTANCE_NAME ||'.'|| :SERVICE_NAME || ' TO APPLICATION ROLE APP_PUBLIC';
 EXECUTE IMMEDIATE
   'GRANT SERVICE ROLE '|| :INSTANCE_NAME ||'.'|| :SERVICE_NAME || '!' || :SERVICE_NAME || '_ROLE TO APPLICATION ROLE APP_PUBLIC';

 RETURN 'service created';
END
$$
;


CREATE OR REPLACE PROCEDURE APP.CREATE_KNOWLEDGE_SERVICE(INSTANCE_NAME VARCHAR,SERVICE_NAME VARCHAR, POOL_NAME VARCHAR, WAREHOUSE_NAME VARCHAR, APP_DATABASE VARCHAR)
RETURNS VARCHAR NOT NULL
LANGUAGE SQL
AS
$$
  DECLARE
    EAI_LIST STRING;
  BEGIN
 LET spec VARCHAR := (
      SELECT REGEXP_REPLACE(VALUE
        ,'{{app_db_sch}}',lower(:APP_DATABASE)||'.'||lower(:INSTANCE_NAME)) AS VALUE
      FROM APP.YAML WHERE NAME=:SERVICE_NAME);

  CALL CORE.GET_EAI_LIST(:INSTANCE_NAME) INTO :EAI_LIST;

  IF (LEN(EAI_LIST) > 0) THEN
    EXECUTE IMMEDIATE
      'CREATE SERVICE IF NOT EXISTS '|| :INSTANCE_NAME ||'.'|| :SERVICE_NAME ||
      ' IN COMPUTE POOL  '|| :POOL_NAME ||
      ' FROM SPECIFICATION  '||chr(36)||chr(36)||'\n'|| :spec ||'\n'||chr(36)||chr(36) ||
      ' EXTERNAL_ACCESS_INTEGRATIONS = (' || :EAI_LIST || ')' ||
      ' QUERY_WAREHOUSE = '||:WAREHOUSE_NAME;
  ELSE
    EXECUTE IMMEDIATE
      'CREATE SERVICE IF NOT EXISTS '|| :INSTANCE_NAME ||'.'|| :SERVICE_NAME ||
      ' IN COMPUTE POOL  '|| :POOL_NAME ||
      ' FROM SPECIFICATION  '||chr(36)||chr(36)||'\n'|| :spec ||'\n'||chr(36)||chr(36) ||
      ' QUERY_WAREHOUSE = '||:WAREHOUSE_NAME;
  END IF;

 EXECUTE IMMEDIATE
   'GRANT USAGE ON SERVICE '|| :INSTANCE_NAME ||'.'|| :SERVICE_NAME ||' TO APPLICATION ROLE APP_PUBLIC';
 EXECUTE IMMEDIATE
   'GRANT MONITOR ON SERVICE '|| :INSTANCE_NAME ||'.'|| :SERVICE_NAME || ' TO APPLICATION ROLE APP_PUBLIC';
 EXECUTE IMMEDIATE
   'GRANT SERVICE ROLE '|| :INSTANCE_NAME ||'.'|| :SERVICE_NAME || '!' || :SERVICE_NAME || '_ROLE TO APPLICATION ROLE APP_PUBLIC';

 RETURN 'service created';
END
$$
;


CREATE OR REPLACE PROCEDURE APP.CREATE_TASK_SERVICE(INSTANCE_NAME VARCHAR,SERVICE_NAME VARCHAR, POOL_NAME VARCHAR, WAREHOUSE_NAME VARCHAR, APP_DATABASE VARCHAR)
RETURNS VARCHAR NOT NULL
LANGUAGE SQL
AS
$$
  DECLARE
    EAI_LIST STRING;
  BEGIN
 LET spec VARCHAR := (
      SELECT REGEXP_REPLACE(VALUE
        ,'{{app_db_sch}}',lower(:APP_DATABASE)||'.'||lower(:INSTANCE_NAME)) AS VALUE
      FROM APP.YAML WHERE NAME=:SERVICE_NAME);

  CALL CORE.GET_EAI_LIST(:INSTANCE_NAME) INTO :EAI_LIST;

  IF (LEN(EAI_LIST) > 0) THEN
    EXECUTE IMMEDIATE
      'CREATE SERVICE IF NOT EXISTS '|| :INSTANCE_NAME ||'.'|| :SERVICE_NAME ||
      ' IN COMPUTE POOL  '|| :POOL_NAME ||
      ' FROM SPECIFICATION  '||chr(36)||chr(36)||'\n'|| :spec ||'\n'||chr(36)||chr(36) ||
      ' EXTERNAL_ACCESS_INTEGRATIONS = (' || :EAI_LIST || ')' ||
      ' QUERY_WAREHOUSE = '||:WAREHOUSE_NAME;
  ELSE
    EXECUTE IMMEDIATE
      'CREATE SERVICE IF NOT EXISTS '|| :INSTANCE_NAME ||'.'|| :SERVICE_NAME ||
      ' IN COMPUTE POOL  '|| :POOL_NAME ||
      ' FROM SPECIFICATION  '||chr(36)||chr(36)||'\n'|| :spec ||'\n'||chr(36)||chr(36) ||
      ' QUERY_WAREHOUSE = '||:WAREHOUSE_NAME;
  END IF;

 EXECUTE IMMEDIATE
   'GRANT USAGE ON SERVICE '|| :INSTANCE_NAME ||'.'|| :SERVICE_NAME ||' TO APPLICATION ROLE APP_PUBLIC';
 EXECUTE IMMEDIATE
   'GRANT MONITOR ON SERVICE '|| :INSTANCE_NAME ||'.'|| :SERVICE_NAME || ' TO APPLICATION ROLE APP_PUBLIC';
 EXECUTE IMMEDIATE
   'GRANT SERVICE ROLE '|| :INSTANCE_NAME ||'.'|| :SERVICE_NAME || '!' || :SERVICE_NAME || '_ROLE TO APPLICATION ROLE APP_PUBLIC';

 RETURN 'service created';
END
$$
;

CREATE OR REPLACE PROCEDURE CORE.INITIALIZE_APP_INSTANCE( INSTANCE_NAME VARCHAR, POOL_NAME VARCHAR, APP_WAREHOUSE VARCHAR)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    v_current_database STRING;
    WAREHOUSE_NAME STRING;
BEGIN
  WAREHOUSE_NAME := :APP_WAREHOUSE;

  SELECT CURRENT_DATABASE() INTO :v_current_database;

  EXECUTE IMMEDIATE 'CREATE SCHEMA '||:INSTANCE_NAME;
  EXECUTE IMMEDIATE 'GRANT USAGE ON SCHEMA '||:INSTANCE_NAME||' TO APPLICATION ROLE APP_PUBLIC';

  EXECUTE IMMEDIATE 'CREATE STAGE IF NOT EXISTS '||:INSTANCE_NAME||'.'||'WORKSPACE DIRECTORY = ( ENABLE = true ) ENCRYPTION = (TYPE = '||CHR(39)||'SNOWFLAKE_SSE'||chr(39)||')';
  EXECUTE IMMEDIATE 'GRANT READ ON STAGE '||:INSTANCE_NAME||'.'||'WORKSPACE TO APPLICATION ROLE APP_PUBLIC';

  EXECUTE IMMEDIATE 'CREATE OR REPLACE STAGE '||:INSTANCE_NAME||'.'||'BOT_GIT DIRECTORY = ( ENABLE = true ) ENCRYPTION = (TYPE = '||CHR(39)||'SNOWFLAKE_SSE'||chr(39)||')';
  EXECUTE IMMEDIATE 'GRANT READ, WRITE ON STAGE '||:INSTANCE_NAME||'.'||'BOT_GIT TO APPLICATION ROLE APP_PUBLIC';

  CALL APP.CREATE_SERVER_SERVICE(:INSTANCE_NAME,'GENESISAPP_SERVICE_SERVICE',:POOL_NAME,:WAREHOUSE_NAME, :v_current_database);
  CALL APP.CREATE_HARVESTER_SERVICE(:INSTANCE_NAME,'GENESISAPP_HARVESTER_SERVICE',:POOL_NAME, :WAREHOUSE_NAME, :v_current_database);
  CALL APP.CREATE_KNOWLEDGE_SERVICE(:INSTANCE_NAME,'GENESISAPP_KNOWLEDGE_SERVICE',:POOL_NAME, :WAREHOUSE_NAME, :v_current_database);
  CALL APP.CREATE_TASK_SERVICE(:INSTANCE_NAME,'GENESISAPP_TASK_SERVICE',:POOL_NAME, :WAREHOUSE_NAME, :v_current_database);
  CALL APP.WAIT_FOR_STARTUP(:INSTANCE_NAME,'GENESISAPP_SERVICE_SERVICE',600);

  RETURN :v_current_database||'.'||:INSTANCE_NAME||'.GENESISAPP_SERVICE_SERVICE';

END
$$
;


GRANT USAGE ON PROCEDURE CORE.INITIALIZE_APP_INSTANCE(VARCHAR, VARCHAR, VARCHAR) TO  APPLICATION ROLE APP_PUBLIC;

CREATE OR REPLACE PROCEDURE CORE.SET_DEFAULT_EMAIL(default_email VARCHAR)
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
  CREATE OR REPLACE TABLE APP1.DEFAULT_EMAIL (
    DEFAULT_EMAIL VARCHAR
  );

  INSERT INTO APP1.DEFAULT_EMAIL (DEFAULT_EMAIL)
  VALUES (:default_email);

  RETURN 'Default email set successfully to '||:default_email;
END;
$$
;

GRANT USAGE ON PROCEDURE CORE.SET_DEFAULT_EMAIL(VARCHAR) TO APPLICATION ROLE APP_PUBLIC;

CREATE OR REPLACE PROCEDURE CORE.GET_APP_ENDPOINT(INSTANCE_NAME VARCHAR)
RETURNS TABLE(VARCHAR, INTEGER, VARCHAR, VARCHAR, VARCHAR  )
LANGUAGE SQL
AS
$$
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
$$
;
GRANT USAGE ON PROCEDURE CORE.GET_APP_ENDPOINT(VARCHAR) TO  APPLICATION ROLE APP_PUBLIC;


CREATE OR REPLACE PROCEDURE CORE.START_APP_INSTANCE(INSTANCE_NAME VARCHAR, POOL_NAME VARCHAR, APP_WAREHOUSE VARCHAR)
RETURNS TABLE(SERVICE_NAME VARCHAR,CONTAINER_NAME VARCHAR,STATUS VARCHAR, MESSAGE VARCHAR)
LANGUAGE SQL
AS
$$
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

 IF (x < 4) THEN
   CALL APP.RECREATE_APP_INSTANCE(:INSTANCE_NAME, :POOL_NAME, :APP_WAREHOUSE);
 END IF;

 LET RS3 RESULTSET := (CALL CORE.LIST_APP_INSTANCE(:INSTANCE_NAME));
 RETURN TABLE(RS3);
END;
$$
;
GRANT USAGE ON PROCEDURE CORE.START_APP_INSTANCE(VARCHAR,VARCHAR,VARCHAR) TO  APPLICATION ROLE APP_PUBLIC;


CREATE OR REPLACE PROCEDURE CORE.STOP_APP_INSTANCE(INSTANCE_NAME VARCHAR)
RETURNS TABLE(SERVICE_NAME VARCHAR,CONTAINER_NAME VARCHAR,STATUS VARCHAR, MESSAGE VARCHAR)
LANGUAGE SQL
AS
$$
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
$$
;
GRANT USAGE ON PROCEDURE CORE.STOP_APP_INSTANCE(VARCHAR) TO  APPLICATION ROLE APP_PUBLIC;


CREATE OR REPLACE PROCEDURE CORE.DROP_APP_INSTANCE(INSTANCE_NAME VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
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
$$
;
GRANT USAGE ON PROCEDURE CORE.DROP_APP_INSTANCE(VARCHAR) TO APPLICATION ROLE APP_PUBLIC;


CREATE OR REPLACE PROCEDURE CORE.RESTART_APP_INSTANCE(INSTANCE_NAME VARCHAR)
RETURNS TABLE(SERVICE_NAME VARCHAR,CONTAINER_NAME VARCHAR,STATUS VARCHAR, MESSAGE VARCHAR)
LANGUAGE SQL
AS
$$
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
$$
;
GRANT USAGE ON PROCEDURE CORE.RESTART_APP_INSTANCE(VARCHAR) TO APPLICATION ROLE APP_PUBLIC;

CREATE OR REPLACE PROCEDURE APP.RECREATE_APP_INSTANCE( INSTANCE_NAME VARCHAR, POOL_NAME VARCHAR, APP_WAREHOUSE VARCHAR)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    v_current_database STRING;
BEGIN
  SELECT CURRENT_DATABASE() INTO :v_current_database;

  CALL APP.CREATE_SERVER_SERVICE(:INSTANCE_NAME,'GENESISAPP_SERVICE_SERVICE',:POOL_NAME, :APP_WAREHOUSE, :v_current_database);
  CALL APP.CREATE_HARVESTER_SERVICE(:INSTANCE_NAME,'GENESISAPP_HARVESTER_SERVICE',:POOL_NAME, :APP_WAREHOUSE, :v_current_database);
  CALL APP.CREATE_KNOWLEDGE_SERVICE(:INSTANCE_NAME,'GENESISAPP_KNOWLEDGE_SERVICE',:POOL_NAME, :APP_WAREHOUSE, :v_current_database);
  CALL APP.CREATE_TASK_SERVICE(:INSTANCE_NAME,'GENESISAPP_TASK_SERVICE',:POOL_NAME, :APP_WAREHOUSE, :v_current_database);
  CALL APP.WAIT_FOR_STARTUP(:INSTANCE_NAME,'GENESISAPP_SERVICE_SERVICE',600);

  RETURN :v_current_database||'.'||:INSTANCE_NAME||'.GENESISAPP_SERVICE_SERVICE';

END
$$
;

CREATE OR REPLACE PROCEDURE CORE.UPGRADE_SERVICES()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    TABLE_EXISTS BOOLEAN;
    WAREHOUSE_NAME STRING;
    INSTANCE_NAME STRING := 'APP1';
BEGIN
    -- Show warehouses and set the warehouse name
    LET RS RESULTSET := (EXECUTE IMMEDIATE 'SHOW WAREHOUSES');
    SELECT "name" INTO :WAREHOUSE_NAME
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
    WHERE "name" NOT IN ('SYSTEM$STREAMLIT_NOTEBOOK_WH','APP_XSMALL','APP_XSMALL_1')
    LIMIT 1;

    -- Upgrade services
    CALL APP.UPGRADE_APP('APP1', 'GENESISAPP_SERVICE_SERVICE', TRUE, CURRENT_DATABASE(), 'GENESIS_DEMO_POOL', :WAREHOUSE_NAME);
    CALL APP.UPGRADE_APP('APP1', 'GENESISAPP_HARVESTER_SERVICE', FALSE, CURRENT_DATABASE(), 'GENESIS_DEMO_POOL', :WAREHOUSE_NAME);
    CALL APP.UPGRADE_APP('APP1', 'GENESISAPP_KNOWLEDGE_SERVICE', FALSE, CURRENT_DATABASE(), 'GENESIS_DEMO_POOL', :WAREHOUSE_NAME);
    CALL APP.UPGRADE_APP('APP1', 'GENESISAPP_TASK_SERVICE', FALSE, CURRENT_DATABASE(), 'GENESIS_DEMO_POOL', :WAREHOUSE_NAME);

    RETURN 'Services upgraded successfully';
END;
$$;
GRANT USAGE ON PROCEDURE CORE.UPGRADE_SERVICES() TO APPLICATION ROLE APP_PUBLIC;

CALL CORE.UPGRADE_SERVICES();


CREATE OR REPLACE PROCEDURE CORE.TEST_BILLING_EVENT()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
 EXECUTE IMMEDIATE 'SELECT SYSTEM$CREATE_BILLING_EVENT(\'TEST_BILL_EVENT\',\'\',CURRENT_TIMESTAMP(),CURRENT_TIMESTAMP(),10,\'\',\'\')';
 RETURN 'BILLED';
END;
$$
;


GRANT USAGE ON PROCEDURE CORE.TEST_BILLING_EVENT() TO  APPLICATION ROLE APP_PUBLIC;



CREATE OR REPLACE PROCEDURE CORE.LIST_APP_INSTANCE(INSTANCE_NAME VARCHAR)
RETURNS TABLE(SERVICE_NAME VARCHAR,CONTAINER_NAME VARCHAR,STATUS VARCHAR, MESSAGE VARCHAR)
LANGUAGE SQL
AS
$$
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
$$
;

GRANT USAGE ON PROCEDURE CORE.LIST_APP_INSTANCE(VARCHAR) TO APPLICATION ROLE APP_PUBLIC;


CREATE OR REPLACE PROCEDURE CORE.GET_POOLS()
RETURNS TABLE(NAME VARCHAR, STATE VARCHAR)
LANGUAGE SQL
AS
$$
BEGIN
 LET stmt VARCHAR := 'SELECT NAME, STATE FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))';
 EXECUTE IMMEDIATE 'SHOW COMPUTE POOLS';
 LET RS1 RESULTSET := (EXECUTE IMMEDIATE :stmt);
 RETURN TABLE(RS1);
END;
$$
;
GRANT USAGE ON PROCEDURE CORE.GET_POOLS() TO APPLICATION ROLE APP_PUBLIC;


GRANT USAGE ON STREAMLIT CORE.GENESIS TO APPLICATION ROLE app_public;

CREATE OR REPLACE PROCEDURE CORE.RUN_ARBITRARY(sql_query VARCHAR)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
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
$$;

GRANT USAGE ON PROCEDURE CORE.RUN_ARBITRARY(VARCHAR) TO APPLICATION ROLE app_public;


CREATE OR REPLACE PROCEDURE CORE.CREATE_MISSING_GRANT_VIEWS(INSTANCE_NAME STRING, APP_NAME STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
    DECLARE
        SQL_COMMAND STRING;
    BEGIN
        SQL_COMMAND := 'CREATE VIEW IF NOT EXISTS CORE.MISSING_DATABASE_GRANTS AS
        select distinct ''"'' || REPLACE(replace(database_name,''"'',''''), ''.'', ''"."'') || ''"'' database_name from ' || APP_NAME || '.' || INSTANCE_NAME || '.HARVEST_RESULTS
        where SPLIT_PART(replace(qualified_table_name,''"'',''''), ''.'', 1) <> ''SNOWFLAKE''
        AND SPLIT_PART(replace(qualified_table_name,''"'',''''), ''.'', 2) NOT IN (''BASEBALL'', ''FORMULA_1'')
        minus
        select distinct  ''"'' || REPLACE(replace(name,''"'',''''), ''.'', ''"."'') || ''"'' database_name
        from GENESIS_LOCAL_DB.GRANTS.GRANTS_TO_APP
        where granted_on  in (''DATABASE'')
        and SPLIT_PART(name, ''.'', 1) <> ''SNOWFLAKE''
        AND SPLIT_PART(name, ''.'', 2) NOT IN (''BASEBALL'', ''FORMULA_1'') ';
        EXECUTE IMMEDIATE SQL_COMMAND;

        SQL_COMMAND := 'CREATE VIEW IF NOT EXISTS CORE.MISSING_SCHEMA_GRANTS AS
        select ''"'' || REPLACE(replace(database_name || ''.'' || schema_name,''"'',''''), ''.'', ''"."'') || ''"'' database_schema_name from ' || APP_NAME || '.' || INSTANCE_NAME || '.HARVEST_RESULTS
        where SPLIT_PART(replace(qualified_table_name,''"'',''''), ''.'', 1) <> ''SNOWFLAKE''
        AND SPLIT_PART(replace(qualified_table_name,''"'',''''), ''.'', 2) NOT IN (''BASEBALL'', ''FORMULA_1'')
        minus
        select distinct  ''"'' || REPLACE(replace(name,''"'',''''), ''.'', ''"."'') || ''"'' database_schema_name
        from GENESIS_LOCAL_DB.GRANTS.GRANTS_TO_APP
        where granted_on  in (''SCHEMA'')
        and SPLIT_PART(name, ''.'', 1) <> ''SNOWFLAKE''
        AND SPLIT_PART(name, ''.'', 2) NOT IN (''BASEBALL'', ''FORMULA_1'') ';
        EXECUTE IMMEDIATE SQL_COMMAND;

        SQL_COMMAND := 'CREATE VIEW IF NOT EXISTS CORE.MISSING_OBJECT_GRANTS AS
        select qualified_table_name  from ' || APP_NAME || '.' || INSTANCE_NAME || '.HARVEST_RESULTS
        where SPLIT_PART(replace(qualified_table_name,''"'',''''), ''.'', 1) <> ''SNOWFLAKE''
        AND SPLIT_PART(replace(qualified_table_name,''"'',''''), ''.'', 2) NOT IN (''BASEBALL'', ''FORMULA_1'')
        minus
        select distinct ''"'' || REPLACE(replace(name,''"'',''''), ''.'', ''"."'') || ''"'' qualified_table_name
        from GENESIS_LOCAL_DB.GRANTS.GRANTS_TO_APP
        where granted_on  not in (''DATABASE'',''SCHEMA'')
        and SPLIT_PART(name, ''.'', 1) <> ''SNOWFLAKE''
        AND SPLIT_PART(name, ''.'', 2) NOT IN (''BASEBALL'', ''FORMULA_1'') ';
        EXECUTE IMMEDIATE SQL_COMMAND;

        EXECUTE IMMEDIATE 'GRANT SELECT ON VIEW CORE.MISSING_DATABASE_GRANTS TO APPLICATION ROLE APP_PUBLIC';
        EXECUTE IMMEDIATE 'GRANT SELECT ON VIEW CORE.MISSING_SCHEMA_GRANTS TO APPLICATION ROLE APP_PUBLIC';
        EXECUTE IMMEDIATE 'GRANT SELECT ON VIEW CORE.MISSING_OBJECT_GRANTS TO APPLICATION ROLE APP_PUBLIC';

        RETURN 'Views created successfully';
    END;

$$;


GRANT USAGE ON PROCEDURE CORE.CREATE_MISSING_GRANT_VIEWS(STRING, STRING) TO APPLICATION ROLE APP_PUBLIC;

CREATE OR REPLACE PROCEDURE CORE.CHECK_APPLICATION_SHARING()
RETURNS STRING
LANGUAGE SQL
AS
$$
    -- Execute the function to check if application sharing events are being shared with the provider
    DECLARE
      sharing_status STRING;
    BEGIN
      SELECT SYSTEM$IS_APPLICATION_SHARING_EVENTS_WITH_PROVIDER() INTO sharing_status;
      -- Return the result
      RETURN sharing_status;
    END;

$$;

GRANT USAGE ON PROCEDURE CORE.CHECK_APPLICATION_SHARING() TO APPLICATION ROLE APP_PUBLIC;

