// currently setup on DSHRNXX.CVB46967 account to backup the demo GENESIS_BOTS metadata

/* 
1. In the current GENESIS Bots app, ask Eliza to run the following statement:
  CALL core.run_arbitrary('GRANT USAGE ON PROCEDURE core.run_arbitrary(VARCHAR) TO APPLICATION ROLE app_public');
2. Using the script below:
2.a. Stop all services and compute pool
2.b. Backup all metadata from the currently installed app
*/

USE DATABASE GENESIS_BOTS;

call GENESIS_BOTS.core.stop_app_instance('APP1');
alter compute pool GENESIS_POOL SUSPEND; -- to pause the compute pool


CREATE DATABASE IF NOT EXISTS GENESIS_BACKUP;

/**** Backup stored proc ****/

CREATE OR REPLACE PROCEDURE GENESIS_BACKUP.PUBLIC.BACKUP_DATA(APP_NAME STRING, BACKUP_DATABASE STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    schema_name STRING;
    table_name STRING;
    stage_name STRING;
    create_command STRING;
    stage_command STRING;
    output STRING;
    sql STRING;
    rs_schemas RESULTSET;
    rs_tables RESULTSET;
    rs_stages RESULTSET;
BEGIN
     
    EXECUTE IMMEDIATE 'GRANT USAGE ON DATABASE ' || :BACKUP_DATABASE || ' TO APPLICATION ' || :APP_NAME;
    output := '\nDatabase ' || :BACKUP_DATABASE || ' granted to application ' || :APP_NAME || '.';
    sql := 'SELECT SCHEMA_NAME s_name FROM ' || APP_NAME || '.INFORMATION_SCHEMA.SCHEMATA WHERE CATALOG_NAME = ''' || APP_NAME || ''' AND SCHEMA_NAME NOT IN (''INFORMATION_SCHEMA'', ''PUBLIC'', ''APP'', ''CORE'')';
    rs_schemas := (EXECUTE IMMEDIATE :sql);

    -- Loop through each schema
    FOR schema_record IN rs_schemas DO
        schema_name := schema_record.s_name;

        -- grant objects
        EXECUTE IMMEDIATE 'CALL ' || :APP_NAME || '.CORE.RUN_ARBITRARY(''GRANT USAGE ON SCHEMA ' || :APP_NAME || '.' || :schema_name || ' TO APPLICATION ROLE APP_PUBLIC'')';
        EXECUTE IMMEDIATE 'CALL ' || :APP_NAME || '.CORE.RUN_ARBITRARY(''GRANT SELECT ON ALL TABLES IN SCHEMA ' || :APP_NAME || '.' || :schema_name || ' TO APPLICATION ROLE APP_PUBLIC'')';
        EXECUTE IMMEDIATE 'CALL ' || :APP_NAME || '.CORE.RUN_ARBITRARY(''GRANT READ ON ALL STAGES IN SCHEMA ' || :APP_NAME || '.' || :schema_name || ' TO APPLICATION ROLE APP_PUBLIC'')';
        output := :output || '\nGrants made on application schema ' || :APP_NAME || '.' || :schema_name || ' to application role successfully';
        
        create_command := 'CREATE SCHEMA IF NOT EXISTS ' || :BACKUP_DATABASE || '.' || :schema_name;
        EXECUTE IMMEDIATE create_command;
        EXECUTE IMMEDIATE 'GRANT USAGE ON SCHEMA ' || :BACKUP_DATABASE || '.' || :schema_name || ' TO APPLICATION ' || :APP_NAME;
        output := :output || '\n' || :BACKUP_DATABASE || '.' || :schema_name || ' schema created.';

        sql := 'SELECT TABLE_NAME FROM ' || APP_NAME || '.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ''' || schema_name || '''';
        rs_tables := (EXECUTE IMMEDIATE :sql);
        
        -- Loop through each table in the schema
        FOR table_record IN rs_tables DO
            table_name := table_record.TABLE_NAME;
            create_command := 'CREATE OR REPLACE TABLE ' || :BACKUP_DATABASE || '.' || :schema_name || '."' || :table_name || '" AS SELECT * FROM ' || :APP_NAME || '.' || :schema_name || '."' || :table_name || '"';
            EXECUTE IMMEDIATE create_command;
            output := :output || '\n' || :BACKUP_DATABASE || '.' || :schema_name || '.' || :table_name || ' table created and backed up.';
        END FOR;
        
        -- views TBD

        sql := 'SELECT STAGE_NAME FROM ' || APP_NAME || '.INFORMATION_SCHEMA.STAGES WHERE STAGE_SCHEMA = ''' || schema_name || '''';
        rs_stages := (EXECUTE IMMEDIATE :sql);
        
        -- Loop through each stage in the schema
        FOR stage_record IN rs_stages DO
            stage_name := stage_record.STAGE_NAME;
            create_command := 'CREATE OR REPLACE STAGE ' || :BACKUP_DATABASE || '.' || :schema_name || '.' || :stage_name;
            EXECUTE IMMEDIATE create_command;
            
            EXECUTE IMMEDIATE 'GRANT READ,WRITE ON STAGE ' || :BACKUP_DATABASE || '.' || :schema_name || '.' || :stage_name || ' TO APPLICATION ' || :APP_NAME;
            stage_command := 'COPY FILES INTO @' || :BACKUP_DATABASE || '.' || :schema_name || '.' || :stage_name || ' FROM @' || :APP_NAME || '.' || :schema_name || '.' || :stage_name;
            
            EXECUTE IMMEDIATE 'call ' || :APP_NAME || '.core.run_arbitrary(''' || :stage_command || ''')';
            output := :output || '\n' || :BACKUP_DATABASE || '.' || :schema_name || '.' || :stage_name || ' stage created and backed up.';
        END FOR;
    
    END FOR;

    output := :output || '\nBackup completed successfully';
 
    RETURN :output;
END;
$$;    

call GENESIS_BACKUP.PUBLIC.BACKUP_DATA('GENESIS_BOTS','GENESIS_BACKUP');


// Verify your back up data, then uninstall the Genesis Bots application via script below or from the Snowflake Data Products-->Apps UI
// DROP APPLICATION GENESIS_BOTS;





// if needed, create a task to backup metadtata that runs every morning  

DROP TASK GENESIS_BACKUP.PUBLIC.backup_data_task;
CREATE TASK GENESIS_BACKUP.PUBLIC.backup_data_task
  WAREHOUSE = XSMALL
  SCHEDULE = 'USING CRON 0 10 * * * UTC'
  AS
  CALL GENESIS_BACKUP.PUBLIC.BACKUP_DATA('GENESIS_BOTS','GENESIS_BACKUP');

ALTER TASK GENESIS_BACKUP.PUBLIC.backup_data_task RESUME;