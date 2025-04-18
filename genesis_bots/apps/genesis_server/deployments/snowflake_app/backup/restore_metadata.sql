/*** restoring data from backup to a new GENESIS_BOTS application ***/
/*
1. Install the Genesis Bots application from the Snowflake Marketplace listing
2. Start all services and input your OpenAI key
3. Ask Eliza to run the following statement:
  CALL core.run_arbitrary('GRANT USAGE ON PROCEDURE core.run_arbitrary(VARCHAR) TO APPLICATION ROLE app_public');
4. Using the script below:
4.a. Stop all services and suspend the compute pool
4.b. Restore the metadata and files
4.c. Once restored, start the compute pool and services
*/


call GENESIS_BOTS.core.stop_app_instance('APP1');
alter compute pool GENESIS_POOL SUSPEND; -- to pause the compute pool


-- TODO handle workspaces 

CREATE OR REPLACE PROCEDURE GENESIS_BACKUP.PUBLIC.RESTORE_DATA(APP_NAME STRING, BACKUP_DATABASE STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    schema_name STRING;
    table_name STRING;
    stage_name STRING;
    truncate_command STRING;
    stage_command STRING;
    restore_command STRING;
    output STRING;
    sql STRING;
    rs_schemas RESULTSET;
    rs_tables RESULTSET;
    rs_stages RESULTSET;
BEGIN

    EXECUTE IMMEDIATE 'GRANT USAGE ON DATABASE ' || :BACKUP_DATABASE || ' TO APPLICATION ' || :APP_NAME;
    EXECUTE IMMEDIATE 'GRANT USAGE ON ALL SCHEMAS IN DATABASE ' || :BACKUP_DATABASE || ' TO APPLICATION ' || :APP_NAME;
    output := '\nDatabase ' || :BACKUP_DATABASE || ' and all schemas granted to application.';

    sql := 'SELECT SCHEMA_NAME s_name FROM ' || BACKUP_DATABASE || '.INFORMATION_SCHEMA.SCHEMATA WHERE CATALOG_NAME = ''' || BACKUP_DATABASE || ''' AND SCHEMA_NAME NOT IN (''INFORMATION_SCHEMA'', ''ELIZA_34SXLO_WORKSPACE'',''PUBLIC'')';
    rs_schemas := (EXECUTE IMMEDIATE :sql);

    -- Loop through each schema
    FOR schema_record IN rs_schemas DO
        schema_name := schema_record.s_name;

        -- grant objects
        EXECUTE IMMEDIATE 'GRANT SELECT ON ALL TABLES IN SCHEMA ' || :BACKUP_DATABASE || '.' || :schema_name || ' TO APPLICATION ' || :APP_NAME;
        EXECUTE IMMEDIATE 'GRANT READ ON ALL STAGES IN SCHEMA ' || :BACKUP_DATABASE || '.' || :schema_name || ' TO APPLICATION ' || :APP_NAME;
        output := :output || '\nGrants made on all tables and stages in backup schema ' || :BACKUP_DATABASE || '.' || :schema_name || ' to application successfully';

        sql := 'SELECT ''INSERT INTO ' || APP_NAME || '.' || schema_name || '.'' || TABLE_NAME || ''('' || cols || '') SELECT '' || cols || '' FROM ' || BACKUP_DATABASE || '.' || schema_name || '.'' || TABLE_NAME as insert_stmt, TABLE_NAME from (select LISTAGG(COLUMN_NAME, '', '') WITHIN GROUP (ORDER BY ORDINAL_POSITION) cols, TABLE_NAME FROM GENESIS_BACKUP.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ''' || schema_name || ''' AND TABLE_NAME  IN (''BOT_SERVICING'',''HARVEST_RESULTS'',''HARVEST_CONTROL'',''SLACK_APP_CONFIG_TOKENS'', ''TASKS'', ''LLM_TOKENS'',''MESSAGE_LOG'', ''TASK_HISTORY'') GROUP BY TABLE_NAME, TABLE_SCHEMA)';
        
        rs_tables := (EXECUTE IMMEDIATE :sql);
        
        -- Loop through each table in the schema
        FOR table_record IN rs_tables DO
            table_name := table_record.TABLE_NAME;
            
            truncate_command := 'TRUNCATE TABLE ' || :APP_NAME || '.' || :schema_name || '.' || :table_name;
            EXECUTE IMMEDIATE 'call ' || :APP_NAME || '.core.run_arbitrary(''' || :truncate_command || ''')';

            restore_command := table_record.insert_stmt;
            EXECUTE IMMEDIATE 'call ' || :APP_NAME || '.core.run_arbitrary(''' || :restore_command || ''')';
            output := :output || '\n' || :APP_NAME || '.' || :schema_name || '.' || :table_name || ' table restored successfully.';
        END FOR;
        
        -- views TBD

        sql := 'SELECT STAGE_NAME FROM ' || BACKUP_DATABASE || '.INFORMATION_SCHEMA.STAGES WHERE STAGE_SCHEMA = ''' || schema_name || '''';
        rs_stages := (EXECUTE IMMEDIATE :sql);
        
        -- Loop through each stage in the schema
        FOR stage_record IN rs_stages DO
            stage_name := stage_record.STAGE_NAME;
            EXECUTE IMMEDIATE 'GRANT READ ON STAGE ' || :BACKUP_DATABASE || '.' || :schema_name || '.' || :stage_name || ' TO APPLICATION ' || :APP_NAME;

            stage_command := 'COPY FILES INTO @' || :schema_name || '.' || :stage_name || ' FROM @' || :BACKUP_DATABASE || '.' || :schema_name || '.' || :stage_name;
            EXECUTE IMMEDIATE 'call ' || :APP_NAME || '.core.run_arbitrary(''' || :stage_command || ''')';
            output := :output || '\n' || :APP_NAME || '.' || :schema_name || '.' || :stage_name || ' stage restored successfully.';

        END FOR;
    
    END FOR;
 
    output := :output || '\nRestore completed successfully';
 
    RETURN :output;
END;
$$;    

USE DATABASE GENESIS_BOTS;
call GENESIS_BACKUP.PUBLIC.RESTORE_DATA('GENESIS_BOTS','GENESIS_BACKUP');

// start compute pool and services
alter compute pool GENESIS_POOL RESUME; -- if you paused the compute pool
call GENESIS_BOTS.core.start_app_instance('APP1','GENESIS_POOL','XSMALL'); 


