
/*** restoring data from backup to a new GENESIS_BOTS_ALPHA application ***/
// first, create the new GENESIS_BOTS_ALPHA application, or ensure it exists
// start all services
// stop all services and suspend the compute pool
// run the following to restore the metadata and files
// once restored, start the compute pool and services



CREATE OR REPLACE PROCEDURE GENESIS_BACKUP_ALPHA.PUBLIC.RESTORE_ALPHA_DATA(APP_NAME STRING, BACKUP_DATABASE STRING)
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

    sql := 'SELECT SCHEMA_NAME s_name FROM ' || APP_NAME || '.INFORMATION_SCHEMA.SCHEMATA WHERE CATALOG_NAME = ''' || APP_NAME || ''' AND SCHEMA_NAME NOT IN (''INFORMATION_SCHEMA'', ''PUBLIC'')';
    rs_schemas := (EXECUTE IMMEDIATE :sql);

    -- Loop through each schema
    FOR schema_record IN rs_schemas DO
        schema_name := schema_record.s_name;

        -- grant objects
        EXECUTE IMMEDIATE 'CALL ' || :APP_NAME || '.CORE.RUN_ARBITRARY(''GRANT SELECT ON ALL TABLES IN SCHEMA ' || :BACKUP_DATABASE || '.' || :schema_name || ' TO APPLICATION ' || :APP_NAME || ''')';
        EXECUTE IMMEDIATE 'CALL ' || :APP_NAME || '.CORE.RUN_ARBITRARY(''GRANT READ ON ALL STAGES IN SCHEMA ' || :BACKUP_DATABASE || '.' || :schema_name || ' TO APPLICATION ' || :APP_NAME || ''')';
        output := :output || '\nGrants made on all tables and stages in backup schema ' || :BACKUP_DATABASE || '.' || :schema_name || ' to application successfully';

        sql := 'SELECT TABLE_NAME FROM ' || APP_NAME || '.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ''' || schema_name || '''';
        rs_tables := (EXECUTE IMMEDIATE :sql);
        
        -- Loop through each table in the schema
        FOR table_record IN rs_tables DO
            table_name := table_record.TABLE_NAME;
            
            truncate_command := 'TRUNCATE TABLE ' || :APP_NAME || '.' || :schema_name || '.' || :table_name;
            EXECUTE IMMEDIATE 'call ' || :APP_NAME || '.core.run_arbitrary(''' || :truncate_command || ''')';
            output := :output || '\n' || :APP_NAME || '.' || :schema_name || '.' || :table_name || ' truncated.';

            restore_command := 'INSERT INTO ' || :APP_NAME || '.' || :schema_name || '.' || :table_name || ' SELECT * FROM ' || :BACKUP_DATABASE || '.' || :schema_name || '.' || :table_name;
            EXECUTE IMMEDIATE 'call ' || :APP_NAME || '.core.run_arbitrary(''' || :restore_command || ''')';
            output := :output || '\n' || :APP_NAME || '.' || :schema_name || '.' || :table_name || ' table restored successfully.';
        END FOR;
        
        -- views TBD

        sql := 'SELECT STAGE_NAME FROM ' || APP_NAME || '.INFORMATION_SCHEMA.STAGES WHERE STAGE_SCHEMA = ''' || schema_name || '''';
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

// call GENESIS_BACKUP_ALPHA.PUBLIC.RESTORE_ALPHA_DATA('GENESIS_BOTS_ALPHA','GENESIS_BACKUP_ALPHA');

