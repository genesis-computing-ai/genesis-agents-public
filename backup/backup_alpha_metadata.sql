// currently setup on DSHRNXX.CVB46967 account to backup the demo GENESIS_BOTS_ALPHA metadata
/**** manual backup sql ****/

// grant objects just in case...
call core.run_arbitrary('grant select on all tables in schema app1 to application role app_public');

// create backup of the backup
create or replace schema genesis_backup_demo.alpha2days clone genesis_backup_demo.alpha;
create or replace stage genesis_backup_demo.alpha2days.bot_files_stage;
copy files into @genesis_backup_demo.alpha2days.bot_files_stage from @genesis_backup_demo.alpha.bot_files_stage;

// create backup of alpha
create or replace table genesis_backup_demo.alpha.harvest_results as select * from genesis_bots_alpha.app1.harvest_results;
create or replace table genesis_backup_demo.alpha.bot_servicing as select * from genesis_bots_alpha.app1.bot_servicing;
create or replace table genesis_backup_demo.alpha.SLACK_APP_CONFIG_TOKENS as select * from genesis_bots_alpha.app1.SLACK_APP_CONFIG_TOKENS;
create or replace table genesis_backup_demo.alpha.LLM_TOKENS as select * from genesis_bots_alpha.app1.LLM_TOKENS;
create or replace table genesis_backup_demo.alpha.TASKS as select * from genesis_bots_alpha.app1.TASKS;
create or replace table genesis_backup_demo.alpha.MESSAGE_LOG as select * from genesis_bots_alpha.app1.MESSAGE_LOG;
create or replace table genesis_backup_demo.alpha.TASK_HISTORY as select * from genesis_bots_alpha.app1.TASK_HISTORY;
create or replace table genesis_backup_demo.alpha.NGROK_TOKENS as select * from genesis_bots_alpha.app1.NGROK_TOKENS;
create or replace table genesis_backup_demo.alpha.KNOWLEDGE as select * from genesis_bots_alpha.app1.KNOWLEDGE;

create or replace stage genesis_backup_demo.alpha.bot_files_stage;
grant read,write on stage genesis_backup_demo.alpha.bot_files_stage to application genesis_bots_alpha;

call core.run_arbitrary($$copy files into @genesis_backup_demo.alpha.bot_files_stage from @genesis_bots_alpha.app1.bot_files_stage;$$);

/**** end manual backup sql ****/

CREATE OR REPLACE PROCEDURE GENESIS_BACKUP_DEMO.PUBLIC.BACKUP_ALPHA_DATA()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- grant objects just in case...
    EXECUTE IMMEDIATE 'CALL GENESIS_BOTS_ALPHA.CORE.RUN_ARBITRARY(''GRANT SELECT ON ALL TABLES IN SCHEMA GENESIS_BOTS_ALPHA.APP1 TO APPLICATION ROLE APP_PUBLIC'')';

    -- create backup of the backup
    EXECUTE IMMEDIATE 'CREATE OR REPLACE SCHEMA GENESIS_BACKUP_DEMO.ALPHA2DAYS CLONE GENESIS_BACKUP_DEMO.ALPHA';
    EXECUTE IMMEDIATE 'CREATE OR REPLACE STAGE GENESIS_BACKUP_DEMO.ALPHA2DAYS.BOT_FILES_STAGE';
    EXECUTE IMMEDIATE 'COPY FILES INTO @GENESIS_BACKUP_DEMO.ALPHA2DAYS.BOT_FILES_STAGE FROM @GENESIS_BACKUP_DEMO.ALPHA.BOT_FILES_STAGE';

    -- create backup of alpha
    EXECUTE IMMEDIATE 'CREATE OR REPLACE TABLE GENESIS_BACKUP_DEMO.ALPHA.HARVEST_RESULTS AS SELECT * FROM GENESIS_BOTS_ALPHA.APP1.HARVEST_RESULTS';
    EXECUTE IMMEDIATE 'CREATE OR REPLACE TABLE GENESIS_BACKUP_DEMO.ALPHA.BOT_SERVICING AS SELECT * FROM GENESIS_BOTS_ALPHA.APP1.BOT_SERVICING';
    EXECUTE IMMEDIATE 'CREATE OR REPLACE TABLE GENESIS_BACKUP_DEMO.ALPHA.SLACK_APP_CONFIG_TOKENS AS SELECT * FROM GENESIS_BOTS_ALPHA.APP1.SLACK_APP_CONFIG_TOKENS';
    EXECUTE IMMEDIATE 'CREATE OR REPLACE TABLE GENESIS_BACKUP_DEMO.ALPHA.LLM_TOKENS AS SELECT * FROM GENESIS_BOTS_ALPHA.APP1.LLM_TOKENS';
    EXECUTE IMMEDIATE 'CREATE OR REPLACE TABLE GENESIS_BACKUP_DEMO.ALPHA.TASKS AS SELECT * FROM GENESIS_BOTS_ALPHA.APP1.TASKS';
    EXECUTE IMMEDIATE 'CREATE OR REPLACE TABLE GENESIS_BACKUP_DEMO.ALPHA.MESSAGE_LOG AS SELECT * FROM GENESIS_BOTS_ALPHA.APP1.MESSAGE_LOG';
    EXECUTE IMMEDIATE 'CREATE OR REPLACE TABLE GENESIS_BACKUP_DEMO.ALPHA.TASK_HISTORY AS SELECT * FROM GENESIS_BOTS_ALPHA.APP1.TASK_HISTORY';
    EXECUTE IMMEDIATE 'CREATE OR REPLACE TABLE GENESIS_BACKUP_DEMO.ALPHA.NGROK_TOKENS AS SELECT * FROM GENESIS_BOTS_ALPHA.APP1.NGROK_TOKENS';
    EXECUTE IMMEDIATE 'CREATE OR REPLACE TABLE GENESIS_BACKUP_DEMO.ALPHA.KNOWLEDGE AS SELECT * FROM GENESIS_BOTS_ALPHA.APP1.KNOWLEDGE';

    EXECUTE IMMEDIATE 'CREATE OR REPLACE STAGE GENESIS_BACKUP_DEMO.ALPHA.BOT_FILES_STAGE';
    EXECUTE IMMEDIATE 'GRANT READ,WRITE ON STAGE GENESIS_BACKUP_DEMO.ALPHA.BOT_FILES_STAGE TO APPLICATION GENESIS_BOTS_ALPHA';

    EXECUTE IMMEDIATE 'call GENESIS_BOTS_ALPHA.core.run_arbitrary(''COPY FILES INTO @GENESIS_BACKUP_DEMO.ALPHA.BOT_FILES_STAGE FROM @GENESIS_BOTS_ALPHA.APP1.BOT_FILES_STAGE'')';

    RETURN 'Backup completed successfully';
END;
$$;

// runs every morning  

CREATE TASK backup_alpha_data_task
  WAREHOUSE = XSMALL
  SCHEDULE = 'USING CRON 0 10 * * * UTC'
  AS
  CALL GENESIS_BACKUP_DEMO.PUBLIC.BACKUP_ALPHA_DATA();

ALTER TASK backup_alpha_data_task RESUME;

/*** restoring data from backup to a new GENESIS_BOTS_ALPHA application ***/
// first, create the new GENESIS_BOTS_ALPHA application, or ensure it exists
// start all services
// stop all services and suspend the compute pool
// run the following to restore the metadata and files
// once restored, start the compute pool and services

grant usage on database genesis_backup_demo to application genesis_bots_alpha;
grant usage on schema genesis_backup_demo.alpha to application genesis_bots_alpha;
grant select on all tables in schema genesis_backup_demo.alpha to application genesis_bots_alpha;

use database genesis_bots_alpha;

call core.run_arbitrary('truncate table genesis_bots_alpha.app1.bot_servicing;');
call core.run_arbitrary('insert into genesis_bots_alpha.app1.bot_servicing select * from genesis_backup_demo.alpha.bot_servicing;');

call core.run_arbitrary('truncate table genesis_bots_alpha.app1.harvest_control;');
call core.run_arbitrary('insert into genesis_bots_alpha.app1.harvest_control select * from genesis_backup_demo.alpha.harvest_control;');

call core.run_arbitrary('truncate table genesis_bots_alpha.app1.harvest_results;');
call core.run_arbitrary('insert into genesis_bots_alpha.app1.harvest_results select * from genesis_backup_demo.alpha.harvest_results;');

call core.run_arbitrary('truncate table genesis_bots_alpha.app1.LLM_TOKENS;');
call core.run_arbitrary('insert into genesis_bots_alpha.app1.LLM_TOKENS select * from genesis_backup_demo.alpha.LLM_TOKENS;');

call core.run_arbitrary('truncate table genesis_bots_alpha.app1.SLACK_APP_CONFIG_TOKENS;');
call core.run_arbitrary('insert into genesis_bots_alpha.app1.SLACK_APP_CONFIG_TOKENS select * from genesis_backup_demo.alpha.SLACK_APP_CONFIG_TOKENS;');

call core.run_arbitrary('truncate table genesis_bots_alpha.app1.TASKS;');
call core.run_arbitrary('insert into genesis_bots_alpha.app1.TASKS select * from genesis_backup_demo.alpha.TASKS;');

call core.run_arbitrary('truncate table genesis_bots_alpha.app1.TASK_HISTORY;');
call core.run_arbitrary('insert into genesis_bots_alpha.app1.TASK_HISTORY select * from genesis_backup_demo.alpha.TASK_HISTORY;');

call core.run_arbitrary('truncate table genesis_bots_alpha.app1.MESSAGE_LOG;');
call core.run_arbitrary('insert into genesis_bots_alpha.app1.MESSAGE_LOG select * from genesis_backup_demo.alpha.MESSAGE_LOG;');

call core.run_arbitrary('truncate table genesis_bots_alpha.app1.NGROK_TOKENS;');
call core.run_arbitrary('insert into genesis_bots_alpha.app1.NGROK_TOKENS select * from genesis_backup_demo.alpha.NGROK_TOKENS;');

call core.run_arbitrary('truncate table genesis_bots_alpha.app1.KNOWLEDGE;');
call core.run_arbitrary('insert into genesis_bots_alpha.app1.KNOWLEDGE select * from genesis_backup_demo.alpha.KNOWLEDGE;');


// files
grant read,write on stage genesis_backup_demo.alpha.bot_files_stage to application genesis_bots_alpha;
call core.run_arbitrary($$copy files into @app1.bot_files_stage from @genesis_backup_demo.alpha.bot_files_stage;$$);
