

/* 
1. In the current GENESIS Bots app, ask Eliza to run the following statement:
  CALL core.run_arbitrary('GRANT USAGE ON PROCEDURE core.run_arbitrary(VARCHAR) TO APPLICATION ROLE app_public');
2. Using the script below:
2.a. Stop all services and compute pool
2.b. Backup all metadata from the currently installed app
*/

/**** manual backup sql ****/

USE DATABASE GENESIS_BOTS;

call GENESIS_BOTS.core.stop_app_instance('APP1');
alter compute pool GENESIS_POOL SUSPEND; -- to pause the compute pool

// grant access to metadata objects 
call core.run_arbitrary('grant select on all tables in schema APP1 to application role APP_PUBLIC');
call core.run_arbitrary('grant read,write on stage APP1.BOT_FILES_STAGE to application role APP_PUBLIC');

// create a database to store the backup metadata
CREATE OR REPLACE DATABASE GENESIS_BACKUP;
create or replace schema GENESIS_BACKUP.APP1;

// create backup of the metadata
create or replace table GENESIS_BACKUP.APP1.harvest_results as select * from GENESIS_BOTS.app1.harvest_results;
create or replace table GENESIS_BACKUP.APP1.harvest_control as select * from GENESIS_BOTS.app1.harvest_control;
create or replace table GENESIS_BACKUP.APP1.bot_servicing as select * from GENESIS_BOTS.app1.bot_servicing;
create or replace table GENESIS_BACKUP.APP1.SLACK_APP_CONFIG_TOKENS as select * from GENESIS_BOTS.app1.SLACK_APP_CONFIG_TOKENS;
create or replace table GENESIS_BACKUP.APP1.LLM_TOKENS as select * from GENESIS_BOTS.app1.LLM_TOKENS;
create or replace table GENESIS_BACKUP.APP1.TASKS as select * from GENESIS_BOTS.app1.TASKS;
create or replace table GENESIS_BACKUP.APP1.MESSAGE_LOG as select * from GENESIS_BOTS.app1.MESSAGE_LOG;
create or replace table GENESIS_BACKUP.APP1.TASK_HISTORY as select * from GENESIS_BOTS.app1.TASK_HISTORY;
create or replace table GENESIS_BACKUP.APP1.NGROK_TOKENS as select * from GENESIS_BOTS.app1.NGROK_TOKENS;

// create a stage to store a backup of the bot files and allow the GENESIS_BOTS application to access it
create or replace stage GENESIS_BACKUP.APP1.bot_files_stage;
grant usage on database GENESIS_BACKUP to application GENESIS_BOTS;
grant usage on schema GENESIS_BACKUP.APP1 to application GENESIS_BOTS;
grant read,write on stage GENESIS_BACKUP.APP1.bot_files_stage to application GENESIS_BOTS;

USE DATABASE GENESIS_BOTS;
call core.run_arbitrary($$copy files into @GENESIS_BACKUP.APP1.bot_files_stage from @GENESIS_BOTS.app1.bot_files_stage;$$);

// Uninstall the Genesis Bots application via script below or from the Data Products-->Apps UI
DROP APPLICATION GENESIS_BOTS;

/**** end manual backup sql ****/



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

grant usage on database GENESIS_BACKUP to application GENESIS_BOTS;
grant usage on schema GENESIS_BACKUP.APP1 to application GENESIS_BOTS;
grant select on all tables in schema GENESIS_BACKUP.APP1 to application GENESIS_BOTS;

use database GENESIS_BOTS;

call core.run_arbitrary('select * from GENESIS_BACKUP.APP1.bot_servicing;');
call core.run_arbitrary('truncate table GENESIS_BOTS.app1.bot_servicing;');
call core.run_arbitrary('insert into GENESIS_BOTS.app1.bot_servicing select * from GENESIS_BACKUP.APP1.bot_servicing;');

call core.run_arbitrary('select * from GENESIS_BACKUP.APP1.harvest_control;');
call core.run_arbitrary('truncate table GENESIS_BOTS.app1.harvest_control;');
call core.run_arbitrary('insert into GENESIS_BOTS.app1.harvest_control select * from GENESIS_BACKUP.APP1.harvest_control;');

call core.run_arbitrary('select * from GENESIS_BACKUP.APP1.harvest_results;');
call core.run_arbitrary('truncate table GENESIS_BOTS.app1.harvest_results;');
call core.run_arbitrary('insert into GENESIS_BOTS.app1.harvest_results select * from GENESIS_BACKUP.APP1.harvest_results;');

call core.run_arbitrary('select * from GENESIS_BACKUP.APP1.LLM_TOKENS;');
call core.run_arbitrary('truncate table GENESIS_BOTS.app1.LLM_TOKENS;');
call core.run_arbitrary('insert into GENESIS_BOTS.app1.LLM_TOKENS select * from GENESIS_BACKUP.APP1.LLM_TOKENS;');

call core.run_arbitrary('select * from GENESIS_BACKUP.APP1.SLACK_APP_CONFIG_TOKENS;');
call core.run_arbitrary('truncate table GENESIS_BOTS.app1.SLACK_APP_CONFIG_TOKENS;');
call core.run_arbitrary('insert into GENESIS_BOTS.app1.SLACK_APP_CONFIG_TOKENS select * from GENESIS_BACKUP.APP1.SLACK_APP_CONFIG_TOKENS;');

call core.run_arbitrary('select * from GENESIS_BACKUP.APP1.TASKS;');
call core.run_arbitrary('truncate table GENESIS_BOTS.app1.TASKS;');
call core.run_arbitrary('insert into GENESIS_BOTS.app1.TASKS select * from GENESIS_BACKUP.APP1.TASKS;');

call core.run_arbitrary('select * from GENESIS_BACKUP.APP1.TASK_HISTORY;');
call core.run_arbitrary('truncate table GENESIS_BOTS.app1.TASK_HISTORY;');
call core.run_arbitrary('insert into GENESIS_BOTS.app1.TASK_HISTORY select * from GENESIS_BACKUP.APP1.TASK_HISTORY;');

call core.run_arbitrary('select * from GENESIS_BACKUP.APP1.MESSAGE_LOG;');
call core.run_arbitrary('truncate table GENESIS_BOTS.app1.MESSAGE_LOG;');
call core.run_arbitrary('insert into GENESIS_BOTS.app1.MESSAGE_LOG select * from GENESIS_BACKUP.APP1.MESSAGE_LOG;');

call core.run_arbitrary('select * from GENESIS_BACKUP.APP1.NGROK_TOKENS;');
call core.run_arbitrary('truncate table GENESIS_BOTS.app1.NGROK_TOKENS;');
call core.run_arbitrary('insert into GENESIS_BOTS.app1.NGROK_TOKENS select * from GENESIS_BACKUP.APP1.NGROK_TOKENS;');


// copy files
grant read,write on stage GENESIS_BACKUP.APP1.bot_files_stage to application GENESIS_BOTS;
call core.run_arbitrary($$copy files into @app1.bot_files_stage from @GENESIS_BACKUP.APP1.bot_files_stage;$$);

// restart the compute pool and servcies
alter compute pool GENESIS_POOL RESUME; -- if you paused the compute pool
call GENESIS_BOTS.core.start_app_instance('APP1','GENESIS_POOL','GENESIS_EAI','XSMALL'); 
