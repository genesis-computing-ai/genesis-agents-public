

// ON EACH REMOTE REGION

show databases;
show tables in database GENESIS_LOCAL_DB;
ALTER DATABASE GENESIS_LOCAL_DB ENABLE REPLICATION TO ACCOUNTS DSHRNXX.GENESIS;

select count(*) from GENESIS_LOCAL_DB.EVENTS.GENESIS_APP_EVENTS;

create table GENESIS_LOCAL_DB.EVENTS.GENESIS_APP_EVENTS_COPY as 
select * from GENESIS_LOCAL_DB.EVENTS.GENESIS_APP_EVENTS
limit 100;

select count(*) from GENESIS_LOCAL_DB.EVENTS.GENESIS_APP_EVENTS_COPY;


// ON CENTRAL

SHOW REPLICATION DATABASES;

-- Create a replica of the 'mydb1' primary database
-- If the primary database has the DATA_RETENTION_TIME_IN_DAYS parameter set to a value other than the default value,
-- set the same value for the parameter on the secondary database.
CREATE DATABASE EVENTS_FROM_GENESIS_DEV
  AS REPLICA OF DSHRNXX.GENESIS_DEV.GENESIS_LOCAL_DB;

-- Verify the secondary database
SHOW REPLICATION DATABASES;

ALTER DATABASE EVENTS_FROM_GENESIS_DEV REFRESH;

show tables in database EVENTS_FROM_GENESIS_DEV;

select *
  from table(information_schema.database_refresh_progress(EVENTS_FROM_GENESIS_DEV));

select * from EVENTS_FROM_GENESIS_DEV.EVENTS.GENESIS_APP_EVENTS_COPY;
