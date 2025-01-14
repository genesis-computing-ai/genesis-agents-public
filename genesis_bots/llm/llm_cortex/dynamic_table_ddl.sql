create or replace table genesis_test.public.genesis_threads
--as 
--select * from genesis_test.genesis_internal.message_log
--where 1=0;
(
    timestamp TIMESTAMP,
    bot_id VARCHAR, -- Replace VARCHAR with the exact datatype based on your schema
    bot_name VARCHAR, -- Replace VARCHAR with the exact datatype based on your schema
    thread_id VARCHAR, -- Replace VARCHAR with the exact datatype based on your schema
    message_type VARCHAR, -- Replace VARCHAR with the exact datatype based on your schema
    message_payload VARCHAR, -- Replace VARCHAR with the datatype returned by SNOWFLAKE.CORTEX.COMPLETE
    message_metadata VARCHAR, -- Replace VARIANT with the exact datatype based on your schema
    tokens_in number,
    tokens_out number
);
grant all on table genesis_test.public.genesis_threads to public;

truncate table genesis_test.public.genesis_threads;
truncate table genesis_test.public.genesis_threads_manual;

select 
    * from genesis_test.public.genesis_threads;
    
CREATE OR REPLACE DYNAMIC TABLE genesis_test.public.genesis_threads_dynamic
    WAREHOUSE=xsmall
    TARGET_LAG = '1 minutes'
    REFRESH_MODE = 'auto';

create or replace table genesis_test.public.genesis_threads_manual
(
    timestamp TIMESTAMP,
    bot_id VARCHAR, -- Replace VARCHAR with the exact datatype based on your schema
    bot_name VARCHAR, -- Replace VARCHAR with the exact datatype based on your schema
    thread_id VARCHAR, -- Replace VARCHAR with the exact datatype based on your schema
    message_type VARCHAR, -- Replace VARCHAR with the exact datatype based on your schema
    message_payload VARCHAR, -- Replace VARCHAR with the datatype returned by SNOWFLAKE.CORTEX.COMPLETE
    message_metadata VARCHAR, -- Replace VARIANT with the exact datatype based on your schema
    tokens_in NUMBER, -- Replace NUMBER with the exact datatype based on your schema
    tokens_out NUMBER, -- Replace NUMBER with the exact datatype based on your schema
    model_name VARCHAR, -- either mistral-large, snowflake-arctic, etc.
    messages_concatenated VARCHAR
);
--create or replace materialized view my_data.public.genesis_threads_mv 

CREATE OR REPLACE PROCEDURE genesis_test.public.update_threads()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN 
insert into genesis_test.public.genesis_threads_manual
--as
with input as 
(
select 
    i.* from genesis_test.public.genesis_threads i
    LEFT JOIN genesis_threads_manual o ON i.thread_id = o.thread_id and i.timestamp = o.timestamp
    WHERE o.thread_id IS NULL
),

prior_in_thread as
(
select 
    i.* from genesis_test.public.genesis_threads i
),
threads as 
(
SELECT
  i1.thread_id,
  i1.timestamp,
  LISTAGG('<' || i2.message_type || '/> : ' || i2.message_payload, ' ') WITHIN GROUP (ORDER BY i2.timestamp, i2.message_type desc) AS concatenated_payload
FROM
  prior_in_thread i1
LEFT JOIN prior_in_thread i2 ON i1.thread_id = i2.thread_id AND i2.timestamp <= i1.timestamp
GROUP BY
  i1.thread_id,
  i1.timestamp
ORDER BY
  i1.thread_id,
  i1.timestamp
)

select 
    *, 'user', '' from input
union all 
select 
    i.timestamp,
    i.bot_id,
    i.bot_name,
    i.thread_id,
    'Assistant Response',
    SNOWFLAKE.CORTEX.COMPLETE('mistral-large', left(concatenated_payload, 32000)) as message_payload,
    i.message_metadata, --concatenated_payload as metadata,
    0 as tokens_in,
    0 as tokens_out,
    'mistral-large',
    left(concatenated_payload, 16000)
from input as i
join threads  on i.thread_id = threads.thread_id and i.timestamp = threads.timestamp
--where i.message_type = 'User Prompt'
union all

select 
    i.timestamp,
    i.bot_id,
    i.bot_name,
    i.thread_id,
    'Assistant Response',
    SNOWFLAKE.CORTEX.COMPLETE('snowflake-arctic', left(concatenated_payload,4000)) as message_payload,
    i.message_metadata, --concatenated_payload as metadata,
    0 as tokens_in,
    0 as tokens_out,
    'snowflake-arctic',
    left(concatenated_payload, 4000)
from input as i
join threads  on i.thread_id = threads.thread_id and i.timestamp = threads.timestamp
;
END;
$$;

grant all on genesis_test.public.genesis_threads_manual to public;
grant all on procedure genesis_test.public.update_threads() to role public;

grant all on genesis_test.public.genesis_threads_dynamic to public;


select * from genesis_test.public.genesis_threads_dynamic
order by timestamp, message_type desc, model_name;

call genesis_test.public.update_threads();

select * from genesis_test.public.genesis_threads_manual
order by timestamp, message_type desc, model_name;


INSERT INTO GENESIS_TEST.PUBLIC.GENESIS_THREADS (TIMESTAMP, BOT_ID, BOT_NAME, THREAD_ID, MESSAGE_TYPE, MESSAGE_PAYLOAD, MESSAGE_METADATA        ) VALUES ('2024-05-02 17:00:00.518537', 'allisongenbot-gxy9fs', 'default_bot_name', 'Cortex_thread_be1325f2-52d5-4bae-9d50-ec0d6bb58a76', 'System Prompt', 'You are a Snowflake Data Engineer','');

INSERT INTO GENESIS_TEST.PUBLIC.GENESIS_THREADS (TIMESTAMP, BOT_ID, BOT_NAME, THREAD_ID, MESSAGE_TYPE, MESSAGE_PAYLOAD, MESSAGE_METADATA        ) VALUES ('2024-05-02 17:05:00.518537', 'allisongenbot-gxy9fs', 'default_bot_name', 'Cortex_thread_be1325f2-52d5-4bae-9d50-ec0d6bb58a76', 'User Prompt', 'who are you?','');

