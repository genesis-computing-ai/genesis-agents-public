create or replace table genesis_test.public.genesis_threads
as 
select * from genesis_test.genesis_internal.message_log
where 1=0;
grant all on table genesis_test.public.genesis_threads to public;

truncate table genesis_test.public.genesis_threads;

select 
    * from genesis_test.public.genesis_threads;
    

CREATE OR REPLACE DYNAMIC TABLE genesis_test.public.genesis_threads_dynamic
    WAREHOUSE=xsmall
    TARGET_LAG = '1 minutes'
    REFRESH_MODE = 'auto'
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
    model_name VARCHAR -- either mistral-large, snowflake-arctic, etc.
)
--create or replace materialized view my_data.public.genesis_threads_mv 

as
with input as 
(
select 
    * from genesis_test.public.genesis_threads
),
threads as 
(
SELECT
  i1.thread_id,
  i1.timestamp,
  LISTAGG('<' || i2.message_type || '/> : ' || i2.message_payload, ' ') WITHIN GROUP (ORDER BY i2.timestamp, i2.message_type desc) AS concatenated_payload
FROM
  input i1
LEFT JOIN input i2 ON i1.thread_id = i2.thread_id AND i2.timestamp <= i1.timestamp
GROUP BY
  i1.thread_id,
  i1.timestamp
ORDER BY
  i1.thread_id,
  i1.timestamp
)--,

--response as
--(
--select thread_id,
--       count(*) response_count
--from my_data.public.genesis_threads
--where message_type = 'Assistant Response'
--group by thread_id
--)

select 
    *, 'user' from input
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
    'mistral-large'
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
    'snowflake-arctic'
from input as i
join threads  on i.thread_id = threads.thread_id and i.timestamp = threads.timestamp
;
grant all on genesis_test.public.genesis_threads_dynamic to public;


select * from genesis_test.public.genesis_threads_dynamic
order by timestamp, message_type desc, model_name
