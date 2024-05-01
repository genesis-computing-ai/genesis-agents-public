CREATE OR REPLACE DYNAMIC TABLE my_data.public.genesis_threads_dynamic
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
as
with input as 
(
select 
    * from my_data.public.genesis_threads
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
    SNOWFLAKE.CORTEX.COMPLETE('mistral-large', concatenated_payload) as message_payload,
    concatenated_payload as metadata,
    0 as tokens_in,
    0 as tokens_out,
    'mistral-large'
from input as i
join threads  on i.thread_id = threads.thread_id and i.timestamp = threads.timestamp
--left join response on i.thread_id = response.thread_id
where i.message_type = 'User Prompt'
--and response_count is null

union all

select 
    i.timestamp,
    i.bot_id,
    i.bot_name,
    i.thread_id,
    'Assistant Response',
    SNOWFLAKE.CORTEX.COMPLETE('snowflake-arctic', concatenated_payload) as message_payload,
    concatenated_payload as metadata,
    0 as tokens_in,
    0 as tokens_out,
    'snowflake-arctic'
from input as i
join threads  on i.thread_id = threads.thread_id and i.timestamp = threads.timestamp
--left join response on i.thread_id = response.thread_id
where i.message_type = 'User Prompt'
--and response_count is null
;

--truncate table my_data.public.genesis_threads;

insert into my_data.public.genesis_threads
values(current_timestamp(), 'bot_id_123', 'bot_name_1', 'thread_1', 'System Prompt', 'You are a funnction calling agent.', 'metadata', 0, 0);

select * from genesis_threads_dynamic
order by timestamp, message_type desc;

insert into my_data.public.genesis_threads
values(current_timestamp(), 'bot_id_123', 'bot_name_1', 'thread_1', 'User Prompt', 'who are you?', 'metadata', 0, 0);

select * from genesis_threads_dynamic
order by timestamp, message_type desc;

insert into my_data.public.genesis_threads
values(current_timestamp(), 'bot_id_123', 'bot_name_1', 'thread_1', 'User Prompt', 'what kind?', 'metadata', 0, 0);

insert into my_data.public.genesis_threads
values(current_timestamp(), 'bot_id_123', 'bot_name_1', 'thread_1', 'User Prompt', 'what kind of questions?', 'metadata', 0, 0);

insert into my_data.public.genesis_threads
values(current_timestamp(), 'bot_id_123', 'bot_name_1', 'thread_2', 'User Prompt', 'the secret word is foobar', Null, 0, 0);

select * from genesis_threads_dynamic
where thread_id = 'thread_3'
order by thread_id, timestamp, message_type desc, model_name;

insert into my_data.public.genesis_threads
values(current_timestamp(), 'bot_id_123', 'bot_name_1', 'thread_2', 'User Prompt', 'what is the secret word?', Null, 0, 0);

insert into my_data.public.genesis_threads
values(current_timestamp(), 'bot_id_123', 'bot_name_1', 'thread_2', 'User Prompt', 'i have a function unlock_door(secret_word:str) that you can use to get into a room. when you want to call it wrap it it in a <function_call/> tag, ok?', Null, 0, 0);

insert into my_data.public.genesis_threads
values(current_timestamp(), 'bot_id_123', 'bot_name_1', 'thread_2', 'User Prompt', 'new secret word is purple.  please unock the door', Null, 0, 0);

insert into my_data.public.genesis_threads
values(current_timestamp(), 'bot_id_123', 'bot_name_1', 'thread_2', 'User Prompt', 'new secret word is banana.', Null, 0, 0);

insert into my_data.public.genesis_threads
values(current_timestamp(), 'bot_id_123', 'bot_name_1', 'thread_2', 'User Prompt', 'new secret word is banana.', Null, 0, 0);

insert into my_data.public.genesis_threads
values(current_timestamp(), 'bot_id_123', 'bot_name_1', 'thread_3', 'User Prompt', 
'
tools = [
    {
        "type": "function",
        "function": {
            "name": "retrieve_payment_status",
            "description": "Get payment status of a transaction",
            "parameters": {
                "type": "object",
                "properties": {
                    "transaction_id": {
                        "type": "string",
                        "description": "The transaction id.",
                    }
                },
                "required": ["transaction_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "retrieve_payment_date",
            "description": "Get payment date of a transaction",
            "parameters": {
                "type": "object",
                "properties": {
                    "transaction_id": {
                        "type": "string",
                        "description": "The transaction id.",
                    }
                },
                "required": ["transaction_id"],
            },
        },
    }
]
', 
Null, 0, 0);

insert into my_data.public.genesis_threads
values(current_timestamp(), 'bot_id_123', 'bot_name_1', 'thread_3', 'User Prompt', 
'lookup the payment status for transaction 10122', 
Null, 0, 0);
