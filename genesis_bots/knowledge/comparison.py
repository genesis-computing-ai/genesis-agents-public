import os
import json
import sys

from genesis_bots.llm.llm_openai.openai_utils import get_openai_client

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

import time
from openai import OpenAI
from dotenv import load_dotenv
load_dotenv()

from genesis_bots.connectors.snowflake_connector.snowflake_connector import SnowflakeConnector

knowledge_db_connector = SnowflakeConnector(connection_name='Snowflake')


client = get_openai_client()
model = os.getenv("OPENAI_KNOWLEDGE_MODEL", 'gpt-4o')
assistant = client.beta.assistants.create(
            name="Knowledge Explorer",
            description="You are a Knowledge Explorer to extract, synthesize, and inject knowledge that bots learn from doing their jobs",
            model=model,
            response_format={"type": "json_object"})

thread_id = 'thread_px24rDds8HEUwqdnIUx2zO2A'

query = f"""SELECT * FROM {knowledge_db_connector.message_log_table_name}
                        WHERE thread_id = '{thread_id}'
                        ORDER BY TIMESTAMP;"""

query = '''
    SELECT * FROM GENESIS_BOTS_ALPHA.APP1.MESSAGE_LOG
WHERE THREAD_ID = 'thread_6jBPGzKUgqsxpAOyCfiAUbYO'
ORDER BY TIMESTAMP DESC;'''

msg_log = knowledge_db_connector.run_query(query)

import pandas as pd
messages = '\n'.join(pd.read_csv(r'C:\Users\VAGHEFI\Downloads\knowledge.csv')['MESSAGE_PAYLOAD'].astype(str).tolist())

messages = [f"{msg['MESSAGE_TYPE']}: {msg['MESSAGE_PAYLOAD']}:" for msg in msg_log]
messages = '\n'.join(messages)


content = f'''Given the following conversations between the user and agent, analyze them and extract the 4 requested information:
                Conversation:
                {messages}

                Requested information:
            - thread_summary: Extract summary of the conversation
            - user_learning: Extract what you learned about this user, their preferences, and interests
            - tool_learning: For any tools you called in this thread, what did you learn about how to best use them or call them
            - data_learning: For any data you analyzed, what did you learn about the data that was not obvious from the metadata that you were provided by search_metadata.
            - snowflake_learning: For any interaction with snowflake, what did you learn about snowflake

            Expected output in JSON:
            {{'thread_summary': STRING,
                'user_learning': STRING,
                'tool_learning': STRING,
                'data_learning': STRING,
                'snowflake_learning': STRING}}
        '''
knowledge_thread_id = client.beta.threads.create().id
client.beta.threads.messages.create(
    thread_id=knowledge_thread_id, content=content, role="user" )

run = client.beta.threads.runs.create(
    thread_id = knowledge_thread_id,
    assistant_id = assistant.id
)

while not client.beta.threads.runs.retrieve(thread_id=knowledge_thread_id, run_id=run.id).completed_at:
    time.sleep(1)

response = json.loads(client.beta.threads.messages.list(knowledge_thread_id).data[0].content[0].text.value)

thread_summary = response['thread_summary']
user_learning = response['user_learning']
tool_learning = response['tool_learning']
data_learning = response['data_learning']

print('###############################')
print('Approach 1')
print('###############################')
print('****thread_summary:\n',thread_summary)
print('****user_learning:\n',user_learning)
print('****tool_learning:\n',tool_learning)
print('****data_learning:\n',data_learning)


assistant = client.beta.assistants.create(
            name="Knowledge Explorer",
            description="You are a Knowledge Explorer to extract, synthesize, and inject knowledge that bots learn from doing their jobs",
            model=model)


content = f'''Given the following conversations between the user and agent, analyze them and extract the 4 requested information:
                Conversation:
                {messages}
        '''
knowledge_thread_id = client.beta.threads.create().id
client.beta.threads.messages.create(
    thread_id=knowledge_thread_id, content=content, role="user" )

prompts = {'thread_summary': 'Extract a short summary of the conversation',
            'user_learning': 'Extract a short summary of what you learned about this user, their preferences, and interests',
            'tool_learning': 'For any tools you called in this thread, extract a short summary of what you learn about how to best use them or call them',
            'data_learning': 'For any data you analyzed, extract a short summary of what you learn about the data that was not obvious from the metadata that you were provided by search_metadata.'
}

response = {}
for item, prompt in prompts.items():
    print(item)
    client.beta.threads.messages.create(
        thread_id=knowledge_thread_id, content=prompt, role="user" )

    run = client.beta.threads.runs.create(
        thread_id = knowledge_thread_id,
        assistant_id = assistant.id
    )

    while not client.beta.threads.runs.retrieve(thread_id=knowledge_thread_id, run_id=run.id).completed_at:
        time.sleep(1)

    res = client.beta.threads.messages.list(knowledge_thread_id).data[0].content[0].text.value
    response[item] = res

thread_summary = response['thread_summary']
user_learning = response['user_learning']
tool_learning = response['tool_learning']
data_learning = response['data_learning']

print('###############################')
print('Approach 2')
print('###############################')
print('****thread_summary:\n',thread_summary)
print('****user_learning:\n',user_learning)
print('****tool_learning:\n',tool_learning)
print('****data_learning:\n',data_learning)

