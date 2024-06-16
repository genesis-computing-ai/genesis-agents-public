from dotenv import load_dotenv
load_dotenv()
import threading
import time
import logging
import json
import queue
import os
import sys
from openai import OpenAI
from datetime import datetime, timedelta

refresh_seconds = os.getenv("KNOWLEDGE_REFRESH_SECONDS", 60)
refresh_seconds = int(refresh_seconds)

class KnowledgeServer:
    def __init__(self, db_connector, maxsize=100):
        self.db_connector = db_connector
        self.maxsize = maxsize
        self.queue = queue.Queue(maxsize)
        self.condition = threading.Condition()
        self.thread_set = set()  
        self.thread_set_lock = threading.Lock()  
        self.openai_api_key = os.getenv('OPENAI_API_KEY')
        self.client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        self.model = os.getenv("OPENAI_KNOWLEDGE_MODEL", 'gpt-4o')
        self.assistant = self.client.beta.assistants.create(
            name="Knowledge Explorer",
            description="You are a Knowledge Explorer to extract, synthesize, and inject knowledge that bots learn from doing their jobs",
            model=self.model,
            response_format={"type": "json_object"})
    
    def producer(self):
        while True:
            # join inside snowflake
            cutoff = (datetime.now() - timedelta(minutes=10)).strftime('%Y-%m-%d %H:%M:%S')
            query = f'''
                WITH K AS (SELECT thread_id, max(last_timestamp) as last_timestamp FROM {self.db_connector.knowledge_table_name}
                    GROUP BY thread_id),
                M AS (SELECT thread_id, max(timestamp) as timestamp, COUNT(*) as count FROM {self.db_connector.message_log_table_name} 
                    GROUP BY thread_id
                    HAVING count > 3)
                SELECT M.thread_id, timestamp as timestamp, COALESCE(K.last_timestamp, DATE('2000-1-1')) as last_timestamp FROM M
                LEFT JOIN K on M.thread_id = K.thread_id
                WHERE timestamp > COALESCE(K.last_timestamp, DATE('2000-1-1')) AND timestamp < TO_TIMESTAMP('{cutoff}')'''
            threads = self.db_connector.run_query(query)   
            for thread in threads:
                thread_id = thread['THREAD_ID']
                with self.thread_set_lock:
                    if thread_id not in self.thread_set:                    
                        self.thread_set.add(thread_id)
                    else:
                        continue
                
                with self.condition:
                    if self.queue.full():
                        print("Queue is full, producer is waiting...")
                        self.condition.wait()
                    self.queue.put(thread)
                    print(f'Produced {thread_id}')
                    self.condition.notify()

            sys.stdout.write(f'Pausing KnowledgeServer Producer for {refresh_seconds} seconds before next check.\n')
            sys.stdout.flush()
            time.sleep(refresh_seconds)
    
    def consumer(self):
        while True:
            with self.condition:
                if self.queue.empty():
                    print("Queue is empty, consumer is waiting...")
                    self.condition.wait()
                thread = self.queue.get()
                self.condition.notify()
            
            thread_id = thread['THREAD_ID']
            timestamp = thread['TIMESTAMP']
            last_timestamp = thread['LAST_TIMESTAMP'].strftime('%Y-%m-%d %H:%M:%S')

            query = f"""SELECT * FROM {self.db_connector.message_log_table_name} 
                        WHERE timestamp > TO_TIMESTAMP('{last_timestamp}') AND
                        thread_id = '{thread_id}'
                        ORDER BY TIMESTAMP;""" 
            msg_log = self.db_connector.run_query(query)

            messages = [f"{msg['MESSAGE_TYPE']}: {msg['MESSAGE_PAYLOAD']}:" for msg in msg_log]
            messages = '\n'.join(messages)            

            query = f"""SELECT DISTINCT(knowledge_thread_id) FROM {self.db_connector.knowledge_table_name}
                        WHERE thread_id = '{thread_id}';"""
            knowledge_thread_id = self.db_connector.run_query(query)
            if knowledge_thread_id:                
                content = f'''Find a new batch of conversations between the user and agent and update 4 requested information in the original prompt and return it in JSON format:
                             Conversation:
                             {messages}
                        '''
                self.client.beta.threads.messages.create(
                    thread_id=knowledge_thread_id[0]['KNOWLEDGE_THREAD_ID'], content=content, role="user" )
            else:
                content = f'''Given the following conversations between the user and agent, analyze them and extract the 4 requested information:
                             Conversation:
                             {messages}
                            
                             Requested information:
                            - thread_summary: Extract summary of the conversation                                                       
                            - user_learning: Extract what you learned about this user, their preferences, and interests                            
                            - tool_learning: For any tools you called in this thread, what did you learn about how to best use them or call them
                            - data_learning: For any data you analyzed, what did you learn about the data that was not obvious from the metadata that you were provided by search_metadata.                             

                            Expected output in JSON:
                            {{'thread_summary': STRING, 
                             'user_learning': STRING,
                             'tool_learning': STRING,
                             'data_learning': STRING}}
                        '''
                knowledge_thread_id = self.client.beta.threads.create().id
                self.client.beta.threads.messages.create(
                    thread_id=knowledge_thread_id, content=content, role="user" )

            run = self.client.beta.threads.runs.create(
                thread_id = knowledge_thread_id,
                assistant_id = self.assistant.id 
            )
            while not self.client.beta.threads.runs.retrieve(thread_id=knowledge_thread_id, run_id=run.id).completed_at:
                time.sleep(1)
            
            response = json.loads(self.client.beta.threads.messages.list(knowledge_thread_id).data[0].content[0].text.value)

            try:
                # Ensure the timestamp is in the correct format for Snowflake
                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                last_timestamp = msg_log[-1]['TIMESTAMP'].strftime('%Y-%m-%d %H:%M:%S')
                bot_id = msg_log[-1]['BOT_ID']
                primary_user = msg_log[-1]['PRIMARY_USER']
                thread_summary = response['thread_summary']
                user_learning = response['user_learning']
                tool_learning = response['tool_learning']
                data_learning = response['data_learning']
                
                insert_query = f"""
                INSERT INTO {self.db_connector.knowledge_table_name} 
                    (timestamp, thread_id, knowledge_thread_id, primary_user, bot_id, last_timestamp, thread_summary, user_learning, tool_learning, data_learning)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)                    
                """
                cursor = self.db_connector.client.cursor()
                cursor.execute(insert_query, (timestamp, thread_id, knowledge_thread_id, primary_user, bot_id, last_timestamp, thread_summary, user_learning, 
                                              tool_learning, data_learning))
                self.db_connector.client.commit()
            except Exception as e:
                print(f"Encountered errors while inserting into knowledge table row: {e}")
            finally:
                if cursor is not None:
                    cursor.close()


                        
            with self.thread_set_lock:
                self.thread_set.remove(thread_id)
                print(f'Consumed {thread_id}')
    
    def start_threads(self):
        producer_thread = threading.Thread(target=self.producer)
        consumer_thread = threading.Thread(target=self.consumer)
        
        producer_thread.start()
        consumer_thread.start()
        
        producer_thread.join()
        consumer_thread.join()
