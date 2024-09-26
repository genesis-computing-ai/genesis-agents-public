import threading
import time
import logging
import json
import queue
import os
import sys
from openai import OpenAI
from datetime import datetime, timedelta
import ast

print("     ┌───────┐     ")
print("    ╔═════════╗    ")
print("   ║  ◉   ◉  ║   ")
print("  ║    ───    ║  ")
print(" ╚═══════════╝ ")
print("     ╱     ╲     ")
print("    ╱│  ◯  │╲    ")
print("   ╱ │_____│ ╲   ")
print("      │   │      ")
print("      │   │      ")
print("     ╱     ╲     ")
print("    ╱       ╲    ")
print("   ╱         ╲   ")
print("  G E N E S I S ")
print("    B o t O S")
print(" ---- KNOWLEDGE SERVER----")
print('Knowledge Start Version 0.183',flush=True)



refresh_seconds = os.getenv("KNOWLEDGE_REFRESH_SECONDS", 60)
refresh_seconds = int(refresh_seconds)

print("waiting 60 seconds for other services to start first...", flush=True)
time.sleep(60)

class KnowledgeServer:
    def __init__(self, db_connector, llm_type, maxsize=100):
        self.db_connector = db_connector
        self.maxsize = maxsize
        self.thread_queue = queue.Queue(maxsize)
        self.user_queue = queue.Queue(0)
        self.condition = threading.Condition()
        self.thread_set = set()
        self.thread_set_lock = threading.Lock()
        if llm_type == 'OpenAI':
            llm_type = 'openai'
        self.llm_type = llm_type
        if llm_type == 'openai' or llm_type == 'OpenAI':
            self.openai_api_key = os.getenv("OPENAI_API_KEY")
            self.client = get_openai_client() 
            self.model = os.getenv("OPENAI_KNOWLEDGE_MODEL", os.getenv('OPENAI_MODEL_NAME',"gpt-4o"))
            self.assistant = self.client.beta.assistants.create(
                name="Knowledge Explorer",
                description="You are a Knowledge Explorer to extract, synthesize, and inject knowledge that bots learn from doing their jobs",
                model=self.model,
                response_format={"type": "json_object"},
            )

    def producer(self):
        while True:
  
            # join inside snowflake
            cutoff = (datetime.now() - timedelta(minutes=10)).strftime("%Y-%m-%d %H:%M:%S")
            threads = self.db_connector.query_threads_message_log(cutoff)
            print(f"Producer found {len(threads)} threads", flush=True)
            for thread in threads:
                thread_id = thread["THREAD_ID"]
                with self.thread_set_lock:
                    if thread_id not in self.thread_set:
                        self.thread_set.add(thread_id)
                    else:
                        continue
                    
                query = f"""
                        WITH BOTS AS (SELECT BOT_SLACK_USER_ID, 
                            CONCAT('{{"user_id": "', BOT_SLACK_USER_ID, '", "user_name": "', BOT_NAME, '", "user_email": "Unknown Email"}}') as PRIMARY_USER 
                            FROM {self.db_connector.bot_servicing_table_name})
                        SELECT COUNT(DISTINCT M.PRIMARY_USER) AS CNT FROM {self.db_connector.message_log_table_name} M
                        LEFT JOIN BOTS ON M.PRIMARY_USER = BOTS.PRIMARY_USER
                        WHERE THREAD_ID = '{thread_id}' AND BOT_SLACK_USER_ID IS NULL;"""
                count_non_bot_users = self.db_connector.run_query(query)
                # this is needed to exclude channels with more than one user

                if count_non_bot_users and count_non_bot_users[0]["CNT"] != 1:
                    continue

                with self.condition:
                    if self.thread_queue.full():
                        print("Queue is full, producer is waiting...")
                        self.condition.wait()
                    self.thread_queue.put(thread)
                    print(f"Produced {thread_id}")
                    self.condition.notify()

            sys.stdout.write(
                f"Pausing KnowledgeServer Producer for {refresh_seconds} seconds before next check.\n"
            )
            sys.stdout.flush()

            wake_up = False
            while not wake_up:
                time.sleep(refresh_seconds)

                cursor = self.db_connector.client.cursor()
                check_bot_active = f"DESCRIBE TABLE {self.db_connector.schema}.BOTS_ACTIVE"
                cursor.execute(check_bot_active)
                result = cursor.fetchone()

                bot_active_time_dt = datetime.strptime(result[0], '%Y-%m-%d %H:%M:%S %Z')
                current_time = datetime.now()
                time_difference = current_time - bot_active_time_dt

                print(f"BOTS ACTIVE TIME: {result[0]} | CURRENT TIME: {current_time} | TIME DIFFERENCE: {time_difference} | producer", flush=True)

                if time_difference < timedelta(minutes=5):
                    wake_up = True
    #                print("Bot is active")

    def consumer(self):
        while True:
            with self.condition:
                if self.thread_queue.empty():
                    #print("Queue is empty, consumer is waiting...")
                    self.condition.wait()
                thread = self.thread_queue.get()
                self.condition.notify()

            thread_id = thread["THREAD_ID"]
            timestamp = thread["TIMESTAMP"]
            if type(thread["LAST_TIMESTAMP"]) != str:
                last_timestamp = thread["LAST_TIMESTAMP"].strftime("%Y-%m-%d %H:%M:%S")
            else:
                last_timestamp = thread["LAST_TIMESTAMP"]

            msg_log = self.db_connector.query_timestamp_message_log(thread_id, last_timestamp, max_rows=50)

            messages = [f"{msg['MESSAGE_TYPE']}: {msg['MESSAGE_PAYLOAD']}" for msg in msg_log if "'EMBEDDING': " not in msg['MESSAGE_PAYLOAD']]
            messages = "\n".join(messages)[:200_000] # limit to 200k char for now

            query = f"""SELECT DISTINCT(knowledge_thread_id) FROM {self.db_connector.knowledge_table_name}
                        WHERE thread_id = '{thread_id}';"""
            knowledge_thread_id = self.db_connector.run_query(query)
            if knowledge_thread_id and ( self.llm_type == 'openai' or self.llm_type == 'OpenAI'):
                knowledge_thread_id = knowledge_thread_id[0]["KNOWLEDGE_THREAD_ID"]
                content = f"""Find a new batch of conversations between the user and agent and update 4 requested information in the original prompt and return it in JSON format:
                             Conversation:
                             {messages}
                        """
                try:
                    print('openai create ', knowledge_thread_id)
                    self.client.beta.threads.messages.create(
                        thread_id=knowledge_thread_id, content=content, role="user"
                    )
                except Exception as e:
                    print('openai create exception ', e)
                    knowledge_thread_id = None
            else:
                content = f"""Given the following conversations between the user and agent, analyze them and extract the 4 requested information:
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
                        """
                if self.llm_type == 'openai' or self.llm_type == 'OpenAI':
                    knowledge_thread_id = self.client.beta.threads.create().id
                    self.client.beta.threads.messages.create(
                        thread_id=knowledge_thread_id, content=content, role="user"
                    )
                else: # cortex
                    knowledge_thread_id = ''
            response = None
            if (self.llm_type == 'openai' or self.llm_type == 'OpenAI') and knowledge_thread_id is not None:
                run = self.client.beta.threads.runs.create(
                    thread_id=knowledge_thread_id, assistant_id=self.assistant.id
                )
                while not self.client.beta.threads.runs.retrieve(
                    thread_id=knowledge_thread_id, run_id=run.id
                ).completed_at:
                    time.sleep(1)

                raw_knowledge = (
                    self.client.beta.threads.messages.list(knowledge_thread_id)
                    .data[0]
                    .content[0]
                    .text.value
                )
                try:                
                    response = json.loads(raw_knowledge)
                except:
                    response = None
                    print('Skipped thread ',knowledge_thread_id,' knowledge unparseable')
            else:
                system = "You are a Knowledge Explorer to extract, synthesize, and inject knowledge that bots learn from doing their jobs"
                res, status_code  = self.db_connector.cortex_chat_completion(content, system)
                response = ast.literal_eval(res.split("```")[1])
                
                

            try:
                if response is not None:
                    # Ensure the timestamp is in the correct format for Snowflake
                    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    timestamp = thread["TIMESTAMP"]
                    if type(msg_log[-1]["TIMESTAMP"]) != str:
                        last_timestamp = msg_log[-1]["TIMESTAMP"].strftime("%Y-%m-%d %H:%M:%S")
                    else:
                        last_timestamp = msg_log[-1]["TIMESTAMP"]
                    bot_id = msg_log[-1]["BOT_ID"]
                    primary_user = msg_log[-1]["PRIMARY_USER"]
                    thread_summary = response["thread_summary"]
                    user_learning = response["user_learning"]
                    tool_learning = response["tool_learning"]
                    data_learning = response["data_learning"]

                    self.db_connector.run_insert(self.db_connector.knowledge_table_name, timestamp=timestamp,thread_id=thread_id,knowledge_thread_id=knowledge_thread_id,
                                                primary_user=primary_user,bot_id=bot_id,last_timestamp=last_timestamp,thread_summary=thread_summary,
                                                user_learning=user_learning,tool_learning=tool_learning,data_learning=data_learning)

                    self.user_queue.put((primary_user, bot_id, response))
            except Exception as e:
                print(f"Encountered errors while inserting into {self.db_connector.knowledge_table_name} row: {e}")
            
            with self.thread_set_lock:
                self.thread_set.remove(thread_id)
                print(f"Consumed {thread_id}")

    def refiner(self):


        while True:
            if self.user_queue.empty():
                #print("Queue is empty, refiner is waiting...")
                time.sleep(refresh_seconds)
                continue
            primary_user, bot_id, knowledge = self.user_queue.get()
            print('refining...', flush=True)
            if primary_user is not None:
                try:
                    user_json = json.loads(primary_user)
                except Exception as e:
                    print('Error on user_json ',e)
                    print('    primary user is ',primary_user,' switching to unknown user')
                    primary_user = None
                    user_json = {'user_email': 'Unknown Email'}
            else:
                user_json = {'user_email': 'Unknown Email'}
            if user_json.get('user_email','Unknown Email') != 'Unknown Email':
                user_query = user_json['user_email']
            else:
                user_query = user_json.get('user_id', 'Unknown User ID')
                
            query = f"""SELECT * FROM {self.db_connector.user_bot_table_name}
                        WHERE primary_user = '{user_query}' AND BOT_ID = '{bot_id}'
                        ORDER BY TIMESTAMP DESC
                        LIMIT 1;"""

            user_bot_knowledge = self.db_connector.run_query(query)

            new_knowledge = {}
            prompts = {
                "USER_LEARNING": "user",
                "TOOL_LEARNING": "tools the user used",
                "DATA_LEARNING": "data the user used",
            }
            for item, prompt in prompts.items():
                raw_knowledge = knowledge[item.lower()]
                if user_bot_knowledge:
                    previous_knowledge = user_bot_knowledge[0][item]
                    content = f"""This is the previous summary:
                                {previous_knowledge}

                                And this is the new raw knowledge
                                {raw_knowledge}
                            """
                else:
                    content = f"""This is the new raw knowledge:
                                {raw_knowledge}
                            """
                if self.llm_type == 'openai' or self.llm_type == 'OpenAI':
                    response = self.client.chat.completions.create(
                        model=self.model,
                        messages=[
                            {
                                "role": "system",
                                "content": f"Use the following raw knowledge information about the interaction of the user and the bot, \
                                    summarize what we learned about the {prompt} in bullet point.",
                            },
                            {"role": "user", "content": content},
                        ],
                    )
                    new_knowledge[item] = response.choices[0].message.content
                else:
                    system = f"Use the following raw knowledge information about the interaction of the user and the bot, \
                                    summarize what we learned about the {prompt} in bullet point."
                    response, status_code  = self.db_connector.cortex_chat_completion(content, system)                    
                    new_knowledge[item] = response


            try:
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")                
                self.db_connector.run_insert(self.db_connector.user_bot_table_name, timestamp=timestamp, primary_user=user_query, bot_id=bot_id,                
                                              user_learning=new_knowledge["USER_LEARNING"],tool_learning=new_knowledge["TOOL_LEARNING"],
                                              data_learning=new_knowledge["DATA_LEARNING"])
            except Exception as e:
                print(f"Encountered errors while inserting into {self.db_connector.user_bot_table_name} row: {e}")


    def start_threads(self):
        producer_thread = threading.Thread(target=self.producer)
        consumer_thread = threading.Thread(target=self.consumer)
        refiner_thread = threading.Thread(target=self.refiner)

        producer_thread.start()
        consumer_thread.start()
        refiner_thread.start()

        producer_thread.join()
        consumer_thread.join()
        refiner_thread.join()
