import threading
import time
import json
import queue
import os
import sys
from openai import OpenAI
from datetime import datetime, timedelta
import ast
from genesis_bots.llm.llm_openai.openai_utils import get_openai_client
import pandas as pd
import re
import traceback
from genesis_bots.core.logging_config import logger

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
print('****** GENBOT VERSION 0.300 *******',flush=True)



refresh_seconds = os.getenv("KNOWLEDGE_REFRESH_SECONDS", 60)
refresh_seconds = int(refresh_seconds)

logger.info("waiting 60 seconds for other services to start first...")
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
        self.llm_type = llm_type.lower()
        self.sleepytime = True
        if llm_type == 'openai':
            self.openai_api_key = os.getenv("OPENAI_API_KEY")
            self.client = get_openai_client()
            self.model = os.getenv("OPENAI_KNOWLEDGE_MODEL", os.getenv('OPENAI_MODEL_NAME',"gpt-4o-2024-11-20"))
            self.assistant = self.client.beta.assistants.create(
                name="Knowledge Explorer",
                description="You are a Knowledge Explorer to extract, synthesize, and inject knowledge that bots learn from doing their jobs",
                model=self.model,
                response_format={"type": "json_object"},
            )

    def producer(self):
        while True:

            wake_up = False
            i = 0
            while not wake_up:
                time.sleep(refresh_seconds)
                self.sleepytime = True
                cursor = self.db_connector.client.cursor()
                check_bot_active = f"DESCRIBE TABLE {self.db_connector.schema}.BOTS_ACTIVE"
                cursor.execute(check_bot_active)
                print('KNOWLEDGE_SERVER:', 'RUN DESCRIBE TABLE BOTS_ACTIVE')
                result = cursor.fetchone()

                bot_active_time_dt = datetime.strptime(result[0], '%Y-%m-%d %H:%M:%S %Z')
                current_time = datetime.now()
                time_difference = current_time - bot_active_time_dt

                i += 1
                if i >= 30:
                    logger.info(f"BOTS ACTIVE TIME: {result[0]} | CURRENT TIME: {current_time} | TIME DIFFERENCE: {time_difference} | producer")
                    i = 0

                if time_difference < timedelta(minutes=5):
                    logger.info(f"Knowledge Server is Active | TIME DIFFERENCE: {time_difference}")
                    wake_up = True
                    self.sleepytime = False

            # join inside snowflake
            cutoff = (datetime.now() - timedelta(minutes=10)).strftime("%Y-%m-%d %H:%M:%S")
            threads = self.db_connector.query_threads_message_log(cutoff)
            print('KNOWLEDGE_SERVER:', 'query_threads_message_log')
            logger.info(f"Producer found {len(threads)} threads")
            for thread in threads:
                thread_id = thread["THREAD_ID"]
                with self.thread_set_lock:
                    if thread_id not in self.thread_set:
                        self.thread_set.add(thread_id)
                    else:
                        continue

                with self.condition:
                    if self.thread_queue.full():
                        logger.info("Queue is full, producer is waiting...")
                        self.condition.wait()
                    self.thread_queue.put(thread)
                    logger.info(f"Produced {thread_id}")
                    self.condition.notify()
    #                logger.info("Bot is active")

    def consumer(self):
        while True:
            with self.condition:
                if self.thread_queue.empty():
                    #logger.info("Queue is empty, consumer is waiting...")
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
            print('KNOWLEDGE_SERVER:', 'query_timestamp_message_log')

            non_bot_users_query = f"""
                WITH BOTS AS (SELECT BOT_SLACK_USER_ID,
                    CONCAT('{{"user_id": "', BOT_SLACK_USER_ID, '", "user_name": "', BOT_NAME, '", "user_email": "unknown_email"}}') as PRIMARY_USER
                    FROM  {self.db_connector.bot_servicing_table_name}),
                    BOTS2 AS (SELECT BOT_SLACK_USER_ID,
                    CONCAT('{{"user_id": "', BOT_SLACK_USER_ID, '", "user_name": "', BOT_NAME, '"}}') as PRIMARY_USER
                    FROM  {self.db_connector.bot_servicing_table_name})
                SELECT count(DISTINCT M.PRIMARY_USER) as CNT FROM {self.db_connector.message_log_table_name} M
                LEFT JOIN BOTS ON M.PRIMARY_USER = BOTS.PRIMARY_USER
                LEFT JOIN BOTS2 ON M.PRIMARY_USER = BOTS2.PRIMARY_USER
                WHERE THREAD_ID = '{thread_id}'
                AND  BOTS.BOT_SLACK_USER_ID IS NULL AND  BOTS2.BOT_SLACK_USER_ID IS NULL
                and m.primary_user <> '{{"user_id": "unknown_id", "user_name": "unknown_name"}}';
                """
                # this is needed to exclude channels with more than one user
            count_non_bot_users = self.db_connector.run_query(non_bot_users_query)
            print('KNOWLEDGE_SERVER:', 'count_non_bot_users')

            skipped_thread = False
            if count_non_bot_users and count_non_bot_users[0]["CNT"] != 1:
                logger.info(f"Skipped {thread_id}, {count_non_bot_users[0]['CNT']} non-bot-users is not 1")
                response = {'thread_summary': 'Skipped due to empty or multiple non-bot-users',
                            'user_learning' : 'Skipped due to empty or multiple non-bot-users',
                            'tool_learning' : 'Skipped due to empty or multiple non-bot-users',
                            'data_learning' : 'Skipped due to empty or multiple non-bot-users'}
                skipped_thread = True

            else:
                messages = [f"{msg['MESSAGE_TYPE']}: {msg['MESSAGE_PAYLOAD']}" for msg in msg_log if "'EMBEDDING': " not in msg['MESSAGE_PAYLOAD']]
                messages = "\n".join(messages)[:200_000] # limit to 200k char for now

                query = f"""SELECT DISTINCT(knowledge_thread_id) FROM {self.db_connector.knowledge_table_name}
                            WHERE thread_id = '{thread_id}';"""
                knowledge_thread_id = self.db_connector.run_query(query)
                print('KNOWLEDGE_SERVER:', 'knowledge_thread_id')
                if knowledge_thread_id and self.llm_type == 'openai':
                    knowledge_thread_id = knowledge_thread_id[0]["KNOWLEDGE_THREAD_ID"]
                    content = f"""Find a new batch of conversations between the user and agent and update 4 requested information in the original prompt and return it in JSON format:
                                Conversation:
                                {messages}
                            """
                    try:
                        logger.info('openai create ', knowledge_thread_id)
                        self.client.beta.threads.messages.create(
                            thread_id=knowledge_thread_id, content=content, role="user"
                        )
                    except Exception as e:
                        logger.info('openai create exception ', e)
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
                    if self.llm_type == 'openai':
                        knowledge_thread_id = self.client.beta.threads.create().id
                        self.client.beta.threads.messages.create(
                            thread_id=knowledge_thread_id, content=content, role="user"
                        )
                    else: # cortex
                        knowledge_thread_id = ''
                response = None
                if self.llm_type == 'openai' and knowledge_thread_id is not None:
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
                        logger.info('Skipped thread ',knowledge_thread_id,' knowledge unparseable')
                        response = {'thread_summary': 'Skipped due to invalid summary generated by LLM',
                            'user_learning' : 'Skipped due to invalid summary generated by LLM',
                            'tool_learning' : 'Skipped due to invalid summary generated by LLM',
                            'data_learning' : 'Skipped due to invalid summary generated by LLM'}
                        skipped_thread = True
                else:
                    system = "You are a Knowledge Explorer to extract, synthesize, and inject knowledge that bots learn from doing their jobs"
                    res, status_code  = self.db_connector.cortex_chat_completion(content, system=system)
                    print('KNOWLEDGE_SERVER:', 'cortex_chat_completion')
                    response = ast.literal_eval(res.split("```")[1])



            try:
                if response is not None:
                    # Ensure the timestamp is in the correct format for Snowflake
                    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    if type(msg_log[-1]["TIMESTAMP"]) != str:
                        last_timestamp = msg_log[-1]["TIMESTAMP"].strftime("%Y-%m-%d %H:%M:%S")
                    else:
                        last_timestamp = msg_log[-1]["TIMESTAMP"]
                    bot_id = msg_log[-1]["BOT_ID"]
                    primary_user = msg_log[-1]["PRIMARY_USER"]
                    thread_summary = response.get("thread_summary", '')
                    user_learning = response.get("user_learning",'')
                    tool_learning = response.get("tool_learning",'')
                    data_learning = response.get("data_learning",'')

                    self.db_connector.run_insert(self.db_connector.knowledge_table_name, timestamp=timestamp,thread_id=thread_id,knowledge_thread_id=knowledge_thread_id,
                                                primary_user=primary_user,bot_id=bot_id,last_timestamp=last_timestamp,thread_summary=thread_summary,
                                                user_learning=user_learning,tool_learning=tool_learning,data_learning=data_learning)
                    print('KNOWLEDGE_SERVER:', 'run_insert - line 256')
                    if not skipped_thread:
                        self.user_queue.put((primary_user, bot_id, response))
            except Exception as e:
                logger.info(f"Encountered errors processing knowledge for thread {thread_id}, {self.db_connector.knowledge_table_name} row: {e}")
                logger.info(traceback.format_exc())

            with self.thread_set_lock:
                self.thread_set.remove(thread_id)
                logger.info(f"Consumed {thread_id}")

    def refiner(self):
        while True:
            if self.user_queue.empty():
                #logger.info("Queue is empty, refiner is waiting...")
                time.sleep(refresh_seconds)
                continue
            primary_user, bot_id, knowledge = self.user_queue.get()
            logger.info('refining...')
            if primary_user is not None:
                try:
                    user_json = json.loads(primary_user)
                except Exception as e:
                    logger.info('Error on user_json ',e)
                    logger.info('    primary user is ',primary_user,' switching to unknown user')
                    primary_user = None
                    user_json = {'user_email': 'unknown_email'}
            else:
                user_json = {'user_email': 'unknown_email'}
            if user_json.get('user_email','unknown_email') != 'unknown_email':
                user_query = user_json['user_email']
            else:
                user_query = user_json.get('user_id', 'unknown_id')

            query = f"""SELECT * FROM {self.db_connector.user_bot_table_name}
                        WHERE primary_user = '{user_query}' AND BOT_ID = '{bot_id}'
                        ORDER BY TIMESTAMP DESC
                        LIMIT 1;"""

            user_bot_knowledge = self.db_connector.run_query(query)
            print('KNOWLEDGE_SERVER:', 'user_bot_knowledge - line 296')

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
                if self.llm_type == 'openai':
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
                    response, status_code  = self.db_connector.cortex_chat_completion(content, system=system)
                    print('KNOWLEDGE_SERVER:', 'cortex_chat_completion - line 335')
                    new_knowledge[item] = response


            try:
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                self.db_connector.run_insert(self.db_connector.user_bot_table_name, timestamp=timestamp, primary_user=user_query, bot_id=bot_id,
                                              user_learning=new_knowledge["USER_LEARNING"],tool_learning=new_knowledge["TOOL_LEARNING"],
                                              data_learning=new_knowledge["DATA_LEARNING"])
                print('KNOWLEDGE_SERVER:', 'run_insert - line 344')
            except Exception as e:
                logger.info(f"Encountered errors while inserting into {self.db_connector.user_bot_table_name} row: {e}")
                logger.info(traceback.format_exc())


    def tool_knowledge(self):
        while True:
            if self.sleepytime:
                logger.info("tool_knowledge is sleeping for 120 seconds...") 
                time.sleep(120)
                continue
            cutoff = (datetime.now() - timedelta(minutes=10)).strftime("%Y-%m-%d %H:%M:%S")
            query = f"""
                    WITH K AS (SELECT COALESCE(max(last_timestamp),  DATE('2000-01-01')) as last_timestamp FROM {self.db_connector.tool_knowledge_table_name})
                    SELECT * FROM {self.db_connector.message_log_table_name}, K
                    WHERE timestamp > K.last_timestamp AND timestamp < TO_TIMESTAMP('{cutoff}')
                    AND MESSAGE_TYPE LIKE 'Tool%'
                    ORDER by timestamp;
                    """
            tools = self.db_connector.run_query(query, max_rows=100)
            print('KNOWLEDGE_SERVER:', 'run_query - line 362')
            if tools:
                last_timestamp = max([row['TIMESTAMP'] for row in tools])
                function_name = None
                bot_id = None
                groups = {}
                for row in tools:
                    if row['MESSAGE_TYPE'] == 'Tool Call':
                        function_name = row['MESSAGE_PAYLOAD'].split('(', 1)[0]
                        if 'action' in row['MESSAGE_PAYLOAD']:
                            action = re.findall('"action":"(.+?)"', row['MESSAGE_PAYLOAD'])
                            if action:
                                function_name += '_' + action[0]
                        function_params = row['MESSAGE_PAYLOAD']
                        bot_id = row['BOT_ID']
                    else:
                        if "'success': False" in row['MESSAGE_PAYLOAD']: continue
                        if bot_id is None: continue
                        groups.setdefault((bot_id, function_name), [])
                        groups[(bot_id, function_name)].append(f'{function_params}:\n\n' + row['MESSAGE_PAYLOAD'][:200])

                for (bot_id, function_name), function_content in groups.items():
                    if function_content:
                        messages = '\n\n'.join(function_content)
                        system = 'Given the following outputs from a tool call summerize how this tool is used for future reference'
                        content = f"""
                                    Function Name:
                                    {function_name}

                                    Function Outputs:
                                    {messages}
                                """
                        if self.llm_type == 'openai':
                            response = self.client.chat.completions.create(
                                model=self.model,
                                messages=[{"role": "system", "content": system},
                                        {"role": "user", "content": content}]
                            )
                            response = response.choices[0].message.content
                        else:
                            response, status_code  = self.db_connector.cortex_chat_completion(content, system=system)
                            print('KNOWLEDGE_SERVER:', 'cortex_chat_completion - line 403')
                        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        self.db_connector.run_insert(self.db_connector.tool_knowledge_table_name, timestamp=timestamp,
                                                    last_timestamp=last_timestamp, bot_id=bot_id, tool = function_name, summary=response)
                        print('KNOWLEDGE_SERVER:', 'run_insert - line 407')

            logger.info(f"Pausing Tool Knowledge for {refresh_seconds} seconds before next check.")
            time.sleep(refresh_seconds)

    def data_knowledge(self):
        while True:
            if self.sleepytime:
                logger.info("data_knowledge is sleeping for 120 seconds...") 
                time.sleep(120)
                continue
            cutoff = (datetime.now() - timedelta(minutes=10)).strftime("%Y-%m-%d %H:%M:%S")
            query = f"""
                    WITH K AS (SELECT COALESCE(max(last_timestamp),  DATE('2000-01-01')) as last_timestamp FROM {self.db_connector.data_knowledge_table_name})
                    SELECT * FROM {self.db_connector.message_log_table_name}, K
                    WHERE timestamp > K.last_timestamp AND timestamp < TO_TIMESTAMP('{cutoff}')
                    AND MESSAGE_TYPE LIKE 'Tool%'
                    ORDER by timestamp;
                    """
            tools = self.db_connector.run_query(query, max_rows=100)
            print('KNOWLEDGE_SERVER:', 'run_query - data_knowledge')
            if tools:
                last_timestamp = max([row['TIMESTAMP'] for row in tools])
                groups = {}
                bot_id = None
                for row in tools:
                    if row['MESSAGE_TYPE'] == 'Tool Call':
                        if 'run_query' not in row['MESSAGE_PAYLOAD']:
                            continue
                        func_args = str(json.loads(row['MESSAGE_METADATA'])['func_args'])
                        dataset = re.findall('from (.+?) ', func_args.lower().replace('\\','',))
                        if dataset:
                            dataset = dataset[0]
                        else:
                            dataset = ''
                        bot_id = row['BOT_ID']
                    else:
                        if "'success': False" in row['MESSAGE_PAYLOAD']: continue
                        if bot_id is None: continue
                        groups.setdefault((bot_id, dataset), [])
                        groups[(bot_id, dataset)].append(f'{func_args}:\n\n' + row['MESSAGE_PAYLOAD'][:200])

                for (bot_id, dataset), function_content in groups.items():
                    if function_content:
                        messages = '\n\n'.join(function_content)
                        system = 'Given the following outputs from a database call summerize how this table is used for future reference'
                        content = f"""
                                    Dataset Name:
                                    {dataset}

                                    Query Outputs:
                                    {messages}
                                """
                        if self.llm_type == 'openai':
                            response = self.client.chat.completions.create(
                                model=self.model,
                                messages=[{"role": "system", "content": system},
                                        {"role": "user", "content": content}]
                            )
                            response = response.choices[0].message.content
                        else:
                            response, status_code  = self.db_connector.cortex_chat_completion(content, system=system)
                            print('KNOWLEDGE_SERVER:', 'cortex_chat_completion - line 466')
                        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        self.db_connector.run_insert(self.db_connector.data_knowledge_table_name, timestamp=timestamp,
                                                    last_timestamp=last_timestamp, bot_id=bot_id, dataset = dataset, summary=response)
                        print('KNOWLEDGE_SERVER:', 'run_insert - line 470')
            logger.info(f"Pausing Data Knowledge for {refresh_seconds} seconds before next check.")
            time.sleep(refresh_seconds)

    def proc_knowledge(self):
        while True:
            if self.sleepytime:
                logger.info("proc_knowledge is sleeping for 120 seconds...")  
                time.sleep(120)
                continue
            cutoff = (datetime.now() - timedelta(minutes=10)).strftime("%Y-%m-%d %H:%M:%S")
            query = f"""
                    WITH K AS (SELECT COALESCE(max(last_timestamp),  DATE('2000-01-01')) as last_timestamp FROM {self.db_connector.proc_knowledge_table_name})
                    SELECT DISTINCT THREAD_ID FROM {self.db_connector.message_log_table_name}, K
                    WHERE timestamp > K.last_timestamp AND timestamp < TO_TIMESTAMP('{cutoff}')
                    AND MESSAGE_PAYLOAD LIKE '%run_process%';
                    """
            processes = self.db_connector.run_query(query, max_rows=100)
            print('KNOWLEDGE_SERVER:', 'run_query - line 486')
            for process in processes:
                thread_id = process['THREAD_ID']
                query = f"""
                        SELECT * FROM {self.db_connector.message_log_table_name}
                        WHERE THREAD_ID = '{thread_id}'
                        ORDER by timestamp;
                        """
                rows = self.db_connector.run_query(query, max_rows=100)
                last_timestamp = max([row['TIMESTAMP'] for row in rows])
                bot_id = max([row['BOT_ID'] for row in rows])

                message_content = []
                process_name = None
                i = 0
                while i < len(rows):
                    row = rows[i]
                    try:
                        meta = json.loads(row['MESSAGE_METADATA'])
                        if 'process_name' in meta:
                            process_name = meta['process_name']
                    except:
                        pass
                    if row['MESSAGE_TYPE'] == 'Supervisor Prompt':
                        prompt = row['MESSAGE_PAYLOAD']
                        if i < len(rows) - 1:
                            response = rows[i + 1]['MESSAGE_PAYLOAD']
                            i += 1
                            message_content.append(f'Prompt: {prompt}\n\nResponse: {response}')
                    i += 1

                if message_content and process_name is not None:
                    messages = '\n\n'.join(message_content)
                    system = 'Given the following outputs from a process call check if there are any issues which can be avoided for future reference'
                    content = f"""
                                Process Name:
                                {process_name}

                                Process Outputs:
                                {messages}
                            """
                    if self.llm_type == 'openai':
                        response = self.client.chat.completions.create(
                            model=self.model,
                            messages=[{"role": "system", "content": system},
                                    {"role": "user", "content": content}]
                        )
                        response = response.choices[0].message.content
                    else:
                        response, status_code  = self.db_connector.cortex_chat_completion(content, system=system)
                        print('KNOWLEDGE_SERVER:', 'cortex_chat_completion - line 536')
                    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    self.db_connector.run_insert(self.db_connector.proc_knowledge_table_name, timestamp=timestamp,
                                                 last_timestamp=last_timestamp, bot_id=bot_id, process = process_name, summary=response)
                    print('KNOWLEDGE_SERVER:', 'run_insert - line 540')

            logger.info(f"Pausing Proc Knowledge for {refresh_seconds} seconds before next check.")
            time.sleep(refresh_seconds)

    def start_threads(self):
        producer_thread = threading.Thread(target=self.producer)
        consumer_thread = threading.Thread(target=self.consumer)
        refiner_thread  = threading.Thread(target=self.refiner)
        tool_thread     = threading.Thread(target=self.tool_knowledge)
        data_thread     = threading.Thread(target=self.data_knowledge)
        proc_thread     = threading.Thread(target=self.proc_knowledge)

        producer_thread.start()
        consumer_thread.start()
        refiner_thread.start()
        tool_thread.start()
        data_thread.start()
        proc_thread.start()

        producer_thread.join()
        consumer_thread.join()
        refiner_thread.join()
        tool_thread.join()
        data_thread.join()
        proc_thread.join()
