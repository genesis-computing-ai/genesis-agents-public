import json
import os
import time
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from openai import OpenAI
from datetime import datetime
import threading 
import random
import string
import pytz


from jinja2 import Template
from bot_genesis.make_baby_bot import MAKE_BABY_BOT_DESCRIPTIONS, make_baby_bot_tools
from connectors import database_tools
from connectors.bigquery_connector import BigQueryConnector
from connectors.snowflake_connector import SnowflakeConnector
from connectors.sqlite_connector import SqliteConnector
from slack.slack_tools import slack_tools, slack_tools_descriptions
from connectors.database_tools import (
    image_functions,
    image_tools,
    bind_run_query,
    bind_search_metadata,
    bind_semantic_copilot,
    autonomous_functions,
    autonomous_tools,
    process_manager_tools,
    process_manager_functions,
    database_tool_functions,
    database_tools,
    snowflake_stage_functions,
    snowflake_stage_tools,
    snowflake_semantic_functions,
    snowflake_semantic_tools,
    process_scheduler_functions,
    process_scheduler_tools,
)
from schema_explorer.harvester_tools import (
    harvester_tools_list,
    harvester_tools_functions,
)
from development.integration_tools import (
    integration_tool_descriptions,
    integration_tools,
)
from bot_genesis.make_baby_bot import get_bot_details
from core.bot_os import BotOsSession
from core.bot_os_corpus import URLListFileCorpus
from core.bot_os_defaults import (
    BASE_BOT_INSTRUCTIONS_ADDENDUM,
    BASE_BOT_PRE_VALIDATION_INSTRUCTIONS,
    BASE_BOT_PROACTIVE_INSTRUCTIONS,
    BASE_BOT_VALIDATION_INSTRUCTIONS,
)
from core.bot_os_input import BotOsInputAdapter, BotOsInputMessage, BotOsOutputMessage
from core.bot_os_memory import BotOsKnowledgeAnnoy_Metadata

from core.bot_os_tool_descriptions import process_runner_tools


# import sys
# sys.path.append('/Users/mglickman/helloworld/bot_os')  # Adjust the path as necessary
import logging

from core.bot_os_tool_descriptions import (
    process_runner_functions,
    process_runner_tools,
    webpage_downloader_functions,
    webpage_downloader_tools,
)

logger = logging.getLogger(__name__)

genesis_source = os.getenv("GENESIS_SOURCE", default="Snowflake")

# module level
belts = 0

class ToolBelt:
    def __init__(self, db_adapter, openai_api_key=None):
        self.db_adapter = db_adapter
        self.openai_api_key = os.getenv("OPENAI_API_KEY",None) # openai_api_key 

        # print(f"API KEY IN ENV VAR OPENAI_API_KEY: {self.openai_api_key}")

        self.client = OpenAI(api_key=openai_api_key) if openai_api_key else None
        self.counter = {}
        self.instructions = {}
        self.process_config = {}
        self.process_history = {}
        self.done = {}
        self.silent_mode = {}
        self.last_fail= {}
        self.fail_count = {}
        self.lock = threading.Lock()
        global belts
        belts = belts + 1 

        self.sys_default_email = self.get_sys_email()
   #     print(belts)

    # Function to make HTTP request and get the entire content
    def get_webpage_content(self, url):
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.service import Service
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        import time
        chrome_options = Options()
        chrome_options.add_argument("--headless")  # Run in headless mode (no browser window)
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")

        current_file_path = os.path.abspath(__file__)
        print(current_file_path)

        service = Service('../../chromedriver')  
        # driver = webdriver.Chrome(service=service, options=chrome_options)
        driver = webdriver.Chrome(options=chrome_options)

        driver.get(url)    
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, 'body'))
            )
        except Exception as e:
            print("Error: ", e)
            driver.quit()

        data = driver.page_source #find_element(By.XPATH, '//*[@id="data-id"]').text
        print(f"Data scraped from {url}: \n{data}\n")
        return data  

    # Function for parsing HTML content, extracting links, and then chunking the beautified content
    def parse_and_chunk_content(self, content, base_url, chunk_size=256 * 1024):
        soup = BeautifulSoup(content, "html.parser")
        links = [urljoin(base_url, a["href"]) for a in soup.find_all("a", href=True)]
        pretty_content = soup.prettify()
        encoded_content = pretty_content.encode("utf-8")
        encoded_links = json.dumps(links).encode("utf-8")

        # Combine the content and links
        combined_content = encoded_content + encoded_links

        # Chunk the combined content
        chunks = []
        for i in range(0, len(combined_content), chunk_size):
            chunks.append({"content": combined_content[i : i + chunk_size]})

        if not chunks:
            raise ValueError("No content available within the size limit.")

        return chunks, len(chunks)  # Return chunks and total number of chunks

    # Main function to download webpage, extract links, and ensure each part is within the size limit
    def download_webpage(self, url, chunk_index=0, thread_id=None):
        try:
            content = self.get_webpage_content(url)
            chunks, total_chunks = self.parse_and_chunk_content(content, url)
            if chunk_index >= total_chunks:
                return {"error": "Requested chunk index exceeds available chunks."}

            response = {
                "chunk": chunks[chunk_index],
                "next_chunk_index": (
                    chunk_index + 1 if chunk_index + 1 < total_chunks else None
                ),
                "total_chunks": total_chunks,
            }
            return response
        except Exception as e:
            return {"error": str(e)}
    
    def chat_completion(self, message, db_adapter, bot_id = None, bot_name = None, thread_id=None, process_id="", process_name=""):
        process_name = "" if process_name is None else process_name
        process_id = "" if process_id is None else process_id
        message_metadata ={"process_id": process_id, "process_name": process_name}
        return_msg = None

        self.write_message_log_row(db_adapter, bot_id, bot_name, thread_id, 'Supervisor Prompt', message, message_metadata)

        model = None

        if "BOT_LLMS" in os.environ and os.environ["BOT_LLMS"]:
            # Convert the JSON string back to a dictionary
            bot_llms = json.loads(os.environ["BOT_LLMS"])

        # Find the model for the specific bot_id in bot_llms
        model = None
        if bot_id and bot_id in bot_llms:
            model = bot_llms[bot_id].get('current_llm')
            
            
        if not model:
            if os.getenv("BOT_OS_DEFAULT_LLM_ENGINE",'').lower() == 'openai':
                model = 'openai'
            else:
                model = 'cortex'

        if model == 'openai':
                    api_key = os.getenv("OPENAI_API_KEY")
                    if not api_key:
                        print("OpenAI API key is not set in the environment variables.")
                        return None

                    openai_model = os.getenv("OPENAI_MODEL_SUPERVISOR",os.getenv("OPENAI_MODEL_NAME","gpt-4o"))

                    print('process supervisor using model: ', openai_model)
                    try:
                        openai_api_key = os.getenv("OPENAI_API_KEY")
                        client = OpenAI(api_key=openai_api_key)
                        response = client.chat.completions.create(
                            model=openai_model,
                            messages=[
                                {
                                    "role": "user",
                                    "content": message,
                                },
                            ],
                        )
                    except Exception as e:
                        if os.getenv("OPENAI_MODEL_SUPERVISOR", None) is not None:
                            print(f"Error occurred while calling OpenAI API with supervisor model {openai_model}: {e}")
                            print(f'Retrying with main model {os.getenv("OPENAI_MODEL_NAME","gpt-4o")}')
                            openai_model = os.getenv("OPENAI_MODEL_NAME","gpt-4o")
                            response = client.chat.completions.create(
                                model=openai_model,
                                messages=[
                                    {
                                        "role": "user",
                                        "content": message,
                                    },
                                ],
                            )
                        else:
                            print(f"Error occurred while calling OpenAI API: {e}")

                    return_msg = response.choices[0].message.content

        elif model == 'cortex':
            if not db_adapter.check_cortex_available():
                print("Cortex is not available.")
                return None
            else:
                response, status_code = db_adapter.cortex_chat_completion(message)
                return_msg = response
        
        if return_msg is None:
            return_msg = 'Error Chat_completion, return_msg is none, llm_type = ',os.getenv("BOT_OS_DEFAULT_LLM_ENGINE").lower()
            print(return_msg)

        self.write_message_log_row(db_adapter, bot_id, bot_name, thread_id, 'Supervisor Response', return_msg, message_metadata)
            
        return return_msg

    def write_message_log_row(self, db_adapter, bot_id="", bot_name="", thread_id="", message_type="", message_payload="", message_metadata={}):
        """
        Inserts a row into the MESSAGE_LOG table.

        Args:
            db_adapter: The database adapter to use for the insertion.
            bot_id (str): The ID of the bot.
            bot_name (str): The name of the bot.
            thread_id (str): The ID of the thread.
            message_type (str): The type of the message.
            message_payload (str): The payload of the message.
            message_metadata (str): The metadata of the message.
        """
        timestamp = datetime.now()
        query = f"""
            INSERT INTO {db_adapter.schema}.MESSAGE_LOG (timestamp, bot_id, bot_name, thread_id, message_type, message_payload, message_metadata)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        # print(f"Writing message log row: {timestamp}, {bot_id}, {bot_name}, {thread_id}, {message_type}, {message_payload}, {message_metadata}")
        values = (timestamp, bot_id, bot_name, thread_id, message_type, message_payload, json.dumps(message_metadata))
        
        try:
            cursor = db_adapter.connection.cursor()
            cursor.execute(query, values)
            db_adapter.connection.commit()
        except Exception as e:
            print(f"Error writing message log row: {e}")
            db_adapter.connection.rollback()
        finally:
            cursor.close()

    def send_email(self, to_addr_list: list, subject: str, body: str, thread_id: str = None, bot_id: str = None):
        """
        Send an email using Snowflake's SYSTEM$SEND_EMAIL function.

        Args:
            to_addr_list (list): A list of recipient email addresses.
            subject (str): The subject of the email.
            body (str): The body content of the email.
            thread_id (str, optional): The thread ID for the current operation.
            bot_id (str, optional): The bot ID for the current operation.

        Returns:
            dict: The result of the query execution.
        """

        # Check if to_addr_list is a string representation of a list
        if isinstance(to_addr_list, str):
            try:
                # Attempt to parse the string as a Python list
                if to_addr_list.startswith('[') and to_addr_list.endswith(']'):
                    # Remove brackets and split by comma
                    content = to_addr_list[1:-1]
                    parsed_list = [addr.strip().strip("'\"") for addr in content.split(',') if addr.strip()]
                    if parsed_list:
                        to_addr_list = parsed_list
                    else:
                        raise ValueError("Parsed result is an empty list")
                else:
                    # If it's not in list format, split by comma
                    to_addr_list = [addr.strip() for addr in to_addr_list.split(',') if addr.strip()]
            except Exception:
                # If parsing fails, split by comma
                to_addr_list = [addr.strip() for addr in to_addr_list.split(',')]
        
        # Ensure to_addr_list is a list
        if not isinstance(to_addr_list, list):
            to_addr_list = [to_addr_list]
        
        # Remove any empty strings and strip quotes from each address
        to_addr_list = [addr.strip("'\"") for addr in to_addr_list if addr]
        
        if not to_addr_list:
            return {"Success": False, "Error": "No valid email addresses provided."}
        # Join the email addresses with commas
        to_addr_string = ', '.join(to_addr_list)
        
        # Remove any instances of $$ from to_addr_string, subject and body
        to_addr_string = to_addr_string.replace('$$', '')
        subject = subject.replace('$$', '')
        body = body.replace('$$', '')
        query = f"""
        CALL SYSTEM$SEND_EMAIL(
            'genesis_email_int',
            $${to_addr_string}$$,
            $${subject}$$,
            $${body}$$
        );
        """
        
        # Execute the query using the database adapter's run_query method
        result = self.db_adapter.run_query(query, thread_id=thread_id, bot_id=bot_id)
        
        return result


    def set_process_cache(self, bot_id, thread_id, process_id):
        cache_dir = "./process_cache"
        os.makedirs(cache_dir, exist_ok=True)
        cache_file = os.path.join(cache_dir, f"{bot_id}_{thread_id}_{process_id}.json")
        
        cache_data = {
            "counter": self.counter.get(thread_id, {}).get(process_id),
            "last_fail": self.last_fail.get(thread_id, {}).get(process_id),
            "fail_count": self.fail_count.get(thread_id, {}).get(process_id),
            "instructions": self.instructions.get(thread_id, {}).get(process_id),
            "process_history": self.process_history.get(thread_id, {}).get(process_id),
            "done": self.done.get(thread_id, {}).get(process_id),
            "silent_mode":  self.silent_mode.get(thread_id, {}).get(process_id)
        }
        
        with open(cache_file, 'w') as f:
            json.dump(cache_data, f)

        

    def get_process_cache(self, bot_id, thread_id, process_id):
        cache_file = os.path.join("./process_cache", f"{bot_id}_{thread_id}_{process_id}.json")
        
        if os.path.exists(cache_file):
            with open(cache_file, 'r') as f:
                cache_data = json.load(f)
            
            with self.lock:
                if thread_id not in self.counter:
                    self.counter[thread_id] = {}
                self.counter[thread_id][process_id] = cache_data.get("counter")
                
                if thread_id not in self.last_fail:
                    self.last_fail[thread_id] = {}
                self.last_fail[thread_id][process_id] = cache_data.get("last_fail")
                
                if thread_id not in self.fail_count:
                    self.fail_count[thread_id] = {}
                self.fail_count[thread_id][process_id] = cache_data.get("fail_count")

                if thread_id not in self.instructions:
                    self.instructions[thread_id] = {}
                self.instructions[thread_id][process_id] = cache_data.get("instructions")
                
                if thread_id not in self.process_history:
                    self.process_history[thread_id] = {}
                self.process_history[thread_id][process_id] = cache_data.get("process_history")
                
                if thread_id not in self.done:
                    self.done[thread_id] = {}
                self.done[thread_id][process_id] = cache_data.get("done")
                
                if thread_id not in self.silent_mode:
                    self.silent_mode[thread_id] = {}
                self.silent_mode[thread_id][process_id] = cache_data.get("silent_mode", False)
            
            return True
        return False

    def clear_process_cache(self, bot_id, thread_id, process_id):
        cache_file = os.path.join("./process_cache", f"{bot_id}_{thread_id}_{process_id}.json")
        
        if os.path.exists(cache_file):
            os.remove(cache_file)
            return True
        return False
    
    def get_current_time_with_timezone(self):
        current_time = datetime.now().astimezone()
        return current_time.strftime("%Y-%m-%d %H:%M:%S %Z")
    
    def get_sys_email(self):
        cursor = db_adapter.client.cursor()
        try:
            get_sys_email_query = f"SELECT default_email FROM {db_adapter.genbot_internal_project_and_schema}.DEFAULT_EMAIL"
            cursor.execute(get_sys_email_query)
            result = cursor.fetchall()
            default_email = result[0][0] if result else None
            return default_email
        except Exception as e:
            print(f"Error getting sys email: {e}")
            return None

    def run_process(
        self,
        action,
        previous_response="",
        process_name="",
        process_id=None,
        process_config=None,
        thread_id=None,
        bot_id=None,
        concise_mode=False,
        bot_name=None
    ):
      #  print(f"Running processes Action: {action} | process_id: {process_id or 'None'} | Thread ID: {thread_id or 'None'}")

        if process_id is not None and process_id == '':
            process_id = None
        if process_name is not None and process_name == '':
            process_name = None

        if action == "TIME":
            return {
                "current_system_time": datetime.now()
            }

        if bot_id is None:
            return {
                "Success": False,
                "Error": "Bot_id and either process_id or process_name are required parameters."
            }
        
        # Convert verbose to boolean if it's a string

        # Invert silent_mode if it's a boolean
        silent_mode = concise_mode
        if isinstance(silent_mode, bool):
            verbose = not silent_mode
        
        if isinstance(silent_mode, str):
            if silent_mode.upper() == 'TRUE':
                silent_mode = True
                verbose = False
            else:
                silent_mode = False
                verbose = True

        # Ensure verbose is a boolean
        if not isinstance(silent_mode, bool):
            verbose = True
        
        # Check if both process_name and process_id are None
        if process_name is None and process_id is None:
            return {
                "Success": False,
                "Error": "Either process_name or process_id must be provided."
            }

        # Initialize thread-specific data structures if not already present
        with self.lock:
            if thread_id not in self.counter:
                self.counter[thread_id] = {}
         #   if thread_id not in self.process:
         #       self.process[thread_id] = {}
            if thread_id not in self.last_fail:
                self.last_fail[thread_id] = {}
            if thread_id not in self.fail_count:
                self.fail_count[thread_id] = {}
            if thread_id not in self.instructions:
                self.instructions[thread_id] = {}
            if thread_id not in self.process_history:
                self.process_history[thread_id] = {}
            if thread_id not in self.done:
                self.done[thread_id] = {}
            if thread_id not in self.silent_mode:
                self.silent_mode[thread_id] = {}
            if thread_id not in self.process_config:
                self.process_config[thread_id] = {}

        # Try to get process info from PROCESSES table
        process = self.get_process_info(bot_id, process_name=process_name, process_id=process_id)

        if len(process) == 0:
            # Get a list of processes for the bot
            processes = self.db_adapter.get_processes_list(bot_id)
            if processes is not None:
                process_list = ", ".join([p['process_name'] for p in processes['processes']])
                return_dict = {
                    "Success": False,
                    "Message": f"Process not found. Available processes are {process_list}.",
                    "Suggestion": "If one of the available processess is a very close match for what you're looking for, go ahead and run it."
                }
                if silent_mode is True:
                    return_dict["Reminder"] = "Remember to call the process in concise_mode as requested previously once you identify the right one"
                return return_dict
            else:
                return {
                    "Success": False,
                    "Message": f"Process not found. {bot_id} has no processes defined.",
                }        
        process = process['Data']       
        process_id = process['PROCESS_ID']
        process_name = process['PROCESS_NAME']
        process_config = process.get('PROCESS_CONFIG', '')
        if process_config is None:
            process_config = "None"
            process['PROCESS_CONFIG'] = "None"

        if action == "KICKOFF_PROCESS":
            print("Kickoff process.")
            
            with self.lock:
                self.counter[thread_id][process_id] = 1
         #       self.process[thread_id][process_id] = process
                self.last_fail[thread_id][process_id] = None
                self.fail_count[thread_id][process_id] = 0
                self.instructions[thread_id][process_id] = None
                self.process_config[thread_id][process_id] = process_config
                self.process_history[thread_id][process_id] = None
                self.done[thread_id][process_id] = False
                self.silent_mode[thread_id][process_id] = silent_mode


            print(
                f"Process {process_name} has been kicked off.  Process object: \n{process}\n\n"
            )

            extract_instructions = f"""
You will need to break the process instructions below up into individual steps and and return them one at a time.  
By the way the current system time is {datetime.now()}.
Start by returning the first step of the process instructions below.
Simply return the first instruction on what needs to be done first without removing or changing any details.

Process Instructions:
{process['PROCESS_INSTRUCTIONS']}
""" 

            if process['PROCESS_CONFIG'] != "None":
                extract_instructions += f"""

Process configuration: 
{process['PROCESS_CONFIG']}.
""" 

            first_step = self.chat_completion(extract_instructions, self.db_adapter, bot_id = bot_id, bot_name = '', thread_id=thread_id, process_id=process_id, process_name=process_name)
            with self.lock:
                self.process_history[thread_id][process_id] = "First step: "+ first_step + "\n"

                self.instructions[thread_id][process_id] = f"""
Hey **@{process['BOT_ID']}**

{first_step}

Execute this instruction now and then pass your response to the _run_process tool as a parameter called previous_response and an action of GET_NEXT_STEP.  
Execute the instructions you were given without asking for permission.
Do not ever verify anything with the user, unless you need to get a specific input from the user to be able to continue the process.
"""
            if self.sys_default_email:
                self.instructions[thread_id][process_id] += f"""
The system default email address (SYS$DEFAULT_EMAIL) is {self.sys_default_email}.  If you need to send an email, use this address.
"""
            

#            if self.process_config[thread_id][process_id]:
#                self.instructions[thread_id][process_id] += f"""
#Process configuration: {self.process_config[thread_id][process_id]}.
#"""
            if verbose:
                    self.instructions[thread_id][process_id] += """
However DO generate text explaining what you are doing and showing interium outputs, etc. while you are running this and further steps to keep the user informed what is going on, preface these messages by 🔄 aka :arrows_counterclockwise:.
Oh, and mention to the user before you start running the process that they can send "stop" to you at any time to stop the running of the process, and if they want less verbose output next time they can run request to run the process in "concise mode".
And keep them informed while you are running the process about what you are up to, especially before you call various tools.
"""
            else:
                self.instructions[thread_id][process_id] += """
This process is being run in low verbosity mode. Do not directly repeat the first_step instructions to the user, just perform the steps as instructed.
"""
            self.instructions[thread_id][process_id] += f"""
In your response back to _run_process, provide a DETAILED description of what you did, what result you achieved, and why you believe this to have successfully completed the step.
Do not use your memory or any cache that you might have.  Do not simulate any user interaction or tools calls.  Do not ask for any user input unless instructed to do so.
If you are told to run another process as part of this process, actually run it, and run it completely before returning the results to this parent process.
By the way the current system time is {datetime.now()}.  You can call manage_process with
action TIME to get updated time if you need it when running the process.

Now, start by performing the FIRST_STEP indicated above.
"""
          #  if not verbose:
          #      self.instructions[thread_id][process_id] += "..... P.S. I KNOW YOU ARE IN SILENT MODE BUT ACTUALLY PERFORM THIS STEP NOW, YOU ARE NOT DONE YET!"

            self.instructions[thread_id][process_id] = "\n".join(
                line.lstrip() for line in self.instructions[thread_id][process_id].splitlines()
                )

            print("\nKICK-OFF STEP: \n", self.instructions[thread_id][process_id], "\n")

            # Call set_process_cache to save the current state
            self.set_process_cache(bot_id, thread_id, process_id)
            print(f'Process cached with bot_id: {bot_id}, thread_id: {thread_id}, process_id: {process_id}')

            return {"Success": True, "Instructions": self.instructions[thread_id][process_id], "process_id": process_id}

        elif action == "GET_NEXT_STEP":
            print("Entered GET NEXT STEP")
            
            if thread_id not in self.counter and process_id not in self.counter[thread_id]:
                return {
                    "Success": False,
                    "Message": f"Error: GET_NEXT_STEP seems to have been run before KICKOFF_PROCESS. Please retry from KICKOFF_PROCESS."
                }

            # Load process cache
            if not self.get_process_cache(bot_id, thread_id, process_id):
                return {
                    "Success": False,
                    "Message": f"Error: Process cache for {process_id} couldn't be loaded. Please retry from KICKOFF_PROCESS."
                }
            # Print that the process cache has been loaded and the 3 params to get_process_cache
            print(f"Process cache loaded with params: bot_id: {bot_id}, thread_id: {thread_id}, process_id: {process_id}")
                
            # Check if silent_mode is set for the thread and process
            verbose = True
            if thread_id in self.silent_mode and process_id in self.silent_mode[thread_id]:
                if self.silent_mode[thread_id][process_id]:
                    verbose = False

            with self.lock:
                if process_id not in self.process_history[thread_id]:
                    return {
                        "Success": False,
                        "Message": f"Error: Process {process_name} with id {process_id} couldn't be continued. Please retry once more from KICKOFF_PROCESS."
                    }

                if self.done[thread_id][process_id]:
                    self.last_fail[thread_id][process_id] = None
                    self.fail_count[thread_id][process_id] = None
                    return {
                        "Success": True,
                        "Message": f"Process {process_name} run complete.",
                    }

                if self.last_fail[thread_id][process_id] is not None:
                    check_response = f"""
A bot has retried a step of a process based on your prior feedback (shown below).  Also below is the previous question that the bot was 
asked and the response the bot gave after re-trying to perform the task based on your feedback.  Review the response and determine if the 
bot's response is now better in light of the instructions and the feedback you gave previously. You can accept the final results of the
previous step without asking to see the sql queries and results that led to the final conclusion.  Do not nitpick validity of actual data value 
like names and similar. If you are very seriously concerned that the step 
may still have not have been correctly perfomed, return a request to again re-run the step of the process by returning the text "**fail**" 
followed by a DETAILED EXPLAINATION as to why it did not pass and what your concern is, and why its previous attempt to respond to your criticism 
was not sufficient, and any suggestions you have on how to succeed on the next try. If the response looks correct, return only the text string 
"**success**" (no explanation needed) to continue to the next step.  At this point its ok to give the bot the benefit of the doubt to avoid
going in circles.  By the way the current system time is {datetime.now()}. 

Process Config: {self.process_config[thread_id][process_id]}

Full Process Instructions: {process['PROCESS_INSTRUCTIONS']}

Process History so far this run: {self.process_history[thread_id][process_id]}

Your previous guidance: {self.last_fail[thread_id][process_id]}

Bot's latest response: {previous_response}
"""
                else:
                    check_response = f"""
Check the previous question that the bot was asked in the process history below and the response the bot gave after trying to perform the task.  Review the response and 
determine if the bot's response was correct and makes sense given the instructions it was given.  You can accept the final results of the
previous step without asking to see the sql queries and results that led to the final conclusion.  You don't need to validate things like names or other
text values unless they seem wildly incorrect. If you are very seriously concerned that the step may not 
have been correctly perfomed, return a request to re-run the step of the process again by returning the text "**fail**" followed by a 
DETAILED EXPLAINATION as to why it did not pass and what your concern is, and any suggestions you have on how to succeed on the next try.  
If the response seems like it is likely correct, return only the text string "**success**" (no explanation needed) to continue to the next step.  If the process is complete,
tell the process to stop running.  Remember, proceed under your own direction and do not ask the user for permission to proceed.

Process Config: 
{self.process_config[thread_id][process_id]}

Full process Instructions: 
{process['PROCESS_INSTRUCTIONS']}

Process History so far this run: 
{self.process_history[thread_id][process_id]}

Current system time: 
{datetime.now()}

Bot's most recent response: 
{previous_response}
"""

            print(f"\nSENT TO 2nd LLM:\n{check_response}\n")

            result = self.chat_completion(check_response, self.db_adapter, bot_id = bot_id, bot_name = '', thread_id=thread_id, process_id=process_id, process_name = process_name)

            with self.lock:
                self.process_history[thread_id][process_id] += "\nBots response: " + previous_response

            if not isinstance(result, str):
                self.set_process_cache(bot_id, thread_id, process_id)
                print(f'Process cached with bot_id: {bot_id}, thread_id: {thread_id}, process_id: {process_id}')

                return {
                    "success": False,
                    "message": "Process failed: The checking function didn't return a string."
                }
            
           # print("RUN 2nd LLM...")

            print(f"\nRESULT FROM 2nd LLM: {result}\n")

            if "**fail**" in result.lower():
                with self.lock:
                    self.last_fail[thread_id][process_id] = result
                    self.fail_count[thread_id][process_id] += 1
                    self.process_history[thread_id][process_id] += "\nSupervisors concern: " + result
                if self.fail_count[thread_id][process_id] <= 5:
                    print(f"\nStep {self.counter[thread_id][process_id]} failed. Fail count={self.fail_count[thread_id][process_id]} > 5 failures on this step, stopping process...\n")
                    self.set_process_cache(bot_id, thread_id, process_id)
                    print(f'Process cached with bot_id: {bot_id}, thread_id: {thread_id}, process_id: {process_id}')

                    return_dict = {
                        "success": False,
                        "feedback_from_supervisor": result,
                        "current system time": {datetime.now()},
                        "recovery_step": f"Review the message above and submit a clarification, and/or try this Step {self.counter[thread_id][process_id]} again:\n{self.instructions[thread_id][process_id]}"
                    }
                    if verbose:
                        return_dict["additional_request"] = "Please also explain and summarize this feedback from the supervisor bot to the user so they know whats going on, and how you plan to rectify it."
                    else:
                        return_dict["shhh"] = "Remember you are running in slient, non-verbose mode. Limit your output as much as possible."

                    return return_dict

                else:
                    print(f"\nStep {self.counter[thread_id][process_id]} failed. Fail count={self.fail_count[thread_id][process_id]} Trying again...\n")

                    with self.lock:
                        self.done[thread_id][process_id] = True
                    self.clear_process_cache(bot_id, thread_id, process_id)
                    try:
                        del self.counter[thread_id][process_id]
                    except:
                        pass
                    print(f'Process cache cleared for bot_id: {bot_id}, thread_id: {thread_id}, process_id: {process_id}')

                    return {"success": "False", "message": f'The process {process_name} has failed due to > 5 repeated step completion failures.  Do not start this process again without user approval.'}


            with self.lock:
                self.last_fail[thread_id][process_id] = None
                self.fail_count[thread_id][process_id] = 0
                print(f"\nThis step passed.  Moving to next step\n")
                self.counter[thread_id][process_id] += 1
                
            extract_instructions = f"""
Extract the text for the next step from the process instructions and return it, using the section marked 'Process History' to see where you are in the process. 
Remember, the process instructions are a set of individual steps that need to be run in order.  
Return the text of the next step only, do not make any other comments or statements.
If the process is complete, respond "**done**" with no other text.

Process History: {self.process_history[thread_id][process_id]}

Current system time: {datetime.now()}

Process Configuration: 
{self.process_config[thread_id][process_id]}

Process Instructions: 

{process['PROCESS_INSTRUCTIONS']}
                """

            print(f"\nEXTRACT NEXT STEP:\n{extract_instructions}\n")

       #     print("RUN 2nd LLM...")
            next_step = self.chat_completion(extract_instructions, self.db_adapter, bot_id = bot_id, bot_name = '', thread_id=thread_id, process_id=process_id, process_name=process_name)

            print(f"\nRESULT (NEXT_STEP_): {next_step}\n")

            if next_step == '**done**' or next_step == '***done***' or next_step.strip().endswith('**done**'):
                with self.lock:
                    self.last_fail[thread_id][process_id] = None
                    self.fail_count[thread_id][process_id] = None
                    self.done[thread_id][process_id] = True
                # Clear the process cache when the process is complete
                self.clear_process_cache(bot_id, thread_id, process_id)
                print(f'Process cache cleared for bot_id: {bot_id}, thread_id: {thread_id}, process_id: {process_id}')

                return {
                    "success": True,
                    "process_complete": True,
                    "message": f"Congratulations, the process {process_name} is complete.",
                    "reminder": f"If you were running this as a subprocess inside another process, be sure to continue the parent process."
                }

            print(f"\n{next_step}\n")

            with self.lock:
                self.instructions[thread_id][process_id] = f"""
Hey **@{process['BOT_ID']}**, here is the next step of the process.

{next_step}

Execute these instructions now and then pass your response to the run_process tool as a parameter called previous_response and an action of GET_NEXT_STEP. 
If you are told to run another process in these instructions, actually run it using _run_process before calling GET_NEXT_STEP for this process, do not just pretend to run it.
If need to terminate the process early, call with action of END_PROCESS. 
                    """
                if verbose:
                    self.instructions[thread_id][process_id] += """
Tell the user what you are going to do in this step and showing interium outputs, etc. while you are running this and further steps to keep the user informed what is going on.
For example if you are going to call a tool to perform this step, first tell the user what you're going to do.
                    """
                else:
                    self.instructions[thread_id][process_id] += """
This process is being run in low verbosity mode, so do not generate a lot of text while running this process. Just do whats required, call the right tools, etc.
                            """
                self.instructions[thread_id][process_id] += f"""
Don't stop to verify anything with the user unless specifically told to.
By the way the current system time id: {datetime.now()}.
In your response back to run_process, provide a detailed description of what you did, what result you achieved, and why you believe this to have successfully completed the step.
                """

            print(f"\nEXTRACTED NEXT STEP: \n{self.instructions[thread_id][process_id]}\n")

            with self.lock:
                self.process_history[thread_id][process_id] += "\nNext step: " + next_step

            self.set_process_cache(bot_id, thread_id, process_id)
            print(f'Process cached with bot_id: {bot_id}, thread_id: {thread_id}, process_id: {process_id}')

            return {
                "success": True,
                "message": self.instructions[thread_id][process_id],
            }

        elif action == "END_PROCESS":
            print(f"Received END_PROCESS action for process {process_name}.")
            with self.lock:
                self.done[thread_id][process_id] = True
            self.clear_process_cache(bot_id, thread_id, process_id)
            print(f'Process cache cleared for bot_id: {bot_id}, thread_id: {thread_id}, process_id: {process_id}')
            return {"success": True, "message": f'The process {process_name} has finished.  You may now end the process.'}
        else:
            print("No action specified.")
            return {"success": False, "message": "No action specified."}
        
    # def delete_process_thread(self, thread_id):
    #     if thread_id in self.counter:
    #         del self.counter[thread_id]
    #     if thread_id in self.process:
    #         del self.process[thread_id]
    #     if thread_id in self.last_fail:
    #         del self.last_fail[thread_id]
    #     if thread_id in self.instructions:
    #         del self.instructions[thread_id]
    #     if thread_id in self.process_history:
    #         del self.process_history[thread_id]
    #     if thread_id in self.done:
    #         del self.done[thread_id]
    #     return {"success": True, "message": "Process thread deleted."}
 # ========================================================================================================

    def get_processes_list(self, bot_id="all"):
        cursor = db_adapter.client.cursor()
        try:
            if bot_id == "all":
                list_query = f"SELECT process_id, bot_id, process_name FROM {db_adapter.schema}.PROCESSES" if db_adapter.schema else f"SELECT process_id, bot_id, process_name FROM PROCESSES"
                cursor.execute(list_query)
            else:
                list_query = f"SELECT process_id, bot_id, process_name FROM {db_adapter.schema}.PROCESSES WHERE upper(bot_id) = upper(%s)" if db_adapter.schema else f"SELECT process_id, bot_id, process_name FROM PROCESSES WHERE upper(bot_id) = upper(%s)"
                cursor.execute(list_query, (bot_id,))
            processs = cursor.fetchall()
            process_list = []
            for process in processs:
                process_dict = {
                    "process_id": process[0],
                    "bot_id": process[1],
                    "process_name": process[2],
                }
                process_list.append(process_dict)
            return {"Success": True, "processes": process_list}
        except Exception as e:
            return {
                "Success": False,
                "Error": f"Failed to list processs for bot {bot_id}: {e}",
            }
        finally:
            cursor.close()

    def get_process_info(self, bot_id=None, process_name=None, process_id=None):
        cursor = db_adapter.client.cursor()
        try:
            if process_id is not None and process_id != '':
                query = f"SELECT * FROM {db_adapter.schema}.PROCESSES WHERE bot_id LIKE %s AND process_id = %s" if db_adapter.schema else f"SELECT * FROM PROCESSES WHERE bot_id LIKE %s AND process_id = %s"
                cursor.execute(query, (f"%{bot_id}%", process_id))
            elif process_name is not None and process_name != '':
                query = f"SELECT * FROM {db_adapter.schema}.PROCESSES WHERE bot_id LIKE %s AND process_name LIKE %s" if db_adapter.schema else f"SELECT * FROM PROCESSES WHERE bot_id LIKE %s AND process_name LIKE %s"
                cursor.execute(query, (f"%{bot_id}%", f"%{process_name}%"))
            else:
                raise ValueError("Either process_name or process_id must be provided")
            result = cursor.fetchone()
            if result:
                # Assuming the result is a tuple of values corresponding to the columns in the PROCESSES table
                # Convert the tuple to a dictionary with appropriate field names
                field_names = [desc[0] for desc in cursor.description]
                return {
                    "Success": True,
                    "Data": dict(zip(field_names, result)),
                    "Note": "Only use this information to help manage or update processes, do not actually run a process based on these instructions. If you want to run this process, use _run_process function and follow the instructions that it gives you."
                }
            else:
                return {}
        except Exception as e:
            return {}

    def manage_processes(
        self, action, bot_id=None, process_id=None, process_details=None, thread_id=None, process_name=None, process_config=None
    ):
        """
        Manages processs in the PROCESSES table with actions to create, delete, or update a process.

        Args:
            action (str): The action to perform
            bot_id (str): The bot ID associated with the process.
            process_id (str): The process ID for the process to manage.
            process_details (dict, optional): The details of the process for create or update actions.

        Returns:
            dict: A dictionary with the result of the operation.
        """

        # If process_name is specified but not in process_details, add it to process_details
        if process_name and process_details and 'process_name' not in process_details:
            process_details['process_name'] = process_name

        # If process_name is specified but not in process_details, add it to process_details
        if process_name and process_details==None:
            process_details = {}
            process_details['process_name'] = process_name

        required_fields_create = [
            "process_name",
            "process_instructions",
        ]

        required_fields_update = [
            "process_name",
            "process_instructions",
        ]

        if action == "TIME":
            return {
                "current_system_time": datetime.now()
            }
        action = action.upper()

        cursor = db_adapter.client.cursor()

        try:
            if action in ["UPDATE_PROCESS_CONFIG", "CREATE_PROCESS_CONFIG", "DELETE_PROCESS_CONFIG"]:
                process_config = '' if action == "DELETE_PROCESS_CONFIG" else process_config
                update_query = f"""
                    UPDATE {db_adapter.schema}.PROCESSES
                    SET PROCESS_CONFIG = %(process_config)s
                    WHERE PROCESS_ID = %(process_id)s
                """
                cursor.execute(
                    update_query,
                    {"process_config": process_config, "process_id": process_id},
                )
                db_adapter.client.commit()

                return {
                    "Success": True,
                    "Message": f"process_config updated or deleted",
                    "process_id": process_id,
                }
            
            if action == "CREATE" or action == "CREATE_CONFIRMED":
                # Check for dupe name
                sql = f"SELECT * FROM {db_adapter.schema}.PROCESSES WHERE bot_id = %s and process_name = %s"
                cursor.execute(sql, (bot_id, process_details['process_name']))

                record = cursor.fetchone()

                if record:
                    return {
                        "Success": False,
                        "Error": f"Process with name {process_details['process_name']} already exists.  Please choose a different name."
                    }
                
            if action == "UPDATE" or action == 'UPDATE_CONFIRMED':
                # Check for dupe name
                sql = f"SELECT * FROM {db_adapter.schema}.PROCESSES WHERE bot_id = %s and process_name = %s"
                cursor.execute(sql, (bot_id, process_details['process_name']))

                record = cursor.fetchone()

                if record and '_golden' in record[1]:
                    return {
                        "Success": False,
                        "Error": f"Process with name {process_details['process_name']} is a system process and can not be updated.  Suggest making a copy with a new name."
                    }

            if action == "CREATE" or action == "UPDATE":
                # Check for dupe name
                # sql = f"SELECT * FROM {db_adapter.schema}.PROCESSES WHERE bot_id = %s and process_name = %s"
                # cursor.execute(sql, (bot_id, process_details['process_name']))

                # record = cursor.fetchone()

                # if record and '_golden' in record['process_id']:
                #     return {
                #         "Success": False,
                #         "Error": f"Process with name {process_details['process_name']}.  Please choose a different name."
                #     }
            
                # Send process_instructions to 2nd LLM to check it and format nicely
                tidy_process_instructions = f"""
                Below is a process that has been submitted by a user.  Please review it to insure it is something
                that will make sense to the run_process tool.  If not, make changes so it is organized into clear
                steps.  Make sure that it is tidy, legible and properly formatted. 
                Do not create multiple options for the instructions, as whatever you return will be used immediately.
                Return the updated and tidy process.  If there is an issue with the process, return an error message.

                The process is as follows:\n {process_details['process_instructions']}
                """

                tidy_process_instructions = "\n".join(
                    line.lstrip() for line in tidy_process_instructions.splitlines()
                )

                process_details['process_instructions'] = self.chat_completion(tidy_process_instructions, self.db_adapter, bot_id = bot_id, bot_name = '', thread_id=thread_id, process_id=process_id, process_name=process_name)

            if action == "CREATE":
                return {
                    "Success": False,
                    "Cleaned up instructions": process_details['process_instructions'],
                    "Confirmation_Needed": "I've run the process instructions through a cleanup step.  Please reconfirm these instructions and all the other process details with the user, then call this function again with the action CREATE_CONFIRMED to actually create the process.",
                    "Next Step": "If you're ready to create this process call this function again with action CREATE_CONFIRMED instead of CREATE"
                #    "Info": f"By the way the current system time is {datetime.now().strftime('%Y-%m-%d %H:%M:%S %Z')}",
                }

            if action == "UPDATE":
                return {
                    "Success": False,
                    "Cleaned up instructions": process_details['process_instructions'],
                    "Confirmation_Needed": "I've run the process instructions through a cleanup step.  Please reconfirm these instructions and all the other process details with the user, then call this function again with the action UPDATE_CONFIRMED to actually update the process.",
                    "Next Step": "If you're ready to update this process call this function again with action UPDATE_CONFIRMED instead of UPDATE"
                #    "Info": f"By the way the current system time is {datetime.now().strftime('%Y-%m-%d %H:%M:%S %Z')}",
                }

        except Exception as e:
            return {"Success": False, "Error": f"Error connecting to LLM: {e}"}

        
        if action == "CREATE_CONFIRMED":
            action = "CREATE"
        if action == "UPDATE_CONFIRMED":
            action = "UPDATE"

        if action == "DELETE":
            return {
                "Success": False,
                "Confirmation_Needed": "Please reconfirm that you are deleting the correct process_ID, and double check with the user they want to delete this process, then call this function again with the action DELETE_CONFIRMED to actually delete the process.  Call with LIST to double-check the process_id if you aren't sure that its right.",
            }

        if action == "DELETE_CONFIRMED":
            action = "DELETE"

        if action not in ["CREATE", "DELETE", "UPDATE", "LIST", "SHOW"]:
            return {"Success": False, "Error": "Invalid action specified. Should be CREATE, DELETE, UPDATE, LIST, or SHOW."}

        if action == "LIST":
            print("Running get processes list")
            return self.get_processes_list(bot_id if bot_id is not None else "all")

        if action == "SHOW":
            print("Running show process info")
            if bot_id is None:
                return {"Success": False, "Error": "bot_id is required for SHOW action"}
            if process_id is None:
                if process_details is None or ('process_name' not in process_details and 'process_id' not in process_details):
                    return {"Success": False, "Error": "Either process_name or process_id is required in process_details for SHOW action"}
            
            if process_id is not None or 'process_id' in process_details:
                if process_id is None:
                    process_id = process_details['process_id']
                return self.get_process_info(bot_id=bot_id, process_id=process_id)
            else:
                process_name = process_details['process_name']
                return self.get_process_info(bot_id=bot_id, process_name=process_name)

        process_id_created = False
        if process_id is None:
            if action == "CREATE":
                process_id = f"{bot_id}_{''.join(random.choices(string.ascii_letters + string.digits, k=6))}"
                process_id_created = True
            else:
                return {"Success": False, "Error": f"Missing process_id field"}

        if action in ["CREATE", "UPDATE"] and not process_details:
            return {
                "Success": False,
                "Error": "Process details must be provided for CREATE or UPDATE action.",
            }

        if action in ["CREATE"] and any(
            field not in process_details for field in required_fields_create
        ):
            missing_fields = [
                field
                for field in required_fields_create
                if field not in process_details
            ]
            return {
                "Success": False,
                "Error": f"Missing required process details: {', '.join(missing_fields)}",
            }

        if action in ["UPDATE"] and any(
            field not in process_details for field in required_fields_update
        ):
            missing_fields = [
                field
                for field in required_fields_update
                if field not in process_details
            ]
            return {
                "Success": False,
                "Error": f"Missing required process details: {', '.join(missing_fields)}",
            }

        if bot_id is None:
            return {
                "Success": False,
                "Error": "The 'bot_id' field is required."
            }
    
        try:
            if action == "CREATE":
                insert_query = f"""
                    INSERT INTO {db_adapter.schema}.PROCESSES (
                        timestamp, process_id, bot_id, process_name, process_instructions
                    ) VALUES (
                        current_timestamp(), %(process_id)s, %(bot_id)s, %(process_name)s, %(process_instructions)s
                    )
                """ if db_adapter.schema else f"""
                    INSERT INTO PROCESSES (
                        timestamp, process_id, bot_id, process_name, process_instructions
                    ) VALUES (
                        current_timestamp(), %(process_id)s, %(bot_id)s, %(process_name)s, %(process_instructions)s
                    )
                """

                # Generate 6 random alphanumeric characters
                if process_id_created == False:
                    random_suffix = "".join(
                    random.choices(string.ascii_letters + string.digits, k=6)
                     )
                    process_id_with_suffix = process_id + "_" + random_suffix
                else:
                    process_id_with_suffix = process_id
                cursor.execute(
                    insert_query,
                    {
                        **process_details,
                        "process_id": process_id_with_suffix,
                        "bot_id": bot_id,
                    },
                )
                # Get process_name from process_details if available, otherwise set to "Unknown"
                process_name = process_details.get('process_name', "Unknown")
                db_adapter.client.commit()
                return {
                    "Success": True,
                    "Message": f"process successfully created.",
                    "process_id": process_id_with_suffix,
                    "process_name": process_name,
                    "Suggestion": "Now that the process is created, remind the user of the process_id and process_name, and offer to test it using run_process, and if there are any issues you can later on UPDATE the process using manage_processes to clarify anything needed.  OFFER to test it, but don't just test it unless the user agrees.",
                    "Reminder": "If you are asked to test the process, use _run_process function to each step, don't skip ahead since you already know what the steps are, pretend you don't know what the process is and let run_process give you one step at a time!",
                }

            elif action == "DELETE":
                delete_query = f"""
                    DELETE FROM {db_adapter.schema}.PROCESSES
                    WHERE process_id = %s
                """ if db_adapter.schema else f"""
                    DELETE FROM PROCESSES
                    WHERE process_id = %s
                """
                cursor.execute(delete_query, (process_id))
                db_adapter.client.commit()
                return {
                    "Success": True,
                    "Message": f"process deleted",
                    "process_id": process_id,
                }

            elif action == "UPDATE":
                update_query = f"""
                    UPDATE {db_adapter.schema}.PROCESSES
                    SET {', '.join([f"{key} = %({key})s" for key in process_details.keys()])}
                    WHERE process_id = %(process_id)s
                """ if db_adapter.schema else f"""
                    UPDATE PROCESSES
                    SET {', '.join([f"{key} = %({key})s" for key in process_details.keys()])}
                    WHERE process_id = %(process_id)s
                """
                cursor.execute(
                    update_query,
                    {**process_details, "process_id": process_id},
                )
                db_adapter.client.commit()
                return {
                    "Success": True,
                    "Message": f"process successfully updated",
                    "process_id": process_id,
                    "Suggestion": "Now that the process is updated, offer to test it using run_process, and if there are any issues you can later on UPDATE the process again using manage_processes to clarify anything needed.  OFFER to test it, but don't just test it unless the user agrees.",
                    "Reminder": "If you are asked to test the process, use _run_process function to each step, don't skip ahead since you already know what the steps are, pretend you don't know what the process is and let run_process give you one step at a time!",
                }

            return {"Success": True, "Message": f"process update or delete confirmed."}
        except Exception as e:
            return {"Success": False, "Error": str(e)}

        finally:
            cursor.close()

    def insert_process_history(
        self,
        process_id,
        work_done_summary,
        process_status,
        updated_process_learnings,
        report_message="",
        done_flag=False,
        needs_help_flag="N",
        process_clarity_comments="",
    ):
        """
        Inserts a row into the PROCESS_HISTORY table.

        Args:
            process_id (str): The unique identifier for the process.
            work_done_summary (str): A summary of the work done.
            process_status (str): The status of the process.
            updated_process_learnings (str): Any new learnings from the process.
            report_message (str): The message to report about the process.
            done_flag (bool): Flag indicating if the process is done.
            needs_help_flag (bool): Flag indicating if help is needed.
            process_clarity_comments (str): Comments on the clarity of the process.
        """
        insert_query = f"""
            INSERT INTO {db_adapter.schema}.PROCESS_HISTORY (
                process_id, work_done_summary, process_status, updated_process_learnings, 
                report_message, done_flag, needs_help_flag, process_clarity_comments
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s
            )
        """ if db_adapter.schema else f"""
            INSERT INTO PROCESS_HISTORY (
                process_id, work_done_summary, process_status, updated_process_learnings, 
                report_message, done_flag, needs_help_flag, process_clarity_comments
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s
            )
        """
        try:
            cursor = db_adapter.client.cursor()
            cursor.execute(
                insert_query,
                (
                    process_id,
                    work_done_summary,
                    process_status,
                    updated_process_learnings,
                    report_message,
                    done_flag,
                    needs_help_flag,
                    process_clarity_comments,
                ),
            )
            db_adapter.client.commit()
            cursor.close()
            print(
                f"Process history row inserted successfully for process_id: {process_id}"
            )
        except Exception as e:
            print(f"An error occurred while inserting the process history row: {e}")
            if cursor is not None:
                cursor.close()

    # ========================================================================================================
    
if genesis_source == "BigQuery":
    credentials_path = os.getenv(
        "GOOGLE_APPLICATION_CREDENTIALS", default=".secrets/gcp.json"
    )
    with open(credentials_path) as f:
        connection_info = json.load(f)
    # Initialize BigQuery client
    db_adapter = BigQueryConnector(connection_info, "BigQuery")
elif genesis_source == 'Sqlite':
    db_adapter = SqliteConnector(connection_name="Sqlite")
    connection_info = {"Connection_Type": "Sqlite"}
elif genesis_source == 'Snowflake':  # Initialize Snowflake client
    db_adapter = SnowflakeConnector(connection_name="Snowflake")
    connection_info = {"Connection_Type": "Snowflake"}
else:
    raise ValueError('Invalid Source')
    # tool_belt = (ToolBelt(db_adapter, os.getenv("OPENAI_API_KEY")),)

def get_tools(which_tools, db_adapter, slack_adapter_local=None, include_slack=True, tool_belt=None):

    tools = []
    available_functions_load = {}
    function_to_tool_map = {}
    if "autonomous_functions" in which_tools and "autonomous_tools" not in which_tools:
        which_tools = [
            tool if tool != "autonomous_functions" else "autonomous_tools"
            for tool in which_tools
        ]
    which_tools = [tool for tool in which_tools if tool != "autonomous_functions"]
    for tool in which_tools:
        try:
            tool_name = tool.get("tool_name")
        except:
            tool_name = tool

        if False:  # tool_name == 'integration_tools':
            tools.extend(integration_tool_descriptions)
            available_functions_load.update(integration_tools)
            function_to_tool_map[tool_name] = integration_tool_descriptions
        elif include_slack and tool_name == "slack_tools":
            tools.extend(slack_tools_descriptions)
            available_functions_load.update(slack_tools)
            function_to_tool_map[tool_name] = slack_tools_descriptions
        elif tool_name == "harvester_tools":
            tools.extend(harvester_tools_functions)
            available_functions_load.update(harvester_tools_list)
            function_to_tool_map[tool_name] = harvester_tools_functions
        elif tool_name == "make_baby_bot":
            tools.extend(MAKE_BABY_BOT_DESCRIPTIONS)
            available_functions_load.update(make_baby_bot_tools)
            function_to_tool_map[tool_name] = MAKE_BABY_BOT_DESCRIPTIONS
        elif tool_name == "bot_dispatch":
            tools.extend(BOT_DISPATCH_DESCRIPTIONS)
            available_functions_load.update(bot_dispatch_tools)
            function_to_tool_map[tool_name] = BOT_DISPATCH_DESCRIPTIONS
        elif tool_name == "database_tools":
            tools.extend(database_tool_functions)
            available_functions_load.update(database_tools)
            run_query_f = bind_run_query([connection_info])
            search_metadata_f = bind_search_metadata("./kb_vector")
            semantic_copilot_f = bind_semantic_copilot([connection_info])
            function_to_tool_map[tool_name] = database_tool_functions
        elif tool_name == "image_tools":
            tools.extend(image_functions)
            available_functions_load.update(image_tools)
            function_to_tool_map[tool_name] = image_functions
        elif tool_name == "snowflake_semantic_tools":
            print('Note: Semantic Tools are currently disabled pending refactoring or removal.')
            tools.extend(snowflake_semantic_functions)
            available_functions_load.update(snowflake_semantic_tools)
            function_to_tool_map[tool_name] = snowflake_semantic_functions
        elif tool_name == "snowflake_stage_tools":
            tools.extend(snowflake_stage_functions)
            available_functions_load.update(snowflake_stage_tools)
            function_to_tool_map[tool_name] = snowflake_stage_functions
        elif tool_name == "autonomous_tools" or tool_name == "autonomous_functions":
            tools.extend(autonomous_functions)
            available_functions_load.update(autonomous_tools)
            function_to_tool_map[tool_name] = autonomous_functions
        elif tool_name == "process_runner_tools":
            tools.extend(process_runner_functions)
            available_functions_load.update(process_runner_tools)
            function_to_tool_map[tool_name] = process_runner_functions
        elif tool_name == "process_manager_tools":
            tools.extend(process_manager_functions)
            available_functions_load.update(process_manager_tools)
            function_to_tool_map[tool_name] = process_manager_functions
        elif tool_name == "process_scheduler_tools":
            tools.extend(process_scheduler_functions)
            available_functions_load.update(process_scheduler_tools)
            function_to_tool_map[tool_name] = process_scheduler_functions
        elif tool_name == "webpage_downloader":
            tools.extend(webpage_downloader_functions)
            available_functions_load.update(webpage_downloader_tools)
            function_to_tool_map[tool_name] = webpage_downloader_functions
        else:
            try:
                module_path = "generated_modules." + tool_name
                desc_func = "TOOL_FUNCTION_DESCRIPTION_" + tool_name.upper()
                functs_func = tool_name.lower() + "_action_function_mapping"
                module = __import__(module_path, fromlist=[desc_func, functs_func])
                # here's how to get the function for generated things even new ones...
                func = [getattr(module, desc_func)]
                tools.extend(func)
                function_to_tool_map[tool_name] = func
                func_af = getattr(module, functs_func)
                available_functions_load.update(func_af)
            except:
                logger.warn(f"Functions for tool '{tool_name}' could not be found.")

    available_functions = {}
    for name, full_func_name in available_functions_load.items():
        if callable(full_func_name):
            available_functions[name] = full_func_name
        else:
            module_path, func_name = full_func_name.rsplit(".", 1)
            if module_path in locals():
                module = locals()[module_path]
                try:
                    func = getattr(module, func_name)
                except:
                    func = module
                # print("existing local: ",func)
            elif module_path in globals():
                module = globals()[module_path]
                try:
                    func = getattr(module, func_name)
                except:
                    func = module
                # print("existing global: ",func)
            else:
                module = __import__(module_path, fromlist=[func_name])
                func = getattr(module, func_name)
                # print("imported: ",func)
            available_functions[name] = func
    # Insert additional code here if needed

    return tools, available_functions, function_to_tool_map
    # print("imported: ",func)


class BotOsDispatchInputAdapter(BotOsInputAdapter):
    def __init__(self, bot_id) -> None:
        bot_config = get_bot_details(bot_id=bot_id)
        self.session = make_session_for_dispatch(bot_config)
        self.tasks = {}

    # allows for polling from source
    def add_event(self, event):
        pass

    # allows for polling from source
    def get_input(self, thread_map=None, active=None, processing=None, done_map=None):
        pass

    # allows response to be sent back with optional reply
    def handle_response(
        self,
        session_id: str,
        message: BotOsOutputMessage,
        in_thread=None,
        in_uuid=None,
        task_meta=None,
    ):
        if message.status == "completed":
            self.tasks[message.thread_id]["result"] = message.output

    def dispatch_task(self, task):
        # thread_id = self.session.add_task(task, self)
        thread_id = self.session.create_thread(self)
        self.tasks[thread_id] = {"task": task, "result": None}
        self.session.add_message(BotOsInputMessage(thread_id=thread_id, msg=task))

    def check_tasks(self):
        self.session.execute()
        if all(task["result"] is not None for task in self.tasks.values()):
            return [task["result"] for task in self.tasks.values()]
        else:
            return False


def dispatch_to_bots(task_template, args_array, dispatch_bot_id=None):
    """
    Dispatches a task to multiple bots, each instantiated by creating a new thread with a specific task.
    The task is created by filling in the task template with arguments from the args_array using Jinja templating.

    Args:
        task_template (str): A natural language task template using Jinja templating.
        args_array (list of dict): An array of dictionaries to plug into the task template for each bot.

    Returns:
        list: An array of responses.
    """

    if len(args_array) < 2:
        return "Error: args_array size must be at least 2."

    template = Template(task_template)
    adapter = BotOsDispatchInputAdapter(bot_id=dispatch_bot_id)

    for s_args in args_array:
        # Fill in the task template with the current arguments
        args = json.loads(s_args)
        task = template.render(**args)
        adapter.dispatch_task(task)

    while True:
        responses = adapter.check_tasks()
        if responses:
            print(f"dispatch_to_bots - {responses}")
            return responses
        time.sleep(1)


BOT_DISPATCH_DESCRIPTIONS = [
    {
        "type": "function",
        "function": {
            "name": "dispatch_to_bots",
            "description": 'Specify an arry of templated natual language tasks you want to execute in parallel to a set of bots like you. for example, "Who is the president of {{ country_name }}". Never use this tool for arrays with < 2 items.',
            "parameters": {
                "type": "object",
                "properties": {
                    "task_template": {
                        "type": "string",
                        "description": "Jinja template for the tasks you want to farm out to other bots",
                    },
                    "args_array": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": 'Arguments you want to fill in for each jinja template variable of the form [{"country_name": "france"}, {"country_name": "spain"}]',
                    },
                },
                "required": ["task_template", "args_array"],
                "bot_id": {
                    "type": "string",
                    "description": "The unique identifier for an existing bot you are aware of to dispatch the tasks to. Should be the bot_name dash a 6 letter alphanumeric random code, for example mybot-w73hxg. Pass None to dispatch to yourself.",
                },
            },
        },
    }
]
# "bot_id": {
#     "type": "string",
#     "description": "The unique identifier for an existing bot you are aware of to dispatch the tasks to. Should be the bot_name dash a 6 letter alphanumeric random code, for example mybot-w73hxg."
# }


bot_dispatch_tools = {"dispatch_to_bots": "core.bot_os_tools.dispatch_to_bots"}


def make_session_for_dispatch(bot_config):
    input_adapters = []
    bot_tools = json.loads(bot_config["available_tools"])

    genesis_source = os.getenv("GENESIS_SOURCE", default="BigQuery")

    if genesis_source == "BigQuery":
        credentials_path = os.getenv(
            "GOOGLE_APPLICATION_CREDENTIALS", default=".secrets/gcp.json"
        )
        with open(credentials_path) as f:
            connection_info = json.load(f)
        # Initialize BigQuery client
        db_adapter = BigQueryConnector(connection_info, "BigQuery")
    else:  # Initialize BigQuery client
        db_adapter = SnowflakeConnector(connection_name="Snowflake")
        connection_info = {"Connection_Type": "Snowflake"}

    print("---> CONNECTED TO DATABASE: ", genesis_source)
    tools, available_functions, function_to_tool_map = get_tools(
        bot_tools, db_adapter, include_slack=False
    )  # FixMe remove slack adapter if

    instructions = (
        bot_config["bot_instructions"] + "\n" + BASE_BOT_INSTRUCTIONS_ADDENDUM
    )
    print(instructions, f'{bot_config["bot_name"]}, id: {bot_config["bot_id"]}')

    # TESTING UDF ADAPTER W/EVE and ELSA
    # add a map here to track botid to adapter mapping

    bot_id = bot_config["bot_id"]
    if os.getenv("BOT_DO_PLANNING_REFLECTION"):
        pre_validation = BASE_BOT_PRE_VALIDATION_INSTRUCTIONS
        post_validation = BASE_BOT_VALIDATION_INSTRUCTIONS
    else:
        pre_validation = ""
        post_validation = None
    if os.getenv("BOT_BE_PROACTIVE", "False").lower() == "true":
        proactive_instructions = BASE_BOT_PROACTIVE_INSTRUCTIONS
    else:
        proactive_instructions = ""
    session = BotOsSession(
        bot_config["bot_id"],
        instructions=instructions + proactive_instructions + pre_validation,
        validation_instructions=post_validation,
        input_adapters=input_adapters,
        knowledgebase_implementation=BotOsKnowledgeAnnoy_Metadata(
            f"./kb_{bot_config['bot_id']}"
        ),
        file_corpus=(
            URLListFileCorpus(json.loads(bot_config["files"]))
            if bot_config["files"]
            else None
        ),
        log_db_connector=db_adapter,  # Ensure connection_info is defined or fetched appropriately
        tools=tools,
        available_functions=available_functions,
        all_tools=tools,
        all_functions=available_functions,
        all_function_to_tool_map=function_to_tool_map,
        bot_id=bot_config["bot_id"],
    )
    # if os.getenv("BOT_BE_PROACTIVE").lower() == "true" and slack_adapter_local:
    #      session.add_task("Check in with Michael Gold to see if he has any tasks for you to work on.",
    ##                       thread_id=session.create_thread(slack_adapter_local))
    #                       input_adapter=slack_adapter_local))

    return session  # , api_app_id, udf_adapter_local, slack_adapter_local
