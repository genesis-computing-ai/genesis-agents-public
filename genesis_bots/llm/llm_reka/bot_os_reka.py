import reka
import json
import os, uuid
from genesis_bots.core.bot_os_assistant_base import (
    BotOsAssistantInterface,
    execute_function_blocking,
)
import requests
from genesis_bots.core.logging_config import logger


# You can either set the API key as below, or use the
# environment variable export REKA_API_KEY="your-api-key"
REKA_API_KEY = "a77d6f730fc1d9398bc5961c4f797444aa869e86c1f5cc85ebf918193f2598e2"
reka.API_KEY = REKA_API_KEY


def test_reka():
    response = reka.list_models()
    logger.info(response)
    # ['default', 'reka-edge', 'reka-flash']

    a = input(">")
    api_key = os.environ["REKA_API_KEY"]
    reka.API_KEY = api_key

    response = reka.chat(
        "I'd like you to generate a SQL query to calculate 1+1 on Snowflake.",
        conversation_history=[
            {"type": "human", "text": "My name is Matt."},
            {"type": "model", "text": "Hello Matt! How can I help you today?\n\n"},
        ],
        model_name="reka-flash",
        #  request_output_len=512,
        #  temperature=0.4,
        #  stop_words=["```"],
    )

    logger.info(response)
    logger.info(response["text"])
    logger.info(response["metadata"])


# test_reka()

from genesis_bots.core.bot_os_input import BotOsInputMessage, BotOsOutputMessage

from genesis_bots.core.logging_config import logger


def _get_function_details(run):
    function_details = []
    for tool_call in run.required_action.submit_tool_outputs.tool_calls:
        function_details.append(
            (tool_call.function.name, tool_call.function.arguments, tool_call.id)
        )
    return function_details


class BotOsAssistantReka(BotOsAssistantInterface):
    def __init__(
        self,
        name: str,
        instructions: str,
        tools: list[dict],
        available_functions={},
        files=[],
        update_existing=False,
        log_db_connector=None,
        bot_id="default_bot_id",
        bot_name="default_bot_name",
    ) -> None:
        logger.debug("BotOsAssistantReka:__init__")
        api_key = os.environ["REKA_API_KEY"]
        reka.API_KEY = api_key
        self.model_name = os.getenv(
            "REKA_MODEL_NAME", default="fantastic-lily-20240327"
        )
        self.next_thread_id = 0
        self.thread_messages_map = {}
        self.thread_run_map = {}
        self.available_functions = available_functions
        self.instructions = instructions
        self.recent_thread = None
        if tools is not None:
            self.instructions += "\nIf you think it will be useful, you can call one of these tools by providing your response as shown in the examples below.  When you call I tool I will respond back to you with the tools output in the next message. Only call a tool if you need to, otherwise just respond to the user without calling a tool.\n"
            self.instructions += json.dumps(tools)
            self.instructions += """
\nExample 1:
USER: do we have data about potatoes?
ASSISTANT: {"tool_name":"search_metadata","parameters":{"query": "potatoes"}}

Example 2:
USER: run a query to find 1+1 using the bigquery database
ASSISTANT: {"tool_name":"query_database","parameters":{"query": "select 1+1", "connection": "BigQuery", "max_rows": 1}}

Example 3:
USER: Hi, how are you today?
ASSISTANT: I'm fine thanks, how can I help?

When calling a tool, respond ONLY with the JSON, No other text or explanation, as it interferes with the tool calling process and the user won't see it anyway.

Do you understand? If so and you are ready, respond simply "I'm ready.".
"""

    def create_thread(self) -> str:
        thread = "reka-thread-" + str(self.next_thread_id)
        self.next_thread_id += 1
        self.thread_messages_map[thread] = [
            {"type": "human", "text": self.instructions},
            {"type": "model", "text": "I'm ready."},
        ]
        # response = reka.chat(
        # self.instructions ,
        # conversation_history=[],
        # model_name = self.model_name,
        ##temperature=0.4,
        ##stop_words=["```"]
        # )
        # logger.info(response["text"])
        # logger.info(response["metadata"])
        # self.thread_messages_map[thread].append({"type": "model", "text": response["text"]})
        return thread

    def download_file(self, url):
        # Send a GET request to the URL
        # this is slack specific, should probably not be in the openai instance of this class
        token = os.getenv("SLACK_APP_TOKEN", default=None)
        subdirectory = str(uuid.uuid4())
        subdirectory_path = os.path.join("./tmp_uploaded_files", subdirectory)

        # Create the subdirectory
        if not os.path.exists(subdirectory_path):
            os.makedirs(subdirectory_path)

        # Extract the filename from the URL
        filename = url.split("/")[-1]
        file_path = os.path.join(subdirectory_path, filename)

        with requests.get(
            url, headers={"Authorization": "Bearer %s" % token}, stream=True
        ) as r:
            # Raise an exception for bad responses
            r.raise_for_status()
            # Open a local file with write-binary mode
            with open(file_path, "wb") as f:
                # Write the content to the local file
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        return file_path

    def _upload_files(self, files):
        file_ids = []
        token = os.getenv("SLACK_APP_TOKEN", default=None)
        for f in files:
            # add handler to download from URL and save to temp file for upload then cleanup
            logger.info("loading files")
            local_filename = self.download_file(f["url_private"])
            fo = open(local_filename, "rb")
            file = self.client.files.create(file=fo, purpose="assistants")
            file_ids.append(file.id)
            self.file_storage[file.id] = fo
        logger.debug(
            f"BotOsAssistantReka:_upload_files - uploaded {len(file_ids)} files"
        )
        return file_ids

    def add_message(
        self, input_message: BotOsInputMessage
    ):  # thread_id:str, message:str, files):
        logger.debug("BotOsAssistantReka:add_message")
        thread_id = input_message.thread_id
        if thread_id is None:
            raise (Exception("thread_id is None"))

        response = reka.chat(
            input_message.msg,
            conversation_history=self.thread_messages_map[thread_id],
            model_name=self.model_name,
            # temperature=0.4,
            # stop_words=["```"]
        )

        self.thread_messages_map[thread_id].append(
            {"type": "human", "text": input_message.msg}
        )

        logger.info(response["text"])
        logger.info(response["metadata"])

        self.thread_messages_map[thread_id].append(
            {"type": "model", "text": response["text"]}
        )

        self.recent_thread = {
            "thread_id": thread_id,
            "metadata": input_message.metadata,
            "response": response["text"],
        }

    def check_runs(self, event_callback):

        def extract_first_json(self, text):
            """
            Extracts the first complete JSON string from a larger text string by going through the text character by character.

            :param text: The text string containing JSON data
            :return: The first complete JSON string if found, otherwise None
            """
            import json

            open_brackets = 0
            json_start_index = None
            for i, char in enumerate(text):
                if char == "{":
                    open_brackets += 1
                    if open_brackets == 1:
                        # Mark the start of JSON string
                        json_start_index = i
                elif char == "}":
                    open_brackets -= 1
                    if open_brackets == 0 and json_start_index is not None:
                        # End of JSON string
                        try:
                            json_data = json.loads(text[json_start_index : i + 1])
                            return json_data
                        except json.JSONDecodeError:
                            # If JSON is not valid, reset and continue looking
                            json_start_index = None
                            continue
            return None

        logger.debug("BotOsAssistantReka:check_runs")

        if self.recent_thread is None:
            return

        recent_thread = self.recent_thread
        self.recent_thread = None

        function_call = None
        try:
            if "{" in recent_thread["response"] and "}" in recent_thread["response"]:
                logger.info("checking for function: ", recent_thread["response"])
                clean_output = recent_thread["response"].replace("\\_", "_")
                clean_output = clean_output.replace("```", "")
                clean_output = clean_output.strip()
                clean_output = clean_output.replace("json\n{", "{")
                first_json = extract_first_json(self, text=clean_output)
                function_call = first_json
                # has_tool_name = "tool_name" in function_call
        except:
            pass

        if function_call is not None:
            result = execute_function_blocking(
                function_call["tool_name"],
                function_call["parameters"],
                self.available_functions,
            )

            resultstr = (
                "```Tool Call: "
                + function_call["tool_name"]
                + " Parameters: "
                + str(function_call["parameters"])
                + " Response payload size: "
                + str(len(str(result)))
                + "```"
            )
            logger.info(resultstr)

            # now add tool result as a new input
            new_event = BotOsInputMessage(
                thread_id=recent_thread["thread_id"],
                msg=str(result),
                metadata=recent_thread["metadata"],
            )
            self.add_message(new_event)
        else:
            o = BotOsOutputMessage(
                recent_thread["thread_id"],
                "completed",
                recent_thread["response"],
                [],
                input_metadata=recent_thread["metadata"],
            )

            event_callback("Mistral_has_no_assistant_id", o)
            # self.recent_thread = None


def test():
    m = BotOsAssistantReka("Test", "Talk like a pirate.", [])
    thread = m.create_thread()

    while True:
        txt = input(">")

        msg = BotOsInputMessage(thread, txt)
        m.add_message(msg)
        m.check_runs(lambda a, e: logger.info(e))
