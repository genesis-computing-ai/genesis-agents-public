import json
import os, uuid
from typing import TypedDict
from genesis_bots.core.bot_os_assistant_base import BotOsAssistantInterface, execute_function, execute_function_blocking
import requests
import anthropic


from genesis_bots.core.logging_config import logger

from genesis_bots.core.bot_os_input import BotOsInputMessage, BotOsOutputMessage


def _get_function_details(run):
      function_details = []
      for tool_call in run.required_action.submit_tool_outputs.tool_calls:
         function_details.append(
            (tool_call.function.name, tool_call.function.arguments, tool_call.id)
         )
      return function_details

class BotOsAssistantAnthropic(BotOsAssistantInterface):
   def __init__(self, name:str, instructions:str,
                tools:list[dict], available_functions={}, files=[], update_existing=False) -> None:
      logger.debug("BotOsAssistantAnthropic:__init__")
      api_key = os.environ["ANTHROPIC_API_KEY"]
      self.model_name = os.getenv("ANTHROPIC_MODEL_NAME", default="claude-3-opus-20240229")
      self.next_thread_id = 0
      self.thread_messages_map = {}
      self.thread_run_map = {}
      self.available_functions = available_functions
      self.instructions = instructions
      self.recent_response = None
      self.recent_thread = None

      api_key = os.environ["ANTHROPIC_API_KEY"]

      self.client = anthropic.Anthropic(
        # defaults to os.environ.get("ANTHROPIC_API_KEY")
        api_key=api_key,
      )

      if False:

        self.client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

        self.thread_run_map = {}
        self.file_storage = {}
        self.available_functions = available_functions
        self.running_tools = {}
        self.tool_completion_status = {}
        tools += [{"type": "code_interpreter"}, {"type": "retrieval"}]

        my_assistants = self.client.beta.assistants.list(
            order="desc",
            limit=20,
        )
        my_assistants = [a for a in my_assistants if a.name == name]
        if len(my_assistants) == 0:
            self.assistant = self.client.beta.assistants.create(
                name=name,
                instructions=instructions,
                tools=tools, # type: ignore
                model=model_name,
                file_ids=self._upload_files(files) #FixMe: what if the file contents change?
            )
        else:
            self.assistant = my_assistants[0]
            if update_existing and (
                self.assistant.instructions != instructions or \
                self.assistant.tools        != tools or \
                self.assistant.model        != model_name):
                self.client.beta.assistants.update(self.assistant.id,
                                            instructions=instructions,
                                            tools=tools, # type: ignore
                                            model=model_name)

        logger.debug(f"BotOsAssistantOpenAI:__init__: assistant.id={self.assistant.id}")

   def create_thread(self) -> str:
      thread = "mistral-thread-"+str(self.next_thread_id)
      self.next_thread_id += 1
      self.thread_messages_map[thread] = []
      return thread

   def download_file(self, url):
    # Send a GET request to the URL
    # this is slack specific, should probably not be in the openai instance of this class
    token=os.getenv('SLACK_APP_TOKEN',default=None)
    subdirectory = str(uuid.uuid4())
    subdirectory_path = os.path.join("./tmp_uploaded_files", subdirectory)

    # Create the subdirectory
    if not os.path.exists(subdirectory_path):
        os.makedirs(subdirectory_path)

    # Extract the filename from the URL
    filename = url.split('/')[-1]
    file_path = os.path.join(subdirectory_path, filename)

    with requests.get(url, headers={'Authorization': 'Bearer %s' % token}, stream=True) as r:
        # Raise an exception for bad responses
        r.raise_for_status()
        # Open a local file with write-binary mode
        with open(file_path, 'wb') as f:
            # Write the content to the local file
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    return file_path


   def _upload_files(self, files):
      file_ids = []
      token=os.getenv('SLACK_APP_TOKEN',default=None)
      for f in files:
         # add handler to download from URL and save to temp file for upload then cleanup
         logger.info("loading files")
         local_filename = self.download_file(f["url_private"])
         fo = open(local_filename,"rb")
         file = self.client.files.create(file=fo, purpose="assistants")
         file_ids.append(file.id)
         self.file_storage[file.id] = fo
      logger.debug(f"BotOsAssistantMistral:_upload_files - uploaded {len(file_ids)} files")
      return file_ids

   def add_message(self, input_message:BotOsInputMessage):#thread_id:str, message:str, files):
      logger.debug("BotOsAssistantMistral:add_message")
      thread_id = input_message.thread_id
      if thread_id is None:
         raise(Exception("thread_id is None"))
      self.thread_messages_map[thread_id].append({ "role": "user", "content": input_message.msg })

      chat_response = self.client.messages.create(
         model=self.model_name,
         system=self.instructions,
         max_tokens=1024,
         messages=self.thread_messages_map[thread_id]
         )

      logger.info("Anthropic: ",chat_response.content)
      self.thread_messages_map[thread_id].append({ "role": "assistant", "content": chat_response.content[0].text })

      self.recent_thread = {"thread_id": thread_id, "metadata": input_message.metadata,
                            "response":  chat_response.content[0].text}

   def check_runs(self, event_callback):
      logger.debug("BotOsAssistantAnthropic:check_runs")

      if self.recent_thread is None:
         return

      function_call = None
      try:
         clean_output = self.recent_thread["response"].replace('\\_','_')
         clean_output = clean_output.replace('```','')
         clean_output = clean_output.replace('json\\n{','{')
         function_call = json.loads(clean_output)
         #has_tool_name = "tool_name" in function_call
      except:
         pass

      if function_call is not None:
         result = execute_function_blocking(function_call["tool_name"], function_call["parameters"], self.available_functions)
         logger.info(result)
         result_string = "```Tool Call: "+function_call["tool_name"]+" Parameters: "+str(function_call["parameters"])+" Response payload size: "+str(len(str(result)))+"```"
         logger.info(result_string)
         # ideally post this to slack as well to show tool call details

         # now add tool result as a new input
         new_event = BotOsInputMessage(thread_id=self.recent_thread["thread_id"], msg=str(result),
                     metadata=self.recent_thread["metadata"])
         self.add_message(new_event)
      else:
         o = BotOsOutputMessage( self.recent_thread["thread_id"],"completed",self.recent_thread["response"],[],
                                 input_metadata=self.recent_thread["metadata"])

         event_callback("Anthropic_has_no_assistant_id", o)
         self.recent_thread = None

def test():
    m = BotOsAssistantAnthropic("Claude 3","Talk like a pirate.",[])
    thread = m.create_thread()

    while True:
        txt = input(">")

        msg = BotOsInputMessage(thread,txt)
        m.add_message(msg)


#test()
