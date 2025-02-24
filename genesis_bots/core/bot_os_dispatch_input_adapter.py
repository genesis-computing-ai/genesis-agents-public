import json, os

from genesis_bots.bot_genesis.make_baby_bot import (
    MAKE_BABY_BOT_DESCRIPTIONS,
    get_bot_details,
    make_baby_bot_tools,
)

from genesis_bots.core.bot_os_input import BotOsInputAdapter, BotOsInputMessage, BotOsOutputMessage

from genesis_bots.core.logging_config import logger

#from genesis_bots.connectors.bigquery_connector import BigQueryConnector
from genesis_bots.connectors.snowflake_connector.snowflake_connector import SnowflakeConnector
from genesis_bots.core.bot_os import BotOsSession
from genesis_bots.core.bot_os_corpus import URLListFileCorpus
from genesis_bots.core.bot_os_defaults import (
    BASE_BOT_INSTRUCTIONS_ADDENDUM,
    BASE_BOT_PRE_VALIDATION_INSTRUCTIONS,
    BASE_BOT_PROACTIVE_INSTRUCTIONS,
    BASE_BOT_VALIDATION_INSTRUCTIONS,
)
from genesis_bots.core.bot_os_memory import BotOsKnowledgeAnnoy_Metadata

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


def make_session_for_dispatch(bot_config):
    from genesis_bots.core.bot_os_tools import get_tools

    input_adapters = []
    bot_tools = json.loads(bot_config["available_tools"])

    # Create a DB connector for this session.
    # TODO - use the utility functions in the connectors module to DRY up.
    genesis_source = os.getenv("GENESIS_SOURCE", default="Snowflake")

    if genesis_source == "BigQuery":
        raise NotImplementedError("BigQueryConnector is not implemented")
    else:  # Initialize BigQuery client
        db_adapter = SnowflakeConnector(connection_name="Snowflake")
        connection_info = {"Connection_Type": "Snowflake"}

    logger.info("---> CONNECTED TO DATABASE: ", genesis_source)
    tools, available_functions, function_to_tool_map = get_tools(which_tools=bot_tools,
                                                                 db_adapter=db_adapter,
                                                                 include_slack=False)  # FixMe remove include_slack
    instructions = (
        bot_config["bot_instructions"] + "\n" + BASE_BOT_INSTRUCTIONS_ADDENDUM
    )
    logger.info(instructions, f'{bot_config["bot_name"]}, id: {bot_config["bot_id"]}')

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
