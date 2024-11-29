import os
from typing import Dict
import uuid
from connectors import get_global_db_connector
from core.bot_os_llm import LLMKeyHandler
from core.bot_os_server import BotOsServer
from apscheduler.schedulers.background import BackgroundScheduler
from demo.sessions_creator import create_sessions

from genesis_base import GenesisServer
from streamlit_gui.udf_proxy_bot_os_adapter import UDFBotOsInputAdapter

class GenesisLocalSnowflakeServer(GenesisServer):
    def __init__(self, scope, sub_scope="app1"):
        super().__init__(scope)
        self.bot_id_to_udf_adapter_map: Dict[str, UDFBotOsInputAdapter] = {}
        if f"{scope}.{sub_scope}" != os.getenv("GENESIS_INTERNAL_DB_SCHEMA"):
            raise Exception(f"Scope {scope}.{sub_scope} does not match environment variable GENESIS_INTERNAL_DB_SCHEMA {os.getenv('GENESIS_INTERNAL_DB_SCHEMA')}")

        db_adapter = get_global_db_connector("Snowflake")
        llm_key_handler = LLMKeyHandler(db_adapter=db_adapter)
        _, llm_api_key_struct = llm_key_handler.get_llm_key_from_db()
        if llm_api_key_struct is not None and llm_api_key_struct.llm_key is not None:
            (
                sessions,
                api_app_id_to_session_map,
                self.bot_id_to_udf_adapter_map,
                bot_id_to_slack_adapter_map #SystemVariables.bot_id_to_slack_adapter_map,
            ) = create_sessions(
                db_adapter,
                None, # bot_id_to_udf_adapter_map,
                stream_mode=True,
        )
        scheduler = BackgroundScheduler(
            {
                "apscheduler.job_defaults.max_instances": 100,
                "apscheduler.job_defaults.coalesce": True,
            }
        )
        self.server = BotOsServer(
            None,
            sessions=sessions,
            scheduler=scheduler,
            scheduler_seconds_interval=2,
            slack_active=False, #global_flags.slack_active,
            db_adapter=db_adapter,
            bot_id_to_udf_adapter_map = self.bot_id_to_udf_adapter_map,
            api_app_id_to_session_map = api_app_id_to_session_map,
            bot_id_to_slack_adapter_map = bot_id_to_slack_adapter_map,
        )
        BotOsServer.stream_mode = True
        scheduler.start()

    def add_message(self, bot_id, message, thread_id) -> str: # returns request_id
        if not thread_id:
            thread_id = str(uuid.uuid4())
        request_id = self.bot_id_to_udf_adapter_map[bot_id].submit(message, thread_id, bot_id={})
        return f"Request submitted on thread {thread_id} . To get response use: get_response --bot_id {bot_id} --request_id {request_id}"

    def get_message(self, bot_id, request_id) -> str:
        return self.bot_id_to_udf_adapter_map[bot_id].lookup_udf(request_id)

#server = GenesisLocalSnowflakeServer(scope="GENESIS_TEST.GENESIS_INTERNAL")
