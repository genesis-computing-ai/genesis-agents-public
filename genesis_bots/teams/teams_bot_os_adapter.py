from flask import Flask, request, jsonify
import uuid
import os
from core.bot_os_input import BotOsInputAdapter, BotOsInputMessage, BotOsOutputMessage
from collections import deque
import threading
from botbuilder.core import ActivityHandler, MessageFactory, TurnContext
from teams.bots.bot import EchoBot
from core.bot_os_artifacts import ARTIFACT_ID_REGEX, get_artifacts_store
from connectors import get_global_db_connector
import functools
import json
import sys
import traceback
from datetime import datetime
from http import HTTPStatus
from botbuilder.core import TurnContext
from botbuilder.core.integration import aiohttp_error_middleware
from botbuilder.integration.aiohttp import CloudAdapter, ConfigurationBotFrameworkAuthentication
from botbuilder.schema import Activity, ActivityTypes
from teams.config import DefaultConfig
from core.logging_config import logger

app = Flask(__name__)

async def teams_on_error(context: TurnContext, error: Exception):
    traceback.print_exc()
    await context.send_activity("The bot encountered an error or bug.")
    await context.send_activity("To continue to run this bot, please fix the bot source code.")
    if context.activity.channel_id == "emulator":
        trace_activity = Activity(
            label="TurnError",
            name="on_turn_error Trace",
            timestamp=datetime.utcnow(),
            type=ActivityTypes.trace,
            value=f"{error}",
            value_type="https://www.botframework.com/schemas/error",
        )
        await context.send_activity(trace_activity)

class TeamsBotOsInputAdapter(BotOsInputAdapter):
    def __init__(self, bot_name=None, app_id=None, app_password=None, app_type=None, app_tenantid=None, bot_id=None, response_map=None, proxy_messages_in=None, events=None, genbot_internal_project_and_schema=None):
        super().__init__()

        print('TeamsBotOsInputAdapter __init__')

        self.app_id = app_id if app_id is not None else os.environ.get("MicrosoftAppID", "")
        self.app_password = app_password if app_password is not None else os.environ.get("MicrosoftAppPassword", "")
        self.app_type = app_type if app_type is not None else os.environ.get("MicrosoftAppType", "MultiTenant")
        self.app_tenantid = app_tenantid if app_tenantid is not None else os.environ.get("MicrosoftAppTenantId", "")

        self.response_map = response_map if response_map is not None else {}
        self.proxy_messages_in = proxy_messages_in if proxy_messages_in is not None else []
        self.events = events if events is not None else deque()
        self.genbot_internal_project_and_schema = genbot_internal_project_and_schema if genbot_internal_project_and_schema is not None else os.getenv('GENESIS_INTERNAL_DB_SCHEMA','None')
        self.bot_name = bot_name
        self.bot_id = bot_id if bot_id is not None else {}

        self.response_map = {}
        self.proxy_messages_in = []
        self.events = deque()
        self.id_to_turncontext_map = {}

        CONFIG = DefaultConfig()
        ADAPTER = CloudAdapter(ConfigurationBotFrameworkAuthentication(CONFIG))
        ADAPTER.on_turn_error = teams_on_error

        BOT = EchoBot(add_event=self.add_event, response_map=self.response_map)

        @app.route("/api/messages", methods=["POST"])
        def messages():
            data = request.json
            print(f"Received from Emulator: {data.get('text','')}")
            return ADAPTER.process(request, BOT)

        @app.route("/healthcheck", methods=["GET"])
        def healthcheck():
            print("3978 healthcheck")
            return jsonify({"status": "I'm ready (from get /healthcheck:3978)"})

        def start_flask_app():
            # app.run(host="0.0.0.0", port=3978)
            pass

        t = threading.Thread(target=start_flask_app)
        t.start()

        @functools.cached_property
        def db_connector(self):
            return get_global_db_connector()

    def add_event(self, event):
        self.events.append(event)

    def get_input(self, thread_map=None, active=None, processing=None, done_map=None):
        if len(self.events) == 0:
            return None
        try:
            event_tc = self.events.popleft()
        except IndexError:
            return None
        try:
            event = event_tc.activity
            uu = event.id
            channel = event.channel_id
            bot_id = event.recipient
            metadata = {}
            if uu:
                metadata["input_uuid"] = uu
            metadata["channel_type"] = "msteams"
            metadata["channel_name"] = channel
            metadata["user_id"] = bot_id.id
            metadata["user_name"] = bot_id.name
            thread_id = 'thread_1'
            message_text = event.text
        except Exception as e:
            return None
        return BotOsInputMessage(thread_id=thread_id, msg=message_text, metadata=metadata)

    async def return_result(self, turn_context: TurnContext, message: BotOsOutputMessage):
        await turn_context.send_activity(
            MessageFactory.text(f"Response: {message.output}")
        )

    def handle_response(self, session_id: str, message: BotOsOutputMessage, in_thread=None, in_uuid=None, task_meta=None):
        if in_uuid is not None:
            if message.output == '!NO_RESPONSE_REQUIRED':
                self.response_map[in_uuid] = "(no response needed)"
            else:
                try:
                    check_message = message.output.replace("\n","").replace("json","").replace("```","")
                    process_json = json.loads(check_message)
                    print(process_json)
                    self.response_map[in_uuid] = process_json
                except json.JSONDecodeError:
                    self.response_map[in_uuid] = message.output

    def submit(self, input, thread_id, bot_id):
        if type(bot_id) == str:
            bot_id = json.loads(bot_id)

        uu = str(uuid.uuid4())
        self.proxy_messages_in.append({"msg": input, "uuid": uu, "thread_id": thread_id, "bot_id": bot_id})

        self.add_event({"msg": input, "thread_id": thread_id, "uuid": uu, "bot_id": bot_id})

        return uu

    def get_artifacts_store(db_adapter):
        from connectors import SnowflakeConnector
        if isinstance(db_adapter, SnowflakeConnector):
            return SnowflakeStageArtifactsStore(db_adapter)
        else:
            raise NotImplementedError(f"No artifacts store is implemented for {db_adapter}")
