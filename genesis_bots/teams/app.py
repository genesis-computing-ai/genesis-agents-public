# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.

import sys
import traceback
from datetime import datetime

from aiohttp import web
from aiohttp.web import Request, Response, json_response
from botbuilder.core import (
    BotFrameworkAdapterSettings,
    TurnContext,
    BotFrameworkAdapter,
)
from botbuilder.core.integration import aiohttp_error_middleware
from botbuilder.schema import Activity, ActivityTypes

from genesis_bots.teams.bot import EchoBot
from genesis_bots.teams.config import DefaultConfig

from genesis_bots.core.logging_config import logger

#from bots import BOT

CONFIG = DefaultConfig()

# Create adapter.
# See https://aka.ms/about-bot-adapter to learn more about how bots work.
SETTINGS = BotFrameworkAdapterSettings(CONFIG.APP_ID, CONFIG.APP_PASSWORD)
ADAPTER = BotFrameworkAdapter(SETTINGS)


# Catch-all for errors.
async def on_error(context: TurnContext, error: Exception):
    # This check writes out errors to console log .vs. app insights.
    # NOTE: In production environment, you should consider logging this to Azure
    #       application insights.
    logger.info(f"\n [on_turn_error] unhandled error: {error}", file=sys.stderr)
    traceback.print_exc()

    # Send a message to the user
    await context.send_activity("The bot encountered an error or bug.")
    await context.send_activity(
        "To continue to run this bot, please fix the bot source code."
    )
    # Send a trace activity if we're talking to the Bot Framework Emulator
    if context.activity.channel_id == "emulator":
        # Create a trace activity that contains the error object
        trace_activity = Activity(
            label="TurnError",
            name="on_turn_error Trace",
            timestamp=datetime.utcnow(),
            type=ActivityTypes.trace,
            value=f"{error}",
            value_type="https://www.botframework.com/schemas/error",
        )
        # Send a trace activity, which will be displayed in Bot Framework Emulator
        await context.send_activity(trace_activity)


ADAPTER.on_turn_error = on_error

# Create the Bot
BOT = MyBot()


# Listen for incoming requests on /api/messages
async def messages(req: Request) -> Response:
    # Main bot message handler.
    if "application/json" in req.headers["Content-Type"]:
        body = await req.json()
    else:
        return Response(status=415)

    activity = Activity().deserialize(body)
    auth_header = req.headers["Authorization"] if "Authorization" in req.headers else ""

    response = await ADAPTER.process_activity(activity, auth_header, BOT.on_turn)
    print(response)
    if response:
        return json_response(data=response.body, status=response.status)
    return Response(status=201)


APP = web.Application(middlewares=[aiohttp_error_middleware])
APP.router.add_post("/api/messages", messages)

if __name__ == "__main__":
    try:
        web.run_app(APP, host="localhost", port=CONFIG.PORT)
    except Exception as error:
        raise error

""" #ECHO BOT USING WEBSOCKETS
import asyncio
from aiohttp import web
from botbuilder.core import BotFrameworkAdapterSettings, TurnContext, ActivityHandler
from botbuilder.schema import Activity
from botbuilder.integration.aiohttp import AiohttpBotFrameworkAdapter



class MyBot(ActivityHandler):
    async def on_message_activity(self, turn_context: TurnContext):
        await turn_context.send_activity(f"You said: {turn_context.activity.text}")

# Define bot settings
settings = BotFrameworkAdapterSettings("a0f356cd-3de8-4b1f-95b5-41605677667f", "TeR8Q~wTTqTlKXz2xSMUyrKXNqfuOKSFGMaEuaQo")

# Initialize the adapter
adapter = AiohttpBotFrameworkAdapter(settings)

# Create the bot
bot = MyBot()

# WebSocket connection handler
async def websocket_handler(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    async for msg in ws:
        if msg.type == web.WSMsgType.TEXT:
            # Process incoming WebSocket message
            activity = Activity().deserialize(msg.data)
            context = TurnContext(adapter, activity)
            await bot.on_turn(context)
        elif msg.type == web.WSMsgType.ERROR:
            logger.info(f'WebSocket connection closed with exception {ws.exception()}')

    return ws

# Initialize aiohttp app and routes
app = web.Application()
app.router.add_get("/ws", websocket_handler)

# Start the app
if __name__ == "__main__":
    web.run_app(app, host="localhost", port=3978)
  """
