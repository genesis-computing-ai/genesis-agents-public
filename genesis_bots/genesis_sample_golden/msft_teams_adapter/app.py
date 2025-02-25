import logging
import os
from aiohttp import web
from botbuilder.core import BotFrameworkAdapter, BotFrameworkAdapterSettings
from botbuilder.core.integration import aiohttp_error_middleware
from botbuilder.schema import Activity
from bot import EchoBot, login
import asyncio
import json
from datetime import datetime
from pytz import timezone

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Get credentials from environment variables (more secure)
APP_ID = os.environ.get("APP_ID", "")
APP_PASSWORD = os.environ.get("APP_PASSWORD", "")

logger.info("Initializing bot with APP_ID: %s", APP_ID)

SETTINGS = BotFrameworkAdapterSettings(APP_ID, APP_PASSWORD)
ADAPTER = BotFrameworkAdapter(SETTINGS)
BOT = EchoBot()

last_wake_up = datetime.now()

async def wake_up():
    """Background task that runs every 59 minutes"""
    global last_wake_up
    while True:
        refresh_interval = os.environ.get("TOKEN_LIFETIME", 59) * 60
        retry_interval = os.environ.get("RETRY_INTERVAL", 5) * 60
        try:
            print(f'Entered wake up task - previous wake up: {last_wake_up.strftime("%Y-%m-%d %H:%M:%S")}', flush=True)
            last_wake_up = datetime.now()
            login()
            await asyncio.sleep(refresh_interval)  # 59 min x 60 = 3540 seconds
        except Exception as e:
            logger.error(f"Error in wake_up task: {e}")
            await asyncio.sleep(retry_interval)  # Keep trying every 5 min even if there's an error

# Error handler
async def on_error(context, error):
    logger.error(f"Error processing request: {error}")
    await context.send_activity("Sorry, something went wrong!")

ADAPTER.on_turn_error = on_error

# Define the handler functions before using them in router
async def messages(request):
    if "application/json" not in request.headers.get("Content-Type", ""):
        return web.Response(text="Expected Content-Type: application/json", status=415)

    try:
        auth_header = request.headers.get("Authorization", "")
        body = await request.json()

        if not body:
            return web.Response(text="Request body is empty", status=400)

        print(f"Received activity: {json.dumps(body, indent=2)}")
        print(f"Auth header: {auth_header}")

        activity = Activity().deserialize(body)

        async def turn_call(context):
            await BOT.on_turn(context)

        response = await ADAPTER.process_activity(activity, auth_header, turn_call)

        if response:
            return web.json_response(data=response.body, status=response.status)
        return web.Response(status=201)

    except Exception as e:
        print(f"Error processing message: {str(e)}")
        return web.Response(text=f"Error: {str(e)}", status=500)

async def health_check(request):
    pacific = timezone('US/Pacific')
    last_wake_up_pacific = last_wake_up.astimezone(pacific).strftime("%Y-%m-%d %H:%M:%S")
    return web.Response(text=f"A votre sante! Last wake up: {last_wake_up_pacific}", status=200)

async def init_app():
    app = web.Application(middlewares=[aiohttp_error_middleware])
    app.router.add_post("/api/messages", messages)
    app.router.add_get("/health", health_check)

    if os.environ.get("KEEP-ALIVE"):
        print('Starting wake up task...', flush=True)
        asyncio.create_task(wake_up())

    return app

if __name__ == "__main__":
    try:
        logger.info("Starting web app on port 8000")
        app = asyncio.get_event_loop().run_until_complete(init_app())
        web.run_app(app, host="0.0.0.0", port=8000)
    except Exception as error:
        logger.error(f"Error running app: {error}")
        raise error