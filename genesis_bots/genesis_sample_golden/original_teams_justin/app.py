import logging
import os
from aiohttp import web
from botbuilder.core import BotFrameworkAdapter, BotFrameworkAdapterSettings
from botbuilder.core.integration import aiohttp_error_middleware
from botbuilder.schema import Activity
from bot import EchoBot
import asyncio
import json

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Get credentials from environment variables (more secure)
APP_ID = os.environ.get("APP_ID", "73e855e4-fefc-441d-bbca-5256a95dabf6")
APP_PASSWORD = os.environ.get("APP_PASSWORD", "<ADD APP PASSWORD>")

logger.info("Initializing bot with APP_ID: %s", APP_ID)

SETTINGS = BotFrameworkAdapterSettings(APP_ID, APP_PASSWORD)
ADAPTER = BotFrameworkAdapter(SETTINGS)
BOT = EchoBot()

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
    return web.Response(text="Healthy", status=200)

# Now we can use these handlers in the router
app = web.Application(middlewares=[aiohttp_error_middleware])
app.router.add_post("/api/messages", messages)
app.router.add_get("/health", health_check)

if __name__ == "__main__":
    try:
        logger.info("Starting web app on port 8000")
        web.run_app(app, host="0.0.0.0", port=8000)
    except Exception as error:
        logger.error(f"Error running app: {error}")
        raise