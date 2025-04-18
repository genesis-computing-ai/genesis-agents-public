import logging
import os
from datetime import datetime
from aiohttp import web
from botbuilder.core import BotFrameworkAdapter, BotFrameworkAdapterSettings
from botbuilder.core.integration import aiohttp_error_middleware
from botbuilder.schema import Activity
from bot import EchoBot, login
import asyncio
import json
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Get credentials from environment variables (more secure)
APP_ID = os.environ.get("APP_ID", None)
APP_PASSWORD = os.environ.get(
    "APP_PASSWORD", None
)

logger.info("\nInitializing bot with APP_ID: %s | APP_PASSWORD: %s\n", APP_ID, APP_PASSWORD)

SETTINGS = BotFrameworkAdapterSettings(APP_ID, APP_PASSWORD)
ADAPTER = BotFrameworkAdapter(SETTINGS)
BOT = EchoBot()

last_wake_up = datetime.now()


async def wake_up():
    """Background task that runs every 59 minutes"""
    global last_wake_up
    while True:
        refresh_interval = int(os.environ.get("TOKEN_LIFETIME", "59")) * 60
        retry_interval = int(os.environ.get("RETRY_INTERVAL", "5")) * 60

        try:
            print(
                f'Entered wake up task - previous wake up: {last_wake_up.strftime("%Y-%m-%d %H:%M:%S")}',
                flush=True,
            )
            last_wake_up = datetime.now()
            login()
            await asyncio.sleep(refresh_interval)  # 59 min x 60 = 3540 seconds
        except Exception as e:
            logger.error(f"Error in wake_up task: {e}")
            await asyncio.sleep(
                retry_interval
            )  # Keep trying every 5 min even if there's an error

# Error handler
async def on_error(context, error):
    """Error handler for the bot"""
    logger.error(f"Error processing request: {error}")
    await context.send_activity("Sorry, something went wrong!")


ADAPTER.on_turn_error = on_error


async def messages(request):
    """ Define the handler functions before using them in router """
    logger.info("=== ENTERING messages handler ===")
    if "application/json" in request.headers.get("Content-Type", ""):
        body = await request.json()
        
        # Log the entire activity object for debugging
        logger.info(f"Received activity: {json.dumps(body, indent=2)}")
        
        # Log auth header for debugging
        auth_header = request.headers.get("Authorization", "")
        logger.info(f"Auth header: {auth_header}")
        
        try:
            logger.info("=== Deserializing activity ===")
            activity = Activity().deserialize(body)
            logger.info(f"=== Activity deserialized successfully: type={activity.type} ===")
            
            # Debug: Log activity attributes
            logger.info(f"=== Activity attributes: ===")
            for attr in dir(activity):
                if not attr.startswith('_'):
                    try:
                        value = getattr(activity, attr)
                        logger.info(f"  {attr}: {value}")
                    except Exception as attr_err:
                        logger.info(f"  Error accessing {attr}: {str(attr_err)}")
            
            logger.info("=== Setting up turn_call function ===")
            async def turn_call(context):
                logger.info("=== Inside turn_call, about to call BOT.on_turn ===")
                try:
                    await BOT.on_turn(context)
                    logger.info("=== BOT.on_turn completed successfully ===")
                except Exception as turn_err:
                    logger.error(f"=== Error in BOT.on_turn: {str(turn_err)} ===")
                    # Get full traceback
                    import traceback
                    logger.error(f"=== Traceback: {traceback.format_exc()} ===")
                    raise

            logger.info("=== About to call ADAPTER.process_activity ===")
            response = await ADAPTER.process_activity(activity, auth_header, turn_call)
            logger.info("=== ADAPTER.process_activity completed ===")
            
            if response:
                logger.info(f"=== Returning response with status: {response.status} ===")
                return web.json_response(data=response.body, status=response.status)
            logger.info("=== Returning 201 status (no response body) ===")
            return web.Response(status=201)
        except Exception as e:
            logger.error(f"=== Error processing request: {str(e)} ===")
            # Get full traceback
            import traceback
            logger.error(f"=== Traceback: {traceback.format_exc()} ===")
            raise
    else:
        logger.info("=== Invalid Content-Type, returning 415 ===")
        return web.Response(text="Expected Content-Type: application/json", status=415)


async def health_check(request):
    try:
        logger.debug("Health check endpoint called")
        health_info = {
            "status": "healthy",
            "version": "1.0.0",
            "bot": {
                "id": APP_ID,
                "status": "running"
            },
            "environment": {
                "port": os.environ.get("PORT", "8000"),
                "websites_port": os.environ.get("WEBSITES_PORT", "Not set"),
                "python_path": os.environ.get("PYTHONPATH", "Not set")
            }
        }
        logger.debug("Health check response: %s", health_info)
        return web.json_response(health_info)
    except Exception as e:
        logger.error("Health check failed: %s", str(e), exc_info=True)
        return web.json_response(
            {"status": "unhealthy", "error": str(e)},
            status=500
        )

async def init_app():
    """Initialize the app"""
    app = web.Application(middlewares=[aiohttp_error_middleware])
    app.router.add_post("/api/messages", messages)
    app.router.add_get("/health", health_check)

    if os.environ.get("KEEP-ALIVE"):
        print('Starting wake up task...', flush=True)
        asyncio.create_task(wake_up())
    return app

# Now we can use these handlers in the router
# app = web.Application(middlewares=[aiohttp_error_middleware])
# app.router.add_post("/api/messages", messages)
# app.router.add_get("/health", health_check)

if __name__ == "__main__":
    try:
        logger.info("Starting web app on port 8000")
        app = asyncio.get_event_loop().run_until_complete(init_app())
        web.run_app(app, host="0.0.0.0", port=8000)
    except Exception as error:
        logger.error(f"Error running app: {error}")
        raise
