import asyncio
from discord_adapter import DiscordBotAdapter  # Assuming you've named your file discord_bot_adapter.py
from core.logging_config import logger

async def main():
    bot_adapter = DiscordBotAdapter(
        token="MTI3NzczMjY0MzE4MTY5NDk3Ng.GmeAdq.VhOiKzvwIEZzQKcrefXHiqHm0F011LH6cbWKVA",
        channel_id="1277729849401675870",
        bot_user_id="1277732643181694976",
        bot_name="Eve"
    )
    # server id 1277729849401675867

    try:
        await bot_adapter.start_bot()
    except KeyboardInterrupt:
        logger.info("Bot is shutting down...")
    finally:
        # Perform any cleanup if necessary
        pass

if __name__ == "__main__":
    asyncio.run(main())