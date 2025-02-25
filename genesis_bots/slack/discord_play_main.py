import asyncio
from discord_adapter import DiscordBotAdapter  # Assuming you've named your file discord_bot_adapter.py
from genesis_bots.core.logging_config import logger

async def main():
    bot_adapter = DiscordBotAdapter(
        token="xxxxx",
        channel_id="xxx",
        bot_user_id="xxx",
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