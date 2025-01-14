import discord
from discord.ext import commands
import asyncio

from genesis_bots.core.bot_os_input import BotOsInputAdapter, BotOsInputMessage, BotOsOutputMessage

from genesis_bots.core.logging_config import logger
class DiscordBotAdapter(BotOsInputAdapter):
    def __init__(
        self,
        token: str,
        channel_id: str,
        bot_user_id: str,
        bot_name: str = "Unknown",
    ) -> None:
        super().__init__()
        intents = discord.Intents.default()
        intents.message_content = True
        self.bot = commands.Bot(command_prefix='!', intents=intents)
        self.token = token
        self.channel_id = channel_id
        self.bot_user_id = bot_user_id
        self.bot_name = bot_name
        self.user_info_cache = {}
        self.events = asyncio.Queue()

        @self.bot.event
        async def on_ready():
            logger.info(f'{self.bot.user} has connected to Discord!')

        @self.bot.event
        async def on_message(message):
            if message.author == self.bot.user:
                return

            await self.handle_message(message)

    async def start_bot(self):
        await self.bot.start(self.token)

    async def handle_message(self, message):
        # Reverse the message content
        reversed_content = message.content[::-1]

        # Send the reversed message back
        await message.channel.send(f"Reversed: {reversed_content}")

    async def get_input(self) -> BotOsInputMessage | None:
        # This method is not used in this simple example, but kept for compatibility
        return None

    async def handle_response(self, session_id: str, message: BotOsOutputMessage):
        # This method is not used in this simple example, but kept for compatibility
        pass

# Run the bot
if __name__ == "__main__":
    bot_adapter = DiscordBotAdapter(
        token="YOUR_DISCORD_BOT_TOKEN",
        channel_id="YOUR_DEFAULT_CHANNEL_ID",
        bot_user_id="YOUR_BOT_USER_ID",
        bot_name="YourBotName"
    )
    asyncio.run(bot_adapter.start_bot())