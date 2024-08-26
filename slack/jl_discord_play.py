from __future__ import annotations
import discord
from discord.ext import commands
import logging
import os
import random
import re
import datetime
from core.bot_os_input import BotOsInputAdapter, BotOsInputMessage, BotOsOutputMessage

logger = logging.getLogger(__name__)

# ... (keep the print statements for the ASCII art)

class DiscordBotAdapter(BotOsInputAdapter):

    def __init__(
        self,
        token: str,
        channel_id: int,
        bot_user_id: int,
        bot_name: str = "Unknown",
    ) -> None:
        logger.debug("DiscordBotAdapter")
        super().__init__()
        self.bot = commands.Bot(command_prefix='!', intents=discord.Intents.all())
        self.token = token
        self.channel_id = channel_id
        self.bot_user_id = bot_user_id
        self.bot_name = bot_name
        self.events = []
        self.user_info_cache = {}

        @self.bot.event
        async def on_ready():
            print(f'{self.bot.user} has connected to Discord!')

        @self.bot.event
        async def on_message(message):
            if message.author == self.bot.user:
                return

            # Add the message to the events queue
            self.events.append(message)

        # Start the bot
        self.bot.run(self.token)

    async def get_input(self) -> BotOsInputMessage | None:
        if not self.events:
            return None

        message = self.events.pop(0)
        
        user_full_name = str(message.author)
        user_id = message.author.id
        msg = message.content

        metadata = {
            "channel_id": message.channel.id,
            "message_id": message.id,
            "user_id": user_id,
            "user_name": user_full_name,
        }

        files = []
        for attachment in message.attachments:
            file_path = f"./downloaded_files/{message.id}/{attachment.filename}"
            await attachment.save(file_path)
            files.append(file_path)

        return BotOsInputMessage(
            thread_id=str(message.channel.id),
            msg=f"{user_full_name} says: {msg}",
            files=files,
            metadata=metadata,
        )

    async def handle_response(self, session_id: str, message: BotOsOutputMessage):
        try:
            channel = self.bot.get_channel(int(message.thread_id))
            if not channel:
                logger.error(f"Channel not found: {message.thread_id}")
                return

            # Split message if it's too long
            if len(message.output) > 2000:
                chunks = [message.output[i:i+2000] for i in range(0, len(message.output), 2000)]
                for chunk in chunks:
                    await channel.send(chunk)
            else:
                await channel.send(message.output)

            # Handle file uploads
            for file_path in message.files:
                await channel.send(file=discord.File(file_path))

        except Exception as e:
            logger.error(f"DiscordBotAdapter:handle_response - Error posting message: {e}")

    async def send_discord_direct_message(self, user_id: int, message: str, attachments=[]):
        try:
            user = await self.bot.fetch_user(user_id)
            await user.send(message)
            
            for attachment in attachments:
                await user.send(file=discord.File(attachment))

            return {"success": True, "message": f"Message sent to user {user_id} successfully."}
        except Exception as e:
            return {"success": False, "message": f"Error sending message: {str(e)}"}

    async def send_discord_channel_message(self, channel_id: int, message: str, attachments=[]):
        try:
            channel = self.bot.get_channel(channel_id)
            if not channel:
                return {"success": False, "message": f"Channel with ID {channel_id} not found."}

            await channel.send(message)

            for attachment in attachments:
                await channel.send(file=discord.File(attachment))

            return {"success": True, "message": f"Message sent to channel {channel_id} successfully."}
        except Exception as e:
            return {"success": False, "message": f"Error sending message: {str(e)}"}

    async def lookup_discord_user_id(self, user_name: str):
        for guild in self.bot.guilds:
            member = discord.utils.get(guild.members, name=user_name)
            if member:
                return {"success": True, "user_id": member.id}
        
        return {"success": False, "message": "User not found"}

# ... (keep any other utility functions you need)
