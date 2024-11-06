# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.

# from botbuilder.core import ActivityHandler, TurnContext
from botbuilder.core import ActivityHandler, MessageFactory, TurnContext
from botbuilder.schema import ChannelAccount

import logging
import asyncio

class EchoBot(ActivityHandler):
    def __init__(self, add_event = None, response_map = None):

        #from teams_bot_os_adapter import TeamsBotOsInputAdapter
        super().__init__()
        self.add_event = add_event
        self.response_map = response_map
        #self.teams_bot= TeamsBotOsInputAdapter(self)

    # See https://aka.ms/about-bot-activity-message to learn more about the message and other activity types.

    async def on_message_activity(self, turn_context: TurnContext):
        user_message = turn_context.activity.text #message input
        thread_id = TurnContext.get_conversation_reference(turn_context.activity).activity_id

        self.add_event(turn_context)
        uu = turn_context.activity.id
        while uu not in self.response_map:
            await asyncio.sleep(0.1)  # Small delay to prevent busy waiting
        # This loop waits until any of the last 10 characters of the response mapped to the unique user ID (uu) are not the speech balloon emoji (💬)
        while '💬' in self.response_map[uu][-10:]:
            await asyncio.sleep(0.1)
        response = self.response_map.pop(uu)
        return await turn_context.send_activity(
            MessageFactory.text(response)
        )
        await turn_context.send_activity(f"You said '{ turn_context.activity.text }'")

    async def on_members_added_activity(
        self,
        members_added: ChannelAccount,
        turn_context: TurnContext
    ):
        for member_added in members_added:
            if member_added.id != turn_context.activity.recipient.id:
                await turn_context.send_activity("Hello and welcome!!!")
                print('Added member id: ', member_added.id)
                MessageFactory.text(f"member id: {member_added.id}" )
                MessageFactory.text(f"You said ffff: {turn_context.activity.recipient.id}")
