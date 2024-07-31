# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.

from botbuilder.core import ActivityHandler, MessageFactory, TurnContext
from botbuilder.schema import ChannelAccount

import time

class EchoBot(ActivityHandler):
    async def on_members_added_activity(
        self, members_added: [ChannelAccount], turn_context: TurnContext
    ):
        for member in members_added:
            if member.id != turn_context.activity.recipient.id:
                await turn_context.send_activity("Hello and welcome!")
                MessageFactory.text(f"member id: {member.id}" )
                MessageFactory.text(f"You said ffff: {turn_context.activity.recipient.id}")

    async def on_message_activity(self, turn_context: TurnContext):
        print('got message: ',turn_context.activity.text)
        print('waiting 1')
        time.sleep(1)
        print('waited...')
        resp = 'Genbot says: '+turn_context.activity.turn_context.reverse()+' haha!'
        return await turn_context.send_activity(
            MessageFactory.text(f"{resp}")
        )
