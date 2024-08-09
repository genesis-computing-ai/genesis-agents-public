# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.

from botbuilder.core import ActivityHandler, MessageFactory, TurnContext
from botbuilder.schema import ChannelAccount, ConversationReference, Activity
#from core.bot_os_input import BotOsInputAdapter, BotOsInputMessage, BotOsOutputMessage
import logging
import asyncio

logger = logging.getLogger(__name__)


class EchoBot(ActivityHandler):
    def __init__(self, add_event = None, response_map = None):
    
        #from teams_bot_os_adapter import TeamsBotOsInputAdapter
        super().__init__()
        self.add_event = add_event
        self.response_map = response_map
        #self.teams_bot= TeamsBotOsInputAdapter(self)
        
    async def on_members_added_activity(
        self, members_added: [ChannelAccount], turn_context: TurnContext
    ):
        for member in members_added:
            if member.id != turn_context.activity.recipient.id: 
                #print(turn_context.activity.id) conversation ID? 
                await turn_context.send_activity("Hello and welcome!")
                MessageFactory.text(f"member id: {member.id}" )
                MessageFactory.text(f"You said ffff: {turn_context.activity.recipient.id}")

    async def on_message_activity(self, turn_context: TurnContext):

        user_message = turn_context.activity.text #message input
        
        thread_id = TurnContext.get_conversation_reference(turn_context.activity).activity_id
       
        #self.EchoBot.get_input(user_message)

        """ self.teams_bot.submit(user_message, thread_id=thread_id, bot_id=self.teams_bot.bot_id)
        
        logger.info(f"Message activity received and submitted: {user_message}, thread_id: {thread_id}")
        bot_os_input_message = BotOsInputMessage(thread_id=thread_id, msg=user_message)
        self.teams_bot.handle_response(thread_id, bot_os_input_message)
         """
    
    
        """ test_msg = BotOsOutputMessage(output="Test response")
        await self.teams_bot.return_result(turn_context, test_msg)  """
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
""" 
    async def return_result(self,turn_context: TurnContext, message:BotOsOutputMessage):
        return await turn_context.send_activity(
            MessageFactory.text(f"Response: {message.output}")
        ) 
 """