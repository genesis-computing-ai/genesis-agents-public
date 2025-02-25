# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.

# from botbuilder.core import ActivityHandler, TurnContext
from botbuilder.core import ActivityHandler, MessageFactory, TurnContext, CardFactory
from botbuilder.schema import ChannelAccount, Activity, ActivityTypes, HeroCard, CardAction, ActionTypes, Attachment

import logging
import asyncio
import json
import re
import os
import base64
from PIL import Image
import io
from pathlib import Path
from core.bot_os_artifacts import ARTIFACT_ID_REGEX, get_artifacts_store
from connectors import get_global_db_connector
import functools

ARTIFACT_ID_REGEX = r'[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}' # regrex for matching a valid artifact UUID

class EchoBot(ActivityHandler):
    def __init__(self, add_event = None, response_map = None):

        # from teams_bot_os_adapter import TeamsBotOsInputAdapter
        super().__init__()

        print('EchoBot __init__')

        self.add_event = add_event
        self.response_map = response_map
        # self.teams_bot= TeamsBotOsInputAdapter(self)

    @functools.cached_property
    def db_connector(self):
        return get_global_db_connector()

    async def on_turn(self, turn_context: TurnContext):
        print('\nTurn Context:', turn_context.activity, '\n')
        await super().on_turn(turn_context)

    async def on_event_activity(self, turn_context: TurnContext):
        print('\nEvent Activity:', turn_context.activity, '\n')
        return await super().on_event_activity(turn_context)


    async def on_message_reaction_activity(self, turn_context: TurnContext):
        print('\nReaction Activity:', turn_context.activity, '\n')
        return await super().on_message_reaction_activity(turn_context)

    async def on_conversation_update_activity(self, turn_context: TurnContext):
        print('\nConversation Update Activity:', turn_context.activity, '\n')
        return await super().on_conversation_update_activity(turn_context)

    async def on_teams_task_module_submit(self, turn_context: TurnContext):
        print('\nTask Module Submit:', turn_context.activity, '\n')
        return await super().on_teams_task_module_submit(turn_context)

    async def on_teams_task_module_fetch(self, turn_context: TurnContext):
        print('\nTask Module Fetch:', turn_context.activity, '\n')
        return await super().on_teams_task_module_fetch(turn_context)

    async def on_message_activity(self, turn_context: TurnContext):
        if turn_context.activity.text:
            user_message = turn_context.activity.text #message input
        elif turn_context.activity.value and turn_context.activity.value['type'] == 'process':
            user_message = turn_context.activity.text = 'Create a new process with the folllowing details: ' + json.dumps(turn_context.activity.value)
        elif turn_context.activity.value and turn_context.activity.value['type'] == 'note':
            user_message = turn_context.activity.text = 'Create a new note with the folllowing details: ' + json.dumps(turn_context.activity.value)
        thread_id = TurnContext.get_conversation_reference(turn_context.activity).activity_id
        print('Message Activity', user_message)

        self.add_event(turn_context)
        uu = turn_context.activity.id
        while uu not in self.response_map:
            await asyncio.sleep(0.1)  # Small delay to prevent busy waiting
        # This loop waits until any of the last 10 characters of the response mapped to the unique user ID (uu) are not the speech balloon emoji (💬)
        while isinstance(self.response_map[uu], str) and '💬' in self.response_map[uu][-4:]:
            await asyncio.sleep(0.1)

        response = self.response_map.pop(uu)

        print('Response Type:', type(response))
        if isinstance(response, str):
            response = re.sub(r'🧰.*?\n', '', response)

        if isinstance(response, dict):
            json_returned = True
        else:
            try:
                stripped = response.replace("\n", "").replace("json", "").replace("```", "")
                response = json.loads(stripped)
                json_returned = True
            except:
                json_returned = False

        if json_returned or isinstance(response, dict):
            if response.get('dict_type',None) == 'create_process':
                with open('./teams/resources/ProcessInputCard.json', "r") as file:
                    card_data_str = file.read()
                    card_data_dict = json.loads(card_data_str)
            elif response['dict_type'] == 'create_note':
                with open('./teams/resources/NoteInputCard.json', "r") as file:
                    card_data_str = file.read()
                    card_data_dict = json.loads(card_data_str)
            else:
                await turn_context.send_activity(
                    MessageFactory.text(response)
                )
                return

            def replace_values(d, key, value):
                if isinstance(d, dict):
                    for k, v in d.items():
                        if isinstance(v, (dict, list)):
                            replace_values(v, key, value)
                        elif v == '{' + key + '}':
                            d[k] = value
                elif isinstance(d, list):
                    for item in d:
                        if isinstance(item, dict):
                            replace_values(item, key, value)
                elif v == '{' + key + '}':
                    d[k] = value

            for k, v in response.items():
                replace_values(card_data_dict['body'], k, v)

            card = CardFactory.adaptive_card(card_data_dict)

            message = Activity(
                type=ActivityTypes.message,
                attachments=[card]
            )

            await turn_context.send_activity(message)
        else:
            print(f"Text only response: {response}")
            artifact_pattern = re.compile(r'(\[([^\]]+)\]\(artifact:/(' + ARTIFACT_ID_REGEX + r')\))')
            # artifact_pattern = re.compile(r'artifact:/.*?\)')
            matches = artifact_pattern.findall(response)

            if matches:
                # Locate all artifact markdowns, save those artifacts to the local 'sandbox' and replace with sandbox markdown
                # so that they will be handled with other local files below.
                af = None
                for full_match, description, uuid in matches:
                    af = af or get_artifacts_store(self.db_connector)
                    try:
                        # Download the artifact data into a local file
                        local_dir = f"downloaded_files/msteams" #{message_thread_id}" # follow the conventions used by sandbox URLs.
                        Path(local_dir).mkdir(parents=True, exist_ok=True)
                        downloaded_filename = af.read_artifact(uuid, local_dir)
                    except Exception as e:
                        print(f"{self.__class__.__name__}: Failed to fetch data for artifact {uuid}. Error: {e}")
                    else:
                        # Update the markdown in the message to look like a sandbox URL
                        response = response.replace(full_match, f"[{description}](sandbox:/mnt/data/{downloaded_filename})")

                # Extract local paths from the msg
                local_paths = self._extract_local_file_markdowns(response, 'msteams')
                for local_path in local_paths:
                    file_path = os.path.join(os.getcwd(), local_path)
                    with open(file_path, "rb") as in_file:
                        base64_image = base64.b64encode(in_file.read()).decode()

                    # Resize the image
                    # image = Image.open(file_path)
                    # image = image.resize((100, 100))  # Resize to 300x300 pixels
                    # buffered = io.BytesIO()
                    # image.save(buffered, format="PNG")
                    # base64_image = base64.b64encode(buffered.getvalue()).decode()

                    attachment = Attachment(
                        name=description,
                        content_type="image/png",
                        content_url=f"data:image/png;base64,{base64_image}",
                        imageSize = 'large'
                    )

                    message = Activity(
                        type=ActivityTypes.message,
                        attachments=[attachment]
                    )

                    await turn_context.send_activity(
                        message
                    )

                return

            await turn_context.send_activity(
                MessageFactory.text(response)
            )

        return

    async def _display_options(self, turn_context: TurnContext):
        """
        Create a HeroCard with options for the user to interact with the bot.
        :param turn_context:
        :return:
        """

        # Note that some channels require different values to be used in order to get buttons to display text.
        # In this code the emulator is accounted for with the 'title' parameter, but in other channels you may
        # need to provide a value for other parameters like 'text' or 'displayText'.
        card = HeroCard(
            text="You can upload an image or select one of the following choices",
            buttons=[
                CardAction(
                    type=ActionTypes.im_back, title="1. Inline Attachment", value="1"
                ),
                CardAction(
                    type=ActionTypes.im_back, title="2. Internet Attachment", value="2"
                ),
                CardAction(
                    type=ActionTypes.im_back, title="3. Uploaded Attachment", value="3"
                ),
            ],
        )

        reply = MessageFactory.attachment(CardFactory.hero_card(card))
        await turn_context.send_activity(reply)

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

    def _extract_local_file_markdowns(self, msg, message_thread_id):
        """
        Extracts file paths from non-Slack-compatible markdown links like [name](url) in the msg, which 
        contain local path placeholders, and transforms them into actual local paths.

        This function searches for specific patterns in the message that represent local file links 
        like 'sandbox:", "./downloaded_files", etc. and converts these links into local file paths 
        based on the message thread ID. 

        Args:
            msg (str): The message containing markdown links to files.
            message_thread_id (str): The ID of the message thread, used to construct local paths.

        Returns:
            list: A list of unique local file paths extracted from the message.
        """
        local_paths = set()

        # Define patterns and their corresponding local path transformations
        patterns = [
            # typical image path using sandbox:
            (r"\[.*?\]\((sandbox:/mnt/data(?:/downloads)?/.*?)\)",
                lambda match: match.replace("sandbox:/mnt/data/downloads", f"./downloaded_files/{message_thread_id}").replace("sandbox:/mnt/data", f"./downloaded_files/{message_thread_id}")),
            # 'task' path
            (r"\[(.*?)\]\(./downloaded_files/thread_(.*?)/(.*?)\)",
                lambda match: f"downloaded_files/thread_{match[1]}/{match[2]}"),
            # paths that use /mnt/data
            (r"\[.*?\]\((sandbox:/mnt/data/downloaded_files/.*?)\)",
                lambda match: match.replace("sandbox:/mnt/data", ".")),
            # 'chart' patterns
            (r"\(sandbox:/mnt/data/(.*?)\)\n2\. \[(.*?)\]",
                lambda match: f"downloaded_files/{message_thread_id}/{match}"),
            # when using attachment:
            (r"!\[.*?\]\(attachment://\.(.*?)\)",
                lambda match: match),
            # using thread id + file ## duplicate??
            (r"!\[.*?\]\(\./downloaded_files/thread_(.*?)/(.+?)\)",
                lambda match: f"downloaded_files/thread_{match[0]}/{match[1]}")
        ]

        # match patterns and apply transformations
        for pattern, path_transform in patterns:
            compiled_pattern = re.compile(pattern)
            matches = compiled_pattern.findall(msg)
            for match in matches:
                local_path = path_transform(match)
                local_paths.add(local_path)
        return sorted(local_paths)
