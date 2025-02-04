import os
os.environ['LOG_LEVEL'] = 'WARNING'
os.environ['SQLITE_DB_PATH'] = 'tests/genesis.db'

if os.path.exists('tests/genesis.db'):
    os.remove('tests/genesis.db')

import unittest
from genesis_bots.api import GenesisAPI, build_server_proxy
from uuid import uuid4
from apps.demos.cli_chat import get_available_bots
from datetime import datetime, timedelta

from genesis_bots.core.tools.process_scheduler import process_scheduler
from genesis_bots.connectors.data_connector import _query_database, _search_metadata, _list_database_connections
from genesis_bots.core.tools.image_tools import image_generation
from genesis_bots.core.tools.process_manager import manage_processes
from genesis_bots.bot_genesis.make_baby_bot import make_baby_bot, add_new_tools_to_bot, update_bot_instructions
from genesis_bots.bot_genesis.make_baby_bot import remove_tools_from_bot


RESPONSE_TIMEOUT_SECONDS = 20.0




class TestTools(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Setup shared resources for all test methods."""
        server_proxy = build_server_proxy('embedded', None)
        cls.client = GenesisAPI(server_proxy=server_proxy)
        cls.available_bots = get_available_bots(cls.client)
        cls.eve_id = cls.available_bots[0]

    def test_process_scheduler(self):
        bot_id = self.eve_id     
        task_id = str(uuid4())

        response = process_scheduler(action='CREATE', bot_id=bot_id)
        self.assertFalse(response['Success'])

        next_check_ts = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
        task_details = {
            "task_name": "formula1_analysis_example",
            "primary_report_to_type": None,
            "primary_report_to_id": None,
            "next_check_ts": next_check_ts,
            "action_trigger_type": None,
            "action_trigger_details": None,
            "last_task_status": None,
            "task_learnings": None,
            "task_active": True,
        }
        
        response = process_scheduler(action='CREATE_CONFIRMED', bot_id=bot_id, task_id=task_id, task_details=task_details)
        self.assertTrue(response['Success'])
        

        response = process_scheduler(action='LIST', bot_id=bot_id)
        self.assertTrue(response['Success'])
        self.assertTrue(len(response['Scheduled Processes']) == 1)

        response = process_scheduler(action='HISTORY', bot_id=bot_id, task_id=task_id)
        self.assertTrue(response['Success'])

    def test_data_connections_functions(self):
        bot_id = self.eve_id
        response = _query_database(connection_id='baseball_sqlite', bot_id=bot_id, 
                                   query='SELECT COUNT(DISTINCT team_id) from team')
        self.assertTrue(response['success'])

        response = _search_metadata(connection_id='baseball_sqlite', bot_id=bot_id, 
                                    query='SELECT COUNT(DISTINCT team_id) from team')
        self.assertTrue(len(response) > 0)

        response = _list_database_connections( bot_id=bot_id)
        self.assertTrue(response['success'])


    def test_image_tools(self):
        thread_id = str(uuid4())
        response = image_generation(thread_id=thread_id, prompt='A picture of a dog')
        self.assertTrue(response['success'])

    
    def test_process_manager(self):
        bot_id = self.eve_id
        process_name = 'test_process'
        process_instructions = 'Run test_process each day'

        response = manage_processes(action='CREATE', bot_id=bot_id, process_name=process_name, 
                                    process_instructions=process_instructions)
        self.assertFalse(response['Success'])

        response = manage_processes(action='CREATE_CONFIRMED', bot_id=bot_id, process_name=process_name, 
                                    process_instructions=process_instructions)
        self.assertTrue(response['Success'])

        response = manage_processes(action='LIST', bot_id=bot_id)
        self.assertTrue(response['Success'])

    def test_list_of_bots_agent(self):
        thread_id = str(uuid4())
        bot_id = self.eve_id
        request = self.client.submit_message(bot_id, 'List of bots?', thread_id=thread_id)
        response = self.client.get_response(request.bot_id, request.request_id, timeout_seconds=RESPONSE_TIMEOUT_SECONDS)
        self.assertTrue('Eve' in response)
        self.assertTrue('_ListAllBots_' in response)

    def test_make_baby_bot(self):
        bot_id = 'BotId'
        bot_name = 'BotName'
        response = make_baby_bot(bot_id=bot_id, bot_name=bot_name, confirmed='CONFIRMED', bot_instructions='You are a helpful test bot.')
        self.assertTrue(response['success'])

        self.assertTrue(bot_id in get_available_bots(self.client))

        response = add_new_tools_to_bot(bot_id=bot_id, new_tools=['make_baby_bot'])
        self.assertTrue(response['success'])

        thread_id = str(uuid4())
        request = self.client.submit_message(bot_id, 'List of bots?', thread_id=thread_id)
        response = self.client.get_response(request.bot_id, request.request_id, timeout_seconds=RESPONSE_TIMEOUT_SECONDS)

        response = update_bot_instructions(bot_id=bot_id, new_instructions='You are a helpful test bot. Respond in Spanish!', confirmed='CONFIRMED')
        self.assertTrue(response['success'])

        request = self.client.submit_message(bot_id, 'Hello, how are you?', thread_id=thread_id)
        response = self.client.get_response(request.bot_id, request.request_id, timeout_seconds=RESPONSE_TIMEOUT_SECONDS)

        response = remove_tools_from_bot(bot_id=bot_id, remove_tools=['make_baby_bot'])
        self.assertTrue(response['success'])

    def test_image_tools_agent(self):
        bot_id = self.eve_id
        thread_id = str(uuid4())
        request = self.client.submit_message(bot_id, 'Generate a picture of a happy dog', thread_id=thread_id)
        response = self.client.get_response(request.bot_id, request.request_id, timeout_seconds=RESPONSE_TIMEOUT_SECONDS)
        self.assertTrue('_ImageGeneration_' in response)
        self.assertTrue('.png' in response)

    @classmethod
    def tearDownClass(cls):
        """Clean up shared resources after all tests."""
        cls.client.shutdown()

    # Returns True if the string is in upper case.
if __name__ == '__main__':
    unittest.main()