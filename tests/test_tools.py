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
# from genesis_bots.connectors.data_connector import _query_database
from genesis_bots.core.tools.process_scheduler import process_scheduler

RESPONSE_TIMEOUT_SECONDS = 20.0




class TestTools(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Setup shared resources for all test methods."""
        server_proxy = build_server_proxy('embedded', None)
        cls.client = GenesisAPI(server_proxy=server_proxy)
        cls.available_bots = get_available_bots(cls.client)

    def test_process_scheduler(self):
        bot_id = self.available_bots[0]        
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

        response = process_scheduler(action='HISTORY', bot_id=bot_id, task_id=task_id)
        print(response)
        self.assertTrue(response['Success'])

    def test_list_of_bots(self):
        thread_id = str(uuid4())
        curr_bot_id = self.available_bots[0]
        request = self.client.submit_message(curr_bot_id, 'List of bots?', thread_id=thread_id)
        response = self.client.get_response(request.bot_id, request.request_id, timeout_seconds=RESPONSE_TIMEOUT_SECONDS)
        print(response)

    @classmethod
    def tearDownClass(cls):
        """Clean up shared resources after all tests."""
        cls.client.shutdown()

    # Returns True if the string is in upper case.
if __name__ == '__main__':
    unittest.main()