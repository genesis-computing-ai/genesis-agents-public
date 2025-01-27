import unittest
import os
from genesis_bots.api import GenesisAPI, build_server_proxy
from uuid import uuid4
from apps.demos.cli_chat import get_available_bots

RESPONSE_TIMEOUT_SECONDS = 20.0
os.environ['LOG_LEVEL'] = 'WARNING'


class TestTools(unittest.TestCase):

    def setUp(self):
        server_proxy = build_server_proxy('embedded', None)
        self.client = GenesisAPI(server_proxy=server_proxy)
        self.available_bots = get_available_bots(self.client)

    # Returns True if the string contains 4 a.
    def test_list_of_bots(self):
        thread_id = uuid4()
        curr_bot_id = self.available_bots[0]
        request = self.client.submit_message(curr_bot_id, 'List of bots?', thread_id=thread_id)
        response = self.client.get_response(request.bot_id, request.request_id, timeout_seconds=RESPONSE_TIMEOUT_SECONDS)
        print(response)

    def tearDown(self):
        self.client.shutdown()


    # Returns True if the string is in upper case.
if __name__ == '__main__':
    unittest.main()