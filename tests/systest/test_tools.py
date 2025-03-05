import os
os.environ['LOG_LEVEL'] = 'WARNING'
os.environ['SQLITE_DB_PATH'] = 'tests/genesis.db'

if os.path.exists('tests/genesis.db'):
    os.remove('tests/genesis.db')

SNOWFLAKE = os.getenv("SNOWFLAKE_METADATA", "False").lower() == "true"

if SNOWFLAKE:
    os.environ.update(dict(
        SNOWFLAKE_ACCOUNT_OVERRIDE='eqb52188',
        SNOWFLAKE_DATABASE_OVERRIDE='GENESIS_TEST',
        SNOWFLAKE_WAREHOUSE_OVERRIDE='XSMALL',
        SNOWFLAKE_ROLE_OVERRIDE='PUBLIC',
        SNOWFLAKE_SECURE='FALSE',
        GENESIS_SOURCE='Snowflake',
        GENESIS_INTERNAL_DB_SCHEMA='GENESIS_TEST.UNITTEST_RUNNER',
        GENESIS_LOCAL_RUNNER='TRUE',
        RUNNER_ID='snowflake-1',
        BOT_OS_DEFAULT_LLM_ENGINE='openai'
    ))

import unittest
from genesis_bots.api import GenesisAPI, build_server_proxy
from uuid import uuid4
from datetime import datetime, timedelta
import json

from genesis_bots.core.tools.process_scheduler import process_scheduler
from genesis_bots.connectors.data_connector import _query_database, _search_metadata, _list_database_connections
from genesis_bots.core.tools.image_tools import image_generation
from genesis_bots.core.tools.process_manager import manage_processes
from genesis_bots.bot_genesis.make_baby_bot import make_baby_bot, add_new_tools_to_bot, update_bot_instructions
from genesis_bots.bot_genesis.make_baby_bot import remove_tools_from_bot
from genesis_bots.core.tools.git_action import git_action
from genesis_bots.core.bot_os_web_access import _search_google, _scrape_url
from genesis_bots.core.tools.send_email import send_email
from api_examples.cli_chat import get_available_bots
from genesis_bots.core.tools.google_drive import google_drive

RESPONSE_TIMEOUT_SECONDS = 20.0

def _get_available_bot_ids(client: GenesisAPI) -> list[str]:
    all_bot_configs = client.list_available_bots()
    all_bot_ids = sorted([bot.bot_id for bot in all_bot_configs])
    return all_bot_ids


class TestTools(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Setup shared resources for all test methods."""
        server_proxy = build_server_proxy('embedded')
        cls.db_adapter = server_proxy.genesis_app.db_adapter
        cls.db_adapter.disable_cortex()
        os.environ["BOT_OS_DEFAULT_LLM_ENGINE"] = "openai"

        cls.client = GenesisAPI(server_proxy=server_proxy)
        cls.available_bot_ids = _get_available_bot_ids(cls.client)
        cls.tool_belt = cls.client._server_proxy.genesis_app.sessions[0].tool_belt

        cls.eve_id = cls.available_bot_ids[0]
        for bot_id in cls.available_bot_ids:
            if 'Eve' in bot_id:
                cls.eve_id = bot_id


        project_id = os.getenv('GOOGLE_PROJECT_ID', '')
        private_key_id = os.getenv('GOOGLE_PRIVATE_KEY_ID', '')
        private_key = os.getenv('GOOGLE_PRIVATE_KEY', '')
        client_email = os.getenv('GOOGLE_CLIENT_EMAIL', '')
        client_id = os.getenv('GOOGLE_CLIENT_ID', '')
        shared_folder_id = os.getenv('GOOGLE_SHARED_FOLDER_ID', '')
        key_pairs = {
                    "type": "service_account",
                    "project_id": project_id,
                    "private_key_id": private_key_id,
                    "private_key": private_key.replace('\n','&'),
                    "client_email": client_email,
                    "client_id": client_id,
                    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                    "token_uri": "https://oauth2.googleapis.com/token",
                    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
                    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/genesis-workspace-creds%40" + project_id + ".iam.gserviceaccount.com",
                    "universe_domain": "googleapis.com",
                    "shared_folder_id": shared_folder_id
                }
        cls.db_adapter.set_api_config_params('g-sheets', json.dumps(key_pairs))
        cls.db_adapter.create_google_sheets_creds()

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
        task_id = response['Scheduled Processes'][0]['task_id']

        response = process_scheduler(action='HISTORY', bot_id=bot_id, task_id=task_id)
        self.assertTrue(response['Success'])

        response = process_scheduler(action='DELETE_CONFIRMED', bot_id=bot_id, task_id=task_id)
        self.assertTrue(response['Success'])

        response = process_scheduler(action='LIST', bot_id=bot_id)
        self.assertTrue(response['Success'])
        self.assertTrue(len(response['Scheduled Processes']) == 0)


    def test_data_connections_functions(self):
        bot_id = self.eve_id
        if not SNOWFLAKE:
            response = _query_database(connection_id='baseball_sqlite', bot_id=bot_id,
                                    query='SELECT COUNT(DISTINCT team_id) from team')
            self.assertTrue(response['success'])

        connection_id = 'baseball_sqlite' if not SNOWFLAKE else 'Snowflake'
        response = _search_metadata(connection_id=connection_id, bot_id=bot_id,
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
        process_instructions = f'''
            Run test_process each day where you need to run a query and report the number of rows in harvest_control table using the SQL following query:
                SELECT COUNT(*) FROM {self.db_adapter.harvest_control_table_name}'
        '''

        response = manage_processes(action='CREATE', bot_id=bot_id, process_name=process_name,
                                    process_instructions=process_instructions)
        self.assertFalse(response['Success'])

        response = manage_processes(action='CREATE_CONFIRMED', bot_id=bot_id, process_name=process_name,
                                    process_instructions=process_instructions)
        self.assertTrue(response['Success'])

        response = manage_processes(action='LIST', bot_id=bot_id)
        self.assertTrue(response['Success'])
        self.assertTrue(response['processes'][-1]['process_name'] == 'test_process')
        process_id = response['processes'][-1]['process_id']

        response = self.tool_belt.run_process(action='KICKOFF_PROCESS', process_id=process_id, bot_id=bot_id)
        self.assertTrue(response['Success'])

        response = self.tool_belt.run_process(action='GET_NEXT_STEP', process_id=process_id, bot_id=bot_id)
        self.assertTrue(response['success'])

        response = self.tool_belt.run_process(action='END_PROCESS', process_id=process_id, bot_id=bot_id)
        self.assertTrue(response['success'])

        response = manage_processes(action='DELETE_CONFIRMED', bot_id=bot_id, process_id=process_id)
        self.assertTrue(response['Success'])

    def test_process_manager_agent(self):
        rnd = str(uuid4()).split('-')[0]
        bot_id = self.eve_id
        process_name = 'test_process'
        process_instructions = 'Run test_process each day'
        process_id = f'{bot_id}-{rnd}'

        prompt = f'Create a process named {process_name} with the following instructions: {process_instructions}'
        thread_id = str(uuid4())
        request = self.client.submit_message(bot_id, prompt, thread_id=thread_id)
        response = self.client.get_response(request.bot_id, request.request_id, timeout_seconds=RESPONSE_TIMEOUT_SECONDS)
        print(response)
        self.assertTrue('process' in response)

        prompt = f'Run manage_processes function with the following action: CREATE_CONFIRMED, bot_id: {bot_id}, process_id: {process_id}, process_name: {process_name}, process_instructions: {process_instructions}'
        thread_id = str(uuid4())
        request = self.client.submit_message(bot_id, prompt, thread_id=thread_id)
        response = self.client.get_response(request.bot_id, request.request_id, timeout_seconds=RESPONSE_TIMEOUT_SECONDS)
        self.assertTrue('process' in response)

    def test_list_of_bots_agent(self):
        thread_id = str(uuid4())
        bot_id = self.eve_id
        request = self.client.submit_message(bot_id, 'List of bots?', thread_id=thread_id)
        response = self.client.get_response(request.bot_id, request.request_id, timeout_seconds=RESPONSE_TIMEOUT_SECONDS)
        self.assertTrue('Eve' in response)
        self.assertTrue('_ListAllBots_' in response)

    def test_make_baby_bot(self):
        rnd = str(uuid4()).split('-')[0]
        bot_id = f'BotId-{rnd}'
        bot_name = f'BotName-{rnd}'
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
        response = self.client.get_response(request.bot_id, request.request_id, timeout_seconds=40)
        print(response)
        self.assertTrue('_ImageGeneration_' in response)
        self.assertTrue('.png' in response)

    @unittest.skipIf(not SNOWFLAKE, "Skipping test_snowflake_tools on Sqlite")
    def test_snowflake_tools(self):
        bot_id = self.eve_id
        thread_id = str(uuid4())
        code = '# Calculate the sum\nresult_sum = 10 + 20\n\n# Printing the result\nresult = result_sum'
        purpose = 'Generate and plot y = x for x from 0 to 10 using Snowpark'
        packages = ''
        response = self.db_adapter.run_python_code(purpose=purpose, packages=packages, bot_id=bot_id, code=code, thread_id=thread_id)
        self.assertTrue(response == 30)

    @unittest.skipIf(not SNOWFLAKE, "Skipping test_send_email on Sqlite")
    def test_send_email(self):
        bot_id = self.eve_id
        thread_id = str(uuid4())
        response = send_email(to_addr_list='reza.vaghefi@genesiscomputing.ai', subject='Unittest',
                   body='Test', bot_id=bot_id, thread_id=thread_id, save_as_artifact=False)
        self.assertTrue(response['Success'])

    def test_git_action(self):
        bot_id = self.eve_id
        thread_id = str(uuid4())
        response = git_action(action='list_files', thread_id=thread_id, bot_id=bot_id)
        self.assertTrue(response['success'])

        response = git_action(action='get_branch', thread_id=thread_id, bot_id=bot_id)
        self.assertTrue(response['success'])

        response = git_action(action='get_status', thread_id=thread_id, bot_id=bot_id)
        self.assertTrue(response['success'])

        response = git_action(action='get_history', thread_id=thread_id, bot_id=bot_id)
        self.assertTrue(response['success'])

        response = git_action(action='read_file', thread_id=thread_id, bot_id=bot_id, file_path='README.md')
        self.assertTrue(response['success'])

        content = "This is the content of the file"
        response = git_action(action='write_file', thread_id=thread_id, bot_id=bot_id, file_path='test.txt', content=content)
        self.assertTrue(response['success'])

        response = git_action(action='list_files', thread_id=thread_id, bot_id=bot_id)
        self.assertTrue(response['success'])
        self.assertTrue('test.txt' in response['files'])

    def test_web_acces_tools(self):
        bot_id = self.eve_id
        thread_id = str(uuid4())
        response = _search_google(query='What is the current bitcoin price?', search_type='search', bot_id=bot_id, thread_id=thread_id)
        self.assertTrue(response['success'], str(response))
        self.assertTrue('Dollar' in response['data']['answerBox']['answer'])

        response = _search_google(query='Where is Apple HD in CA?', search_type='places', bot_id=bot_id, thread_id=thread_id)
        self.assertTrue(response['success'])
        self.assertTrue('CA' in response['data']['places'][0]['address'])

        response = _scrape_url(url='https://en.wikipedia.org/wiki/IEEE_Transactions_on_Pattern_Analysis_and_Machine_Intelligence')
        self.assertTrue(response['success'])
        self.assertTrue('Impact' in response['data']['text'], str(response))

    def test_web_acces_tools_agent(self):
        bot_id = self.eve_id
        thread_id = str(uuid4())
        request = self.client.submit_message(bot_id, 'Use google search to find the current bitcoin price', thread_id=thread_id)
        response = self.client.get_response(request.bot_id, request.request_id, timeout_seconds=RESPONSE_TIMEOUT_SECONDS)
        self.assertTrue('_SearchGoogle_' in response)
        # self.assertTrue('Dollars' in response or '$' in response) # it is flaky

    # @unittest.skipIf(True, "Skipping test_google_drive_tools for now")
    def test_google_drive_tools(self):
        thread_id = str(uuid4())
        g_folder_id = '1-o_QvvejVllkz0XZeRYRl6-KcZGQYeub'

        response = google_drive(action="SAVE_QUERY_RESULTS_TO_G_SHEET", g_sheet_query='SELECT * FROM HARVEST_CONTROL',
                                 thread_id=thread_id)
        self.assertTrue(response['Success'])
        file_id = response['file_id']

        response = google_drive(action="LIST", g_folder_id=g_folder_id)
        self.assertTrue(response['Success'])
        print(response)
        filename = response['files']['Files'][0]['name']

        response = google_drive(action="GET_LINK_FROM_FILE_ID", g_file_id=file_id)
        self.assertTrue(response['Success'])

        response = google_drive(action="GET_FILE_VERSION_NUM", g_file_id=file_id)
        self.assertTrue(response['Success'])

        response = google_drive(action="GET_SHEET", g_file_id=file_id, g_sheet_cell='A1',
                                 thread_id=thread_id)
        self.assertTrue(response['Success'])

        response = google_drive(action="GET_FILE_BY_NAME", g_file_name=filename)
        self.assertTrue(response['Success'])

        response = google_drive(action="ADD_COMMENT", g_file_id=file_id, g_sheet_values='Test Comment')
        self.assertTrue(response['Success'])

        response = google_drive(action="GET_COMMENTS", g_file_id=file_id)
        self.assertTrue(response['Success'])

        response = google_drive(action="DELETE_SHEET", g_file_id=file_id)
        self.assertTrue(response['Success'])



    @classmethod
    def tearDownClass(cls):
        """Clean up shared resources after all tests."""
        if SNOWFLAKE:
            response = cls.db_adapter.run_query(f'SHOW TABLES IN {cls.db_adapter.schema};')
            tables = [row['NAME'] for row in response]
            for table in tables:
                query = f'DROP TABLE IF EXISTS {cls.db_adapter.schema}.{table} CASCADE;'
                cls.db_adapter.run_query(query)
        cls.client.shutdown()


    # Returns True if the string is in upper case.
if __name__ == '__main__':
    unittest.main()
