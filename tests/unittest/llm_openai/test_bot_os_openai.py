'''
Test BotOsAssistantOpenAI
'''

#TODO: figure out why export GENESIS_SOURCE=SQLITE does not work
#TODO: how prevent slack adapter from starting

import os, shutil, tempfile
tmp_dir = tempfile.mkdtemp(prefix='test_openai_')
os.chdir(tmp_dir)
print(f'created temp folder for test: {tmp_dir}')

import sys, unittest, json
from uuid import uuid4
from datetime import datetime, timedelta
from types import SimpleNamespace
import genesis_bots.llm.llm_openai.bot_os_openai
from genesis_bots.core.bot_os_input import BotOsInputMessage, BotOsOutputMessage
from genesis_bots.connectors import get_global_db_connector
from genesis_bots.core.bot_os_tools2 import gc_tool, ToolFuncGroup

def module_cleanup():
    shutil.rmtree(tmp_dir)
    print(f'{os.linesep}deleted {tmp_dir}')

unittest.addModuleCleanup(module_cleanup)

# OpenAI API mocks
class Completions:
    def register_create_mock(self, create_mock):
        self.create_mock = create_mock
        
    def create(self, *, messages, model, stream, stream_options, tools=None):
        return self.create_mock(messages=messages, model=model, stream=stream,
                                stream_options=stream_options, tools=tools);

completions = Completions()

class Client:
    def __init__(self):
        self.chat = SimpleNamespace(completions=completions)

def init():
    '''
    One time initialize overall env for the tests.
    we do not have BotOsSession/Server so handle config directly
    create all the necessary tables, set stream_mode and override OpenAI client 
    '''
    genesis_bots.llm.llm_openai.bot_os_openai.get_openai_client = lambda : Client()
    genesis_bots.llm.llm_openai.bot_os_openai.BotOsAssistantOpenAI.stream_mode = True
    get_global_db_connector().ensure_table_exists()

def make_assistant(name='bot_name', instr='bot_instructions') -> genesis_bots.llm.llm_openai.bot_os_openai.BotOsAssistantOpenAI:
    return genesis_bots.llm.llm_openai.bot_os_openai.BotOsAssistantOpenAI(name, instr,
                                                                          log_db_connector=get_global_db_connector())

def make_input_message(thread_id, msg) -> BotOsInputMessage:
    thread_id = thread_id
    files = None
    metadata = {}
    msg_type = 'chat_input'
    return BotOsInputMessage(thread_id, msg, files, metadata, msg_type)

def round_trip(msg:str, completions_create, event_callback=None, assistant=None) -> str:
    '''excercise adapter cycle: add_message() followed by check_runs()'''

    assistant = assistant or make_assistant()
    thread_id = f'[thread_{uuid4()}]'
    completions.register_create_mock(completions_create)
    assistant.add_message(make_input_message(thread_id, msg))
    
    response = 'done'
    def default_event_callback(session_id, output_message: BotOsOutputMessage):
        nonlocal response
        response = output_message.output

    for i in range(10):
        assistant.check_runs(event_callback or default_event_callback)
    return response, thread_id

'''OpenAI chat completions streaming response chunks'''
def null_chunk(id):
    return SimpleNamespace(
        id=id,
        usage=None,
        choices=[
            SimpleNamespace(
                delta=SimpleNamespace(
                    content=None, 
                    function_call=None,
                    refusal=None,
                    role='assistant',
                    tool_calls=None),
                finish_reason=None,
                index=0,
                logprobs=None)
        ]
    )

def function_name_chunk(id, index, name, call_id ):
    return SimpleNamespace(
        id=id,
        usage=None,
        choices=[
            SimpleNamespace(
                delta=SimpleNamespace(
                    content=None, 
                    function_call=None,
                    refusal=None,
                    role='assistant',
                    tool_calls=[
                        SimpleNamespace(
                            index=index,
                            id=call_id,
                            type='function',
                            function=SimpleNamespace(arguments='', name=name)
                        )]
                ),
                finish_reason=None,
                index=0,
                logprobs=None
            )]
    )

def function_arguments_chunk(id, index, arguments):
    return SimpleNamespace(
        id=id,
        usage=None,
        choices=[
            SimpleNamespace(
                delta=SimpleNamespace(
                    content=None, 
                    function_call=None,
                    refusal=None,
                    role=None,
                    tool_calls=[
                        SimpleNamespace(
                            index=index,
                            id=None,
                            type=None,
                            function=SimpleNamespace(arguments=arguments, name=None)
                        )]
                ),
                finish_reason=None,
                index=0,
                logprobs=None
            )]
    )

def finish_reason_chunk(id, finish_reason='tool_calls'):
    return SimpleNamespace(
        id=id,
        usage=None,
        choices=[
            SimpleNamespace(
                delta=SimpleNamespace(
                    content=None, 
                    function_call=None,
                    refusal=None,
                    role=None,
                    tool_calls=None),
                finish_reason=finish_reason,
                index=0,
                logprobs=None)
        ]
    )
    
def usage_chunk(id):
    return SimpleNamespace(
        id=id,
        choices=[],
        usage=SimpleNamespace(
            completion_tokens=81,
            prompt_tokens=85,
            total_tokens=166,
            completion_tokens_details=SimpleNamespace(
                accepted_prediction_tokens=0,
                audio_tokens=0,
                reasoning_tokens=0,
                rejected_prediction_tokens=0),
            prompt_tokens_details=SimpleNamespace(audio_tokens=0, cached_tokens=0)
        ))

def content_chunk(id, content):
    return SimpleNamespace(
        id=id,
        usage=None,
        choices=[
            SimpleNamespace(
                delta=SimpleNamespace(content=content)
            )
        ]
    )

def query_message_log(thread_id):
    db_connector=get_global_db_connector()
    cutoff = (datetime.now() - timedelta(minutes=10)).strftime("%Y-%m-%d %H:%M:%S")
    return db_connector.query_timestamp_message_log(thread_id, cutoff)
    
class TestOpenAIAdapter(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        init()

    def test_ping_pong(self):
        '''prompt OpenAI with 'ping' and receive 'pong' in response'''

        def openai_mock(*, messages, model, stream, stream_options, tools=None):
            self.assertEqual(len(messages), 2)
            self.assertEqual(next(x['content'] for x in messages if x['role'] == 'system'), 'bot_instructions')
            self.assertEqual(next(x['content'] for x in messages if x['role'] == 'user'), 'ping')
            id = f'chatcmpl-{uuid4()}'
            return [
                null_chunk(id),
                content_chunk(id, 'pong'),
                finish_reason_chunk(id, 'stop'),
                usage_chunk(id)
            ]

        response, thread_id = round_trip('ping', openai_mock)
        self.assertEqual(response, 'pong')

        logs = query_message_log(thread_id)
        self.assertEqual(len(logs), 2)
        self.assertTrue(logs[0]['MESSAGE_TYPE'] == 'User Prompt' and logs[0]['MESSAGE_PAYLOAD'] == 'ping')
        self.assertTrue(logs[1]['MESSAGE_TYPE'] == 'Assistant Response' and logs[1]['MESSAGE_PAYLOAD'] == 'pong')

    def test_tool_call(self):
        '''simulate call to OpenAI with a tool invocation'''

        exception = None
        def captureException(e):
            nonlocal exception
            if exception == None:
                exception = e
            
        gr1_tag = ToolFuncGroup("group1", "this is group 1")
        @gc_tool(_group_tags_=[gr1_tag], x="this is param x")
        def tool_function(x: int):
            "this is the sample_function description"
            try:
                self.assertEqual(x, 69)
                return x * 2
            except Exception as e:
                captureException(e)
                raise

        assistant = make_assistant()
        assistant.all_functions['tool_function'] = tool_function
        assistant.tools = [tool_function.gc_tool_descriptor.to_llm_description_dict()]

        call_count = 0
        call_id = f'call_{uuid4()}'
        def openai_mock(*, messages, model, stream, stream_options, tools=None):
            try:
                nonlocal call_count
                nonlocal call_id
                call_count += 1
                id = f'chatcmpl-{uuid4()}'
                self.assertEqual(tools, [
                    {'type': 'function', 'function':
                     {'name': 'tool_function',
                      'description': 'this is the sample_function description',
                      'parameters': {
                          'type': 'object',
                          'properties': {
                              'x': {'type': 'integer', 'description': 'this is param x'}},
                          'required': ['x']}}}])
                if call_count == 1:
                    self.assertEqual(messages, [
                        {'role': 'system', 'content': 'bot_instructions'},
                        {'role': 'user', 'content': 'Hello! Please call my function'}])
                    return [
                        null_chunk(id),
                        function_name_chunk(id, 0, 'tool_function', call_id),
                        function_arguments_chunk(id, 0, json.dumps({'x': 69})),
                        finish_reason_chunk(id, 'tool_calls'),
                        usage_chunk(id)
                    ]
                else:
                    self.assertEqual(messages, [
                        {'role': 'system', 'content': 'bot_instructions'},
                        {'role': 'user', 'content': 'Hello! Please call my function'},
                        {'role': 'assistant', 'content': None, 'tool_calls': [
                            {'id': call_id, 'type': 'function',
                             'function': {'name': 'tool_function', 'arguments': '{"x": 69}'}}]},
                        {'role': 'tool', 'tool_call_id': call_id, 'content': '138'}])
                    return [
                        null_chunk(id),
                        content_chunk(id, 'all good'),
                        finish_reason_chunk(id, 'stop'),
                        usage_chunk(id)
                    ]
            except Exception as e:
                captureException(e)
                raise
            
        event_count = 0
        def event_callback(session_id, output_message: BotOsOutputMessage):
            try:
                nonlocal event_count
                event_count += 1
                self.assertTrue('using tool:' in output_message.output.lower())
                self.assertTrue('toolfunction' in output_message.output.lower())
                self.assertTrue('all good' in  output_message.output.lower() if event_count > 1 else True)
            except Exception as e:
                captureException(e)
                raise
            
        _, thread_id = round_trip('Hello! Please call my function', openai_mock, event_callback, assistant)
        
        logs = query_message_log(thread_id)
        self.assertEqual(len(logs), 5)
        self.assertTrue(logs[0]['MESSAGE_TYPE'] == 'User Prompt' and logs[0]['MESSAGE_PAYLOAD'] == 'Hello! Please call my function')
        self.assertTrue(logs[1]['MESSAGE_TYPE'] == 'Tool Call' and logs[1]['MESSAGE_PAYLOAD'] == 'tool_function({"x": 69})')
        self.assertTrue(logs[2]['MESSAGE_TYPE'] == 'User Prompt' and logs[2]['MESSAGE_PAYLOAD'] == 'Tool call completed, results')
        self.assertTrue(logs[3]['MESSAGE_TYPE'] == 'Tool Output' and logs[3]['MESSAGE_PAYLOAD'] == 138)
        self.assertTrue(logs[4]['MESSAGE_TYPE'] == 'Assistant Response' and
                        'Using tool: _ToolFunction_...\n\nall good' in logs[4]['MESSAGE_PAYLOAD'])
        
        if exception:
            raise exception

    def test_tool_call2(self):
        '''simulate call to OpenAI with 2 tools'''

        exception = None
        def captureException(e):
            nonlocal exception
            if exception == None:
                exception = e
            
        gr1_tag = ToolFuncGroup("group1", "this is group 1")
        @gc_tool(_group_tags_=[gr1_tag], x="this is an int param x")
        def mul2(x: int):
            "double the argument"
            try:
                return x * 2
            except Exception as e:
                captureException(e)
                raise

        @gc_tool(_group_tags_=[gr1_tag], s="this is a string param s")
        def conc(s: str):
            "concatenate"
            try:
                return {'answer': f'{s}+{s}'}
            except Exception as e:
                captureException(e)
                raise

        assistant = make_assistant()
        assistant.all_functions['mul2'] = mul2
        assistant.all_functions['conc'] = conc
        assistant.tools = [mul2.gc_tool_descriptor.to_llm_description_dict(),
                           conc.gc_tool_descriptor.to_llm_description_dict()]

        call_count = 0
        call_id = [f'call_{uuid4()}', f'call_{uuid4()}']
        def openai_mock(*, messages, model, stream, stream_options, tools=None):
            try:
                nonlocal call_count
                nonlocal call_id
                call_count += 1
                id = f'chatcmpl-{uuid4()}'
                self.assertEqual(tools, [
                    {'type': 'function', 'function': {
                        'name': 'mul2', 'description': 'double the argument',
                        'parameters': {
                            'type': 'object',
                            'properties': {
                                'x': {'type': 'integer', 'description': 'this is an int param x'}},
                            'required': ['x']}}},
                    {'type': 'function', 'function': {
                        'name': 'conc', 'description': 'concatenate',
                        'parameters': {
                            'type': 'object',
                            'properties': {
                                's': {'type': 'string', 'description': 'this is a string param s'}},
                            'required': ['s']}}}])
                if call_count == 1:
                    self.assertEqual(messages, [
                        {'role': 'system', 'content': 'bot_instructions'},
                        {'role': 'user', 'content': 'Call my two functions'}])
                    return [
                        null_chunk(id),
                        function_name_chunk(id, 0, 'mul2', call_id[0]),
                        function_arguments_chunk(id, 0, json.dumps({'x': 47})),
                        function_name_chunk(id, 1, 'conc', call_id[1]),
                        function_arguments_chunk(id, 1, json.dumps({'s': 'adapt'})),
                        finish_reason_chunk(id, 'tool_calls'),
                        usage_chunk(id)
                    ]
                else:
                    self.assertEqual(messages, [
                        {'role': 'system', 'content': 'bot_instructions'},
                        {'role': 'user', 'content': 'Call my two functions'},
                        {'role': 'assistant', 'content': None, 'tool_calls': [
                            {'id': call_id[0], 'type': 'function', 'function': {
                                'name': 'mul2', 'arguments': '{"x": 47}'}},
                            {'id': call_id[1], 'type': 'function', 'function': {
                                'name': 'conc', 'arguments': '{"s": "adapt"}'}}]},
                        {'role': 'tool', 'tool_call_id': call_id[0], 'content': '94'},
                        {'role': 'tool', 'tool_call_id': call_id[1], 'content': "{'answer': 'adapt+adapt'}"}])
                    return [
                        null_chunk(id),
                        content_chunk(id, 'done'),
                        finish_reason_chunk(id, 'stop'),
                        usage_chunk(id)
                    ]
            except Exception as e:
                captureException(e)
                raise
            
        event_count = 0
        def event_callback(session_id, output_message: BotOsOutputMessage):
            try:
                nonlocal event_count
                event_count += 1
                self.assertTrue('using tool:' in output_message.output.lower())
                self.assertTrue('mul2' in output_message.output.lower())
                self.assertTrue('conc' in output_message.output.lower() if event_count > 1 else True)
                self.assertTrue('done' in output_message.output.lower() if event_count == 3 else True)
                
            except Exception as e:
                captureException(e)
                raise
            
        _, thread_id = round_trip('Call my two functions', openai_mock, event_callback, assistant)
        
        logs = query_message_log(thread_id)
        self.assertEqual(len(logs), 7)        
        self.assertTrue(logs[0]['MESSAGE_TYPE'] == 'User Prompt' and logs[0]['MESSAGE_PAYLOAD'] == 'Call my two functions')
        self.assertTrue(logs[1]['MESSAGE_TYPE'] == 'Tool Call' and logs[1]['MESSAGE_PAYLOAD'] == 'mul2({"x": 47})')
        self.assertTrue(logs[2]['MESSAGE_TYPE'] == 'Tool Call' and logs[2]['MESSAGE_PAYLOAD'] == 'conc({"s": "adapt"})')
        self.assertTrue(logs[3]['MESSAGE_TYPE'] == 'User Prompt' and logs[3]['MESSAGE_PAYLOAD'] == 'Tool call completed, results')
        self.assertTrue(logs[4]['MESSAGE_TYPE'] == 'Tool Output' and logs[4]['MESSAGE_PAYLOAD'] == 94)
        self.assertTrue(logs[5]['MESSAGE_TYPE'] == 'Tool Output' and logs[5]['MESSAGE_PAYLOAD'] == "{'answer': 'adapt+adapt'}")
        self.assertTrue(logs[6]['MESSAGE_TYPE'] == 'Assistant Response' and 'Using tool: _Mul2_' in logs[6]['MESSAGE_PAYLOAD'] and
                        'Using tool: _Conc_...\n\ndone' in logs[6]['MESSAGE_PAYLOAD'])

        if exception:
            raise exception

    def test_tool_calls_seq(self):
        '''simulate sequence of tool calls, i.e. multiple run steps'''

        exception = None
        def captureException(e):
            nonlocal exception
            if exception == None:
                exception = e

        gr1_tag = ToolFuncGroup("group1", "this is group 1")
        @gc_tool(_group_tags_=[gr1_tag], x="this is param x")
        def mul2(x: int):
            "double the argument"
            try:
                return x * 2
            except Exception as e:
                captureException(e)
                raise

        assistant = make_assistant()
        assistant.all_functions['mul2'] = mul2
        assistant.tools = [mul2.gc_tool_descriptor.to_llm_description_dict()]

        call_count = 0
        call_id = []
        def openai_mock(*, messages, model, stream, stream_options, tools=None):
            try:
                nonlocal call_count
                nonlocal call_id
                call_count += 1
                call_id.append(f'call_{uuid4()}')
                id = f'chatcmpl-{call_count}'
                if call_count == 1:
                    self.assertEqual(messages, [
                        {'role': 'system', 'content': 'bot_instructions'},
                        {'role': 'user', 'content': 'Let us start the loop!'}])
                    return [
                        null_chunk(id),
                        function_name_chunk(id, 0, 'mul2', call_id[-1]),
                        function_arguments_chunk(id, 0, json.dumps({'x': call_count})),
                        finish_reason_chunk(id, 'tool_calls'),
                        usage_chunk(id)
                    ]
                elif call_count <= 3:
                    if call_count == 2:
                        self.assertEqual(messages,
                                         [{'role': 'system', 'content': 'bot_instructions'},
                                          {'role': 'user', 'content': 'Let us start the loop!'},
                                          {'role': 'assistant', 'content': None, 'tool_calls': [
                                              {'id': call_id[0], 'type': 'function',
                                               'function': {'name': 'mul2', 'arguments': '{"x": 1}'}}]},
                                          {'role': 'tool', 'tool_call_id': call_id[0], 'content': '2'}])
                        pass
                    if call_count == 3:
                        self.assertEqual(messages,
                                         [{'role': 'system', 'content': 'bot_instructions'},
                                          {'role': 'user', 'content': 'Let us start the loop!'},

                                          {'role': 'assistant', 'content': None, 'tool_calls': [
                                              {'id': call_id[0], 'type': 'function',
                                               'function': {'name': 'mul2', 'arguments': '{"x": 1}'}}]},
                                          {'role': 'tool', 'tool_call_id': call_id[0], 'content': '2'},

                                          {'role': 'assistant', 'content': None, 'tool_calls': [
                                              {'id': call_id[1], 'type': 'function',
                                               'function': {'name': 'mul2', 'arguments': '{"x": 2}'}}]},
                                          {'role': 'tool', 'tool_call_id': call_id[1], 'content': '4'}])
                        pass
                    return [
                        null_chunk(id),
                        function_name_chunk(id, 0, 'mul2', call_id[-1]),
                        function_arguments_chunk(id, 0, json.dumps({'x': call_count})),
                        finish_reason_chunk(id, 'tool_calls'),
                        usage_chunk(id)
                    ]
                else:
                    self.assertEqual(call_count, 4)
                    self.assertEqual(messages,
                                     [{'role': 'system', 'content': 'bot_instructions'},
                                      {'role': 'user', 'content': 'Let us start the loop!'},
                                      {'role': 'assistant', 'content': None, 'tool_calls': [
                                          {'id': call_id[0], 'type': 'function',
                                           'function': {'name': 'mul2', 'arguments': '{"x": 1}'}}]},
                                      {'role': 'tool', 'tool_call_id': call_id[0], 'content': '2'},

                                      {'role': 'assistant', 'content': None, 'tool_calls': [
                                          {'id': call_id[1], 'type': 'function',
                                           'function': {'name': 'mul2', 'arguments': '{"x": 2}'}}]},
                                      {'role': 'tool', 'tool_call_id': call_id[1], 'content': '4'},

                                      {'role': 'assistant', 'content': None, 'tool_calls': [
                                          {'id': call_id[2], 'type': 'function',
                                           'function': {'name': 'mul2', 'arguments': '{"x": 3}'}}]},
                                      {'role': 'tool', 'tool_call_id': call_id[2], 'content': '6'}])
                    return [
                        null_chunk(id),
                        content_chunk(id, 'all good'),
                        finish_reason_chunk(id, 'stop'),
                        usage_chunk(id)
                    ]
            except Exception as e:
                captureException(e)
                raise

        event_count = 0
        def event_callback(session_id, output_message: BotOsOutputMessage):
            try:
                nonlocal event_count
                event_count += 1
                self.assertTrue('using tool:' in output_message.output.lower())
                self.assertTrue('mul2' in output_message.output.lower())
                self.assertTrue('all good' in  output_message.output.lower() if event_count == 6 else True)
            except Exception as e:
                captureException(e)
                raise

        _, thread_id = round_trip('Let us start the loop!', openai_mock, event_callback, assistant)

        logs = query_message_log(thread_id)
        self.assertEqual(len(logs), 11)
        self.assertTrue(logs[0]['MESSAGE_TYPE'] == 'User Prompt' and logs[0]['MESSAGE_PAYLOAD'] == 'Let us start the loop!')
        self.assertTrue(logs[1]['MESSAGE_TYPE'] == 'Tool Call' and logs[1]['MESSAGE_PAYLOAD'] == 'mul2({"x": 1})')
        self.assertTrue(logs[2]['MESSAGE_TYPE'] == 'User Prompt' and logs[2]['MESSAGE_PAYLOAD'] == 'Tool call completed, results')
        self.assertTrue(logs[3]['MESSAGE_TYPE'] == 'Tool Output' and logs[3]['MESSAGE_PAYLOAD'] == 2)
        self.assertTrue(logs[4]['MESSAGE_TYPE'] == 'Tool Call' and logs[4]['MESSAGE_PAYLOAD'] == 'mul2({"x": 2})')
        self.assertTrue(logs[5]['MESSAGE_TYPE'] == 'User Prompt' and logs[5]['MESSAGE_PAYLOAD'] == 'Tool call completed, results')
        self.assertTrue(logs[6]['MESSAGE_TYPE'] == 'Tool Output' and logs[6]['MESSAGE_PAYLOAD'] == 4)
        self.assertTrue(logs[7]['MESSAGE_TYPE'] == 'Tool Call' and logs[7]['MESSAGE_PAYLOAD'] == 'mul2({"x": 3})')
        self.assertTrue(logs[8]['MESSAGE_TYPE'] == 'User Prompt' and logs[8]['MESSAGE_PAYLOAD'] == 'Tool call completed, results')
        self.assertTrue(logs[9]['MESSAGE_TYPE'] == 'Tool Output' and logs[9]['MESSAGE_PAYLOAD'] == 6)
        self.assertTrue(logs[10]['MESSAGE_TYPE'] == 'Assistant Response' and
                        'Using tool: _Mul2_...\n\nall good' in logs[10]['MESSAGE_PAYLOAD'])

        if exception:
            raise exception

if __name__ == '__main__':
    unittest.main()
