# tests/unittests/core/test_bot_os_util.py
import unittest
from core.bot_os_utils import tupleize 

class TestTupleize(unittest.TestCase):
    
    def test_tupleize_with_single_element(self):
        result = tupleize('element')
        self.assertEqual(result, ('element',))
    
    def test_tupleize_with_multiple_elements(self):
        result = tupleize(['element1', 'element2'])
        self.assertEqual(result, ('element1', 'element2'))
    
    def test_tupleize_with_empty_input(self):
        result = tupleize([])
        self.assertEqual(result, ())
        result = tupleize()
        self.assertEqual(result, ())
    
    def test_tupleize_with_none(self):
        result = tupleize(None)
        self.assertEqual(result, (None,))
    
    # Add more test cases as needed

