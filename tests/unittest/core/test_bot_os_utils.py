# tests/unittests/core/test_bot_os_util.py
import unittest
from genesis_bots.core.bot_os_utils import *

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


class TestTruncateString(unittest.TestCase):

    def test_truncate_string_no_truncation(self):
        result = truncate_string("Hello, World!", 20)
        self.assertEqual(result, "Hello, World!")

    def test_truncate_string_with_truncation(self):
        result = truncate_string("Hello, World!", 11)
        self.assertEqual(result, "Hello, W...")

    def test_truncate_string_with_custom_truncation_string(self):
        result = truncate_string("Hello, World!", 11, "--")
        self.assertEqual(result, "Hello, Wo--")

    def test_truncate_string_exact_length(self):
        result = truncate_string("Hello, World!", 13)
        self.assertEqual(result, "Hello, World!")

    def test_truncate_string_empty_string(self):
        result = truncate_string("", 5)
        self.assertEqual(result, "")

    def test_truncate_string_truncation_string_longer_than_max_length(self):
        result = truncate_string("Hello", 3, "...")
        self.assertEqual(result, "...")

        result = truncate_string("aaa", 2, "...")
        self.assertEqual(result, "..")

