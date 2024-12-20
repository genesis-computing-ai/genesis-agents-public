import unittest
from core.bot_os_tools2 import *

class TestGCTools(unittest.TestCase):

    def test_gc_tool(self):
        gr1_tag = ToolFuncGroup("group1", "this is group 1")
        gr2_tag = ToolFuncGroup("group2", "this is group 2")

        # Test gc_tool decorator with a multiple group tag    
        @gc_tool(_group_tags_=[gr1_tag, gr2_tag], x="this is param x")
        def sample_function(x: int):
            "this is the sample_function description"
            return x * 2

        # Test that the decorator does not alter the function's output
        self.assertEqual(sample_function(2), 4)
        self.assertEqual(sample_function(5), 10)

        # Test gc_tool_descriptor properties
        descriptor = sample_function.gc_tool_descriptor
        self.assertEqual(descriptor.name, "sample_function")
        self.assertEqual(descriptor.description, "this is the sample_function description")
        self.assertIn("x", [param.name for param in descriptor.parameters_desc])
        param_x = next(param for param in descriptor.parameters_desc if param.name == "x")
        self.assertEqual(param_x.description, "this is param x")
        self.assertEqual(param_x.param_type, "int")
        self.assertEqual(descriptor.groups, (gr1_tag, gr2_tag))

        # Test gc_tool_descriptor to_llm_description_dict() method
        expected_dict = {
            "type": "function",
            "function": {
                "name": "sample_function",
                "description": "this is the sample_function description",
                "parameters": {
                    "properties": {
                        "x": {
                            "description": "this is param x",
                            "type": "int"
                        }
                    },
                    "type": "object",
                    "required": ["x"]
                },
            }
        }
        self.assertEqual(descriptor.to_llm_description_dict(), expected_dict)

        
    def test_tool_funcs_with_lifecycle(self):
        registry = ToolsFuncRegistry()
        gr1_tag = ToolFuncGroup("gr1", "this is group 1", lifetime=ToolFuncGroupLifetime.PERSISTENT)
        gr2_tag = ToolFuncGroup("gr2", "this is group 2", lifetime=ToolFuncGroupLifetime.EPHEMERAL)

        # Test adding a tool function with persistent group
        @gc_tool(_group_tags_=[gr1_tag], a="this is param a")
        def persistent_function(a: int):
            "this is persistent_function description"
            return a + 1

        registry.add_tool_func(persistent_function)
        self.assertIn("persistent_function", registry.list_tool_func_names())

        # Test adding a tool function with ephemeral group
        @gc_tool(_group_tags_=[gr2_tag], b="this is param b")
        def ephemeral_function(b: int):
            "this is ephemeral_function description"
            return b * 2

        registry.add_tool_func(ephemeral_function)
        self.assertIn("ephemeral_function", registry.list_tool_func_names())

        # Test that functions with different lifetimes cannot be in the same group
        with self.assertRaises(ValueError):
            @gc_tool(_group_tags_=[gr1_tag, gr2_tag], c="this is param c")
            def mixed_lifetime_function(c: int):
                "this is mixed_lifetime_function description"
                return c - 1

        # Test retrieving tool functions by group
        persistent_funcs = registry.get_tool_funcs_by_group("gr1")
        self.assertEqual(len(persistent_funcs), 1)
        self.assertEqual(persistent_funcs[0], persistent_function)

        ephemeral_funcs = registry.get_tool_funcs_by_group("gr2")
        self.assertEqual(len(ephemeral_funcs), 1)
        self.assertEqual(ephemeral_funcs[0], ephemeral_function)

        # Test removing tool functions
        registry.remove_tool_func(persistent_function)
        self.assertNotIn("persistent_function", registry.list_tool_func_names())

        registry.remove_tool_func(ephemeral_function)
        self.assertNotIn("ephemeral_function", registry.list_tool_func_names())

        
    def test_tools_func_registry(self):
        registry = ToolsFuncRegistry()
        gr1_tag = ToolFuncGroup("gr1", "this is group 1")
        
        # Test adding a tool function
        @gc_tool(_group_tags_=[gr1_tag], y="this is param y")
        def another_function(y: int):
            "this is another_function description"
            return y + 3

        registry.add_tool_func(another_function)
        self.assertIn("another_function", registry.list_tool_func_names())

        # Test retrieving a tool function
        retrieved_func = registry.get_tool_func("another_function")
        self.assertEqual(retrieved_func, another_function)
        self.assertEqual(len(registry.list_tool_funcs()), 1)

        # Test removing a tool function by name
        removed_func = registry.remove_tool_func("another_function")
        self.assertEqual(removed_func, another_function)
        self.assertNotIn("another_function", registry.list_tool_func_names())

        # Test removing a tool function by function object
        registry.add_tool_func(another_function)
        removed_func = registry.remove_tool_func(another_function)
        self.assertEqual(removed_func, another_function)
        self.assertNotIn("another_function", registry.list_tool_func_names())
        # Assert the expected number of registered functions
        self.assertEqual(len(registry.list_tool_funcs()), 0)

        # Add a tool function and assert the count
        registry.add_tool_func(another_function)
        self.assertEqual(len(registry.list_tool_funcs()), 1)

        # Add another tool function and assert the count
        @gc_tool(_group_tags_=[gr1_tag], z="this is param z")
        def yet_another_function(z: int):
            "this is yet_another_function description"
            return z * 4

        registry.add_tool_func(yet_another_function)
        self.assertEqual(len(registry.list_tool_funcs()), 2)

        # Remove a tool function and assert the count
        registry.remove_tool_func(another_function)
        self.assertEqual(len(registry.list_tool_funcs()), 1)

        # Remove the remaining tool function and assert the count
        registry.remove_tool_func(yet_another_function)
        self.assertEqual(len(registry.list_tool_funcs()), 0)

        # Test adding a duplicate tool function
        registry.add_tool_func(another_function)
        with self.assertRaises(ValueError):
            registry.add_tool_func(another_function)

        # Test getting a non-existent tool function
        with self.assertRaises(ValueError):
            registry.get_tool_func("non_existent_function")

        # Test removing a non-existent tool function
        with self.assertRaises(ValueError):
            registry.remove_tool_func("non_existent_function")

        # Test listing tool functions
        gr2_tag = ToolFuncGroup("gr2", "this is group 2")
        @gc_tool(_group_tags_=[gr2_tag], z="this is param z")
        def yet_another_function(z: int):
            "this is yet_another_function description"
            return z * 4

        self.assertEqual(len(registry.list_tool_funcs()), 1)
        registry.add_tool_func(yet_another_function)
        self.assertIn("yet_another_function", registry.list_tool_func_names())
        self.assertEqual(len(registry.list_tool_funcs()), 2)

        # Test getting tool functions by tag
        funcs_by_tag = registry.get_tool_funcs_by_group("gr2")
        self.assertEqual(len(funcs_by_tag), 1)
        self.assertEqual(funcs_by_tag[0], yet_another_function)

        # Test getting all unique group tags
        groups = registry.list_groups()
        self.assertEqual({group.name for group in groups}, {"gr1", "gr2"})
