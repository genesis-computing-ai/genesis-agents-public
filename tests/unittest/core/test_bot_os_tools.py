import unittest
from genesis_bots.core.bot_os_tools2 import *
from genesis_bots.core.bot_os_tools import *

class TestGCTools(unittest.TestCase):

    def test_gc_tool(self):

        ToolFuncGroup._clear_instances() # avoid any lingering instances from previous tests


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
        self.assertEqual(param_x.llm_type['type'], "integer")
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
                            "type": "integer"
                        }
                    },
                    "type": "object",
                    "required": ["x"]
                },
            }
        }
        self.assertEqual(descriptor.to_llm_description_dict(), expected_dict)


    def test_python_type_to_llm_type(self):
        # Test integer type
        self.assertEqual(ToolFuncParamDescriptor._python_type_to_llm_type(int),
                         {'type': 'integer'})

        # Test string type
        self.assertEqual(ToolFuncParamDescriptor._python_type_to_llm_type(str),
                         {'type': 'string'})

        # Test float type
        self.assertEqual(ToolFuncParamDescriptor._python_type_to_llm_type(float),
                         {'type': 'float'})

        # Test boolean type
        self.assertEqual(ToolFuncParamDescriptor._python_type_to_llm_type(bool),
                         {'type': 'boolean'})

        # Test list of integers
        self.assertEqual(ToolFuncParamDescriptor._python_type_to_llm_type(List[int]),
                         {'type': 'array', 'items': {'type': 'integer'}})

        # Test list of strings
        self.assertEqual(ToolFuncParamDescriptor._python_type_to_llm_type(List[str]),
                         {'type': 'array', 'items': {'type': 'string'}})

        # Test dictionary with string keys and integer values
        self.assertEqual(ToolFuncParamDescriptor._python_type_to_llm_type(Dict[str, int]),
                         {'type': 'object', 'properties': {'string': {'type': 'integer'}}})

        # Test dictionary with string keys and list of integers as values
        self.assertEqual(ToolFuncParamDescriptor._python_type_to_llm_type(Dict[str, List[int]]),
                         {'type': 'object', 'properties': {'string': {'type': 'array',
                                                                             'items': {'type': 'integer'}}}})

        # Test unsupported type
        with self.assertRaises(ValueError):
            ToolFuncParamDescriptor._python_type_to_llm_type(complex)



    def test_gc_tool_with_list_and_dict_params(self):
        ToolFuncGroup._clear_instances() # avoid any lingering instances from previous tests

        gr1_tag = ToolFuncGroup("group1", "this is group 1")

        # Test gc_tool decorator with a list parameter
        @gc_tool(_group_tags_=[gr1_tag], numbers="list of numbers")
        def list_function(numbers: List[int]):
            "this is the list_function description"
            return [n * 2 for n in numbers]

        # Test that the decorator does not alter the function's output
        self.assertEqual(list_function([1, 2, 3]), [2, 4, 6])
        self.assertEqual(list_function([4, 5, 6]), [8, 10, 12])

        # Test gc_tool_descriptor properties for list parameter
        descriptor = list_function.gc_tool_descriptor
        self.assertEqual(descriptor.name, "list_function")
        self.assertEqual(descriptor.description, "this is the list_function description")
        self.assertIn("numbers", [param.name for param in descriptor.parameters_desc])
        param_numbers = next(param for param in descriptor.parameters_desc if param.name == "numbers")
        self.assertEqual(param_numbers.description, "list of numbers")
        self.assertEqual(param_numbers.llm_type['type'], "array")
        self.assertEqual(descriptor.groups, (gr1_tag,))

        # Test gc_tool_descriptor to_llm_description_dict() method for list parameter
        expected_dict = {
            "type": "function",
            "function": {
                "name": "list_function",
                "description": "this is the list_function description",
                "parameters": {
                    "properties": {
                        "numbers": {
                            "description": "list of numbers",
                            "type": "array",
                            "items": {"type": "integer"}
                        }
                    },
                    "type": "object",
                    "required": ["numbers"]
                },
            }
        }
        llm_dict = descriptor.to_llm_description_dict()
        self.assertEqual(llm_dict, expected_dict)

        # Test gc_tool decorator with a dict parameter
        @gc_tool(_group_tags_=[gr1_tag], data="dictionary of data")
        def dict_function(data: Dict[str, int]):
            "this is the dict_function description"
            return {k: v * 2 for k, v in data.items()}

        # Test that the decorator does not alter the function's output
        self.assertEqual(dict_function({"a": 1, "b": 2}), {"a": 2, "b": 4})
        self.assertEqual(dict_function({"x": 3, "y": 4}), {"x": 6, "y": 8})

        # Test gc_tool_descriptor properties for dict parameter
        descriptor = dict_function.gc_tool_descriptor
        self.assertEqual(descriptor.name, "dict_function")
        self.assertEqual(descriptor.description, "this is the dict_function description")
        self.assertIn("data", [param.name for param in descriptor.parameters_desc])
        param_data = next(param for param in descriptor.parameters_desc if param.name == "data")
        self.assertEqual(param_data.description, "dictionary of data")
        self.assertEqual(param_data.llm_type['type'], "object")
        self.assertEqual(descriptor.groups, (gr1_tag,))

        # Test gc_tool_descriptor to_llm_description_dict() method for dict parameter
        expected_dict = {
            "type": "function",
            "function": {
                "name": "dict_function",
                "description": "this is the dict_function description",
                "parameters": {
                    "properties": {
                        "data": {
                            "description": "dictionary of data",
                            "type": "object",
                            "properties": {'string': {'type': 'integer'}}
                        }
                    },
                    "type": "object",
                    "required": ["data"]
                },
            }
        }
        llm_dict = descriptor.to_llm_description_dict()
        print(llm_dict)
        self.assertEqual(llm_dict, expected_dict)


    def test_tool_funcs_with_lifecycle(self):
        ToolFuncGroup._clear_instances() # avoid any lingering instances from previous tests
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

        ToolFuncGroup._clear_instances() # avoid any lingering instances from previous tests


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


    def test_gc_tool_with_mixed_param_descriptions(self):

        ToolFuncGroup._clear_instances() # avoid any lingering instances from previous tests

        # Test gc_tool decorator with mixed parameter descriptions
        gr3_tag = ToolFuncGroup("gr3", "this is group 3")

        @gc_tool(
            _group_tags_=[gr3_tag],
            a="this is param a",
            b=ToolFuncParamDescriptor(name="b", description="this is param b", llm_type_desc={"type":"integer"}, required=True),
            c="this is param c"
        )
        def mixed_param_function(a: str, b: int, c: float = 1.0):
            "this is the mixed_param_function description"
            return f"{a}-{b}-{c}"

        # Test that the decorator does not alter the function's output
        self.assertEqual(mixed_param_function("test", 2), "test-2-1.0")
        self.assertEqual(mixed_param_function("hello", 5, 3.5), "hello-5-3.5")

        # Test gc_tool_descriptor properties
        descriptor = mixed_param_function.gc_tool_descriptor
        self.assertEqual(descriptor.name, "mixed_param_function")
        self.assertEqual(descriptor.description, "this is the mixed_param_function description")
        self.assertIn("a", [param.name for param in descriptor.parameters_desc])
        self.assertIn("b", [param.name for param in descriptor.parameters_desc])
        self.assertIn("c", [param.name for param in descriptor.parameters_desc])

        param_a = next(param for param in descriptor.parameters_desc if param.name == "a")
        self.assertEqual(param_a.description, "this is param a")
        self.assertEqual(param_a.llm_type['type'], "string")
        self.assertEqual(param_a.required, True)

        param_b = next(param for param in descriptor.parameters_desc if param.name == "b")
        self.assertEqual(param_b.description, "this is param b")
        self.assertEqual(param_b.llm_type['type'], "integer")
        self.assertEqual(param_b.required, True)

        param_c = next(param for param in descriptor.parameters_desc if param.name == "c")
        self.assertEqual(param_c.description, "this is param c")
        self.assertEqual(param_c.llm_type['type'], "float")
        self.assertEqual(param_c.required, False)

        self.assertEqual(descriptor.groups, (gr3_tag,))

        # Test gc_tool_descriptor to_llm_description_dict() method
        expected_dict = {
            "type": "function",
            "function": {
                "name": "mixed_param_function",
                "description": "this is the mixed_param_function description",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "a": {
                            "description": "this is param a",
                            "type": "string"
                        },
                        "b": {
                            "description": "this is param b",
                            "type": "integer"
                        },
                        "c": {
                            "description": "this is param c",
                            "type": "float"
                        }
                    },
                    "required": ["a", "b"]
                },
            }
        }
        self.assertEqual(descriptor.to_llm_description_dict(), expected_dict)


        # Test cases where using ToolFuncParamDescriptor raises exceptions
        gr1_tag = ToolFuncGroup("group1", "this is group 1")

        # This is a valid case. Shoudl not raise. we start changing the param_desc to make it invalid below.
        param_desc = ToolFuncParamDescriptor(name="x", description="this is param x", llm_type_desc={"type":"integer"}, required=True)
        @gc_tool(_group_tags_=[gr1_tag], x=param_desc)
        def consistent_param(x: int):
            "this is the consistent_param description"
            return x

        # name mismatch
        with self.assertRaises(ValueError) as context:
            param_desc = ToolFuncParamDescriptor(name="y", description="this is param x", llm_type_desc="integer", required=True)
            @gc_tool(_group_tags_=[gr1_tag], x=param_desc)
            def inconsistent_optional_param(x: int):
                "this is the inconsistent_optional_param description"
                return x


        # type mismatch
        with self.assertRaises(ValueError) as context:
            param_desc = ToolFuncParamDescriptor(name="x", description="this is param x", llm_type_desc="integer", required=True)
            @gc_tool(_group_tags_=[gr1_tag], x=param_desc)
            def inconsistent_optional_param(x: str):
                "this is the inconsistent_optional_param description"
                return x

        # type should be 'string' - this should not raise
        param_desc = ToolFuncParamDescriptor(name="x", description="this is param x", llm_type_desc={"type":"string"}, required=True)
        @gc_tool(_group_tags_=[gr1_tag], x=param_desc)
        def inconsistent_optional_param(x: str):
            "this is the inconsistent_optional_param description"
            return x

        # Stating the param is optional but not providing a default value
        with self.assertRaises(ValueError) as context:
            param_desc = ToolFuncParamDescriptor(name="x", description="this is param x", llm_type_desc="integer", required=False)
            @gc_tool(_group_tags_=[gr1_tag], x=param_desc)
            def inconsistent_optional_param(x: int):
                "this is the inconsistent_optional_param description"
                return x

    def test_params_with_required_from_context(self):
        ToolFuncGroup._clear_instances()  # avoid any lingering instances from previous tests

        gr1_tag = ToolFuncGroup("group1", "this is group 1")

        # Test gc_tool decorator with a parameter having required=FROM_CONTEXT
        param_desc = ToolFuncParamDescriptor(name="bot_id",
                                             description="this is param bot_id",
                                             llm_type_desc={"type": "string"},
                                             required=PARAM_IMPLICIT_FROM_CONTEXT)
        @gc_tool(_group_tags_=[gr1_tag], bot_id=param_desc)
        def function_with_from_context_param(bot_id: str):
            "this is the function_with_from_context_param description"
            return bot_id

        descriptor = function_with_from_context_param.gc_tool_descriptor
        param_bot_id = next(param for param in descriptor.parameters_desc if param.name == "bot_id")

        # Check that the parameter has required=PARAM_IMPLICIT_FROM_CONTEXT
        self.assertEqual(param_bot_id.required, PARAM_IMPLICIT_FROM_CONTEXT)

        # Check that the parameter is excluded from LLM description
        self.assertEqual(descriptor.to_llm_description_dict(), {
            "type": "function",
            "function": {
                "name": "function_with_from_context_param",
                "description": "this is the function_with_from_context_param description",
                "parameters": {
                    "properties": {},
                    "type": "object",
                    "required": []
                },
            }
        })

        # Test gc_tool decorator raises exception for invalid required value
        with self.assertRaises(ValueError) as context:
            param_desc = ToolFuncParamDescriptor(name="bot_id", description="this is param bot_id", llm_type_desc={"type": "string"}, required="invalid_value")
            @gc_tool(_group_tags_=[gr1_tag], bot_id=param_desc)
            def function_with_invalid_required(bot_id: str):
                "this is the function_with_invalid_required description"
                return bot_id


    def test_all_params_have_descriptions(self):
        ToolFuncGroup._clear_instances()  # avoid any lingering instances from previous tests

        gr1_tag = ToolFuncGroup("group1", "this is group 1")

        # Test gc_tool decorator with all parameters having descriptions
        @gc_tool(_group_tags_=[gr1_tag], a="description for a", b="description for b")
        def function_with_descriptions(a: int, b: int):
            "this is the function_with_descriptions description"
            return a + b

        descriptor = function_with_descriptions.gc_tool_descriptor
        param_names = [param.name for param in descriptor.parameters_desc]
        param_descriptions = [param.description for param in descriptor.parameters_desc]

        # Check that all parameters have descriptions
        for name, description in zip(param_names, param_descriptions):
            self.assertIsNotNone(description, f"Parameter '{name}' does not have a description")

        # Test gc_tool decorator raises exception for missing description in required params
        with self.assertRaises(ValueError) as context:
            @gc_tool(_group_tags_=[gr1_tag], a="description for a")
            def function_with_missing_description(a: int, b: int):
                "this is the function_with_missing_description description"
                return a + b


        # Test gc_tool decorator does not raise exception for missing description in non-required params
        @gc_tool(_group_tags_=[gr1_tag], a="description for a")
        def function_with_optional_param(a: int, b: int = 0):
            "this is the function_with_optional_param description"
            return a + b

        descriptor = function_with_optional_param.gc_tool_descriptor
        param_names = [param.name for param in descriptor.parameters_desc]
        param_descriptions = [param.description for param in descriptor.parameters_desc]

        # Check that all required parameters have descriptions
        for name, description in zip(param_names, param_descriptions):
            if name == "a":
                self.assertIsNotNone(description, f"Parameter '{name}' does not have a description")
            elif name == "b":
                self.assertIsNone(description, f"Parameter '{name}' should not have a description")


    def test_tool_func_descriptor_json_methods(self):
        ToolFuncGroup._clear_instances()  # avoid any lingering instances from previous tests

        gr11_tag = ToolFuncGroup("group11", "this is group 11")

        # Test gc_tool decorator with all parameters having descriptions
        @gc_tool(_group_tags_=[gr11_tag], a="description for a", b="description for b")
        def function_with_descriptions(a: int, b: str="blah"):
            "this is the function_with_descriptions description"
            return a + b

        descriptor = function_with_descriptions.gc_tool_descriptor

        # Convert descriptor to JSON
        descriptor_json = descriptor.to_json()
        self.assertDictEqual(descriptor_json,
                             {
                                 'name': 'function_with_descriptions',
                                 'description': 'this is the function_with_descriptions description',
                                 'parameters_desc': [
                                     {
                                         'name': 'a',
                                         'description': 'description for a',
                                         'llm_type': {'type': 'integer'},
                                         'required': True
                                     },
                                     {
                                         'name': 'b',
                                         'description': 'description for b',
                                         'llm_type': {'type': 'string'},
                                         'required': False
                                     }
                                 ],
                                 'groups': [
                                     {
                                         'name': 'group11',
                                         'description': 'this is group 11',
                                         'lifetime': 'EPHEMERAL'
                                     }
                                 ]
                             })

        # Create a new descriptor from JSON
        new_descriptor = ToolFuncDescriptor.from_json(descriptor_json)

        # Check that the new descriptor matches the original descriptor
        self.assertEqual(descriptor.name, new_descriptor.name)
        self.assertEqual(descriptor.description, new_descriptor.description)
        self.assertEqual(len(descriptor.parameters_desc), len(new_descriptor.parameters_desc))

        for original_param, new_param in zip(descriptor.parameters_desc, new_descriptor.parameters_desc):
            self.assertEqual(original_param.name, new_param.name)
            self.assertEqual(original_param.description, new_param.description)
            self.assertEqual(original_param.llm_type, new_param.llm_type)
            self.assertEqual(original_param.required, new_param.required)


    def test_function_with_nested_dict_param(self):
        ToolFuncGroup._clear_instances()  # avoid any lingering instances from previous tests
        gr1_tag = ToolFuncGroup("group1", "this is group 1")

        @gc_tool(
                action=ToolFuncParamDescriptor(name="action",
                                                description="Action to perform (CREATE, UPDATE, CHANGE_STATUS, LIST)",
                                                required=True,
                                                llm_type_desc = dict(type="string", enum=["CREATE", "UPDATE", "CHANGE_STATUS", "LIST"]) ,
                                                ),
                bot_id="ID of the bot performing the action",
                todo_id="ID of the todo item (required for UPDATE and CHANGE_STATUS)",
                todo_details=ToolFuncParamDescriptor(
                    name="todo_details",
                    description="Details for the todo item. For CREATE: requires project_id, todo_name, what_to_do, depends_on. "
                                "For CHANGE_STATUS: requires only new_status.",
                    llm_type_desc = dict(type="object",
                                        properties=dict(project_id=dict(type="string", description="ID of the project the todo item belongs to"),
                                                        todo_name=dict(type="string", description="Name of the todo item"),
                                                        what_to_do=dict(type="string", description="What the todo item is about"),
                                                        depends_on=dict(type="string", description="ID of the todo item that this todo item depends on"),
                                                        new_status=dict(type="string", description="New status for the todo item"),
                                                        )
                                        ),
                    required=False,
                    ),
                _group_tags_=[gr1_tag]
                )
        def manage_todos(action: str, bot_id: str, todo_id: str = None, todo_details: dict = None):
            '''
            Manage todo items with various actions.
            When creating Todos try to include any dependencies on other todos where they exist
            it is important to track those to make sure todos are done in the correct order.
            '''
            pass

        expected_dict = {
            "type": "function",
            "function": {
                "name": "manage_todos",
                "description": "Manage todo items with various actions.\nWhen creating Todos try to include any dependencies on other todos where they exist\nit is important to track those to make sure todos are done in the correct order.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "action": {
                            "type": "string",
                            "enum": [
                                "CREATE",
                                "UPDATE",
                                "CHANGE_STATUS",
                                "LIST"
                            ],
                            "description": "Action to perform (CREATE, UPDATE, CHANGE_STATUS, LIST)"
                        },
                        "bot_id": {
                            "type": "string",
                            "description": "ID of the bot performing the action"
                        },
                        "todo_id": {
                            "type": "string",
                            "description": "ID of the todo item (required for UPDATE and CHANGE_STATUS)"
                        },
                        "todo_details": {
                            "type": "object",
                            "properties": {
                                "project_id": {
                                    "type": "string",
                                    "description": "ID of the project the todo item belongs to"
                                },
                                "todo_name": {
                                    "type": "string",
                                    "description": "Name of the todo item"
                                },
                                "what_to_do": {
                                    "type": "string",
                                    "description": "What the todo item is about"
                                },
                                "depends_on": {
                                    "type": "string",
                                    "description": "ID of the todo item that this todo item depends on"
                                },
                                "new_status": {
                                    "type": "string",
                                    "description": "New status for the todo item"
                                }
                            },
                            "description": "Details for the todo item. For CREATE: requires project_id, todo_name, what_to_do, depends_on. For CHANGE_STATUS: requires only new_status."
                        }
                    },
                    "required": [
                        "action",
                        "bot_id"
                    ]
                }
            }
        }

        res = manage_todos.gc_tool_descriptor.to_llm_description_dict()
        print(res)
        self.assertEqual(res, expected_dict)


    def test_tools_func_registry_basic_operations(self):
        ToolFuncGroup._clear_instances()  # avoid any lingering instances from previous tests

        registry = ToolsFuncRegistry()
        gr1_tag = ToolFuncGroup("gr1", "this is group 1")

        # Test adding a tool function
        @gc_tool(_group_tags_=[gr1_tag], param="this is a parameter")
        def sample_function(param: int):
            "this is a sample function"
            return param * 2

        registry.add_tool_func(sample_function)
        self.assertIn("sample_function", registry.list_tool_func_names())

        # Test retrieving a tool function
        retrieved_func = registry.get_tool_func("sample_function")
        self.assertEqual(retrieved_func, sample_function)

        # Test removing a tool function by name
        removed_func = registry.remove_tool_func("sample_function")
        self.assertEqual(removed_func, sample_function)
        self.assertNotIn("sample_function", registry.list_tool_func_names())

        # Test removing a tool function by function object
        registry.add_tool_func(sample_function)
        removed_func = registry.remove_tool_func(sample_function)
        self.assertEqual(removed_func, sample_function)
        self.assertNotIn("sample_function", registry.list_tool_func_names())

        # Test adding a duplicate tool function
        registry.add_tool_func(sample_function)
        with self.assertRaises(ValueError):
            registry.add_tool_func(sample_function)

        # Test getting a non-existent tool function
        with self.assertRaises(ValueError):
            registry.get_tool_func("non_existent_function")

        # Test removing a non-existent tool function
        with self.assertRaises(ValueError):
            registry.remove_tool_func("non_existent_function")

        # Test listing tool functions
        self.assertEqual(len(registry.list_tool_funcs()), 1)
        self.assertIn("sample_function", registry.list_tool_func_names())

        # Test getting tool functions by tag
        funcs_by_tag = registry.get_tool_funcs_by_group("gr1")
        self.assertEqual(len(funcs_by_tag), 1)
        self.assertEqual(funcs_by_tag[0], sample_function)

        # Test getting all unique group tags
        groups = registry.list_groups()
        self.assertEqual({group.name for group in groups}, {"gr1"})


    def test_tools_func_registry_ephemeral_operations(self):
        ToolFuncGroup._clear_instances()  # avoid any lingering instances from previous tests

        registry = ToolsFuncRegistry()

        grP = ToolFuncGroup("gr1", "this is group 1")
        grE = ToolFuncGroup("gr2", "this is group 2", lifetime=ToolFuncGroupLifetime.EPHEMERAL)

        # Test adding a non-ephemeral tool function
        @gc_tool(_group_tags_=[grP], param="this is a parameter")
        def non_ephemeral_function(param: int):
            "this is a non-ephemeral function"
            return param * 3

        registry.add_tool_func(non_ephemeral_function)
        self.assertIn(non_ephemeral_function, registry.list_tool_funcs())

        # Test adding an ephemeral tool function
        @gc_tool(_group_tags_=[grE], param="this is a parameter")
        def ephemeral_function(param: int):
            "this is an ephemeral function"
            return param * 3

        registry.add_tool_func(ephemeral_function)
        self.assertIn(ephemeral_function, registry.list_tool_funcs())

        # Test assigning an ephemeral tool function to a bot
        registry.assign_ephemeral_tool_func_to_bot("bot_1", ephemeral_function)
        self.assertIn(ephemeral_function, registry.get_ephemeral_tool_funcs_for_bot("bot_1"))

        # Test assigning the same ephemeral tool function to the same bot again
        with self.assertLogs(logger, level='INFO') as log:
            registry.assign_ephemeral_tool_func_to_bot("bot_1", "ephemeral_function")
            self.assertIn("Function ephemeral_function is already assigned to bot_id bot_1. No action taken.", log.output[0])

        # Test revoking an ephemeral tool function from a bot
        revoked_bots = registry.revoke_ephemeral_tool_func_from_bot("bot_1", ephemeral_function)
        self.assertEqual(["bot_1"], revoked_bots)
        self.assertNotIn(ephemeral_function, registry.get_ephemeral_tool_funcs_for_bot("bot_1"))

        # Test revoking a non-existent ephemeral tool function from a bot
        with self.assertRaises(ValueError):
            registry.revoke_ephemeral_tool_func_from_bot("bot_1", "non_existent_function")

        # Test getting ephemeral tools for a bot with no assigned tools
        self.assertEqual(len(registry.get_ephemeral_tool_funcs_for_bot("bot_2")), 0)

        # Validate that no ephemeral tools are assigned to bot_1 after revocation
        self.assertEqual(len(registry.get_ephemeral_tool_funcs_for_bot("bot_1")), 0)


    def test_get_persistent_tools_descriptions(self):
        # Test get_persistent_tools_descriptions
        persistent_tools_descriptions = get_persistent_tools_descriptions()
        self.assertIsInstance(persistent_tools_descriptions, dict)
        self.assertGreater(len(persistent_tools_descriptions), 0)
        for tool_name, description in persistent_tools_descriptions.items():
            self.assertIsInstance(description, str)
            self.assertGreater(len(description), 0, f"Tool description for tool (functions group) '{tool_name}' is empty")

