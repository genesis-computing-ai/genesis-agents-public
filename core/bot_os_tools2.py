from   core.bot_os_utils        import tupleize
from   core.logging_config      import logger
from   enum                     import Enum
import importlib
import inspect
from   itertools                import chain
from   textwrap                 import dedent
import threading
from   typing                   import Any, Dict, List, get_args, get_origin, Union, Callable
from   weakref                  import WeakValueDictionary
from   wrapt                    import synchronized


class ToolFuncGroupLifetime(Enum):
    PERSISTENT = "PERSISTENT" # Saved to the tools and bots-specific tables and can be delegate by Eve to other bots
    EPHEMERAL = "EPHEMERAL" # Not saved to to any database. Tool is available to bots as part of their session (e.g. client-side tools)

class ToolFuncGroup:
    """
    Represents a group of functions, often refers elsewhere as 'tool name'. 
    Each tool function is associated with one or more groups.
    group names must be globally unique.
    
    Attributes:
        name (str): The name of the group
        description (str): The description of the group tag.
        lifetime (ToolFuncGroupTagLifetime): The lifetime of this group tag.
    """
    _instances = WeakValueDictionary()

    def __new__(cls, name: str, description: str, lifetime: ToolFuncGroupLifetime = "EPHEMERAL"):
        if name in cls._instances:
            raise ValueError(f"An instance with the name '{name}' already exists.")
        instance = super().__new__(cls)
        cls._instances[name] = instance
        return instance


    def __init__(self, name: str, description: str, lifetime: ToolFuncGroupLifetime = "EPHEMERAL"):
        self.name = name
        self.description = description
        self.lifetime = ToolFuncGroupLifetime(lifetime)


    def __lt__(self, other):
        if not isinstance(other, ToolFuncGroup):
            return NotImplemented
        return self.name < other.name


    def __eq__(self, other):
        if not isinstance(other, ToolFuncGroup):
            return NotImplemented
        # sufficient to check for name equality since we ensure uniqueness of names in the ctor
        return self.name == other.name


    def __hash__(self): # so we can use this as a key in a dict or add to a set
        return hash(self.name)


    @classmethod
    def _clear_instances(cls):
        """
        Clears the global WeakValueDictionary of instances.
        Use this method for testing purposes only.
        """
        cls._instances.clear()


ORPHAN_TOOL_FUNCS_GROUP = ToolFuncGroup(name="ORPHAN_TOOL_FUNCS_GROUP", description="Default group for tools that do not specify a group", lifetime="EPHEMERAL")
"""
A default group for functions that do not specify a group. Ephemeral by definition - i.e. such functions will not be part of a group that persists to registered bots
"""

# Define a unique token for FROM_CONTEXT
PARAM_IMPLICIT_FROM_CONTEXT = object()

class ToolFuncParamDescriptor:
    """
    Represents a descriptor for a tool function parameter, encapsulating its name, description,
    type, and whether it is required.
    If using the @gc_tool decorator, this instance is created automatically by the decorator.

    Attributes:
        name (str): The name of the parameter.
        description (str): A brief description of the parameter.
        llm_type_desc (dict): The type of the parameter as a dictionary, matching the structure expectet by LLM tools, e.g. {'type': 'int'} or {'type': 'array', 'items': {'type': 'int'}}
        required (bool): Indicates whether the parameter is required.

    Methods:
        __lt__(other) -> bool:
            Compares this parameter descriptor with another for sorting purposes.
    """
    def __init__(self, name: str, description: str, llm_type_desc: dict, required: Any):
        self.name = str(name)
        self.description = str(description)
        llm_type_desc = dict(llm_type_desc)
        if 'type' not in llm_type_desc:
            raise ValueError(f"llm_type_desc must be a dictionary with at least a 'type' key, got: {llm_type_desc}")
        self.llm_type = llm_type_desc

        # Allow 'required' to be True, False, or FROM_CONTEXT
        if required not in (True, False, PARAM_IMPLICIT_FROM_CONTEXT):
            raise ValueError(f"Invalid value for 'required': {required}. Must be True, False, or FROM_CONTEXT.")
        self.required = required


    def to_llm_description_dict(self) -> dict:
        """
        Converts the ToolFuncParamDescriptor instance to a dictionary format suitable for LLM description.

        Returns:
            dict: A dictionary containing the parameter's type and description.

        Example:
            >>> param_desc = ToolFuncParamDescriptor(name="x", description="this is param x", llm_type_desc={"type": "int"}, required=True)
            >>> param_desc.to_llm_description_dict()
            {'x': {'description': 'this is param x', 'type': 'int'}}
        """
        if self.required is PARAM_IMPLICIT_FROM_CONTEXT:
            return {}  # Exclude from LLM description if it's FROM_CONTEXT

        dv = self.llm_type.copy()
        dv['description'] = self.description
        return {self.name: dv}


    @classmethod
    def _python_type_to_llm_type(cls, python_type):
        """
        Converts a Python type annotation to a corresponding LLM type.

        Args:
            python_type (type): The Python type annotation to convert.

        Returns:
            dict: A dictionary representing the LLM type e.g. {'type': 'int'} or {'type': 'array', 'items': {'type': 'int'}}

        Raises:
            ValueError: If the Python type annotation cannot be converted to an LLM type.
        """
        origin = get_origin(python_type) or python_type
        args = get_args(python_type)
        if origin in (list, List):
            if not args: # a list without type arguments (e.g. x: List). We do 'best effort' and just ommit the items field
                raise ValueError(f"type hint of type {python_type} is missing type arguments (did you mean List[int] or List[str]?)")
            assert len(args) == 1, f"Expected a single type argument for list type {python_type}, got {args}"
            return {'type': 'array', 'items': cls._python_type_to_llm_type(args[0])}
        elif origin in (dict, Dict):
            if not args: # a dict without type arguments (e.g. x: Dict). We do 'best effort' and just ommit the properties field
                return {'type': 'object'}
            assert len(args) == 2, f"Expected a key, value argument for dict type {python_type}, got {args}"
            k,v = args
            # a dict params is specified as a map from  <key type name> to {"type": <value type name>}
            kn = cls._python_type_to_llm_type(k)['type']
            vn = cls._python_type_to_llm_type(v)
            return {
                'type': 'object',
                'properties': {kn: vn}
            }
        elif python_type is int:
            return {'type': 'integer'}
        elif python_type is str:
            return {'type': 'string'}
        elif python_type is float:
            return {'type': 'float'}
        elif python_type is bool:
            return {'type': 'boolean'}
        else:
            raise ValueError(f"Could not convert annotation type {repr(python_type)} to llm type")

    def __lt__(self, other):
        if not isinstance(other, ToolFuncParamDescriptor):
            return NotImplemented
        return self.description.name < other.description.name


# Use these two constants for teh common implicit 'bot_id' and 'thread_id' param descriptors
# that are expected to be provided by the calling context, not by the LLM
BOT_ID_IMPLICIT_FROM_CONTEXT = ToolFuncParamDescriptor(name="bot_id",
                                                       description="bot_id",
                                                       llm_type_desc={"type": "string"},
                                                       required=PARAM_IMPLICIT_FROM_CONTEXT)
THREAD_ID_IMPLICIT_FROM_CONTEXT = ToolFuncParamDescriptor(name="thread_id",
                                                       description="thread_id",
                                                       llm_type_desc={"type": "string"},
                                                       required=PARAM_IMPLICIT_FROM_CONTEXT)

class ToolFuncDescriptor:
    """
    Represents a descriptor for a tool function, encapsulating its name, description,
    parameter descriptions, and associated groups.

    Attributes:
        name (str): The name of the tool function.
        description (str): A brief description of the tool function.
        parameters_desc (List[_ToolFuncParamDescriptor]): A list of parameter descriptors for the tool function.
        groups (List[ToolFuncGroup]): A list of groups to which the tool function belongs. 
                Defaults to ORPHAN_TOOL_FUNCS_GROUP (which is an ephemeral group and should not be used for server-side tools).

    Methods:
        to_llm_description_dict() -> Dict[str, Any]:
            Generates a dictionary representation of the tool function for use with a language model.
    """

    GC_TOOL_DESCRIPTOR_ATTR_NAME = "gc_tool_descriptor"

    def __init__(self,
                 name: str,
                 description: str,
                 parameters_desc: List[ToolFuncParamDescriptor],
                 groups: List[ToolFuncGroup] = (ORPHAN_TOOL_FUNCS_GROUP,)):
        self.name = name
        self.description = description
        self.parameters_desc = parameters_desc
        if not all(isinstance(gr, ToolFuncGroup) for gr in groups):
            raise ValueError("All group_tags must be instances of ToolFuncGroupTag")
        groups = tupleize(groups)
        lifetimes = {group.lifetime for group in groups}
        if len(lifetimes) > 1:
            raise ValueError(f"All groups for function {name} must have the same lifetime type. Found lifetimes: {lifetimes}")
        self.groups = groups


    def to_llm_description_dict(self) -> Dict[str, Any]:
        """Generate the object used to describe this function to an LLM."""
        params_d = dict()
        for param in self.parameters_desc:
            params_d.update(param.to_llm_description_dict())
        return {
            "type": "function",
            "function": {
                "name": self.name,
                "description": self.description,
                "parameters": {
                    "type": "object",
                    "properties": params_d,
                    "required": [param.name for param in self.parameters_desc if param.required is True]
                },
            }
        }


def gc_tool(_group_tags_: List[str], **param_descriptions):
    """
    A decorator for a 'tool' function that attaches a `gc_tool_descriptor` property to the wrapped function
    as a ToolFuncDescriptor object.

    Example:
        @gctool2(_group_tags_=['group1', 'group2'], param1='this is param1', param2="note that param2 is optional")
        def foo(param1: int, param2: str = "genesis"):
            'This is the description of foo'
            pass

        pprint.pprint(foo.gc_tool_descriptor.to_dict())
    """
    def decorator(func):
        sig = inspect.signature(func)
        if not func.__doc__:
            raise ValueError("Function must have a docstring")
        if func.__annotations__ is None and len(sig.parameters) > 0:
            raise ValueError("Function must have type annotations")

        def _cleanup_docstring(s):
            s = dedent(s)
            s = "\n".join([line for line in s.split("\n") if line])
            return s

        # build/validate a ToolFuncParamDescriptor for each parameter in the signature
        params_desc_list = []
        for pname, pattrs in sig.parameters.items():
            if not pattrs.annotation or pattrs.annotation is pattrs.empty:
                raise ValueError(f"Parameter {pname} has no type annotation")

            # Check if a descriptor is provided for the parameter
            if pname not in param_descriptions:
                if pattrs.default is pattrs.empty:  # Parameter is required
                    raise ValueError(f"Missing descriptor for required parameter {pname!r}")
                continue  # Skip optional parameters without descriptors

            param_desc = param_descriptions[pname]
            if isinstance(param_desc, str):
                # only a param description string is provided. Build a ToolFuncParamDescriptor from the signature
                param_desc = ToolFuncParamDescriptor(
                    name=pname,
                    description=param_desc,
                    llm_type_desc=ToolFuncParamDescriptor._python_type_to_llm_type(pattrs.annotation),
                    required=pattrs.default is pattrs.empty
                )
            elif isinstance(param_desc, ToolFuncParamDescriptor):
                # a ToolFuncParamDescriptor is provided. Validate it.
                if param_desc.name != pname:
                    raise ValueError(f"Descriptor name '{param_desc.name}' does not match parameter name '{pname}'")

                # Check if the type hint matches the descriptor's param_type
                expected_type = ToolFuncParamDescriptor._python_type_to_llm_type(pattrs.annotation)
                if param_desc.llm_type['type'] != expected_type['type']:
                    # Note that we allow for other keys in the user-provided descriptor, such as 'enum'. But we insist the hinted types should match.
                    raise ValueError(f"Type mismatch for parameter {pname}: expected {expected_type}, got {param_desc.llm_type}")



                # Check if the 'required' status matches the descriptor's required attribute
                has_default_val = pattrs.default is not pattrs.empty
                if param_desc.required == has_default_val:
                    suffix = "has a default value" if has_default_val else "does not have a default value"
                    raise ValueError(f"Parameter {pname} marked as required={param_desc.required} but {suffix}")
            else:
                raise ValueError(f"Parameter description for {pname} must be a string or ToolFuncParamDescriptor instance")

            params_desc_list.append(param_desc)

        # Check for descriptors provided for non-existent parameters
        for pname in param_descriptions:
            if pname not in sig.parameters:
                raise ValueError(f"Descriptor provided for non-existent parameter {pname}")

        # Construct the gc_tool_descriptor attribute as a ToolFuncDescriptor object
        descriptor = ToolFuncDescriptor(
            name=func.__name__,
            description=_cleanup_docstring(func.__doc__),
            parameters_desc=params_desc_list,
            groups=_group_tags_
        )
        setattr(func, ToolFuncDescriptor.GC_TOOL_DESCRIPTOR_ATTR_NAME, descriptor)

        return func

    return decorator


def is_tool_func(func):
    """
    Check if a function is a tool function.

    A tool function is identified by having the gc_tool_descriptor attribute,
    which holds an instance of ToolFuncDescriptor.

    Args:
        func (function): The function to check.

    Returns:
        bool: True if the function is a tool function, False otherwise.
    """
    return hasattr(func, ToolFuncDescriptor.GC_TOOL_DESCRIPTOR_ATTR_NAME) and isinstance(getattr(func, ToolFuncDescriptor.GC_TOOL_DESCRIPTOR_ATTR_NAME, None), ToolFuncDescriptor)

def get_tool_func_descriptor(func):
    """
    Get the ToolFuncDescriptor attached to a function.

    Returns:
        ToolFuncDescriptor: The descriptor attached to the function.

    Raises:
        ValueError: If the function is not a proper tool function.
    """
    if is_tool_func(func):
        descriptor = getattr(func, ToolFuncDescriptor.GC_TOOL_DESCRIPTOR_ATTR_NAME)
        if not isinstance(descriptor, ToolFuncDescriptor):
            raise ValueError(f"The attribute {ToolFuncDescriptor.GC_TOOL_DESCRIPTOR_ATTR_NAME} of function {func.__name__} is not an instance of ToolFuncDescriptor.")
        return descriptor
    else:
        raise ValueError(f"Function {func.__name__} is not a proper 'tool function'.")


@synchronized
class ToolsFuncRegistry:
    """
    A registry for managing tool functions.

    This class provides methods to add, remove, retrieve, and list tool functions.    
    """
    # NOTE that we put a class-level lock on this object since tools can be accessed and manipulated by multiple session threads

    def __init__(self) -> None:
        """Initialize the ToolsFuncRegistry with an empty set of tool functions."""
        self._tool_funcs: set = set()

    def add_tool_func(self, func: callable) -> None:
        """Add a tool function to the registry."""
        if not is_tool_func(func):
            raise ValueError(f"Function {func.__name__} does not have the gc_tool_descriptor attribute. Did you forget to decorate it with @gc_tool?")
        func_name = get_tool_func_descriptor(func).name
        if func_name in self.list_tool_func_names():
            raise ValueError(f"A function with the name {func_name} already exists in the registry.")
        self._tool_funcs.add(func)

    def remove_tool_func(self, func: Union[str, Callable]) -> Callable:
        """
        Remove a tool function from the registry and return it.

        This method allows removing a tool function by either its name or the function object itself.

        Returns:
            callable: The function object that was removed.

        Raises:
            ValueError: If the argument is neither a string nor a tool function, or if the tool function does not exist in the registry.
        """
        if isinstance(func, str):
            func_name = func
            func_to_remove = None
            for f in self._tool_funcs:
                if get_tool_func_descriptor(f).name == func_name:
                    func_to_remove = f
                    break
            if func_to_remove is None:
                raise ValueError(f"{self.__class__.__name__}: Could not find a tool function named {func_name}.")
        elif callable(func) and is_tool_func(func):
            func_to_remove = func
        else:
            raise ValueError("Argument must be either a function name (str) or a tool function (callable).")

        if func_to_remove not in self._tool_funcs:
            raise ValueError(f"Function {get_tool_func_descriptor(func_to_remove).name} does not exist in the registry.")
        self._tool_funcs.remove(func_to_remove)
        return func_to_remove

    def get_tool_func(self, func_name: str) -> callable:
        """Retrieve a tool function by its name."""
        for func in self._tool_funcs:
            if get_tool_func_descriptor(func).name == func_name:
                return func
        raise ValueError(f"{self.__class__.__name__}: Could not find a tool function named {func_name}.")

    def list_tool_funcs(self) -> List[callable]:
        """List all tool functions sorted by their description."""
        return sorted(self._tool_funcs, key=lambda func: get_tool_func_descriptor(func).description)

    def list_tool_func_names(self) -> List[str]:
        """List all tool function names."""
        return [get_tool_func_descriptor(f).name for f in self.list_tool_funcs()]

    def get_tool_funcs_by_group(self, group_name: str) -> List[callable]:
        """Retrieve tool functions by their group name."""
        return sorted([func
                       for func in self._tool_funcs
                        if any(group.name == group_name
                               for group in get_tool_func_descriptor(func).groups)
                      ],
                      key=lambda func: get_tool_func_descriptor(func).description)

    def list_groups(self) -> List[ToolFuncGroup]:
        """
        List all unique groups associated with the tool functions in the registry.

        Returns:
            List[ToolFuncGroup]: A sorted list of unique ToolFuncGroup instances.
        """
        return sorted(set(chain.from_iterable(get_tool_func_descriptor(func).groups
                                              for func in self._tool_funcs)))


# Singleton "tools registry for all 'new type' tools.
# Do not use this directly. Use get_global_tools_registry() instead.
_global_tools_registry = None

@synchronized # avoid race conditions on initialization
def get_global_tools_registry():
    """
    Get the global tools registry.
    """
    global _global_tools_registry
    if _global_tools_registry is None:
        try:
            current_thread = threading.current_thread()

            reg =  ToolsFuncRegistry()

            # Register all 'new type' PERSISTENT tools here explicitly
            # ----------------------------------------------------------
            funcs = []

            # IMPORT TOOL FUNCTIONS FROM OTHER MODULES
            import_locations = [
                "data_pipeline_tools.gc_dagster.get_dagster_tool_functions",
                "connectors.data_connector.get_data_connections_functions",
                "connectors.snowflake_connector.snowflake_connector.get_snowflake_connector_functions",
                "core.tools.google_drive.get_google_drive_tool_functions",
                "core.tools.project_manager.get_project_manager_functions",
                "core.tools.test_manager.get_test_manager_functions",
                "core.tools.process_manager.get_process_manager_functions",
                "core.tools.process_scheduler.get_process_scheduler_functions",
                "core.tools.artifact_manager.get_artifact_manager_functions",
                "core.tools.webpage_downloader.get_webpage_downloader_functions",
                "core.tools.delegate_work.get_delegate_work_functions",
                "core.tools.git_action.get_git_action_functions",
                "core.tools.image_tools.get_image_functions",
                "core.tools.jira_connector.get_jira_connector_functions",
                # "core.tools.run_process.run_process_functions",
                # "core.tools.notebook_manager.get_notebook_manager_functions",
                # make_baby_bot
                # harvester_tools
                # bot_dispatch
                # slock_tools
            ]

            for import_location in import_locations:
                try:
                    module_name, func_name = import_location.rsplit('.', 1)
                    module = importlib.import_module(module_name)
                    func_list = getattr(module, func_name)()

                    descs = [get_tool_func_descriptor(func) for func in func_list]
                    added_groups = {group.name for desc in descs for group in desc.groups}
                    logger.info(f"Registering {len(func_list)} tool functions for tool group(s) {added_groups} with the global tools registry")
                    funcs.extend(func_list)
                except Exception as e:
                    logger.error(f"Error registering tool functions from {import_location}: {e}")

            # Verify that we are only registering functions associated with PERSISTENT groups here,
            # as the initial tools registry is expected to list tools (func groups) that are listed in the bots DB and available server-side.
            for func in funcs:
                for group in get_tool_func_descriptor(func).groups:
                    assert group.lifetime is ToolFuncGroupLifetime.PERSISTENT, f"Function {func.__name__} is associated with a non-PERSISTENT group: {group.name}"

            # register all the functions
            for func in funcs:
                reg.add_tool_func(func)

            # set the global registry
            _global_tools_registry = reg
        except Exception as e:
            logger.error(f"Error creating global tools registry: {e}")
            raise e
    return _global_tools_registry
