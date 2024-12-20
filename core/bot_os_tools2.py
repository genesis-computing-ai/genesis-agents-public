from   core.bot_os_utils        import tupleize
from   core.logging_config      import logger
from   enum                     import Enum
import inspect
from   itertools                import chain
from   textwrap                 import dedent
from   typing                   import Any, Dict, List, get_args, get_origin
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


ORPHAN_TOOL_FUNCS_GROUP = ToolFuncGroup(name="ORPHAN_TOOL_FUNCS_GROUP", description="Default group for tools that do not specify a group", lifetime="EPHEMERAL")
"""
A default group for functions that do not specify a group. Ephemeral by definition - i.e. such functions will not be part of a group that persists to registered bots
"""

class _ToolFuncParamDescriptor:
    def __init__(self, name: str, description: str, param_type: str, required: bool):
        self.name = name
        self.description = description
        self.param_type = param_type
        self.required = required

    def __lt__(self, other):
        if not isinstance(other, _ToolFuncParamDescriptor):
            return NotImplemented
        return self.description.name < other.description.name

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
                 parameters_desc: List[_ToolFuncParamDescriptor], 
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
        return {
            "type": "function",
            "function": {
                "name": self.name,
                "description": self.description,
                "parameters": {
                    "type": "object",
                    "properties": {
                        param.name: {
                            "type": param.param_type,
                            "description": param.description
                        } for param in self.parameters_desc
                    },
                    "required": [param.name for param in self.parameters_desc if param.required]
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

        def _python_type_to_llm_type(python_type):
            origin = get_origin(python_type)
            args = get_args(python_type)
            if origin in (list, List):
                return {'type': 'array', 'items': _python_type_to_llm_type(args[0])} if args else {'type': 'array'}
            elif origin in (dict, Dict):
                return {
                    'type': 'object',
                    'properties': {arg: _python_type_to_llm_type(args[i]) for i, arg in enumerate(args)}
                }
            elif python_type is int:
                return {'type': 'int'}
            elif python_type is str:
                return {'type': 'string'}
            elif python_type is float:
                return {'type': 'float'}
            elif python_type is bool:
                return {'type': 'boolean'}
            else:
                raise ValueError(f"Could not convert annotation type {python_type} to llm type")

        def _cleanup_docstring(s):
            s = dedent(s)
            s = "\n".join([line for line in s.split("\n") if line])
            return s

        params_desc_list = []
        for pname, pattrs in sig.parameters.items():
            if not pattrs.annotation or pattrs.annotation is pattrs.empty:
                raise ValueError(f"Parameter {pname} has no type annotation")
            param_desc = _ToolFuncParamDescriptor(
                name=pname,
                description=param_descriptions[pname],
                param_type=_python_type_to_llm_type(pattrs.annotation)['type'],
                required=pattrs.default is pattrs.empty # if default is empty, then the parameter is required
            )
            params_desc_list.append(param_desc)

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


    def remove_tool_func(self, func: str or callable) -> callable:
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

@synchronized # avoid race conditions on initial access
def get_global_tools_registry():
    """
    Get the global tools registry.
    """
    global _global_tools_registry
    if _global_tools_registry is None:
        _global_tools_registry = ToolsFuncRegistry()
        reg = _global_tools_registry

        # Add all 'new type' PERSISTENT tools here explicitly
        #----------------------------------------------------------
        funcs = []
        # dagster tool
        from data_pipeline_tools.gc_dagster import get_dagster_tool_functions
        logger.info(f"Adding dagster tools to global tools registry")
        funcs.extend(get_dagster_tool_functions())

        # Verify that we are only registering functions associated with PERSISTENT groups here,
        # as the initial tools registry is expected to list tools (func groups) that are listed in the bots DB and available server-side.
        for func in funcs:
            for group in get_tool_func_descriptor(func).groups:
                assert group.lifetime is ToolFuncGroupLifetime.PERSISTENT, f"Function {func.__name__} is associated with a non-PERSISTENT group: {group.name}"

        # register all the functions
        for func in funcs:
            reg.add_tool_func(func)

    return _global_tools_registry

