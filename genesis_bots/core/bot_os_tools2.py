from   collections              import defaultdict
from   collections.abc          import Iterable
from   enum                     import Enum
from   genesis_bots.core.bot_os_utils \
                                import is_iterable, tupleize
from   genesis_bots.core.logging_config \
                                import logger
import importlib
import inspect
from   itertools                import chain
from   textwrap                 import dedent
import threading
from   typing                   import (Any, Dict, List, Union, get_args,
                                        get_origin)
from   weakref                  import WeakValueDictionary
from   wrapt                    import synchronized

_ALL_BOTS_TOKEN_ = "_ALL_BOTS_" # special token used for applying operations (e.g. registering/unregistering client tools) for all bots.

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
            existing_instance = cls._instances[name]
            # Assert that all other fields match
            if existing_instance.description != description or existing_instance.lifetime != ToolFuncGroupLifetime(lifetime):
                raise ValueError(f"A {cls.__name__} instance with the name '{name}' already exists, but with different description or lifetime.")
            return existing_instance

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


    def to_json(self) -> dict:
        """
        Converts the ToolFuncGroup instance to a JSON-compatible dictionary.
        """
        return {
            "name": self.name,
            "description": self.description,
            "lifetime": self.lifetime.value
        }

    @classmethod
    def from_json(cls, data: dict):
        """
        Creates a ToolFuncGroup instance from a JSON-compatible dictionary.
        Returns:
            ToolFuncGroup: The created instance.
        """
        if 'name' not in data or 'description' not in data or 'lifetime' not in data:
            raise ValueError("Missing required fields in data to create ToolFuncGroup")

        return cls(
            name=data['name'],
            description=data['description'],
            lifetime=ToolFuncGroupLifetime(data['lifetime'])
        )


ORPHAN_TOOL_FUNCS_GROUP = ToolFuncGroup(name="ORPHAN_TOOL_FUNCS_GROUP", description="Default group for tools that do not specify a group", lifetime="EPHEMERAL")
"""
A default group for functions that do not specify a group. Ephemeral by definition - i.e. such functions will not be part of a group that persists to registered bots
"""

REMOTE_TOOL_FUNCS_GROUP = ToolFuncGroup(name="REMOTE_TOOL_FUNCS_GROUP", description="Special group for tools whose python code is defined remotely (client side)", lifetime="EPHEMERAL")
"""
Special group for tools whose python code is defined remotely (client side) and are not invoked on the server side. Instead, their invocation is delegated to the client side. Ephemeral by definition.
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

        # Handle case where type might be passed as string
        if isinstance(python_type, str):
            # Add handling for 'Any' type
            if python_type.lower() == 'any':
                return {'type': 'object'}

            # Handle generic types in string form like 'list[str]'
            if '[' in python_type and ']' in python_type:
                base_type = python_type.split('[')[0].strip().lower()
                inner_type = python_type[python_type.index('[')+1:python_type.rindex(']')].strip()

                if base_type == 'list':
                    return {'type': 'array', 'items': cls._python_type_to_llm_type(inner_type)}
                elif base_type == 'dict':
                    if ',' not in inner_type:
                        return {'type': 'object'}
                    k_type, v_type = [t.strip() for t in inner_type.split(',', 1)]
                    kn = cls._python_type_to_llm_type(k_type)['type']
                    vn = cls._python_type_to_llm_type(v_type)
                    return {
                        'type': 'object',
                        'properties': {kn: vn}
                    }

            # Handle simple types
            if python_type.lower() == 'str':
                return {'type': 'string'}
            elif python_type.lower() == 'int':
                return {'type': 'integer'}
            elif python_type.lower() == 'float':
                return {'type': 'float'}
            elif python_type.lower() == 'bool':
                return {'type': 'boolean'}
            elif python_type.lower() == 'dict':
                return {'type': 'object'}
            elif python_type.lower() == 'list':
                return {'type': 'array'}
            else:
                raise ValueError(f"Could not convert string type annotation '{python_type}' to llm type")

        if origin in (list, List):
            if not args: # a list without type arguments (e.g. x: List). We do 'best effort' and just ommit the items field
                raise ValueError(f"type hint of type {python_type} is missing type arguments (did you mean List[int] or List[str]?)")
            assert len(args) == 1, f"Expected a single type argument for list type {python_type}, got {args}"
            return {'type': 'array', 'items': cls._python_type_to_llm_type(args[0])}
        elif origin in (dict, Dict) or (isinstance(origin, str) and origin.lower() == 'dict'):  # Handle both actual Dict type and string 'dict'
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
        elif python_type is int or python_type == int:
            return {'type': 'integer'}
        elif python_type is str or python_type == str:
            return {'type': 'string'}
        elif python_type is float or python_type == float:
            return {'type': 'float'}
        elif python_type is bool or python_type == bool:
            return {'type': 'boolean'}
        elif python_type is Any:
            return {'type': 'object'}
        else:
            raise ValueError(f"Could not convert annotation type {repr(python_type)} to llm type")

    def __lt__(self, other):
        if not isinstance(other, ToolFuncParamDescriptor):
            return NotImplemented
        return self.description.name < other.description.name


    def to_json(self) -> dict:
        """
        Converts the ToolFuncParamDescriptor instance to a JSON-compatible dictionary.
        """
        return {
            "name": self.name,
            "description": self.description,
            "llm_type": self.llm_type,
            "required": self.required
        }

    @classmethod
    def from_json(cls, data: dict):
        """
        Creates a ToolFuncParamDescriptor instance from a JSON-compatible dictionary.
        Returns:
            ToolFuncParamDescriptor: The created instance.
        """
        if 'name' not in data or 'description' not in data or 'llm_type' not in data or 'required' not in data:
            raise ValueError("Missing required fields in data to create ToolFuncParamDescriptor")

        return cls(
            name=data['name'],
            description=data['description'],
            llm_type_desc=data['llm_type'],
            required=data['required']
        )


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
                 parameters_desc: Iterable[ToolFuncParamDescriptor],
                 groups: Iterable[ToolFuncGroup] = [ORPHAN_TOOL_FUNCS_GROUP]):
        self._name = str(name)
        self._description = str(description)
        # validate the parameters_desc list
        if not is_iterable(parameters_desc) or not all(isinstance(param, ToolFuncParamDescriptor) for param in parameters_desc):
            raise ValueError("expecting parameters_desc to be an interable of ToolFuncParamDescriptor objects")
        self._parameters_desc = tuple(parameters_desc)
        # validate the group list
        if groups is None or not groups: # we never allow an empty group list
            groups = [ORPHAN_TOOL_FUNCS_GROUP]
        groups = tupleize(groups)
        if not all(isinstance(gr, ToolFuncGroup) for gr in groups):
            raise ValueError("All group_tags must be instances of ToolFuncGroupTag")

        lifetimes = {group.lifetime for group in groups}
        if len(lifetimes) > 1:
            raise ValueError(f"All groups for function {name} must have the same lifetime type. Found lifetimes: {lifetimes}")
        self._groups = groups


    @property
    def name(self) -> str:
        return self._name


    @property
    def description(self) -> str:
        return self._description


    @property
    def parameters_desc(self) -> Iterable[ToolFuncParamDescriptor]:
        return self._parameters_desc


    @property
    def groups(self) -> Iterable[ToolFuncGroup]:
        return self._groups


    def to_llm_description_dict(self) -> Dict[str, Any]:
        """Generate the object used to describe this function to an LLM."""
        params_d = dict()
        for param in self._parameters_desc:
            params_d.update(param.to_llm_description_dict())
        return {
            "type": "function",
            "function": {
                "name": self._name,
                "description": self._description,
                "parameters": {
                    "type": "object",
                    "properties": params_d,
                    "required": [param.name for param in self._parameters_desc if param.required is True]
                },
            }
        }


    def to_json(self) -> dict:
        """
        Converts the ToolFuncDescriptor instance to a JSON-compatible dictionary.

        Returns:
            dict: A dictionary representation of the instance.
        """
        return {
            "name": self._name,
            "description": self._description,
            "parameters_desc": [param.to_json() for param in self._parameters_desc],
            "groups": [group.to_json() for group in self._groups]
        }


    @classmethod
    def from_json(cls, data: dict):
        """
        Creates a ToolFuncDescriptor instance from a JSON-compatible dictionary.

        Args:
            data (dict): A dictionary containing the data to create the instance.

        Returns:
            ToolFuncDescriptor: The created instance.
        """
        if 'name' not in data or 'description' not in data or 'parameters_desc' not in data or 'groups' not in data:
            raise ValueError("Missing required fields in data to create ToolFuncDescriptor")

        parameters_desc = [ToolFuncParamDescriptor.from_json(param) for param in data['parameters_desc']]
        groups = [ToolFuncGroup.from_json(group) for group in data['groups']]

        return cls(
            name=data['name'],
            description=data['description'],
            parameters_desc=parameters_desc,
            groups=groups
        )


    def with_added_param(self, param_desc: ToolFuncParamDescriptor) -> 'ToolFuncDescriptor':
        """
        Returns a new ToolFuncDescriptor with the given parameter descriptor added to the existing parameters.
        """
        return ToolFuncDescriptor(
            name=self._name,
            description=self._description,
            parameters_desc=self._parameters_desc + (param_desc,),
            groups=self._groups
        )



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


def is_ephemeral_tool_func(func) -> bool:
    """
    Check if a function is ephemeral, meaning all its associated groups are ephemeral.

    Args:
        func (callable): The function to check.

    Returns:
        bool: True if the function is ephemeral, False otherwise.
    """
    descriptor = get_tool_func_descriptor(func)
    return all(group.lifetime == ToolFuncGroupLifetime.EPHEMERAL
               for group in descriptor.groups)


@synchronized
class ToolsFuncRegistry:
    """
    A registry for managing tool functions.

    This class provides methods to add, remove, retrieve, and list tool functions.
    """
    # NOTE that we put a class-level lock on this object since tools can be accessed and manipulated by multiple session threads


    def __init__(self) -> None:
        """Initialize the ToolsFuncRegistry with an empty set of tool functions."""
        self._tool_funcs: set = set() # set of registered tool-func callables . These are not associated with any bot_id for the scope of this class.
        self._ephem_func_to_bot_map = defaultdict(set) # map from callables of ephemeral tool functions to a set of bot_ids for which this function has been assigned


    def add_tool_func(self, func: callable, replace_if_exists: bool = False) -> None:
        """
        Add a tool function to the registry.

        Args:
            func (callable): The tool function to add.
            replace_if_exists (bool): If True, replace the existing function with the same name. Default is False.

        Raises:
            ValueError: If the function is not a proper tool function or if a function with the same name already exists and replace_if_exists is False.
        """
        if not is_tool_func(func):
            raise ValueError(f"Function {func.__name__} does not have the gc_tool_descriptor attribute. Did you forget to decorate it with @gc_tool?")
        func_name = get_tool_func_descriptor(func).name
        if func_name in self.list_tool_func_names():
            if replace_if_exists:
                self.remove_tool_func(func_name)
            else:
                raise ValueError(f"A function with the name {func_name} already exists in the registry.")
        self._tool_funcs.add(func)


    def remove_tool_func(self, func: Union[str, callable]) -> callable:
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
        """Retrieve a tool function (callable) by its name."""
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
                      key=lambda func: get_tool_func_descriptor(func).name)


    def get_tool_func_names_by_group(self, group_name: str) -> List[str]:
        """Retrieve tool function names by their group name."""
        tool_funcs = self.get_tool_funcs_by_group(group_name)
        return [get_tool_func_descriptor(func).name for func in tool_funcs]


    def get_tool_to_func_map(self, group_lifetime_incl_filter: ToolFuncGroupLifetime = None) -> Dict[str, List[str]]:
        '''
        Returns a map from group name to a list of tool function names that are associated with the group.

        Args:
            group_lifetime_incl_filter (ToolFuncGroupLifetime): If provided, only groups with this lifetime will be included in the map.

        Returns:
            Dict[str, List[str]]: A map from group name to a list of tool function names.
        '''
        tool_to_func_map = defaultdict(list)
        group_lifetime_incl_filter = ToolFuncGroupLifetime(group_lifetime_incl_filter) if group_lifetime_incl_filter is not None else None
        for func in self._tool_funcs:
            desc = get_tool_func_descriptor(func)
            func_name = desc.name
            func_groups = desc.groups
            for group in func_groups:
                if group_lifetime_incl_filter is None or group.lifetime == group_lifetime_incl_filter:
                    tool_to_func_map[group.name].append(func_name)
        return tool_to_func_map


    def list_groups(self) -> List[ToolFuncGroup]:
        """
        List all unique groups associated with the tool functions in the registry.

        Returns:
            List[ToolFuncGroup]: A sorted list of unique ToolFuncGroup instances.
        """
        return sorted(set(chain.from_iterable(get_tool_func_descriptor(func).groups
                                              for func in self._tool_funcs)))


    def assign_ephemeral_tool_func_to_bot(self, bot_id: str, callable_or_func_name: Union[str, callable]) -> None:
        """
        Assign a remote tool function (already added to the registry) to the given bot_id.
        This func-bot association is kept in memory and is not persisted.
        This will allow the bot to use the ephemeral tool function in any session, as long as the server is running.

        Args:
            bot_id (str): The ID of the bot.
            callable_or_func_name (str or callable): The tool function to add, either by name or as a callable.
            It must have already been added to the registry and it must be ephemeral (i.e. all its associated groups must be ephemeral).

        Raises:
            ValueError: If the function is not ephemeral, is already associated with the bot_id, is not in the registry, or the bot_id is invalid.
        """
        func = None
        if isinstance(callable_or_func_name, str):
            try:
                func = self.get_tool_func(callable_or_func_name)
            except ValueError:
                raise ValueError(f"Function named {callable_or_func_name} is not in the registry.")
        elif callable(callable_or_func_name) and is_tool_func(callable_or_func_name):
            func = callable_or_func_name
            if func not in self._tool_funcs:
                raise ValueError(f"Function {func.__name__} is not in the registry.")
        else:
            raise ValueError("Argument must be either a function name (str) or a tool function (callable).")
        assert func is not None and is_tool_func(func)

        #TODO: valiate the bot_id...

        # check that the function is ephemeral
        if not is_ephemeral_tool_func(func):
            raise ValueError(f"Function {func.__name__} must be ephemeral.")

        if bot_id in self._ephem_func_to_bot_map[func]:
            logger.info(f"Function {func.__name__} is already assigned to bot_id {bot_id}. No action taken.")
        else:
            self._ephem_func_to_bot_map[func].add(bot_id)


    def revoke_ephemeral_tool_func_from_bot(self, bot_id: str, func_name_or_callable: Union[str, callable]) -> Iterable[str]:
        """
        Revoke the association of a remote tool function with a bot_id.
        Note that this does not remove the function from the registry.

        Args:
            bot_id (str): The ID of the bot. Use _ALL_BOTS_TOKEN_ to remove the tool for all bots.
            func_name_or_callable (str or callable): The name or callable of the tool function to remove.

        Returns:
            Iterable[str]: An iterable of bot_ids from which the function was revoked.

        Raises:
            ValueError: If the function is not found or the bot_id does not exist.
        """
        func = None
        if isinstance(func_name_or_callable, str):
            try:
                func = self.get_tool_func(func_name_or_callable)
            except ValueError:
                raise ValueError(f"Function named {func_name_or_callable} is not in the registry.")
        elif callable(func_name_or_callable) and is_tool_func(func_name_or_callable):
            func = func_name_or_callable
            if func not in self._tool_funcs:
                raise ValueError(f"Function {func.__name__} is not in the registry.")
        else:
            raise ValueError("Argument must be either a function name (str) or a tool function (callable).")
        assert func is not None and is_tool_func(func)

        revoked_bot_ids = []

        if bot_id == _ALL_BOTS_TOKEN_:
            if func not in self._ephem_func_to_bot_map:
                raise ValueError(f"Function {func.__name__} is not associated with any bot.")
            revoked_bot_ids = list(self._ephem_func_to_bot_map[func])
            del self._ephem_func_to_bot_map[func]
        else:
            if bot_id not in self._ephem_func_to_bot_map.get(func, set()):
                raise ValueError(f"Function {func.__name__} not associated with bot_id {bot_id}.")
            self._ephem_func_to_bot_map[func].remove(bot_id)
            revoked_bot_ids.append(bot_id)

        return revoked_bot_ids


    def get_ephemeral_tool_funcs_for_bot(self, bot_id: str) -> List[callable]:
        """
        Get all ephemeral tool functions assigned to a bot_id
        """
        return sorted(
            [func for func in self._tool_funcs if bot_id in self._ephem_func_to_bot_map[func]],
            key=lambda func: get_tool_func_descriptor(func).name
        )


    def get_bots_for_ephemeral_tool_func(self, func: callable) -> List[str]:
        """
        Get all bot_ids for which an ephemeral tool function is assigned.
        """
        return list(self._ephem_func_to_bot_map.get(func, set()))


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
            reg =  ToolsFuncRegistry()

            # Register all 'new type' PERSISTENT tools here explicitly
            # ----------------------------------------------------------
            funcs = []

            # IMPORT TOOL FUNCTIONS FROM OTHER MODULES
            import_locations = [
                #"genesis_bots.data_pipeline_tools.gc_dagster.get_dagster_tool_functions", # Moved to /customer_demos while we sort our dagster libs depdendencies
                "genesis_bots.connectors.data_connector.get_data_connections_functions",
                "genesis_bots.connectors.snowflake_connector.snowflake_connector.get_snowflake_connector_functions",
                "genesis_bots.core.tools.google_drive.get_google_drive_tool_functions",
                "genesis_bots.core.tools.project_manager.get_project_manager_functions",
                "genesis_bots.core.tools.test_manager.get_test_manager_functions",
                "genesis_bots.core.tools.process_manager.get_process_manager_functions",
                "genesis_bots.core.tools.process_scheduler.get_process_scheduler_functions",
                "genesis_bots.core.tools.artifact_manager.get_artifact_manager_functions",
                "genesis_bots.core.tools.delegate_work.get_delegate_work_functions",
                "genesis_bots.core.tools.git_action.get_git_action_functions",
                "genesis_bots.core.tools.image_tools.get_image_functions",
                "genesis_bots.core.tools.jira_connector.get_jira_connector_functions",
                # "genesis_bots.core.tools.github_connector.get_github_connector_functions",
                "genesis_bots.core.bot_os_web_access.get_web_access_functions",
                "genesis_bots.core.tools.send_email.get_send_email_functions",
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
                    module = importlib.import_module(module_name, package="genesis_bots")
                    func = getattr(module, func_name)
                    result = func()
                    # Convert result to list if it's a tuple
                    func_list = list(result) if isinstance(result, tuple) else result
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


DEFAULT_CLIENT_TOOL_SERVER_TIMEOUT_SECONDS = 60 # the deault timeout (sec) the server will wait on a client tool call if not specified otherwise by the client

def add_api_client_tool(bot_id: str,
                        tool_func_descriptor: dict, # json-parsed ToolFuncDescriptor
                        botos_server,  # BotOsServer instance (do not import to avoid circular imports)
                        timeout_seconds: int = DEFAULT_CLIENT_TOOL_SERVER_TIMEOUT_SECONDS,
                        ):
    from genesis_bots.core.bot_os_input import BosOsClientAsyncToolInvocationHandle
    logger.info(f"Adding client tool {tool_func_descriptor.get('name')} for bot {bot_id} with timeout {timeout_seconds}")

    # Construct a ToolFuncDescriptor instance
    tool_func_descriptor = ToolFuncDescriptor.from_json(tool_func_descriptor)

    # Add a thread_id parameter to the tool_func_descriptor. The proxy function below will use this to lookup
    # the thread_obj to which to route the tool invocation request.
    param_names = [desc.name for desc in tool_func_descriptor.parameters_desc]
    if "thread_id" in param_names:
        return {
            "Success": False,
            "Message": f"Tool function '{tool_func_descriptor.name}' cannot accept a 'thread_id' parameter. This name is reserved for internal use."
        }

    tool_func_descriptor = tool_func_descriptor.with_added_param(THREAD_ID_IMPLICIT_FROM_CONTEXT)

    # Create a callable function proxy for this specific tool function.
    # Note that a new callable is created for each invocation of this function.
    # We take the client's function tool_func_descriptor, attach it to this
    # instance, and then register it with the global tools
    # registry for the given bot_id.
    def _client_tool_func_proxy(**kwargs):
        logger.info(f"client_tool_func_proxy for {tool_func_descriptor.name} called with kwargs: {kwargs}")

        thread_id = kwargs.pop("thread_id") # remove from the params
        assert thread_id is not None

        # lookup the BotOsThread instance for this thread_id
        thread_obj = None

        for session in botos_server.sessions:
            thh_obj = session.threads.get(thread_id)
            if thh_obj and thh_obj.thread_id == thread_id:
                if thread_obj is None:
                    thread_obj = thh_obj
                else:
                    assert False, f"multiple thread_obj found for thread_id {thread_id}"
        if thread_obj is None:
            raise ValueError(f"thread_obj not found for thread_id {kwargs['thread_id']}")

        # Send a message to the thread asking for the client to run this tool func
        action_request_msg = BosOsClientAsyncToolInvocationHandle(
            thread_id=thread_id,
            tool_func_descriptor=tool_func_descriptor,
            invocation_kwargs=kwargs,
            input_metadata=dict(input_thread=thread_id,
                                thread_id=thread_id,
                                input_uuid=None,
                                )
            )
        thread_obj.handle_response(session_id=None,
                                   output_message=action_request_msg)
        res = action_request_msg.get_func_result(timeout=timeout_seconds)

        logger.info(f"client_tool_func_proxy for {tool_func_descriptor.name} returned: {res}")
        return {"success": True, "result": res}

    # Bind the ToolFuncDescriptor to the callable
    setattr(_client_tool_func_proxy, ToolFuncDescriptor.GC_TOOL_DESCRIPTOR_ATTR_NAME, tool_func_descriptor)

    # Register the callable with the global tools registry and with the bot_id
    tools_registry = get_global_tools_registry()
    tools_registry.add_tool_func(_client_tool_func_proxy, replace_if_exists=True) # TODO: be more conservative about replacing existing functions? Right now we assume that the client is always right
    tools_registry.assign_ephemeral_tool_func_to_bot(bot_id, _client_tool_func_proxy)

    # Reset the sessions for this bot after adding the new ephemeral tools
    for session in botos_server.sessions:
        if session.bot_id == bot_id:
            logger.info(f"Resetting session for bot_id '{bot_id}' since we added a new ephemeral tool")
            botos_server.reset_session(bot_id, session)

    return {
        "Success": True,
        "Message": f"Tool function '{tool_func_descriptor.name}' added successfully for bot_id '{bot_id}'."
    }


def remove_api_client_tool(bot_id, func_name, botos_server):
    """
    Remove a client tool function for a specific bot or all bots.

    Args:
        bot_id: The ID of the bot for which the tool function should be removed. Use _ALL_BOTS_TOKEN_ to remove the tool for all bots.
        func_name: The name of the tool function to remove.
        botos_server: The server instance managing the bot sessions.

    Returns:
        A dictionary indicating success or failure of the tool function removal.
    """
    logger.info(f"Removing client tool-func {func_name} for bot {bot_id}")
    tools_registry = get_global_tools_registry()

    # Check if the tool function exists in the registry
    tool_func = tools_registry.get_tool_func(func_name)
    if tool_func is None:
        return {
            "Success": False,
            "Message": f"Tool function '{func_name}' not found."
        }

    # Unassign the tool function from the bot or all bots
    try:
        revoked_bot_ids = tools_registry.revoke_ephemeral_tool_func_from_bot(bot_id, tool_func)
    except Exception as e:
        return {
            "Success": False,
            "Message": str(e)
        }
    # Remove the tool function from the global registry if it is not assigned to any bot
    if len(tools_registry.get_bots_for_ephemeral_tool_func(tool_func)) == 0:
        tools_registry.remove_tool_func(tool_func)

    # reset the sessions for the bots for which the tool was removed
    revoked_bot_ids = tupleize(revoked_bot_ids)
    if len(revoked_bot_ids) > 0:
        # Reset the sessions for the bot for which the tool was removed
        for revoked_bot_id in revoked_bot_ids:
            for session in botos_server.sessions:
                if session.bot_id == revoked_bot_id:
                    logger.info(f"Resetting session for bot_id '{revoked_bot_id}' since we removed a tool")
                    botos_server.reset_session(revoked_bot_id, session)

    # Return Success
    revoked_bot_ids_str = (", ".join(revoked_bot_ids)) or "<none>"
    return {
        "Success": True,
        "Message": f"Tool function '{func_name}' removed successfully for bot_id(s): '{revoked_bot_ids_str}'."
    }
