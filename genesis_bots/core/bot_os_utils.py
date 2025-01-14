from collections.abc import Iterable
import pprint

def truncate_string(s, max_length, truncation_string="..."):
    """
    Truncates a string to a specified length and appends a truncation string if necessary.

    Args:
        s (str): The string to truncate.
        max_length (int): The maximum length of the truncated string including the truncation string.
        truncation_string (str): The string to append if truncation occurs. Defaults to "...". 
            Note that the truncation string is included in the max_length and maybe truncated as well if max_length is less than the length of the truncation string.

    Returns:
        str: The truncated string with the truncation string appended if truncation occurred.

    Examples:
        >>> truncate_string("Hello, World!", 11)
        'Hello, W...'
        >>> truncate_string("Hello, World!", 20)
        'Hello, World!'
        >>> truncate_string("Hello, World!", 11, "--")
        'Hello, Wo--'
    """
    if len(s) <= max_length:
        return s
    truncation_length = max(max_length - len(truncation_string), 0)
    ret = s[:truncation_length] + truncation_string
    return ret[:max_length]


def is_iterable(obj):
    return isinstance(obj, Iterable) and not isinstance(obj, str)

def datetime_to_string(date_time_obj, format="%Y-%m-%d %H:%M:%S"):
    if type(date_time_obj) == str:
        return date_time_obj
    return date_time_obj.strftime(format)


def tupleize(*args):
    """
    Converts the given arguments into a tuple. If an iterable is passed as a single argument,
    it is converted to a tuple, except for strings which are treated as scalars. If no arguments
    are passed, it yields an empty tuple.

    Args:
        *args: Variable length argument list.

    Returns:
        tuple: A tuple containing all the arguments or the elements of the iterable.

    Examples:
        >>> tupleize(1, 2, 3)
        (1, 2, 3)
        >>> tupleize([1, 2, 3])
        (1, 2, 3)
        >>> tupleize("abc")
        ('abc',)
        >>> tupleize((1, 2), [3, 4])
        ((1, 2), [3, 4])
        >>> tupleize()
        ()
    """
    if len(args) == 0:
        return ()
    if len(args) == 1 and is_iterable(args[0]):
        return tuple(args[0])
    return tuple(args)