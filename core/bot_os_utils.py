from collections.abc import Iterable

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