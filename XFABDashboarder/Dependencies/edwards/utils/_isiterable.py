def isiterable(obj) -> bool:
    """
    Whether an object is iterable.

    Returns
    -------
    True or False
    """
    try:
        iter(obj)
        return True
    except TypeError:
        return False


