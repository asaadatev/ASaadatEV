import os
import pandas as pd


def latest_subdir(path: str, datetime_format: str = '%Y-%m-%d_%H%M%S') -> str:
    """
    Get the latest subdirectory, whose name is a datetime string.

    Parameters
    ----------
    path : str
        Top directory.
    datetime_format : str, optional
        Format of subdirectories' names. Default '%Y-%m-%d_%H%M%S'.

    Returns
    -------
    str, full path of the latest subdirectory,
    return None if no such subdirectory.
    """

    subdirs = next(os.walk(path))[1]
    subdirs_datetime = pd.to_datetime(subdirs,
                                      errors='coerce',
                                      format=datetime_format)
    if all(pd.isnull(subdirs_datetime)):
        return None
    else:
        subdir_latest = subdirs[pd.Series(subdirs_datetime).idxmax()]
        return f'{path}/{subdir_latest}'
