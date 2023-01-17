"""
Useful functions.

@author: Danny Sun
"""
import os
from os import listdir
from os.path import isfile, join
from fnmatch import fnmatch

import pandas as pd


def listfile(path, pattern=None, subdir=True):
    """
    List files.

    Parameters
    ----------
    path : str
        Top directory.
    pattern : str, optional
        `pattern` string. The default is None.
    subdir : bool, optional
        Whether include subdirectories. The default is True.

    Returns
    -------
    files : list of str
        List of full filenames.

    """
    if subdir:
        files = [os.path.join(dirpath, f)
                 for dirpath, dirnames, filenames in os.walk(path)
                 for f in filenames]
    else:
        files = [os.path.join(path, f)
                 for f in listdir(path) if isfile(join(path, f))]

    if pattern is not None:
        files = [f for f in files if fnmatch(f, pattern)]

    return files


def latest_subdir(path, format_spec='%Y-%m-%d_%H%M%S'):
    """
    Get the latest subdirectory, whose name is a datetime string.

    Parameters
    ----------
    path : str
        Top directory.
    format_spec : str

    Returns
    -------
    str, latest subdirector, return None if no such subdirectory.

    """
    subdirs = next(os.walk(path))[1]
    subdirs_datetime = pd.to_datetime(subdirs,
                                      errors='coerce',
                                      format=format_spec)
    if all(pd.isnull(subdirs_datetime)):
        return None
    else:
        subdir_latest = subdirs[pd.Series(subdirs_datetime).idxmax()]
        return f'{path}/{subdir_latest}'
