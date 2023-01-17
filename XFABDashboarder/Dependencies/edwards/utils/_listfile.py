import os
from os import listdir
from os.path import isfile, join
from fnmatch import fnmatch


def listfile(path: str, pattern: str = None, subdir: bool = True):
    """
    List files.

    Parameters
    ----------
    path : str
        Top directory.
    pattern : str, optional
        For example, '*.parquet'. The default is None.
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
