import pandas as pd


def tribble(columns, *data):
    """Create a pandas DataFrame using R tribble style.

    References
    ----------
    https://stackoverflow.com/questions/54368328

    Examples
    --------
    >>> tribble(['colA', 'colB'],
    >>>          'a',    1,
    >>>          'b',    2,
    >>>          'c',    3)
         colA  colB
    0    a     1
    1    b     2
    2    c     3
    """
    return pd.DataFrame(data=list(zip(*[iter(data)]*len(columns))),
                        columns=columns)
