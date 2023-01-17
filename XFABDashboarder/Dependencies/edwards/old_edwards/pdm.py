"""
Created on Sat Mar 27 16:28:49 2021.

@author: sundu
"""

import numpy as np
import pandas as pd


def pdm_export(data,
               system_name=None,
               parameter_name=None,
               subplot=None,
               format=None,
               period=None,
               hline=None):
    """
    Combine data and info into a dataframe to be exported for visualization.

    Parameters
    ----------
    data : Dataframe
    system_name : str, optional
        The default is None.
    parameter_name : str, optional
        The default is None.
    subplot : int, optional
        The default is None.
    format : str, optional
        The default is None.
    period : Dataframe or dictionary, optional
        The default is None.
    hline : Dataframe, optional
        The default is None.

    Raises
    ------
    TypeError

    Returns
    -------
    Dataframe
        Dataframe with columns 'system', 'parameter', 'datetime', 'value',
        'subplot', 'format', 'alert', 'start', 'end',
        'hline_color', 'hline_value'.

    """

    def flattern_dict(period):
        y = None
        for i in period:
            x = period.get(i).copy()
            x['alert'] = i
            y = pd.concat([y, x], axis=0)
        y.reset_index(drop=True, inplace=True)
        return y

    if data is None:
        return None

    if system_name is not None:
        data['system'] = system_name
    else:
        data['system'] = np.nan

    if parameter_name is not None:
        data['parameter'] = parameter_name
    else:
        data['parameter'] = np.nan

    if subplot is not None:
        data['subplot'] = subplot
    else:
        data['subplot'] = 1

    data['format'] = np.nan
    if format is not None:
        data.loc[0, 'format'] = format

    if period is not None:
        if isinstance(period, dict):
            period = flattern_dict(period)
        if not all([i in list(period.columns)
                    for i in ['alert', 'start', 'end']]):
            raise TypeError('Invalid type')
        data = pd.concat([data, period], axis=1)
    else:
        data['alert'] = np.nan
        data['start'] = np.nan
        data['end'] = np.nan

    if hline is not None:
        if not all([i in list(hline.columns)
                    for i in ['hline_color', 'hline_value']]):
            raise TypeError('Invalid type')
        data = pd.concat([data, hline], axis=1)
    else:
        data['hline_color'] = np.nan
        data['hline_value'] = np.nan

    col = ['system', 'parameter', 'datetime', 'value', 'subplot', 'format',
           'alert', 'start', 'end', 'hline_color', 'hline_value']

    data = data[col]

    return data
