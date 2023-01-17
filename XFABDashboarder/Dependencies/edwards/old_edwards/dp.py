# -*- coding: utf-8 -*-
"""
Created on Sat Mar 27 18:32:32 2021

@author: sundu
"""

import os
import datetime

import pandas as pd

from edwards.old_edwards import Odbc
from edwards.old_edwards import pdm_export


def merge_graph(data,
                path,
                reverse=False):
    """
    Merge same type of data output from dp function of multi systems.

    Parameters
    ----------
    data : Dictionary
        DESCRIPTION.
    path : str
        DESCRIPTION.
    reverse : bool, optional
        DESCRIPTION. The default is False.

    Returns
    -------
    None.

    """
    df = []
    for i, key in enumerate(data):
        df.append(data.get(key, None).get('export', None))
    df = pd.concat(df)

    systems = [key for key in data.keys()]
    subplots = df['subplot'].drop_duplicates().values

    mapping = pd.DataFrame({'system': systems,
                            'subplot': list(range(1, 1+len(systems)))})

    if not os.path.exists(path):
        os.makedirs(path)

    for i, i_subplot in enumerate(subplots):
        df1 = df[df['subplot'] == (i_subplot)].copy()
        df2 = df1.drop('subplot', axis=1)
        df3 = df2.merge(mapping, how='left', on='system')
        df3.to_parquet(f'{path}/{i_subplot}_{df3.parameter[0]}.parquet')

    return None


def dp_trend(database,
             system_name,
             parameter_number,
             start_datetime=datetime.date(1970, 1, 1),
             end_datetime=datetime.date.today(),
             resample_rule='30min',
             resample_func=['min', 'mean', 'max'],
             fillna_method='ffill',
             rolling_window=48,
             rolling_func='mean',
             dp_name=None,
             path=None):
    """
    Process data to reveal trend.

    Parameters
    ----------
    database : TYPE
        DESCRIPTION.
    system_name : TYPE
        DESCRIPTION.
    parameter_number : TYPE
        DESCRIPTION.
    start_datetime : TYPE, optional
        DESCRIPTION. The default is datetime.date(1970, 1, 1).
    end_datetime : TYPE, optional
        DESCRIPTION. The default is datetime.date.today().
    resample_rule : TYPE, optional
        DESCRIPTION. The default is '30min'.
    resample_func : TYPE, optional
        DESCRIPTION. The default is ['min', 'mean', 'max'].
    fillna_method : TYPE, optional
        DESCRIPTION. The default is 'ffill'.
    rolling_window : TYPE, optional
        DESCRIPTION. The default is 48.
    rolling_func : TYPE, optional
        DESCRIPTION. The default is 'mean'.
    dp_name : TYPE, optional
        DESCRIPTION. The default is None.
    path : TYPE, optional
        DESCRIPTION. The default is None.

    Returns
    -------
    result : TYPE
        DESCRIPTION.

    """
    ts = Odbc.create(database=database,
                     system_name=system_name,
                     parameter_number=parameter_number,
                     start_datetime=start_datetime,
                     end_datetime=end_datetime)

    if isinstance(resample_func, str):
        resample_func = [resample_func]

    if dp_name is None:
        words = ts.parameter_name.split()
        letters = [word[0] for word in words]
        prefix = ''.join(letters) + '_'
        dp_name = [prefix + func for func in resample_func]

    export = []

    data = pdm_export(data=ts.data.reset_index().copy(),
                      system_name=system_name,
                      parameter_name=ts.parameter_name,
                      subplot=1)
    export.append(data)

    for i, func in enumerate(resample_func):
        data = ts.data.resample(rule=resample_rule,
                                axis=0,
                                closed='right',
                                label='right',
                                origin='start_day').agg(func=func)
        if fillna_method is not None:
            data = data.fillna(method=fillna_method)
        data = data.rolling(window=rolling_window,
                            min_periods=1,
                            center=False,
                            win_type=None,
                            axis=0,
                            closed='both').agg(func=rolling_func)
        data = pdm_export(data=data.reset_index().copy(),
                          system_name=system_name,
                          parameter_name=dp_name[i],
                          subplot=i+2)
        export.append(data)

    export = pd.concat(export)

    if path is not None:
        if not os.path.exists(path):
            os.makedirs(path)
        export.to_parquet(f'{path}/{system_name}.parquet')

    result = {'export': export}

    return result
