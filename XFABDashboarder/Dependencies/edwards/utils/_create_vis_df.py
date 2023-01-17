import pandas as pd
import numpy as np

from .. import COLOR_HLINE


def create_vis_df(data: pd.Series | pd.DataFrame,
                  system_name: str = None,
                  parameter_name: str = None,
                  subplot: int = 1,
                  format: str = None,
                  title: str = None,
                  ylabel: str = None,
                  period: pd.DataFrame | dict = None,
                  hline: pd.DataFrame | dict = None) -> pd.DataFrame | None:
    """
    Store time series data and visualization info into a pd.DataFrame.

    Parameters
    ----------
    data : pd.Series or pd.DataFrame
        Time series data. If pd.DataFrame, it has columns `datetime`,
        and `value`.
    system_name : str, optional
        Default is None.
    parameter_name : str, optional
        Default is None.
    subplot : int, optional
        Subplot index. Default is 1.
    format : str, optional
        Default is None (line).
    title : str, optional
        Title of subplot. Default is None (using `system_name` as `title`).
    ylabel : str, optional
        Label of subplot y-axis. Default is None (using `parameter_name` as
        `ylabel`).
    period : pd.DataFrame or dictionary, optional
        Alert periods to be highlighted in graph.
        If pd.DataFrame, it has three columns `alert` (str, alert level,
        {'advisory', 'warning', 'alarm'}), `start` (datetime), and `end`
        (datetime).
        If dictionary, key is str {'advisory', 'warning', 'alarm'},
        while value is pd.DataFrame with datetime columns `start` and `end`.
        Default is None.
    hline : pd.Dataframe or dictionary, optional
        Colors and y-intercept values of horizontal lines.
        If pd.DataFrame, it has two columns `hline_color` (str) and
        `hline_value` (numeric).
        If dictionary, key is line color, value is line y-intercept value.
        Default is None.

    Returns
    -------
    None or pd.Dataframe with columns 'system', 'parameter', 'datetime',
    'value','subplot', 'format', 'title', 'ylabel', 'alert', 'start', 'end',
    'hline_color', 'hline_value'.
    """

    def dict_to_df(x: dict) -> pd.DataFrame | None:
        """Convert the format of `period` from dictionary to pd.DataFrame.

        Notes
        -----
        See descriptions about two supported formats of parameter `period`.
        """
        y = None
        for k, v in x.items():
            if v is not None:
                temp = v.copy()
                temp['alert'] = k
                y = pd.concat([y, temp], axis=0)
        if y is not None:
            y.reset_index(drop=True, inplace=True)
        return y

    if data is None:
        return None

    data = data.copy()

    if isinstance(data, pd.Series):
        data = data.to_frame().reset_index()
        data.columns = ['datetime', 'value']

    if not isinstance(data, pd.DataFrame):
        raise TypeError('Invalid data type. Expected DataFrame or Series.')

    if ('datetime' not in data.columns.values) \
            | ('value' not in data.columns.values):
        raise TypeError('data as a pd.DataFrame must have '
                        + 'columns `datetime` and `value`.')

    if system_name is not None:
        data['system'] = system_name
    else:
        data['system'] = np.nan

    if parameter_name is not None:
        data['parameter'] = parameter_name
    else:
        data['parameter'] = np.nan

    data['subplot'] = subplot

    data['format'] = np.nan
    if format is not None:
        data.loc[0, 'format'] = format

    data['title'] = np.nan
    if title is not None:
        data.loc[0, 'title'] = title
    elif system_name is not None:
        data.loc[0, 'title'] = system_name

    data['ylabel'] = np.nan
    if ylabel is not None:
        data.loc[0, 'ylabel'] = ylabel
    elif parameter_name is not None:
        data.loc[0, 'ylabel'] = parameter_name

    if period is not None:
        if isinstance(period, dict):
            period = dict_to_df(period)

    if period is not None:
        if not all([col in list(period.columns)
                    for col in ['alert', 'start', 'end']]):
            raise TypeError('Invalid type.')
        data = pd.concat([data, period], axis=1)
    else:
        data['alert'] = np.nan
        data['start'] = np.datetime64('NaT')
        data['end'] = np.datetime64('NaT')

    if hline is not None:
        if isinstance(hline, dict):
            hline = pd.DataFrame(
                {'hline_color': [COLOR_HLINE.get(k) for k in hline.keys()],
                 'hline_value': list(hline.values())})
        if not all([col in list(hline.columns)
                    for col in ['hline_color', 'hline_value']]):
            raise TypeError('Invalid type')
        data = pd.concat([data, hline], axis=1)
    else:
        data['hline_color'] = np.nan
        data['hline_value'] = np.nan

    col = ['system', 'parameter', 'datetime', 'value',
           'subplot', 'format', 'title', 'ylabel',
           'alert', 'start', 'end',
           'hline_color', 'hline_value']

    data = data[col]

    return data
