import datetime

import pandas as pd
import numpy as np


def cal_positive_periods(data: pd.DataFrame | pd.Series,
                         threshold: float | int = None,
                         is_upper: bool = True,
                         include_last: bool = False) -> pd.DataFrame | None:
    """
    Calculate positive periods, determined only by value threshold and no time
    threshold used.

    Parameters
    ----------
    data : pd.DataFrame or pd.Series
      If pd.DataFrame, the first float64 or bool type column will be processed.
    threshold : float or int, optional
      Default None. If None, `data` must be a pd.DataFrame with a
      bool column or a bool type pd.Series.
    is_upper : bool, optional
      If True, data that are greater than or equal to `threshold`
      are regarded as positive.
      If False, data that are less than or equal to `threshold`
      are regarded as positive.
      If `threshold` is None, `is_upper` is ignored, and data values of True
      are regarded as positive.
      Default True.
    include_last : bool, optional
      If True, each positive period starts from the first positive point
      of a section to the first negative point after the end of the section.
      If False, each positive period starts from the first positive point
      of a section to the end positive point of the section.
      Default False.

    Returns
    -------
    None or pd.DataFrame with columns 'start' and 'end'.
    """

    if data is None:
        return None

    if isinstance(data, pd.DataFrame):
        if threshold is not None:
            column = list(data.columns[data.dtypes == 'float64'])
            if not column:
                raise TypeError('Invalid type. Expected float64.')
            if is_upper:
                is_positive = np.array(
                    data.loc[:, column[0]].values >= threshold)
            else:
                is_positive = np.array(
                    data.loc[:, column[0]].values <= threshold)
        else:
            column = list(data.columns[data.dtypes == 'bool'])
            if not column:
                raise TypeError('Invalid type. Expected bool.')
            is_positive = np.array(data.loc[:, column[0]].values)
    elif isinstance(data, pd.Series):
        if threshold is not None:
            if data.dtypes != 'float64':
                raise TypeError('Invalid type. Expected float64.')
            if is_upper:
                is_positive = np.array(data.values >= threshold)
            else:
                is_positive = np.array(data.values <= threshold)
        else:
            if data.dtypes != 'bool':
                raise TypeError('Invalid type. Expected bool.')
            is_positive = np.array(data.values)
    else:
        raise TypeError('Invalid data type. Expected DataFrame or Series.')

    n = data.shape[0]
    is_start = [False] * n
    is_end = [False] * n
    if n >= 2:
        is_start[1:] = (~is_positive[:-1]) & is_positive[1:]
        if include_last:
            is_end[1:] = is_positive[:-1] & (~is_positive[1:])
        else:
            is_end[:-1] = is_positive[:-1] & (~is_positive[1:])

    if is_positive[0]:
        is_start[0] = True
    if is_positive[-1]:
        is_end[-1] = True

    if any(is_start) & any(is_end):
        periods = pd.DataFrame({'start': data.index[is_start].values,
                                'end': data.index[is_end].values})
    else:
        periods = None

    return periods


def add_column_triggered(data: pd.DataFrame | pd.Series,
                         threshold: float | int = None,
                         is_upper: bool = True,
                         t_min_high=datetime.timedelta(days=1),
                         t_min_low=datetime.timedelta(days=1)) \
        -> pd.DataFrame | None:
    """
    Calculate triggered periods, determined by value threshold and time
    threshold.

    If `threshold` is not None and `is_upper` is True, an alert will be
    triggered when data value is equal or greater than `threshold`
    continuously for a certain amount of time, specified by `t_min_high`.
    Once an alert is triggered, the alert will be cleared when data value is
    less than `threshold` continuously for a certain amount of time,
    specified by `t_min_low`. Results will be added as a new bool type column
    'triggered' to indicate whether or not alert is triggered at each data
    point.

    Parameters
    ----------
    data : pd.DataFrame or pd.Series
      If pd.DataFrame, the first float64 or bool type column will be processed.
    threshold : float or int, optional
      Default None. If None, `data` must be a pd.DataFrame with a
      bool column or a bool type pd.Series.
    is_upper : bool, optional
      If True, data that are greater than or equal to `threshold`
      are regarded as positive.
      If False, data that are less than or equal to `threshold`
      are regarded as positive.
      If `threshold` is None, `is_upper` is ignored, and data values of True
      are regarded as positive.
      Default True.
    t_min_high
      Minimum high/positive to trigger an alert.
    t_min_low
      Minimum low/negative to clear an alert.

    Returns
    -------
    None or pd.DataFrame with newly-added bool column 'triggered'.
    """
    if data is None:
        return None

    data = data.copy()

    if isinstance(data, pd.Series):
        data = data.to_frame()
    elif not isinstance(data, pd.DataFrame):
        raise TypeError('Invalid data type. Expected Series or DataFrame.')

    data.loc[:, 'triggered'] = False

    # 'include_last' must be set to True
    periods = cal_positive_periods(data=data, threshold=threshold,
                                   is_upper=is_upper, include_last=True)

    if periods is not None:
        for i in range(periods.shape[0]):
            # If start point is already in triggered state, i.e.,
            # due to the 'clear' delay introduced by last positive period,
            # no need to wait for t_min_high to trigger an alert.
            if data.loc[data.index.values == periods.loc[i, 'start'],
                        'triggered'].values:
                index = (data.index.values >=
                         periods.loc[i, 'start']) \
                        & (data.index.values < (periods.loc[i, 'end']
                                                + t_min_low))
                data.loc[index, 'triggered'] = True
            # Else if start point is not in triggered state, needs to
            # wait for t_min_high to trigger an alert.
            elif (periods.loc[i, 'end'] - periods.loc[i, 'start']) \
                    >= t_min_high:
                index = (data.index.values >=
                         (periods.loc[i, 'start'] + t_min_high)) \
                        & (data.index.values < (periods.loc[i, 'end']
                                                + t_min_low))
                data.loc[index, 'triggered'] = True

    return data


def cal_alert_periods(data: pd.DataFrame | pd.Series,
                      threshold: dict,
                      is_upper: bool,
                      t_min_high: dict,
                      t_min_low: dict) -> dict | None:
    """
    Calculate alert periods.

    Parameters
    ----------
    data : pd.DataFrame or pd.Series
    threshold : dict
      Keys are alert levels, while values are corresponding thresholds.
    is_upper: bool
      If True, data that are greater than or equal to `threshold`
      are regarded as positive.
      If False, data that are less than or equal to `threshold`
      are regarded as positive.
    t_min_high : dict
      Keys are alert levels, while values are corresponding
      minimum high/positive time to trigger multi-level alerts.
    t_min_low : dict
      Keys are alert levels, while values are
      minimum low/negative time to clear multi-level alerts.

    Returns
    -------
    Dict or None.
    """
    if data is None:
        return None

    if (not isinstance(data, pd.Series)) \
            & (not isinstance(data, pd.DataFrame)):
        raise TypeError('Invalid type. Expected Series or DataFrame.')

    alert_levels = list(threshold.keys())

    data_triggered_unmerged = {}
    alert_periods_unmerged = {}
    for k, v in threshold.items():
        data_triggered_unmerged[k] = add_column_triggered(
            data=data,
            threshold=threshold[k],
            is_upper=is_upper,
            t_min_high=t_min_high[k],
            t_min_low=t_min_low[k])
        # Set 'include_last' to False in order to extract first and last time
        # points of each triggered periods
        alert_periods_unmerged[k] = \
            cal_positive_periods(data=data_triggered_unmerged[k],
                                 include_last=False)

    data_triggered = data_triggered_unmerged.copy()
    if len(list(alert_levels)) > 1:
        for i, low_level in enumerate(alert_levels[:-1]):
            for j, high_level in enumerate(alert_levels[(i + 1):]):
                x = data_triggered[low_level].loc[:, 'triggered'].values
                y = data_triggered[high_level].loc[:, 'triggered'].values
                x = x & (~y)
                data_triggered[low_level].loc[:, 'triggered'] = x

    alert_periods = {}
    for k, v in data_triggered.items():
        # Set 'include_last' to False in order to extract first and last time
        # points of each triggered periods
        alert_periods[k] = cal_positive_periods(data=v, include_last=False)

    z = None
    for i, (k, v) in enumerate(data_triggered_unmerged.items(), start=1):
        temp = v.loc[:, 'triggered'].astype(int)
        temp = temp * i
        temp = temp.to_frame().rename(columns={'triggered': k})
        z = pd.concat([z, temp], axis=1, ignore_index=False)
    alert_signal = z.max(axis=1)

    sampling_rate = \
        alert_signal.index.to_series().diff().value_counts().idxmax()

    alert = {'levels': alert_levels,
             'periods': alert_periods,
             'periods_unmerged': alert_periods_unmerged,
             'signal': alert_signal,
             'sampling_rate': sampling_rate}

    return alert
