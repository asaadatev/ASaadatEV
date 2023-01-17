
def rolling_trend(data,
                  resample_rule='30min',
                  resample_func='mean',
                  fillna_method='ffill',
                  rolling_window=48,
                  rolling_func='mean'):
    """

    :param data: pandas series with timestamp as index
    :param resample_rule:
    :param resample_func:
    :param fillna_method:
    :param rolling_window:
    :param rolling_func:
    :return: smoothed data
    """
    data = data.resample(rule=resample_rule,
                            axis=0,
                            closed='right',
                            label='right').agg(func=resample_func)
    if fillna_method is not None:
        data = data.fillna(method=fillna_method)
    data = data.rolling(window=rolling_window,
                        min_periods=1,
                        center=False,
                        win_type=None,
                        axis=0,
                        closed='both').agg(func=rolling_func)
    return data
