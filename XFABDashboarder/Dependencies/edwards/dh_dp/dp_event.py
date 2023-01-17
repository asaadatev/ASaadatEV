import pandas as pd


def pump_swap_event(pump_hour):
    """
    From pump run hours, output pump swap timestamp
    :param pump_hour:
    :return:
    """
    swap_ts = None
    pump_hour = pump_hour.sort_index()
    pump_hour = pump_hour.fillna(method='ffill').dropna()
    df_hour = pd.DataFrame()
    df_hour['run hour'] = pump_hour
    df_hour['diff'] = df_hour['run hour'].diff(1)
    df_hour = df_hour[df_hour['diff'] != 0]
    swap_ts = list(df_hour[df_hour['diff'] < 0].index)

    return swap_ts
