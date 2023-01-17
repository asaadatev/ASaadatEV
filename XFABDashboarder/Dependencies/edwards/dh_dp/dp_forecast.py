from prophet import Prophet
import pandas as pd


def prophet(data: pd.Series,
            ts_origin: pd.DatetimeIndex,
            fit_window='30D',
            predict_window='15D',
            growth='linear',
            cap=None,
            floor=None,
            sample_freq='300S',
            plot=False,
            ax=None) -> list:
    """
    To use Prophet default setting to forecast time-series
    :param data: pandas series with timestamp as index
    :param ts_origin: start of forecast and end of training data
    :param fit_window: range of training data from ts_origin
    :param predict_window: range of prediction from ts_origin
    :param plot: show plot
    :param ax: plot to ax
    :return: list of forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']]
    """
    df_data = data.to_frame(name='y')
    df_data['ds'] = data.index

    train_data = df_data[(df_data['ds'] > ts_origin - pd.Timedelta(fit_window)) & (df_data['ds'] < ts_origin)]
    model = Prophet(growth=growth)
    if growth == 'logistic':
        train_data.loc[:, 'cap'] = float(cap)
        train_data.loc[:, 'floor'] = float(floor)
    model.fit(train_data)

    future = pd.date_range(ts_origin, ts_origin+pd.Timedelta(predict_window), freq=sample_freq)
    df_future = future.to_frame(name='ds')
    if growth == 'logistic':
        df_future.loc[:, 'cap'] = float(cap)
        df_future.loc[:, 'floor'] = float(floor)
    forecast = model.predict(df_future)
    if plot:
        model.plot(forecast, ax=ax, xlabel='', ylabel='')

    return forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']], future


def save_prophet_forecast(prophet_forecast, save_to_path):
    """
    Save forecast result to parquet file
    :param prophet_forecast: prophet forecast result
    :param save_to_path: file path to save as parquet
    :return:
    """
    prophet_forecast.to_parquet(save_to_path)
    return None


