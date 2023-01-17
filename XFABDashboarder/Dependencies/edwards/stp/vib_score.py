from datetime import timedelta
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

"""
When the correlation coefficient is r, r is a real number between -1 and 1, 
but the final index is the calculation result of (1 --r) / 2 
so that it is easy for humans to understand as the degree of anomaly. 
There is. The degree of anomaly of the deviation calculated in this way is a real number from 0 to 1, 
and about 0 to 0.2 indicates that VibXYb and VibXYh are linked (positive correlation). 
At around 0.5, VibXYb and VibXYh are not linked (no correlation). 
And, when it is around 0.8 to 1, it means that VibXYb and VibXYh are moving in opposite directions (negative correlation).
"""


def score(data_ops, plot=True):
    data_ops.fillna(method='ffill', inplace=True)
    start_ts = min(data_ops.index)
    end_ts = max(data_ops.index)
    length_ts = end_ts - start_ts
    size_bin = timedelta(days=15)
    n_bin = length_ts // size_bin
    reminder_bin = length_ts % size_bin

    # plt.figure()
    # spilt the data
    anomaly_score = {}
    for b in range(n_bin + 1):
        if b != n_bin:
            start_index = start_ts + b * size_bin
            end_index = start_index + size_bin
        else:
            start_index = start_ts + n_bin * size_bin
            end_index = start_index + reminder_bin

        data_bin = data_ops[
            (pd.to_datetime(data_ops.index) >= start_index) & (pd.to_datetime(data_ops.index) < end_index)]
        vib_H = data_bin['Vibration H'].fillna(method='ffill').dropna()
        vib_B = data_bin['Vibration B'].fillna(method='ffill').dropna()
        vib_H_trend = vib_H.rolling(1000).mean()
        vib_B_trend = vib_B.rolling(1000).mean()
        len_B = len(vib_B_trend.dropna().values)
        len_H = len(vib_H_trend.dropna().values)
        try:
            r = np.corrcoef(vib_B_trend.dropna().values[:len_H], vib_H_trend.dropna().values)
            anomaly_score[end_index] = (1 - r[0, 1]) / 2
        except ValueError:
            r = np.corrcoef(vib_B_trend.dropna().values, vib_H_trend.dropna().values[:len_B])
            anomaly_score[end_index] = (1 - r[0, 1]) / 2

    if plot:
        _, axs = plt.subplots(2, 1, sharex=True)
        data_ops[data_ops['Vibration B'] < 1][['Vibration B', 'Vibration H']].plot(ax=axs[0])
        axs[1].plot(anomaly_score.keys(), anomaly_score.values(), label='Anomaly Score')
        plt.grid()
        plt.legend()

    return anomaly_score
