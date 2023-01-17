import pandas as pd
import numpy as np
from datetime import timedelta
import matplotlib.pyplot as plt


def fft_cycle(T, days=30, rolling=None, title=None, plot=True):
    # Period cycle
    T = T.resample('1S').pad().dropna()
    if rolling is not None:
        T = T.rolling(rolling).mean().dropna()
    start_ts = T.index[0]
    end_ts = T.index[-1]
    length_ts = end_ts - start_ts
    size_bin = timedelta(days=days)
    n_bin = length_ts // size_bin
    reminder_bin = length_ts % size_bin
    if plot:
        plt.figure()
    T_period = {}
    # spilt the data
    for b in range(n_bin + 1):
        if b != n_bin:
            start_index = start_ts + b * size_bin
            end_index = start_index + size_bin
        else:
            start_index = start_ts + n_bin * size_bin
            end_index = start_index + reminder_bin

        T_bin = T[(pd.to_datetime(T.index) >= start_index) & (pd.to_datetime(T.index) < end_index)]
        n = len(T_bin)
        # fft of T_bin
        fhat = np.fft.fft(T_bin, n)
        PSD = fhat * np.conj(fhat) / n
        # freqence range
        freq = (1 / n) * np.arange(n)
        # index number
        L = np.arange(1, np.floor(n / 2), dtype='int')
        # peak index exclude freq<1e-5 or >0.5 which period>28hours or <2seconds
        idx_start = (np.abs(freq - 1e-5)).argmin()
        idx_end = (np.abs(freq - 0.5)).argmin()
        max_L = np.where(PSD.real == max(PSD.real[idx_start:idx_end]))
        max_L = [i for i in max_L[0] if idx_start < i < idx_end]
        # period in seconds where the top frequence
        period = 1 / freq[max_L]
        T_period[end_index] = period[0]
        if plot:
            plt.plot(freq[L], PSD[L].real, label='{}'.format(start_index)[:10] + ' to ' + '{}'.format(end_index)[:10],
                     alpha=0.7)
            plt.xlabel('Frequency')
            plt.ylabel('Amplitude')
            if title is not None:
                plt.title(title + ' FFT')
            plt.grid()
            plt.legend()
    return T_period


def acc_interval(acc_motor_speed, high, low=500, plot=True):
    acc_period = {}
    start = None
    for i in acc_motor_speed.index:
        if 0 < acc_motor_speed[i] <= low:
            start = i
        elif start and acc_motor_speed[i] > high:
            interval = i - start
            acc_period[start] = interval.seconds
            start = None
    if len(acc_period) == 0:
        acc_period = None
    elif plot:
        plt.figure()
        plt.plot(acc_period.keys(), acc_period.values())
        plt.scatter(acc_period.keys(), acc_period.values(),
                    label='Acceleration Interval in second', linewidths=3)
        plt.grid()
        plt.legend()
    return acc_period


def dec_interval(dec_motor_speed, high, low=250, plot=True):
    dec_period = {}
    start = None
    for i in dec_motor_speed.index:
        if dec_motor_speed[i] > high:
            start = i
        elif start and 0 < dec_motor_speed[i] <= low:
            interval = i - start
            dec_period[start] = interval.seconds
            start = None
    if len(dec_period) == 0:
        dec_period = None
    elif plot:
        plt.figure()
        plt.plot(dec_period.keys(), dec_period.values())
        plt.scatter(dec_period.keys(), dec_period.values(),
                    label='Deceleration Interval in second', linewidths=3)
        plt.grid()
        plt.legend()
    return dec_period
