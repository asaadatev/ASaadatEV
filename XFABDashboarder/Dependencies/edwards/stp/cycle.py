import pandas as pd
import numpy as np
from datetime import timedelta
import matplotlib.pyplot as plt


def fft_cycle(T, rolling=None, title=None):
    # Period cycle
    if rolling is not None:
        T = T.rolling(rolling).mean().dropna()
    start_ts = min(T.index)
    end_ts = max(T.index)
    length_ts = end_ts - start_ts
    size_bin = timedelta(days=30)
    n_bin = length_ts // size_bin
    reminder_bin = length_ts % size_bin

    plt.figure()
    # spilt the data
    for b in range(n_bin+1):
        if b != n_bin:
            start_index = start_ts + b * size_bin
            end_index = start_index + size_bin
        else:
            start_index = start_ts + n_bin * size_bin
            end_index = start_index + reminder_bin

        T_bin = T[(pd.to_datetime(T.index) >= start_index) & (pd.to_datetime(T.index) < end_index)]
        n = len(T_bin)
        fhat = np.fft.fft(T_bin, n)
        PSD = fhat * np.conj(fhat) / n
        freq = (1 / n) * np.arange(n)
        L = np.arange(1, np.floor(n / 2), dtype='int')
        plt.plot(freq[L], PSD[L].real, label='{}'.format(start_index)[:10] + ' to ' + '{}'.format(end_index)[:10], alpha=0.7)
        plt.xlabel('Frequency')
        plt.ylabel('Amplitude')
        if title is not None:
            plt.title(title + ' FFT')
        plt.legend()
