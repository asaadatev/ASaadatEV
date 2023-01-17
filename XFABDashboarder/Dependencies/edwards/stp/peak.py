# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


class PeakDetect(object):
    def __init__(self, data, system_name, parameter_name):
        self.data = data
        self.system_name = system_name
        self.parameter_name = parameter_name

    @staticmethod
    def trend_plot(data, ma, parameter_name):
        plt.figure(figsize=(10,6))
        plt.title(str(ma)+'MA')
        plt.plot(data, label=str(parameter_name))
        plt.plot(data.rolling(ma).mean(), label='Trend')
        plt.xticks(rotation=30)
        plt.grid()
        plt.ylim(0,20)
        plt.legend()
    
    @staticmethod
    def spike_detect(data, spike_t, peak_t, lag, parameter_name, plot=True):
        """
        To detect spike and count the number
        :param spike_t: std threshold for spike
        :param peak_t: std threshold for peak
        :param lag: within lag it count 1 spike
        :param plot: plot the result
        :return spike_count: number of spike
        """
        std = np.std(data)
        avg = np.mean(data)
        spike_upper_limit = avg + spike_t * std
        spike_lower_limit = avg - spike_t * std
        pre_spike = [1 if (i > spike_upper_limit or i < spike_lower_limit) else 0 for i in data]
        spike = [1 if (pre_spike[i] == 1 and sum(pre_spike[i+1:i+lag]) == 0) else 0 for i in range(len(pre_spike))]
        spike_count = [sum(spike[:i+1]) if spike[i]==1 else np.nan for i in range(len(spike))]
        spike = pd.Series(spike, data.index)
        spike_count = pd.Series(spike_count, data.index)
        spike_count.dropna(inplace=True)

        peak_upper_limit = avg + peak_t * std
        peak_lower_limit = avg - peak_t * std
        pre_peak = [1 if (i > peak_upper_limit) else 0 for i in data]
        peak = [1 if (pre_peak[i] == 1 and sum(pre_peak[i+1:i+lag]) == 0) else 0 for i in range(len(pre_peak))]
        peak_count = [sum(peak[:i+1]) if peak[i]==1 else np.nan for i in range(len(peak))]
        peak = pd.Series(peak, data.index)
        peak_count = pd.Series(peak_count, data.index)
        peak_count.dropna(inplace=True)



        if plot == True:
            plt.figure(figsize=(10,6))
            ax1 = plt.subplot(211)
            plt.title('Peak&Spike')
            plt.plot(data, label=str(parameter_name))
            plt.ylim(0, 20)
            plt.grid()
            if abs((len(peak_count)) - len(spike_count)) / len(peak_count) > 0.7:
                try:
                    plt.plot(spike_count.index, [data[i] for i in spike_count.index], 'x', label='Spike')
                except ValueError:
                    plt.plot(spike_count.index, np.zeros(len(spike_count))+spike_upper_limit, 'x', label='Spike')
            try:
                plt.plot(peak_count.index, [data[i] for i in peak_count.index], 'x', label='Peak')
            except ValueError:
                plt.plot(peak_count.index, np.zeros(len(peak_count))+peak_upper_limit, 'x', label='Peak')
            plt.xticks(rotation=30)
            plt.legend()
    
            ax2 = plt.subplot(212)
            plt.grid()
            try:
                if abs((len(peak_count)) - len(spike_count)) / len(peak_count) > 0.9:
                    plt.plot(spike_count.index, spike_count.values,'o', label='Spike count')
                plt.plot(peak_count.index, peak_count.values, 'o', label='Peak count')
            except ValueError:
                pass
            plt.xticks(rotation=30)
            plt.legend()
        return spike_count, peak_count