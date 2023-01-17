# -*- coding: utf-8 -*-
"""
Created on Wed Jun  9 15:29:58 2021

@author: Dennis Hou
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


class PeakDetect(object):
    def __init__(self, data, system_name, parameter_name):
        self.data = data.dropna().sort_index()
        self.system_name = system_name
        self.parameter_name = parameter_name

    @staticmethod
    def trend_plot(data, ma, parameter_name):
        data = data.dropna().sort_index()
        plt.figure(figsize=(10,6))
        plt.title(str(ma)+'MA')
        plt.plot(data, label=str(parameter_name))
        plt.plot(data.rolling(ma).mean(), label='Trend')
        plt.xticks(rotation=30)
        plt.grid()
        if parameter_name == 'Motor Current':
            plt.ylim(0, 20)
        plt.legend()
    
    @staticmethod
    def spike_detect(data, spike_t, peak_t, lag, parameter_name, plot=True):
        """
        To detect spike and count the number

        :param data: data array with datetime index
        :param spike_t: std threshold for spike
        :param peak_t: std threshold for peak
        :param lag: within lag it count 1 spike
        :param parameter_name: for plot label
        :param plot: plot the result
        :return spike_count: number of spike
        """
        data = data.dropna().sort_index()
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
            if parameter_name == 'Motor Current':
                plt.ylim(0, 20)
            plt.grid()

            if len(peak_count) != 0 and ((len(peak_count)) - len(spike_count)) / len(peak_count) > 0.7:
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
    
            ax2 = plt.subplot(212, sharex=ax1)
            plt.grid()
            try:
                if len(peak_count) != 0 and abs((len(peak_count)) - len(spike_count)) / len(peak_count) > 0.9:
                    plt.plot(spike_count.index, spike_count.values,'o', label='Spike count')
                plt.plot(peak_count.index, peak_count.values, 'o', label='Peak count')
            except ValueError:
                pass
            plt.xticks(rotation=30)
            plt.legend()
        return spike_count, peak_count

    @staticmethod
    def spike_detect_local(data_all, spike_t, peak_t, lag, parameter_name, window_size=50000, plot=True):
        """
        To detect spike and count the number within a local window

        :param data: data array with datetime index
        :param spike_t: std threshold for spike
        :param peak_t: std threshold for peak
        :param lag: within lag it count 1 spike
        :param parameter_name: for plot label
        :param window_size: size of window
        :return spike_count: number of spike
        """
        data_all = data_all.dropna().sort_index()
        
        length = len(data_all)
        if length > window_size:
            N = length // window_size
            R = length % window_size
            
            spike_all = pd.Series([0], [data_all.index[0]])
            peak_all = pd.Series([0], [data_all.index[0]])
            
            for i in range(N+1):
                if i != N:
                    data = data_all[window_size*i : window_size*(i+1)]
                else:
                    data = data_all[window_size*i : window_size*(i+1)+R]
                std = np.std(data)
                avg = np.mean(data)
                spike_upper_limit = avg + spike_t * std
                spike_lower_limit = avg - spike_t * std
                pre_spike = [1 if (i > spike_upper_limit or i < spike_lower_limit) else 0 for i in data]
                spike = [1 if (pre_spike[i] == 1 and sum(pre_spike[i+1:i+lag]) == 0) else 0 for i in range(len(pre_spike))]
                spike = pd.Series(spike, data.index)
                spike_all = spike_all.append(spike)
                
                peak_upper_limit = avg + peak_t * std
                peak_lower_limit = avg - peak_t * std
                pre_peak = [1 if (i > peak_upper_limit) else 0 for i in data]
                peak = [1 if (pre_peak[i] == 1 and sum(pre_peak[i+1:i+lag]) == 0) else 0 for i in range(len(pre_peak))]
                peak = pd.Series(peak, data.index)
                peak_all = peak_all.append(peak)
                
            
                
            spike_count = [sum(spike_all[:i+1]) if spike_all[i]==1 else np.nan for i in range(1,len(spike_all))]
            
            spike_count = pd.Series(spike_count, data_all.index)
            spike_count.dropna(inplace=True)
            
            peak_count = [sum(peak_all[:i+1]) if peak_all[i]==1 else np.nan for i in range(1, len(peak_all))]
            
            peak_count = pd.Series(peak_count, data_all.index)
            peak_count.dropna(inplace=True)

        else:
            data = data_all
        
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
            plt.plot(data_all, label=str(parameter_name))
            if parameter_name == 'Motor Current':
                plt.ylim(0, 20)
            plt.grid()

            if len(peak_count) != 0 and ((len(peak_count)) - len(spike_count)) / len(peak_count) > 0.7:
                try:
                    plt.plot(spike_count.index, [data_all[i] for i in spike_count.index], 'x', label='Spike')
                except ValueError:
                    plt.plot(spike_count.index, np.zeros(len(spike_count))+spike_upper_limit, 'x', label='Spike')
            try:
                plt.plot(peak_count.index, [data_all[i] for i in peak_count.index], 'x', label='Peak')
            except ValueError:
                plt.plot(peak_count.index, np.zeros(len(peak_count))+peak_upper_limit, 'x', label='Peak')
            plt.xticks(rotation=30)
            plt.legend()
    
            ax2 = plt.subplot(212, sharex=ax1)
            plt.grid()
            try:
                if len(peak_count) != 0 and abs((len(peak_count)) - len(spike_count)) / len(peak_count) > 0.9:
                    plt.plot(spike_count.index, spike_count.values,'o', label='Spike count')
                plt.plot(peak_count.index, peak_count.values, 'o', label='Peak count')
            except ValueError:
                pass
            plt.xticks(rotation=30)
            plt.legend()
        return spike_count, peak_count