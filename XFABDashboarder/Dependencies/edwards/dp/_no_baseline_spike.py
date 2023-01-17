"""Class for computing spike-related derived parameter."""

# Authors: Danny Sun <duo.sun@edwardsvacuum.com>,
#          Dennis Hou <duanyang.hou@edwardsvacuum.com>

import copy

import numpy as np
import pandas as pd

from ._base import Base
from ..utils._cal_alert_periods import cal_alert_periods


class NoBaselineSpike(Base):
    """ Spike detection by using std threshold after removing baseline """

    def __init__(self,
                 sigma_t=True,
                 spike_t=4.0,
                 peak_t=3.0,
                 lag=2.0,
                 local_spike=False,
                 resample_rule='30min',
                 resample_func='min',
                 upper_limit=None,
                 lower_limit=None,
                 fillna_value=None,
                 fillna_method='ffill',
                 iir_alpha=None,
                 rolling_window=48,
                 rolling_min_periods=1,
                 threshold=None,
                 is_upper=True,
                 include_last=False,
                 t_min_high=None,
                 t_min_low=None,
                 total_count=False):
        """

        @param sigma_t: if True, use std threshold
        @param spike_t: threshold of spike
        @param peak_t: threshold of peak
        @param lag: minimum number of data points between 2 detections
        @param local_spike: if True use moving window average, else use global average
        @param resample_rule: resample resolution
        @param resample_func: 'min','max', or 'mean'
        @param upper_limit: max value to eliminate outliers
        @param lower_limit: min value to eliminate outliers
        @param fillna_value: if True fill nan
        @param fillna_method: method of fill nan
        @param iir_alpha: iir parameter
        @param rolling_window: window size to smooth the data
        @param rolling_min_periods: period of rolling window
        @param threshold: PdM threshold of number of spikes to raise alerts
        @param is_upper: if True, raise PdM alerts if above threshold
        @param include_last: if True, include the last data point in the result plot
        @param t_min_high: PdM duration threshold to raise PdM alerts
        @param t_min_low: PdM duration threshold to reset PdM alerts
        @param total_count: if True final result includes total accumulation count
        """
        # Configurations
        self.sigma_t = sigma_t
        self.spike_t = spike_t
        self.peak_t = peak_t
        self.lag = lag
        self.local_spike = local_spike
        self.resample_rule = resample_rule
        self.resample_func = resample_func
        self.upper_limit = upper_limit
        self.lower_limit = lower_limit
        self.fillna_value = fillna_value
        self.fillna_method = fillna_method
        self.iir_alpha = iir_alpha
        self.rolling_window = rolling_window
        self.rolling_min_periods = rolling_min_periods
        self.threshold = threshold
        self.is_upper = is_upper
        self.include_last = include_last
        self.t_min_high = t_min_high
        self.t_min_low = t_min_low
        self.total_count = total_count

        super(Base, self).__init__()

    def process(self, src):
        """
        Note
        --------
        Processing steps
        (1) Aggregate and fill NA with most recent non-NA value
        (2) Replace outliers with a fixed value or
            most recent non-outlier value (optional)
        (3) Apply moving average or iir filter
        (4) Remove baseline from original
        (5) Count Spikes
        (6) Compare with thresholds to get alert periods
        """

        self.reset()
        self.parse_src(src)

        if self.data is not None:

            self.results_['original'] = self.data.copy()

            # (1) Aggregate and fill NA with most recent non-NA value
            self.derived_parameter_ = copy.deepcopy(self.data)
            self.derived_parameter_ = self.derived_parameter_.resample(
                rule=self.resample_rule,
                closed='right',
                label='right',
                origin='start_day').agg(func=self.resample_func)
            self.derived_parameter_.fillna(method='ffill', inplace=True)
            self.results_['after aggregation & ffill'] = copy.deepcopy(
                self.derived_parameter_)

            # (2) Replace outliers with a fixed value or
            # most recent non-outlier value (optional)
            if self.upper_limit is not None:
                self.derived_parameter_.where(
                    self.derived_parameter_ <= self.upper_limit, inplace=True)
            if self.lower_limit is not None:
                self.derived_parameter_.where(
                    self.derived_parameter_ >= self.lower_limit, inplace=True)
            if (self.upper_limit is not None) | (self.lower_limit is not None):
                self.derived_parameter_.fillna(value=self.fillna_value,
                                               method=self.fillna_method,
                                               inplace=True)
                self.results_['after outlier removal & replacement'] = \
                    copy.deepcopy(self.derived_parameter_)

            # (3) Apply moving average or iir filter
            if self.iir_alpha is not None:
                self.derived_parameter_.loc[:] = self.iir(
                    x=self.derived_parameter_.values, alpha=self.iir_alpha)
            else:
                self.derived_parameter_ = self.derived_parameter_.rolling(
                    window=self.rolling_window,
                    min_periods=self.rolling_min_periods,
                    center=False,
                    win_type=None,
                    closed='right').min()
            self.results_['after smoothing'] = self.derived_parameter_.copy()

            # (4) Remove Baseline
            if any(self.data.index.duplicated())==True:
                self.data = self.data[~self.data.index.duplicated()]

            concat_df = pd.concat([self.derived_parameter_, self.data], axis=1)
            concat_df.columns = ['baseline', 'raw']
            concat_df.sort_index(inplace=True)
            concat_df.fillna(method='ffill', inplace=True)
            concat_df.dropna(inplace=True)
            amplitude = concat_df['raw'] - concat_df['baseline']
            self.derived_parameter_ = amplitude
            self.results_['remove baseline'] = self.derived_parameter_.copy()


            # (5) Count the spike
            if not self.local_spike:
                spike_count, peak_count = self.spike_detect(data=self.derived_parameter_, spike_t=self.spike_t, peak_t=self.peak_t,
                                                            lag=self.lag, sigma_t=self.sigma_t)
            elif self.local_spike:
                spike_count, peak_count = self.spike_detect_local(data_all=self.derived_parameter_, spike_t=self.spike_t,
                                                                  peak_t=self.peak_t, lag=self.lag)

            self.results_['spike count'] = spike_count
            spike_daily = spike_count.resample('1D').count()
            peak_daily = peak_count.resample('1D').count()

            if self.total_count:
                self.derived_parameter_ = pd.Series(data=np.cumsum(spike_daily).astype('float64'), index=spike_daily.index)
            else:
                self.derived_parameter_ = spike_daily.rolling('7D').sum().rolling('14D').mean().astype('float64')
                # self.derived_parameter_ = spike_daily.rolling('7D').sum().rolling('7D').mean().astype('float64')
                #self.derived_parameter_ = spike_daily.rolling('14D').max().astype('float64')
            self.results_['agg count'] = copy.deepcopy(self.derived_parameter_)

            # (6) Compare with thresholds to get alert periods
            if self.threshold is not None:
                self.alert_ = cal_alert_periods(
                    data=self.derived_parameter_, threshold=self.threshold,
                    is_upper=self.is_upper, include_last=self.include_last,
                    t_min_high=self.t_min_high, t_min_low=self.t_min_low)


    @staticmethod
    def iir(x, alpha):
        """ IIR filter """

        y = copy.deepcopy(x)
        if len(x) > 1:
            y[0] = x[0]
            for i in range(1, len(x)):
                y[i] = (1 - alpha) * y[i - 1] + alpha * x[i]
        # y, _ = signal.lfilter(b=[alpha],
        #                       a=[1, alpha - 1],
        #                       x=x,
        #                       zi=[x[0] * (1 - alpha)])
        return y

    @staticmethod
    def spike_detect(data, spike_t, peak_t, lag, sigma_t=True):
        """
        To detect spike and count the number by using std threshold

        :param data: data array with datetime index
        :param spike_t: std threshold for spike as float, real value threshold as tuple
        :param peak_t: std threshold for peak as float, real value threshold as tuple
        :param lag: within lag it count 1 spike
        :param sigma_t: use standard deviation threshold
        :return spike_count: number of spike
        """
        data = data.dropna().sort_index()
        if sigma_t:
            std = np.std(data)
            avg = np.mean(data)
            spike_upper_limit = avg + spike_t * std
            spike_lower_limit = avg - spike_t * std
            peak_upper_limit = avg + peak_t * std
            peak_lower_limit = avg - peak_t * std
        else:
            spike_upper_limit = spike_t[1]
            spike_lower_limit = spike_t[0]
            peak_upper_limit = peak_t[1]
            peak_lower_limit = peak_t[0]
        pre_spike = [1 if (i > spike_upper_limit or i < spike_lower_limit) else 0 for i in data]
        spike = [1 if (pre_spike[i] == 1 and sum(pre_spike[int(i + 1):int(i + lag)]) == 0) else 0 for i in range(len(pre_spike))]
        spike_count = [sum(spike[:i + 1]) if spike[i] == 1 else np.nan for i in range(len(spike))]
        spike = pd.Series(spike, data.index)
        spike_count = pd.Series(spike_count, data.index)
        spike_count.dropna(inplace=True)

        pre_peak = [1 if (peak_upper_limit < i < spike_upper_limit) else 0 for i in data]
        peak = [1 if (pre_peak[i] == 1 and sum(pre_peak[int(i + 1):int(i + lag)]) == 0) else 0 for i in range(len(pre_peak))]
        peak_count = [sum(peak[:i + 1]) if peak[i] == 1 else np.nan for i in range(len(peak))]
        peak = pd.Series(peak, data.index)
        peak_count = pd.Series(peak_count, data.index)
        peak_count.dropna(inplace=True)

        return spike_count, peak_count

    @staticmethod
    def spike_detect_local(data_all, spike_t, peak_t, lag, window_size=50000):
        """
        To detect spike and count the number within a local window

        :param data_all: data array with datetime index
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

            for i in range(N + 1):
                if i != N:
                    data = data_all[window_size * i: window_size * (i + 1)]
                else:
                    data = data_all[window_size * i: window_size * (i + 1) + R]
                std = np.std(data)
                avg = np.mean(data)
                spike_upper_limit = avg + spike_t * std
                spike_lower_limit = avg - spike_t * std
                pre_spike = [1 if (i > spike_upper_limit or i < spike_lower_limit) else 0 for i in data]
                spike = [1 if (pre_spike[i] == 1 and sum(pre_spike[int(i + 1):int(i + lag)]) == 0) else 0 for i in
                         range(len(pre_spike))]
                spike = pd.Series(spike, data.index)
                spike_all = spike_all.append(spike)

                peak_upper_limit = avg + peak_t * std
                peak_lower_limit = avg - peak_t * std
                pre_peak = [1 if (peak_upper_limit < i < spike_upper_limit) else 0 for i in data]
                peak = [1 if (pre_peak[i] == 1 and sum(pre_peak[int(i + 1):int(i + lag)]) == 0) else 0 for i in
                        range(len(pre_peak))]
                peak = pd.Series(peak, data.index)
                peak_all = peak_all.append(peak)

            spike_count = [sum(spike_all[:i + 1]) if spike_all[i] == 1 else np.nan for i in range(1, len(spike_all))]

            spike_count = pd.Series(spike_count, data_all.index)
            spike_count.dropna(inplace=True)

            peak_count = [sum(peak_all[:i + 1]) if peak_all[i] == 1 else np.nan for i in range(1, len(peak_all))]

            peak_count = pd.Series(peak_count, data_all.index)
            peak_count.dropna(inplace=True)

        else:
            data = data_all

            std = np.std(data)
            avg = np.mean(data)
            spike_upper_limit = avg + spike_t * std
            spike_lower_limit = avg - spike_t * std
            pre_spike = [1 if (i > spike_upper_limit or i < spike_lower_limit) else 0 for i in data]
            spike = [1 if (pre_spike[i] == 1 and sum(pre_spike[i + 1:i + lag]) == 0) else 0 for i in
                     range(len(pre_spike))]
            spike_count = [sum(spike[:i + 1]) if spike[i] == 1 else np.nan for i in range(len(spike))]
            spike = pd.Series(spike, data.index)
            spike_count = pd.Series(spike_count, data.index)
            spike_count.dropna(inplace=True)

            peak_upper_limit = avg + peak_t * std
            peak_lower_limit = avg - peak_t * std
            pre_peak = [1 if (i > peak_upper_limit) else 0 for i in data]
            peak = [1 if (pre_peak[i] == 1 and sum(pre_peak[i + 1:i + lag]) == 0) else 0 for i in range(len(pre_peak))]
            peak_count = [sum(peak[:i + 1]) if peak[i] == 1 else np.nan for i in range(len(peak))]
            peak = pd.Series(peak, data.index)
            peak_count = pd.Series(peak_count, data.index)
            peak_count.dropna(inplace=True)

        return spike_count, peak_count

    @staticmethod
    def spike_detect_above_mean(data, rolling_window='1D', th_level=0, th_spike=(0, np.inf), th_mean=(0, np.inf),
                                th_diff=(0, np.inf), th_duration=('1s', '10D')):
        data = data.dropna().sort_index()
        data_mean = data.rolling(rolling_window).agg('mean')
        data_diff = data - data_mean
        df = pd.DataFrame()
        df['diff'] = data_diff
        df['level'] = [0 if i<=th_level else 1 for i in df['diff']]
        df['change'] = df['level'].diff(1)
        df['start'] = np.nan
        df['stop'] = np.nan
        df['start'][:-1] = [df.index[i] if (df.iloc[i+1]['change']==1) else np.nan for i in range(len(df.index)-1)]
        df['stop'] = [df.index[i] if (df.iloc[i]['change'] == -1) else np.nan for i in range(len(df.index))]
        df_spike = pd.DataFrame()
        df_spike['start'] = df['start'].dropna().values[:len(df['stop'].dropna().values)]
        df_spike['stop'] = df['stop'].dropna().values
        df_spike['duration'] = df_spike['stop'] - df_spike['start']
        min_duration = pd.to_timedelta(th_duration[0])
        max_duration = pd.to_timedelta(th_duration[1])
        df_spike = df_spike[(df_spike['duration'] >= min_duration) & (df_spike['duration'] <= max_duration)]
        try:
            assert all(df_spike['duration']>pd.Timedelta('0.001s'))
        except AssertionError:
            print('Miss match between start and stop')
        df_spike['max'] = [max(data[df_spike.iloc[i]['start']:df_spike.iloc[i]['stop']]) for i in
                           range(len(df_spike.index))]
        df_spike['min'] = [min(data[df_spike.iloc[i]['start']:df_spike.iloc[i]['stop']]) for i in
                           range(len(df_spike.index))]
        df_spike['diff'] = df_spike['max'] - df_spike['min']
        df_spike['mean'] = [np.mean(data[df_spike.iloc[i]['start']:df_spike.iloc[i]['stop']]) for i in
                           range(len(df_spike.index))]

        df_spike = df_spike[df_spike['max'] >= th_spike[0]]
        df_spike = df_spike[df_spike['min'] <= th_spike[1]]
        df_spike = df_spike[(th_mean[0] <=df_spike['mean']) & (df_spike['mean'] <= th_mean[1])]
        df_spike = df_spike[(th_diff[0] <= df_spike['diff']) & (df_spike['diff'] <= th_diff[1])]

        df_spike['peak_ts'] = [data[df_spike.iloc[i]['start']:df_spike.iloc[i]['stop']][data==max(data[df_spike.iloc[i]['start']:df_spike.iloc[i]['stop']])].index[0] for i in
                               range(len(df_spike.index))]

        df_spike['gap'] = np.nan
        if len(df_spike.index) > 1:
            df_spike['gap'][:-1] = [df_spike.iloc[i+1]['start'] - df_spike.iloc[i]['stop'] for i in
                                    range(len(df_spike.index)-1)]

        return df_spike