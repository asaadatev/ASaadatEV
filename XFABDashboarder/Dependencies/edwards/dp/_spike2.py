"""Class for computing spike-related derived parameter. Using area above mean"""

# Authors: Dennis Hou <duanyang.hou@edwardsvacuum.com>,
#          Danny Sun <duo.sun@edwardsvacuum.com>

import copy
import pandas as pd
import numpy as np
from ._base import Base
from ..vis._plt_maximize import plt_maximize
import matplotlib.pyplot as plt
import os
import datetime
from .. import COLOR_ALERT

class Spike2(Base):
    """ Spike detection and count based on data point above mean """

    def __init__(self,
                 rolling_window='1D',
                 th_level=0,
                 th_spike=(0, np.inf),
                 th_mean=(0, np.inf),
                 th_diff=(0, np.inf),
                 th_duration=('1s', '10D'),
                 resample_window=None,
                 threshold={'advisory': 2,
                            'warning': 5,
                            'alarm': 10},
                 t_min_high={'advisory': datetime.timedelta(days=4),
                             'warning': datetime.timedelta(days=3),
                             'alarm': datetime.timedelta(days=2)},
                 t_min_low={'advisory': datetime.timedelta(days=15),
                            'warning': datetime.timedelta(days=15),
                            'alarm': datetime.timedelta(days=15)}
                 ):
        """

        @param rolling_window: window length for the mean/baseline to remove
        @param th_level: level threshold to eliminate small value points above/below the mean/baseline
        @param th_spike: spike threshold to filter amplitude
        @param th_mean: mean threshold to filter baseline
        @param th_diff: difference threshold to filter changes in amplitude
        @param th_duration: duration threshold of spike
        @param resample_window: resample window if required fixed interval time series
        @param threshold: PdM threshold of number of spikes to raise alerts
        @param t_min_high: PdM duration threshold to raise PdM alerts
        @param t_min_low: PdM duration threshold to reset PdM alerts
        """
        # Configurations
        self.rolling_window = rolling_window
        self.th_level = th_level
        self.th_spike = th_spike
        self.th_mean = th_mean
        self.th_diff = th_diff
        self.th_duration = th_duration
        self.resample_window = resample_window
        self.threshold = threshold
        self.t_min_high = t_min_high
        self.t_min_low = t_min_low

        super(Base, self).__init__()

    def process(self, src):
        """
        Note
        --------
        Processing steps
        (1) Aggregate and fill NA with most recent non-NA value
        (2) Locate spike
        (3) Count daily spike
        (4) Compare with thresholds to get alert periods
        """

        self.reset()
        self.parse_src(src)

        if self.data is not None:

            self.results_['original'] = self.data.copy()

            # (1) Drop NA values
            if self.resample_window is None:
                self.derived_parameter_ = copy.deepcopy(self.data.dropna())
            else:
                self.derived_parameter_ = copy.deepcopy(self.data.dropna().resample(self.resample_window).agg('max'))

            # self.results_['after dropna'] = copy.deepcopy(self.derived_parameter_)

            # (2) Locate spike
            data = self.derived_parameter_.sort_index().fillna(method='ffill')
            # data_mean = data.rolling(self.rolling_window).agg('mean')
            # To be same with matlab
            rolling_steps = pd.Timedelta(self.rolling_window) / pd.Timedelta(self.resample_window)
            data_mean = data.rolling(int(rolling_steps)).mean().fillna(method='bfill')

            data_diff = data - data_mean
            df = pd.DataFrame()
            df['diff'] = data_diff
            df['level'] = [0 if i <= self.th_level else 1 for i in df['diff']]
            df['change'] = df['level'].diff(1)
            df['start'] = np.nan
            df['stop'] = np.nan
            df['start'][:-1] = [df.index[i] if (df.iloc[i + 1]['change'] == 1) else np.nan for i in
                                range(len(df.index) - 1)]
            df['stop'] = [df.index[i] if (df.iloc[i]['change'] == -1) else np.nan for i in range(len(df.index))]
            df_spike = pd.DataFrame()
            min_length = min([len(df['start'].dropna().values), len(df['stop'].dropna().values)])
            if df['start'].dropna().values[0] <= df['stop'].dropna().values[0]:
                df_spike['start'] = df['start'].dropna().values[:min_length]
                df_spike['stop'] = df['stop'].dropna().values[:min_length]
            else:
                df_spike['start'] = df['start'].dropna().values[:len(df['stop'].dropna().values)-1]
                df_spike['stop'] = df['stop'].dropna().values[1:]

            df_spike['duration'] = df_spike['stop'] - df_spike['start']
            min_duration = pd.to_timedelta(self.th_duration[0])
            max_duration = pd.to_timedelta(self.th_duration[1])
            df_spike = df_spike[(df_spike['duration'] >= min_duration) & (df_spike['duration'] <= max_duration)]
            try:
                assert all(df_spike['duration'] > pd.Timedelta('0.001s'))
            except AssertionError:
                print('Miss match between start and stop')
            df_spike['max'] = [max(data[df_spike.iloc[i]['start']:df_spike.iloc[i]['stop']]) for i in
                               range(len(df_spike.index))]
            df_spike['min'] = [min(data[df_spike.iloc[i]['start']:df_spike.iloc[i]['stop']]) for i in
                               range(len(df_spike.index))]
            df_spike['diff'] = df_spike['max'] - df_spike['min']
            df_spike['mean'] = [np.mean(data[df_spike.iloc[i]['start']:df_spike.iloc[i]['stop']]) for i in
                                range(len(df_spike.index))]

            df_spike = df_spike[df_spike['max'] >= self.th_spike[0]]
            df_spike = df_spike[df_spike['min'] <= self.th_spike[1]]
            df_spike = df_spike[(self.th_mean[0] <= df_spike['mean']) & (df_spike['mean'] <= self.th_mean[1])]
            df_spike = df_spike[(self.th_diff[0] <= df_spike['diff']) & (df_spike['diff'] <= self.th_diff[1])]
            df_spike['peak_ts'] = [data[df_spike.iloc[i]['start']:df_spike.iloc[i]['stop']][
                                       data == max(data[df_spike.iloc[i]['start']:df_spike.iloc[i]['stop']])].index[0]
                                   for i in
                                   range(len(df_spike.index))]
            df_spike['gap'] = np.nan
            if len(df_spike.index) > 1:
                df_spike['gap'][:-1] = [df_spike.iloc[i + 1]['start'] - df_spike.iloc[i]['stop'] for i in
                                        range(len(df_spike.index) - 1)]

            self.derived_parameter_ = data[df_spike['peak_ts']]
            print(data[df_spike['peak_ts']])
            if len(df_spike['peak_ts']) > 0:
                self.results_['after locate'] = copy.deepcopy(self.derived_parameter_)
                self.results_['spike duration s'] = copy.deepcopy(pd.Series(df_spike['duration'].dt.total_seconds().values, index=df_spike['start']))
                if len(df_spike['peak_ts']) > 1:
                    self.results_['spike gap D'] = copy.deepcopy(pd.Series(df_spike['gap'].fillna(method='ffill').dt.total_seconds().values, index=df_spike['start'])/86400)
                else:
                    self.results_['spike gap D'] = copy.deepcopy(pd.Series(0, index=df_spike['start']))

            else:
                self.results_['after locate'] = []
                self.results_['spike duration s'] = []
                self.results_['spike gap D'] = []


            # (3) Count daily spike
            base_count = pd.Series(float(0), index=self.data.resample('1D').count().index)
            if len(df_spike['peak_ts']) > 0:
                spike_count = pd.Series(1, index=df_spike['peak_ts']).resample('1D').count().astype('float64')
                self.derived_parameter_ = base_count.add(spike_count, fill_value=0)
            else:
                self.derived_parameter_ = base_count
            self.results_['after count'] = copy.deepcopy(self.derived_parameter_)


            # (4) Aggregation
            # self.derived_parameter_ = self.derived_parameter_.rolling('14D').agg(sum).rolling('14D').max().rolling('28D').mean()
            # self.results_['after aggregation'] = copy.deepcopy(self.derived_parameter_)

        pass
    def plot_results(self,
                     color: dict[str, str] = COLOR_ALERT,
                     path: str = None):

        if self.results_:

            if self.alert_ is not None:
                nrows = len(self.results_) + 1
            else:
                nrows = len(self.results_)

            fig, axes = plt.subplots(nrows=nrows, ncols=1, sharex=True)

            for i, (k, v) in enumerate(self.results_.items()):
                if k == 'after locate':
                    axes[i].plot(self.results_['original'].index,  self.results_['original'].values)
                    axes[i].plot(v.index, v.values, 'o', color='r', alpha=0.3)
                    axes[i].set_xlabel('')
                    axes[i].set_ylabel(v.name)
                    axes[i].set_title(k)
                elif k == 'after count' or k=='spike duration s' or k=='spike gap D':
                    axes[i].bar(v.index, v.values)
                    axes[i].set_xlabel('')
                    axes[i].set_ylabel(v.name)
                    axes[i].set_title(k)
                else:
                    axes[i].plot(v.index, v.values)
                    axes[i].set_xlabel('')
                    axes[i].set_ylabel(v.name)
                    axes[i].set_title(k)
                axes[i].grid()

            if self.alert_ is not None:
                periods = self.alert_.get('periods')
                for level in self.alert_.get('levels'):
                    if periods.get(level) is not None:
                        for j in range(periods.get(level).shape[0]):
                            axes[len(self.results_) - 1].axvspan(
                                periods.get(level).loc[j, 'start'],
                                periods.get(level).loc[j, 'end'],
                                alpha=1, color=color.get(level))
                axes[len(self.results_)].plot(self.alert_.get('signal').index,
                                              self.alert_.get('signal').values)
                axes[len(self.results_)].set_xlabel('')
                axes[len(self.results_)].set_ylabel('Alert')
                axes[len(self.results_)].set_title('')

            plt.subplots_adjust(left=None, bottom=None, right=None, top=None,
                                wspace=0.2, hspace=0.6)
            fig.suptitle(self.system_name, horizontalalignment='left',
                         x=0.10, y=0.95)
            plt_maximize()
            self.graph_results_ = fig
            # fig.show()
            if path is not None:
                if not os.path.exists(path):
                    os.makedirs(path)
                self.graph_results_.savefig(f'{path}/{self.system_name}.png')


