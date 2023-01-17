"""Class for computing independent spike-related derived parameters."""

# Authors: Dennis Hou <duanyang.hou@edwardsvacuum.com>,
#          Danny Sun <duo.sun@edwardsvacuum.com>

from ._spike import Spike
from ._spike2 import Spike2
import copy
import pandas as pd
import numpy as np
from ._base import Base
from ..vis._plt_maximize import plt_maximize
import matplotlib.pyplot as plt
import os
import datetime
from ..utils._cal_alert_periods import cal_alert_periods
from .. import COLOR_ALERT

class IndependentSpike(Base):
    """ Independent spike detection an count """

    def __init__(self,
                 th_slope_1=None,
                 th_slope_2=None,
                 src_1_th=(0, np.inf),
                 src_2_th=(0, np.inf),
                 th_duration_1='1m',
                 th_duration_2='1m',
                 rolling_window='10m',
                 raw_data = True,
                 resample_window='30s',
                 method_data_points=3,
                 rolling_window_mean_1='1D',
                 th_level_1=0,
                 th_spike_1=(0, np.inf),
                 th_mean_1=(0, np.inf),
                 th_diff_1=(0, np.inf),
                 th_duration_mean_1= ('1s', '10D'),
                 rolling_window_mean_2='1D',
                 th_level_2=0,
                 th_spike_2=(0, np.inf),
                 th_mean_2=(0, np.inf),
                 th_diff_2=(0, np.inf),
                 th_duration_mean_2=('1s', '10D'),
                 threshold={'advisory': 2,
                            'warning': 5,
                            'alarm': 10},
                 is_upper=True,
                 include_last=False,
                 t_min_high={'advisory': datetime.timedelta(days=4),
                             'warning': datetime.timedelta(days=3),
                             'alarm': datetime.timedelta(days=2)},
                 t_min_low={'advisory': datetime.timedelta(days=15),
                            'warning': datetime.timedelta(days=15),
                            'alarm': datetime.timedelta(days=15)}
                 ):
        """

        @param th_slope_1: threshold of gradient/slope tuple (up_threshold, down_threshold) for target input src1
        @param th_slope_2: threshold of gradient/slope tuple (up_threshold, down_threshold) for input src2
        @param src_1_th: threshold range for src1 spike amplitude as big
        @param src_2_th: threshold range for src2 spike amplitude as small
        @param th_duration_1: maximun duration between start and stop of a spike for src1
        @param th_duration_2: maximun duration between start and stop of a spike for src2
        @param rolling_window: window length for the mean/baseline to remove
        @param raw_data: define if original raw data is used
        @param resample_window: if not raw_data, specify resample window if required fixed interval time series
        @param method_data_points: define to use how many data points finding a spike (2:up-peak, 3:up-peak-down, 4:remove baseline)
        @param rolling_window_mean_1: window length for the mean/baseline to remove from src1
        @param th_level_1: level threshold to eliminate small value points above/below the mean/baseline for src1
        @param th_spike_1: spike threshold to filter amplitude for src1
        @param th_mean_1: mean threshold to filter baseline for src1
        @param th_diff_1: difference threshold to filter changes in amplitude for src1
        @param th_duration_mean_1: duration threshold of spike of src1
        @param rolling_window_mean_2: window length for the mean/baseline to remove from src2
        @param th_level_2: level threshold to eliminate small value points above/below the mean/baseline for src2
        @param th_spike_2: spike threshold to filter amplitude for src2
        @param th_mean_2: mean threshold to filter baseline for src2
        @param th_diff_2: difference threshold to filter changes in amplitude for src2
        @param th_duration_mean_2: duration threshold of spike of src2
        @param threshold: PdM threshold of number of spikes to raise alerts
        @param is_upper: if True, raise PdM alerts if above threshold
        @param include_last: if True, include the last data point in the result plot
        @param t_min_high: PdM duration threshold to raise PdM alerts
        @param t_min_low: PdM duration threshold to reset PdM alerts
        """
        # Configurations
        self.th_slope_1 = th_slope_1
        self.th_slope_2 = th_slope_2
        self.src_1_th = src_1_th
        self.src_2_th = src_2_th
        self.th_duration_1 = th_duration_1
        self.th_duration_2 = th_duration_2
        self.raw_data = raw_data
        self.rolling_window = rolling_window
        self.resample_window = resample_window
        self.method_data_points = method_data_points
        self.rolling_window_mean_1 = rolling_window_mean_1
        self.th_level_1 = th_level_1
        self.th_spike_1 = th_spike_1
        self.th_mean_1 = th_mean_1
        self.th_diff_1 = th_diff_1
        self.th_duration_mean_1 = th_duration_mean_1
        self.rolling_window_mean_2 = rolling_window_mean_2
        self.th_level_2 = th_level_2
        self.th_spike_2 = th_spike_2
        self.th_mean_2 = th_mean_2
        self.th_diff_2 = th_diff_2
        self.th_duration_mean_2 = th_duration_mean_2
        self.is_upper = is_upper
        self.include_last = include_last
        self.threshold = threshold
        self.t_min_high = t_min_high
        self.t_min_low = t_min_low

        self.parameter_name = None
        self.parameter_name_2 = None

        super(Base, self).__init__()

    def process(self, src_1, src_2):
        """
        Note
        --------
        Processing steps
        (1) Locate spike for src_1
        (2) Locate spike for src_2
        (3) Locate independent spike using rule 1
        (4) Locate independent spike using rule 2
        (5) Combine independent spike of rule 1 and rule 2
        (6) Count daily spike
        (7) Aggregate daily count spike
        """

        self.reset()
        self.parse_src(src_1, src_2)

        if self.data is not None and self.data_2 is not None:

            self.results_['original'] = self.data.copy()
            if not self.raw_data:
                self.results_['aggregated'] = self.data.dropna().resample(self.resample_window).agg('max').fillna(method='ffill')
            # (1) Locate spike for src_1
            self.derived_parameter_ = copy.deepcopy(self.data)
            if self.method_data_points < 4:
                if self.raw_data:
                    spike_1 = Spike(th_slope=self.th_slope_1, th_duration=self.th_duration_1,
                                    method_data_points=self.method_data_points)
                else:
                    spike_1 = Spike(th_slope=self.th_slope_1, th_duration=self.th_duration_1,
                                    resample_window=self.resample_window, method_data_points=self.method_data_points)
            else:
                spike_1 = Spike2(rolling_window = self.rolling_window_mean_1, th_level=self.th_level_1,
                                 th_spike=self.th_spike_1,
                                 th_mean=self.th_mean_1, th_diff=self.th_diff_1,
                                 th_duration=self.th_duration_mean_1,
                                 resample_window=self.resample_window)
                self.data = self.data.dropna().resample(self.resample_window).agg('max')
            spike_1.process(src_1)
            src_1_spike = spike_1.results_['after locate']

            self.derived_parameter_ = src_1_spike
            self.results_['locate spike main'] = copy.deepcopy(self.derived_parameter_)

            # (2) Locate spike for src_2
            if self.method_data_points < 4:
                if self.raw_data:
                    spike_2 = Spike(th_slope=self.th_slope_2, th_duration=self.th_duration_2,
                                    method_data_points=self.method_data_points)
                else:
                    spike_2 = Spike(th_slope=self.th_slope_2, th_duration=self.th_duration_2,
                                    resample_window=self.resample_window, method_data_points=self.method_data_points)
            else:
                spike_2 = Spike2(rolling_window = self.rolling_window_mean_1, th_level=self.th_level_2,
                                 th_spike=self.th_spike_2,
                                 th_mean=self.th_mean_2, th_diff=self.th_diff_2,
                                 th_duration=self.th_duration_mean_2,
                                 resample_window=self.resample_window)
                self.data_2 = self.data_2.dropna().resample(self.resample_window).agg('max')
            spike_2.process(src_2)
            src_2_spike = spike_2.results_['after locate']

            self.derived_parameter_ = src_2_spike
            self.results_['locate spike second'] = copy.deepcopy(self.derived_parameter_)

            # (3) Locate independent spike using rule 1
            if len(src_1_spike) > 0 and len(src_2_spike)>0:
                src_1_erratic_spike_r1, _ = self.erratic_spike_r1(self.data, src_1_spike, self.data_2, src_2_spike,
                                                             self.rolling_window, self.resample_window)
                self.derived_parameter_ = src_1_erratic_spike_r1
            #self.results_['independent spike rule 1'] = copy.deepcopy(self.derived_parameter_)

            # (4) Locate independent spike using rule 2
                src_1_erratic_spike_r2, _ = self.erratic_spike_r2(src_1=self.data, src_1_spike=src_1_spike,
                                                                  src_2=self.data_2, src_2_spike=src_2_spike,
                                                                  src_1_th=self.src_1_th, src_2_th=self.src_2_th,
                                                                  window_width=self.rolling_window,
                                                                  resample_window=self.resample_window)
                self.derived_parameter_ = src_1_erratic_spike_r2
            #self.results_['independent spike rule 2'] = copy.deepcopy(self.derived_parameter_)

            # (5) Combine independent spike of rule 1 and rule 2
                src_1_erratic_spike = src_1_erratic_spike_r1.append(src_1_erratic_spike_r2)
                self.derived_parameter_ = src_1_erratic_spike
                self.results_['independent spike rule 1 and 2'] = copy.deepcopy(self.derived_parameter_)

            # (6) Count daily spike
                base_count = pd.Series(float(0), index=self.data.resample('1D').count().index)
                spike_count = pd.Series(1, index=src_1_erratic_spike.index).resample('1D').count().astype('float64')
                self.derived_parameter_ = base_count.add(spike_count, fill_value=0)
                self.results_['after count'] = copy.deepcopy(self.derived_parameter_)
            else:
                base_count = pd.Series(float(0), index=self.data.resample('1D').count().index)
                self.derived_parameter_ = base_count
                self.results_['after count'] = copy.deepcopy(self.derived_parameter_)

            # (7) Aggregate count daily spike
            self.derived_parameter_ = self.derived_parameter_.rolling('14D').agg(sum).rolling('14D').max().rolling('28D').mean()
            self.results_['agg count'] = copy.deepcopy(self.derived_parameter_)

            # (8) Compare with thresholds to get alert periods
            if self.threshold is not None and len(self.derived_parameter_) != 0:
                self.alert_ = cal_alert_periods(
                    data=self.derived_parameter_, threshold=self.threshold,
                    is_upper=self.is_upper, include_last=self.include_last,
                    t_min_high=self.t_min_high, t_min_low=self.t_min_low)

        pass

    def plot_results(self,
                     color: dict[str, str] = COLOR_ALERT,
                     path: str = None):
        if self.method_data_points > 3:
            self.data = self.data.dropna().resample(self.resample_window).agg('max').fillna(method='ffill')
            self.data_2 = self.data_2.dropna().resample(self.resample_window).agg('max').fillna(method='ffill')
        if self.results_:

            if self.alert_ is not None:
                nrows = len(self.results_) + 1
            else:
                nrows = len(self.results_)

            fig, axes = plt.subplots(nrows=nrows, ncols=1, sharex=True)

            for i, (k, v) in enumerate(self.results_.items()):
                if k == 'locate spike main' or  k == 'independent spike rule 1' \
                        or k == 'independent spike rule 2' or k == 'independent spike rule 1 and 2':
                    axes[i].plot(self.data.index,  self.data.values)
                    try:
                        axes[i].plot(v.index, v.values, 'o', color='r', alpha=0.3)
                        axes[i].set_ylabel(v.name)
                    except:
                        pass
                    axes[i].set_xlabel('')

                    axes[i].set_title(k)
                elif k == 'locate spike second':
                    axes[i].plot(self.data_2.index,  self.data_2.values)
                    try:
                        axes[i].plot(v.index, v.values, 'o', color='r', alpha=0.3)
                        axes[i].set_ylabel(v.name)
                    except:
                        pass
                    axes[i].set_xlabel('')

                    axes[i].set_title(k)
                elif k == 'after count':
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

    def parse_src(self, src, src_2):
        """ Extract data, system_name, parameter_name from `src` """
        if isinstance(src, pd.Series) | isinstance(src, pd.DataFrame):
            self.data = src
        elif isinstance(src, dict):
            self.data = src.get('data', 'None')
            self.system_name = src.get('system_name', 'None')
            self.parameter_name = src.get('parameter_name', 'None')
        else:
            self.data = src.data
            try:
                self.system_name = src.system_name
            except AttributeError:
                pass
            try:
                self.parameter_name = src.parameter_name
            except AttributeError:
                pass

        if (self.parameter_name is None) & isinstance(self.data, pd.Series):
            self.parameter_name = self.data.name

        if isinstance(src_2, pd.Series) | isinstance(src_2, pd.DataFrame):
            self.data_2 = src_2
        elif isinstance(src_2, dict):
            self.data_2 = src_2.get('data', 'None')
            self.system_name_2 = src_2.get('system_name', 'None')
            self.parameter_name_2 = src_2.get('parameter_name', 'None')
        else:
            self.data_2 = src_2.data
            try:
                self.system_name_2 = src_2.system_name_2
            except AttributeError:
                pass
            try:
                self.parameter_name_2 = src_2.parameter_name_2
            except AttributeError:
                pass

        if (self.parameter_name_2 is None) & isinstance(self.data_2, pd.Series):
            self.parameter_name_2 = self.data_2.name


    def erratic_spike_r1(self, src_1, src_1_spike, src_2, src_2_spike, rolling_window='10m', resample_window='30s'):
        """
        Detect erratic spike where there is a spike in src_1 and there is no spike in src_2
        :param src_1: dp Series with datetime index
        :param src_1_spike: dp spike Series with datetime index
        :param src_2: mb Series with datetime index
        :param src_2_spike: mb spike Series with datetime index
        :param rolling_window: search synchronise spike in this window size
        :param resample_window: select maximum peak point in batch within the resample window
        :return: Datetime indexed Series for dp erratic spikes, the first one with src_1 amplitude,
        second with mb_data amplitude
        """
        src_1.dropna(inplace=True)
        src_2.dropna(inplace=True)
        steps = round(pd.to_timedelta(rolling_window) / pd.to_timedelta(resample_window))

        df_dp = pd.DataFrame(index=src_1.index, columns=[self.parameter_name, 'spike_1', 'index_1'])
        df_dp[self.parameter_name] = src_1
        df_dp['spike_1'] = [1 if i in src_1_spike.index else 0 for i in src_1.index]
        df_dp['index_1'] = pd.to_datetime(src_1.index)
        df_dp = df_dp.resample(resample_window).max()

        df_mb = pd.DataFrame(index=src_2.index, columns=[self.parameter_name_2, 'spike_2', 'index_2'])
        df_mb[self.parameter_name_2] = src_2
        df_mb['spike_2'] = [1 if i in src_2_spike.index else 0 for i in src_2.index]
        df_mb['index_2'] = pd.to_datetime(src_2.index)
        df_mb = df_mb.resample(resample_window).max()

        df = pd.merge(df_dp, df_mb, left_index=True, right_index=True)
        df.fillna(method='ffill', inplace=True)
        for i in range(-round(steps / 2), round(steps / 2)):
            if i != 0:
                mb_s_title = 'spike_2_' + str(i)
                df[mb_s_title] = df['spike_2'].shift(i)

        spike_cols = ['spike_1', 'spike_2']
        for i in range(-round(steps / 2), round(steps / 2)):
            if i != 0:
                spike_cols.append('spike_2_' + str(i))

        df['spike'] = df[spike_cols].sum(axis=1)
        df = df[df['spike'] != 0]

        df_erratic = df[(df['spike'] == 1) & (df['spike_1'] == 1)]
        df_erratic.set_index('index_1', inplace=True)
        df_erratic = df_erratic[~df_erratic.index.duplicated()]
        return df_erratic[self.parameter_name], df_erratic[self.parameter_name_2]


    def erratic_spike_r2(self, src_1, src_1_spike, src_2, src_2_spike,
                         src_1_th=None, src_2_th=None, window_width='10m', resample_window='30s'):
        """
        Detect erratic spike where there is a big spike in src_1 even if there is a small spike in src_2
        but mb_data within range of mb_th
        :param src_1: dp Series with datetime index
        :param src_1_spike: dp spike Series with datetime index
        :param src_2: mb Series with datetime index
        :param src_2_spike: mb spike Series with datetime index
        :param src_1_th: threshold range for dp spike amplitude as big
        :param src_2_th: threshold range for mb spike amplitude as small
        :param window_width: search synchronise spike in this window size
        :param resample_window: select maximum peak point in batch within the resample window
        :return: Datetime indexed Series for dp erratic spikes, the first one with dp_data value, second with mb_data value
        """
        src_1.dropna(inplace=True)
        src_2.dropna(inplace=True)
        steps = round(pd.to_timedelta(window_width) / pd.to_timedelta(resample_window))

        df_dp = pd.DataFrame(index=src_1.index, columns=[self.parameter_name, 'spike_1', 'index_1'])
        df_dp[self.parameter_name] = src_1
        df_dp['spike_1'] = [1 if i in src_1_spike.index else 0 for i in src_1.index]
        df_dp['index_1'] = pd.to_datetime(src_1.index)
        df_dp = df_dp.resample(resample_window).max()

        df_mb = pd.DataFrame(index=src_2.index, columns=[self.parameter_name_2, 'spike_2', 'index_2'])
        df_mb[self.parameter_name_2] = src_2
        df_mb['spike_2'] = [1 if i in src_2_spike.index else 0 for i in src_2.index]
        df_mb['index_2'] = pd.to_datetime(src_2.index)
        df_mb = df_mb.resample(resample_window).max()

        df = pd.merge(df_dp, df_mb, left_index=True, right_index=True)
        df.fillna(method='ffill', inplace=True)
        for i in range(-round(steps / 2), round(steps / 2)):
            if i != 0:
                mb_s_title = 'spike_2_' + str(i)
                df[mb_s_title] = df['spike_2'].shift(i)

        spike_cols = ['spike_1', 'spike_2']
        spike2_cols = ['spike_2']
        for i in range(-round(steps / 2), round(steps / 2)):
            if i != 0:
                spike_cols.append('spike_2_' + str(i))
                spike2_cols.append('spike_2_' + str(i))

        df['spike'] = df[spike_cols].sum(axis=1)
        df['spike_2_all'] = df[spike2_cols].sum(axis=1)
        df = df[df['spike'] != 0]
        if src_1_th is None:
            df_erratic = df[(df['spike_1'] == 1) & (df['spike_2_all'] >= 1) &
                            (df[self.parameter_name_2] <= src_2_th[1]) & (df[self.parameter_name_2] >= src_2_th[0])]
        else:
            df_erratic = df[(df['spike_1'] == 1) & (df['spike_2_all'] >= 1) &
                            (df[self.parameter_name_2] <= src_2_th[1]) & (df[self.parameter_name_2] >= src_2_th[0])&
                            (df[self.parameter_name] <= src_1_th[1]) & (df[self.parameter_name] >= src_1_th[0])]

        df_erratic.set_index('index_1', inplace=True)
        df_erratic = df_erratic[~df_erratic.index.duplicated()]
        return df_erratic[self.parameter_name], df_erratic[self.parameter_name_2]