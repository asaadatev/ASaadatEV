"""Class for computing spike-related derived parameter. Using 3 data points"""

# Authors: Dennis Hou <duanyang.hou@edwardsvacuum.com>,
#          Danny Sun <duo.sun@edwardsvacuum.com>

import copy
import pandas as pd
from ._base import Base
from ..vis._plt_maximize import plt_maximize
import matplotlib.pyplot as plt
import os
import datetime
from .. import COLOR_ALERT

class Spike(Base):
    """ Spike dection and count base on gradient/slope"""

    def __init__(self,
                 th_slope,
                 th_duration='1T',
                 resample_window=None,
                 method_data_points=3,
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

        @param th_slope: threshold of gradient/slope tuple (up_threshold, down_threshold)
        @param th_duration: maximun duration between start and stop of a spike
        @param resample_window: resample if required to get a fixed interval time series
        @param method_data_points: define to use how many data points finding a spike (up-peak, or up-peak-down)
        @param threshold: PdM threshold of number of spikes to raise alerts
        @param t_min_high: PdM duration threshold to raise PdM alerts
        @param t_min_low: PdM duration threshold to reset PdM alerts
        """
        # Configurations
        self.th_slope = th_slope
        self.th_duration = th_duration
        self.resample_window = resample_window
        self.method_data_points = method_data_points
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
        (2) Replace outliers with a fixed value or
            most recent non-outlier value (optional)
        (3) Apply moving average or iir filter
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

            self.results_['after dropna'] = copy.deepcopy(self.derived_parameter_)

            # (2) Locate spike
            th_duration = pd.to_timedelta(self.th_duration)
            df = pd.DataFrame(index=self.derived_parameter_.index, columns=[self.parameter_name, 'rising',
                                                                            'falling', 'duration'])
            df[self.parameter_name] = self.derived_parameter_.dropna().sort_index()
            # Calculate the difference x(t) - x(t-1)
            df['rising'] = self.derived_parameter_.diff(1)
            # Remove all data points with no change
            df = df[(df['rising'] != 0)]
            # Calculate the difference x(t) - x(t+1)
            df['falling'] = df[self.parameter_name].diff(-1)

            ts_rising = pd.to_datetime(df.index[:-1])
            ts_falling = pd.to_datetime(df.index[1:])
            df.loc[1:, 'duration'] = ts_falling - ts_rising

            if self.method_data_points == 3:
                df = df[(df['rising'] >= self.th_slope[0]) & (df['falling'] >= self.th_slope[1]) &
                        (df['duration'] <= th_duration)]
            elif self.method_data_points == 2:
                df = df[(df['rising'] >= self.th_slope[0])]

            self.derived_parameter_ = df[self.parameter_name]
            self.results_['after locate'] = copy.deepcopy(self.derived_parameter_)

            # (3) Count daily spike
            self.derived_parameter_ = pd.Series(1, index= df[self.parameter_name].index).resample('1D').count()
            self.results_['after count'] = copy.deepcopy(self.derived_parameter_)

            # (4) Aggregation
            # self.derived_parameter_ = self.derived_parameter_.rolling('14D').agg(sum).rolling('14D').max().rolling('28D').mean()
            # self.results_['after aggregation'] = copy.deepcopy(self.derived_parameter_)

        pass
    def plot_results(self,
                     color: dict[str, str] = COLOR_ALERT,
                     path: str = None):
        """
        customised plots to show the results step by step
        @param color:
        @param path:
        """
        if self.results_:

            if self.alert_ is not None:
                nrows = len(self.results_) + 1
            else:
                nrows = len(self.results_)

            fig, axes = plt.subplots(nrows=nrows, ncols=1)

            for i, (k, v) in enumerate(self.results_.items()):
                if k == 'after locate':
                    axes[i].plot(self.results_['original'].index,  self.results_['original'].values)
                    axes[i].plot(v.index, v.values, 'o', color='r', alpha=0.3)
                    axes[i].set_xlabel('')
                    axes[i].set_ylabel(v.name)
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

    def plot_results(self,
                     color: dict[str, str] = COLOR_ALERT,
                     path: str = None):

        if self.derived_parameter_ is not None:
            fig, ax = plt.subplots()
            ax.bar(self.derived_parameter_.index,
                   self.derived_parameter_.values)
            ax.set_xlabel('')
            ax.set_ylabel('Derived Parameter')
            ax.set_title(self.system_name)
            xrange = plt.xlim()

            if self.alert_ is not None:
                periods = self.alert_.get('periods')
                for level in self.alert_.get('levels'):
                    if periods.get(level) is not None:
                        for j in range(periods.get(level).shape[0]):
                            ax.axvspan(
                                periods.get(level).loc[j, 'start'],
                                periods.get(level).loc[j, 'end'],
                                alpha=1, color=color.get(level))
                    ax.hlines(y=self.threshold.get(level),
                              xmin=xrange[0], xmax=xrange[1],
                              linestyles="dashed", color=color.get(level))

            # fig.show()
            plt_maximize()
            if path is not None:
                if not os.path.exists(path):
                    os.makedirs(path)
                fig.savefig(f'{path}/{self.system_name}_derived_parameter.png')
            self.graph_derived_parameter_ = fig
