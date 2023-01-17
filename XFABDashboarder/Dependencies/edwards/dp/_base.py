"""Base class for derived parameters."""

# Authors: Danny Sun <duo.sun@edwardsvacuum.com>

import os
import copy
import json
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime

from typing import (
    Callable,
)

from .. import COLOR_ALERT
from ..utils import create_vis_df, iir, cal_alert_periods
from ..vis import plt_maximize


class Base:
    """
    Base class for derived parameters.

    Attributes
    ----------
    derived_parameter_ : pd.Series
        Derived parameter.
    alert_ : dict
        A dictionary contains alert related info and results. Key-value
        paris are as follows.
        'levels' : list of str
            Alert levels, for example ['advisory', 'warning', 'alarm'],
            determined by the keys of the input parameter `threshold`.
        'periods' : dict
            Merged alert periods. Original alerts of different levels are
            computed independently and therefore can have overlapping alert
            periods. 'periods' gives merged periods, which indicate only the
            highest level of alert at each time point.
            {`levels[0]`: pd.DataFrame with columns 'start' and 'end',
             `levels[1]`: pd.DataFrame with columns 'start' and 'end',
             ...}
        'periods_unmerged' : dict
            Unmerged alert periods.
            {`levels[0]`: pd.DataFrame with columns 'start' and 'end',
             `levels[1]`: pd.DataFrame with columns 'start' and 'end',
             ...}
        'signal' : pd.Series
            Alert signal which indicates the highest level of alert at each
            time point.
        'sampling_rate' : pd.Timedelta
            Sampling rate of time series `derived_parameter_`, also known as
            'time interval' or 'frequency'.
    results_ : dict
         A dictionary contains original data, key intermediate processing
         results, as well as derived parameter.
    graph_derived_parameter_ : matplotlib.figure.Figure
        A matplotlib figure showing derived parameter. Assigned after call
        method `plot_derived_parameter()`
    graph_results_ : matplotlib.figure.Figure
        A matplotlib figure showing original data, key intermediate processing
        results, derived parameter, and alert signal. Assigned after call
        method `plot_results()`
    vis_as_df_ : pd.DataFrame
        A pd.DataFrame used to store `results_` and also plot-related info
        such as alerts and thresholds for post-processing and further
        visualization. Assigned after call method `save_vis_as_df()`.
    """

    def __init__(self):
        # Model data input attributes
        # To be extracted from the argument `src` of method `process()`
        self.data = None
        self.system_name = None
        self.parameter_name = None

        # Model configuration attributes
        # Other configurations will be specified in derived class.
        self.threshold = None

        # Model output attributes
        self.derived_parameter_ = None
        self.alert_ = {}
        self.results_ = {}
        self.graph_derived_parameter_ = None
        self.graph_results_ = None
        self.vis_as_df_ = None
        self.processing_time_ = None
        self.pipeline_ = {}

        # Model status attributes
        self.skip = False

    def reset(self, pipeline: bool = True) -> None:
        """
        Except configuration, reset all model input/output/status attributes.

        Parameters
        ----------
        pipeline : bool, default True
            Whether reset `self.pipeline_`.
        """
        self.data = None
        self.system_name = None
        self.parameter_name = None

        self.derived_parameter_ = None
        self.alert_ = {}
        self.results_ = {}
        self.graph_derived_parameter_ = None
        self.graph_results_ = None
        self.vis_as_df_ = None
        self.processing_time_ = None

        if pipeline:
            self.pipeline_ = {}

        self.skip = False

    def copy(self, deep=True):
        """Make a copy of this object."""
        if deep:
            return copy.deepcopy(self)
        else:
            return self

    def process(self, src):
        """Process data.

        Parameters
        ----------
        src: pd.Series, dict, or object
            If `src` is dict/object, it must have key/attribute 'data',
            and it may have key/attribute 'system_name' and 'parameter_name'.
        """

        # Reset input/output/status attributes, except `self.pipeline_`
        self.reset(pipeline=False)

        start = datetime.now()

        # Extract data, system_name, and parameter_name from `src`.
        self.parse_src(src)

        # Processing to be implemented in derived class
        if self.data is not None:
            self.results_['Original'] = self.data.copy()
            self.derived_parameter_ = self.data.copy()

        # Run processing steps if any in pipeline
        self.skip = True
        if self.pipeline_:
            print('Start processing ...\n')
            for k, v in self.pipeline_.items():
                for func, kwargs in v.items():
                    t1 = datetime.now()
                    getattr(self, func)(**kwargs)
                    t2 = datetime.now()
                    print(f'{datetime.now()}: step {k} - {func}'
                          f' {kwargs} - time spent {t2 - t1}\n')
            print('Processing completed!\n')
        self.skip = False

        end = datetime.now()
        self.processing_time_ = end - start
        print(f'Total time spent {self.processing_time_}')

        return self

    def parse_src(self, src):
        """Extract data, system_name, and parameter_name from `src`."""
        self.data, self.system_name, self.parameter_name = self._parse_src(src)

    @staticmethod
    def _parse_src(src):
        """
        Extract data, system_name, and parameter_name from `src`.

        Parameters
        ----------
        src: pd.Series, pd.DataFrame, dict, or object
            If `src` is dict/object, it must have key/attribute 'data',
            and it may have key/attribute 'system_name' and 'parameter_name'.

        Returns
        -------
        data : pd.DataFrame, pd.Series or None
        system_name : str or None
        parameter_name : str or None
        """
        data, system_name, parameter_name = None, None, None

        if isinstance(src, (pd.Series, pd.DataFrame)):
            data = src
        elif isinstance(src, dict):
            data = src['data']
            system_name = src.get('system_name', 'None')
            parameter_name = src.get('parameter_name', 'None')
        else:
            data = src.data
            try:
                system_name = src.system_name
            except AttributeError:
                pass
            try:
                parameter_name = src.parameter_name
            except AttributeError:
                pass

        if (parameter_name is None) & isinstance(data, pd.Series):
            parameter_name = data.name

        return data, system_name, parameter_name

    def plot_results(self,
                     color: dict[str, str] = COLOR_ALERT,
                     path: str = None):
        """
        Plot original data, key intermediate processing results, as well as
        derived parameter.
        
        Plot is created using info from `self.results_`, `self.alert_`,
        `self.threshold`, and `self.system_name`. It's saved as
        `self.graph_results_`.
        """

        if self.results_:

            if self.alert_:
                nrows = len(self.results_) + 1
            else:
                nrows = len(self.results_)

            fig, axes = plt.subplots(nrows=nrows, ncols=1)

            for i, (k, v) in enumerate(self.results_.items()):
                if isinstance(v, pd.DataFrame):
                    for j in v.columns:
                        axes[i].plot(v[j].index, v[j].values)
                    axes[i].legend(v.columns)
                else:
                    axes[i].plot(v.index, v.values)
                    axes[i].set_ylabel(v.name)
                axes[i].set_xlabel('')
                axes[i].set_title(k)

            if self.alert_:
                periods = self.alert_.get('periods')
                # Add alert color to last item of self.results_, which is
                # usually derived parameter.
                for level in self.alert_.get('levels'):
                    if periods.get(level) is not None:
                        for j in range(periods.get(level).shape[0]):
                            axes[len(self.results_) - 1].axvspan(
                                periods.get(level).loc[j, 'start'],
                                periods.get(level).loc[j, 'end']
                                + self.alert_.get('sampling_rate'),
                                alpha=1, color=color.get(level))
                # Add additional subplot for discrete alert signal
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

    def plot_derived_parameter(self,
                               color: dict[str, str] = COLOR_ALERT,
                               path: str = None):
        """
        Plot derived parameter.

        Plot is created using info from `self.derived_parameter_`,
        `self.alert_`, `self.threshold`, and `self.system_name`. It's saved
        as `self.graph_derived_parameter_`.
        """

        if self.derived_parameter_ is not None:
            fig, ax = plt.subplots()
            ax.plot(self.derived_parameter_.index,
                    self.derived_parameter_.values)
            ax.set_xlabel('')
            ax.set_ylabel('Derived Parameter')
            ax.set_title(self.system_name)
            xrange = plt.xlim()

            if self.alert_:
                periods = self.alert_.get('periods')
                for level in self.alert_.get('levels'):
                    if periods.get(level) is not None:
                        for j in range(periods.get(level).shape[0]):
                            ax.axvspan(
                                periods.get(level).loc[j, 'start'],
                                periods.get(level).loc[j, 'end']
                                + self.alert_.get('sampling_rate'),
                                alpha=1, color=color.get(level))
                    try:
                        ax.hlines(y=self.threshold.get(level),
                                  xmin=xrange[0], xmax=xrange[1],
                                  linestyles="dashed", color=color.get(level))
                    except NameError:
                        pass

            # fig.show()
            plt_maximize()
            if path is not None:
                if not os.path.exists(path):
                    os.makedirs(path)
                fig.savefig(f'{path}/{self.system_name}_derived_parameter.png')
            self.graph_derived_parameter_ = fig

    def save_vis_as_df(self,
                       path: str = None,
                       title_suffix: str | int | list = None,
                       highlight_all: bool = False):
        """
        Save results as pd.DataFrame for post-processing and further
        visualization.
        """
        if self.results_:
            x = []
            for i, (k, v) in enumerate(self.results_.items(), start=1):

                if title_suffix is not None:
                    if isinstance(title_suffix, int):
                        title_suffix = str(title_suffix)
                    elif isinstance(title_suffix, list):
                        title_suffix = ' '.join([str(x) for x in title_suffix])
                    system_name = self.system_name + ' ' + title_suffix
                else:
                    system_name = self.system_name
                title = system_name + ' - ' + k

                period = None
                hline = None
                # The last subplot is usually the derived parameter, which is
                # compared against threshold to get alert periods.
                if self.alert_:
                    if highlight_all | (i == len(self.results_)):
                        period = self.alert_.get('periods')

                if i == len(self.results_):
                    try:
                        hline = self.threshold
                    except NameError:
                        pass

                if isinstance(v, pd.DataFrame):
                    # If pd.DataFrame, use column name as parameter_name
                    # but use 'value' as the ylabel of plot
                    for j in v.columns:
                        data = create_vis_df(
                            data=v[j].dropna(),
                            system_name=system_name,
                            parameter_name=j,
                            subplot=i,
                            format=None,
                            title=title,
                            ylabel='value',
                            period=period,
                            hline=hline)
                        x.append(data)
                else:
                    # If pd.Series, series name as parameter_name and ylabel
                    if v.name is None:
                        parameter_name = 'value'
                    else:
                        parameter_name = v.name
                    data = create_vis_df(
                        data=v.dropna(),
                        system_name=system_name,
                        parameter_name=parameter_name,
                        subplot=i,
                        format=None,
                        title=title,
                        ylabel=None,
                        period=period,
                        hline=hline)
                    x.append(data)
            self.vis_as_df_ = pd.concat(x)

        if (path is not None) and (self.vis_as_df_ is not None):
            if path.endswith('.parquet'):
                if not os.path.exists(os.path.dirname(path)):
                    os.makedirs(os.path.dirname(path))
                self.vis_as_df_.to_parquet(path)
            else:
                if not os.path.exists(path):
                    os.makedirs(path)
                self.vis_as_df_.to_parquet(
                    f'{path}/{self.system_name}.parquet')

    def _run_load_src(self, src, title: str | None = 'Original'):
        """Load data.

        Parameters
        ----------
        src: pd.Series, dict, or object
            If `src` is dict/object, it must have key/attribute 'data',
            and it may have key/attribute 'system_name' and 'parameter_name'.
        """
        self.reset()
        self.parse_src(src)
        if self.data is not None:
            self.derived_parameter_ = self.data.copy()
            self._add_to_results(self.derived_parameter_, title=title)
        return self

    def _run_remove_outliers(self,
                             upper_limit: float | int = None,
                             lower_limit: float | int = None,
                             title: str | None = 'After removing outliers'):
        """
        Remove outliers.

        Parameters
        ----------
        upper_limit : float or int, default None
            Values greater than `upper_limit` will be replaced with NA.
        lower_limit : float or int, default None
            Values less than `lower_limit` will be replaced with NA.
        """

        self._add_to_pipeline(self._run_remove_outliers.__name__,
                              kwargs=locals())

        if self.derived_parameter_ is not None:
            if upper_limit is not None:
                self.derived_parameter_.where(
                    self.derived_parameter_ <= upper_limit, inplace=True)
            if lower_limit is not None:
                self.derived_parameter_.where(
                    self.derived_parameter_ >= lower_limit, inplace=True)

        self._add_to_results(self.derived_parameter_, title=title)

        return self

    def _run_dropna(self,
                    title: str | None = 'After dropping NA',
                    *args,
                    **kwargs):
        """Drop missing values.

        See Also
        --------
        pandas.DataFrame.dropna
        pandas.Series.dropna
        """

        self._add_to_pipeline(self._run_dropna.__name__, kwargs=locals())

        if self.derived_parameter_ is not None:
            self.derived_parameter_.dropna(inplace=True, *args, **kwargs)

        self._add_to_results(self.derived_parameter_, title=title)

        return self

    def _run_fillna(self,
                    value=None,
                    method=None,
                    title: str | None = 'After filling NA',
                    *args,
                    **kwargs):
        """
        Fill NA/NaN values using the specified value or method.

        See Also
        --------
        pandas.DataFrame.fillna
        pandas.Series.fillna
        """

        self._add_to_pipeline(self._run_fillna.__name__, kwargs=locals())

        if self.derived_parameter_ is not None:
            if (value is not None) | (method is not None):
                self.derived_parameter_.fillna(
                    value=value,
                    method=method,
                    inplace=True,
                    *args,
                    **kwargs)

        self._add_to_results(self.derived_parameter_, title=title)

        return self

    def _run_resample(self,
                      rule,
                      axis=0,
                      closed=None,  # i.e., 'right'
                      label=None,  # i.e., 'right'
                      convention='start',
                      kind=None,
                      on=None,
                      level=None,
                      origin='start_day',
                      offset=None,
                      title: str | None = 'After resampling',
                      func: Callable | str | list | dict = 'mean',
                      *args,
                      **kwargs):
        """
        Resample and aggregate data.

        See Also
        --------
        pandas.Series.resample: Resample time-series data.
        pandas.DataFrame.resample: Resample time-series data.
        Resampler.aggregate: Aggregate using one or more operations.
        """

        self._add_to_pipeline(self._run_resample.__name__, kwargs=locals())

        if self.derived_parameter_ is not None:
            if (rule is not None) & (func is not None):
                self.derived_parameter_ = \
                    (self.derived_parameter_
                     .resample(rule=rule,
                               axis=axis,
                               closed=closed,
                               label=label,
                               convention=convention,
                               kind=kind,
                               on=on,
                               level=level,
                               origin=origin,
                               offset=offset)
                     .aggregate(func=func, *args, **kwargs))

        self._add_to_results(self.derived_parameter_, title=title)

        return self

    def _run_rolling(self,
                     window,
                     min_periods=None,
                     center=False,  # i.e., label as the right edge'
                     win_type=None,
                     on=None,
                     axis=0,
                     closed=None,  # i.e., 'right'
                     step=None,   # i.e., 1
                     method='single',
                     title: str | None = 'After rolling',
                     func: Callable | str | list | dict = 'mean',
                     *args, **kwargs):
        """
        Perform rolling window and then aggregation calculation.

        See Also
        --------
        pandas.Series.rolling: provide rolling window calculations.
        pandas.DataFrame.rolling: provide rolling window calculations.
        pandas.Rolling.aggregate: aggregate using one or more operations.
        """

        self._add_to_pipeline(self._run_rolling.__name__, kwargs=locals())

        if self.derived_parameter_ is not None:
            self.derived_parameter_ = \
                (self.derived_parameter_
                 .rolling(window=window,
                          min_periods=min_periods,
                          center=center,
                          win_type=win_type,
                          on=on,
                          axis=axis,
                          closed=closed,
                          step=step,
                          method=method)
                 .aggregate(func=func, *args, **kwargs))

        self._add_to_results(self.derived_parameter_, title=title)

        return self

    def _run_iir(self,
                 alpha: float | int,
                 title: str | None = 'After IIR filtering'):
        """
        IIR filter.

        See Also
        --------
        iir
        """

        self._add_to_pipeline(self._run_iir.__name__, kwargs=locals())

        if self.derived_parameter_ is not None:

            if not isinstance(self.derived_parameter_, pd.Series):
                raise TypeError('IIR supports only pd.Series.')

            self.derived_parameter_.loc[:] = iir(
                x=self.derived_parameter_.values, alpha=alpha)

        self._add_to_results(self.derived_parameter_, title=title)

        return self

    def _run_threshold(self,
                       threshold,
                       is_upper,
                       t_min_high,
                       t_min_low):
        """
        Compare `self.derived_parameter_` against thresholds to get alert
        periods.

        See Also
        --------
        cal_alert_periods
        """

        self._add_to_pipeline(self._run_threshold.__name__, kwargs=locals())

        self.threshold = threshold

        if self.derived_parameter_ is not None:

            if not isinstance(self.derived_parameter_, pd.Series):
                raise TypeError('IIR supports only pd.Series.')

            self.alert_ = cal_alert_periods(data=self.derived_parameter_,
                                            threshold=threshold,
                                            is_upper=is_upper,
                                            t_min_high=t_min_high,
                                            t_min_low=t_min_low)
        return self

    def _run(self,
             func: Callable,
             name: str | list | None = None,
             title: str | None = None,
             *args,
             **kwargs):
        """
        Update `self.derived_parameter_` by applying a function to it.

        Parameters
        ----------
        func : function
            Function to be applied to `self.derived_parameter_`.
        name : str | list of str, default None
            If not None, `name` is used as key/colname to extract
            data from `func` results as the updated `self.derived_parameter_`.
        title : str or None, default None
            If not None, `title` and updated `self.derived_parameter_` will be
            added as a key-value pair to dict `self.results_`,
            and subsequently `title` will be used as the title of the
            corresponding subplot when call `plot_results()`.
        *args
            Positional arguments to pass to func.
        **kwargs
            Keyword arguments to pass to func.
        """
        if self.derived_parameter_ is not None:

            x = func(self.derived_parameter_, *args, **kwargs)

            if name is not None:
                self.derived_parameter_ = x[name]
            else:
                self.derived_parameter_ = x

        self._add_to_results(self.derived_parameter_, title=title)

        return self

    def _add_to_results(self,
                        x: pd.Series | pd.DataFrame,
                        title: str | None) -> None:
        """
        Add `x`, e.g, `self.derived_parameter_` as value to dict
        `self.results_` with `title` as key.

        Parameters
        ----------
        x : pd.Series or pd.DataFrame
        title : str or None
            If not None, `title` and `self.derived_parameter_` will be
            added as a key-value pair to dict `self.results_`,
            and subsequently `title` will be used as the title of the
            corresponding subplot when call `plot_results()`.
        """
        if title is not None:
            if title in self.results_.keys():
                raise ValueError(f'title {title} already exits!')
            else:
                self.results_[title] = copy.deepcopy(x)

    def _add_to_pipeline(self, func: str, kwargs: dict) -> None:
        """
        Add function name and arguments to `self.pipeline_`.

        Parameters
        ----------
        func : str
            Function name.
        kwargs : dict
            Function arguments.
        """
        del kwargs['self']

        if 'args' in kwargs.keys():
            if kwargs['args'] == ():
                del kwargs['args']

        if 'kwargs' in kwargs.keys():
            if kwargs['kwargs'] == {}:
                del kwargs['kwargs']

        if not self.skip:
            self.pipeline_[len(self.pipeline_)+1] = {func: kwargs}

    def _save_pipeline(self, file: str = 'pipeline.json') -> None:
        """Save pipeline into a file."""
        with open(file, 'w') as f:
            json.dump(self.pipeline_, f,
                      indent=4, ensure_ascii=False, default=str)

    def _load_pipeline(self, file: str = 'pipeline.json') -> None:
        """Load pipeline from a file."""
        f = open(file)
        self.pipeline_ = json.load(f)
        f.close()
