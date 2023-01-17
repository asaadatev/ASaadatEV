'''
This module contains various tools for dp.
'''

from .dp_dash import DashGenerator

from .dp_data_availability import data_availability

from .dp_peak import spike_detect, spike_detect_abs_threshold, spike_detect_local, spike_detect_slope, spike_remove_baseline, erratic_spike_r1, erratic_spike_r2

from .dp_event import pump_swap_event

from .dp_forecast import prophet

from .dp_trend import rolling_trend

from .dp_plot import plot_derived_baseline_single, plot_erractic_spike, plot_overview, plot_process_count

__all__ = ['DashGenerator',
           'data_availability',
           'spike_detect',
           'spike_detect_abs_threshold',
           'spike_detect_local',
           'spike_detect_slope',
           'spike_remove_baseline',
           'erratic_spike_r1',
           'erratic_spike_r2',
           'pump_swap_event',
           'prophet',
           'rolling_trend',
           'plot_derived_baseline_single',
           'plot_erractic_spike',
           'plot_overview',
           'plot_process_count'
]