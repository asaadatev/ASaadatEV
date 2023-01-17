import pandas as pd
from os import listdir, path
import matplotlib.pyplot as plt
from edwards.dh_dp import dp_trend, dp_peak, dp_forecast, dp_data_availability, dp_event
from math import ceil


def plot_overview(data,
                  overview_cols,
                  fig_folder,
                  anonymous=False,
                  system_name='',
                  raw_overview=True,
                  overview_resample_fun='mean',
                  n_row=6):
    """
    Plot multiple parameters in subplots
    :param data: dataframe with datetime index
    :param overview_cols: parameter column names to be plotted
    :param fig_folder: folder path to save the plot as png
    :param anonymous: anonymize parameter names
    :param system_name: tool name for title and file name
    :param raw_overview: if True plot raw data, if False plot derived data
    :param overview_resample_fun: resample function to derive raw data
    :param n_row: number of row for subplots
    :return:
    """
    try:
        assert all([c in data.columns for c in overview_cols])
        n_col = ceil(len(overview_cols) / n_row)
        n_row = ceil(len(overview_cols) / n_col)
        fig, axs = plt.subplots(n_row, n_col, sharex=True, figsize=(50, 50), squeeze=True)
        plt.suptitle(system_name)

        if raw_overview:
            for i in range(len(overview_cols)):
                col = overview_cols[i]
                if len(overview_cols) > 1:
                    ax = axs.reshape(-1)[i]
                else:
                    ax = axs
                if 'Temperature' in col or 'Temp' in col:
                    ax.plot(data[col] - 273.15)
                else:
                    ax.plot(data[col])
            sub_fix = ''

        else:
            for i in range(len(overview_cols)):
                col = overview_cols[i]
                if len(overview_cols) > 1:
                    ax = axs.reshape(-1)[i]
                else:
                    ax = axs
                if 'Time' in col or 'Speed' in col or 'Hours' in col:
                    ax.plot(data[col])
                elif 'Temperature' in col or 'Temp' in col:
                    ax.plot(dp_trend.rolling_trend(data[col], resample_func=overview_resample_fun) - 273.15)
                else:
                    ax.plot(dp_trend.rolling_trend(data[col], resample_func=overview_resample_fun))
            sub_fix = overview_resample_fun

        if anonymous:
            j = 1
            for i in range(len(overview_cols)):
                col = overview_cols[i]
                if len(overview_cols) > 1:
                    ax = axs.reshape(-1)[i]
                else:
                    ax = axs
                ax.set_ylabel('Parameter ' + str(j), fontsize=6)
                ax.set_yticklabels([])
                ax.grid()
                j += 1

        else:
            for i in range(len(overview_cols)):
                col = overview_cols[i]
                if len(overview_cols) > 1:
                    ax = axs.reshape(-1)[i]
                else:
                    ax = axs
                ax.set_ylabel(col, fontsize=6)
                ax.grid()

        for ax in fig.axes:
            ax.tick_params(axis='both', labelrotation=90, labelsize=6)
        plt.tight_layout()

        if anonymous:
            plt.savefig(path.join(fig_folder, 'anon_overview_' + sub_fix + system_name + '.png'))
        else:
            plt.savefig(path.join(fig_folder, 'overview_' + sub_fix + system_name + '.png'))
        return axs
    except AssertionError:
        print(data.columns, ' Check columns names')


def plot_derived_baseline_single(data,
                                 column_name,
                                 fig_folder,
                                 system_name='',
                                 anonymous=False):
    """
    Plot single parameter with baseline including mean, min, max
    :param data: datetime index pandas Series
    :param column_name: column name for plot legend and file name
    :param fig_folder: folder path to save the png
    :param system_name: tool name for plot title
    :param anonymous: anonymize the parameter name
    :return: 
    """
    fig, axs = plt.subplots(4, 1, sharex=True, figsize=(10, 15))
    if anonymous:
        axs[0].plot(data.fillna(method='ffill'), label='Derived Parameter')
        axs[0].set_yticklabels([])
        axs[1].plot(dp_trend.rolling_trend(data), label='Derived Parameter')
        axs[1].set_yticklabels([])
        axs[2].plot(dp_trend.rolling_trend(data, resample_func='min'), label='Derived Parameter')
        axs[2].set_yticklabels([])
        axs[3].plot(dp_trend.rolling_trend(data, resample_func='max'), label='Derived Parameter')
        axs[3].set_yticklabels([])
    else:
        axs[0].plot(data.fillna(method='ffill'), label=column_name)
        axs[1].plot(dp_trend.rolling_trend(data), label='Mean')
        axs[2].plot(dp_trend.rolling_trend(data, resample_func='min'), label='Min')
        axs[3].plot(dp_trend.rolling_trend(data, resample_func='max'), label='Max')

    for ax in axs.reshape(-1):
        ax.legend(loc='upper left')
        ax.grid()

    plt.suptitle(system_name)
    plt.tight_layout()

    if anonymous:
        plt.savefig(path.join(fig_folder, 'anon_' + '_' + system_name + '_' + column_name + '.png'))
    else:
        plt.savefig(path.join(fig_folder, system_name + '_' + column_name + '.png'))

    return axs


def plot_process_count(data, col, pk_count, fig_folder, system_name=''):
    pk_count_daily = pd.Series(1, index=pk_count.index).resample('1D').count()
    fig, axs = plt.subplots(2, 1, sharex=True)
    plt.suptitle(system_name)
    axs[0].plot(data[col].fillna(method='ffill'), label='MB Temp')
    axs[0].plot(pk_count.keys(), [data[col].loc[i] for i in pk_count.keys()], 'o', c='r', alpha=0.25)
    axs[0].legend()
    axs[0].grid()

    axs[1].bar(pk_count_daily.index, pk_count_daily.values, label='Process Cycle Number')
    axs[1].legend()
    axs[1].grid()

    plt.savefig(path.join(fig_folder, system_name + '_ProcessCount'  + '.png'))
    return axs


def plot_erractic_spike(dp_data, mb_data, dp_spike, mb_spike, dp_erratic_spike_r1, dp_erratic_spike_r2, dp_spike_count,
                        dp_spike_agg, fig_folder, anonymous=False, system_name=''):
    if not anonymous:
        fig, axs = plt.subplots(5, 1, sharex=True)
        plt.suptitle(system_name)
        axs[0].plot(dp_data.fillna(method='ffill'), label='DP Current')
        axs[0].plot(dp_spike.index, dp_spike, 'o', color='r', alpha=0.3)
        axs[1].plot(mb_data.fillna(method='ffill'), label='MB Current')
        axs[1].plot(mb_spike.index, mb_spike, 'o', color='r', alpha=0.3)
        axs[2].plot(dp_data.fillna(method='ffill'), label='DP Erratic Spike')
        axs[2].plot(dp_erratic_spike_r1.index, dp_erratic_spike_r1, 'o', color='r', alpha=0.3)
        axs[2].plot(dp_erratic_spike_r2.index, dp_erratic_spike_r2, '+', color='r', alpha=0.3)
        axs[3].bar(x=dp_spike_count.index, height=dp_spike_count, label='Daily Erratic Spike Count')
        axs[4].plot(dp_spike_agg, label='Aggregated Erratic Spike Count')
        for ax in axs.reshape(-1):
            ax.legend()
            ax.grid()

    if anonymous:
        fig, axs = plt.subplots(2, 1, sharex=True)
        plt.suptitle(system_name)
        axs[0].plot(dp_data.fillna(method='ffill'), label='Derived Parameter')
        axs[0].set_yticklabels([])
        axs[0].legend()
        axs[0].grid()
        axs[1].plot(dp_spike_agg, label='Derived Parameter')
        axs[1].set_yticklabels([])
        axs[1].legend()
        axs[1].grid()

    if anonymous:
        plt.savefig(path.join(fig_folder, 'anon_' + '_' + system_name + '_erratic_spike' +
                              '.png'))
    else:
        plt.savefig(path.join(fig_folder, system_name + '_erratic_spike' + '.png'))

    return axs
