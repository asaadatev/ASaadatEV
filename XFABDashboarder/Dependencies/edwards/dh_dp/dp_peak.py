import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import warnings

warnings.filterwarnings("ignore")


def spike_detect(data, spike_t, peak_t, lag, parameter_name, system_name=None, plot=True):
    """
    To detect spike and count the number by using std threshold

    :param system_name: system_name for title of plot
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
    spike = [1 if (pre_spike[i] == 1 and sum(pre_spike[i + 1:i + lag]) == 0) else 0 for i in range(len(pre_spike))]
    spike_count = [sum(spike[:i + 1]) if spike[i] == 1 else np.nan for i in range(len(spike))]
    spike = pd.Series(spike, data.index)
    spike_count = pd.Series(spike_count, data.index)
    spike_count.dropna(inplace=True)

    peak_upper_limit = avg + peak_t * std
    peak_lower_limit = avg - peak_t * std
    pre_peak = [1 if (peak_upper_limit < i < spike_upper_limit) else 0 for i in data]
    peak = [1 if (pre_peak[i] == 1 and sum(pre_peak[i + 1:i + lag]) == 0) else 0 for i in range(len(pre_peak))]
    peak_count = [sum(peak[:i + 1]) if peak[i] == 1 else np.nan for i in range(len(peak))]
    peak = pd.Series(peak, data.index)
    peak_count = pd.Series(peak_count, data.index)
    peak_count.dropna(inplace=True)

    if plot:
        plt.figure(figsize=(10, 6))
        ax1 = plt.subplot(211)
        plt.title(system_name + ' Peak&Spike')
        plt.plot(data, label=str(parameter_name))
        if parameter_name == 'Motor Current':
            plt.ylim(0, 20)
        plt.grid()

        if len(peak_count) != 0 and ((len(peak_count)) - len(spike_count)) / len(peak_count) > 0.7:
            try:
                plt.plot(spike_count.index, [data[i] for i in spike_count.index], 'x', label='Spike')
            except ValueError:
                plt.plot(spike_count.index, np.zeros(len(spike_count)) + spike_upper_limit, 'x', label='Spike')
        try:
            plt.plot(peak_count.index, [data[i] for i in peak_count.index], 'x', label='Peak')
        except ValueError:
            plt.plot(peak_count.index, np.zeros(len(peak_count)) + peak_upper_limit, 'x', label='Peak')
        plt.xticks(rotation=30)
        plt.legend()

        ax2 = plt.subplot(212, sharex=ax1)
        plt.grid()
        try:
            if len(peak_count) != 0 and abs((len(peak_count)) - len(spike_count)) / len(peak_count) > 0.9:
                plt.plot(spike_count.index, spike_count.values, 'o', label='Spike count')
            plt.plot(peak_count.index, peak_count.values, 'o', label='Peak count')
        except ValueError:
            pass
        plt.xticks(rotation=30)
        plt.legend()
    return spike_count, peak_count


def spike_detect_local(data_all, spike_t, peak_t, lag, parameter_name, system_name=None, window_size=50000, plot=True):
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
            spike = [1 if (pre_spike[i] == 1 and sum(pre_spike[i + 1:i + lag]) == 0) else 0 for i in
                     range(len(pre_spike))]
            spike = pd.Series(spike, data.index)
            spike_all = spike_all.append(spike)

            peak_upper_limit = avg + peak_t * std
            peak_lower_limit = avg - peak_t * std
            pre_peak = [1 if (peak_upper_limit < i < spike_upper_limit) else 0 for i in data]
            peak = [1 if (pre_peak[i] == 1 and sum(pre_peak[i + 1:i + lag]) == 0) else 0 for i in
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

    if plot:
        plt.figure(figsize=(10, 6))
        ax1 = plt.subplot(211)
        plt.title(system_name + ' Peak&Spike')
        plt.plot(data_all, label=str(parameter_name))
        if parameter_name == 'Motor Current':
            plt.ylim(0, 20)
        plt.grid()

        if len(peak_count) != 0 and ((len(peak_count)) - len(spike_count)) / len(peak_count) > 0.7:
            try:
                plt.plot(spike_count.index, [data_all[i] for i in spike_count.index], 'x', label='Spike')
            except ValueError:
                plt.plot(spike_count.index, np.zeros(len(spike_count)) + spike_upper_limit, 'x', label='Spike')
        try:
            plt.plot(peak_count.index, [data_all[i] for i in peak_count.index], 'x', label='Peak')
        except ValueError:
            plt.plot(peak_count.index, np.zeros(len(peak_count)) + peak_upper_limit, 'x', label='Peak')
        plt.xticks(rotation=30)
        plt.legend()

        ax2 = plt.subplot(212, sharex=ax1)
        plt.grid()
        try:
            if len(peak_count) != 0 and abs((len(peak_count)) - len(spike_count)) / len(peak_count) > 0.9:
                plt.plot(spike_count.index, spike_count.values, 'o', label='Spike count')
            plt.plot(peak_count.index, peak_count.values, 'o', label='Peak count')
        except ValueError:
            pass
        plt.xticks(rotation=30)
        plt.legend()
    return spike_count, peak_count


def spike_detect_abs_threshold(data, spike_t, peak_t, lag, parameter_name, system_name=None, plot=True):
    """
    To detect spike and count the number by using real parameter value threshold

    :param system_name: system_name for title of plot
    :param data: data array with datetime index
    :param spike_t: value threshold for spike
    :param peak_t: value threshold for peak
    :param lag: within lag it count 1 spike
    :param parameter_name: for plot label
    :param plot: plot the result
    :return spike_count: number of spike
    """
    data = data.dropna().sort_index()

    spike_upper_limit = spike_t
    spike_lower_limit = -spike_t
    pre_spike = [1 if (i > spike_upper_limit or i < spike_lower_limit) else 0 for i in data]
    spike = [1 if (pre_spike[i] == 1 and sum(pre_spike[i + 1:i + lag]) == 0) else 0 for i in range(len(pre_spike))]
    spike_count = [sum(spike[:i + 1]) if spike[i] == 1 else np.nan for i in range(len(spike))]
    spike = pd.Series(spike, data.index)
    spike_count = pd.Series(spike_count, data.index)
    spike_count.dropna(inplace=True)

    peak_upper_limit = peak_t
    peak_lower_limit = -peak_t
    pre_peak = [1 if (peak_upper_limit < i < spike_upper_limit) else 0 for i in data]
    peak = [1 if (pre_peak[i] == 1 and sum(pre_peak[i + 1:i + lag]) == 0) else 0 for i in range(len(pre_peak))]
    peak_count = [sum(peak[:i + 1]) if peak[i] == 1 else np.nan for i in range(len(peak))]
    peak = pd.Series(peak, data.index)
    peak_count = pd.Series(peak_count, data.index)
    peak_count.dropna(inplace=True)

    if plot:
        plt.figure(figsize=(10, 6))
        ax1 = plt.subplot(211)
        plt.title(system_name + ' Peak&Spike')
        plt.plot(data, label=str(parameter_name))
        if parameter_name == 'Motor Current':
            plt.ylim(0, 20)
        plt.grid()

        if len(peak_count) != 0 and ((len(peak_count)) - len(spike_count)) / len(peak_count) > 0.7:
            try:
                plt.plot(spike_count.index, [data[i] for i in spike_count.index], 'x', label='Spike')
            except ValueError:
                plt.plot(spike_count.index, np.zeros(len(spike_count)) + spike_upper_limit, 'x', label='Spike')
        try:
            plt.plot(peak_count.index, [data[i] for i in peak_count.index], 'x', label='Peak')
        except ValueError:
            plt.plot(peak_count.index, np.zeros(len(peak_count)) + peak_upper_limit, 'x', label='Peak')
        plt.xticks(rotation=30)
        plt.legend()

        ax2 = plt.subplot(212, sharex=ax1)
        plt.grid()
        try:
            if len(peak_count) != 0 and abs((len(peak_count)) - len(spike_count)) / len(peak_count) > 0.9:
                plt.plot(spike_count.index, spike_count.values, 'o', label='Spike count')
            plt.plot(peak_count.index, peak_count.values, 'o', label='Peak count')
        except ValueError:
            pass
        plt.xticks(rotation=30)
        plt.legend()
    return spike_count, peak_count


def spike_detect_slope(data, th_slope, th_duration='1T'):
    """
    Detect spikes based on slopes between each two values with in a short period
    :param data: Datetime indexed Series
    :param th_slope: slope threshold between two points to classify as spike
    :param th_duration: duration threshold for three point to classify as spike
    :return: Datetime indexed Series of spike amplitude
    """
    data.dropna(inplace=True)
    th_duration = pd.to_timedelta(th_duration)
    df = pd.DataFrame(index=data.index, columns=['value', 'rising', 'falling', 'duration'])
    df['value'] = data.dropna().sort_index()
    # Calculate the difference x(t) - x(t-1)
    df['rising'] = data.diff(1)
    # Remove all data points with no change
    df = df[(df['rising'] != 0)]
    # Calculate the difference x(t) - x(t+1)
    df['falling'] = df['value'].diff(-1)

    ts_rising = pd.to_datetime(df.index[:-1])
    ts_falling = pd.to_datetime(df.index[1:])
    df.loc[1:, 'duration'] = ts_falling - ts_rising

    df = df[(df['rising'] >= th_slope[0]) & (df['falling'] >= th_slope[1]) & (df['duration'] <= th_duration)]

    return df['value']


def erratic_spike_r1(dp_data, dp_spike, mb_data, mb_spike, window_width='10m'):
    """
    Detect erratic spike where there is a spike in dp_data and there is no spike in mb_data
    :param dp_data: dp Series with datetime index
    :param dp_spike: dp spike Series with datetime index
    :param mb_data: mb Series with datetime index
    :param mb_spike: mb spike Series with datetime index
    :param window_width: search synchronise spike in this window size
    :return: Datetime indexed Series for dp erratic spikes, the first one with dp_data amplitude,
    second with mb_data amplitude
    """
    dp_data.dropna(inplace=True)
    mb_data.dropna(inplace=True)
    steps = round(pd.to_timedelta(window_width)/pd.to_timedelta('30s'))

    df_dp = pd.DataFrame(index=dp_data.index, columns=['dp_value', 'dp_spike', 'dp_index'])
    df_dp['dp_value'] = dp_data
    df_dp['dp_spike'] = [1 if i in dp_spike.index else 0 for i in dp_data.index]
    df_dp['dp_index'] = pd.to_datetime(dp_data.index)
    df_dp = df_dp.resample('30s').max()

    df_mb = pd.DataFrame(index=mb_data.index, columns=['mb_value', 'mb_spike', 'mb_index'])
    df_mb['mb_value'] = mb_data
    df_mb['mb_spike'] = [1 if i in mb_spike.index else 0 for i in mb_data.index]
    df_mb['mb_index'] = pd.to_datetime(mb_data.index)
    df_mb = df_mb.resample('30s').max()

    df = pd.merge(df_dp, df_mb, left_index=True, right_index=True)
    df.fillna(method='ffill', inplace=True)
    for i in range(-round(steps/2), round(steps/2)):
        if i != 0:
            mb_s_title = 'mb_spike_' + str(i)
            df[mb_s_title] = df['mb_spike'].shift(i)

    spike_cols = ['dp_spike', 'mb_spike']
    for i in range(-round(steps/2), round(steps/2)):
        if i != 0:
            spike_cols.append('mb_spike_' + str(i))

    df['spike'] = df[spike_cols].sum(axis=1)
    df = df[df['spike'] != 0]

    df_erratic = df[(df['spike'] == 1) & (df['dp_spike'] == 1)]
    df_erratic.set_index('dp_index', inplace=True)
    df_erratic = df_erratic[~df_erratic.index.duplicated()]
    return df_erratic['dp_value'], df_erratic['mb_value']


def erratic_spike_r2(dp_data, dp_spike, mb_data, mb_spike, mb_th, window_width='10m'):
    """
    Detect erratic spike where there is a spike in dp_data and there is a spike in mb_data
    but mb_data within range of mb_th
    :param dp_data: dp Series with datetime index
    :param dp_spike: dp spike Series with datetime index
    :param mb_data: mb Series with datetime index
    :param mb_spike: mb spike Series with datetime index
    :param mb_th: threshold range for mb spike amplitude
    :param window_width: search synchronise spike in this window size
    :return: Datetime indexed Series for dp erratic spikes, the first one with dp_data value, second with mb_data value
    """
    dp_data.dropna(inplace=True)
    mb_data.dropna(inplace=True)
    steps = round(pd.to_timedelta(window_width)/pd.to_timedelta('30s'))

    df_dp = pd.DataFrame(index=dp_data.index, columns=['dp_value', 'dp_spike', 'dp_index'])
    df_dp['dp_value'] = dp_data
    df_dp['dp_spike'] = [1 if i in dp_spike.index else 0 for i in dp_data.index]
    df_dp['dp_index'] = pd.to_datetime(dp_data.index)
    df_dp = df_dp.resample('30s').max()

    df_mb = pd.DataFrame(index=mb_data.index, columns=['mb_value', 'mb_spike', 'mb_index'])
    df_mb['mb_value'] = mb_data
    df_mb['mb_spike'] = [1 if i in mb_spike.index else 0 for i in mb_data.index]
    df_mb['mb_index'] = pd.to_datetime(mb_data.index)
    df_mb = df_mb.resample('30s').max()

    df = pd.merge(df_dp, df_mb, left_index=True, right_index=True)
    df.fillna(method='ffill', inplace=True)
    for i in range(-round(steps/2), round(steps/2)):
        if i != 0:
            mb_s_title = 'mb_spike_' + str(i)
            df[mb_s_title] = df['mb_spike'].shift(i)

    spike_cols = ['dp_spike', 'mb_spike']
    for i in range(-round(steps/2), round(steps/2)):
        if i != 0:
            spike_cols.append('mb_spike_' + str(i))

    df['spike'] = df[spike_cols].sum(axis=1)
    df = df[df['spike'] != 0]

    df_erratic = df[(df['dp_spike'] == 1) & (df['mb_spike'] == 1) &
                    (df['mb_value'] <= mb_th[1]) & (df['mb_value'] >= mb_th[0])]
    df_erratic.set_index('dp_index', inplace=True)
    df_erratic = df_erratic[~df_erratic.index.duplicated()]
    return df_erratic['dp_value'], df_erratic['mb_value']


def spike_remove_baseline(data, baseline_outlier_th=13, resample_rule='30min', resample_func='min',
                          fillna_method='ffill', rolling_window=48, rolling_func='min'):
    """

    :param data: pandas series with timestamp as index
    :param baseline_outlier_th: remove all values below this threshold for representative normal status baseline(13A,5A)
    :param resample_rule:
    :param resample_func:
    :param fillna_method:
    :param rolling_window:
    :param rolling_func:
    :return: smoothed data
    """
    data.dropna(inplace=True)
    # remove outliers for more representative baseline
    data = data[data > baseline_outlier_th]
    data.sort_index(inplace=True)
    data_resampled = data.resample(rule=resample_rule,
                            axis=0,
                            closed='right',
                            label='right').agg(func=resample_func)
    if fillna_method is not None:
        data_resampled = data_resampled.fillna(method=fillna_method)
    baseline = data_resampled.rolling(window=rolling_window,
                        min_periods=1,
                        center=False,
                        win_type=None,
                        axis=0,
                        closed='both').agg(func=rolling_func).shift(-int(rolling_window), axis=0)

    concat_df = pd.concat([baseline, data], axis=1)
    concat_df.columns = ['baseline', 'raw']
    concat_df.sort_index(inplace=True)
    concat_df.fillna(method='ffill', inplace=True)
    concat_df.dropna(inplace=True)
    amplitude = concat_df['raw'] - concat_df['baseline']

    # spikes = spike_detect_slope(data=amplitude, th_slope=(1,1), th_duration='1T')
    # For SAAB
    # spike_count, peak_count = spike_detect(data=amplitude,spike_t=4, peak_t=3, lag=2, parameter_name='DP Current', plot=False)
    spike_count, peak_count = spike_detect_abs_threshold(data=amplitude, spike_t=4.5, peak_t=4, lag=2, parameter_name='DP Current', system_name=None, plot=False)
    # For FZRA
    # spike_count, peak_count = spike_detect_local(data_all=amplitude, spike_t=3, peak_t=2, lag=2,
    #                                               parameter_name='DP Current', window_size=5000, plot=False)

    spike_daily = spike_count.resample('1D').count()
    peak_daily = peak_count.resample('1D').count()
    import matplotlib.pyplot as plt
    fig, axs = plt.subplots(5, 1, sharex=True)
    axs[0].plot(data, label='raw')
    axs[1].plot(baseline, label='baseline')
    axs[2].plot(amplitude, label='remove baseline')
    # axs[2].plot(spikes.index, spikes.values, 'o', color='r', alpha=0.3)
    axs[2].plot(spike_count.index, amplitude[spike_count.index], 'o', color='r', alpha=0.3, label='large')
    axs[2].plot(peak_count.index, amplitude[peak_count.index], 'x', color='r', alpha=0.3, label='medium')
    axs[3].bar(spike_daily.index, spike_daily.values, label='Large Spike')
    axs[4].bar(peak_daily.index, peak_daily.values, label='Medium Spike')

    for ax in axs:
        ax.grid()
        ax.legend()
    return spike_count, peak_count
