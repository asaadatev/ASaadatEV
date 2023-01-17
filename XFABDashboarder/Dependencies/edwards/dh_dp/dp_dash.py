from edwards.dh_dp import dp_plot, dp_peak, dp_event
import pandas as pd


class DashGenerator():
    def __init__(self, data_loader):
        self.loader = data_loader

    def one_tool_multi_cols(self, system_name, fig_folder, cols=None, all_col=True, anonymous=False, raw=False,
                                   resample_fun='mean'):
        data = self.loader.get_data()
        num_col = len(data.columns)
        data.sort_index(inplace=True)
        if all_col:
            cols = [col for col in data.columns if any([name in col for name in ['MB', 'Booster', 'DP', 'Dry Pump',
                                                                                 'DryPump', 'Hours', 'Time', 'Pressure',
                                                                                 'Flow', 'VI']])]
            plot_cols = list(set(cols))
            plot_cols.sort()
        else:
            plot_cols = cols
        df = data
        df.sort_index(inplace=True)
        df.fillna(method='ffill', inplace=True)
        #df.interpolate(inplace=True)
        axs = dp_plot.plot_overview(df,
                                    plot_cols,
                                    fig_folder,
                                    anonymous=anonymous,
                                    system_name=system_name,
                                    raw_overview=raw,
                                    overview_resample_fun=resample_fun,
                                    n_row=6)
        return axs

    def one_tool_one_col_baseline(self, system_name, fig_folder, cols=None, all_col=True, anonymous=False):
        axs = []
        data = self.loader.get_data()
        data.sort_index(inplace=True)
        if all_col:
            cols = [col for col in data.columns if any([name in col for name in ['Pressure', 'Flow', 'Temp',
                                                                                 'Temperature',
                                                                                 'Current', 'Power']])]
            individual_cols = list(set(cols))
            individual_cols.sort()
        else:
            individual_cols = cols
        try:
            assert all([c in data.columns for c in individual_cols])
            for col in individual_cols:
                ax = dp_plot.plot_derived_baseline_single(data[col], col, fig_folder, system_name, anonymous=anonymous)
                axs.append(ax)
            return axs
        except AssertionError:
            print(data.columns, '\n Check columns names')

    def process_count_on_mb_temp(self, system_name, fig_folder, spike_t=2, peak_t=1, lag=3):
        data = self.loader.get_data()
        data.sort_index(inplace=True)
        if 'Booster Temperature' in data.columns:
            col = 'Booster Temperature'
            _, pk_count = dp_peak.spike_detect(data[col], spike_t, peak_t, lag, col, system_name, plot=False)
            axs = dp_plot.plot_process_count(data, col, pk_count, fig_folder, system_name)
            return axs
        elif 'MB Temp' in data.columns:
            col = 'MB Temp'
            _, pk_count = dp_peak.spike_detect(data[col], spike_t, peak_t, lag, col, system_name, plot=False)
            axs = dp_plot.plot_process_count(data, col, pk_count, fig_folder, system_name)
            return axs
        else:
            print('Check Column name of booster temperature')

    def erratic_spike_on_current(self, fig_folder, th_slope_dp=(0.7, 0.7), th_duration_dp='1m', th_slope_mb=(0.7, 0.7),
                                 th_duration_mb='1m', window_width='10m', mb_th=(5, 8), system_name='', anonymous=False):
        data = self.loader.get_data()
        data.sort_index(inplace=True)
        if all([c in data.columns for c in ['DryPump Current', 'Booster Current']]):
            dp_data = data['DryPump Current']
            mb_data = data['Booster Current']
            exist = True
        elif all([c in data.columns for c in ['Dry Pump Current', 'Booster Current']]):
            dp_data = data['Dry Pump Current']
            mb_data = data['Booster Current']
            exist = True
        else:
            print('Check column names of current')
            exist = False
        if exist:
            dp_spike = dp_peak.spike_detect_slope(data=dp_data, th_slope=th_slope_dp, th_duration=th_duration_dp)
            mb_spike = dp_peak.spike_detect_slope(data=mb_data, th_slope=th_slope_mb, th_duration=th_duration_mb)

            dp_erratic_spike_r1, _ = dp_peak.erratic_spike_r1(dp_data, dp_spike, mb_data, mb_spike,
                                                              window_width=window_width)
            dp_erratic_spike_r2, _ = dp_peak.erratic_spike_r2(dp_data, dp_spike, mb_data, mb_spike, mb_th=mb_th,
                                                              window_width=window_width)
            dp_erratic_spike = dp_erratic_spike_r1.append(dp_erratic_spike_r2)
            dp_spike_count = pd.Series(1, index=dp_erratic_spike.index).resample('1D').count()

            dp_spike_agg = pd.Series()
            if 'Cumulative Run Time' in data.columns:
                dp_swap = dp_event.pump_swap_event(data['Cumulative Run Time'].fillna(method='ffill'))
            elif 'DP Run Hours' in data.columns:
                dp_swap = dp_event.pump_swap_event(data['DP Run Hours'].fillna(method='ffill'))
            else:
                print('Check columns of run hours')
            if dp_swap is not None:
                dp_swap.insert(0, data.index[0])
                dp_swap.append(data.index[-1])

                for i in range(len(dp_swap) - 1):
                    start = dp_swap[i]
                    stop = dp_swap[i + 1]
                    spike_agg = dp_spike_count[
                        (dp_spike_count.index >= start) & (dp_spike_count.index <= stop)].rolling('14D'). \
                        agg(sum).rolling('14D').max().rolling('28D').mean()
                    dp_spike_agg = dp_spike_agg.append(spike_agg)
            axs = dp_plot.plot_erractic_spike(dp_data, mb_data, dp_spike, mb_spike, dp_erratic_spike_r1,
                                              dp_erratic_spike_r2, dp_spike_count, dp_spike_agg, fig_folder,
                                              anonymous=anonymous, system_name=system_name)
            return axs