import pandas as pd
import numpy as np
from stp import event, trend, peak, cycle, vib_score
import math
from datetime import timedelta
pd.set_option('mode.chained_assignment', None)


class StpIndicators(object):
    """

    STP monitoring/pdm indicators
    to infer contamination status of STP pumps

    """
    def __init__(self, data, after_swap=True):
        """
        Load the dataframe which extracted using odbc.py and transformed to column as feature format using
        STP_Data_Extract.py

        :param data: dataframe of STP measurements with datetime index, each row is at one timestamp, each column is one
        feature.
        :param after_swap: if after_swap is Ture, will slice the data to the newly swapped pump. Otherwise will include
        all data.
        """
        self.data = data
        self.column_names = data.columns
        self.data = self.data.sort_index()

        # Event detect
        equip_status_indata = 'Equipment Status' in self.column_names and self.data['Equipment Status'] is not None
        motor_speed_indata = 'Motor Speed' in self.column_names and self.data['Motor Speed'] is not None
        pump_hour_indata = 'Pump Hour Counter' in self.column_names and self.data['Pump Hour Counter'] is not None

        if pump_hour_indata:
            self.pump_swap_events = None
            pump_hour = self.data['Pump Hour Counter'].dropna()
            self.pump_swap_events = event.STPEvent.pump_swap_event(pump_hour)

            # if after_swap=True, will slice the data to the new swapped pump
            # else data will include all historical record.
            if self.pump_swap_events and after_swap:
                self.data = self.data[self.data.index >= self.pump_swap_events[-1][1]]
        # if there is equipment status, use it to extract data for "Normal", "Acceleration", and "Deceleration" status
        if equip_status_indata:
            self.data['Equipment Status_NF'] = self.data['Equipment Status']
            self.data['Equipment Status'].fillna(method='ffill', inplace=True)
            self.data_acc = self.data[self.data['Equipment Status'] == 3]
            self.data_ops = self.data[self.data['Equipment Status'] == 4]
            self.data_dec = self.data[self.data['Equipment Status'] == 5]
        # if there is not equipment status but motor speed, use it to extract data for "Normal" status
        elif motor_speed_indata:
            motor_speed = self.data['Motor Speed'].dropna()
            vc = self.data['Motor Speed'].value_counts()
            speed = math.floor(vc.index[0] / 100) * 100
            self.motor_start_events = event.STPEvent.motor_start_event(motor_speed)
            self.motor_start_events_ops_speed = event.STPEvent.motor_start_event(motor_speed, speed)
            self.motor_stop_events = event.STPEvent.motor_stop_event(motor_speed)
            self.motor_stop_events_ops_speed = event.STPEvent.motor_stop_event(motor_speed, speed)

            for i in range(len(self.motor_stop_events_ops_speed)):
                self.data_ops = self.data.drop(index=self.data.index[(self.data.index >=
                                                                      self.motor_stop_events_ops_speed[i][0]) &
                                                                     (self.data.index <=
                                                                      self.motor_stop_events_ops_speed[i][1])])

    def acc_period(self, speed_upper_t=0.96, speed_lower_t=0.37):
        """
        To calculate the time interval during all acceleration instances using Motor Speed

        :param speed_upper_t: float [0,1], upper threshold of speed to count the time
        :param speed_lower_t: float [0,1], lower threshold of speed to count the time
        :return: dictionary of acceleration interval, key:datetime, value:seconds
        """
        try:
            acc_motor_speed = self.data_acc['Motor Speed'].dropna()
            if len(acc_motor_speed) != 0:
                high_speed = np.percentile(acc_motor_speed, 99) * speed_upper_t
                low_speed = np.percentile(acc_motor_speed, 99) * speed_lower_t
                acc_periods = cycle.acc_interval(acc_motor_speed, high_speed, low_speed, plot=False)
            else:
                acc_periods = None
        except KeyError:
            acc_periods = None
        return acc_periods

    def dec_period(self, speed_upper_t=0.96, speed_lower_t=0.37):
        """
        To calculate the time interval during all deceleration instances

        :param speed_upper_t: float [0,1], upper threshold of speed to count the time
        :param speed_lower_t: float [0,1], lower threshold of speed to count the time
        :return: dictionary of deceleration interval, key:datetime, value:seconds
        """
        try:
            dec_motor_speed = self.data_dec['Motor Speed'].dropna()
            if len(dec_motor_speed) != 0:
                high_speed = np.percentile(dec_motor_speed, 99) * speed_upper_t
                low_speed = np.percentile(dec_motor_speed, 99) * speed_lower_t
                dec_periods = cycle.dec_interval(dec_motor_speed, high_speed, low_speed, plot=False)
            else:
                dec_periods = None
        except KeyError:
            dec_periods = None
        return dec_periods

    def motor_current_spike(self, spike_t, peak_t, lag=10):
        """
        To count the number of spikes and peaks of motor current

        :param spike_t: float, std threshold for spike
        :param peak_t: float, std threshold for peak
        :param lag: int, within lag it count 1 spike
        :return: spike_count and peak_count with datetime index
        """
        try:
            if self.data['Motor Current'] is not None:
                motor_current = pd.Series(self.data['Motor Current']).dropna()
                # Motor Current Peak and Spike counter
                spike_count, peak_count = peak.PeakDetect.spike_detect(motor_current, spike_t, peak_t, lag, 'Motor Current',
                                                                       plot=False)
            else:
                print('No Motor Current')
                spike_count, peak_count = None, None
        except KeyError:
            spike_count, peak_count = None, None
        return spike_count, peak_count

    def motor_current_trend(self, method='ma', bin_size=100, lambda_value=50, batch_size=100):
        """
        To filter the motor current data and get a smoothed trend

        :param method: str, for 'ma':moving average, 'l1':L1 filter, 'l2':L2 Hodrick-Prescott (H-P) filter
        :param bin_size: int, for bin size of moving average
        :param lambda_value: int, for regulation coefficient of L1 and L2 filter
        :param batch_size: int, for batch process using L1 or L2
        :return: motor current tend Series with datetime index
        """
        try:
            if self.data['Motor Current'] is not None:
                motor_current = pd.Series(self.data['Motor Current']).dropna()
                if method == 'ma':
                    motor_current_trend = motor_current.rolling(bin_size).mean()
                elif method == 'l1':
                    motor_current_trend = trend.trend_filter(motor_current, reg_norm=1, lambda_value=lambda_value,
                                                             batch_size=batch_size)
                elif method == 'l2':
                    motor_current_trend = trend.trend_filter(motor_current, reg_norm=2, lambda_value=lambda_value,
                                                             batch_size=batch_size)
                else:
                    print('Method is not available, please select "ma", "l1" or "l2"')
                    motor_current_trend = None

            else:
                print('No Motor Current')
                motor_current_trend = None
        except KeyError:
            motor_current_trend = None
        return motor_current_trend

    def tms_period(self, days=30):
        """
        Calculate the period of TMS temperature control cycle in seconds

        :param days: int, number of days in a bin for FFT and then cycle period
        :return: TMS temperature cycle period in seconds with datetime index
        """
        try:
            assert self.data_ops['TMS Temperature'] is not None
            tms = pd.Series(self.data_ops['TMS Temperature']).dropna()
            tms_period = cycle.fft_cycle(tms, days=days, plot=False)
        except KeyError:
            tms_period = None
        return tms_period

    def rotor_contact(self, normal=True, current_pk_t=4, vb_pk_t=2, current_lag=10, v_lag=3):
        """
        Count the number of possible rotor blade contacts
        If there is spike in Vibration B and Motor Current at same time(within 1 minute), then large contact.
        If there is spike only in Vibration B, then small contact

        :param normal:if normal = True, then only detect the contact for data in Normal status,otherwise detect contact
        over all data
        :param current_pk_t:std threshold for motor current peak
        :param vb_pk_t:std threshold for vibration b peak
        :param current_lag:only detect 1 peak in lag data points
        :param v_lag:only detect 1 peak in lag data points
        :return:
        """
        try:
            assert 'Motor Current' in self.column_names
            assert 'Vibration B' in self.column_names
            assert 'Vibration H' in self.column_names
            if normal:
                motor_current = pd.Series(self.data_ops['Motor Current']).dropna()
                vib_b = pd.Series(self.data_ops[self.data_ops['Vibration B'] < 0.1]['Vibration B']).dropna()
                vib_h = pd.Series(self.data_ops[self.data_ops['Vibration H'] < 0.1]['Vibration H']).dropna()
            else:
                motor_current = pd.Series(self.data['Motor Current']).dropna()
                vib_b = pd.Series(self.data[self.data['Vibration B'] < 0.1]['Vibration B']).dropna()
                vib_h = pd.Series(self.data[self.data['Vibration H'] < 0.1]['Vibration H']).dropna()

            # Motor Current Peak and Spike counter
            _, current_pk_count = peak.PeakDetect.spike_detect(motor_current, 99, current_pk_t, current_lag, 'Motor Current', plot=False)
            _, vb_pk_count = peak.PeakDetect.spike_detect(vib_b, 99, vb_pk_t, v_lag, 'Vibration B', plot=False)

            large_contact = {}
            small_contact = {}
            l_n, s_n = 0, 0
            small_contact[vib_b.index[0]] = s_n
            large_contact[vib_b.index[0]] = l_n
            if len(vb_pk_count) != 0:
                for i in range(len(vb_pk_count)):
                    idx = vb_pk_count.index[i]
                    value = vb_pk_count.values[i]
                    if value > 0 and len(current_pk_count) != 0:
                        large = any(abs(current_pk_count.index - vb_pk_count.index[i]) < timedelta(minutes=1))
                        if large:
                            l_n += 1
                            large_contact[vb_pk_count.index[i]] = l_n
                        else:
                            s_n += 1
                            small_contact[vb_pk_count.index[i]] = s_n
                    elif value > 0 and len(current_pk_count) == 0:
                        small_contact[vb_pk_count.index[i]] = s_n + 1
        except AssertionError:
            large_contact, small_contact = None, None
        return large_contact, small_contact

    def vib_anomaly_score(self):
        """
        Calculate the anomaly score of Vibration B and Vibration H during Normal status

        :return: dictionary with key as datetime, value as score
        """
        try:
            assert self.data_ops[['Vibration H', 'Vibration B']] is not None
            vib_s = vib_score.score(self.data_ops, plot=False)
        except KeyError:
            vib_s = None
        return vib_s

    def start_anomaly_score(self, start_period=400):
        """
        Calculate the anomaly score between Motor Speed, Motor Current and Vibration H during Acceleration

        :param start_period: the period in seconds which parts of the Acceleration events
        :return: dictionary of anomaly scores with datetime as key and score as value
        """
        try:
            assert self.data[['Motor Speed', 'Motor Current', 'Vibration H']] is not None
            data = self.data.fillna(method='ffill')
            start_score = {}
            start_period = start_period
            for i in range(len(self.data_acc.index)):
                if i != 0 and self.data_acc.index[i] < self.data_acc.index[i - 1] + timedelta(seconds=start_period):
                    continue
                else:
                    data_bin = data[(self.data_acc.index[i] <= data.index) &
                                    (data.index <= (self.data_acc.index[i] + timedelta(seconds=start_period)))][
                        ['Motor Speed', 'Motor Current', 'Vibration H']]
                    data_bin = data_bin.resample('1S').pad().fillna(method='bfill')

                    data_bin['Motor Speed Trend'] = trend.trend_filter(data_bin['Motor Speed'], batch_size=None)
                    data_bin['Motor Current Trend'] = trend.trend_filter(data_bin['Motor Current'], batch_size=None)
                    data_bin['Vibration H Trend'] = trend.trend_filter(data_bin['Vibration H'], batch_size=None)

                    data_trend = data_bin[['Motor Speed Trend', 'Motor Current Trend', 'Vibration H Trend']]
                    R = data_trend.corr()
                    r = min(R.values.reshape(1, 9)[R.values.reshape(1, 9) != 1.0])
                    anomaly_score = (1 - r) / 2
                    acc_index = self.data_acc.index[i]
                    start_score[acc_index] = anomaly_score
        except KeyError:
            start_score = None
        return start_score

    def rotor_shaft_displacement(self):
        try:
            data = self.data
            data['dYh'] = data['Magnetic Bearing Current +YH'] - data[
                'Magnetic Bearing Current -YH']
            data['dXh'] = data['Magnetic Bearing Current +XH'] - data[
                'Magnetic Bearing Current -XH']
            data['dXYh_abs'] = (data['dYh'] ** 2 + data['dXh'] ** 2) ** 0.5
            data['dYb'] = data['Magnetic Bearing Current +YB'] - data[
                'Magnetic Bearing Current -YB']
            data['dXb'] = data['Magnetic Bearing Current +XB'] - data[
                'Magnetic Bearing Current -XB']
            data['dXYb_abs'] = (data['dYb'] ** 2 + data['dXb'] ** 2) ** 0.5
            return data[['Equipment Status', 'dXYb_abs', 'dXYh_abs']]
        except KeyError:
            data = None
            return data
