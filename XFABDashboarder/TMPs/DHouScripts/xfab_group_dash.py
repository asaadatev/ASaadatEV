# -*- coding: utf-8 -*-
"""
Created on Tue Nov  2 09:52:17 2021

@author: Administrator
"""

import matplotlib.pyplot as plt
from os import listdir, path, mkdir
from datetime import timedelta
import pandas as pd
import datetime
import warnings
import pickle
import math

warnings.filterwarnings("ignore", category=RuntimeWarning)


def group_plot(parameters, latest_folder, dash_folder, prefix='', data_folder=None):
    if 'parquet' in listdir(latest_folder)[0]:
        file_names = [file for file in listdir(latest_folder) if file[-7:] == 'parquet']
    elif 'csv' in listdir(latest_folder)[0]:
        file_names = [file for file in listdir(latest_folder) if file[-3:] == 'csv']
    n_files = len(file_names)
    n_row = math.ceil(n_files / 4)
    for parameter in parameters:
        print(parameter)
        fig, axes = plt.subplots(n_row, 4, sharex=True, sharey=True, squeeze=True, figsize=(16, 9))
        fig.suptitle(parameter, y=0.95)
        data_y_max = []
        data_y_min = []
        for file_name, ax in zip(file_names, axes.reshape(-1)):
            file_path = path.join(latest_folder, file_name)
            if 'parquet' in file_name:
                tool_name = file_name.replace('.parquet', '')
                data = pd.read_parquet(file_path)
            elif 'csv' in file_name:
                tool_name = file_name.replace('.csv', '')
                data = pd.read_csv(file_path)
                data.set_index('LogTime', inplace=True)
                data.index = pd.to_datetime(data.index)

            try:
                assert parameter in data.columns
                data_p = data.sort_index()[parameter].dropna()
                if len(data_p) > 100000:
                    data_p = data_p.resample('10T').mean()
                try:
                    if parameter == 'Damage Limit Counter':
                        data_max = 200
                    else:
                        data_max = max(data_p)
                    data_min = min(data_p)
                    data_y_max.append(data_max)
                    data_y_min.append(data_min)
                except:
                    pass
            except AssertionError:
                data_p = []
            if parameter != 'current_pk_count':
                if parameter == 'Damage Limit Counter':
                    data_p = data_p[data_p<200]
                data_p = data_p.fillna(method='ffill')
                ax.plot(data_p)

            else:
                raw = pd.read_parquet(path.join(data_folder, tool_name + '.parquet'))
                if 'Motor Current' in raw.columns and len(data_p) != 0:
                    motor_current = raw['Motor Current']
                    ax.plot(motor_current, alpha=0.5)
                    ax.plot(data_p.keys(),
                            [motor_current.loc[i] for i in data_p.keys()],
                            'o', c='r', alpha=0.5)
                    fig.suptitle('current peak', y=0.95)
                else:
                    pass
            ax.set_ylabel(tool_name)
            ax.grid()

        try:
            if parameter == 'dec_period':
                ylim = (0, 600)
            else:

                ylim = (min(data_y_min), max(data_y_max) * 1.1)
            xlim = (min(data.index) - timedelta(days=2),
                    max(data.index) + timedelta(days=2))
            _ = plt.setp(axes, ylim=ylim, xlim=xlim)

        except:
            pass
        for i in range(4):
            _ = plt.setp(axes[n_row - 1][i].xaxis.get_majorticklabels(), rotation=25)

        with open(path.join(dash_folder, f'{prefix}{parameter}.pkl'), 'wb') as fid:
            pickle.dump(fig, fid)
        fig.savefig(path.join(dash_folder, f'{prefix}{parameter}.png'))


date = datetime.date.today().strftime("%Y-%m-%d")
# Raw parameters -------------------------------------------------------------
data_folder = r'D:\XFab_STP\data'
folder_names = listdir(data_folder)

latest_date = max([datetime.datetime.strptime(i, '%Y-%m-%d')
                   for i in folder_names]).strftime("%Y-%m-%d")
latest_folder = path.join(data_folder, latest_date)

file_names = listdir(latest_folder)

dash_folder = path.join(r'D:\XFab_STP\dash_group',
                        date)
try:
    mkdir(dash_folder)
except OSError:
    print("Failed to create new folder, check if already exist")
else:
    print("Successfully created new folder")

parameters = ['Controller Hour Counter', 'Ctrl Temperature', 'DC Link Voltage',
              'Damage Limit Counter', 'Disturbance Counter',
              'Motor Current', 'Motor Speed', 'Motor Temperature', 'PF Counter',
              'Pos XB', 'Pos XH', 'Pos YB', 'Pos YH', 'Pos Z', 'Pump Hour Counter',
              'Start Counter', 'TMS Temperature', 'Vibration B', 'Vibration H',
              'Vibration Z', 'Equipment Status']

group_plot(parameters, latest_folder, dash_folder)
# ----------------------------------------------------------------------------

date = datetime.date.today().strftime("%Y-%m-%d")
# Indicators -----------------------------------------------------------------
indicator_folder = r'D:\XFab_STP\dash'
folder_names = listdir(indicator_folder)

latest_date = max([datetime.datetime.strptime(i, '%Y-%m-%d')
                   for i in folder_names]).strftime("%Y-%m-%d")
data_folder = path.join(r'D:\XFab_STP\data', latest_date)
latest_folder = path.join(indicator_folder, latest_date)

file_names = [file for file in listdir(latest_folder) if file[-3:] == 'csv']  # csv only

dash_folder = path.join(r'D:\XFab_STP\dash_group',
                        date)
try:
    mkdir(dash_folder)
except OSError:
    print("Failed to create new folder, check if already exist")
else:
    print("Successfully created new folder")

indicators = ['current_pk_count', 'tms_period', 'current_trend_ma',
              'large_contact', 'small_contact', 'vib_anomaly', 'dec_period',
              'start_anomaly', 'dXYb_abs', 'dXYh_abs']

group_plot(indicators, latest_folder, dash_folder, prefix='Ind_', data_folder=data_folder)
