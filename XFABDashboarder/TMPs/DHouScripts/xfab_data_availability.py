from stp import data_availability
from os import path, listdir
import os
import sys
import datetime
import pandas as pd

try: 
    ROOT_PATH = sys.path[0][:sys.path[0].rindex('\\')]
except:
    ROOT_PATH = r'C:\Users\a00555655\OneDrive - ONEVIRTUALOFFICE\Documents\Python Scripts\XFABDashboarder\TMPs'
data_folder = os.path.join(ROOT_PATH, 'Dashboards', 'Data_For_Indicators')
DHouScripts_path = os.path.join(ROOT_PATH, 'DHouScripts')

sys.path.insert(1, DHouScripts_path)

import xfab_STP_Data_Extract

file_names = listdir(data_folder)

availability = {}
for file_name in file_names:
    file_path = path.join(data_folder, file_name)
    data = pd.read_parquet(file_path)
    data = data.sort_index()
    tool_name = file_name.replace('.parquet', '')

    start, stop, total_days, avail_days, avail_rate, cols = data_availability.data_availability(data)
    N_cols = len(cols)
    availability[tool_name] = [start, stop, total_days, avail_days, avail_rate, N_cols, cols]

df_availability = pd.DataFrame.from_dict(availability,orient='index',
                                         columns=['start', 'stop', 'total_days', 'avail_days',
                                                  'avail_rate', 'N_cols', 'cols'])

AVAIL_PATH = os.path.join(ROOT_PATH, 'Dashboards', 'Availability')

df_availability.to_csv(os.path.join(AVAIL_PATH,'xfab_data_availability.csv'))
