import datetime
import pyodbc
from edwards.old_edwards import Odbc
import pandas as pd
import matplotlib.pyplot as plt
from os import path, listdir, mkdir, makedirs
import sys
import os

try:
    ROOT_PATH = sys.path[0][:sys.path[0].rindex('\\')]
except:
    ROOT_PATH = r'C:\Users\a00555655\OneDrive - ONEVIRTUALOFFICE\Documents\Python Scripts\XFABDashboarder\TMPs'

RESOURCES_PATH = os.path.join(ROOT_PATH, r'Resources')

date = datetime.date.today().strftime("%Y-%m-%d")
indicators_data_folder = path.join(ROOT_PATH, 'Dashboards', 'Data_For_Indicators')

try:
    makedirs(indicators_data_folder)
except OSError:
    print("Failed to create new folder, check if already exist")
else:
    print("Successfully created new folder")

server = Odbc.SERVER
# server = r'20.0.0.8\FABWORKS'
uid = Odbc.UID
pwd = Odbc.PWD
database = 'scada_Production_XFAB_France'
system_type_id = 112 # 112 for STP pump

con = pyodbc.connect('Driver={SQL Server};'
                     'Server=' + server + ';'
                     'Database=' + database + ';'
                     'Uid=' + uid + ';'
                     'Pwd=' + pwd + ';')

# sql = ('SELECT [SystemID], [SystemTypeID], [Description] '
#        'FROM [dbo].[fst_GEN_System] '
#        'WHERE [SystemTypeID]=' + str(system_type_id) +
#         " AND [Description] like '%LP42C%' "
#        'ORDER BY [Description]')

sql = ('SELECT [SystemID], [SystemTypeID], [Description] '
       'FROM [dbo].[fst_GEN_System] '
       'WHERE [SystemTypeID]=' + str(system_type_id) +
       'ORDER BY [Description]')

system_info = pd.read_sql_query(sql, con)

systems_namelist = list(set(system_info.Description))

parameter_info = Odbc.get_parameter_info(database,
                                         system_type_id=system_type_id)

parameter_numberlist = list(set(parameter_info.ParameterNumber))

# parameter_numberlist_target = [1,2,4,23,40,41,42,100,101,102,104]

status_map = {'Levitation':1, 'No Levitation':2, 'Acceleration':3, 'Normal':4,
              'Deceleration':5, 'Autotest':6, 'Tuning':7, 'Tuning Complete':8,
              'Driver Enable':0,'Identifying': -1, 'No Communication':-2,
              'Unavailable':-3, 'Comms Fail': -4, 'Network Fault':-5,
              'Error': -6
              }

for system_name in systems_namelist[:]:
    # print(system_name)
    """
    data_target = Odbc.get_data(database= database,
                         system_name= system_name,
                         parameter_number=parameter_numberlist_target,
                         start_datetime=datetime.date(2020, 1, 10),
                         end_datetime=datetime.date(2021, 6, 9))
    """
    status = Odbc._get_status(database=database,
                              system_name=system_name,
                              system_type_id=system_type_id)
    # start_datetime=datetime.date(2020, 1, 10),
    # end_datetime=datetime.date(2021, 6, 19))
    if status is not None:
        status['zzDescription'] = 'Equipment Status'
        status['Value'] = status['primary_message'].map(status_map)
        ops_mode = status.drop(columns=['primary_message'])
        try:
            assert 'LogTime' in ops_mode.columns
        except AssertionError:
            ops_mode = ops_mode.rename(columns={'logTime': 'LogTime'})
    else:
        ops_mode = None
    data = Odbc.get_data(database=database,
                         system_name=system_name,
                         parameter_number=parameter_numberlist)
    # start_datetime=datetime.date(2020, 1, 10),
    # end_datetime=datetime.date(2021, 6, 19))
    if data is not None:
        try:
            assert 'LogTime' in data.columns
        except AssertionError:
            data = data.rename(columns={'logTime': 'LogTime'})

    if data is not None and ops_mode is not None:
        data_all = pd.concat([data, ops_mode])
    else:
        data_all = None

    plot = False

    if data_all is not None:
        df = pd.DataFrame(index=data_all.LogTime)
        df = df[~df.index.duplicated()]
        for col in data_all.zzDescription.unique():
            value = list(data_all.groupby(by='zzDescription').get_group(col).Value)
            logTime = data_all.groupby(by='zzDescription').get_group(col).LogTime
            series = pd.Series(data=value, index=logTime)
            df[col] = series[~series.index.duplicated()]
        if plot == True:
            param_check = ['Motor Current', 'Motor Speed', 'Vibration B', 'Vibration H',
                           'Motor Temperature', 'Ctrl Temperature', 'TMS Temperature']
            df[param_check].plot(subplots=True, figsize=(10, 6), title=system_name)
    else:
        print(system_name, ":No data")

    if data_all is not None:
        file_name = system_name + '.parquet'
        file_path = path.join(indicators_data_folder, file_name)
        df.to_parquet(file_path, compression=None)