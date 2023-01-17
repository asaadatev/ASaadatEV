from ._base import Base
from os import listdir, path
import pyodbc
from edwards.data import Odbc
import datetime
import pandas as pd
from time import time


def log_time(func):
    """Logs the time it took for func to execute"""
    def wrapper(*args, **kwargs):
        start = time()
        val = func(*args, **kwargs)
        end = time()
        duration = end - start
        print(f'{func.__name__} took {duration} seconds to run')
        return val
    return wrapper


class ParquetLoader(Base):

    def __init__(self, folder_path, system_name):
        self.folder_path = folder_path
        self.system_name = system_name
        self.file_names = listdir(folder_path)
        self.subfix_to_remove = ['parquet', 'copy']

    def get_data(self):
        system_names = []
        for f_name in self.file_names:
            name = [i for i in f_name.split('.') if i not in self.subfix_to_remove][0]
            system_names.append(name)
        if self.system_name in system_names:
            files = [file_name for file_name in self.file_names if self.system_name in file_name]
            data = None
            for file in files:
                print(file)
                file_path = path.join(self.folder_path, file)
                df = pd.read_parquet(file_path)
                if data is None:
                    data = df
                else:
                    data = pd.concat([data, df])
            return data
        else:
            print('System name not in data folder')


class OdbcLoaderDP(Base):
    def __init__(self):
        self.server = Odbc.SERVER
        self.uid = Odbc.UID
        self.pwd = Odbc.PWD

    @log_time
    def get_data(self,database, system_type_id, system_name, start_datetime=datetime.date(1970, 1, 1),
                 end_datetime=datetime.date.today()):
        con = pyodbc.connect('Driver={SQL Server};''Server=' + self.server + ';''Database=' + database + ';' 'Uid=' +
                             self.uid + ';''Pwd=' + self.pwd + ';')
        sql = ('SELECT [SystemID], [SystemTypeID], [Description] '
               'FROM [dbo].[fst_GEN_System] '
               'WHERE [SystemTypeID]=' + str(system_type_id) +
               'ORDER BY [Description]')
        system_info = pd.read_sql_query(sql, con)

        systems_namelist = list(set(system_info.Description))

        assert system_name in systems_namelist

        parameter_info = Odbc.get_parameter_info(database, system_type_id=system_type_id)

        parameter_numberlist = list(set(parameter_info.ParameterNumber))
        data = Odbc.get_data(database=database,
                             system_name=system_name,
                             parameter_number=parameter_numberlist,
                            #  system_type_id=system_type_id,
                             start_datetime=start_datetime,
                             end_datetime=end_datetime)
        if data is not None:
            try:
                assert 'LogTime' in data.columns
            except AssertionError:
                data = data.rename(columns={'logTime': 'LogTime'})
            df = pd.DataFrame(index=data.LogTime)
            df = df[~df.index.duplicated()]
            for col in data.zzDescription.unique():
                try:
                    value = list(data.groupby(by='zzDescription').get_group(col).Value)
                    logTime = data.groupby(by='zzDescription').get_group(col).LogTime
                    series = pd.Series(data=value, index=logTime)
                    df[col] = series[~series.index.duplicated()]
                except KeyError:
                    print(col, ' KeyError')
                    pass
            data = df
        else:
            print(system_name, ":No data")
        return data


class OdbcLoaderSTP(Base):
    def __init__(self):
        self.server = Odbc.SERVER
        self.uid = Odbc.UID
        self.pwd = Odbc.PWD

    @log_time
    def get_data(self,database, system_type_id, system_name, start_datetime=datetime.date(1970, 1, 1),
                 end_datetime=datetime.date.today()):
        con = pyodbc.connect('Driver={SQL Server};''Server=' + self.server + ';''Database=' + database + ';' 'Uid=' +
                             self.uid + ';''Pwd=' + self.pwd + ';')
        sql = ('SELECT [SystemID], [SystemTypeID], [Description] '
               'FROM [dbo].[fst_GEN_System] '
               'WHERE [SystemTypeID]=' + str(system_type_id) +
               'ORDER BY [Description]')
        system_info = pd.read_sql_query(sql, con)

        systems_namelist = list(set(system_info.Description))

        assert system_name in systems_namelist

        parameter_info = Odbc.get_parameter_info(database, system_type_id=system_type_id)

        parameter_numberlist = list(set(parameter_info.ParameterNumber))
        status_map = {'Levitation': 1, 'No Levitation': 2, 'Acceleration': 3, 'Normal': 4,
                      'Deceleration': 5, 'Autotest': 6, 'Tuning': 7, 'Tuning Complete': 8,
                      'Driver Enable': 0, 'Identifying': -1, 'No Communication': -2,
                      'Unavailable': -3, 'Comms Fail': -4, 'Network Fault': -5,
                      'Error': -6
                      }

        status = Odbc._get_status(database=database,
                                  system_name=system_name,
                                  system_type_id=system_type_id,
                                  start_datetime=start_datetime,
                                  end_datetime=end_datetime)
        try:
            status['zzDescription'] = 'Equipment Status'
            status['Value'] = status['primary_message'].map(status_map)
            ops_mode = status.drop(columns=['primary_message'])
            try:
                assert 'LogTime' in ops_mode.columns
            except AssertionError:
                ops_mode = ops_mode.rename(columns={'logTime': 'LogTime'})
        except TypeError:
            ops_mode = None
        data = Odbc.get_data(database=database,
                             system_name=system_name,
                             parameter_number=parameter_numberlist,
                             start_datetime=start_datetime,
                             end_datetime=end_datetime)
        if data is not None:
            try:
                assert 'LogTime' in data.columns
            except AssertionError:
                data = data.rename(columns={'logTime': 'LogTime'})
        if ops_mode is not None:
            data_all = pd.concat([data, ops_mode])
        else:
            data_all = data

        if data_all is not None:
            df = pd.DataFrame(index=data_all.LogTime)
            df = df[~df.index.duplicated()]
            for col in data_all.zzDescription.unique():
                value = list(data_all.groupby(by='zzDescription').get_group(col).Value)
                logTime = data_all.groupby(by='zzDescription').get_group(col).LogTime
                series = pd.Series(data=value, index=logTime)
                df[col] = series[~series.index.duplicated()]
            data = df
        else:
            print(system_name, ":No data")
        return data



