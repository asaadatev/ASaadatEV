"""
Query equipment data and meta info from SQL server.

@author: Danny Sun
"""

import warnings
import datetime

import pyodbc
import pandas as pd
import numpy as np


class Odbc:
    """Query equipment data and meta info from SQL server."""

    SERVER = r'10.44.221.8\FABWORKS'
    UID = 'sa'
    PWD = '!Tat00ine'

    def __init__(self,
                 data,
                 database=None,
                 system_name=None,
                 system_type_id=None,
                 parameter_number=None,
                 parameter_name=None,
                 parameter_unit_id=None,
                 start_datetime=None,
                 end_datetime=None):
        self.data = data
        self.database = database
        self.system_name = system_name
        self.system_type_id = system_type_id
        self.parameter_number = parameter_number
        self.parameter_unit_id = parameter_unit_id
        self.parameter_name = parameter_name
        self.start_datetime = start_datetime
        self.end_datetime = end_datetime

    @classmethod
    def create(cls,
               database,
               system_name,
               parameter_number,
               start_datetime=datetime.date(1970, 1, 1),
               end_datetime=datetime.date.today(),
               server=None,
               uid=None,
               pwd=None):
        """Create data object with single paramter.

        Parameters
        ----------
        database : str, list, or tuple
            Database name or names.
        system_name : str
            System name.
        parameter_number : int
            Parameter number.
        start_datetime : datetime, optional
            The default is datetime.date(1970, 1, 1).
        end_datetime : datetime, optional
            The default is datetime.date.today().
        server : str, optional
            Data warehouse address.
        uid : str, optional
            User name.
        pwd : str, optional
            Password.
        """
        if not (isinstance(system_name, str) &
                isinstance(parameter_number, int)):
            raise TypeError('Invalid type')

        data = Odbc.get_data(database=database,
                             system_name=system_name,
                             parameter_number=parameter_number,
                             start_datetime=start_datetime,
                             end_datetime=end_datetime,
                             server=server,
                             uid=uid,
                             pwd=pwd)

        del data['Description']    # System name
        del data['zzDescription']  # Parameter name
        data.rename(columns={'LogTime': 'datetime',
                             'Value': 'value'},
                    inplace=True)
        data.set_index('datetime', inplace=True)

        system_type_id = Odbc.get_system_type_id(
            database=database,
            system_name=system_name,
            server=server,
            uid=uid,
            pwd=pwd)
        parameter_name = Odbc.get_parameter_name(
            database=database,
            parameter_number=parameter_number,
            system_name=system_name,
            server=server,
            uid=uid,
            pwd=pwd)
        parameter_unit_id = Odbc.get_parameter_unit_id(
            database=database,
            parameter_number=parameter_number,
            system_name=system_name,
            server=server,
            uid=uid,
            pwd=pwd)

        return cls(data=data,
                   database=database,
                   system_name=system_name,
                   system_type_id=system_type_id,
                   parameter_number=parameter_number,
                   parameter_name=parameter_name,
                   parameter_unit_id=parameter_unit_id,
                   start_datetime=start_datetime,
                   end_datetime=end_datetime)

    @classmethod
    def create2(cls,
                database,
                system_name,
                parameter_number,
                start_datetime=datetime.date(1970, 1, 1),
                end_datetime=datetime.date.today(),
                col_name='parameter_name',
                server=None,
                uid=None,
                pwd=None):
        """Create data object with single or multiple paramters.

        Parameters
        ----------
        database : str, list, or tuple
            Database name or names.
        system_name : str
            System name.
        parameter_number : int, list, or tuple
            Parameter number or numbers.
        start_datetime : datetime, optional
            The default is datetime.date(1970, 1, 1).
        end_datetime : datetime, optional
            The default is datetime.date.today().
        col_name : {'parameter_name', 'parameter_number'}
            Choice of dataframe column names. Default use 'parameter_name'.
        server : str, optional
            Data warehouse address.
        uid : str, optional
            User name.
        pwd : str, optional
            Password.
        """
        if not isinstance(system_name, str):
            raise TypeError('Invalid type')

        if isinstance(parameter_number, int):
            parameter_number = [parameter_number]

        data = Odbc.get_data(database=database,
                             system_name=system_name,
                             parameter_number=parameter_number,
                             start_datetime=start_datetime,
                             end_datetime=end_datetime,
                             server=server,
                             uid=uid,
                             pwd=pwd)
        del data['Description']  # `system_name`
        data.rename(columns={'LogTime': 'datetime',
                             'zzDescription': 'parameter'},
                    inplace=True)
        data = data.pivot_table(index='datetime', columns='parameter',
                                values='Value', aggfunc=np.mean)
        data.sort_index(inplace=True)

        system_type_id = Odbc.get_system_type_id(
            database=database,
            system_name=system_name,
            server=server,
            uid=uid,
            pwd=pwd)

        parameter_name = {}
        for i in parameter_number:
            parameter_name[i] = Odbc.get_parameter_name(
                database=database,
                parameter_number=i,
                system_name=system_name,
                server=server,
                uid=uid,
                pwd=pwd)

        parameter_unit_id = {}
        for i in parameter_number:
            parameter_unit_id[i] = Odbc.get_parameter_unit_id(
                database=database,
                parameter_number=i,
                system_name=system_name,
                server=server,
                uid=uid,
                pwd=pwd)

        # Use parameter number as the column name
        if col_name == 'parameter_number':
            name_to_number = dict(
                (v, str(k)) for k, v in parameter_name.items())
            data.rename(columns=name_to_number, inplace=True)
        elif col_name != 'parameter_name':
            raise TypeError('Invalid type')

        return cls(data=data,
                   database=database,
                   system_name=system_name,
                   system_type_id=system_type_id,
                   parameter_number=parameter_number,
                   parameter_name=parameter_name,
                   parameter_unit_id=parameter_unit_id,
                   start_datetime=start_datetime,
                   end_datetime=end_datetime)

    @staticmethod
    def get_data(database,
                 system_name,
                 parameter_number,
                 start_datetime=datetime.date(1970, 1, 1),
                 end_datetime=datetime.date.today(),
                 server=None,
                 uid=None,
                 pwd=None):
        """Query data from SQL server.

        Parameters
        ----------
        database : str, list, or tuple
        system_name : str, list, or tuple
        parameter_number : int, list, or tuple
        start_datetime : datetime
        end_datetime : datetime
        server : str, default `Odbc.SERVER`
        uid : str, default `Odbc.UID`
        pwd : str, default `Odbc.PWD`

        Returns
        -------
        DataFrame

        Notes
        -----
        If `database` does not exist, generate Error.
        If no data, or `system_name` / `parameter_name` does not exist,
        return None.
        If multiple databases, return combined & sorted & no duplicated
        data frame.
        """
        server = Odbc.SERVER if server is None else server
        uid = Odbc.UID if uid is None else uid
        pwd = Odbc.PWD if pwd is None else pwd

        if isinstance(database, str):
            database = [database]

        if isinstance(system_name, str):
            system_name = [system_name]

        if isinstance(parameter_number, int):
            parameter_number = [parameter_number]

        i_data = None
        for i_sn in system_name:

            j_data = None
            for j_pn in parameter_number:

                k_data = None
                for k_db in database:
                    temp = Odbc._get_data(database=k_db,
                                          system_name=i_sn,
                                          parameter_number=j_pn,
                                          start_datetime=start_datetime,
                                          end_datetime=end_datetime,
                                          server=server,
                                          uid=uid,
                                          pwd=pwd)
                    if (temp is not None) & (k_data is None):
                        k_data = temp.copy()
                    elif (temp is not None) & (k_data is not None):
                        k_data = pd.concat([k_data, temp],
                                           axis=0,
                                           ignore_index=True)

                if k_data is not None:
                    parameter_name = Odbc.get_parameter_name(
                        database=database,
                        parameter_number=j_pn,
                        system_name=i_sn,
                        server=server,
                        uid=uid,
                        pwd=pwd)
                    k_data['zzDescription'] = parameter_name

                if (k_data is not None) & (j_data is None):
                    j_data = k_data.copy()
                elif (k_data is not None) & (k_data is not None):
                    j_data = pd.concat([j_data, k_data],
                                       axis=0,
                                       ignore_index=True)

            if (j_data is not None) & (i_data is None):
                i_data = j_data.copy()
            elif (j_data is not None) & (i_data is not None):
                i_data = pd.concat([i_data, j_data],
                                   axis=0,
                                   ignore_index=True)

        data = i_data
        if data is not None:
            data = data.drop_duplicates(ignore_index=True)
            data = data.sort_values(['Description',
                                     'zzDescription',
                                     'LogTime'])
            data = data.reset_index(drop=True)

        return data

    @staticmethod
    def _get_data(database,
                  system_name,
                  parameter_number,
                  start_datetime=datetime.date(1970, 1, 1),
                  end_datetime=datetime.date.today(),
                  server=None,
                  uid=None,
                  pwd=None):

        server = Odbc.SERVER if server is None else server
        uid = Odbc.UID if uid is None else uid
        pwd = Odbc.PWD if pwd is None else pwd

        if not isinstance(database, str):
            raise TypeError('Invalid type')

        con = pyodbc.connect('Driver={SQL Server};'
                             'Server=' + server + ';'
                             'Database=' + database + ';'
                             'Uid=' + uid + ';'
                             'Pwd=' + pwd + ';')

        if isinstance(system_name, str):
            system_name = [system_name]

        if isinstance(parameter_number, int):
            parameter_number = [parameter_number]

        sql = (
            'SELECT t3.[Description], t4.[zzDescription], '
            '       t1.[LogTime], t1.[Value] '
            'FROM [dbo].[fst_GEN_ParameterValue] AS t1 '
            '  INNER JOIN '
            '    [dbo].[fst_GEN_Parameter] AS t2 '
            '      ON '
            '    t1.[ParameterId] = t2.[ParameterID] '
            '  INNER JOIN '
            '    [dbo].[fst_GEN_System] AS t3 '
            '      ON '
            '    t2.[SystemID] = t3.[SystemID] '
            '  INNER JOIN '
            '    [dbo].fst_GEN_ParameterType AS t4 '
            '      ON '
            '    t2.[SystemTypeID] = t4.[SystemTypeID] '
            '      AND '
            '    t2.[ParameterNumber] = t4.[ParameterNumber] '
            'WHERE '
            ' t3.[Description] in '
            + '(\'' + '\',\''.join(system_name) + '\')' +
            '    AND '
            ' t2.[ParameterNumber] in '
            '(\''
            + '\',\''.join([str(i) for i in parameter_number]) +
            '\')'
            '    AND '
            ' t1.[LogTime] BETWEEN '
            + '\'' + '{:%Y-%m-%d %H:%M:%S}'.format(start_datetime) + '\'' +
            ' AND '
            + '\'' + '{:%Y-%m-%d %H:%M:%S}'.format(end_datetime) + '\' ' +
            'ORDER BY t1.[LogTime]')

        data = pd.read_sql_query(sql, con)

        if data.empty:
            data = None

        return data

    @staticmethod
    def _get_status(database,
                  system_name,
                  system_type_id,
                  start_datetime=datetime.date(1970, 1, 1),
                  end_datetime=datetime.date.today(),
                  server=None,
                  uid=None,
                  pwd=None):

        server = Odbc.SERVER if server is None else server
        uid = Odbc.UID if uid is None else uid
        pwd = Odbc.PWD if pwd is None else pwd

        if not isinstance(database, str):
            raise TypeError('Invalid type')

        con = pyodbc.connect('Driver={SQL Server};'
                             'Server=' + server + ';'
                             'Database=' + database + ';'
                             'Uid=' + uid + ';'
                             'Pwd=' + pwd + ';')

        if isinstance(system_name, str):
            system_name = [system_name]


        sql = (
            'SELECT t3.[Description], t1.[logTime], t2.[primary_message]'
            'FROM [dbo].[fst_GEN_SystemStatus] AS t1 '
            '  INNER JOIN '
            '    [dbo].[fst_LNG_Message] AS t2 '
            '      ON '
            '    t1.[Message] = t2.[hub_id] '
            '  INNER JOIN '
            '    [dbo].[fst_GEN_System] AS t3 '
            '      ON '
            '    t1.[SystemID] = t3.[SystemID] '
            'WHERE '
            ' t3.[Description] in '
            + '(\'' + '\',\''.join(system_name) + '\')' +
            '    AND '
            ' t1.[SystemTypeID] = ' + str(system_type_id) +
            '    AND '
            ' t1.[LogTime] BETWEEN '
            + '\'' + '{:%Y-%m-%d %H:%M:%S}'.format(start_datetime) + '\'' +
            ' AND '
            + '\'' + '{:%Y-%m-%d %H:%M:%S}'.format(end_datetime) + '\' ' +
            'ORDER BY t1.[LogTime]')

        data = pd.read_sql_query(sql, con)

        if data.empty:
            data = None

        return data

    @staticmethod
    def get_system_info(database,
                        server=None,
                        uid=None,
                        pwd=None):
        """Query system info from SQL server.

        Parameters
        ----------
        database : str, list, or tuple
        server : str, default `Odbc.SERVER`
        uid : str, default `Odbc.UID`
        pwd : str, default `Odbc.PWD`

        Returns
        -------
        DataFrame

        Notes
        -----
        If database does not exist, generate Error.
        """
        server = Odbc.SERVER if server is None else server
        uid = Odbc.UID if uid is None else uid
        pwd = Odbc.PWD if pwd is None else pwd

        system_info = None

        if isinstance(database, str):
            database = [database]

        for i_database in database:
            temp = Odbc._get_system_info(
                database=i_database,
                server=server,
                uid=uid,
                pwd=pwd)
            if (temp is not None) & (system_info is None):
                system_info = temp.copy()
            elif (temp is not None) & (system_info is not None):
                system_info = pd.concat([system_info, temp],
                                        axis=0,
                                        ignore_index=True)

        if system_info is not None:
            system_info = system_info.drop_duplicates(ignore_index=True)
            system_info = system_info.reset_index(drop=True)

        return system_info

    @staticmethod
    def _get_system_info(database,
                         server=None,
                         uid=None,
                         pwd=None):
        server = Odbc.SERVER if server is None else server
        uid = Odbc.UID if uid is None else uid
        pwd = Odbc.PWD if pwd is None else pwd

        con = pyodbc.connect('Driver={SQL Server};'
                             'Server=' + server + ';'
                             'Database=' + database + ';'
                             'Uid=' + uid + ';'
                             'Pwd=' + pwd + ';')

        sql = ('SELECT [SystemID], [SystemTypeID], [Description] '
               'FROM [dbo].[fst_GEN_System] '
               'ORDER BY [Description]')

        system_info = pd.read_sql_query(sql, con)

        if system_info.empty:
            system_info = None

        return system_info

    @staticmethod
    def get_system_type_id(database,
                           system_name,
                           server=None,
                           uid=None,
                           pwd=None):
        """Query system type id from SQL server.

        Parameters
        ----------
        database : str, list, or tuple
        system_name : str
        parameter_number : int
        server : str, default `Odbc.SERVER`
        uid : str, default `Odbc.UID`
        pwd : str, default `Odbc.PWD`

        Returns
        -------
        int

        Notes
        -----
        If `database` does not exist, generate Error.
        If `system_name` does not exist, return None.
        """
        system_info = Odbc.get_system_info(database=database,
                                           server=server,
                                           uid=uid,
                                           pwd=pwd)

        system_type_id = system_info.loc[system_info.loc[:, 'Description']
                                         == system_name, 'SystemTypeID']

        if len(system_type_id) == 0:
            return None

        if system_type_id.shape[0] > 1:
            warnings.warn((str(system_name) +
                           ' has multiple system type ids! ' +
                           str(max(system_type_id.values)) +
                           ' is chosen!'), UserWarning)
            system_type_id = max(system_type_id.values)
            return system_type_id

        return system_type_id.values[0]

    @staticmethod
    def get_parameter_info(database,
                           system_name=None,
                           system_type_id=None,
                           server=None,
                           uid=None,
                           pwd=None):
        """Query parameter info from SQL server.

        Parameters
        ----------
        database : str, list, or tuple
        system_name : str, default None
        system_type_id : int, defult None
        server : str, default `Odbc.SERVER`
        uid : str, default `Odbc.UID`
        pwd : str, default `Odbc.PWD`

        Returns
        -------
        DataFrame

        Notes
        -----
        If `database` does not exist, generate Error.
        If `system_name` does not exist, return None.
        If `system_type_id` does not exist, return None.
        """
        server = Odbc.SERVER if server is None else server
        uid = Odbc.UID if uid is None else uid
        pwd = Odbc.PWD if pwd is None else pwd

        parameter_info = None

        if isinstance(database, str):
            database = [database]

        for i_database in database:
            temp = Odbc._get_parameter_info(
                database=i_database,
                system_name=system_name,
                system_type_id=system_type_id,
                server=server,
                uid=uid,
                pwd=pwd)
            if (temp is not None) & (parameter_info is None):
                parameter_info = temp.copy()
            elif (temp is not None) & (parameter_info is not None):
                parameter_info = pd.concat([parameter_info, temp],
                                           axis=0,
                                           ignore_index=True)

        if parameter_info is not None:
            parameter_info = parameter_info.drop_duplicates(ignore_index=True)
            parameter_info = parameter_info.reset_index(drop=True)

        return parameter_info

    @staticmethod
    def _get_parameter_info(database,
                            system_name=None,
                            system_type_id=None,
                            server=None,
                            uid=None,
                            pwd=None):
        server = Odbc.SERVER if server is None else server
        uid = Odbc.UID if uid is None else uid
        pwd = Odbc.PWD if pwd is None else pwd

        con = pyodbc.connect('Driver={SQL Server};'
                             'Server=' + server + ';'
                             'Database=' + database + ';'
                             'Uid=' + uid + ';'
                             'Pwd=' + pwd + ';')

        if system_type_id is None:

            sql_1 = ('SELECT [SystemID], [SystemTypeID], [Description] '
                     'FROM [dbo].[fst_GEN_System] '
                     'ORDER BY [Description]')
            system_info = pd.read_sql_query(sql_1, con)

            if system_name is None:
                system_type_id = system_info.loc[0, 'SystemTypeID']
                print(('Neither system_name nor system_type id is '
                       'specified. system_type_id ' +
                       str(system_type_id) + ' is chosen.'))
            else:
                system_type_id = system_info.loc[
                    system_info.loc[:, 'Description'] == system_name,
                    'SystemTypeID'
                ]
                if len(system_type_id) == 0:
                    return None
                if system_type_id.shape[0] > 1:
                    warnings.warn((str(system_name) +
                                   ' has multiple system type ids! ' +
                                   str(max(system_type_id.values)) +
                                   ' is chosen!'), UserWarning)
                system_type_id = max(system_type_id.values)

        sql_2 = (
            'SELECT DISTINCT param.[ParameterNumber], '
            '    [zzDescription], [SIUnitID] '
            'FROM '
            '    fst_GEN_Parameter param INNER JOIN '
            '    fst_GEN_ParameterType paramT '
            '       ON param.[SystemTypeID] = paramT.SystemTypeID '
            '       AND param.[ParameterNumber] = paramT.[ParameterNumber] '
            'WHERE '
            '    paramT.[SystemTypeId] = ' + str(system_type_id) +
            'ORDER BY '
            '    param.[ParameterNumber] ASC')

        parameter_info = pd.read_sql_query(sql_2, con)

        if parameter_info.empty:
            return None

        return parameter_info

    @staticmethod
    def get_parameter_name(database,
                           parameter_number,
                           system_name=None,
                           system_type_id=None,
                           server=None,
                           uid=None,
                           pwd=None):
        """Query parameter name from SQL server.

        Parameters
        ----------
        database : str, list, or tuple
        parameter_number : int
        system_name : str, default None
        system_type_id : int, defult None
        server : str, default `Odbc.SERVER`
        uid : str, default `Odbc.UID`
        pwd : str, default `Odbc.PWD`

        Returns
        -------
        str

        Notes
        -----
        If `database` does not exist, generate Error.
        If `system_name` does not exist, generate Warning and return None.
        If `parameter_name` does not exist, generate Warning and return None.
        If `system_type_id` does not exist, generate Warning and return None.
        """
        server = Odbc.SERVER if server is None else server
        uid = Odbc.UID if uid is None else uid
        pwd = Odbc.PWD if pwd is None else pwd

        parameter_info = Odbc.get_parameter_info(
            database=database,
            system_name=system_name,
            system_type_id=system_type_id,
            server=server,
            uid=uid,
            pwd=pwd)

        if parameter_info is None:
            if system_type_id is not None:
                warnings.warn(('System type id ' + str(system_type_id) +
                               ' does not exist!'), UserWarning)
            elif system_name is not None:
                warnings.warn(('System name ' + str(system_name) +
                               ' does not exist!'), UserWarning)
            return None

        parameter_name = parameter_info.loc[
            parameter_info.loc[:, 'ParameterNumber'] ==
            parameter_number]['zzDescription']

        try:
            parameter_name = parameter_name.values[0]
        except IndexError:
            warnings.warn(('Parameter number ' + str(parameter_number) +
                           ' does not exist!'), UserWarning)
            return None
        else:
            return parameter_name

    @staticmethod
    def get_parameter_unit_id(database,
                              parameter_number,
                              system_name=None,
                              system_type_id=None,
                              server=None,
                              uid=None,
                              pwd=None):
        """Query parameter unit id from SQL server.

        Parameters
        ----------
        database : str, list, or tuple
        parameter_number : int
        system_name : str, default None
        system_type_id : int, defult None
        server : str, default `Odbc.SERVER`
        uid : str, default `Odbc.UID`
        pwd : str, default `Odbc.PWD`

        Returns
        -------
        int

        Notes
        -----
        If `database` does not exist, generate Error.
        If `system_name` does not exist, generate Warning and return None.
        If `parameter_name` does not exist, generate Warning and return None.
        If `system_type_id` does not exist, generate Warning and return None.
        """
        server = Odbc.SERVER if server is None else server
        uid = Odbc.UID if uid is None else uid
        pwd = Odbc.PWD if pwd is None else pwd

        parameter_info = Odbc.get_parameter_info(
            database=database,
            system_name=system_name,
            system_type_id=system_type_id,
            server=server,
            uid=uid,
            pwd=pwd)

        if parameter_info is None:
            if system_type_id is not None:
                warnings.warn(('System type id ' + str(system_type_id) +
                               ' does not exist!'), UserWarning)
            elif system_name is not None:
                warnings.warn(('System name ' + str(system_name) +
                               ' does not exist!'), UserWarning)
            return None

        parameter_unit_id = parameter_info.loc[
            parameter_info.loc[:, 'ParameterNumber'] ==
            parameter_number]['SIUnitID']

        try:
            parameter_unit_id = parameter_unit_id.values[0]
        except IndexError:
            warnings.warn(('Parameter number ' + str(parameter_number) +
                           ' does not exist!'), UserWarning)
            return None
        else:
            return parameter_unit_id
