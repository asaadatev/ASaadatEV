"""
Query equipment data and meta info from SQL server.
"""

# Authors: Danny Sun <duo.sun@edwardsvacuum.com>,
#          Dennis Hou <duanyang.hou@edwardsvacuum.com>

import warnings
import datetime
import json

import sqlalchemy
import pandas as pd
import numpy as np


class Odbc:
    """Query equipment data and meta info from SQL server."""

    def __init__(self,
                 server: str,
                 uid: str,
                 pwd: str):

        self.server = server
        self.uid = uid
        self.pwd = pwd

    @classmethod
    def load_login(cls, file: str = 'login.json'):
        """Load login info from a file."""
        f = open(file)
        login = json.load(f)
        f.close()
        return cls(server=login.get('server'),
                   uid=login.get('uid'),
                   pwd=login.get('pwd'))

    def save_login(self, file: str = 'login.json'):
        """Save login info into a file."""
        login = {'server': self.server,
                 'uid': self.uid,
                 'pwd': self.pwd}
        with open(file, 'w') as f:
            json.dump(login, f, indent=4, ensure_ascii=False)

    def create(self,
               database: str | list | tuple,
               system_name: str | list | tuple,
               parameter_number: int | list | tuple,
               start_datetime: datetime.datetime | datetime.date
               = datetime.date(1970, 1, 1),
               end_datetime: datetime.datetime | datetime.date
               = datetime.date.today(),
               col_name: str = 'parameter_name',
               pivot_table: bool = True):
        """
        Create a dictionary object that stores data and meta info, or a dict
        of dictionary objects.

        Parameters
        ----------
        database : str, list, or tuple
            Database name/names.
        system_name : str, list, or tuple
            System name.
            System name/names.
        parameter_number : int, list, or tuple
            Parameter number/numbers.
        start_datetime : datetime.datetime or datetime.date, optional
            The default is datetime.date(1970, 1, 1).
        end_datetime : datetime.datetime or datetime.date, optional
            The default is datetime.date.today().
        pivot_table : bool, optional
            Default True.
            If `False`, the value the of key `data` in the returned dictionary
            will be a pd.DataFrame with columns `Description`,
            `zzDescription`, `LogTime`, `Value`. See Odbc.get_data() Returns.
            If `True`, perform pandas.pivot_table() and the value of key `data`
            in the returned dictionary will be a pd.DataFrame with
            `datetime` as index name and parameter names or parameter
            numbers as column names.
        col_name : {'parameter_name', 'parameter_number'}, optional
            Default uses 'parameter_name'. Only when `pivot_table` is True and
            `parameter_number` has more than one numbers, `col_name` chooses
            the way to name returned pd.DataFrame `data` columns.
            Otherwise, ignored.

        Returns
        -------
        if system_name is str, return a dict with following key-value pairs
            {'database': str, list, or tuple,
            'system_name': str,
            'parameter_number': int, list, or tuple,
            'start_datetime': datetime.datetime or datetime.date,
            'end_datetime': datetime.datetime or datetime.date,
            'data': pd.Series or pd.DataFrame,
            'system_type_id': int or dict,
            'parameter_name': str or dict,
            'parameter_unit_id': int or dict}
        if system_name is list or tuple, return a dict of key-value pairs.
            For each pair, key is a name in the list or tuple and value is
            corresponding dict as described above.
        """

        if isinstance(system_name, str):
            result = self._create(database=database,
                                  system_name=system_name,
                                  parameter_number=parameter_number,
                                  start_datetime=start_datetime,
                                  end_datetime=end_datetime,
                                  col_name=col_name,
                                  pivot_table=pivot_table)
        elif isinstance(system_name, (list, tuple)):
            result = {}
            for sn in system_name:
                result[sn] = self._create(database=database,
                                          system_name=sn,
                                          parameter_number=parameter_number,
                                          start_datetime=start_datetime,
                                          end_datetime=end_datetime,
                                          col_name=col_name,
                                          pivot_table=pivot_table)
        else:
            raise TypeError('Invalid type')

        return result

    def _create(self,
                database: str | list | tuple,
                system_name: str,
                parameter_number: int | list | tuple,
                start_datetime: datetime.datetime | datetime.date
                = datetime.date(1970, 1, 1),
                end_datetime: datetime.datetime | datetime.date
                = datetime.date.today(),
                col_name: str = 'parameter_name',
                pivot_table: bool = True):
        """Create a dictionary object that stores data and meta info.

        Parameters
        ----------
        database : str, list, or tuple
            Database name/names.
        system_name : str
            System name.
        parameter_number : int, list, or tuple
            Parameter number/numbers.
        start_datetime : datetime.datetime or datetime.date, optional
            The default is datetime.date(1970, 1, 1).
        end_datetime : datetime.datetime or datetime.date, optional
            The default is datetime.date.today().
        pivot_table : bool, optional
            Default True.
            If `False`, the value the of key `data` in the returned dictionary
            will be a pd.DataFrame with columns `Description`,
            `zzDescription`, `LogTime`, `Value`. See Odbc.get_data() Returns.
            If `True`, perform pandas.pivot_table() and the value of key `data`
            in the returned dictionary will be a pd.DataFrame with
            `datetime` as index name and parameter names or parameter
            numbers as column names.
        col_name : {'parameter_name', 'parameter_number'}, optional
            Default uses 'parameter_name'. Only when `pivot_table` is True and
            `parameter_number` has more than one numbers, `col_name` chooses
            the way to name returned pd.DataFrame `data` columns.
            Otherwise, ignored.

        Returns
        -------
        dict, with key-value pairs {
            'database': str, list, or tuple,
            'system_name': str,
            'parameter_number': int, list, or tuple,
            'start_datetime': datetime.datetime or datetime.date,
            'end_datetime': datetime.datetime or datetime.date,
            'data': pd.Series or pd.DataFrame,
            'system_type_id': int or dict,
            'parameter_name': str or dict,
            'parameter_unit_id': int or dict}
        """

        # Single system only
        if not isinstance(system_name, str):
            raise TypeError('Invalid type')

        # Whether single parameter or multiple parameters
        is_multiple = False
        if not isinstance(parameter_number, int):
            if len(parameter_number) == 1:
                parameter_number = parameter_number[0]
            else:
                is_multiple = True

        data = self.get_data(database=database,
                             system_name=system_name,
                             parameter_number=parameter_number,
                             start_datetime=start_datetime,
                             end_datetime=end_datetime)

        system_type_id = self.get_system_type_id(
            database=database,
            system_name=system_name)

        if is_multiple:
            parameter_name = {}
            for i in parameter_number:
                parameter_name[i] = self.get_parameter_name(
                    database=database,
                    parameter_number=i,
                    system_name=system_name)

            parameter_unit_id = {}
            for i in parameter_number:
                parameter_unit_id[i] = self.get_parameter_unit_id(
                    database=database,
                    parameter_number=i,
                    system_name=system_name)

            # pd.DataFrame with `datetime` as index name
            if (data is not None) and pivot_table:
                del data['Description']  # As only one system
                data.rename(columns={'LogTime': 'datetime',
                                     'zzDescription': 'parameter'},
                            inplace=True)
                data = data.pivot_table(index='datetime',
                                        columns='parameter',
                                        values='Value',
                                        aggfunc=np.mean)
                data.sort_index(inplace=True)

                # Choose parameter names or numbers as the column names
                if col_name == 'parameter_name':
                    # Sort columns
                    col = [x for x in list(parameter_name.values())
                           if x in list(data.columns.values)]
                    data = data[col]
                elif col_name == 'parameter_number':
                    name_to_number = dict(
                        (v, str(k)) for k, v in parameter_name.items())
                    data.rename(columns=name_to_number, inplace=True)
                    # Sort columns
                    col = [str(x) for x in parameter_number
                           if str(x) in list(data.columns.values)]
                    data = data[col]
                else:
                    raise TypeError('Invalid value')
        else:
            parameter_name = self.get_parameter_name(
                database=database,
                parameter_number=parameter_number,
                system_name=system_name)

            parameter_unit_id = self.get_parameter_unit_id(
                database=database,
                parameter_number=parameter_number,
                system_name=system_name)

            # pd.Series, `datetime` as index name and parameter_name as name
            if (data is not None) and pivot_table:
                del data['Description']  # As only one system
                del data['zzDescription']  # As only one parameter
                if parameter_name is None:
                    parameter_name = 'value'
                data.rename(columns={'LogTime': 'datetime',
                                     'Value': parameter_name},
                            inplace=True)
                data = data.set_index('datetime')[parameter_name]

        return {'database': database,
                'system_name': system_name,
                'parameter_number': parameter_number,
                'start_datetime': start_datetime,
                'end_datetime': end_datetime,
                'data': data,
                'system_type_id': system_type_id,
                'parameter_name': parameter_name,
                'parameter_unit_id': parameter_unit_id}

    def get_data(self,
                 database: str | list | tuple,
                 system_name: str | list | tuple,
                 parameter_number: int | list | tuple,
                 start_datetime: datetime.datetime | datetime.date
                 = datetime.date(1970, 1, 1),
                 end_datetime: datetime.datetime | datetime.date
                 = datetime.date.today()) -> pd.DataFrame | None:
        """Get data.

        Parameters
        ----------
        database : str, list, or tuple
        system_name : str, list, or tuple
        parameter_number : int, list, or tuple
        start_datetime : datetime.datetime or datetime.date, optional
            The default is datetime.date(1970, 1, 1).
        end_datetime : datetime.datetime or datetime.date, optional
            The default is datetime.date.today().

        Returns
        -------
        None or `pd.DataFrame` with following columns :
            `Description`, `zzDescription`, `LogTime`, `Value`

        Notes
        -----
        If any of `database` does not exist, generate Error.
        If no data, `system_name` or `parameter_number` does not exist,
        return None.
        If multiple databases, return pd.DataFrame with combined, sorted and
        distinct rows.
        """

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
                    temp = self._get_data(database=k_db,
                                          system_name=i_sn,
                                          parameter_number=j_pn,
                                          start_datetime=start_datetime,
                                          end_datetime=end_datetime)
                    if (temp is not None) & (k_data is None):
                        k_data = temp.copy()
                    elif (temp is not None) & (k_data is not None):
                        k_data = pd.concat([k_data, temp],
                                           axis=0,
                                           ignore_index=True)

                if k_data is not None:
                    parameter_name = self.get_parameter_name(
                        database=database,
                        parameter_number=j_pn,
                        system_name=i_sn)
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

    def _get_data(self,
                  database: str,
                  system_name: str | list | tuple,
                  parameter_number: int | list | tuple,
                  start_datetime: datetime.datetime | datetime.date
                  = datetime.date(1970, 1, 1),
                  end_datetime: datetime.datetime | datetime.date
                  = datetime.date.today()) -> pd.DataFrame | None:

        con = sqlalchemy.create_engine('mssql+pyodbc://'
                                       + self.uid + ':' + self.pwd
                                       + '@' + self.server + '/' + database
                                       + '?driver=SQL+Server')

        if isinstance(system_name, str):
            system_name = [system_name]

        if isinstance(parameter_number, int):
            parameter_number = [parameter_number]

        sql = ('SELECT t3.[Description], t4.[zzDescription], '
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

    def get_system_info(self, database: str | list | tuple) -> \
            pd.DataFrame | None:
        """Get system info.

        Parameters
        ----------
        database : str, list, or tuple

        Returns
        -------
        pd.DataFrame with columns : 'SystemID', 'SystemTypeID', 'Description'.
            
        Notes
        -----
        If any of `database` does not exist, generate Error.
        """

        system_info = None

        if isinstance(database, str):
            database = [database]

        for i_database in database:
            temp = self._get_system_info(database=i_database)
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

    def _get_system_info(self, database: str) -> pd.DataFrame | None:

        con = sqlalchemy.create_engine('mssql+pyodbc://'
                                       + self.uid + ':' + self.pwd
                                       + '@' + self.server + '/' + database
                                       + '?driver=SQL+Server')

        sql = ('SELECT [SystemID], [SystemTypeID], [Description] '
               'FROM [dbo].[fst_GEN_System] '
               'ORDER BY [Description]')

        system_info = pd.read_sql_query(sql, con)

        if system_info.empty:
            system_info = None

        return system_info

    def get_system_type_id(self,
                           database: str | list | tuple,
                           system_name: str) -> int | None:
        """Get system type id.

        Parameters
        ----------
        database : str, list, or tuple
        system_name : str

        Returns
        -------
        int or None

        Notes
        -----
        If any of `database` does not exist, generate Error.
        If system does not exist, return None.
        """
        system_info = self.get_system_info(database=database)

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

    def get_parameter_info(self,
                           database: str | list | tuple,
                           system_name: str = None,
                           system_type_id: int = None) -> pd.DataFrame | None:
        """Get parameter info.

        Parameters
        ----------
        database : str, list, or tuple
        system_name : str, default None
        system_type_id : int, default None

        Returns
        -------
        pd.DataFrame with columns: 'ParameterNumber', 'zzDescription',
        'SIUnitID'.

        Notes
        -----
        If any of `database` does not exist, generate Error.
        If `system_name` does not exist, return None.
        If `system_type_id` does not exist, return None.
        """

        parameter_info = None

        if isinstance(database, str):
            database = [database]

        for i_database in database:
            temp = self._get_parameter_info(
                database=i_database,
                system_name=system_name,
                system_type_id=system_type_id)
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

    def _get_parameter_info(self,
                            database: str,
                            system_name: str = None,
                            system_type_id: int = None) -> pd.DataFrame | None:

        con = sqlalchemy.create_engine('mssql+pyodbc://'
                                       + self.uid + ':' + self.pwd
                                       + '@' + self.server + '/' + database
                                       + '?driver=SQL+Server')

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
                '  [zzDescription], [SIUnitID] '
                'FROM '
                '  fst_GEN_Parameter param INNER JOIN '
                '  fst_GEN_ParameterType paramT '
                '    ON param.[SystemTypeID] = paramT.SystemTypeID '
                '    AND param.[ParameterNumber] = paramT.[ParameterNumber] '
                'WHERE '
                '  paramT.[SystemTypeId] = ' + str(system_type_id) +
                'ORDER BY '
                '  param.[ParameterNumber] ASC')

        parameter_info = pd.read_sql_query(sql_2, con)

        if parameter_info.empty:
            return None

        return parameter_info

    def get_parameter_name(self,
                           database: str | list | tuple,
                           parameter_number: int,
                           system_name: str = None,
                           system_type_id: int = None) -> str | None:
        """Get parameter name.

        Parameters
        ----------
        database : str, list, or tuple
        parameter_number : int
        system_name : str, default None
        system_type_id : int, default None

        Returns
        -------
        str or None

        Notes
        -----
        If any of `database` does not exist, generate Error.
        If `system_name` does not exist, generate Warning and return None.
        If `parameter_name` does not exist, generate Warning and return None.
        If `system_type_id` does not exist, generate Warning and return None.
        """

        parameter_info = self.get_parameter_info(
            database=database,
            system_name=system_name,
            system_type_id=system_type_id)

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

    def get_parameter_unit_id(self,
                              database: str | list | tuple,
                              parameter_number: int,
                              system_name: str = None,
                              system_type_id: int = None) -> int | None:
        """Get parameter unit id.

        Parameters
        ----------
        database : str, list, or tuple
        parameter_number : int
        system_name : str, default None
        system_type_id : int, default None

        Returns
        -------
        int or None

        Notes
        -----
        If any of `database` does not exist, generate Error.
        If `system_name` does not exist, generate Warning and return None.
        If `parameter_name` does not exist, generate Warning and return None.
        If `system_type_id` does not exist, generate Warning and return None.
        """

        parameter_info = self.get_parameter_info(
            database=database,
            system_name=system_name,
            system_type_id=system_type_id)

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

    def _get_status(self,
                    database: str,
                    system_name: str | list | tuple,
                    system_type_id: int,
                    start_datetime: datetime.datetime | datetime.date
                    = datetime.date(1970, 1, 1),
                    end_datetime: datetime.datetime | datetime.date
                    = datetime.date.today()) -> pd.DataFrame | None:

        if not isinstance(database, str):
            raise TypeError('Invalid type')

        con = sqlalchemy.create_engine('mssql+pyodbc://'
                                       + self.uid + ':' + self.pwd
                                       + '@' + self.server + '/' + database
                                       + '?driver=SQL+Server')

        if isinstance(system_name, str):
            system_name = [system_name]

        sql = ('SELECT t3.[Description], t1.[logTime], t2.[primary_message]'
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

        status = pd.read_sql_query(sql, con)

        if status.empty:
            status = None

        return status
