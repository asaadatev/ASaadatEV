# -*- coding: utf-8 -*-
"""
Created on Fri Nov 27 14:15:12 2020

@author: Danny Sun
"""

import datetime

import pyodbc
import pytest

from edwards.data import Odbc


@pytest.mark.skipif(False, reason="Skip when unable to access SQL server")
class TestOdBC(object):

    def test_get_data(self):
        data = Odbc.get_data(database='scada_Production_UMC5',
                             system_name='SIN-B03',
                             parameter_number=4,
                             start_datetime=datetime.date(2019, 1, 14),
                             end_datetime=datetime.date(2019, 1, 15))
        assert len(data.index) > 0

        data = Odbc.get_data(database='scada_Production_UMC5',
                             system_name=('SIN-B03', 'SIN-B04'),
                             parameter_number=(4, 8),
                             start_datetime=datetime.date(2019, 1, 14),
                             end_datetime=datetime.date(2019, 1, 15))
        assert len(data.index) > 0

        data = Odbc.get_data(database='scada_Production_UMC5',
                             system_name=['SIN-B03', 'SIN-B04'],
                             parameter_number=[4, 8],
                             start_datetime=datetime.date(2019, 1, 14),
                             end_datetime=datetime.date(2019, 1, 15))
        assert len(data.index) > 0

    def test_get_data_multiple_databases(self):
        database = ('fsdb_Production_P3_2', 'fsdb_Production_P4_2',
                    'fsdb_Production_UMCP3', 'fsdb_Production_UMCP4')
        system_name = 'SIN-A39-CH'
        parameter_number = 4
        start_datetime = datetime.date(2020, 4, 15)
        end_datetime = datetime.date(2020, 8, 2)
        data = Odbc.get_data(database=database,
                             system_name=system_name,
                             parameter_number=parameter_number,
                             start_datetime=start_datetime,
                             end_datetime=end_datetime)
        data0 = Odbc.get_data(database=database[0],
                              system_name=system_name,
                              parameter_number=parameter_number,
                              start_datetime=start_datetime,
                              end_datetime=end_datetime)
        data1 = Odbc.get_data(database=database[1],
                              system_name=system_name,
                              parameter_number=parameter_number,
                              start_datetime=start_datetime,
                              end_datetime=end_datetime)
        data2 = Odbc.get_data(database=database[2],
                              system_name=system_name,
                              parameter_number=parameter_number,
                              start_datetime=start_datetime,
                              end_datetime=end_datetime)
        data3 = Odbc.get_data(database=database[3],
                              system_name=system_name,
                              parameter_number=parameter_number,
                              start_datetime=start_datetime,
                              end_datetime=end_datetime)
        assert len(data.index) > 0
        assert data0 is None
        assert len(data1.index) > 0
        assert data2 is None
        assert len(data3.index) > 0
        assert len(data.index) == (len(data1.index) + len(data3.index))

    def test_get_data_no_data(self):
        """If no data for specified duration, return empty data frame."""
        data = Odbc.get_data(database='scada_Production_UMC5',
                             system_name=['SIN-B03', 'SIN-B04'],
                             parameter_number=[4, 8],
                             start_datetime=datetime.date(1970, 1, 14),
                             end_datetime=datetime.date(1970, 1, 15))
        assert data is None

    def test_get_data_non_existing_system(self):
        """If system does not exist, return empty data frame."""
        data = Odbc.get_data(database='scada_Production_UMC5',
                             system_name='dummy_system',
                             parameter_number=4,
                             start_datetime=datetime.date(2019, 1, 14),
                             end_datetime=datetime.date(2019, 1, 15))
        assert data is None

    def test_get_data_error_non_existing_database(self):
        """If database does not exist, raise pyodbc.ProgrammingError."""
        with pytest.raises(pyodbc.ProgrammingError):
            Odbc.get_data(database='dummy_database',
                          system_name=['SIN-B03', 'SIN-B04'],
                          parameter_number=[4, 8],
                          start_datetime=datetime.date(2019, 1, 14),
                          end_datetime=datetime.date(2019, 1, 15))

    def test_get_system_info(self):
        system_info = Odbc.get_system_info('scada_Production_UMC5')
        assert len(system_info.index) > 0

    def test_get_system_info_multiple_databases(self):
        database = ('fsdb_Production_P3_2', 'fsdb_Production_P4_2',
                    'fsdb_Production_UMCP3', 'fsdb_Production_UMCP4')
        system_info = Odbc.get_system_info(database)
        assert len(system_info.index) > 0

    def test_get_system_type_id(self):
        system_type_id = Odbc.get_system_type_id('scada_Production_UMC5',
                                                 system_name='SIN-B03')
        assert system_type_id == 25

    def test_get_system_type_id_none_non_existing_system(self):
        """If system does not exist, return None."""
        system_type_id = Odbc.get_system_type_id('scada_Production_UMC5',
                                                 system_name='Dummy')
        assert system_type_id is None

    def test_get_parameter_info(self):
        parameter_info = Odbc.get_parameter_info('scada_Production_UMC5')
        assert len(parameter_info) > 0

        parameter_info = Odbc.get_parameter_info('scada_Production_UMC5',
                                                 system_name='SIN-B03')
        assert len(parameter_info) > 0

        parameter_info = Odbc.get_parameter_info('scada_Production_UMC5',
                                                 system_type_id=25)
        assert len(parameter_info) > 0

    def test_get_parameter_info_non_existing_system(self):
        """If system does not exist, return None."""
        parameter_info = Odbc.get_parameter_info('scada_Production_UMC5',
                                                 system_name='Dummy')
        assert parameter_info is None

    def test_get_parameter_info_non_existing_system_type_id(self):
        """If system type id does not exist, return None."""
        dummy_id = 100000
        parameter_info = Odbc.get_parameter_info('scada_Production_UMC5',
                                                 system_type_id=dummy_id)
        assert parameter_info is None

    def test_get_parameter_info_multiple_system_type_ids(self):
        """If multiple system type ids, raise warning."""
        with pytest.warns(UserWarning):
            Odbc.get_parameter_info(
                database='scada_Production_AMAT', system_name='Nova-141A')

    def test_get_parameter_name(self):
        parameter_name = Odbc.get_parameter_name('scada_Production_UMC5',
                                                 parameter_number=3)
        assert parameter_name == 'Dry Pump Current'

        parameter_name = Odbc.get_parameter_name('scada_Production_UMC5',
                                                 parameter_number=3,
                                                 system_name='SIN-B03')
        assert parameter_name == 'Dry Pump Current'

        parameter_name = Odbc.get_parameter_name('scada_Production_UMC5',
                                                 parameter_number=3,
                                                 system_type_id=25)
        assert parameter_name == 'Dry Pump Current'

    def test_get_parameter_name_non_existing_parameter_number(self):
        """If parameter number does not exist, return None."""
        with pytest.warns(UserWarning):
            dummy_number = 100000
            parameter_name = Odbc.get_parameter_name(
                database='scada_Production_UMC5',
                parameter_number=dummy_number)
        assert parameter_name is None

    def test_get_parameter_name_non_existing_system_type_id(self):
        """If parameter number does not exist, return None."""
        with pytest.warns(UserWarning):
            dummy_id = 100000
            parameter_name = Odbc.get_parameter_name(
                'scada_Production_UMC5',
                parameter_number=3,
                system_type_id=dummy_id)
        assert parameter_name is None

    def test_get_parameter_name_non_existing_system(self):
        """If system does not exist, return None."""
        with pytest.warns(UserWarning):
            parameter_name = Odbc.get_parameter_name(
                'scada_Production_UMC5',
                parameter_number=3,
                system_name='dummy_system')
        assert parameter_name is None

    def test_get_parameter_unit_id(self):
        parameter_unit_id = Odbc.get_parameter_unit_id(
            'scada_Production_UMC5',
            parameter_number=3)
        assert parameter_unit_id == 11

        parameter_unit_id = Odbc.get_parameter_unit_id(
            'scada_Production_UMC5',
            parameter_number=3,
            system_name='SIN-B03')
        assert parameter_unit_id == 11

        parameter_unit_id = Odbc.get_parameter_unit_id(
            'scada_Production_UMC5',
            parameter_number=3,
            system_type_id=25)
        assert parameter_unit_id == 11

    def test_get_parameter_unit_id_non_existing_parameter_number(self):
        """If parameter number does not exist, return None."""
        with pytest.warns(UserWarning):
            parameter_unit_id = Odbc.get_parameter_unit_id(
                database='scada_Production_UMC5',
                parameter_number=100000)
        assert parameter_unit_id is None

    def test_get_parameter_unit_id_non_existing_system_type_id(self):
        """If system type id does not exist, return None."""
        with pytest.warns(UserWarning):
            parameter_unit_id = Odbc.get_parameter_unit_id(
                 database='scada_Production_UMC5',
                 parameter_number=3,
                 system_type_id=100000)
        assert parameter_unit_id is None

    def test_get_parameter_unit_id_error_non_existing_system(self):
        """If system does not exist, return None."""
        with pytest.warns(UserWarning):
            parameter_unit_id = Odbc.get_parameter_unit_id(
                'scada_Production_UMC5',
                parameter_number=3,
                system_name='dummy_system')
        assert parameter_unit_id is None
