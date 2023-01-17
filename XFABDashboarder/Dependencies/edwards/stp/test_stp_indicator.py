from stp import stp_indicator, extract_data_from_db
from os import listdir, path
import pandas as pd
import datetime
from datetime import timedelta
import pytest


@pytest.fixture(scope='module')
def data_a(dw_access=False):
    # Test using data extract from data warehouse
    # Should be done this way however the connection through 10. network via vpn is bad based on experience.
    if dw_access:
        database = 'scada_Production_Infineon_Dresden'
        system_type_id = 112
        system_name = 'ETC021-11-PC3-STP-2'
        data_a = extract_data_from_db(database, system_type_id, system_name,
                                      datetime.date(2021, 3, 1), datetime.date(2021, 6, 1))
    # Test using local data file
    else:
        data_folder = r'C:\Users\houdu\OneDrive - Atlas Copco Vacuum Technique\Documents\0_Data\STP\Infineon'
        file_names = listdir(data_folder)
        file_name = 'ETC021-11-PC3-STP-2.parquet'
        file_path = path.join(data_folder, file_name)
        data_a = pd.read_parquet(file_path)
    return data_a


@pytest.fixture(scope='module')
def data_b(dw_access=False):
    # Test using data extract from data warehouse
    # Should be done this way however the connection through 10. network is bad based on experience.
    if dw_access:
        database = 'scada_Production_Infineon_Dresden'
        system_type_id = 112
        system_name = 'ETC021-11-PC3-STP-2'
        data_b = extract_data_from_db(database, system_type_id, system_name,
                                    datetime.date(2021, 1, 1), datetime.date(2021, 6, 1))
    # Test using local data file
    else:
        data_folder = r'C:\Users\houdu\OneDrive - Atlas Copco Vacuum Technique\Documents\0_Data\STP\Infineon'
        file_names = listdir(data_folder)
        file_name = 'ETCP70-02-B-STP-3.parquet'
        file_path = path.join(data_folder, file_name)
        data_b = pd.read_parquet(file_path)
    return data_b


def test_pump_swap(data_a):
    # test if pump swap data slice or not
    stp_0_S = stp_indicator.StpIndicators(data_a, after_swap=True)
    assert stp_0_S.data.shape[0] < data_a.shape[0]
    assert stp_0_S.pump_swap_events[-1][-1] - pd.to_datetime('2021-05-14') < timedelta(days=1)


def test_column_added(data_a):
    if data_a['Equipment Status'] is not None:
        stp_0_S = stp_indicator.StpIndicators(data_a, after_swap=True)
        assert stp_0_S.data.shape[1] - data_a.shape[1] == 1


def test_row_number(data_a):
    stp_0 = stp_indicator.StpIndicators(data_a, after_swap=False)
    assert stp_0.data.shape[0] == data_a.shape[0]


def test_acc_period(data_a):
    stp_0_S = stp_indicator.StpIndicators(data_a, after_swap=True)
    stp_0 = stp_indicator.StpIndicators(data_a, after_swap=False)
    acc_period = stp_0_S.acc_period()
    assert all([timedelta(seconds=i) < timedelta(minutes=10) for i in acc_period.values()])

    acc_period = stp_0.acc_period()
    assert all([timedelta(seconds=i) < timedelta(minutes=10) for i in acc_period.values()])

    dec_period = stp_0.dec_period()
    assert all([timedelta(seconds=i) < timedelta(minutes=10) for i in dec_period.values()])

    acc_period = stp_0_S.acc_period(speed_upper_t=2)
    assert acc_period is None


def test_motor_current_trend(data_a):
    stp_0_S = stp_indicator.StpIndicators(data_a, after_swap=True)
    current_trend_ma = stp_0_S.motor_current_trend()
    current_trend_l1 = stp_0_S.motor_current_trend(method='l1')
    current_trend_l2 = stp_0_S.motor_current_trend(method='l2')
    current_trend_l3 = stp_0_S.motor_current_trend(method='l3')
    assert current_trend_ma is not None
    assert current_trend_l1 is not None
    assert current_trend_l2 is not None
    assert current_trend_l3 is None


def test_tms_period(data_b):
    stp_1 = stp_indicator.StpIndicators(data_b)
    tms_period = stp_1.tms_period()
    assert all([1000 < i < 3000 for i in tms_period.values()])


def test_rotor_contact(data_b):
    stp_1 = stp_indicator.StpIndicators(data_b)
    large_contact, small_contact = stp_1.rotor_contact(current_pk_t=4, vb_pk_t=4)
    # 2 empty contact: number = 0
    assert len(large_contact) + len(small_contact) == 2

    large_contact, small_contact = stp_1.rotor_contact(current_pk_t=4, vb_pk_t=3.5)
    assert len(small_contact) > 2
    assert len(large_contact) == 1

    large_contact, small_contact = stp_1.rotor_contact(current_pk_t=2, vb_pk_t=3.5)
    assert len(small_contact) > 2
    assert len(large_contact) > 2


def test_vib_anomaly(data_b):
    stp_1 = stp_indicator.StpIndicators(data_b)
    vib_anomaly = stp_1.vib_anomaly_score()
    assert len(vib_anomaly) != 0
    assert all([0 < i < 1 for i in vib_anomaly.values()])


def test_start_anomaly(data_b):
    stp_1 = stp_indicator.StpIndicators(data_b)
    start_anomaly = stp_1.start_anomaly_score()
    assert len(start_anomaly) != 0
    assert all([0 < i < 1 for i in start_anomaly.values()])


def test_rotor_shaft_displacement(data_b):
    stp_1 = stp_indicator.StpIndicators(data_b)
    data = stp_1.rotor_shaft_displacement()
    dXYh_abs = data['dXYh_abs'].dropna()
    dXYb_abs = data['dXYb_abs'].dropna()
    assert len(dXYh_abs) != 0
    assert len(dXYb_abs) != 0
