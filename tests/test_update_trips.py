"""Tests for update_trips and load_parquet robustness."""
import pandas as pd
import pytest
from unittest.mock import MagicMock, patch

from bikeraccoon.tracker.tracker_functions import (
    GBFSSystem,
    load_parquet,
    update_trips,
)


def _make_system():
    s = GBFSSystem({'name': 'test_city', 'tz': 'America/Toronto'})
    s.logger = MagicMock()
    s.data_path = '/tmp/fake'
    return s


def _minimal_trips_df(year):
    return pd.DataFrame({
        'datetime': [pd.Timestamp(f'{year}-03-01 10:00:00')],
        'station_id': ['s1'],
        'vehicle_type_id': ['bike'],
        'trips': [1],
        'returns': [0],
    })


def _multi_month_trips_df(year):
    return pd.DataFrame({
        'datetime': [pd.Timestamp(f'{year}-01-31 23:00:00'), pd.Timestamp(f'{year}-02-01 00:00:00')],
        'station_id': ['s1', 's1'],
        'vehicle_type_id': ['bike', 'bike'],
        'trips': [1, 2],
        'returns': [0, 0],
    })


def _write_month_partition(tmp_path, year, month, trips_value):
    d = tmp_path / 'trips.station.hourly' / f'year={year}' / f'month={month}'
    d.mkdir(parents=True)
    pd.DataFrame({
        'datetime': [pd.Timestamp(f'{year}-{month:02d}-01 00:00:00')],
        'station_id': ['s1'],
        'vehicle_type_id': ['bike'],
        'trips': [trips_value],
        'returns': [0],
    }).to_parquet(d / 'part-0.parquet', index=False)


# ── update_trips: unexpected load failure ────────────────────────────────────

def test_update_trips_aborts_save_on_unexpected_load_error():
    system = _make_system()
    minimal_trips = _minimal_trips_df('2024')
    with patch('bikeraccoon.tracker.tracker_functions.pd.read_parquet', return_value=pd.DataFrame()):
        with patch('bikeraccoon.tracker.tracker_functions.make_station_trips', return_value=minimal_trips):
            with patch('bikeraccoon.tracker.tracker_functions.load_parquet', side_effect=OSError('corrupt')):
                with patch('bikeraccoon.tracker.tracker_functions.save_to_parquet') as mock_save:
                    update_trips(system, 'station')
    mock_save.assert_not_called()


def test_update_trips_warns_on_unexpected_load_error():
    system = _make_system()
    minimal_trips = _minimal_trips_df('2024')
    with patch('bikeraccoon.tracker.tracker_functions.pd.read_parquet', return_value=pd.DataFrame()):
        with patch('bikeraccoon.tracker.tracker_functions.make_station_trips', return_value=minimal_trips):
            with patch('bikeraccoon.tracker.tracker_functions.load_parquet', side_effect=OSError('corrupt')):
                with patch('bikeraccoon.tracker.tracker_functions.save_to_parquet'):
                    update_trips(system, 'station')
    system.logger.warning.assert_called()


def test_update_trips_proceeds_when_no_historical_file():
    system = _make_system()
    minimal_trips = _minimal_trips_df('2024')
    with patch('bikeraccoon.tracker.tracker_functions.pd.read_parquet', return_value=pd.DataFrame()):
        with patch('bikeraccoon.tracker.tracker_functions.make_station_trips', return_value=minimal_trips):
            with patch('bikeraccoon.tracker.tracker_functions.load_parquet', side_effect=FileNotFoundError()):
                with patch('bikeraccoon.tracker.tracker_functions.trim_raw'):
                    with patch('bikeraccoon.tracker.tracker_functions.save_to_parquet') as mock_save:
                        update_trips(system, 'station')
    mock_save.assert_called_once()


# ── load_parquet: non-parquet files in directory ─────────────────────────────

def test_load_parquet_ignores_non_parquet_files(tmp_path):
    year_dir = tmp_path / 'trips.station.hourly' / 'year=2024' / 'month=1'
    year_dir.mkdir(parents=True)
    _minimal_trips_df('2024').to_parquet(year_dir / 'part-0.parquet', index=False)
    (year_dir / 'part-0.parquet.bak').write_bytes(b'not a parquet file')
    system = _make_system()
    system.data_path = str(tmp_path)
    result = load_parquet(system, '2024', 'station')
    assert len(result) > 0


# ── load_parquet: month scoping ───────────────────────────────────────────────

def test_load_parquet_scopes_to_specified_months(tmp_path):
    """months=[1] must not also match the month=10/11/12 partitions."""
    _write_month_partition(tmp_path, 2024, 1, trips_value=1)
    _write_month_partition(tmp_path, 2024, 10, trips_value=10)
    system = _make_system()
    system.data_path = str(tmp_path)

    result = load_parquet(system, '2024', 'station', months=[1])

    assert sorted(result['trips'].tolist()) == [1]


def test_load_parquet_without_months_loads_whole_year(tmp_path):
    """Omitting months preserves the old whole-year read for direct callers."""
    _write_month_partition(tmp_path, 2024, 1, trips_value=1)
    _write_month_partition(tmp_path, 2024, 10, trips_value=10)
    system = _make_system()
    system.data_path = str(tmp_path)

    result = load_parquet(system, '2024', 'station')

    assert sorted(result['trips'].tolist()) == [1, 10]


# ── update_trips: scopes historical load to months present in new data ───────

def test_update_trips_scopes_historical_load_to_present_month():
    system = _make_system()
    minimal_trips = _minimal_trips_df('2024')  # single row in March
    empty_historical = pd.DataFrame(columns=['datetime', 'station_id', 'vehicle_type_id', 'trips', 'returns'])
    with patch('bikeraccoon.tracker.tracker_functions.pd.read_parquet', return_value=pd.DataFrame()):
        with patch('bikeraccoon.tracker.tracker_functions.make_station_trips', return_value=minimal_trips):
            with patch('bikeraccoon.tracker.tracker_functions.load_parquet', return_value=empty_historical) as mock_load:
                with patch('bikeraccoon.tracker.tracker_functions.trim_raw'):
                    with patch('bikeraccoon.tracker.tracker_functions.save_to_parquet'):
                        update_trips(system, 'station')
    mock_load.assert_called_once_with(system, '2024', 'station', months=[3])


def test_update_trips_scopes_historical_load_across_month_boundary():
    system = _make_system()
    multi_month = _multi_month_trips_df('2024')  # spans Jan 31 -> Feb 1
    empty_historical = pd.DataFrame(columns=['datetime', 'station_id', 'vehicle_type_id', 'trips', 'returns'])
    with patch('bikeraccoon.tracker.tracker_functions.pd.read_parquet', return_value=pd.DataFrame()):
        with patch('bikeraccoon.tracker.tracker_functions.make_station_trips', return_value=multi_month):
            with patch('bikeraccoon.tracker.tracker_functions.load_parquet', return_value=empty_historical) as mock_load:
                with patch('bikeraccoon.tracker.tracker_functions.trim_raw'):
                    with patch('bikeraccoon.tracker.tracker_functions.save_to_parquet'):
                        update_trips(system, 'station')
    mock_load.assert_called_once_with(system, '2024', 'station', months=[1, 2])
