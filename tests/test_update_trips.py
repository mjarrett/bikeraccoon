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
