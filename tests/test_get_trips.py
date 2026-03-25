"""Tests for get_trips — DuckDB query logic with temp parquet fixtures."""
import datetime as dt
import json
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

import bikeraccoon.api.api_functions as af
from bikeraccoon.api.api import app


@pytest.fixture
def trip_parquets(tmp_path):
    """Create a minimal hourly and daily parquet tree for 'test_sys'."""
    rows = [
        {'datetime': pd.Timestamp('2024-06-01 10:00:00'),
         'station_id': 'A', 'vehicle_type_id': 'bike', 'trips': 5, 'returns': 3},
        {'datetime': pd.Timestamp('2024-06-01 11:00:00'),
         'station_id': 'A', 'vehicle_type_id': 'bike', 'trips': 2, 'returns': 4},
        {'datetime': pd.Timestamp('2024-06-01 10:00:00'),
         'station_id': 'B', 'vehicle_type_id': 'ebike', 'trips': 1, 'returns': 0},
    ]
    df = pd.DataFrame(rows)

    hourly_dir = tmp_path / 'trips.station.hourly' / 'year=2024' / 'month=6'
    hourly_dir.mkdir(parents=True)
    df.to_parquet(hourly_dir / 'data.parquet', index=False)

    daily_dir = tmp_path / 'trips.station.daily' / 'year=2024' / 'month=6'
    daily_dir.mkdir(parents=True)
    daily = df.copy()
    daily['datetime'] = pd.Timestamp('2024-06-01 00:00:00')
    daily = daily.groupby(['datetime', 'station_id', 'vehicle_type_id'], as_index=False).sum()
    daily.to_parquet(daily_dir / 'data.parquet', index=False)

    return tmp_path


def _glob(tmp_path, feed, freq_type):
    return str(tmp_path / f'trips.{feed}.{freq_type}' / 'year=*' / 'month=*' / '*.parquet')


def _call_get_trips(tmp_path, monkeypatch, t1, t2, feed='station',
                    station_id=None, vehicle_type_id=None, frequency='h'):
    monkeypatch.setattr(af, 'get_system_tz', lambda _: 'UTC')
    freq_type = 'hourly' if frequency in ('h', 't') else 'daily'
    monkeypatch.setattr(af, 'get_data_path',
                        lambda *a, **kw: _glob(tmp_path, feed, freq_type))

    with app.app_context():
        response = af.get_trips(t1, t2, 'test_sys', feed, station_id, vehicle_type_id, frequency)

    return json.loads(response.get_data())


T1 = dt.datetime(2024, 6, 1, 0, tzinfo=ZoneInfo('UTC'))
T2 = dt.datetime(2024, 6, 1, 23, tzinfo=ZoneInfo('UTC'))


def test_get_trips_returns_data(trip_parquets, monkeypatch):
    result = _call_get_trips(trip_parquets, monkeypatch, T1, T2)
    assert 'data' in result
    assert len(result['data']) > 0


def test_get_trips_response_has_trips_and_returns(trip_parquets, monkeypatch):
    result = _call_get_trips(trip_parquets, monkeypatch, T1, T2)
    row = result['data'][0]
    assert 'trips' in row
    assert 'returns' in row


def test_get_trips_total_frequency(trip_parquets, monkeypatch):
    result = _call_get_trips(trip_parquets, monkeypatch, T1, T2, frequency='t')
    assert len(result['data']) == 1
    assert result['data'][0]['trips'] == 8  # 5+2+1


def test_get_trips_filters_by_station(trip_parquets, monkeypatch):
    result = _call_get_trips(trip_parquets, monkeypatch, T1, T2, station_id='A')
    stations = {r['station_id'] for r in result['data']}
    assert stations == {'A'}


def test_get_trips_filters_by_vehicle_type(trip_parquets, monkeypatch):
    result = _call_get_trips(trip_parquets, monkeypatch, T1, T2, vehicle_type_id='ebike')
    vehicles = {r['vehicle_type_id'] for r in result['data']}
    assert vehicles == {'ebike'}


def test_get_trips_all_stations(trip_parquets, monkeypatch):
    result = _call_get_trips(trip_parquets, monkeypatch, T1, T2, station_id='all')
    stations = {r['station_id'] for r in result['data']}
    assert 'A' in stations
    assert 'B' in stations


def test_get_trips_daily_frequency(trip_parquets, monkeypatch):
    result = _call_get_trips(trip_parquets, monkeypatch, T1, T2, frequency='d')
    assert len(result['data']) > 0


def test_get_trips_empty_range_returns_empty(trip_parquets, monkeypatch):
    t1 = dt.datetime(2020, 1, 1, 0, tzinfo=ZoneInfo('UTC'))
    t2 = dt.datetime(2020, 1, 1, 23, tzinfo=ZoneInfo('UTC'))
    result = _call_get_trips(trip_parquets, monkeypatch, t1, t2)
    assert result['data'] == []


def test_get_trips_response_includes_version(trip_parquets, monkeypatch):
    result = _call_get_trips(trip_parquets, monkeypatch, T1, T2)
    assert 'version' in result


def test_get_trips_response_includes_query_time(trip_parquets, monkeypatch):
    result = _call_get_trips(trip_parquets, monkeypatch, T1, T2)
    assert 'query_time' in result
