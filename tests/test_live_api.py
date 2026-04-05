"""Tests for LiveAPI client — with mocked HTTP requests."""
import datetime as dt
from unittest.mock import patch, MagicMock

import pandas as pd
import pytest

from bikeraccoon import LiveAPI


# ── Mock helpers ──────────────────────────────────────────────────────────────

SYSTEM_INFO = {
    'name': 'test_system',
    'tz': 'America/Toronto',
    'url': 'https://example.com/gbfs/',
    'tracking': True,
}

SYSTEMS_RESPONSE = {'data': [SYSTEM_INFO], 'query_time': '0:00:00.001', 'version': '3.0'}

TRIPS_RESPONSE = {
    'data': [
        {'datetime': '2024-06-01T12:00:00+00:00', 'trips': 10, 'returns': 8,
         'station_id': None, 'vehicle_type_id': None},
    ],
    'query_time': '0:00:00.001',
    'version': '3.0',
}

STATIONS_RESPONSE = {
    'data': [{'station_id': '1', 'name': 'Station A', 'lat': 43.6, 'lon': -79.4}],
    'query_time': '0:00:00.001',
    'version': '3.0',
}

EMPTY_RESPONSE = {'data': [], 'query_time': '0:00:00', 'version': '3.0'}


def _mock_get(url, *args, **kwargs):
    mock = MagicMock()
    if '/systems' in url:
        mock.json.return_value = SYSTEMS_RESPONSE
    elif '/activity' in url:
        mock.json.return_value = TRIPS_RESPONSE
    elif '/stations' in url:
        mock.json.return_value = STATIONS_RESPONSE
    else:
        mock.json.return_value = EMPTY_RESPONSE
    return mock


@pytest.fixture
def api():
    with patch('requests.get', side_effect=_mock_get):
        yield LiveAPI('test_system', api_key='testkey')


# ── URL construction ──────────────────────────────────────────────────────────

def test_build_url_no_key():
    with patch('requests.get', side_effect=_mock_get):
        a = LiveAPI('test_system')
    assert a._build_url('/activity') == 'http://api.raccoon.bike/activity'


def test_build_url_with_key():
    with patch('requests.get', side_effect=_mock_get):
        a = LiveAPI('test_system', api_key='mykey')
    assert a._build_url('/activity') == 'http://api.raccoon.bike/activity?key=mykey'


def test_build_url_appends_key_to_existing_query():
    with patch('requests.get', side_effect=_mock_get):
        a = LiveAPI('test_system', api_key='mykey')
    url = a._build_url('/activity?system=x')
    assert 'key=mykey' in url
    assert url.count('?') == 1


def test_build_url_without_key_no_key_param():
    with patch('requests.get', side_effect=_mock_get):
        a = LiveAPI('test_system')
    assert 'key=' not in a._build_url('/activity')


# ── get_system_info ───────────────────────────────────────────────────────────

def test_system_info_populated_on_init(api):
    assert api.info['name'] == 'test_system'
    assert api.info['tz'] == 'America/Toronto'


def test_system_info_unknown_system_raises():
    with patch('requests.get', side_effect=_mock_get):
        with pytest.raises(IndexError):
            LiveAPI('nonexistent_system')


# ── get_trips ─────────────────────────────────────────────────────────────────

def test_get_trips_returns_dataframe(api):
    t1 = dt.datetime(2024, 6, 1, 12)
    result = api.get_trips(t1)
    assert isinstance(result, pd.DataFrame)
    assert 'trips' in result.columns
    assert 'returns' in result.columns


def test_get_trips_url_includes_system(api):
    t1 = dt.datetime(2024, 6, 1, 12)
    with patch('requests.get', side_effect=_mock_get) as mock:
        api.get_trips(t1)
    assert 'system=test_system' in mock.call_args[0][0]


def test_get_trips_url_includes_date_range(api):
    t1 = dt.datetime(2024, 6, 1, 12)
    with patch('requests.get', side_effect=_mock_get) as mock:
        api.get_trips(t1)
    url = mock.call_args[0][0]
    assert 'start=' in url
    assert 'end=' in url


def test_get_trips_url_includes_frequency(api):
    t1 = dt.datetime(2024, 6, 1, 12)
    with patch('requests.get', side_effect=_mock_get) as mock:
        api.get_trips(t1, freq='d')
    assert 'frequency=d' in mock.call_args[0][0]


def test_get_trips_empty_response_returns_none(api):
    def _empty(url, *args, **kwargs):
        m = MagicMock()
        m.json.return_value = EMPTY_RESPONSE
        return m

    t1 = dt.datetime(2024, 6, 1, 12)
    with patch('requests.get', side_effect=_empty):
        result = api.get_trips(t1)
    assert result is None


def test_get_trips_with_station_filter(api):
    t1 = dt.datetime(2024, 6, 1, 12)
    with patch('requests.get', side_effect=_mock_get) as mock:
        api.get_trips(t1, station='42')
    assert 'station=42' in mock.call_args[0][0]


def test_get_trips_with_vehicle_filter(api):
    t1 = dt.datetime(2024, 6, 1, 12)
    with patch('requests.get', side_effect=_mock_get) as mock:
        api.get_trips(t1, vehicle='ebike')
    assert 'vehicle=ebike' in mock.call_args[0][0]


# ── get_station_trips / get_free_bike_trips ───────────────────────────────────

def test_get_station_trips_sets_feed(api):
    t1 = dt.datetime(2024, 6, 1, 12)
    with patch('requests.get', side_effect=_mock_get) as mock:
        api.get_station_trips(t1)
    assert 'feed=station' in mock.call_args[0][0]


def test_get_free_bike_trips_sets_feed(api):
    t1 = dt.datetime(2024, 6, 1, 12)
    with patch('requests.get', side_effect=_mock_get) as mock:
        api.get_free_bike_trips(t1)
    assert 'feed=free_bike' in mock.call_args[0][0]


# ── get_stations ──────────────────────────────────────────────────────────────

def test_get_stations_returns_dataframe(api):
    result = api.get_stations()
    assert isinstance(result, pd.DataFrame)
    assert 'station_id' in result.columns


def test_get_stations_url_includes_system(api):
    with patch('requests.get', side_effect=_mock_get) as mock:
        api.get_stations()
    assert 'system=test_system' in mock.call_args[0][0]
