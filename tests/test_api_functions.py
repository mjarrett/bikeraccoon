"""Tests for bikeraccoon.api.api_functions — pure utility functions."""
import datetime as dt
import pytest
from zoneinfo import ZoneInfo

from bikeraccoon.api.api_functions import string_to_datetime, get_data_path, BRJSONProvider


# ── string_to_datetime ────────────────────────────────────────────────────────

def test_string_to_datetime_basic():
    result = string_to_datetime('2024060112', 'UTC')
    assert result == dt.datetime(2024, 6, 1, 12, tzinfo=ZoneInfo('UTC'))


def test_string_to_datetime_midnight():
    result = string_to_datetime('2024010100', 'UTC')
    assert result == dt.datetime(2024, 1, 1, 0, tzinfo=ZoneInfo('UTC'))


def test_string_to_datetime_timezone_aware():
    result = string_to_datetime('2024060112', 'America/Toronto')
    assert result.tzinfo is not None
    assert result.hour == 12
    assert result.year == 2024


def test_string_to_datetime_different_timezones_differ():
    utc = string_to_datetime('2024060112', 'UTC')
    toronto = string_to_datetime('2024060112', 'America/Toronto')
    # Same wall-clock time, different absolute instants
    assert utc != toronto


def test_string_to_datetime_invalid_raises():
    with pytest.raises(Exception):
        string_to_datetime('notadate', 'UTC')


# ── get_data_path ─────────────────────────────────────────────────────────────

def test_get_data_path_hourly_freq():
    path = get_data_path('mysys', 'station', None, 'h')
    assert 'mysys' in path
    assert 'station' in path
    assert 'hourly' in path


def test_get_data_path_total_freq_uses_hourly():
    assert 'hourly' in get_data_path('mysys', 'station', None, 't')


def test_get_data_path_daily_freq():
    assert 'daily' in get_data_path('mysys', 'station', None, 'd')


def test_get_data_path_monthly_freq():
    assert 'daily' in get_data_path('mysys', 'station', None, 'm')


def test_get_data_path_yearly_freq():
    assert 'daily' in get_data_path('mysys', 'station', None, 'y')


def test_get_data_path_includes_feed_type():
    path = get_data_path('mysys', 'free_bike', None, 'h')
    assert 'free_bike' in path


def test_get_data_path_includes_glob_wildcards():
    path = get_data_path('mysys', 'station', None, 'h')
    assert '*' in path


# ── BRJSONProvider ────────────────────────────────────────────────────────────

def test_brjsonprovider_serializes_datetime():
    from bikeraccoon.api.api import app
    provider = BRJSONProvider(app)
    ts = dt.datetime(2024, 6, 1, 12, 0, 0)
    assert provider.default(ts) == ts.isoformat()


def test_brjsonprovider_serializes_timezone_aware_datetime():
    from bikeraccoon.api.api import app
    provider = BRJSONProvider(app)
    ts = dt.datetime(2024, 6, 1, 12, 0, 0, tzinfo=ZoneInfo('America/Toronto'))
    result = provider.default(ts)
    assert '2024-06-01T12:00:00' in result


def test_brjsonprovider_raises_for_unknown_type():
    from bikeraccoon.api.api import app
    provider = BRJSONProvider(app)
    with pytest.raises(TypeError):
        provider.default(object())
