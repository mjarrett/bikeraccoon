"""Tests for _dates2strings and trim_raw."""
import datetime as dt
import tempfile
import pandas as pd
import pytest

from bikeraccoon import _dates2strings
from bikeraccoon.tracker.tracker_functions import trim_raw


# ── _dates2strings ────────────────────────────────────────────────────────────

def test_dates2strings_explicit_t2():
    t1 = dt.datetime(2024, 6, 1, 8, 0)
    t2 = dt.datetime(2024, 6, 1, 12, 0)
    r1, r2 = _dates2strings(t1, t2)
    assert r1 == "2024060108"
    assert r2 == "2024060112"


def test_dates2strings_hourly_t2_none():
    """freq='h' with no t2 — t2 defaults to t1."""
    t = dt.datetime(2024, 6, 1, 9, 0)
    r1, r2 = _dates2strings(t, None, freq='h')
    assert r1 == r2 == "2024060109"


def test_dates2strings_daily_t2_none():
    """freq='d' with no t2 — returns full day from hour 0 to 23."""
    t = dt.datetime(2024, 6, 15, 14, 30)
    r1, r2 = _dates2strings(t, None, freq='d')
    assert r1 == "2024061500"
    assert r2 == "2024061523"


def test_dates2strings_monthly_t2_none():
    """freq='m' with no t2 — returns full month."""
    t = dt.datetime(2024, 6, 15, 14, 0)
    r1, r2 = _dates2strings(t, None, freq='m')
    assert r1 == "2024060100"
    assert r2 == "2024063023"


def test_dates2strings_monthly_february():
    """February edge case — last day should be 28 or 29."""
    t = dt.datetime(2024, 2, 10, 0, 0)  # 2024 is a leap year
    r1, r2 = _dates2strings(t, None, freq='m')
    assert r1 == "2024020100"
    assert r2 == "2024022923"


def test_dates2strings_yearly_t2_none():
    """freq='y' with no t2 — returns full year."""
    t = dt.datetime(2024, 6, 15, 9, 0)
    r1, r2 = _dates2strings(t, None, freq='y')
    assert r1 == "2024010100"
    assert r2 == "2024123123"


def test_dates2strings_swaps_reversed():
    """t1 > t2 — should be swapped."""
    t1 = dt.datetime(2024, 6, 10, 0, 0)
    t2 = dt.datetime(2024, 6, 1, 0, 0)
    r1, r2 = _dates2strings(t1, t2)
    assert r1 < r2


def test_dates2strings_output_format():
    """Output should always be 10-character strings."""
    t1 = dt.datetime(2024, 1, 1, 0, 0)
    t2 = dt.datetime(2024, 12, 31, 23, 0)
    r1, r2 = _dates2strings(t1, t2)
    assert len(r1) == 10
    assert len(r2) == 10


# ── trim_raw ──────────────────────────────────────────────────────────────────

def _make_raw_parquet(polls, path):
    """Write a raw parquet file with given (datetime, value) polls."""
    df = pd.DataFrame([
        {'datetime': ts, 'station_id': 'A', 'num_bikes_available': n}
        for ts, n in polls
    ])
    df.to_parquet(path, index=False)
    return df


T0 = dt.datetime(2024, 6, 1, 10, 0)
T1 = dt.datetime(2024, 6, 1, 10, 5)
T2 = dt.datetime(2024, 6, 1, 10, 10)


def test_trim_raw_keeps_only_latest():
    """Multiple polls — only the latest timestamp is kept."""
    with tempfile.NamedTemporaryFile(suffix='.parquet') as f:
        _make_raw_parquet([(T0, 5), (T1, 4), (T2, 3)], f.name)
        trim_raw(f.name)
        result = pd.read_parquet(f.name)
    assert len(result) == 1
    assert result.iloc[0]['datetime'] == T2


def test_trim_raw_single_poll_unchanged():
    """Single poll — file should be unchanged."""
    with tempfile.NamedTemporaryFile(suffix='.parquet') as f:
        _make_raw_parquet([(T0, 5)], f.name)
        trim_raw(f.name)
        result = pd.read_parquet(f.name)
    assert len(result) == 1
    assert result.iloc[0]['datetime'] == T0


def test_trim_raw_multiple_rows_same_latest():
    """Multiple rows share the latest timestamp — all should be kept."""
    with tempfile.NamedTemporaryFile(suffix='.parquet') as f:
        df = pd.DataFrame([
            {'datetime': T0, 'station_id': 'A', 'num_bikes_available': 5},
            {'datetime': T1, 'station_id': 'A', 'num_bikes_available': 4},
            {'datetime': T1, 'station_id': 'B', 'num_bikes_available': 3},
        ])
        df.to_parquet(f.name, index=False)
        trim_raw(f.name)
        result = pd.read_parquet(f.name)
    assert len(result) == 2
    assert (result['datetime'] == T1).all()


def test_trim_raw_missing_file():
    """Non-existent file — should not raise."""
    trim_raw('/tmp/does_not_exist_bikeraccoon.parquet')
