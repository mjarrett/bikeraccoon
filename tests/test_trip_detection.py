"""Tests for trip detection logic in make_station_trips and make_free_bike_trips."""
import datetime as dt
import pandas as pd
import pytest

from bikeraccoon.tracker.tracker_functions import make_station_trips, make_free_bike_trips


def make_station_df(polls):
    """
    Build a raw station poll DataFrame.
    polls: list of (datetime, station_id, vehicle_type_id, num_bikes_available)
    """
    rows = [
        {'datetime': ts, 'station_id': sid, 'vehicle_type_id': vt, 'num_bikes_available': n}
        for ts, sid, vt, n in polls
    ]
    return pd.DataFrame(rows)


def make_free_bike_df(polls):
    """
    Build a raw free bike poll DataFrame.
    polls: list of (datetime, station_id, vehicle_type_id, num_bikes_available, lat, lon)
    Use distinct lat/lon pairs per logical "zone" to avoid duplicate column names in pivot.
    """
    rows = [
        {'datetime': ts, 'station_id': sid, 'vehicle_type_id': vt,
         'num_bikes_available': n, 'lat': lat, 'lon': lon}
        for ts, sid, vt, n, lat, lon in polls
    ]
    return pd.DataFrame(rows)


T0 = dt.datetime(2024, 6, 1, 10, 0)
T1 = dt.datetime(2024, 6, 1, 10, 5)
T2 = dt.datetime(2024, 6, 1, 10, 10)


# ── make_station_trips ────────────────────────────────────────────────────────

def test_station_empty_input():
    result = make_station_trips(pd.DataFrame())
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 0


def test_station_no_change():
    """No bikes move — expect zero trips and returns."""
    df = make_station_df([
        (T0, 'A', 'bike', 5),
        (T1, 'A', 'bike', 5),
    ])
    result = make_station_trips(df)
    assert result['trips'].sum() == 0
    assert result['returns'].sum() == 0


def test_station_one_trip():
    """One bike leaves between polls — expect one trip."""
    df = make_station_df([
        (T0, 'A', 'bike', 5),
        (T1, 'A', 'bike', 4),
    ])
    result = make_station_trips(df)
    assert result['trips'].sum() == 1
    assert result['returns'].sum() == 0


def test_station_one_return():
    """One bike returns between polls — expect one return."""
    df = make_station_df([
        (T0, 'A', 'bike', 4),
        (T1, 'A', 'bike', 5),
    ])
    result = make_station_trips(df)
    assert result['trips'].sum() == 0
    assert result['returns'].sum() == 1


def test_station_multiple_stations():
    """Trips at two stations are counted independently."""
    df = make_station_df([
        (T0, 'A', 'bike', 5),
        (T0, 'B', 'bike', 3),
        (T1, 'A', 'bike', 4),  # 1 trip from A
        (T1, 'B', 'bike', 4),  # 1 return to B
    ])
    result = make_station_trips(df)
    assert result['trips'].sum() == 1
    assert result['returns'].sum() == 1


def test_station_multiple_vehicle_types():
    """Trips are tracked per vehicle type."""
    df = make_station_df([
        (T0, 'A', 'ebike', 3),
        (T0, 'A', 'bike', 5),
        (T1, 'A', 'ebike', 2),  # 1 ebike trip
        (T1, 'A', 'bike', 5),   # no change
    ])
    result = make_station_trips(df)
    ebike = result[result['vehicle_type_id'] == 'ebike']
    bike = result[result['vehicle_type_id'] == 'bike']
    assert ebike['trips'].sum() == 1
    assert bike['trips'].sum() == 0


def test_station_trips_aggregated_hourly():
    """Multiple polls within the same hour are summed."""
    df = make_station_df([
        (T0, 'A', 'bike', 5),
        (T1, 'A', 'bike', 4),  # 1 trip
        (T2, 'A', 'bike', 3),  # 1 trip
    ])
    result = make_station_trips(df)
    assert result['trips'].sum() == 2


def test_station_output_columns():
    """Result has expected columns."""
    df = make_station_df([
        (T0, 'A', 'bike', 5),
        (T1, 'A', 'bike', 4),
    ])
    result = make_station_trips(df)
    assert set(result.columns) >= {'datetime', 'station_id', 'vehicle_type_id', 'trips', 'returns'}


# ── make_free_bike_trips ──────────────────────────────────────────────────────

def test_free_bike_empty_input():
    result = make_free_bike_trips(pd.DataFrame())
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 0


def test_free_bike_no_change():
    """Bike count unchanged — expect zero trips."""
    df = make_free_bike_df([
        (T0, 'z1', 'bike', 10, 49.2, -123.1),
        (T1, 'z1', 'bike', 10, 49.2, -123.1),
    ])
    result = make_free_bike_trips(df)
    assert result['trips'].sum() == 0
    assert result['returns'].sum() == 0


def test_free_bike_one_trip():
    """One bike disappears — expect one trip."""
    df = make_free_bike_df([
        (T0, 'z1', 'bike', 5, 49.2, -123.1),
        (T1, 'z1', 'bike', 4, 49.2, -123.1),
    ])
    result = make_free_bike_trips(df)
    assert result['trips'].sum() == 1
    assert result['returns'].sum() == 0


def test_free_bike_one_return():
    """One bike appears — expect one return."""
    df = make_free_bike_df([
        (T0, 'z1', 'bike', 4, 49.2, -123.1),
        (T1, 'z1', 'bike', 5, 49.2, -123.1),
    ])
    result = make_free_bike_trips(df)
    assert result['trips'].sum() == 0
    assert result['returns'].sum() == 1


def test_free_bike_output_columns():
    """Result has expected columns."""
    df = make_free_bike_df([
        (T0, 'z1', 'bike', 5, 49.2, -123.1),
        (T1, 'z1', 'bike', 4, 49.2, -123.1),
    ])
    result = make_free_bike_trips(df)
    assert set(result.columns) >= {'datetime', 'station_id', 'vehicle_type_id', 'trips', 'returns'}
