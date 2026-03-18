"""Basic smoke tests — verify the package structure is intact."""
import datetime as dt


def test_package_imports():
    import bikeraccoon
    from bikeraccoon import LiveAPI, get_systems


def test_liveapi_class_exists():
    from bikeraccoon import LiveAPI
    assert callable(LiveAPI)


def test_get_systems_callable():
    from bikeraccoon import get_systems
    assert callable(get_systems)


def test_dates2strings_same_day():
    from bikeraccoon import _dates2strings
    t = dt.datetime(2024, 6, 1, 12, 0)
    t1, t2 = _dates2strings(t, t)
    assert t1 == "2024060112"
    assert t2 == "2024060112"


def test_dates2strings_swaps_order():
    from bikeraccoon import _dates2strings
    t1 = dt.datetime(2024, 6, 2, 0, 0)
    t2 = dt.datetime(2024, 6, 1, 0, 0)
    r1, r2 = _dates2strings(t1, t2)
    assert r1 < r2


def test_dates2strings_daily_default_range():
    from bikeraccoon import _dates2strings
    t = dt.datetime(2024, 6, 15, 9, 0)
    t1, t2 = _dates2strings(t, None, freq='d')
    assert t1 == "2024061500"
    assert t2 == "2024061523"
