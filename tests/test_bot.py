"""Tests for the bot module."""
import datetime as dt
import json
import os
import tempfile
from unittest.mock import patch, MagicMock

import pandas as pd
import pytest

from bikeraccoon import LiveAPI


# ── Shared mock helpers ───────────────────────────────────────────────────────

SYSTEM_INFO = {
    'name': 'test_system',
    'tz': 'America/Toronto',
    'url': 'https://example.com/gbfs/',
    'tracking': True,
}

SYSTEMS_RESPONSE = {'data': [SYSTEM_INFO], 'query_time': '0:00:00.001', 'version': '3.0'}

STATIONS_RESPONSE = {
    'data': [
        {'station_id': '1', 'name': 'Station A (Main)', 'lat': 43.6, 'lon': -79.4, 'active': True},
        {'station_id': '2', 'name': 'Station B', 'lat': 43.7, 'lon': -79.3, 'active': True},
        {'station_id': '3', 'name': 'Station C', 'lat': 43.5, 'lon': -79.5, 'active': False},
    ],
    'query_time': '0:00:00.001',
    'version': '3.0',
}

TRIPS_RESPONSE = {
    'data': [
        {'datetime': '2024-06-01T00:00:00+00:00', 'trips': 50, 'returns': 48,
         'station_id': None, 'vehicle_type_id': None},
    ],
    'query_time': '0:00:00.001',
    'version': '3.0',
}

TRIPS_ALL_STATIONS_RESPONSE = {
    'data': [
        {'datetime': '2024-06-01T00:00:00+00:00', 'trips': 30, 'returns': 28,
         'station_id': '1', 'vehicle_type_id': None},
        {'datetime': '2024-06-01T00:00:00+00:00', 'trips': 20, 'returns': 20,
         'station_id': '2', 'vehicle_type_id': None},
        {'datetime': '2024-06-01T00:00:00+00:00', 'trips': 0, 'returns': 0,
         'station_id': '3', 'vehicle_type_id': None},
    ],
    'query_time': '0:00:00.001',
    'version': '3.0',
}

EMPTY_RESPONSE = {'data': [], 'query_time': '0:00:00', 'version': '3.0'}


def _mock_get(url, *args, **kwargs):
    mock = MagicMock()
    if '/systems' in url:
        mock.json.return_value = SYSTEMS_RESPONSE
    elif '/stations' in url:
        mock.json.return_value = STATIONS_RESPONSE
    elif 'station=all' in url:
        mock.json.return_value = TRIPS_ALL_STATIONS_RESPONSE
    elif '/activity' in url:
        mock.json.return_value = TRIPS_RESPONSE
    else:
        mock.json.return_value = EMPTY_RESPONSE
    return mock


@pytest.fixture
def api():
    with patch('requests.get', side_effect=_mock_get):
        a = LiveAPI('test_system')
        a.brand = 'TestBike'
        a.sys_name = 'test_system'
        a.sys_type = 'stations'
        a.palette = ('#77ACA2', '#3286AD')
        a.extent = [-79.6, -79.2, 43.4, 43.9]
        yield a


# ── load_config ───────────────────────────────────────────────────────────────

def test_load_config():
    from bikeraccoon.bot.bot_functions import load_config

    master = {'mapbox_token': 'abc', 'visual_crossing_key': 'xyz'}
    bot = {'sys_name': 'mobi_vancouver', 'account': 'van.raccoon.bike', 'bsky_password': 'pw'}

    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as mf:
        json.dump(master, mf)
        master_path = mf.name
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as bf:
        json.dump(bot, bf)
        bot_path = bf.name

    try:
        m, b = load_config(master_path, bot_path)
        assert m['mapbox_token'] == 'abc'
        assert b['sys_name'] == 'mobi_vancouver'
    finally:
        os.unlink(master_path)
        os.unlink(bot_path)


# ── check_zero_trips ──────────────────────────────────────────────────────────

def test_check_zero_trips_false_when_trips_exist(api):
    from bikeraccoon.bot.bot_functions import check_zero_trips
    t1 = dt.datetime(2024, 6, 1, 0)
    t2 = dt.datetime(2024, 6, 1, 23)
    with patch('requests.get', side_effect=_mock_get):
        assert not check_zero_trips(t1, t2, api)


def test_check_zero_trips_raises_when_no_data(api):
    from bikeraccoon.bot.bot_functions import check_zero_trips

    def _empty(url, *a, **kw):
        m = MagicMock()
        m.json.return_value = EMPTY_RESPONSE
        return m

    t1 = dt.datetime(2024, 6, 1, 0)
    t2 = dt.datetime(2024, 6, 1, 23)
    with patch('requests.get', side_effect=_empty):
        with pytest.raises(ValueError, match='No trip data'):
            check_zero_trips(t1, t2, api)


def test_check_zero_trips_threshold(api):
    from bikeraccoon.bot.bot_functions import check_zero_trips
    t1 = dt.datetime(2024, 6, 1, 0)
    t2 = dt.datetime(2024, 6, 1, 23)
    with patch('requests.get', side_effect=_mock_get):
        # 50 trips, threshold 100 → True
        assert check_zero_trips(t1, t2, api, m=100)
        # 50 trips, threshold 10 → False
        assert not check_zero_trips(t1, t2, api, m=10)


# ── make_tweet_text ───────────────────────────────────────────────────────────

def test_make_tweet_text_en(api):
    from bikeraccoon.bot.bot_functions import make_tweet_text
    t1 = dt.datetime(2024, 6, 1, 12)
    with tempfile.TemporaryDirectory() as tmpdir:
        with patch('requests.get', side_effect=_mock_get):
            text = make_tweet_text(api, t1, path=tmpdir, lang='EN')
    assert 'TestBike' in text
    assert '50' in text
    assert 'Station A' in text


def test_make_tweet_text_fr(api):
    from bikeraccoon.bot.bot_functions import make_tweet_text
    t1 = dt.datetime(2024, 6, 1, 12)
    with tempfile.TemporaryDirectory() as tmpdir:
        with patch('requests.get', side_effect=_mock_get):
            text = make_tweet_text(api, t1, path=tmpdir, lang='FR')
    assert 'déplacements' in text
    assert 'TestBike' in text


def test_make_tweet_text_writes_file(api):
    from bikeraccoon.bot.bot_functions import make_tweet_text
    t1 = dt.datetime(2024, 6, 1, 12)
    with tempfile.TemporaryDirectory() as tmpdir:
        with patch('requests.get', side_effect=_mock_get):
            make_tweet_text(api, t1, path=tmpdir, lang='EN')
        assert os.path.exists(os.path.join(tmpdir, 'test_system_bot_text.txt'))


def test_make_tweet_text_strips_parentheticals(api):
    from bikeraccoon.bot.bot_functions import make_tweet_text
    t1 = dt.datetime(2024, 6, 1, 12)
    with tempfile.TemporaryDirectory() as tmpdir:
        with patch('requests.get', side_effect=_mock_get):
            text = make_tweet_text(api, t1, path=tmpdir, lang='EN')
    # "Station A (Main)" should have the parenthetical stripped
    assert '(Main)' not in text
    assert 'Station A' in text


# ── post_bsky ─────────────────────────────────────────────────────────────────

def test_post_bsky_calls_login_and_send():
    from bikeraccoon.bot.bot_functions import post_bsky

    mock_client = MagicMock()

    with patch('bikeraccoon.bot.bot_functions.Client', return_value=mock_client), \
         patch('bikeraccoon.bot.bot_functions.models'), \
         patch('bikeraccoon.bot.bot_functions.client_utils') as mock_utils:
        mock_utils.TextBuilder.return_value = MagicMock()
        with tempfile.NamedTemporaryFile(suffix='.png', delete=False) as f:
            f.write(b'fakeimage')
            img_path = f.name
        try:
            post_bsky('user.bsky.social', 'secret', 'hello', images=[img_path], descriptions=['desc'])
        finally:
            os.unlink(img_path)

    mock_client.login.assert_called_once_with('user.bsky.social', 'secret')
    mock_client.send_post.assert_called_once()


def test_post_bsky_no_images():
    from bikeraccoon.bot.bot_functions import post_bsky

    mock_client = MagicMock()
    with patch('bikeraccoon.bot.bot_functions.Client', return_value=mock_client), \
         patch('bikeraccoon.bot.bot_functions.models'), \
         patch('bikeraccoon.bot.bot_functions.client_utils') as mock_utils:
        mock_utils.TextBuilder.return_value = MagicMock()
        post_bsky('user.bsky.social', 'secret', 'hello world')

    mock_client.login.assert_called_once()
    mock_client.send_post.assert_called_once()
