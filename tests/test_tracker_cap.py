"""Tests for raw snapshot cap: _trim_raw_snapshots and _handle_feed_alerts backlog tracking."""
import datetime as dt
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from bikeraccoon.tracker.tracker_functions import GBFSSystem, _trim_raw_snapshots
from bikeraccoon.tracker.tracker import _handle_feed_alerts


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_system(max_raw_snapshots=20, smtp_config=None):
    s = GBFSSystem({'name': 'test_city', 'tz': 'America/Toronto'})
    s.logger = MagicMock()
    s.max_raw_snapshots = max_raw_snapshots
    s.smtp_config = smtp_config
    return s


def _make_raw_df(n_timestamps, rows_per_timestamp=10):
    """Build a fake raw parquet DataFrame with n distinct timestamps."""
    base = pd.Timestamp('2026-01-01 00:00:00', tz='America/Toronto')
    times = [base + dt.timedelta(minutes=i) for i in range(n_timestamps)]
    rows = []
    for t in times:
        for i in range(rows_per_timestamp):
            rows.append({'datetime': t, 'station_id': str(i), 'num_bikes_available': 5})
    return pd.DataFrame(rows)


SMTP_CONFIG = {
    'host': 'smtp.example.com', 'port': 587, 'tls': False,
    'from': 'from@example.com', 'to': 'to@example.com',
}


# ── _trim_raw_snapshots ───────────────────────────────────────────────────────

def test_trim_no_op_when_under_cap():
    system = _make_system(max_raw_snapshots=20)
    df = _make_raw_df(10)
    result, dropped = _trim_raw_snapshots(df, 'station', system)
    assert dropped == 0
    assert len(result) == len(df)


def test_trim_no_op_when_exactly_at_cap():
    system = _make_system(max_raw_snapshots=20)
    df = _make_raw_df(20)
    result, dropped = _trim_raw_snapshots(df, 'station', system)
    assert dropped == 0
    assert result['datetime'].nunique() == 20


def test_trim_drops_to_cap_when_over():
    system = _make_system(max_raw_snapshots=5)
    df = _make_raw_df(8)
    result, dropped = _trim_raw_snapshots(df, 'free_bike', system)
    assert dropped == 3
    assert result['datetime'].nunique() == 5


def test_trim_keeps_newest_timestamps():
    system = _make_system(max_raw_snapshots=3)
    df = _make_raw_df(5)
    result, _ = _trim_raw_snapshots(df, 'station', system)
    kept = sorted(result['datetime'].unique())
    all_times = sorted(df['datetime'].unique())
    assert kept == all_times[-3:]


def test_trim_warns_when_dropped_gt_1():
    system = _make_system(max_raw_snapshots=5)
    df = _make_raw_df(8)  # drops 3
    _trim_raw_snapshots(df, 'free_bike', system)
    system.logger.warning.assert_called_once()


def test_trim_no_warn_when_dropped_eq_1():
    """Steady-state single-drop (cap+1) should be silent."""
    system = _make_system(max_raw_snapshots=5)
    df = _make_raw_df(6)  # drops exactly 1
    _trim_raw_snapshots(df, 'free_bike', system)
    system.logger.warning.assert_not_called()


def test_trim_no_warn_when_nothing_dropped():
    system = _make_system(max_raw_snapshots=20)
    df = _make_raw_df(5)
    _trim_raw_snapshots(df, 'station', system)
    system.logger.warning.assert_not_called()


# ── _handle_feed_alerts: cap backlog tracking ─────────────────────────────────

def _run_alerts(systems, results, smtp_config=None):
    logger = MagicMock()
    _handle_feed_alerts(systems, results, failure_threshold=5,
                        smtp_config=smtp_config, logger=logger)
    return logger


def _ok_result(station_cap_dropped=0, free_bike_cap_dropped=0):
    return {
        'station': True, 'station_error': None,
        'free_bike': True, 'free_bike_error': None,
        'station_cap_dropped': station_cap_dropped,
        'free_bike_cap_dropped': free_bike_cap_dropped,
    }


def test_cap_alert_sent_on_first_backlog():
    system = _make_system()
    with patch('bikeraccoon.tracker.tracker.send_alert_email') as mock_email:
        _run_alerts([system], [_ok_result(free_bike_cap_dropped=5)], smtp_config=SMTP_CONFIG)
    mock_email.assert_called_once()
    subject = mock_email.call_args[1]['subject'] if mock_email.call_args[1] else mock_email.call_args[0][1]
    assert 'backlog' in subject.lower()


def test_cap_alerted_flag_set_after_alert():
    system = _make_system()
    with patch('bikeraccoon.tracker.tracker.send_alert_email'):
        _run_alerts([system], [_ok_result(free_bike_cap_dropped=5)], smtp_config=SMTP_CONFIG)
    assert system.get('__free_bike_cap_alert_sent') is True


def test_cap_alert_not_resent_if_already_alerted():
    system = _make_system()
    system['__free_bike_cap_alert_sent'] = True
    with patch('bikeraccoon.tracker.tracker.send_alert_email') as mock_email:
        _run_alerts([system], [_ok_result(free_bike_cap_dropped=5)], smtp_config=SMTP_CONFIG)
    mock_email.assert_not_called()


def test_cap_no_alert_for_single_drop():
    """dropped==1 is steady-state and must not trigger an alert."""
    system = _make_system()
    with patch('bikeraccoon.tracker.tracker.send_alert_email') as mock_email:
        _run_alerts([system], [_ok_result(free_bike_cap_dropped=1)], smtp_config=SMTP_CONFIG)
    mock_email.assert_not_called()


def test_cap_no_alert_without_smtp_config():
    system = _make_system()
    with patch('bikeraccoon.tracker.tracker.send_alert_email') as mock_email:
        _run_alerts([system], [_ok_result(free_bike_cap_dropped=5)], smtp_config=None)
    mock_email.assert_not_called()


def test_cap_warning_logged_only_once_without_smtp():
    """Alerted flag should be set even without smtp_config so the warning doesn't repeat."""
    system = _make_system()
    logger = MagicMock()
    with patch('bikeraccoon.tracker.tracker.send_alert_email'):
        _handle_feed_alerts([system], [_ok_result(free_bike_cap_dropped=5)],
                            failure_threshold=5, smtp_config=None, logger=logger)
        _handle_feed_alerts([system], [_ok_result(free_bike_cap_dropped=5)],
                            failure_threshold=5, smtp_config=None, logger=logger)
    warning_calls = [c for c in logger.warning.call_args_list
                     if 'backlog' in str(c).lower()]
    assert len(warning_calls) == 1


def test_cap_recovery_email_sent_when_backlog_clears():
    system = _make_system()
    system['__free_bike_cap_alert_sent'] = True
    with patch('bikeraccoon.tracker.tracker.send_alert_email') as mock_email:
        _run_alerts([system], [_ok_result(free_bike_cap_dropped=0)], smtp_config=SMTP_CONFIG)
    mock_email.assert_called_once()
    subject = mock_email.call_args[1]['subject'] if mock_email.call_args[1] else mock_email.call_args[0][1]
    assert 'recovered' in subject.lower()


def test_cap_alerted_flag_cleared_on_recovery():
    system = _make_system()
    system['__free_bike_cap_alert_sent'] = True
    with patch('bikeraccoon.tracker.tracker.send_alert_email'):
        _run_alerts([system], [_ok_result(free_bike_cap_dropped=0)], smtp_config=SMTP_CONFIG)
    assert system.get('__free_bike_cap_alert_sent') is False


def test_cap_no_recovery_when_still_at_cap_boundary():
    """cap_dropped==1 means file is still at the cap limit — not genuinely recovered."""
    system = _make_system()
    system['__free_bike_cap_alert_sent'] = True
    with patch('bikeraccoon.tracker.tracker.send_alert_email') as mock_email:
        _run_alerts([system], [_ok_result(free_bike_cap_dropped=1)], smtp_config=SMTP_CONFIG)
    mock_email.assert_not_called()
    assert system.get('__free_bike_cap_alert_sent') is True  # flag stays set


def test_cap_no_recovery_email_without_prior_alert():
    """Backlog clearing when no alert was sent should not send recovery email."""
    system = _make_system()
    with patch('bikeraccoon.tracker.tracker.send_alert_email') as mock_email:
        _run_alerts([system], [_ok_result(free_bike_cap_dropped=0)], smtp_config=SMTP_CONFIG)
    mock_email.assert_not_called()
