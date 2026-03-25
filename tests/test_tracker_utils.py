"""Tests for tracker utility functions: _fmt_dt, summaries, send_alert_email."""
import datetime as dt
from unittest.mock import patch

import pandas as pd
import pytest

from bikeraccoon.tracker.tracker_functions import (
    _fmt_dt,
    build_system_summary,
    build_system_summary_html,
    send_alert_email,
    GBFSSystem,
)


# ── _fmt_dt ───────────────────────────────────────────────────────────────────

def test_fmt_dt_none_returns_dash():
    assert _fmt_dt(None) == '—'


def test_fmt_dt_nan_returns_dash():
    assert _fmt_dt(float('nan')) == '—'


def test_fmt_dt_valid_timestamp():
    assert _fmt_dt(pd.Timestamp('2024-06-01 12:30:00')) == '2024-06-01 12:30'


def test_fmt_dt_string_input():
    assert _fmt_dt('2024-06-01 09:00:00') == '2024-06-01 09:00'


def test_fmt_dt_invalid_returns_string():
    result = _fmt_dt('notadate')
    assert isinstance(result, str)


# ── build_system_summary helpers ──────────────────────────────────────────────

def _fresh_system(name='test_city', tz='America/Toronto'):
    """A non-stale system with no data files (graceful fallback expected)."""
    s = GBFSSystem({
        'name': name,
        'tz': tz,
        'latest_update': dt.datetime.now(dt.timezone.utc).isoformat(),
    })
    s.data_path = '/nonexistent/path'
    return s


def _stale_system():
    """A system whose latest_update is over an hour ago."""
    s = GBFSSystem({
        'name': 'stale_city',
        'tz': 'UTC',
        'latest_update': (dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=2)).isoformat(),
    })
    s.data_path = '/nonexistent/path'
    return s


# ── build_system_summary (plain text) ────────────────────────────────────────

def test_summary_contains_name():
    assert 'test_city' in build_system_summary(_fresh_system())


def test_summary_contains_timezone():
    assert 'America/Toronto' in build_system_summary(_fresh_system())


def test_summary_stale_flag_present_when_stale():
    assert 'STALE' in build_system_summary(_stale_system())


def test_summary_stale_flag_absent_when_fresh():
    assert 'STALE' not in build_system_summary(_fresh_system())


def test_summary_handles_no_latest_update():
    s = GBFSSystem({'name': 'no_update', 'tz': 'UTC'})
    s.data_path = '/nonexistent/path'
    result = build_system_summary(s)
    assert 'no_update' in result


def test_summary_shows_no_data_for_missing_feeds():
    result = build_system_summary(_fresh_system())
    assert 'no data' in result


# ── build_system_summary_html ─────────────────────────────────────────────────

def test_html_summary_contains_name():
    assert 'test_city' in build_system_summary_html(_fresh_system())


def test_html_summary_contains_timezone():
    assert 'America/Toronto' in build_system_summary_html(_fresh_system())


def test_html_summary_stale_badge_when_stale():
    result = build_system_summary_html(_stale_system())
    assert 'STALE' in result
    assert '#c0392b' in result  # red header


def test_html_summary_no_stale_badge_when_fresh():
    result = build_system_summary_html(_fresh_system())
    assert 'STALE' not in result
    assert '#2c3e50' in result  # normal header


def test_html_summary_escapes_name():
    s = GBFSSystem({'name': '<script>xss</script>', 'tz': 'UTC'})
    s.data_path = '/nonexistent/path'
    result = build_system_summary_html(s)
    assert '<script>' not in result


def test_html_summary_is_valid_html_fragment():
    result = build_system_summary_html(_fresh_system())
    assert result.startswith('<div')
    assert result.endswith('</div>')


# ── send_alert_email ──────────────────────────────────────────────────────────

SMTP_CONFIG = {
    'host': 'smtp.example.com',
    'port': 587,
    'tls': False,
    'from': 'from@example.com',
    'to': ['to@example.com'],
}


def test_plain_text_email_content_type():
    with patch('smtplib.SMTP') as mock_smtp:
        instance = mock_smtp.return_value.__enter__.return_value
        send_alert_email(SMTP_CONFIG, 'Subject', 'Plain body')
    sent = instance.send_message.call_args[0][0]
    assert sent.get_content_type() == 'text/plain'


def test_html_email_is_multipart_alternative():
    with patch('smtplib.SMTP') as mock_smtp:
        instance = mock_smtp.return_value.__enter__.return_value
        send_alert_email(SMTP_CONFIG, 'Subject', 'Plain body', html_body='<p>HTML</p>')
    sent = instance.send_message.call_args[0][0]
    assert sent.get_content_type() == 'multipart/alternative'


def test_html_email_has_both_parts():
    with patch('smtplib.SMTP') as mock_smtp:
        instance = mock_smtp.return_value.__enter__.return_value
        send_alert_email(SMTP_CONFIG, 'Subject', 'Plain body', html_body='<p>HTML</p>')
    sent = instance.send_message.call_args[0][0]
    content_types = [p.get_content_type() for p in sent.get_payload()]
    assert 'text/plain' in content_types
    assert 'text/html' in content_types


def test_tls_is_started_when_configured():
    config = {**SMTP_CONFIG, 'tls': True}
    with patch('smtplib.SMTP') as mock_smtp:
        instance = mock_smtp.return_value.__enter__.return_value
        send_alert_email(config, 'Sub', 'Body')
    instance.starttls.assert_called_once()


def test_tls_not_started_when_disabled():
    with patch('smtplib.SMTP') as mock_smtp:
        instance = mock_smtp.return_value.__enter__.return_value
        send_alert_email(SMTP_CONFIG, 'Sub', 'Body')  # tls=False
    instance.starttls.assert_not_called()


def test_login_called_with_credentials():
    config = {**SMTP_CONFIG, 'username': 'user', 'password': 'pass'}
    with patch('smtplib.SMTP') as mock_smtp:
        instance = mock_smtp.return_value.__enter__.return_value
        send_alert_email(config, 'Sub', 'Body')
    instance.login.assert_called_once_with('user', 'pass')


def test_login_not_called_without_credentials():
    with patch('smtplib.SMTP') as mock_smtp:
        instance = mock_smtp.return_value.__enter__.return_value
        send_alert_email(SMTP_CONFIG, 'Sub', 'Body')
    instance.login.assert_not_called()


def test_to_as_list_joined():
    with patch('smtplib.SMTP') as mock_smtp:
        instance = mock_smtp.return_value.__enter__.return_value
        send_alert_email(SMTP_CONFIG, 'Sub', 'Body')
    sent = instance.send_message.call_args[0][0]
    assert sent['To'] == 'to@example.com'


def test_to_as_string():
    config = {**SMTP_CONFIG, 'to': 'single@example.com'}
    with patch('smtplib.SMTP') as mock_smtp:
        instance = mock_smtp.return_value.__enter__.return_value
        send_alert_email(config, 'Sub', 'Body')
    sent = instance.send_message.call_args[0][0]
    assert sent['To'] == 'single@example.com'
