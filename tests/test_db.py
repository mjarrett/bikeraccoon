"""Tests for bikeraccoon.api.db — SQLite API key management."""
import pytest
import bikeraccoon.api.db as db


@pytest.fixture
def tmp_db(tmp_path):
    path = str(tmp_path / 'test.db')
    db.init_db(path)
    return path


# ── create_key / lookup_key ───────────────────────────────────────────────────

def test_create_and_lookup_key(tmp_db):
    key = db.create_key(tmp_db, name='Alice')
    row = db.lookup_key(tmp_db, key)
    assert row is not None
    assert row['name'] == 'Alice'
    assert row['active'] == 1


def test_lookup_unknown_key_returns_none(tmp_db):
    assert db.lookup_key(tmp_db, 'doesnotexist') is None


def test_create_key_stores_metadata(tmp_db):
    key = db.create_key(tmp_db, name='Bob', email='b@example.com', description='research')
    row = db.lookup_key(tmp_db, key)
    assert row['email'] == 'b@example.com'
    assert row['description'] == 'research'


def test_create_key_optional_fields_default_to_none(tmp_db):
    key = db.create_key(tmp_db, name='Carol')
    row = db.lookup_key(tmp_db, key)
    assert row['email'] is None
    assert row['description'] is None


def test_keys_are_unique(tmp_db):
    k1 = db.create_key(tmp_db, name='Dave')
    k2 = db.create_key(tmp_db, name='Eve')
    assert k1 != k2


# ── deactivate_key ────────────────────────────────────────────────────────────

def test_deactivated_key_not_found(tmp_db):
    key = db.create_key(tmp_db, name='Frank')
    row = db.lookup_key(tmp_db, key)
    db.deactivate_key(tmp_db, row['id'])
    assert db.lookup_key(tmp_db, key) is None


def test_deactivate_does_not_remove_row(tmp_db):
    """Deactivated keys should still appear in get_keys_with_stats."""
    key = db.create_key(tmp_db, name='Grace')
    row = db.lookup_key(tmp_db, key)
    db.deactivate_key(tmp_db, row['id'])
    stats = db.get_keys_with_stats(tmp_db)
    assert len(stats) == 1
    assert stats[0]['active'] == 0


# ── log_request / get_recent_requests ────────────────────────────────────────

def test_log_request_appears_in_recent(tmp_db):
    key = db.create_key(tmp_db, name='Hal')
    row = db.lookup_key(tmp_db, key)
    db.log_request(tmp_db, key_id=row['id'], endpoint='/activity',
                   query_str='system=test', status_code=200, response_ms=42)
    recent = db.get_recent_requests(tmp_db)
    assert len(recent) == 1
    assert recent[0]['endpoint'] == '/activity'
    assert recent[0]['status_code'] == 200
    assert recent[0]['response_ms'] == 42
    assert recent[0]['name'] == 'Hal'


def test_get_recent_requests_respects_limit(tmp_db):
    key = db.create_key(tmp_db, name='Ivy')
    row = db.lookup_key(tmp_db, key)
    for i in range(5):
        db.log_request(tmp_db, key_id=row['id'], endpoint='/activity',
                       query_str='', status_code=200, response_ms=i)
    assert len(db.get_recent_requests(tmp_db, limit=3)) == 3


def test_get_recent_requests_ordered_newest_first(tmp_db):
    key = db.create_key(tmp_db, name='Jack')
    row = db.lookup_key(tmp_db, key)
    for status in [200, 404, 500]:
        db.log_request(tmp_db, key_id=row['id'], endpoint='/activity',
                       query_str='', status_code=status, response_ms=1)
    recent = db.get_recent_requests(tmp_db)
    assert recent[0]['status_code'] == 500


# ── get_keys_with_stats ───────────────────────────────────────────────────────

def test_get_keys_with_stats_no_requests(tmp_db):
    db.create_key(tmp_db, name='Kim')
    stats = db.get_keys_with_stats(tmp_db)
    assert len(stats) == 1
    assert stats[0]['total_requests'] == 0
    assert stats[0]['last_seen'] is None


def test_get_keys_with_stats_counts_requests(tmp_db):
    key = db.create_key(tmp_db, name='Lee')
    row = db.lookup_key(tmp_db, key)
    for _ in range(3):
        db.log_request(tmp_db, key_id=row['id'], endpoint='/activity',
                       query_str='', status_code=200, response_ms=10)
    stats = db.get_keys_with_stats(tmp_db)
    assert stats[0]['total_requests'] == 3
    assert stats[0]['last_seen'] is not None


def test_get_keys_with_stats_recent_requests_counted(tmp_db):
    key = db.create_key(tmp_db, name='Mia')
    row = db.lookup_key(tmp_db, key)
    db.log_request(tmp_db, key_id=row['id'], endpoint='/activity',
                   query_str='', status_code=200, response_ms=5)
    stats = db.get_keys_with_stats(tmp_db)
    assert stats[0]['requests_7d'] == 1


def test_get_keys_with_stats_multiple_keys(tmp_db):
    for name in ['Nan', 'Owen', 'Pat']:
        db.create_key(tmp_db, name=name)
    stats = db.get_keys_with_stats(tmp_db)
    assert len(stats) == 3
