"""Tests for Flask auth middleware and admin panel system actions."""
import base64
import json
import pytest
import bikeraccoon.api.db as db
import bikeraccoon.api.api as api_module


def _b64(user, password):
    return base64.b64encode(f'{user}:{password}'.encode()).decode()


@pytest.fixture
def env(monkeypatch, tmp_path):
    db_path = str(tmp_path / 'test.db')
    systems_file = tmp_path / 'systems.json'
    systems_file.write_text('[]')

    db.init_db(db_path)
    monkeypatch.setattr(api_module, 'BR_DB_PATH', db_path)
    monkeypatch.setattr(api_module, 'BR_ADMIN_KEY', 'secret')
    monkeypatch.setattr(api_module, 'BR_SYSTEMS_FILE', str(systems_file))

    return {'db': db_path, 'systems': str(systems_file)}


@pytest.fixture
def client(env):
    with api_module.app.test_client() as c:
        yield c, env


@pytest.fixture
def admin(client):
    c, env = client
    headers = {'Authorization': f'Basic {_b64("admin", "secret")}'}
    return c, env, headers


def _systems(env):
    return json.loads(open(env['systems']).read())


# ── Authentication ────────────────────────────────────────────────────────────

@pytest.mark.parametrize('path', ['/', '/status', '/systems'])
def test_public_paths_need_no_auth(client, path):
    c, _ = client
    resp = c.get(path)
    assert resp.status_code != 401


def test_activity_without_key_returns_401(client):
    c, _ = client
    assert c.get('/activity?system=test').status_code == 401


def test_activity_with_invalid_key_returns_401(client):
    c, _ = client
    assert c.get('/activity?system=test&key=badkey').status_code == 401


def test_admin_without_auth_returns_401(client):
    c, _ = client
    assert c.get('/admin').status_code == 401


def test_admin_wrong_password_returns_401(client):
    c, _ = client
    resp = c.get('/admin', headers={'Authorization': f'Basic {_b64("admin", "wrong")}'})
    assert resp.status_code == 401


def test_admin_correct_password_returns_200(client):
    c, _ = client
    resp = c.get('/admin', headers={'Authorization': f'Basic {_b64("admin", "secret")}'})
    assert resp.status_code == 200


# ── add_system ────────────────────────────────────────────────────────────────

def test_add_system_redirects_on_success(admin):
    c, env, headers = admin
    resp = c.post('/admin', headers=headers, data={
        '_action': 'add_system', 'name': 'test_city',
        'url': 'https://example.com/gbfs/', 'tz': 'America/Toronto',
    })
    assert resp.status_code == 302


def test_add_system_written_to_systems_file(admin):
    c, env, headers = admin
    c.post('/admin', headers=headers, data={
        '_action': 'add_system', 'name': 'test_city',
        'url': 'https://example.com/gbfs/', 'tz': 'America/Toronto',
    })
    systems = _systems(env)
    assert len(systems) == 1
    assert systems[0]['name'] == 'test_city'
    assert systems[0]['tz'] == 'America/Toronto'
    assert systems[0]['url'] == 'https://example.com/gbfs/'


def test_add_system_optional_fields(admin):
    c, env, headers = admin
    c.post('/admin', headers=headers, data={
        '_action': 'add_system', 'name': 'test_city',
        'url': 'https://example.com/', 'tz': 'UTC',
        'brand': 'Test Bikes', 'city': 'Testville',
        'province': 'ON', 'country': 'CA',
        'gbfs_system_id': 'test_bikes',
    })
    s = _systems(env)[0]
    assert s['brand'] == 'Test Bikes'
    assert s['city'] == 'Testville'
    assert s['province'] == 'ON'
    assert s['country'] == 'CA'
    assert s['gbfs_system_id'] == 'test_bikes'


def test_add_system_tracking_defaults_off_when_unchecked(admin):
    c, env, headers = admin
    c.post('/admin', headers=headers, data={
        '_action': 'add_system', 'name': 'test_city',
        'url': 'https://example.com/', 'tz': 'UTC',
        # tracking checkboxes not submitted
    })
    s = _systems(env)[0]
    assert s['tracking'] is False
    assert s['track_stations'] is False
    assert s['track_free_bikes'] is False


def test_add_system_tracking_on_when_checked(admin):
    c, env, headers = admin
    c.post('/admin', headers=headers, data={
        '_action': 'add_system', 'name': 'test_city',
        'url': 'https://example.com/', 'tz': 'UTC',
        'tracking': 'on', 'track_stations': 'on', 'track_free_bikes': 'on',
    })
    s = _systems(env)[0]
    assert s['tracking'] is True
    assert s['track_stations'] is True
    assert s['track_free_bikes'] is True


def test_add_system_duplicate_name_shows_error(admin):
    c, env, headers = admin
    data = {'_action': 'add_system', 'name': 'dupe', 'url': 'https://x.com/', 'tz': 'UTC'}
    c.post('/admin', headers=headers, data=data)
    resp = c.post('/admin', headers=headers, data=data)
    assert resp.status_code == 200
    assert b'already exists' in resp.data
    assert len(_systems(env)) == 1


def test_add_system_missing_name_shows_error(admin):
    c, env, headers = admin
    resp = c.post('/admin', headers=headers, data={
        '_action': 'add_system', 'url': 'https://x.com/', 'tz': 'UTC',
    })
    assert resp.status_code == 200
    assert b'required' in resp.data
    assert len(_systems(env)) == 0


def test_add_system_missing_url_shows_error(admin):
    c, env, headers = admin
    resp = c.post('/admin', headers=headers, data={
        '_action': 'add_system', 'name': 'test', 'tz': 'UTC',
    })
    assert resp.status_code == 200
    assert b'required' in resp.data


def test_add_system_missing_tz_shows_error(admin):
    c, env, headers = admin
    resp = c.post('/admin', headers=headers, data={
        '_action': 'add_system', 'name': 'test', 'url': 'https://x.com/',
    })
    assert resp.status_code == 200
    assert b'required' in resp.data


# ── delete_system ─────────────────────────────────────────────────────────────

def test_delete_system_removes_entry(admin):
    c, env, headers = admin
    c.post('/admin', headers=headers, data={
        '_action': 'add_system', 'name': 'to_delete',
        'url': 'https://x.com/', 'tz': 'UTC',
    })
    c.post('/admin', headers=headers, data={
        '_action': 'delete_system', '_system': 'to_delete',
    })
    assert len(_systems(env)) == 0


def test_delete_system_only_removes_named_system(admin):
    c, env, headers = admin
    for name in ['keep_me', 'delete_me']:
        c.post('/admin', headers=headers, data={
            '_action': 'add_system', 'name': name,
            'url': 'https://x.com/', 'tz': 'UTC',
        })
    c.post('/admin', headers=headers, data={
        '_action': 'delete_system', '_system': 'delete_me',
    })
    systems = _systems(env)
    assert len(systems) == 1
    assert systems[0]['name'] == 'keep_me'


def test_delete_nonexistent_system_is_safe(admin):
    c, env, headers = admin
    resp = c.post('/admin', headers=headers, data={
        '_action': 'delete_system', '_system': 'ghost',
    })
    assert resp.status_code == 302


# ── toggle_system ─────────────────────────────────────────────────────────────

def test_toggle_system_enables_disabled(admin):
    c, env, headers = admin
    c.post('/admin', headers=headers, data={
        '_action': 'add_system', 'name': 'toggleme',
        'url': 'https://x.com/', 'tz': 'UTC',
        # tracking not checked → False
    })
    c.post('/admin', headers=headers, data={
        '_action': 'toggle_system', '_system': 'toggleme',
    })
    assert _systems(env)[0]['tracking'] is True


def test_toggle_system_disables_enabled(admin):
    c, env, headers = admin
    c.post('/admin', headers=headers, data={
        '_action': 'add_system', 'name': 'toggleme',
        'url': 'https://x.com/', 'tz': 'UTC', 'tracking': 'on',
    })
    c.post('/admin', headers=headers, data={
        '_action': 'toggle_system', '_system': 'toggleme',
    })
    assert _systems(env)[0]['tracking'] is False


# ── edit_system ───────────────────────────────────────────────────────────────

def test_edit_system_updates_fields(admin):
    c, env, headers = admin
    c.post('/admin', headers=headers, data={
        '_action': 'add_system', 'name': 'editable',
        'url': 'https://old.com/', 'tz': 'UTC',
    })
    c.post('/admin', headers=headers, data={
        '_action': 'edit_system', '_system': 'editable',
        'url': 'https://new.com/', 'tz': 'America/Vancouver',
        'brand': 'New Brand', 'city': 'New City',
        'tracking': 'on', 'track_stations': 'on', 'track_free_bikes': 'on',
    })
    s = _systems(env)[0]
    assert s['url'] == 'https://new.com/'
    assert s['tz'] == 'America/Vancouver'
    assert s['brand'] == 'New Brand'
    assert s['city'] == 'New City'
    assert s['tracking'] is True


def test_edit_system_clears_optional_fields(admin):
    c, env, headers = admin
    c.post('/admin', headers=headers, data={
        '_action': 'add_system', 'name': 'editable',
        'url': 'https://x.com/', 'tz': 'UTC', 'brand': 'Old Brand',
    })
    c.post('/admin', headers=headers, data={
        '_action': 'edit_system', '_system': 'editable',
        'url': 'https://x.com/', 'tz': 'UTC',
        'brand': '',  # cleared
    })
    assert _systems(env)[0]['brand'] is None


# ── generate API key ──────────────────────────────────────────────────────────

def test_generate_api_key_shown_in_response(admin):
    c, env, headers = admin
    resp = c.post('/admin', headers=headers, data={
        'name': 'Test User', 'email': 'test@example.com',
    })
    assert resp.status_code == 200
    assert b'Test User' in resp.data


def test_generate_api_key_stored_in_db(admin):
    c, env, headers = admin
    c.post('/admin', headers=headers, data={'name': 'Test User'})
    keys = db.get_keys_with_stats(env['db'])
    assert len(keys) == 1
    assert keys[0]['name'] == 'Test User'
