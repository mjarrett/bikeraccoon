#!/usr/bin/env python3

from flask_cors import CORS
from flask import (Flask, request, make_response, g,
                   send_from_directory, render_template, jsonify, redirect)

import json
import hashlib
import sqlite3
import pytz
import datetime as dt
import itertools
import os
import pathlib
import time
import requests
from urllib.parse import urlencode, parse_qsl
import pyarrow.parquet as pq

from .api_functions import *
from . import db as apidb

from .. import gbfs


app = Flask(__name__, template_folder='../templates/')

app.json_provider_class = BRJSONProvider
app.json = BRJSONProvider(app)
app.json.compact = False

CORS(app)  # Prevents CORS errors

BR_DB_PATH = os.environ.get('BR_DB_PATH', './api.db')
BR_ADMIN_KEY = os.environ.get('BR_ADMIN_KEY', '')
BR_ENV = os.environ.get('BR_ENV', '')
BR_SYSTEMS_FILE = os.environ.get('BR_SYSTEMS_FILE', './systems.json')


def _load_systems():
    try:
        with open(BR_SYSTEMS_FILE) as f:
            return json.load(f)
    except Exception:
        return []


def _save_systems(systems):
    with open(BR_SYSTEMS_FILE, 'w') as f:
        json.dump(systems, f, indent=4)


apidb.init_db(BR_DB_PATH)

NO_AUTH_PATHS = {'/', '/status', '/favicon.ico', '/tests', '/systems', '/stations', '/vehicles'}


@app.before_request
def authenticate():
    if request.path in NO_AUTH_PATHS:
        g.key_id = None
        return

    if request.path.startswith('/admin'):
        auth = request.authorization
        if not BR_ADMIN_KEY or not auth or auth.password != BR_ADMIN_KEY:
            return make_response('Unauthorized', 401,
                                 {'WWW-Authenticate': 'Basic realm="BikeRaccoon Admin"'})
        g.key_id = None
        return

    key_str = request.args.get('key')
    if not key_str:
        return jsonify({'error': 'Missing key parameter'}), 401

    key_row = apidb.lookup_key(BR_DB_PATH, key_str)
    if key_row is None:
        return jsonify({'error': 'Invalid or inactive API key'}), 401

    g.key_id = key_row['id']
    g.start_time = time.monotonic()


@app.after_request
def log_request(response):
    key_id = getattr(g, 'key_id', None)
    if key_id is None:
        return response
    response_ms = int((time.monotonic() - g.start_time) * 1000)
    qs = {k: v for k, v in parse_qsl(request.query_string.decode()) if k != 'key'}
    apidb.log_request(
        BR_DB_PATH,
        key_id=key_id,
        endpoint=request.path,
        query_str=urlencode(qs),
        status_code=response.status_code,
        response_ms=response_ms,
    )
    return response


@app.route('/favicon.ico')
def favicon():
    return send_from_directory(os.path.join(app.root_path, 'static'),
                               'favicon.ico', mimetype='image/vnd.microsoft.icon')


@app.route('/')
def default():
    return render_template("frontpage.html", env=BR_ENV)


@app.route('/tests')
def tests():
    return render_template("tests.html")


@app.route('/admin', methods=['GET', 'POST'])
def admin():
    new_key = None
    if request.method == 'POST':
        action = request.form.get('_action')

        if action == 'toggle_system':
            sys_name = request.form.get('_system', '')
            systems = _load_systems()
            for s in systems:
                if s['name'] == sys_name:
                    s['tracking'] = not s.get('tracking', False)
                    break
            _save_systems(systems)
            return redirect('/admin')

        elif action == 'edit_system':
            sys_name = request.form.get('_system', '')
            systems = _load_systems()
            for s in systems:
                if s['name'] == sys_name:
                    tz = request.form.get('tz', '').strip()
                    if tz:
                        s['tz'] = tz
                    url = request.form.get('url', '').strip()
                    if url:
                        s['url'] = url
                    s['brand'] = request.form.get('brand', '').strip() or None
                    s['city'] = request.form.get('city', '').strip() or None
                    s['tracking'] = request.form.get('tracking') == 'on'
                    s['track_stations'] = request.form.get('track_stations') == 'on'
                    s['track_free_bikes'] = request.form.get('track_free_bikes') == 'on'
                    break
            _save_systems(systems)
            return redirect('/admin')

        elif action == 'delete_system':
            sys_name = request.form.get('_system', '')
            systems = _load_systems()
            systems = [s for s in systems if s['name'] != sys_name]
            _save_systems(systems)
            return redirect('/admin')

        elif action == 'add_system':
            name = request.form.get('name', '').strip()
            url = request.form.get('url', '').strip()
            tz = request.form.get('tz', '').strip()
            add_error = None
            if not name or not url or not tz:
                add_error = 'Name, GBFS URL, and timezone are required.'
            else:
                systems = _load_systems()
                if any(s['name'] == name for s in systems):
                    add_error = f'A system named "{name}" already exists.'
                else:
                    new_system = {
                        'name': name,
                        'url': url,
                        'tz': tz,
                        'tracking': request.form.get('tracking') == 'on',
                        'track_stations': request.form.get('track_stations') == 'on',
                        'track_free_bikes': request.form.get('track_free_bikes') == 'on',
                    }
                    for field in ('brand', 'city', 'province', 'country', 'gbfs_system_id'):
                        val = request.form.get(field, '').strip()
                        if val:
                            new_system[field] = val
                    systems.append(new_system)
                    _save_systems(systems)
                    return redirect('/admin')
            keys = apidb.get_keys_with_stats(BR_DB_PATH)
            recent = apidb.get_recent_requests(BR_DB_PATH)
            systems_data = _load_systems()
            return render_template("admin.html", keys=keys, recent=recent, new_key=None, env=BR_ENV,
                                   systems_data=systems_data, add_error=add_error, add_open=True)

        else:
            name = request.form.get('name', '').strip()
            email = request.form.get('email', '').strip() or None
            description = request.form.get('description', '').strip() or None
            if name:
                new_key = apidb.create_key(BR_DB_PATH, name=name, email=email, description=description)

    keys = apidb.get_keys_with_stats(BR_DB_PATH)
    recent = apidb.get_recent_requests(BR_DB_PATH)
    systems_data = _load_systems()
    return render_template("admin.html", keys=keys, recent=recent, new_key=new_key, env=BR_ENV,
                           systems_data=systems_data, add_error=None, add_open=False)


@app.route('/systems', methods=['GET'])
@api_response
def get_systems():
    sys_name = request.args.get('system', default=None, type=str)

    if sys_name is None:
        res = get_systems_info()
    else:
        res = get_system_info(sys_name)
    return res


@app.route('/stations', methods=['GET'])
@api_response
def get_stations():

    sys_name = request.args.get('system', default=None, type=str)

    if sys_name is None:
        return  # Add a 404

    table = pq.read_table(f'./tracker-data/{sys_name}/stations.parquet')
    res = table.to_pylist()

    return res


@app.route('/vehicles', methods=['GET'])
@api_response
def get_vehicles():
    sys_name = request.args.get('system', default=None, type=str)
    if sys_name is None:
        return  # Add a 404

    table = pq.read_table(f'./tracker-data/{sys_name}/vehicle_types.parquet')
    res = table.to_pylist()

    return res


@app.route('/activity', methods=['GET'])
def get_activity():
    sys_name = request.args.get('system', default=None, type=str)
    t1 = request.args.get('start', default=None, type=str)
    t2 = request.args.get('end', default=None, type=str)
    frequency = request.args.get('frequency', default='h', type=str)
    station_id = request.args.get('station', default=None, type=str)
    limit = request.args.get('limit', default=None, type=int)
    system = get_system_info(sys_name)
    vehicle_type_id = request.args.get('vehicle', default=None, type=str)
    feed_type = request.args.get('feed', default='station', type=str)

    tz = system['tz']
    try:
        t1 = string_to_datetime(t1, tz)
        t2 = string_to_datetime(t2, tz)
    except:

        return return_api_error()

    res = get_trips(t1, t2, sys_name, feed_type, station_id, vehicle_type_id, frequency, tz=tz)
    return res


@app.route('/status')
def get_status():
    stale_threshold = dt.timedelta(minutes=5)
    now = dt.datetime.now(dt.timezone.utc)
    systems = []
    for path in sorted(pathlib.Path('./tracker-data').glob('*/system.parquet')):
        try:
            row = {k: v[0] for k, v in pq.read_table(path).to_pydict().items()}
            latest = row.get('latest_update')
            if latest is not None and latest.tzinfo is None:
                latest = latest.replace(tzinfo=dt.timezone.utc)
            stale = latest is None or (now - latest) > stale_threshold
            systems.append({
                'name': row.get('name'),
                'last_update': latest.isoformat() if latest else None,
                'stale': stale,
            })
        except Exception:
            pass
    active = bool(systems) and any(not s['stale'] for s in systems)
    return jsonify({'active': active, 'env': BR_ENV, 'systems': systems})


@app.route('/gbfs', methods=['GET'])
def get_live_gbfs():
    sys_name = request.args.get('system', default=None, type=str)
    feed = request.args.get('feed', default=None, type=str)
    table = pq.read_table(f'./tracker-data/{sys_name}/system.parquet')
    sys_url = table.to_pylist()[0]['url']
    feed_url = [x for x in requests.get(sys_url).json()['data']['en']['feeds'] if x['name'] == feed][0]['url']

    data = requests.get(feed_url).json()
    return jsonify(data)


if __name__ == '__main__':
    app.run(debug=True, host='127.0.0.1', port=8001)
