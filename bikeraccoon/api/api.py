#!/usr/bin/env python3

from flask_cors import CORS
from flask import (Flask, request, make_response, g,
                   send_from_directory, render_template, jsonify)

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
    return render_template("frontpage.html")


@app.route('/tests')
def tests():
    return render_template("tests.html")


@app.route('/admin', methods=['GET', 'POST'])
def admin():
    new_key = None
    if request.method == 'POST':
        name = request.form.get('name', '').strip()
        email = request.form.get('email', '').strip() or None
        description = request.form.get('description', '').strip() or None
        if name:
            new_key = apidb.create_key(BR_DB_PATH, name=name, email=email, description=description)
    keys = apidb.get_keys_with_stats(BR_DB_PATH)
    recent = apidb.get_recent_requests(BR_DB_PATH)
    return render_template("admin.html", keys=keys, recent=recent, new_key=new_key)


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

    res = get_trips(t1, t2, sys_name, feed_type, station_id, vehicle_type_id, frequency)
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
    return jsonify({'active': active, 'systems': systems})


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
