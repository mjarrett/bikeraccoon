from flask import Flask, request, make_response, send_from_directory, jsonify
from flask.json.provider import DefaultJSONProvider

import json
import datetime as dt
from zoneinfo import ZoneInfo
import pathlib

import threading
import time
import duckdb
from importlib.metadata import version as _get_version

_con = duckdb.connect()
_con_lock = threading.Lock()

_cache = {}
_CACHE_TTL = 60  # seconds

version = _get_version("bikeraccoon")


def _cache_get(key):
    entry = _cache.get(key)
    if entry and time.monotonic() - entry['ts'] < _CACHE_TTL:
        return entry['value'], True
    return None, False


def _cache_set(key, value):
    _cache[key] = {'value': value, 'ts': time.monotonic()}


class BRJSONProvider(DefaultJSONProvider):
    """
    By default Flask jsonify outputs datetimes as GMT.
    This overrides and outputs as TZ aware datetime strings.
    """

    def default(self, obj):
        if isinstance(obj, dt.datetime):
            return obj.isoformat()  # Customize JSON representation
        return super().default(obj)


def get_system_tz(sys_name):
    key = f'tz:{sys_name}'
    value, hit = _cache_get(key)
    if hit:
        return value
    with _con_lock:
        value = _con.execute(f'''select tz from './tracker-data/{sys_name}/system.parquet' ''').fetchall()[0][0]
    _cache_set(key, value)
    return value


def get_data_path(sys_name, feed_type, vehicle_type, freq):
    vehicle_type = 'all' if vehicle_type is None else vehicle_type

    if freq in ['h', 't']:
        return f'./tracker-data/{sys_name}/trips.{feed_type}.hourly/year=*/month=*/*.parquet'
    elif freq in ['d', 'm', 'y']:
        return f'./tracker-data/{sys_name}/trips.{feed_type}.daily/year=*/month=*/*.parquet'


def api_response(f):
    def api_func(*args, **kwargs):
        start = dt.datetime.now()
        try:
            res = f(*args, **kwargs)
        except Exception as e:
            return return_api_error(e)
        t = dt.datetime.now() - start
        res = {'data': res, 'query_time': str(t),
               'version': version}
        return jsonify(res)
    api_func.__name__ = f.__name__
    return api_func


@api_response
def get_trips(t1, t2, sys_name, feed_type, station_id, vehicle_type_id, frequency, tz=None):
    data_path = get_data_path(sys_name, feed_type, vehicle_type_id, frequency)

    if frequency == 't':
        dt_select = "FIRST(datetime)"
        groupby = ""
        where = f"datetime BETWEEN '{t1}' and '{t2}'"
        orderby = ""
    else:
        dt_select = f"date_trunc('{frequency}',datetime)"
        groupby = f"date_trunc('{frequency}',datetime)"
        where = f"datetime BETWEEN '{t1}' and '{t2}'"
        orderby = f"ORDER BY date_trunc('{frequency}',datetime)"

    trips_sum  = "SUM(CASE WHEN COALESCE(is_renting, true) THEN trips ELSE 0 END)"
    returns_sum = "SUM(CASE WHEN COALESCE(is_returning, true) THEN returns ELSE 0 END)"
    select      = f"{dt_select},{trips_sum},{returns_sum}"
    select_base = f"{dt_select},SUM(trips),SUM(returns)"

    vehicle_select = "null"
    vehicle_groupby = ""
    vehicle_where = ""
    station_select = "null"
    station_groupby = ""
    station_where = ""
    if vehicle_type_id == "all":
        vehicle_select = "vehicle_type_id"
        vehicle_groupby = "vehicle_type_id"
    elif vehicle_type_id not in [None, "all"]:
        vehicle_select = "vehicle_type_id"
        vehicle_groupby = "vehicle_type_id"
        vehicle_where = f"vehicle_type_id = '{vehicle_type_id}'"

    if station_id == "all":
        station_select = "station_id"
        station_groupby = "station_id"
    elif station_id not in [None, "all"]:
        station_select = "station_id"
        station_groupby = "station_id"
        station_where = f"station_id = '{station_id}'"

    partition_where = (f"(year * 12 + month) BETWEEN "
                       f"({t1.year} * 12 + {t1.month}) AND ({t2.year} * 12 + {t2.month})")

    select      = ",".join(x for x in [station_select, vehicle_select, select] if x != "")
    select_base = ",".join(x for x in [station_select, vehicle_select, select_base] if x != "")
    where       = " AND ".join(x for x in [station_where, vehicle_where, partition_where, where] if x != "")
    groupby     = ",".join(x for x in [station_groupby, vehicle_groupby, groupby] if x != "")

    if tz is None:
        tz = get_system_tz(sys_name)

    def _build_query(sel):
        return f'''
               SET TIMEZONE='{tz}';
               SELECT {sel}
               FROM read_parquet('{data_path}', hive_partitioning=true)
               WHERE {where}
               {"GROUP BY" if groupby != "" else ""} {groupby}
               {orderby}
               '''

    try:
        with _con_lock:
            qry = _con.execute(_build_query(select))
    except duckdb.BinderException:
        # Flag columns absent in old parquet files — retry without flag-aware expressions
        try:
            with _con_lock:
                qry = _con.execute(_build_query(select_base))
        except duckdb.IOException:
            return []
    except duckdb.IOException:
        return []
    # -- Convert to dict
    res = [{k: v for k, v in zip(['station_id', 'vehicle_type_id', 'datetime', 'trips', 'returns'], x)}
           for x in qry.fetchall()]

    return res


def string_to_datetime(t, tz):
    y = int(t[:4])
    m = int(t[4:6])
    d = int(t[6:8])
    h = int(t[8:10])
    return dt.datetime(y, m, d, h, tzinfo=ZoneInfo(tz))


def return_api_error(text=""):

    content = f"Invalid API request :( \n{text}"
    return content, 400


def get_systems_info():
    datapath = pathlib.Path('./tracker-data/')

    res = []
    for sys_name in [x.name for x in datapath.glob('*')]:
        try:
            res.append(get_system_info(sys_name))
        except:
            pass
    # qry = duckdb.query(f"select * from './tracker-data/*/system.parquet' ")
    return res


def get_system_info(sys_name):
    key = f'sysinfo:{sys_name}'
    value, hit = _cache_get(key)
    if hit:
        return value
    tz = get_system_tz(sys_name)
    with _con_lock:
        qry = _con.execute(f"set timezone='{tz}'; select * from './tracker-data/{sys_name}/system.parquet' ")
    value = qry.fetchdf().to_dict('records')[0]
    _cache_set(key, value)
    return value
