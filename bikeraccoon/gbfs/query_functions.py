import pandas as pd
import json
import requests
import datetime as dt
import timeout_decorator
import ssl
from time import sleep

_REQUEST_TIMEOUT = 15


def check_gbfs_url(sys_url):
    try:
        requests.get(sys_url, timeout=_REQUEST_TIMEOUT).json()['data']
        return True
    except:
        return False


def _lookup_feed(sys_url, feed_name):
    r = requests.get(sys_url, timeout=_REQUEST_TIMEOUT)
    r.raise_for_status()
    data = r.json()
    feeds = data['data']['en']['feeds']
    matches = [x for x in feeds if x['name'] == feed_name]
    if not matches:
        raise ValueError(f"'{feed_name}' feed not found in GBFS autodiscovery at {sys_url}")
    return matches[0]['url']


def get_station_status_url(sys_url):
    return _lookup_feed(sys_url, 'station_status')


def get_station_info_url(sys_url):
    return _lookup_feed(sys_url, 'station_information')


def get_system_info_url(sys_url):
    return _lookup_feed(sys_url, 'system_information')


def get_free_bike_url(sys_url):
    return _lookup_feed(sys_url, 'free_bike_status')


def get_vehicle_url(sys_url):
    return _lookup_feed(sys_url, 'vehicle_status')


def get_vehicle_types_url(sys_url):
    return _lookup_feed(sys_url, 'vehicle_types')


@timeout_decorator.timeout(30)
def query_system_info(sys_url):
    url = get_system_info_url(sys_url)
    r = requests.get(url, timeout=_REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json()


@timeout_decorator.timeout(30)
def query_vehicle_types(sys_url):
    """
    Query vehicle_types.json
    """

    url = get_vehicle_types_url(sys_url)
    r = requests.get(url, timeout=_REQUEST_TIMEOUT)
    r.raise_for_status()
    data = r.json()

    df = pd.DataFrame(data['data']['vehicle_types'])

    try:
        df['datetime'] = data['last_updated']
        df['datetime'] = df['datetime'].map(lambda x: dt.datetime.utcfromtimestamp(x))
    except KeyError:
        df['datetime'] = dt.datetime.utcnow()
    df['datetime'] = df['datetime'].dt.tz_localize('UTC')

    df = df[['vehicle_type_id', 'form_factor', 'propulsion_type']]

    return df


@timeout_decorator.timeout(30)
def query_station_status(sys_url):
    """
    Query station_status.json
    """
    # Helper function for vehicle types
    def f(x):
        if 'vehicle_types_available' not in x.keys():
            return x

        res = []
        for vehicle_type in x['vehicle_types_available']:
            res.append({'station_id': x['station_id'],
                        'vehicle_type_id': vehicle_type['vehicle_type_id'],
                        'num_bikes_available': vehicle_type['count'],
                       'last_reported': x['last_reported'],
                        'is_renting': x['is_renting']
                        })
        return res

    url = get_station_status_url(sys_url)

    r = requests.get(url, timeout=_REQUEST_TIMEOUT)
    r.raise_for_status()
    data = r.json()

    # if data returns string, it might be an error message. wait 2 seconds and try again
    # this was added to handle HOPR rate limit
    if isinstance(data, str):
        sleep(2)
        r = requests.get(url, timeout=_REQUEST_TIMEOUT)
        r.raise_for_status()
        data = r.json()

    data = [f(x) for x in data['data']['stations']]  # Reformat if vehicle types are present
    data = [y if 'vehicle_type_id' in y else x for x in data for y in x]  # flatten list
    df = pd.DataFrame(data)

    if 'vehicle_type_id' not in df.columns:
        df['vehicle_type_id'] = ""

    df = df.drop_duplicates(['station_id', 'last_reported', 'vehicle_type_id'])
    try:
        df['datetime'] = df['last_updated']
        df['datetime'] = df['datetime'].map(lambda x: dt.datetime.utcfromtimestamp(x))
    except KeyError:
        df['datetime'] = dt.datetime.utcnow()

    df['datetime'] = df['datetime'].dt.tz_localize('UTC')

    df = df[['datetime', 'num_bikes_available', 'is_renting', 'station_id', 'vehicle_type_id']]

    return df


@timeout_decorator.timeout(30)
def query_free_bike_status(sys_url):
    """
    Query station status if vehicle types are specified.

    If 'vehicle_types_available' attribute not present, raise exception
    """

    try:
        url = get_free_bike_url(sys_url)
        gbfs_ver = 2
    except ValueError:
        try:
            url = get_vehicle_url(sys_url)
            gbfs_ver = 3
        except ValueError:
            raise ValueError(f"Free bikes JSON feed not available at {sys_url}")

    if gbfs_ver == 2:
        bikes_slug = 'bikes'
        bike_id_slug = 'bike_id'

    elif gbfs_ver == 3:
        bikes_slug = 'vehicles'
        bike_id_slug = 'vehicle_id'

    r = requests.get(url, timeout=_REQUEST_TIMEOUT)
    r.raise_for_status()
    data = r.json()

    try:
        df = pd.DataFrame(data['data'][bikes_slug])
    except KeyError:
        df = pd.DataFrame(data[bikes_slug])

    if not data['data'][bikes_slug] or 'vehicle_type_id' not in data['data'][bikes_slug][0]:
        df['vehicle_type_id'] = ""

    if 'lat' not in df.columns or 'lon' not in df.columns:
        df['lat'] = 0
        df['lon'] = 0

    if 'station_id' not in df.columns:
        df['station_id'] = ""

    # df = df.groupby(['station_id','vehicle_type_id']).agg({bike_id_slug:'count'}).reset_index()
    df = df.groupby(['station_id', 'vehicle_type_id', 'lat', 'lon'],
                    dropna=False).agg({bike_id_slug: 'count'}).reset_index()
    df = df.rename(columns={bike_id_slug: 'num_bikes_available'})

    try:
        df['datetime'] = data['last_updated']
        df['datetime'] = df['datetime'].map(lambda x: dt.datetime.fromtimestamp(x, dt.UTC))
    except KeyError:
        df['datetime'] = dt.datetime.utcnow()

    df = df.reset_index()
    df = df[['station_id', 'vehicle_type_id', 'datetime', 'num_bikes_available', 'lat', 'lon']]
    df['num_bikes_available'] = df['num_bikes_available'].fillna(0).astype(int)

    df['is_renting'] = True

    return df


@timeout_decorator.timeout(30)
def query_station_info(sys_url):
    """
    Query station_information.json
    """
    url = get_station_info_url(sys_url)

    r = requests.get(url, timeout=_REQUEST_TIMEOUT)
    r.raise_for_status()
    data = r.json()

    try:
        df = pd.DataFrame(data['data']['stations'])
    except KeyError:
        df = pd.DataFrame(data['stations'])
    return df[['name', 'station_id', 'lat', 'lon']]
