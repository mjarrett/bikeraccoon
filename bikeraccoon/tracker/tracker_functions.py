import pandas as pd
import json
import pathlib

from .. import gbfs

import logging
from logging.handlers import TimedRotatingFileHandler

import datetime as dt
from zoneinfo import ZoneInfo
from collections import UserDict
import os
import sys
import duckdb

# -- Get logger
logger = logging.getLogger('Tracker')


class GBFSSystem(UserDict):

    def set_logger(self, log_path):

        # setup system-specific logger
        self.logger = setup_logger(self['name'], log_path=log_path, log_name=f"{self['name']}.log")

    def check_url(self):
        """If url is working, keep it. Otherwise check systems.csv"""
        if 'url' in self.keys() and gbfs.check_gbfs_url(self['url']):
            return
        self['url'] = self.get_gbfs_url()

    def get_gbfs_url(self):
        if 'gbfs_system_id' not in self.keys():
            return
        try:
            df = pd.read_csv('https://raw.githubusercontent.com/MobilityData/gbfs/refs/heads/master/systems.csv')
            system = df[df['System ID'] == self['gbfs_system_id']].to_dict('records')[0]
            new_url = system['Auto-Discovery URL']
            self.logger.info(f'Setting GBFS source URL: {new_url}')
            return new_url
        except Exception as e:
            self.logger.warning(f"Failed to get GBFS URL: {e}")
            return

    def get_system_time(self):
        return dt.datetime.now(ZoneInfo(self['tz']))

    def update_tracking_range(self):
        if 'tracking_start' not in self.keys():
            try:
                self['tracking_start'] = check_tracking_start(self)
            except:
                self['tracking_start'] = None

        try:
            self['tracking_end'] = check_tracking_end(self)
        except:
            self['tracking_end'] = None

        self['latest_update'] = self.get_system_time()

    def to_parquet(self):
        path = pathlib.Path(f"{self.data_path}/system.parquet")
        path.parent.mkdir(parents=True, exist_ok=True)
        df = pd.DataFrame([self])
        df.to_parquet(path, index=False)


def setup_logger(name, log_path, log_name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s')

    if log_path is not None:
        log_file = log_path.joinpath(log_name)
        handler = TimedRotatingFileHandler(log_file,
                                           when="d",
                                           interval=1,
                                           backupCount=14)

        handler.setFormatter(formatter)
        logger.addHandler(handler)

    streamhandler = logging.StreamHandler()
    streamhandler.setLevel(logging.INFO)
    streamhandler.setFormatter(formatter)
    logger.addHandler(streamhandler)
    return logger


def update_system_table(system):

    # Add anything here you want updated reqularly
    system.update_tracking_range()
    system.to_parquet()


def update_station_status_raw(system):
    """Returns (success, error_message)."""
    system.logger.info("Updating station bikes")
    ddf_file = f"{system.data_path}/raw.station.parquet"
    try:
        ddf = pd.read_parquet(ddf_file)
    except FileNotFoundError:
        ddf = None
    except Exception as e:
        system.logger.warning(f"Could not read {ddf_file}: {type(e).__name__}: {e}")
        ddf = None
    try:
        ddf_query = gbfs.query_station_status(system['url'])
        ddf_query['datetime'] = ddf_query['datetime'].dt.tz_convert(system['tz'])
        ddf = pd.concat([ddf, ddf_query])
    except Exception as e:
        system.logger.warning(f"gbfs query error, skipping stations_raw db update (url={system.get('url')}): {type(e).__name__}: {e}")
        return False, str(e)

    ddf.to_parquet(ddf_file, index=False)
    return True, None


def update_free_bike_status_raw(system):
    """Returns (success, error_message)."""
    system.logger.info("Updating free bikes")
    bdf_file = f"{system.data_path}/raw.free_bike.parquet"
    try:
        bdf = pd.read_parquet(bdf_file)
    except FileNotFoundError:
        bdf = None
    except Exception as e:
        system.logger.warning(f"Could not read {bdf_file}: {type(e).__name__}: {e}")
        bdf = None
    try:
        bdf_query = gbfs.query_free_bike_status(system['url'])
        bdf_query['datetime'] = bdf_query['datetime'].dt.tz_convert(system['tz'])
        bdf = pd.concat([bdf, bdf_query])
    except Exception as e:
        system.logger.warning(f"gbfs query error, skipping free_bikes_raw db update (url={system.get('url')}): {type(e).__name__}: {e}")
        return False, str(e)

    bdf.to_parquet(bdf_file, index=False)
    return True, None


def send_alert_email(smtp_config, subject, body, html_body=None):
    """Send an alert email via SMTP. If html_body is provided, sends multipart/alternative."""
    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart

    if html_body is not None:
        msg = MIMEMultipart('alternative')
        msg.attach(MIMEText(body, 'plain'))
        msg.attach(MIMEText(html_body, 'html'))
    else:
        msg = MIMEText(body, 'plain')

    msg['Subject'] = subject
    msg['From'] = smtp_config['from']
    to = smtp_config['to']
    msg['To'] = ', '.join(to) if isinstance(to, list) else to

    with smtplib.SMTP(smtp_config['host'], smtp_config.get('port', 587)) as server:
        if smtp_config.get('tls', True):
            server.starttls()
        if 'username' in smtp_config:
            server.login(smtp_config['username'], smtp_config['password'])
        server.send_message(msg)


def _fmt_dt(val):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "—"
    try:
        return pd.Timestamp(val).strftime("%Y-%m-%d %H:%M")
    except Exception:
        return str(val)


def _query_trip_summary(system_data_path, feed):
    glob = str(pathlib.Path(system_data_path) / f"trips.{feed}.hourly" / "year=*" / "month=*" / "*.parquet")
    try:
        rows = duckdb.query(f"""
            SELECT min(datetime), max(datetime), sum(trips), sum(returns)
            FROM read_parquet('{glob}', hive_partitioning=true)
        """).fetchall()
    except Exception:
        return None

    if not rows or rows[0][0] is None:
        return None

    first, last, total_trips, total_returns = rows[0]
    cutoff = pd.Timestamp(last) - pd.Timedelta(hours=24)
    try:
        rows24 = duckdb.query(f"""
            SELECT sum(trips), sum(returns)
            FROM read_parquet('{glob}', hive_partitioning=true)
            WHERE datetime >= '{cutoff}'
        """).fetchall()
        trips_24 = int(rows24[0][0]) if rows24 and rows24[0][0] is not None else 0
        returns_24 = int(rows24[0][1]) if rows24 and rows24[0][1] is not None else 0
    except Exception:
        trips_24 = returns_24 = 0

    return {
        "first":      _fmt_dt(first),
        "last":       _fmt_dt(last),
        "trips_24":   trips_24,
        "returns_24": returns_24,
    }


def _system_summary_data(system):
    """Extract summary data for a system as a dict."""
    name = system['name']
    tz = system.get('tz', '?')

    latest_upd = system.get('latest_update')
    if latest_upd is not None:
        try:
            age = dt.datetime.now(dt.timezone.utc) - pd.Timestamp(latest_upd).tz_convert('UTC')
            age_str = f"{int(age.total_seconds() // 60)}m ago"
            stale = age > dt.timedelta(hours=1)
        except Exception:
            age_str, stale = '?', False
    else:
        age_str, stale = '?', False

    try:
        sdf = pd.read_parquet(pathlib.Path(system.data_path) / "stations.parquet")
        n_active = int(sdf['active'].sum()) if 'active' in sdf.columns else '?'
        stations = f"{n_active} active / {len(sdf)} total"
    except Exception:
        stations = '—'

    feeds = {}
    for feed in ('station', 'free_bike'):
        feeds[feed] = _query_trip_summary(system.data_path, feed)

    return {
        'name': name,
        'tz': tz,
        'latest_upd': latest_upd,
        'age_str': age_str,
        'stale': stale,
        'tracking_start': system.get('tracking_start'),
        'tracking_end': system.get('tracking_end'),
        'stations': stations,
        'feeds': feeds,
    }


def build_system_summary(system):
    d = _system_summary_data(system)
    lines = []
    stale_flag = '  *** STALE ***' if d['stale'] else ''
    lines.append(f"{'='*50}")
    lines.append(f"  {d['name']}  [{d['tz']}]{stale_flag}")
    lines.append(f"{'='*50}")
    lines.append(f"  Last update : {_fmt_dt(d['latest_upd'])}  ({d['age_str']})")
    lines.append(f"  Data range  : {_fmt_dt(d['tracking_start'])}  →  {_fmt_dt(d['tracking_end'])}")
    lines.append(f"  Stations    : {d['stations']}")
    for feed in ('station', 'free_bike'):
        label = feed.replace('_', ' ').title()
        s = d['feeds'][feed]
        if s is None:
            lines.append(f"  {label:12s}: no data")
        else:
            lines.append(f"  {label:12s}: {s['first']} → {s['last']}")
            lines.append(f"  {'':12s}  last 24h — {s['trips_24']} trips, {s['returns_24']} returns")
    return '\n'.join(lines)


def build_system_summary_html(system):
    import html as _html
    d = _system_summary_data(system)

    header_bg = '#c0392b' if d['stale'] else '#2c3e50'
    stale_badge = ' <span style="background:#e74c3c;color:#fff;padding:2px 8px;border-radius:3px;font-size:0.8em;vertical-align:middle;">STALE</span>' if d['stale'] else ''

    rows = [
        ('Last update', f"{_fmt_dt(d['latest_upd'])}  ({d['age_str']})"),
        ('Data range', f"{_fmt_dt(d['tracking_start'])} &rarr; {_fmt_dt(d['tracking_end'])}"),
        ('Stations', _html.escape(d['stations'])),
    ]
    for feed in ('station', 'free_bike'):
        label = feed.replace('_', ' ').title()
        s = d['feeds'][feed]
        if s is None:
            rows.append((label, 'no data'))
        else:
            rows.append((label, f"{_fmt_dt(s['first'])} &rarr; {_fmt_dt(s['last'])}"))
            rows.append(('Last 24h', f"{s['trips_24']:,} trips, {s['returns_24']:,} returns"))

    table_rows = ''.join(
        f'<tr><td style="padding:5px 12px 5px 0;color:#666;white-space:nowrap;vertical-align:top;">{k}</td>'
        f'<td style="padding:5px 0;">{v}</td></tr>'
        for k, v in rows
    )

    return (
        f'<div style="margin-bottom:24px;border:1px solid #ddd;border-radius:6px;overflow:hidden;">'
        f'<div style="background:{header_bg};color:#fff;padding:10px 16px;font-size:1.05em;font-weight:bold;">'
        f'{_html.escape(d["name"])}'
        f'<span style="font-weight:normal;font-size:0.85em;margin-left:10px;opacity:0.8;">[{_html.escape(d["tz"])}]</span>'
        f'{stale_badge}</div>'
        f'<div style="padding:12px 16px;"><table style="border-collapse:collapse;font-size:0.95em;">{table_rows}</table></div>'
        f'</div>'
    )


def build_daily_summary(systems):
    import socket
    header = f"Tracker status — {dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  [{socket.gethostname()}]\n"
    body = '\n\n'.join(build_system_summary(s) for s in systems)
    return header + '\n' + body


def build_daily_summary_html(systems):
    import socket
    import html as _html
    hostname = _html.escape(socket.gethostname())
    timestamp = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    system_blocks = '\n'.join(build_system_summary_html(s) for s in systems)
    return f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"></head>
<body style="margin:0;padding:0;background:#f4f4f4;font-family:Arial,Helvetica,sans-serif;">
<div style="max-width:620px;margin:24px auto;background:#fff;border-radius:8px;box-shadow:0 1px 4px rgba(0,0,0,0.1);overflow:hidden;">
  <div style="background:#1a252f;color:#fff;padding:16px 24px;">
    <div style="font-size:1.2em;font-weight:bold;">bikeraccoon tracker status</div>
    <div style="font-size:0.85em;margin-top:4px;opacity:0.7;">{timestamp} &nbsp;&bull;&nbsp; {hostname}</div>
  </div>
  <div style="padding:20px 24px;">
    {system_blocks}
  </div>
</div>
</body>
</html>"""


def update_trips(system, feed_type, save_temp_data=False):
    """
    Pulls raw data from raw files, computes trips, saves trip data to file
    """

    system.logger.info(f"Updating tables: {feed_type}")

    if feed_type == 'station':
        # Compute hourly station trips, append to trips table

        try:
            ddf = pd.read_parquet(f"{system.data_path}/raw.station.parquet")
            thdf = make_station_trips(ddf)
        except Exception as e:
            system.logger.warning(f"Skipping station trips update (file={system.data_path}/raw.station.parquet): {type(e).__name__}: {e}")
            return

    elif feed_type == 'free_bike':
        # Compute hourly free bike trips, append to trips table
        try:
            bdf = pd.read_parquet(f"{system.data_path}/raw.free_bike.parquet")
            thdf = make_free_bike_trips(bdf)
        except Exception as e:
            system.logger.warning(f"Skipping free_bike trips update (file={system.data_path}/raw.free_bike.parquet): {type(e).__name__}: {e}")
            return

    year_tag = thdf['datetime'].iloc[0].strftime('%Y')
    # Add rows to measurements table
    try:
        thdf_historical = load_parquet(system, year_tag, feed_type)
    except FileNotFoundError:
        thdf_historical = None
    except Exception as e:
        system.logger.warning(f"Could not load historical {feed_type} parquet for {year_tag}: {type(e).__name__}: {e}")
        thdf_historical = None

    thdf = pd.concat([thdf_historical, thdf])
    thdf = thdf.groupby(['datetime', 'station_id', 'vehicle_type_id'], dropna=False).agg({
        'returns': 'sum',
        'trips': 'sum'})
    thdf = thdf.reset_index()

    # Drop records in raw tables except for most recent query
    try:
        trim_raw(f"{system.data_path}/raw.{feed_type}.parquet")
    except FileNotFoundError:
        pass

    # Save
    save_to_parquet(system, thdf, feed_type)


def load_parquet(system, year_tag, feed_type):
    outpath = pathlib.Path(f"{system.data_path}/")
    parquet_dir = outpath / f"trips.{feed_type}.hourly"
    return pd.read_parquet(parquet_dir, filters=[('year', '==', int(year_tag))])


def save_to_parquet(system, thdf, feed_type):

    outpath = pathlib.Path(f"{system.data_path}/")
    outpath.mkdir(parents=True, exist_ok=True)

    hourly = thdf.copy()
    hourly['year'] = hourly['datetime'].dt.year
    hourly['month'] = hourly['datetime'].dt.month
    hourly.to_parquet(outpath / f"trips.{feed_type}.hourly",
                      partition_cols=['year', 'month'], index=False,
                      existing_data_behavior='delete_matching')

    daily = thdf.set_index('datetime').groupby(
        [pd.Grouper(freq='d'), 'station_id', 'vehicle_type_id'], dropna=False).sum().reset_index()
    daily['year'] = daily['datetime'].dt.year
    daily['month'] = daily['datetime'].dt.month
    daily.to_parquet(outpath / f"trips.{feed_type}.daily",
                     partition_cols=['year', 'month'], index=False,
                     existing_data_behavior='delete_matching')


def trim_raw(fname):
    """
    Only keep the latest query, drop older queries
    """

    try:
        df = pd.read_parquet(fname)
    except FileNotFoundError:
        return

    df = df[df['datetime'] == df.iloc[-1].loc['datetime']]

    df.to_parquet(fname, index=False)


def make_station_trips(ddf):

    if len(ddf) == 0:
        return pd.DataFrame()

    pdf = pd.pivot_table(ddf, columns=['station_id', 'vehicle_type_id'],
                         index='datetime', values='num_bikes_available', dropna=False)
    df = pdf.copy()
    for col in pdf.columns:
        df[col] = pdf[col] - pdf[col].shift(-1)
    df = df.fillna(0.0).astype(int)

    df_stack = df.stack(future_stack=True).stack(future_stack=True).reset_index()
    df_stack.columns = ['datetime', 'vehicle_type_id', 'station_id', 'diff']

    # positive diff = bikes decreased = departure (trip)
    # negative diff = bikes increased = arrival (return)
    df_stack['trips'] = df_stack['diff'].clip(lower=0)
    df_stack['returns'] = (-df_stack['diff']).clip(lower=0)
    df_stack = df_stack.drop(columns='diff')

    df_stack = df_stack.set_index('datetime').groupby(
        [pd.Grouper(freq='h'), 'station_id', 'vehicle_type_id'], dropna=False).sum().reset_index()

    return df_stack


def make_free_bike_trips(bdf):
    """
    This handles both cases allowed by the GBFS spec: populated station_id, or populated lat/lon. In either case,
    the populated field is treated as a "station" and trips are measured as bikes come and go.
    """

    if len(bdf) == 0:
        return pd.DataFrame()

    bdf['latlon'] = list(zip(bdf['lat'], bdf['lon']))
    pivot_col = 'latlon' if len(set(bdf['latlon'])) > len(set(bdf['station_id'].fillna(0))) else 'station_id'

    pdf = pd.pivot_table(bdf, columns=[pivot_col, 'vehicle_type_id'],
                         index='datetime', values='num_bikes_available',
                         dropna=False)
    df = pdf.copy()
    for col in pdf.columns:
        df[col] = pdf[col] - pdf[col].shift(-1)
    df = df.fillna(0.0).astype(int)

    df_stack = df.stack(future_stack=True).stack(future_stack=True).reset_index()
    df_stack.columns = ['datetime', 'vehicle_type_id', 'station_id', 'diff']
    df_stack['station_id'] = None if pivot_col == 'latlon' else df_stack['station_id']

    # positive diff = bikes decreased = departure (trip)
    # negative diff = bikes increased = arrival (return)
    df_stack['trips'] = df_stack['diff'].clip(lower=0)
    df_stack['returns'] = (-df_stack['diff']).clip(lower=0)
    df_stack = df_stack.drop(columns='diff')

    df_stack = df_stack.set_index('datetime').groupby(
        [pd.Grouper(freq='h'), 'station_id', 'vehicle_type_id'], dropna=False).sum().reset_index()

    return df_stack


def check_tracking_start(system):

    try:
        data_file = f"{system.data_path}/trips.*.hourly/year=*/month=*/*.parquet"
        qry = duckdb.query(f"""select min(datetime) from read_parquet('{data_file}')""")
        return qry.fetchall()[0][0]
    except:
        return None


def check_tracking_end(system):
    try:
        data_file = f"{system.data_path}/trips.*.hourly/year=*/month=*/*.parquet"
        qry = duckdb.query(f"""select max(datetime) from read_parquet('{data_file}')""")
        return qry.fetchall()[0][0]
    except:
        return None


def get_vehicle_types(system):
    vehicles_file = f"{system.data_path}/vehicle_types.parquet"
    try:
        vdf_current = pd.read_parquet(vehicles_file)
        return list(vdf_current['vehicle_type_id'])
    except:
        return [None]


def update_vehicle_types(system):
    """
    Update vehicle types table
    """
    system.logger.info("Vehicle Types Update")

    # -- Load vehicle types file if exists
    vehicles_file = f"{system.data_path}/vehicle_types.parquet"
    vehicles_file_bak = f"{system.data_path}/vehicles_BAK.parquet"
    try:
        vdf_current = pd.read_parquet(vehicles_file)
    except:
        vdf_current = None

    # -- Query stations
    try:
        vdf = gbfs.query_vehicle_types(system['url'])
    except Exception as e:
        system.logger.info("Unable to load vehicle types")
        system.logger.debug(f"{e}")
        return

    # -- Save stations file
    try:
        os.rename(vehicles_file, vehicles_file_bak)
    except FileNotFoundError:
        pass
    vdf.to_parquet(vehicles_file, index=False)

    system.logger.info("Vehicle Type Update Complete")


def update_stations(system):
    """
    Update stations table
    Adds station if doesn't exist, updates active status
    """
    system.logger.info("Station Update")

    # -- Load stations file if exists
    stations_file = f"{system.data_path}/stations.parquet"
    stations_file_bak = f"{system.data_path}/stations_BAK.parquet"
    try:
        sdf_current = pd.read_parquet(stations_file)
    except:
        sdf_current = None

    # -- Query stations
    try:

        sdf = gbfs.query_station_info(system['url'])
        sdf['active'] = True
    except Exception as e:
        system.logger.debug(f"Failed to load stations: {e}")
        return

    # -- Query station status
    try:
        ddf = gbfs.query_station_status(system['url'])
    except Exception as e:
        system.logger.debug(f"Failed to load station status: {e}")
        return

    # -- Add any legacy stations that aren't in current station query
    if sdf_current is not None:

        legacy_stations_df = sdf_current[~sdf_current['station_id'].isin(sdf['station_id'])].copy()
        if len(legacy_stations_df) > 0:
            legacy_stations_df['active'] = False
        sdf = pd.concat([sdf, legacy_stations_df])

    # -- Run through station status data to label disabled stations
    disabled_ids = ddf.loc[ddf['is_renting'] == 0, 'station_id']
    sdf.loc[sdf['station_id'].isin(disabled_ids), 'active'] = False

    # -- Save stations file
    try:
        os.rename(stations_file, stations_file_bak)
    except FileNotFoundError:
        pass
    sdf.to_parquet(stations_file, index=False)

    system.logger.info("Station Update Complete")
