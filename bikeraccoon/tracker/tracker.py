import time
import datetime as dt
import pandas as pd
import sys
import json
import logging
import pathlib
from multiprocessing import Pool

from .tracker_functions import *


def update_system_raw(system):
    """Returns dict with feed success/error info for failure tracking in the main loop."""
    if not system['tracking']:
        return {'station': None, 'free_bike': None}

    system.logger.info("querying GBFS info")

    station_ok, station_err = None, None
    free_bike_ok, free_bike_err = None, None

    if system.get('track_stations', True):
        station_ok, station_err = update_station_status_raw(system)
    else:
        system.logger.info("skipping station check")

    if system.get('track_free_bikes', True):
        free_bike_ok, free_bike_err = update_free_bike_status_raw(system)
    else:
        system.logger.info("skipping free bike check")

    system['tracking_end'] = dt.datetime.utcnow()

    return {
        'station': station_ok, 'station_error': station_err,
        'free_bike': free_bike_ok, 'free_bike_error': free_bike_err,
    }


def update_system(system):
    if not system['tracking']:
        return False

    vehicle_types = get_vehicle_types(system)
    for feed_type in ['station', 'free_bike']:
        update_trips(system, feed_type)

    if system.get_system_time().hour == system.station_check_hour:  # check stations at 4am local time
        system.logger.info("updating stations")
        update_stations(system)
        update_vehicle_types(system)

    update_system_table(system)

    return True


def _handle_feed_alerts(systems, results, failure_threshold, smtp_config, logger):
    """Track consecutive failures per system/feed and send alert/recovery emails."""
    for system, result in zip(systems, results):
        for feed in ('station', 'free_bike'):
            ok = result.get(feed)
            if ok is None:  # feed not tracked for this system
                continue

            key_failures = f'__{feed}_consecutive_failures'
            key_alerted = f'__{feed}_alert_sent'

            if ok:
                if system.get(key_alerted):
                    # Send recovery email
                    if smtp_config:
                        try:
                            send_alert_email(
                                smtp_config,
                                subject=f"[bikeraccoon] RECOVERED: {system['name']} ({feed})",
                                body=f"System '{system['name']}' ({feed} feed) is processing normally again.",
                            )
                        except Exception as e:
                            logger.warning(f"Failed to send recovery email for {system['name']}: {e}")
                system[key_failures] = 0
                system[key_alerted] = False
            else:
                n = system.get(key_failures, 0) + 1
                system[key_failures] = n
                logger.warning(f"{system['name']} {feed} feed failure ({n} consecutive, alert at {failure_threshold}): {result.get(feed + '_error')}")
                if n >= failure_threshold and not system.get(key_alerted) and smtp_config:
                    try:
                        send_alert_email(
                            smtp_config,
                            subject=f"[bikeraccoon] Feed failure: {system['name']} ({feed})",
                            body=(
                                f"System '{system['name']}' has had {n} consecutive failures "
                                f"on the {feed} feed.\n\nLast error:\n{result.get(feed + '_error')}"
                            ),
                        )
                        system[key_alerted] = True
                    except Exception as e:
                        logger.warning(f"Failed to send alert email for {system['name']}: {e}")


def tracker(systems_file='systems.json', log_path=None, data_path='tracker-data',
            update_interval=20, query_interval=20, station_check_hour=4,
            save_temp_data=False, smtp_config=None, failure_threshold=5,
            summary_hour=8):

    # SETUP LOGGING
    if log_path is not None:
        log_path = pathlib.Path(log_path)
        log_path.mkdir(parents=True, exist_ok=True)
    logger = setup_logger('Tracker', log_path=log_path, log_name='tracker.log')

    # Setup
    last_update = dt.datetime.now()
    query_time = dt.datetime.now()
    update_delta = dt.timedelta(minutes=update_interval)

    # Load stations from json file into list of System objects
    with open(systems_file) as f:
        systems = json.load(f)
        systems = [GBFSSystem(x) for x in systems]

    # Initial Setup
    for system in systems:
        system.set_logger(log_path)
        system.data_path = f'{data_path}/{system["name"]}/'
        system.station_check_hour = station_check_hour
        system.check_url()

        # Set up system table
        update_system_table(system)

        if system['tracking']:

            pathlib.Path(f"{system.data_path}").mkdir(parents=True, exist_ok=True)

            update_stations(system)
            update_vehicle_types(system)

    logger.info("Daemon started successfully")

    last_summary_date = None

    while True:

        if dt.datetime.now() < query_time:
            time.sleep(1)  # Check whether it's time to update every second (actual query interval time determined by
            continue
        else:
            query_time = dt.datetime.now() + dt.timedelta(seconds=query_interval)

        logger.info(f"start: {dt.datetime.now()}")

        with Pool(4) as p:
            raw_results = p.map(update_system_raw, systems)

        _handle_feed_alerts(systems, raw_results, failure_threshold, smtp_config, logger)

        if dt.datetime.now() > last_update + update_delta:
            last_update = dt.datetime.now()

            with Pool(4) as p:
                res = p.map(update_system, systems)

        today = dt.date.today()
        if dt.datetime.now().hour == summary_hour and last_summary_date != today:
            last_summary_date = today
            if smtp_config:
                try:
                    for system in systems:
                        try:
                            meta = pd.read_parquet(
                                pathlib.Path(system.data_path) / 'system.parquet'
                            ).iloc[0].to_dict()
                            for k in ('tracking_start', 'tracking_end', 'latest_update'):
                                if k in meta:
                                    system[k] = meta[k]
                        except Exception:
                            pass
                    send_alert_email(
                        smtp_config,
                        subject=f"[bikeraccoon] Daily summary — {today}",
                        body=build_daily_summary(systems),
                    )
                    logger.info("Daily summary email sent")
                except Exception as e:
                    logger.warning(f"Failed to send daily summary email: {e}")

        logger.info(f"end: {dt.datetime.now()}")
        logger.debug(f"Next DB update: {last_update + update_delta}")


if __name__ == '__main__':
    tracker()
