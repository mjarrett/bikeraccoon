#!/usr/bin/env python3

import os
from pathlib import Path
from bikeraccoon.tracker import tracker

workingdir = Path(__file__).parent.parent

smtp_config = None
if os.environ.get('BR_SMTP_HOST'):
    smtp_config = {
        'host': os.environ['BR_SMTP_HOST'],
        'port': int(os.environ.get('BR_SMTP_PORT', 587)),
        'from': os.environ['BR_SMTP_FROM'],
        'to': os.environ['BR_SMTP_TO'],
        'tls': os.environ.get('BR_SMTP_TLS', 'true').lower() == 'true',
    }
    if os.environ.get('BR_SMTP_USERNAME'):
        smtp_config['username'] = os.environ['BR_SMTP_USERNAME']
        smtp_config['password'] = os.environ['BR_SMTP_PASSWORD']

tracker(
    systems_file=os.environ.get('BR_SYSTEMS_FILE', '/srv/br-tracker/systems.json'),
    log_path=os.environ.get('BR_LOG_PATH', '/srv/br-tracker/logs/'),
    data_path=os.environ.get('BR_DATA_PATH', '/srv/br-tracker/tracker-data/'),
    query_interval=int(os.environ.get('BR_QUERY_INTERVAL', 60)),
    update_interval=int(os.environ.get('BR_UPDATE_INTERVAL', 10)),
    station_check_hour=int(os.environ.get('BR_STATION_CHECK_HOUR', 4)),
    failure_threshold=int(os.environ.get('BR_FAILURE_THRESHOLD', 1)),
    smtp_config=smtp_config,
)
