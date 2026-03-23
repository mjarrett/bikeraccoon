#!/usr/bin/env python3
"""
tracker_status.py — summarize tracker data in ./tracker-data/

Usage:
    python tracker_status.py [data_path]
"""

import sys
import pathlib
import datetime as dt
import pandas as pd

from bikeraccoon.tracker.tracker_functions import GBFSSystem, build_daily_summary


def load_systems(data_path):
    systems = []
    for system_dir in sorted(p for p in data_path.iterdir() if p.is_dir()):
        try:
            meta = pd.read_parquet(system_dir / "system.parquet").iloc[0].to_dict()
        except Exception:
            meta = {'name': system_dir.name}

        system = GBFSSystem(meta)
        system.data_path = str(system_dir) + '/'
        systems.append(system)
    return systems


def main():
    data_path = pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else "tracker-data")

    if not data_path.exists():
        print(f"Data path not found: {data_path}")
        sys.exit(1)

    systems = load_systems(data_path)
    if not systems:
        print(f"No system directories found in {data_path}")
        sys.exit(0)

    print(build_daily_summary(systems))


if __name__ == "__main__":
    main()
