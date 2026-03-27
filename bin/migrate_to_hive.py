#!/usr/bin/env python3
"""
Migrate old flat parquet files to hive-partitioned format, then rebuild daily files.

Old format: tracker-data/{system}/trips.{feed_type}.hourly.YYYY*.parquet
New format: tracker-data/{system}/trips.{feed_type}.{hourly|daily}/year=YYYY/month=M/*.parquet

After migrating hourly files, re-builds the daily hive-partitioned files from them.
"""

import pathlib
import re
import sys
import pandas as pd

DATA_DIR = pathlib.Path(sys.argv[1])

# Matches e.g. "trips.station.hourly.2024.parquet" or "trips.station.hourly.2024v2.parquet"
OLD_FILE_RE = re.compile(r"^(trips\.([^.]+)\.hourly)\.(\d{4}[^.]*)\.parquet$")


def migrate_system(system_dir: pathlib.Path, dry_run: bool = False) -> None:
    old_files = [f for f in system_dir.iterdir() if OLD_FILE_RE.match(f.name)]
    if not old_files:
        return

    print(f"\n{system_dir.name}: found {len(old_files)} old file(s)")

    # Group by base name (e.g. "trips.station.hourly") so we can merge all years at once
    groups: dict[str, list[pathlib.Path]] = {}
    for f in old_files:
        m = OLD_FILE_RE.match(f.name)
        base = m.group(1)
        groups.setdefault(base, []).append(f)

    for base, files in groups.items():
        feed_type = base.split('.')[1]  # e.g. "station" from "trips.station.hourly"
        hourly_dir = system_dir / base
        daily_dir = system_dir / f"trips.{feed_type}.daily"

        if hourly_dir.exists() and any(hourly_dir.iterdir()):
            print(f"  SKIP {base}: destination already exists and is non-empty")
            continue

        print(f"  Migrating {base} ({len(files)} year file(s))...")

        if dry_run:
            for f in sorted(files):
                df = pd.read_parquet(f)
                print(f"    [dry-run] would write {len(df):,} rows from {f.name} → {hourly_dir}")
                del df
            print(f"    [dry-run] would rebuild daily → {daily_dir}")
            continue

        hourly_dir.mkdir(parents=True, exist_ok=True)
        for f in sorted(files):
            df = pd.read_parquet(f)
            print(f"    read {f.name}: {len(df):,} rows")

            # Ensure partition columns are present
            if 'year' not in df.columns:
                df['year'] = df['datetime'].dt.year
            if 'month' not in df.columns:
                df['month'] = df['datetime'].dt.month

            df.to_parquet(
                hourly_dir,
                partition_cols=['year', 'month'],
                index=False,
                existing_data_behavior='delete_matching',
            )
            del df
            print(f"    wrote → {hourly_dir}")

            f.unlink()
            print(f"    removed {f.name}")

        # Rebuild daily from the full hourly hive data
        print(f"  Rebuilding daily from {hourly_dir}...")
        hourly_df = pd.read_parquet(hourly_dir)
        daily = (
            hourly_df[['datetime', 'station_id', 'vehicle_type_id', 'trips', 'returns']]
            .set_index('datetime')
            .groupby([pd.Grouper(freq='d'), 'station_id', 'vehicle_type_id'], dropna=False)
            [['trips', 'returns']].sum()
            .reset_index()
        )
        daily['year'] = daily['datetime'].dt.year
        daily['month'] = daily['datetime'].dt.month
        daily.to_parquet(
            daily_dir,
            partition_cols=['year', 'month'],
            index=False,
            existing_data_behavior='delete_matching',
        )
        del hourly_df, daily
        print(f"  wrote daily → {daily_dir}")


def main() -> None:
    dry_run = '--dry-run' in sys.argv

    if not DATA_DIR.exists():
        print(f"Error: {DATA_DIR} not found. Run from the repo root.", file=sys.stderr)
        sys.exit(1)

    if dry_run:
        print("Dry-run mode — no files will be written or deleted.\n")

    system_dirs = [d for d in DATA_DIR.iterdir() if d.is_dir()]
    if not system_dirs:
        print("No system directories found.")
        return

    for system_dir in sorted(system_dirs):
        migrate_system(system_dir, dry_run=dry_run)

    print("\nDone.")


if __name__ == "__main__":
    main()
