import datetime as dt
import bikeraccoon as br
import bikeraccoon.bot.plots as plots
import matplotlib.pyplot as plt
import pandas as pd
import os
import json
import sys
from glob import glob

from atproto import Client, models, client_utils


def load_config(master_config_file, bot_config_file):
    with open(master_config_file) as f:
        master = json.load(f)
    with open(bot_config_file) as f:
        bot = json.load(f)
    return master, bot


def post_bsky(account, password, text, images=[], descriptions=[], hashtags=[]):
    client = Client()
    client.login(account, password)

    bsky_images = []
    for image, description in zip(images, descriptions):
        with open(image, 'rb') as f:
            img_data = f.read()
        upload = client.com.atproto.repo.upload_blob(img_data)
        bsky_images.append(models.AppBskyEmbedImages.Image(alt=description, image=upload.blob))

    embed = models.AppBskyEmbedImages.Main(images=bsky_images)

    output_string = client_utils.TextBuilder()
    output_string.text(text)
    for hashtag in hashtags:
        output_string.tag(hashtag, hashtag[1:]).text(' ')

    client.send_post(text=output_string, embed=embed)


def check_zero_trips(t1, t2, api, m=0):
    thdf = api.get_station_trips(t1, t2, freq='d')
    if thdf is None:
        raise ValueError(f"No trip data returned for {api.sys_name}")
    return bool(thdf['trips'].sum() <= m)


def make_tweet_text(api, t1, path='./', lang='EN'):
    t1 = t1.replace(hour=0, minute=0, second=0)
    t2 = t1 + dt.timedelta(hours=23)

    sdf = api.get_stations()
    thdf = api.get_station_trips(t1, t2, freq='d', station='all')
    thdf_sdf = pd.merge(sdf, thdf, how='inner', on='station_id')
    ntrips = thdf_sdf['trips'].sum()

    sdf = sdf[sdf['active']]

    thdf_sdf = thdf_sdf.sort_values('trips', ascending=False)
    busiest_station = thdf_sdf['name'].iloc[0].split('(')[0].strip()
    busiest_station_trips = int(thdf_sdf['trips'].iloc[0])
    n_busiest_stations = len(thdf_sdf[thdf_sdf['trips'] == busiest_station_trips])
    plural = "" if n_busiest_stations == 2 else "s"
    n_busiest_str = f" and {n_busiest_stations-1} other{plural}" if n_busiest_stations > 1 else ""

    least_busy_station = thdf_sdf['name'].iloc[-1].split('(')[0].strip()
    least_busy_station_trips = thdf_sdf['trips'].iloc[-1]
    n_least_busy_stations = len(thdf_sdf[thdf_sdf['trips'] == least_busy_station_trips])
    plural = "" if n_least_busy_stations == 2 else "s"
    if n_least_busy_stations > 1:
        n_least_busy_str = f" and {n_least_busy_stations-1} other{plural}" if lang == 'EN' else f" et {n_least_busy_stations-1} autre{plural}"
    else:
        n_least_busy_str = ""

    active_stations = len(sdf)

    if lang == 'EN':
        s = f"""Yesterday there were approximately {ntrips:,} {api.brand} bikeshare trips
Most used station: {busiest_station}{n_busiest_str} ({busiest_station_trips} trips)
Least used station: {least_busy_station}{n_least_busy_str} ({least_busy_station_trips} trips)
Active stations: {active_stations}
"""
    elif lang == 'FR':
        s = f"""Hier, il y a eu approximativement {ntrips:,} déplacements en vélopartage {api.brand}.
Station la plus utilisée: {busiest_station}{n_busiest_str} ({busiest_station_trips} déplacements)
Station la moins utilisée: {least_busy_station}{n_least_busy_str} ({least_busy_station_trips} déplacements)
Stations actives: {active_stations}"""

    with open(f'{path}/{api.sys_name}_bot_text.txt', 'w') as ofile:
        ofile.write(s)
    return s


def make_monthly_trips_plot(api, t1, path='./', weather=True):
    t1 = t1.replace(hour=23, minute=0, second=0)
    t2 = t1 - dt.timedelta(days=31)
    trips = api.get_station_trips(t1, t2, freq='d')['trips']

    if weather:
        f, ax = plt.subplots(2, sharex=True, gridspec_kw={'height_ratios': [4.5, 1]})
        plots.plot_daily_trips(api, api.sys_type, trips, ax=ax[0], palette=api.palette, weather=weather)
        try:
            plots.plot_daily_weather(api, t1, t2, ax[1])
        except Exception as e:
            sys.stderr.write(f"Unable to create weather subplot\n{e}\n")
            weather = False
    if not weather:
        f, ax = plt.subplots()
        plots.plot_daily_trips(api, api.sys_type, trips, ax=ax, palette=api.palette, weather=weather)

    f.tight_layout()
    f.savefig(f"{path}/2.{api.sys_name}_last_month.png")


def make_weekly_trips_plot(api, t1, path='./', weather=True):
    t1 = t1.replace(hour=23, minute=0, second=0)
    t2 = t1 - dt.timedelta(days=7, hours=23)
    trips = api.get_station_trips(t1, t2)['trips']

    if weather:
        f, ax = plt.subplots(2, sharex=True, gridspec_kw={'height_ratios': [4.5, 1]})
        plots.plot_hourly_trips(api, api.sys_type, trips, ax=ax[0], palette=api.palette, weather=weather)
        try:
            plots.plot_hourly_weather(api, t1, t2, ax[1])
        except Exception as e:
            sys.stderr.write(f"Unable to create weather subplot\n{e}\n")
            weather = False
    if not weather:
        f, ax = plt.subplots()
        plots.plot_hourly_trips(api, api.sys_type, trips, ax=ax, palette=api.palette, weather=weather)

    f.tight_layout()
    f.savefig(f"{path}/1.{api.sys_name}_last_week.png")


def make_alltime_plot(api, t1, path='./'):
    t1 = t1.replace(hour=23, minute=0, second=0)
    t2 = api.get_system_info()['tracking_start']
    trips = api.get_station_trips(t1, t2, freq='d')['trips']

    f, ax = plt.subplots()
    plots.plot_alltime_trips(api, trips, api.sys_type, ax=ax, palette=api.palette)
    f.tight_layout()
    f.savefig(f"{path}/3.{api.sys_name}_alltime.png")


def make_stations_map(api, t1, path='./'):
    t1 = t1.replace(hour=0)
    t2 = t1.replace(hour=23)
    thdf = api.get_station_trips(t1, t2, freq='d', station='all')
    f, ax = plots.plot_stations(api, thdf, extent=api.extent, palette=api.palette)
    f.savefig(f'{path}/4.{api.sys_name}_stations.png',
              bbox_inches='tight', pad_inches=0.0, transparent=False, dpi=100)


def run(master_config_file, bot_config_file, path='./', t1=None, skip_zero_check=False):
    master, bot = load_config(master_config_file, bot_config_file)

    plots.configure(
        mapbox_token=master.get('mapbox_token', ''),
        mapbox_style_username=master.get('mapbox_style_username', ''),
        mapbox_style_id=master.get('mapbox_style_id', ''),
        visual_crossing_key=master.get('visual_crossing_key', ''),
    )

    api = br.LiveAPI(bot['sys_name'], api_key=master.get('api_key'))
    api.brand = bot.get('brand', bot['sys_name'])
    api.sys_name = bot['sys_name']
    api.sys_type = bot.get('sys_type', 'stations')
    api.palette = bot.get('palette')
    api.extent = bot.get('extent')

    if t1 is None:
        t1 = dt.datetime.now() - dt.timedelta(days=1)

    os.makedirs(path, exist_ok=True)
    for x in glob(f'{path}/*'):
        os.remove(x)

    if not skip_zero_check and check_zero_trips(t1.replace(hour=0), t1.replace(hour=23), api):
        return

    text = make_tweet_text(api, t1, path=path, lang=bot.get('lang', 'EN'))
    make_weekly_trips_plot(api, t1, path=path)
    make_monthly_trips_plot(api, t1, path=path)
    make_alltime_plot(api, t1, path=path)
    make_stations_map(api, t1, path=path)

    images = sorted(glob(f'{path}/*.png'))
    descriptions = [os.path.basename(img) for img in images]

    post_bsky(
        account=bot['account'],
        password=bot['bsky_password'],
        text=text,
        images=images,
        descriptions=descriptions,
        hashtags=bot.get('hashtags', []),
    )
