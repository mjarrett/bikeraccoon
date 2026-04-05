from cartopy.io.img_tiles import MapboxStyleTiles
import cartopy.crs as ccrs
import geopandas
from shapely.geometry import Point
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.ticker as ticker
import seaborn as sns
import numpy as np
import pandas as pd

from bikeraccoon.bot.weather import get_weather_range

_MAPBOX_TOKEN = ''
_MAPBOX_STYLE_USERNAME = ''
_MAPBOX_STYLE_ID = ''

c_blue = '#3286AD'
c_light_blue = '#50AAD3'
c_indigo = '#8357B2'
c_red = '#FF5B71'
c_yellow = '#E5DE50'
c_green = '#77ACA2'


def configure(mapbox_token='', mapbox_style_username='', mapbox_style_id='', visual_crossing_key=''):
    global _MAPBOX_TOKEN, _MAPBOX_STYLE_USERNAME, _MAPBOX_STYLE_ID
    _MAPBOX_TOKEN = mapbox_token
    _MAPBOX_STYLE_USERNAME = mapbox_style_username
    _MAPBOX_STYLE_ID = mapbox_style_id
    if visual_crossing_key:
        import bikeraccoon.bot.weather as weather
        weather.VISUAL_CROSSING_KEY = visual_crossing_key


def plot_hourly_trips(api, kind, trips, ax=None, palette=None, weather=True):
    sns.set(style='ticks', palette=palette)
    color = sns.color_palette()[0]

    if ax is None:
        f, ax = plt.subplots()

    ax.plot(trips.index, trips.values, color=color)
    ax.fill_between(trips.index, 0, trips.values, alpha=0.4, color=color)
    ax.xaxis_date(trips.index.tz)
    ax.xaxis.set_major_locator(mdates.DayLocator(tz=trips.index.tz))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%A", tz=trips.index.tz))
    ax.tick_params(axis='x', labelrotation=45)

    try:
        ax.xaxis.get_ticklabels()[-1].set_visible(False)
    except Exception:
        pass

    ax.set_ylabel('Hourly trips')
    if weather:
        sns.despine(top=True, bottom=True, left=True, right=True)
    else:
        sns.despine(top=True, bottom=False, left=False, right=True)
    ax.tick_params(axis='both', which='both', length=0)
    ax.grid(which='both')
    return ax


def plot_daily_trips(api, kind, trips, ax=None, palette=None, weather=True):
    sns.set(style='ticks', palette=palette)
    color = sns.color_palette()[0]

    if ax is None:
        f, ax = plt.subplots()

    trips.index = [x - pd.Timedelta(6, 'h') for x in trips.index]
    ax.xaxis_date(trips.index.tz)
    ax.xaxis.set_major_locator(mdates.WeekdayLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
    ax.tick_params(axis='x', labelrotation=45)
    ax.bar(trips.index, trips.values, color=color)
    ax.set_ylabel('Daily trips')
    if weather:
        sns.despine(top=True, bottom=True, left=True, right=True)
    else:
        sns.despine(top=True, bottom=False, left=False, right=True)
    ax.tick_params(axis='both', which='both', length=0)
    ax.grid(which='both')
    return ax


def plot_alltime_trips(api, trips, kind, ax=None, palette=None):
    sns.set(style='ticks', palette=palette)
    color = sns.color_palette()[0]

    if ax is None:
        f, ax = plt.subplots()

    ax.xaxis_date(trips.index.tz)
    ax.xaxis.set_major_locator(mdates.MonthLocator(tz=trips.index.tz))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%B", tz=trips.index.tz))
    ax.tick_params(axis='x', labelrotation=45)

    for trips_yr in trips.groupby(trips.index.year):
        year = trips_yr[0]
        trips_yr = trips_yr[1]
        trips_yr.index = trips_yr.index.map(lambda t: t.replace(year=2020))
        ax.plot(trips_yr.index, trips_yr.values, color='grey', linewidth=0.5, alpha=0.6)
    ax.plot(trips_yr.index, trips_yr.values, alpha=1.0, color=palette[0], linewidth=1.3, label=f"{year}")
    ax.plot(trips_yr.index[-1], trips_yr.values[-1], marker='o', markersize=4, markeredgecolor='k', color=palette[0])
    ax.set_ylabel('Daily trips')
    sns.despine()
    ax.grid(which='both')
    ax.legend()
    return ax


def plot_daily_weather(api, date1, date2, ax=None):
    df = get_weather_range(api, 'daily', date1, date2)

    if ax is None:
        f, ax = plt.subplots()

    ax2 = ax.twinx()
    ax.set_ylabel('Daily high')
    ax2.bar(df.index, df['precip'].values * 24, color=c_light_blue)
    ax.plot(df.index, df['tempmax'], color=c_yellow, zorder=1000)
    ax2.set_ylabel('Precipitation')
    ax.yaxis.label.set_color(c_yellow)
    ax2.yaxis.label.set_color(c_light_blue)
    ax.spines['top'].set_visible(False)
    ax2.spines['top'].set_visible(False)
    ax.xaxis.set_major_locator(mdates.WeekdayLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
    ax.tick_params(axis='x', labelrotation=45)
    ax2.tick_params(axis='x', labelrotation=45)
    sns.despine(ax=ax, top=True, bottom=True, left=True, right=True)
    sns.despine(ax=ax2, top=True, bottom=True, left=True, right=True)
    ax.tick_params(axis='both', which='both', length=0)
    ax2.tick_params(axis='both', which='both', length=0)
    ax.grid(which='both')
    ax.set_yticklabels([])
    ax2.set_yticklabels([])
    return ax, ax2


def plot_hourly_weather(api, date1, date2, ax=None):
    df = get_weather_range(api, 'hourly', date1, date2)

    if ax is None:
        f, ax = plt.subplots()

    ax2 = ax.twinx()
    ax.set_ylabel('Temperature')
    ax2.plot(df.index, df['precip'].values, color=c_light_blue, zorder=1001)
    ax2.fill_between(df.index, 0, df['precip'].values, alpha=0.8, color=c_light_blue)
    ax.plot(df.index, df['temp'], color=c_yellow, zorder=1000)
    ax2.set_ylabel('Precipitation')
    ax.yaxis.label.set_color(c_yellow)
    ax2.yaxis.label.set_color(c_light_blue)
    ax.spines['top'].set_visible(False)
    ax2.spines['top'].set_visible(False)
    ax.xaxis.set_major_locator(mdates.DayLocator(tz=df.index.tz))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%A", tz=df.index.tz))
    ax.tick_params(axis='x', labelrotation=45)
    ax2.tick_params(axis='x', labelrotation=45)
    ax2.set_ylim(0, max(df['precip'].max(), 2.5))
    sns.despine(ax=ax, top=True, bottom=True, left=True, right=True)
    sns.despine(ax=ax2, top=True, bottom=True, left=True, right=True)
    ax.tick_params(axis='both', which='both', length=0)
    ax2.tick_params(axis='both', which='both', length=0)
    ax.grid(which='both')
    ax.set_yticklabels([])
    ax2.set_yticklabels([])
    return ax, ax2


def plot_stations(api, trips, extent=None, palette=None):
    """Plot stations on a map. Station size proportional to usage in date range."""
    sns.set(style='ticks', palette=palette)
    color = sns.color_palette()[0]
    color2 = sns.color_palette()[1]

    tile = MapboxStyleTiles(_MAPBOX_TOKEN, _MAPBOX_STYLE_USERNAME, _MAPBOX_STYLE_ID)

    sdf = api.get_stations()
    sdf['geometry'] = [Point(xy) for xy in zip(sdf.lon, sdf.lat)]

    f, ax = plt.subplots(subplot_kw={'projection': tile.crs}, figsize=(7, 7))

    if extent is None:
        extent = [sdf.lon.min(), sdf.lon.max(), sdf.lat.min(), sdf.lat.max()]
    ax.set_extent(extent)
    ax.add_image(tile, 13)
    ax.patch.set_visible(False)
    ax.spines[:].set_visible(False)

    sdf = pd.merge(sdf, trips, how='inner', on='station_id')
    sdf = geopandas.GeoDataFrame(sdf)
    sdf = sdf.set_crs('EPSG:4326').to_crs('EPSG:3857')
    sdf.plot(ax=ax, markersize='trips', color=color, alpha=0.7)
    sdf[sdf.trips == 0].plot(ax=ax, color=color2, alpha=0.7, markersize=10, marker='x')

    l1 = ax.scatter([0], [0], s=10, edgecolors='none', color=color, alpha=0.7)
    l2 = ax.scatter([0], [0], s=100, edgecolors='none', color=color, alpha=0.7)
    l3 = ax.scatter([0], [0], s=10, marker='x', edgecolors='none', color=color2, alpha=0.7)

    ax.legend([l3, l1, l2], ['0', '10', '100'], title='Station Activity')
    return f, ax
