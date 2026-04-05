import pandas as pd
import requests
import datetime as dt

VISUAL_CROSSING_KEY = ''


def get_weather_range(api, freq, day1, day2=None):
    """
    api: a bikeraccoon API instance
    freq: 'daily' or 'hourly'
    day1,day2: pandas.Timestamp object representing the start and end days (inclusive)

    Returns a pandas.DataFrame
    """
    sdf = api.get_stations()
    lat = sdf['lat'].mean()
    lon = sdf['lon'].mean()

    if day2 is None:
        day2 = day1
    elif day2 < day1:
        day1, day2 = day2, day1

    day1 = day1.strftime('%Y-%m-%d')
    day2 = day2.strftime('%Y-%m-%d')

    freq_key = 'days' if freq == 'daily' else 'hours'

    weather_url = (
        f'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/'
        f'{lat}%2C{lon}/{day1}/{day2}'
        f'?unitGroup=metric&key={VISUAL_CROSSING_KEY}&contentType=json&include={freq_key}'
    )
    data = requests.get(weather_url).json()

    if freq == 'daily':
        df = pd.DataFrame(data['days'])
        df.index = [dt.datetime.fromtimestamp(x, dt.UTC) for x in df['datetimeEpoch']]
    elif freq == 'hourly':
        df = pd.concat([pd.DataFrame(day['hours']) for day in data['days']])
        df.index = [dt.datetime.fromtimestamp(x, dt.UTC) for x in df['datetimeEpoch']]

    df.index = df.index.tz_convert(api.info['tz'])
    return df
