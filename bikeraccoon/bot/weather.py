import pandas as pd
import json
import urllib
import datetime as dt

def get_weather_range(api,freq,day1,day2=None):
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
    else:
        
        if day2 < day1:
            day1,day2 = day2,day1
            
    day1 = day1.strftime('%Y-%m-%d')
    day2 = day2.strftime('%Y-%m-%d')   
    
    if freq == 'daily':
        freq_key = 'days'
    elif freq == 'hourly':
        freq_key = 'hours'

    weather_url = f'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{lat}%2C{lon}/{day1}/{day2}?unitGroup=metric&key={api.VISUAL_CROSSING_KEY}&contentType=json&include={freq_key}'
    with urllib.request.urlopen(weather_url) as url:    
        #data = json.loads(url.read().decode())
        data = url.read().decode()

        
    if freq == 'daily':
        df = pd.DataFrame(json.loads(data)['days'])
        df.index = [dt.datetime.utcfromtimestamp(x) for x in df['datetimeEpoch']]
        
    elif freq == 'hourly':
        df = pd.concat([pd.DataFrame(day['hours']) for day in json.loads(data)['days']])
        df.index = [dt.datetime.utcfromtimestamp(x) for x in df['datetimeEpoch']]
            
    return df