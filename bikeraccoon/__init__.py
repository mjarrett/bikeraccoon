import pandas as pd
import datetime as dt
import requests
import calendar
import sys
import pathlib
import glob

import tracker
import api 
import dash
import gbfs

from functools import cached_property, lru_cache

class APIBase():

    def __init__(self):
        #self.api_base_url = 'http://api.raccoon.bike'
        self.api_base_url = 'http://api.mikejarrett.ca'


class LiveAPI(APIBase):
    def __init__(self,system, echo=False):
        super().__init__()
        self.system = system
        self.info = self.get_system_info()
        self.echo = echo
        


        

    def get_system_info(self):
        systems = get_systems()
        return [x for x in systems if x['name'] == self.system][0]


        
    def get_station_trips(self,*args,**kwargs):
        kwargs['feed'] = 'station'
        return self.get_trips(*args,**kwargs)

    def get_free_bike_trips(self,*args,**kwargs):
        kwargs['feed'] = 'free_bike'
        return self.get_trips(*args,**kwargs)

        
    def get_trips(self,t1,t2=None,freq='h',station=None,feed='station',vehicle=None, cache=False):
        t1,t2 = _dates2strings(t1,t2,freq)

        if vehicle is not None:
            vehicle_string = f'&vehicle={vehicle}'
        else:
            vehicle_string = ''
        if station is not None:
            station_string = f'&station={station}'
        else:
            station_string = ''

        
        
        query_url = f'/activity?system={self.system}&start={t1}&end={t2}&frequency={freq}{station_string}&feed={feed}{vehicle_string}'
        if self.echo:
            print(self.api_base_url + query_url)
        df =  self._to_df(self.api_base_url + query_url)

        
        
        if len(df) == 0:
            return None


        return df

    def get_stations(self):
        query_url = f"/stations?system={self.system}"
        if self.echo:
            print(self.api_base_url + query_url)
        r = requests.get(APIBase().api_base_url + query_url)
        df =  pd.DataFrame(r.json()['data'])

        if len(df) == 0:
            return None
        return df

    
    # def query_free_bikes(self):

    #     """
    #     Query free_bikes.json
    #     """

    #     sys_url = self.get_system_info()['url']
    #     try:
    #         url = _get_free_bike_url(sys_url)
    #     except IndexError:
    #         return None

    #     r = requests.get(url)
    #     data = r.json()

        
    #     try:
    #         df = pd.DataFrame(data['data']['bikes'])
    #     except KeyError:
    #         df = pd.DataFrame(data['bikes'])
    #     try:
    #         df['bike_id'] = df['bike_id'].astype(str)
    #     except KeyError:
    #         return None

    #     try:
    #         df['datetime'] = data['last_updated']
    #         df['datetime'] = df['datetime'].map(lambda x: dt.datetime.utcfromtimestamp(x))
    #     except KeyError:
    #         df['datetime'] = dt.datetime.utcnow()
        
    #     df['datetime'] = df['datetime'].dt.tz_localize('UTC')


    #     df = df[['bike_id','lat','lon','datetime']]

    #     return df

    def _to_df(self,url):

        r = requests.get(url)
        df =  pd.DataFrame(r.json()['data'])

        if self.echo:
            print({k:v for k,v in r.json().items() if k != 'data'})
        
        # Need to import as UTC then re-set TZ because of some DST issues.
        #df['datetime'] = pd.to_datetime(df['datetime'], utc=True).dt.tz_convert(self.info['tz'])
        
        df = df.set_index('datetime')
        
        

        return df
    
    
    
    
def get_systems():
    query_url = f'/systems'
    r = requests.get(APIBase().api_base_url + query_url)
    #df =  pd.DataFrame(r.json())
#     print(self.api_base_url + query_url)
    return r.json()['data']


def _dates2strings(t1,t2,freq='h'):
    if t2 is None:
        if freq=='h':
            t2 = t1
        elif freq=='d':
            t1 = t1.replace(hour=0)
            t2 = t1.replace(hour=23)
        elif freq=='m':
            t1 = t1.replace(hour=0,day=1)
            last_day = calendar.monthrange(t1.year, t1.month)[1]
            t2 = t1.replace(hour=23,day=last_day)
        elif freq=='y':
            t1 = t1.replace(hour=0,day=1,month=1)
            t2 = t1.replace(hour=23,day=31,month=12)

    if t2 < t1:
        t1,t2 = t2,t1

    t1 = t1.strftime('%Y%m%d%H')
    t2 = t2.strftime('%Y%m%d%H')

    return t1,t2


    
    
    

