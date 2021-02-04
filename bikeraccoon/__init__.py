import pandas as pd
import datetime as dt
import requests

class APIBase():
    
    def __init__(self):
        self.api_base_url = 'http://api2.mikejarrett.ca'
        
        
        
class LiveAPI(APIBase):
    def __init__(self,system):
        super().__init__()
        self.system = system
        
        
    def get_system_trips(self,t1,t2=None,freq='h'):
        t1,t2 = dates2strings(t1,t2)
        
        query_url = f'/activity?system={self.system}&start={t1}&end={t2}&frequency={freq}'
        print(self.api_base_url + query_url)
        return to_df(self.api_base_url + query_url)
    
    def get_station_trips(self,t1,t2=None,freq='h',station='all'):
        t1,t2 = dates2strings(t1,t2)
        
        query_url = f'/activity?system={self.system}&start={t1}&end={t2}&frequency={freq}&station={station}'
        print(self.api_base_url + query_url)
        df =  to_df(self.api_base_url + query_url)
        return df

    def get_free_bike_trips(self,t1,t2=None,freq='h'):
        t1,t2 = dates2strings(t1,t2)

        query_url = f'/activity?system={self.system}&start={t1}&end={t2}&frequency={freq}&station=free_bikes'
        print(self.api_base_url + query_url)
        df =  to_df(self.api_base_url + query_url)
        return df
    
    
    def get_systems(self):
        query_url = f'/systems'
        r = requests.get(self.api_base_url + query_url)
        df =  pd.DataFrame(r.json())
        print(self.api_base_url + query_url)
        return df
        
        
def dates2strings(t1,t2):
    if t2 is None:
        t2 = t1
    if t2 < t1:
        t1,t2 = t2,t1
            
    t1 = t1.strftime('%Y%m%d%H')
    t2 = t2.strftime('%Y%m%d%H')
    
    return t1,t2

def to_df(url):

    r = requests.get(url)
    df =  pd.DataFrame(r.json())
    if len(df) == 0:
        df = pd.DataFrame(columns=['num_bikes_available','num_docks_available','returns','station','station_id','trips'],
                           )
        df.index.name = 'datetime'
        return df
    df = df.set_index('datetime')
    df.index = pd.to_datetime(df.index)
    return df