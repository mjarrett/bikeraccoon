import pandas as pd
import json
import requests
import datetime as dt
import timeout_decorator
import ssl
from time import sleep


def get_station_status_url(sys_url):
    data = requests.get(sys_url).json()
    return [x for x in data['data']['en']['feeds'] if x['name']=='station_status'][0]['url']      

def get_station_info_url(sys_url):
    data = requests.get(sys_url).json()
    return [x for x in data['data']['en']['feeds'] if x['name']=='station_information'][0]['url']   


def get_system_info_url(sys_url):
    data = requests.get(sys_url).json()
    return [x for x in data['data']['en']['feeds'] if x['name']=='system_information'][0]['url']

def get_free_bike_url(sys_url):
    data = requests.get(sys_url).json()
    return [x for x in data['data']['en']['feeds'] if x['name'] == 'free_bike_status'][0]['url']

def get_vehicle_url(sys_url):
    data = requests.get(sys_url).json()
    return [x for x in data['data']['en']['feeds'] if x['name'] == 'vehicle_status'][0]['url']

def get_vehicle_types_url(sys_url):
    data = requests.get(sys_url).json()
    return [x for x in data['data']['en']['feeds'] if x['name']=='vehicle_types'][0]['url']


@timeout_decorator.timeout(30) 
def query_system_info(sys_url):
    url = get_system_info_url(sys_url)

    data = requests.get(url).json()

    return data

@timeout_decorator.timeout(30) 
def query_vehicle_types(sys_url):
    """
    Query vehicle_types.json
    """
    
    url = get_vehicle_types_url(sys_url)
    data = requests.get(url).json()
    
    
    df = pd.DataFrame(data['data']['vehicle_types'])
    
    try:
        df['datetime'] = data['last_updated']
        df['datetime'] = df['datetime'].map(lambda x: dt.datetime.utcfromtimestamp(x))
    except KeyError:
        df['datetime'] = dt.datetime.utcnow()
    df['datetime'] = df['datetime'].dt.tz_localize('UTC')
    
    df = df[['vehicle_type_id','form_factor','propulsion_type']]
    
    return df
  




@timeout_decorator.timeout(30) 
def query_station_status(sys_url):
    """
    Query station_status.json
    """
    # Helper function for vehicle types
    def f(x):
        if 'vehicle_types_available' not in x.keys():
            return x
            
        res = []
        for vehicle_type in x['vehicle_types_available']:
            res.append({'station_id':x['station_id'],
                        'vehicle_type_id':vehicle_type['vehicle_type_id'],
                        'num_bikes_available':vehicle_type['count'],
                       'last_reported':x['last_reported'],
                        'is_renting':x['is_renting']
                       })
        return res

    url = get_station_status_url(sys_url)
    
    data = requests.get(url).json()

    # if data returns string, it might be an error message. wait 2 seconds and try again
    # this was added to handle HOPR rate limit
    if type(data) == str:
        sleep(2)
        data = requests.get(url).json()
    
    data = [f(x) for x in data['data']['stations']] # Reformat if vehicle types are present
    data = [y if 'vehicle_type_id' in y else x for x in data for y in x] #flatten list
    df = pd.DataFrame(data)
    
    if 'vehicle_type_id' not in df.columns:
        df['vehicle_type_id'] = None
    
    df = df.drop_duplicates(['station_id','last_reported','vehicle_type_id'])
    try:
        df['datetime'] = df['last_updated']
        df['datetime'] = df['datetime'].map(lambda x: dt.datetime.utcfromtimestamp(x))
    except KeyError:
        df['datetime'] = dt.datetime.utcnow()
    
    df['datetime'] = df['datetime'].dt.tz_localize('UTC')
    
    df = df[['datetime','num_bikes_available','is_renting','station_id','vehicle_type_id']]


    return df

@timeout_decorator.timeout(30)
def query_free_bike_status(sys_url):
    """
    Query station status if vehicle types are specified.
    
    If 'vehicle_types_available' attribute not present, raise exception
    """
    
    try:
        url = get_free_bike_url(sys_url)
        gbfs_ver = 2
    except IndexError:
        try:
            url = get_vehicle_url(sys_url)
            gbfs_ver = 3
        except IndexError:
            raise ValueError("Free bikes JSON feed not available")

            
    if gbfs_ver == 2:
        bikes_slug = 'bikes'
        bike_id_slug = 'bike_id'
        
    elif gbfs_ver == 3:
        bikes_slug = 'vehicles'
        bike_id_slug = 'vehicle_id'
            
            
    data = requests.get(url).json()
    
    try:    
        df = pd.DataFrame(data['data'][bikes_slug])
    except KeyError:
        df = pd.DataFrame(data[bikes_slug])

    if 'vehicle_type_id' not in data['data'][bikes_slug][0]:
        df['vehicle_type_id'] = None

    if 'lat' not in df.columns or 'lon' not in df.columns:
        df['lat'] = 0
        df['lon'] = 0
        
    if 'station_id' not in df.columns:
        df['station_id'] = None

    
    #df = df.groupby(['station_id','vehicle_type_id']).agg({bike_id_slug:'count'}).reset_index()
    df = df.groupby(['station_id','vehicle_type_id','lat','lon'],dropna=False).agg({bike_id_slug:'count'}).reset_index()
    df = df.rename(columns={bike_id_slug: 'num_bikes_available'})

    
    

    try:
        df['datetime'] = data['last_updated']
        df['datetime'] = df['datetime'].map(lambda x: dt.datetime.fromtimestamp(x,dt.UTC))
    except KeyError:
        df['datetime'] = dt.datetime.utcnow()
   
    #df['datetime'] = df['datetime'].dt.tz_localize('UTC')
    


    
    df = df.reset_index()
    df = df[['station_id','vehicle_type_id','datetime','num_bikes_available','lat','lon']]
    df['num_bikes_available'] = df['num_bikes_available'].fillna(0).astype(int)
    
    #df['num_docks_available'] = None
    df['is_renting'] = True
    
    return df    

    
@timeout_decorator.timeout(30) 
def query_station_info(sys_url):
    
    """
    Query station_information.json
    """
    url = get_station_info_url(sys_url)

    data = requests.get(url).json()

    try:
        df =  pd.DataFrame(data['data']['stations'])
    except KeyError:
        df =  pd.DataFrame(data['stations'])
    return df[['name','station_id','lat','lon']]

# @timeout_decorator.timeout(30) 
# def query_free_bikes(sys_url):
    
#     """
#     Query free_bikes.json
#     """
    
#     try:
#         url = get_free_bike_url(sys_url)
#         gbfs_ver = 2
#     except IndexError:
#         try:
#             url = get_vehicle_url(sys_url)
#             gbfs_ver = 3
#         except IndexError:
#             raise ValueError("Free bikes JSON feed not available")

            
#     if gbfs_ver == 2:
#         bikes_slug = 'bikes'
#         bike_id_slug = 'bike_id'
        
#     elif gbfs_ver == 3:
#         bikes_slug = 'vehicles'
#         bike_id_slug = 'vehicle_id'
            
            
#     data = requests.get(url).json()

#     try:    
#         df = pd.DataFrame(data['data'][bikes_slug])
#     except KeyError:
#         df = pd.DataFrame(data[bikes_slug])
        
    
#     df['vehicle_id'] = df[bike_id_slug].astype(str)

#     try:
#         df['datetime'] = data['last_updated']
#         df['datetime'] = df['datetime'].map(lambda x: dt.datetime.utcfromtimestamp(x))
#     except KeyError:
#         df['datetime'] = dt.datetime.utcnow()
    
#     df['datetime'] = df['datetime'].dt.tz_localize('UTC')
    
#     if 'lat' not in df.columns or 'lon' not in df.columns:
#         df['lat'] = 0
#         df['lon'] = 0
        
#     if 'station_id' not in df.columns:
#         df['station_id'] = None
    
#     df = df[['station_id','vehicle_id','lat','lon','datetime']]
    
#     return df

    
#@timeout_decorator.timeout(30)
# def query_free_bikes_vehicle_type_status(sys_url,vehicle_type):
#     """
#     Query station status if vehicle types are specified.
    
#     If 'vehicle_types_available' attribute not present, raise exception
#     """
    
#     try:
#         url = get_free_bike_url(sys_url)
#         gbfs_ver = 2
#     except IndexError:
#         try:
#             url = get_vehicle_url(sys_url)
#             gbfs_ver = 3
#         except IndexError:
#             raise ValueError("Free bikes JSON feed not available")

            
#     if gbfs_ver == 2:
#         bikes_slug = 'bikes'
#         bike_id_slug = 'bike_id'
        
#     elif gbfs_ver == 3:
#         bikes_slug = 'vehicles'
#         bike_id_slug = 'vehicle_id'
            
            
#     data = requests.get(url).json()
#     if 'vehicle_type_id' not in data['data'][bikes_slug][0]:
#         raise ValueError("vehicle_type_id attribute is not present")
#     try:    
#         df = pd.DataFrame(data['data'][bikes_slug])
#     except KeyError:
#         df = pd.DataFrame(data[bikes_slug])

#     gdf = df.groupby(['station_id','vehicle_type_id']).agg({bike_id_slug:'count'}).reset_index()
#     df = gdf.pivot(index='station_id',columns='vehicle_type_id',values=bike_id_slug)
    
    
    
#     # Filter to only vehicle type specified in function call

#     try:
#         df['datetime'] = data['last_updated']
#         df['datetime'] = df['datetime'].map(lambda x: dt.datetime.utcfromtimestamp(x))
#     except KeyError:
#         df['datetime'] = dt.datetime.utcnow()
   
#     df['datetime'] = df['datetime'].dt.tz_localize('UTC')


#     if vehicle_type not in df.columns:
#         df[vehicle_type] = 0

#     df = df.reset_index()
#     df = df[['station_id',vehicle_type,'datetime']]
#     df = df.rename(columns={vehicle_type: 'num_bikes_available'})
#     df['num_bikes_available'] = df['num_bikes_available'].fillna(0).astype(int)
    
#     df['num_docks_available'] = None
#     df['is_renting'] = True
    
#     return df    

# @timeout_decorator.timeout(30)
# def query_vehicle_type_status(sys_url,vehicle_type):
#     """
#     Query station status if vehicle types are specified.
    
#     If 'vehicle_types_available' attribute not present, raise exception
#     """
#     url = get_station_status_url(sys_url)
    
#     data = requests.get(url).json()
    
#     if 'vehicle_types_available' not in data['data']['stations'][0]:
#         raise ValueError("vehicle_types_available attribute must be present")
    
    
#     # This creates a dataframe with index:station_id and columns: vehicle_type_id and values: vehicle counts
#     df = pd.DataFrame([{y['vehicle_type_id']:y['count'] for y in x['vehicle_types_available']} for x in data['data']['stations']], index=pd.Index([x['station_id'] for x in data['data']['stations']], name='station_id'))
#     # Filter to only vehicle type specified in function call

#     try:
#         df['datetime'] = data['last_updated']
#         df['datetime'] = df['datetime'].map(lambda x: dt.datetime.utcfromtimestamp(x))
#     except KeyError:
#         df['datetime'] = dt.datetime.utcnow()
   
#     df['datetime'] = df['datetime'].dt.tz_localize('UTC')


#     if vehicle_type not in df.columns:
#         df[vehicle_type] = 0

#     df = df.reset_index()
#     df = df[['station_id',vehicle_type,'datetime']]
#     df = df.rename(columns={vehicle_type: 'num_bikes_available'})

#     df['num_docks_available'] = None
#     df['is_renting'] = True
    
#     return df
