import pandas as pd
import json
import pathlib

from bikeraccoon.gbfs import *


import logging

import datetime as dt
import os
import sys     
import duckdb

#-- Get loggers
logger = logging.getLogger('Tracker')

with open('systems.json') as f:
    systems = json.load(f)

loggers = {}
for system in systems:
    loggers[system['name']] = logging.getLogger(system['name'])
    
    
def update_station_status_raw(system):
    loggers[system['name']].info(f"Updating station bikes")
    # Query stations and save to temp table
    ddf_file = f"tracker-data/{system['name']}/raw.station.parquet"
    try:
        ddf = pd.read_parquet(ddf_file)
    except:
        ddf = None
    try:
        ddf_query = query_station_status(system['url'])
        ddf_query['datetime'] = ddf_query['datetime'].dt.tz_convert(system['tz'])
        # ddf_query['station_id'] = ddf_query['station_id'].astype(str)
        
        
        
        ddf = pd.concat([ddf,ddf_query])
        
        
        
        
    except Exception as e:
        loggers[system['name']].debug(f"gbfs query error, skipping stations_raw db update: {e}")
        return 
        
    ddf.to_parquet(ddf_file,index=False)
    
def update_free_bike_status_raw(system):
    loggers[system['name']].info(f"Updating free bikes")
    bdf_file = f"tracker-data/{system['name']}/raw.free_bike.parquet"
    try:
        bdf = pd.read_parquet(bdf_file)
    except:
        bdf = None
    try:
        bdf_query = query_free_bike_status(system['url'])
        bdf_query['datetime'] = bdf_query['datetime'].dt.tz_convert(system['tz'])
        bdf = pd.concat([bdf,bdf_query])
        
    except Exception as e:
        loggers[system['name']].debug(f"gbfs query error, skipping free_bikes_raw db update: {e}")
        return 


    bdf.to_parquet(bdf_file,index=False)

def map_station_id_to_station(station_id,system,session):
    """
    Given a station_id string and a system obj, return a station ORM object
    with the latest 'created_by' date for that station_id and system
    """
    
    qry = session.query(Station).join(System).filter(System.name==system.name)
    qry = qry.filter(Station.station_id==station_id).order_by(Station.created_date.desc())
    
    return qry.first()
    
def update_trips(system,feed_type,save_temp_data=False):
    
    """
    Pulls raw data from raw files, computes trips, saves trip data to file
    """
    

    loggers[system['name']].info(f"Updating tables: {feed_type}")

    if feed_type == 'station':
        ## Compute hourly station trips, append to trips table

        try:
            ddf = pd.read_parquet(f"tracker-data/{system['name']}/raw.station.parquet")  
            thdf = make_station_trips(ddf)        
        except Exception as e:
            loggers[system['name']].debug(f"Skipping station trips update: {e}")
            return 
            
            
        
    elif feed_type == 'free_bike':
        ## Compute hourly free bike trips, append to trips table
        try:
            bdf = pd.read_parquet(f"tracker-data/{system['name']}/raw.free_bike.parquet")  
            thdf = make_free_bike_trips(bdf)
        except Exception as e:
            loggers[system['name']].debug(f"Skipping free_bike trips update: {e}")
            return

 
    

        
    
    year_tag = thdf['datetime'].iloc[0].strftime('%Y')
    # Add rows to measurements table
    try:
        thdf_historical = load_parquet(system,year_tag,feed_type)
    except:
        thdf_historical = None

    thdf = pd.concat([thdf_historical,thdf])
    thdf = thdf.groupby(['datetime','station_id','vehicle_type_id'],dropna=False).agg({
                                                       'returns':'sum',
                                                       'trips':'sum'})
    thdf = thdf.reset_index()
    
    # Drop records in raw tables except for most recent query
    try:
        trim_raw(f"tracker-data/{system['name']}/raw.{feed_type}.parquet")
    except FileNotFoundError:
        pass

    
    # Save
    save_to_parquet(system,thdf,feed_type)
    
    


def load_parquet(system,year_tag,feed_type):
    outpath=pathlib.Path(f"tracker-data/{system['name']}/")
    return pd.read_parquet(f"{outpath}/trips.{feed_type}.hourly.{year_tag}.parquet")
    
    
def save_to_parquet(system,thdf,feed_type):
    
    outpath=pathlib.Path(f"tracker-data/{system['name']}/")
    outpath.mkdir(parents=True,exist_ok=True)
    
    years = set(thdf['datetime'].dt.year)
    
    for year_tag in years:


        outfile = outpath.joinpath(f"trips.{feed_type}.hourly.{year_tag}.parquet")         
        thdf[thdf['datetime'].dt.year==year_tag].to_parquet(outfile, index=False)
        

        # Also groupby day for faster querying
        tddf = thdf.set_index('datetime').groupby([pd.Grouper(freq='d'),'station_id','vehicle_type_id'],dropna=False).sum().reset_index()
        outfile = outpath.joinpath(f"trips.{feed_type}.daily.{year_tag}.parquet")
        tddf.to_parquet(outfile, index=False)


def trim_raw(fname):
    """
    Only keep the latest query, drop older queries
    """
    
    try:
        df = pd.read_parquet(fname)
    except FileNotFoundError:
        return
    
    df = df[df['datetime']== df.iloc[-1].loc['datetime']]
    
    df.to_parquet(fname,index=False)
    
        
 
    
def make_station_trips(ddf):
    
    if len(ddf) == 0:
        return pd.DataFrame()
    
    
    
    pdf = pd.pivot_table(ddf,columns=['station_id','vehicle_type_id'],index='datetime',values='num_bikes_available',dropna=False)
    df = pdf.copy()
    for col in pdf.columns:
        df[col] = pdf[col] - pdf[col].shift(-1)
    df = df.fillna(0.0).astype(int)
    
    #df_stack = df.stack(level=0).reset_index()
    df_stack = df.stack(future_stack=True).stack(future_stack=True).reset_index()
    df_stack.columns = ['datetime','vehicle_type_id','station_id','trips']

    df_stack['returns'] = df_stack['trips']
    df_stack.loc[df_stack['returns']<0,'returns'] = 0

    df_stack.loc[df_stack['trips']>0,'trips'] = 0
    df_stack['trips'] = -1*df_stack['trips']

    df_stack = df_stack.set_index('datetime').groupby([pd.Grouper(freq='h'),'station_id','vehicle_type_id'],dropna=False).sum().reset_index()
    
    # Add available bikes and docks
    # num_bikes_xw = ddf.groupby('station_id').max()['num_bikes_available'].to_dict()
    # num_docks_xw = ddf.groupby('station_id').max()['num_docks_available'].to_dict()

    # df_stack['num_bikes_available'] = df_stack['station_id'].map(num_bikes_xw)
    # df_stack['num_docks_available'] = df_stack['station_id'].map(num_docks_xw)
    
    return df_stack


def make_free_bike_trips(bdf):
    """
    This handles both cases allowed by the GBFS spec: populated station_id, or populated lat/lon. In either case,
    the populated field is treated as a "station" and trips are measured as bikes come and go.
    """
    
    if len(bdf) == 0:
        return pd.DataFrame()
    
    #bdf = bdf.drop_duplicates(['station_id','vehicle_id','datetime','lat','lon']) # resolves issue where bikes are double counted
    
    # ddf = bdf.groupby(['datetime','station_id','lat','lon','vehicle_type_id'],dropna=False)['vehicle_id'].count()
    # ddf = ddf.reset_index()
    # ddf = ddf.rename(columns={'vehicle_id':'num_bikes_available'})
    bdf['latlon'] = list(zip(bdf['lat'],bdf['lon']))
    pivot_col = 'latlon' if len(set(bdf['latlon'])) > len(set(bdf['station_id'].fillna(0))) else 'station_id'
    
    pdf = pd.pivot_table(bdf,columns=[pivot_col,'vehicle_type_id'],
                         index='datetime',values='num_bikes_available',
                         dropna=False)
    df = pdf.copy()
    for col in pdf.columns:
        df[col] = pdf[col] - pdf[col].shift(-1)
    df = df.fillna(0.0).astype(int)

    
    df_stack = df.stack(future_stack=True).stack(future_stack=True).reset_index()
    df_stack.columns = ['datetime','vehicle_type_id','station_id','trips']
    df_stack['station_id'] = None if pivot_col == 'latlon' else df_stack['station_id']

    df_stack['returns'] = df_stack['trips']
    df_stack.loc[df_stack['returns']<0,'returns'] = 0

    df_stack.loc[df_stack['trips']>0,'trips'] = 0
    df_stack['trips'] = -1*df_stack['trips']
    
    # if pivot_col == 'latlon':
    #     df_stack['station_id'] = None
    #     num_bikes_xw = bdf.groupby('station_id',dropna=False).agg({'num_bikes_available':'sum'}).to_dict()
    # else:
    #     num_bikes_xw = bdf.groupby('station_id',dropna=False).agg({'num_bikes_available':'max'}).to_dict()
    
    df_stack = df_stack.set_index('datetime').groupby([pd.Grouper(freq='h'),'station_id','vehicle_type_id'],dropna=False).sum().reset_index()
    # 
    # # Add available bikes and docks
    # num_bikes_xw = ddf.groupby('station_id',dropna=False).max()['num_bikes_available'].to_dict()

    # df_stack['num_bikes_available'] = df_stack['station_id'].map(num_bikes_xw)
    # df_stack['num_docks_available'] = 0
    
    return df_stack

# def make_free_bike_trips_old(bdf):
#     if len(bdf) == 0:
#         return pd.DataFrame()
    

#     n_bikes = 0
#     bikes = {} # set of bike_ids at each datetime
#     for t,df in bdf.groupby(['datetime','station_id'],dropna=False):
#         active_bikes = set([(vid,lat,lon) for vid,lat,lon in df[['vehicle_id','lat','lon']].itertuples(index=False)])
#         bikes[t] = active_bikes
#         n_bikes = n_bikes if len(active_bikes) < n_bikes else len(active_bikes)
        
#     t = {}
    

    
#     timestamps = sorted(set([x for x,y in bikes.keys()]))
#     station_ids = sorted(set([y for x,y in bikes.keys()]))
#     df = pd.DataFrame()
    
#     for station_id in station_ids:
#         for i,ts in enumerate(timestamps):
#             print(station_id,ts)
#             if i==len(timestamps)-1:
#                 continue
            
#             if station_id != '0001':
#                 sys.exit()
#             print("Bikes at timepoint: ", bikes[(ts,station_id)])
#             print("Bikes at timepoint + 1", bikes[(timestamps[i+1],station_id)])
#             trips_started = len(bikes[(ts,station_id)].difference(bikes[(timestamps[i+1],station_id)]))
#             trips_ended = len(bikes[(timestamps[i+1],station_id)].difference(bikes[(ts,station_id)]))

#             t[i] = {'trips':trips_started,'returns':trips_ended,
#                     'datetime':ts,'station_id':station_id,
#                     'num_bikes_available':n_bikes,
#                    'num_docks_available':None}

    
#         sdf = pd.DataFrame(t).T
#         sdf = sdf.set_index('datetime')
#         sdf.index = pd.to_datetime(sdf.index)
#         df = pd.concat([df,sdf])
#         print(df)
#     df = df.groupby([pd.Grouper(freq='h'),'station_id'],dropna=False).sum()
      
#     df = df.reset_index()
    
#     return df

def check_tracking_start(system):
    data_file = f"tracker-data/{system['name']}/trips.*.parquet"
    qry = duckdb.query("""select min(datetime) from read_parquet('tracker-data/bixi_montreal/trips*parquet')""")
    return qry.fetchall()[0][0]

def get_vehicle_types(system):
    vehicles_file = f"tracker-data/{system['name']}/vehicle_types.parquet"
    try:
        vdf_current = pd.read_parquet(vehicles_file)
        return list(vdf_current['vehicle_type_id'])
    except:
        return [None]
    
def update_vehicle_types(system):
    """
    Update vehicle types table
    """
    loggers[system['name']].info(f"Vehicle Types Update")
    
    #-- Load vehicle types file if exists
    vehicles_file = f"tracker-data/{system['name']}/vehicle_types.parquet"
    vehicles_file_bak = f"tracker-data/{system['name']}/vehicles_BAK.parquet"
    try:
        vdf_current = pd.read_parquet(vehicles_file)
    except:
        vdf_current = None

    #-- Query stations
    try:
        vdf = query_vehicle_types(system['url'])
    except Exception as e:
        loggers[system['name']].info(f"Unable to load vehicle types")
        loggers[system['name']].debug(f"{e}")
        return        
    
    
        
    #-- Save stations file
    try:
        os.rename(vehicles_file,vehicles_file_bak)
    except FileNotFoundError:
        pass
    vdf.to_parquet(vehicles_file,index=False)
    
    loggers[system['name']].info(f"Vehicle Type Update Complete")
    
    
def update_stations(system):
    """
    Update stations table
    Adds station if doesn't exist, updates active status
    """
    loggers[system['name']].info(f"Station Update")
    
    
    #-- Load stations file if exists
    stations_file = f"tracker-data/{system['name']}/stations.parquet"
    stations_file_bak = f"tracker-data/{system['name']}/stations_BAK.parquet"
    try:
        sdf_current = pd.read_parquet(stations_file)
    except:
        sdf_current = None

    #-- Query stations
    try:
        sdf = query_station_info(system['url'])
    except Exception as e:
        loggers[system['name']].debug(f"Failed to load stations: {e}")
        return 
    
    #-- Query station status
    try:
        ddf = query_station_status(system['url'])
    except Exception as e:
        loggers[system['name']].debug(f"Failed to load station status: {e}")
        return 
    
    
    #-- Add any legacy stations that aren't in current station query
    if sdf_current is not None:

        legacy_stations_df = sdf_current[~sdf_current['station_id'].isin(sdf['station_id'])]
        if len(legacy_stations_df) > 0:
            legacy_stations_df.loc[:,'active'] = False
        sdf = pd.concat([sdf,legacy_stations_df])
    

    

    #-- Run through station status data to label disabled stations
    sdf[sdf['station_id'].isin(ddf['is_renting']==0)]['active'] = False
    
    
    #-- Save stations file
    try:
        os.rename(stations_file,stations_file_bak)
    except FileNotFoundError:
        pass
    sdf.to_parquet(stations_file,index=False)
    
    loggers[system['name']].info(f"Station Update Complete")
    
    
        