import pandas as pd
import json
import pathlib

from .. import gbfs

import logging
from logging.handlers import TimedRotatingFileHandler

import datetime as dt
from zoneinfo import ZoneInfo
from collections import UserDict
import os
import sys     
import duckdb

#-- Get logger
logger = logging.getLogger('Tracker')




class GBFSSystem(UserDict):

    def set_logger(self,log_path):        
        
        # setup system-specific logger
        self.logger = setup_logger(self['name'], log_path=log_path, log_name=f"{self['name']}.log")

        
    
    def check_url(self):
        """If url is working, keep it. Otherwise check systems.csv"""
        if 'url' in self.keys() and gbfs.check_gbfs_url(self['url']):
            return
        self['url'] = self.get_gbfs_url()

    def get_gbfs_url(self):
        if 'gbfs_system_id' not in self.keys():
            return
        try:
            df = pd.read_csv('https://raw.githubusercontent.com/MobilityData/gbfs/refs/heads/master/systems.csv')
            system = df[df['System ID'] == self['gbfs_system_id']].to_dict('records')[0]
            new_url =  system['Auto-Discovery URL']
            self.logger.info(f'Setting GBFS source URL: {new_url}')
            return new_url
        except Exception as e:
            print(e)
            return 
    
    def get_system_time(self):
        return dt.datetime.now(ZoneInfo(self['tz']))


    def update_tracking_range(self):
        if 'tracking_start' not in self.keys():
            try:
                self['tracking_start'] = check_tracking_start(self)
            except:
                self['tracking_start'] = None

        try:
            self['tracking_end'] = check_tracking_end(self)
        except:
            self['tracking_end'] = None
            
    def to_parquet(self):
        path = pathlib.Path(f"{self.data_path}/system.parquet")
        path.parent.mkdir(parents=True, exist_ok=True)
        df = pd.DataFrame([self])
        df.to_parquet(path,index=False)


def setup_logger(name, log_path, log_name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s')
    
    if log_path is not None:
        log_file = log_path.joinpath(log_name)
        handler = TimedRotatingFileHandler(log_file,
                                       when="d",
                                       interval=1,
                                       backupCount=5)

        handler.setFormatter(formatter)
        logger.addHandler(handler)
    
    streamhandler = logging.StreamHandler()
    streamhandler.setFormatter(formatter)
    logger.addHandler(streamhandler)
    return logger

def update_system_table(system):

    # Add anything here you want updated reqularly
    system.update_tracking_range()    
    system.to_parquet()

    
    
def update_station_status_raw(system):
    system.logger.info(f"Updating station bikes")
    # Query stations and save to temp table
    ddf_file = f"{system.data_path}/raw.station.parquet"
    try:
        ddf = pd.read_parquet(ddf_file)
    except:
        ddf = None
    try:
        ddf_query = gbfs.query_station_status(system['url'])
        ddf_query['datetime'] = ddf_query['datetime'].dt.tz_convert(system['tz'])
        # ddf_query['station_id'] = ddf_query['station_id'].astype(str)
        
        
        
        ddf = pd.concat([ddf,ddf_query])
        
        
        
        
    except Exception as e:
        system.logger.debug(f"gbfs query error, skipping stations_raw db update: {e}")
        return 
        
    ddf.to_parquet(ddf_file,index=False)
    
def update_free_bike_status_raw(system):
    system.logger.info(f"Updating free bikes")
    bdf_file = f"{system.data_path}/raw.free_bike.parquet"
    try:
        bdf = pd.read_parquet(bdf_file)
    except:
        bdf = None
    try:
        bdf_query = gbfs.query_free_bike_status(system['url'])
        bdf_query['datetime'] = bdf_query['datetime'].dt.tz_convert(system['tz'])
        bdf = pd.concat([bdf,bdf_query])
        
    except Exception as e:
        system.logger.debug(f"gbfs query error, skipping free_bikes_raw db update: {e}")
        return 


    bdf.to_parquet(bdf_file,index=False)


    
def update_trips(system,feed_type,save_temp_data=False):
    
    """
    Pulls raw data from raw files, computes trips, saves trip data to file
    """
    
    system.logger.info(f"Updating tables: {feed_type}")

    if feed_type == 'station':
        ## Compute hourly station trips, append to trips table

        try:
            ddf = pd.read_parquet(f"{system.data_path}/raw.station.parquet")  
            thdf = make_station_trips(ddf)        
        except Exception as e:
            system.logger.debug(f"Skipping station trips update: {e}")
            return 
            
            
        
    elif feed_type == 'free_bike':
        ## Compute hourly free bike trips, append to trips table
        try:
            bdf = pd.read_parquet(f"{system.data_path}/raw.free_bike.parquet")  
            thdf = make_free_bike_trips(bdf)
        except Exception as e:
            system.logger.debug(f"Skipping free_bike trips update: {e}")
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
        trim_raw(f"{system.data_path}/raw.{feed_type}.parquet")
    except FileNotFoundError:
        pass

    
    # Save
    save_to_parquet(system,thdf,feed_type)
    
    


def load_parquet(system,year_tag,feed_type):
    outpath=pathlib.Path(f"{system.data_path}/")
    return pd.read_parquet(f"{outpath}/trips.{feed_type}.hourly.{year_tag}.parquet")
    
    
def save_to_parquet(system,thdf,feed_type):
    
    outpath=pathlib.Path(f"{system.data_path}/")
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
    
    df_stack = df.stack(future_stack=True).stack(future_stack=True).reset_index()
    df_stack.columns = ['datetime','vehicle_type_id','station_id','trips']

    df_stack['returns'] = df_stack['trips']
    df_stack.loc[df_stack['returns']<0,'returns'] = 0

    df_stack.loc[df_stack['trips']>0,'trips'] = 0
    df_stack['trips'] = -1*df_stack['trips']

    df_stack = df_stack.set_index('datetime').groupby([pd.Grouper(freq='h'),'station_id','vehicle_type_id'],dropna=False).sum().reset_index()

    
    return df_stack


def make_free_bike_trips(bdf):
    """
    This handles both cases allowed by the GBFS spec: populated station_id, or populated lat/lon. In either case,
    the populated field is treated as a "station" and trips are measured as bikes come and go.
    """
    
    if len(bdf) == 0:
        return pd.DataFrame()
    

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
    

    
    df_stack = df_stack.set_index('datetime').groupby([pd.Grouper(freq='h'),'station_id','vehicle_type_id'],dropna=False).sum().reset_index()

    return df_stack



def check_tracking_start(system):
    
    try:
        data_file = f"{system.data_path}/trips.*.hourly.*.parquet"
        qry = duckdb.query(f"""select min(datetime) from read_parquet('{data_file}')""")
        return qry.fetchall()[0][0]
    except:
        return  None

def check_tracking_end(system):
    try:
        data_file = f"{system.data_path}/trips.*.hourly.*.parquet"
        qry = duckdb.query(f"""select max(datetime) from read_parquet('{data_file}')""")
        return qry.fetchall()[0][0]
    except:
        return None


def get_vehicle_types(system):
    vehicles_file = f"{system.data_path}/vehicle_types.parquet"
    try:
        vdf_current = pd.read_parquet(vehicles_file)
        return list(vdf_current['vehicle_type_id'])
    except:
        return [None]
    
def update_vehicle_types(system):
    """
    Update vehicle types table
    """
    system.logger.info(f"Vehicle Types Update")
    
    #-- Load vehicle types file if exists
    vehicles_file = f"{system.data_path}/vehicle_types.parquet"
    vehicles_file_bak = f"{system.data_path}/vehicles_BAK.parquet"
    try:
        vdf_current = pd.read_parquet(vehicles_file)
    except:
        vdf_current = None

    #-- Query stations
    try:
        vdf = gbfs.query_vehicle_types(system['url'])
    except Exception as e:
        system.logger.info(f"Unable to load vehicle types")
        system.logger.debug(f"{e}")
        return        
    
    
        
    #-- Save stations file
    try:
        os.rename(vehicles_file,vehicles_file_bak)
    except FileNotFoundError:
        pass
    vdf.to_parquet(vehicles_file,index=False)
    
    system.logger.info(f"Vehicle Type Update Complete")
    
    
def update_stations(system):
    """
    Update stations table
    Adds station if doesn't exist, updates active status
    """
    system.logger.info(f"Station Update")
    
    
    #-- Load stations file if exists
    stations_file = f"{system.data_path}/stations.parquet"
    stations_file_bak = f"{system.data_path}/stations_BAK.parquet"
    try:
        sdf_current = pd.read_parquet(stations_file)
    except:
        sdf_current = None

    #-- Query stations
    try:
        
        sdf = gbfs.query_station_info(system['url'])
        sdf['active'] = True
    except Exception as e:
        system.logger.debug(f"Failed to load stations: {e}")
        return 
    
    #-- Query station status
    try:
        ddf = gbfs.query_station_status(system['url'])
    except Exception as e:
        system.logger.debug(f"Failed to load station status: {e}")
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
    
    system.logger.info(f"Station Update Complete")
    
    
        
