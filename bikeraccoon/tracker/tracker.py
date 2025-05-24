import time
import pandas as pd
import pyarrow as pa
import datetime as dt
import sys
import json
import logging
from logging.handlers import TimedRotatingFileHandler
import pathlib
from multiprocessing import Pool

from .tracker_functions import *







def get_system_time(system):
    return pd.Timestamp(dt.datetime.utcnow()).tz_localize('UTC').tz_convert(system['tz'])




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

def update_system_raw(system):
    if not system['tracking']:
        return
       
    
    
    loggers[system['name']].info("querying GBFS info")
    
    
    if system.get('track_stations',True):
        try:
            update_station_status_raw(system)
        except Exception as e:
            loggers[system['name']].info(f"failed station update \n{e}")
    
    
        # try:
        #     update_station_status_vehicle_type_raw(system)
        # except Exception as e:
        #     loggers[system['name']].info(f"failed vehicle type update \n{e}")
        
    else:
        loggers[system['name']].info("skipping station check")
        
        
    if system.get('track_free_bikes',True):
        try:
            update_free_bike_status_raw(system)
        except Exception as e:
            loggers[system['name']].info(f"failed free bike update \n{e}")
    
        # try:
        #     update_free_bikes_vehicle_type_raw(system)
        # except Exception as e:
        #     loggers[system['name']].info(f"failed free bike vehicle type update \n{e}")
    
    
    else:
        loggers[system['name']].info(f"skipping free bike check")
            
            
        
    system['tracking_end'] = dt.datetime.utcnow() # Last update

def update_system(system):
    if not system['tracking']:
        return False
    
    vehicle_types = get_vehicle_types(system)
    for feed_type in ['station','free_bike']:
        update_trips(system,feed_type)
        
        
    if get_system_time(system).hour == 4: # check stations at 4am local time
        loggers[system['name']].info(f"updating stations")
        update_stations(system)
        update_vehicle_types(system)


    return True

def tracker(systems_file='systems.json',log_path=None,
            update_interval=20, query_interval=20, station_check_hour=4,
            save_temp_data=False):
    
    ## SETUP LOGGING
    if log_path is not None:
        log_path = pathlib.Path(log_path)
        log_path.mkdir(parents=True,exist_ok=True)
    logger = setup_logger('Tracker', log_path=log_path, log_name='tracker.log')
    
    
    ## Setup 
    last_update = dt.datetime.now()
    query_time = dt.datetime.now()
    update_delta = dt.timedelta(minutes=update_interval)
    

    loggers = {}

    ## Do an initial station update on startup
    with open('systems.json') as f:
        systems = json.load(f)

    for system in systems:
        
        # setup system-specific logger
        loggers[system['name']] = setup_logger(system['name'], log_path=log_path, log_name=f"{system['name']}.log")
        
        if system['tracking']:
            
            pathlib.Path(f'./tracker-data/{system['name']}').mkdir(parents=True, exist_ok=True)
            
            update_stations(system)
            update_vehicle_types(system)
            try:
                system['tracking_start'] = check_tracking_start(system).strftime('%Y-%m-%d %H:%M:%S')
                loggers[system['name']].debug("Updated system tracking_start attribute")
            except:
                loggers[system['name']].debug("Unable to check earliest tracking date")
    logger.info("Daemon started successfully")
    
    while True:
        
        
        if dt.datetime.now() < query_time:
            time.sleep(1)  # Check whether it's time to update every second (actual query interval time determined by 
            continue
        else:
            query_time = dt.datetime.now() + dt.timedelta(seconds=query_interval)

        logger.info(f"start: {dt.datetime.now()}")

        
        # for system in systems:
        #     update_system_raw(system) # GBFS query and update raw files
        with Pool(4) as p:
            p.map(update_system_raw, systems)

        
        if dt.datetime.now() >  last_update + update_delta:
            last_update = dt.datetime.now()
            
            # for system in systems:
            #     update_system(system)
            with Pool(4) as p:
                res = p.map(update_system,systems)
                        
            # Update tracking end datetime if update_system completes
            with open('systems.json','w') as f:
                for b, system in zip(res,systems):
                    if b:
                
                        system['tracking_end'] = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                json.dump(systems,f,indent=4)           
        
        
        logger.info(f"end: {dt.datetime.now()}")
        logger.debug(f"Next DB update: {last_update + update_delta}")


if __name__ == '__main__':
    tracker()
