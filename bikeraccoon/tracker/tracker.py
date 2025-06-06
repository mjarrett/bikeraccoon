import time
import datetime as dt
import sys
import json
import logging
import pathlib
from multiprocessing import Pool

from .tracker_functions import *



def update_system_raw(system):
    if not system['tracking']:
        return
       
    
    system.logger.info("querying GBFS info")
    
    
    if system.get('track_stations',True):
        try:
            update_station_status_raw(system)
        except Exception as e:
            system.logger.info(f"failed station update \n{e}")
    
    
        # try:
        #     update_station_status_vehicle_type_raw(system)
        # except Exception as e:
        #     system.logger.info(f"failed vehicle type update \n{e}")
        
    else:
        system.logger.info("skipping station check")
        
        
    if system.get('track_free_bikes',True):
        try:
            update_free_bike_status_raw(system)
        except Exception as e:
            system.logger.info(f"failed free bike update \n{e}")
    
        # try:
        #     update_free_bikes_vehicle_type_raw(system)
        # except Exception as e:
        #     system.logger.info(f"failed free bike vehicle type update \n{e}")
    
    
    else:
        system.logger.info(f"skipping free bike check")
            
            
        
    system['tracking_end'] = dt.datetime.utcnow() # Last update

def update_system(system):
    if not system['tracking']:
        return False
    
    vehicle_types = get_vehicle_types(system)
    for feed_type in ['station','free_bike']:
        update_trips(system,feed_type)
        
        
    if system.get_system_time().hour == system.station_check_hour: # check stations at 4am local time
        system.logger.info(f"updating stations")
        update_stations(system)
        update_vehicle_types(system)
        
    update_system_table(system)
    
    return True

def tracker(systems_file='systems.json',log_path=None,data_path='tracker-data',
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
    
    ## Load stations from json file into list of System objects
    with open(systems_file) as f:
        systems = json.load(f)
        systems = [GBFSSystem(x) for x in systems]
        
    
    ## Initial Setup
    for system in systems:
        system.set_logger(log_path)
        system.data_path = f'{data_path}/{system['name']}/'
        system.station_check_hour = station_check_hour
        system.check_url()

        # Set up system table
        update_system_table(system)
        
        
        
        if system['tracking']:
            
            pathlib.Path(f"{system.data_path}").mkdir(parents=True, exist_ok=True)
            
            update_stations(system)
            update_vehicle_types(system)

    logger.info("Daemon started successfully")
    
    while True:
        
        
        if dt.datetime.now() < query_time:
            time.sleep(1)  # Check whether it's time to update every second (actual query interval time determined by 
            continue
        else:
            query_time = dt.datetime.now() + dt.timedelta(seconds=query_interval)

        logger.info(f"start: {dt.datetime.now()}")


        with Pool(4) as p:
            p.map(update_system_raw, systems)

        
        if dt.datetime.now() >  last_update + update_delta:
            last_update = dt.datetime.now()
            

            with Pool(4) as p:
                res = p.map(update_system,systems)
                        
                    
        
        
        logger.info(f"end: {dt.datetime.now()}")
        logger.debug(f"Next DB update: {last_update + update_delta}")


if __name__ == '__main__':
    tracker()
