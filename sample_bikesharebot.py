

import datetime as dt
import time
import bikeraccoon as br

import matplotlib.pyplot as plt
import pandas as pd
import os
from glob import glob
import random
import sys

import bikeraccoon.bot as brbot



###################################
# EDIT THESE LINES

sys_name = 'mobi_vancouver'
lang='EN'
account_name = 'VanBikeShareBot'
brand = 'Mobi'
hashtag = '#bikeyvr'
DARKSKY_KEY = ''
MAPBOX_TOKEN=''
palette = ('#77ACA2','#3286AD')
sys_type = 'stations'
path = f'.'  # working dir

lon_min = -123.185
lon_max = -123.056
lat_min = 49.245
lat_max = 49.315
extent = [lon_min,lon_max,lat_min,lat_max]

earliest = 9 # server time
latest = 11  # server time

#####################################
# CONNECT TO API

api = br.LiveAPI(sys_name, echo=True)


# Add values to the api object for easy passing
api.DARKSKY_KEY = DARKSKY_KEY
api.MAPBOX_TOKEN = MAPBOX_TOKEN
api.hashtag = hashtag
api.palette = palette
api.sys_type = sys_type
api.brand = brand
api.sys_name = sys_name
api.extent = extent



########################################################
# Make figures

if not os.path.exists(f'{path}'):
    os.makedirs(f'{path}')

# Clear older files
for x in glob(f'{path}/*'):
    os.remove(x)    


    
brbot.make_tweet_text(api,path,lang=lang)
brbot.make_weekly_trips_plot(api,path)
brbot.make_monthly_trips_plot(api,path)
brbot.make_alltime_plot(api,path)
brbot.make_stations_map(api,path)



           