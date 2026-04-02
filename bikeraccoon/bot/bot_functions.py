import datetime as dt
import bikeraccoon as br
import bikeraccoon.bot.plots as plots
import matplotlib.pyplot as plt
import pandas as pd
import os
from glob import glob
import sys
import json

from atproto import Client, models, client_utils


def post_bsky(account,text,images=[],descriptions=[],hashtags=[],credentials_file=None):
    client = Client()
    
    # account = 'montreal.raccoon.bike'
    
    # with open() as f:
    with open(credentials_file) as f:
        account_data = json.load(f)
        pw = account_data['accounts'][account]
    
    client.login(account,pw)
    
    
    bsky_images = []
    
    for image,description in zip(images,descriptions):
        with open(image,'rb') as f:
            img_data = f.read()
    
        upload = client.com.atproto.repo.upload_blob(img_data)
        bsky_images.append(models.AppBskyEmbedImages.Image(alt=description, image=upload.blob))
        
    embed = models.AppBskyEmbedImages.Main(images=bsky_images)
    
    output_string = client_utils.TextBuilder()
    output_string.text(text)
    for hashtag in hashtags:
        output_string.tag(hashtag,hashtag[1:]).text(' ')
        
    client.send_post(text=output_string, embed=embed)


def check_zero_trips(t1,t2,api,m=0):
    # Get yesterday's data
    # t1 = yday.replace(hour=0)
    # t2 = yday.replace(hour=23)
    thdf = api.get_station_trips(t1,t2,freq='d')

    ntrips = thdf['trips'].sum()

    if ntrips <= m:
        return True
    else:
        return False

def make_tweet_text(api,t1,path='./', lang='EN'):

    t1 = t1.replace(hour=0,minute=0,second=0)
    t2 = t1 + dt.timedelta(hours=23)
        
    sdf = api.get_stations()
    thdf = api.get_station_trips(t1,t2,freq='d',station='all')
    thdf_sdf = pd.merge(sdf,thdf,how='inner',on='station_id')
    ntrips = thdf_sdf['trips'].sum()

    sdf = sdf[sdf['active']]

    thdf_sdf = thdf_sdf.sort_values('trips', ascending=False)
    busiest_station = thdf_sdf['name'].iloc[0]
    busiest_station = busiest_station.split('(')[0].strip() # trip parentheticals to shorten name
    busiest_station_trips = int(thdf_sdf['trips'].iloc[0])
    n_busiest_stations = len(thdf_sdf[thdf_sdf['trips']==busiest_station_trips])

    plural = "" if n_busiest_stations == 2 else "s"

    if n_busiest_stations > 1:
        n_busiest_str = f" and {n_busiest_stations-1} other{plural}"
    else:
        n_busiest_str = ""

    least_busy_station = thdf_sdf['name'].iloc[-1]
    least_busy_station = least_busy_station.split('(')[0].strip() # trip parentheticals to shorten name
    least_busy_station_trips = thdf_sdf['trips'].iloc[-1]

    n_least_busy_stations = len(thdf_sdf[thdf_sdf['trips']==least_busy_station_trips])
    plural = "" if n_least_busy_stations == 2 else "s"
    if n_least_busy_stations > 1:
        if lang=='EN':
            n_least_busy_str = f" and {n_least_busy_stations-1} other{plural}"  
        elif lang=='FR':
            n_least_busy_str = f" et {n_least_busy_stations-1} autre{plural}"
    else:
        n_least_busy_str = ""

    active_stations = len(sdf)

    if lang=='EN':
        s = f"""Yesterday there were approximately {ntrips:,} {api.brand} bikeshare trips
Most used station: {busiest_station}{n_busiest_str} ({busiest_station_trips} trips)
Least used station: {least_busy_station}{n_least_busy_str} ({least_busy_station_trips} trips)
Active stations: {active_stations}
"""
        
    elif lang=='FR':
        s=f"""Hier, il y a eu approximativement {ntrips:,} déplacements en vélopartage {api.brand}.
Station la plus utilisée: {busiest_station}{n_busiest_str} ({busiest_station_trips} déplacements)
Station la moins utilisée: {least_busy_station}{n_least_busy_str} ({least_busy_station_trips} déplacements)
Stations actives: {active_stations}"""

    print(s)
    with open(f'{path}/{api.sys_name}_bot_text.txt','w') as ofile:
        ofile.write(s)
    
    
    
# Last month daily 
def make_monthly_trips_plot(api,t1,path='./',weather=True):
    t1 = t1.replace(hour=23,minute=0,second=0)
    t2 = t1 - dt.timedelta(days=31)
    trips = api.get_station_trips(t1,t2,freq='d')['trips']
    
    

    if weather:
        f,ax = plt.subplots(2,sharex=True,gridspec_kw={'height_ratios':[4.5,1]})
        plots.plot_daily_trips(api,api.sys_type,trips,ax=ax[0],palette=api.palette,weather=weather)

        try:
            plots.plot_daily_weather(api,t1,t2,ax[1])
        except Exception as e:
            sys.stderr.write(f"Unable to create weather subplot\n{e}\n")
            weather=False
    if not weather:
        f,ax = plt.subplots()
        plots.plot_daily_trips(api,api.sys_type,trips,ax=ax,palette=api.palette,weather=weather)


            
    f.tight_layout()
    f.savefig(f"{path}/2.{api.sys_name}_last_month.png")


# Last week
def make_weekly_trips_plot(api,t1,path='./',weather=True):
    
    
    t1 = t1.replace(hour=23,minute=0,second=0)
    t2 = t1 - dt.timedelta(days=7,hours=23)
    trips = api.get_station_trips(t1,t2)['trips']
    
    if weather:
        f,ax = plt.subplots(2,sharex=True,gridspec_kw={'height_ratios':[4.5,1]})
        plots.plot_hourly_trips(api,api.sys_type,trips,ax=ax[0],palette=api.palette,weather=weather)

        try:
            plots.plot_hourly_weather(api,t1,t2,ax[1])
        except Exception as e:
            sys.stderr.write(f"Unable to create weather subplot\n{e}\n")
            weather=False

        
    if not weather:
        f,ax = plt.subplots()
        plots.plot_hourly_trips(api,api.sys_type,trips,ax=ax,palette=api.palette,weather=weather)

    f.tight_layout()
    f.savefig(f"{path}/1.{api.sys_name}_last_week.png")

# All time
def make_alltime_plot(api,t1,path='./'):
    t1 = t1.replace(hour=23,minute=0,second=0)
    t2 = api.get_system_info()['tracking_start']
    trips = api.get_station_trips(t1,t2, freq='d')['trips']
    
    f,ax = plt.subplots()
    plots.plot_alltime_trips(api,trips,api.sys_type,ax=ax, palette=api.palette)
    f.tight_layout()
    f.savefig(f"{path}/3.{api.sys_name}_alltime.png")
           
# Stations
def make_stations_map(api,t1,path='./'):
    t1 = t1.replace(hour=0)
    t2 = t1.replace(hour=23)
    thdf = api.get_station_trips(t1,t2,freq='d',station='all')
    print(thdf)
    f,ax = plots.plot_stations(api,thdf,extent=api.extent, palette=api.palette)
    f.savefig(f'{path}/4.{api.sys_name}_stations.png',
                  bbox_inches='tight',pad_inches=0.0,transparent=False,dpi=100)
