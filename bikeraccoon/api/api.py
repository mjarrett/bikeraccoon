#!/usr/bin/env python3

from flask_cors import CORS
from flask import (Flask, request, make_response, 
                    send_from_directory,render_template,jsonify)

import json
import hashlib
import sqlite3
import pytz
import datetime as dt
import itertools
import os
import requests
import pyarrow.parquet as pq

from .api_functions import *

from .. import gbfs


app = Flask(__name__,template_folder='../templates/')

app.json_provider_class = BRJSONProvider 
app.json = BRJSONProvider(app)
app.json.compact = False

CORS(app) #Prevents CORS errors 




    
@app.route('/favicon.ico')
def favicon():
    return send_from_directory(os.path.join(app.root_path, 'static'),
                          'favicon.ico',mimetype='image/vnd.microsoft.icon')    


@app.route('/')
def default():
    return render_template("frontpage.html")

@app.route('/tests')
def tests():
    return render_template("tests.html")



@app.route('/systems', methods=['GET'])
@api_response
def get_systems():
    sys_name = request.args.get('system', default=None,type=str)

    if sys_name is None:
        res = get_systems_info()
    else:
        res = get_system_info(sys_name)
    return res

@app.route('/stations', methods=['GET'])
@api_response
def get_stations():
    
    sys_name = request.args.get('system', default=None,type=str)
    
    if sys_name is None:
        return # Add a 404
    
    table = pq.read_table(f'./tracker-data/{sys_name}/stations.parquet')
    res = table.to_pylist()
    

    return res


@app.route('/vehicles',methods=['GET'])
@api_response
def get_vehicles():
    sys_name = request.args.get('system', default=None,type=str)
    if sys_name is None:
        return # Add a 404

    table = pq.read_table(f'./tracker-data/{sys_name}/vehicle_types.parquet')
    res = table.to_pylist()

    
    return res

@app.route('/activity', methods=['GET'])
def get_activity():
    sys_name = request.args.get('system', default=None,type=str)
    t1 = request.args.get('start', default=None, type=str)
    t2 = request.args.get('end', default=None, type=str)
    frequency = request.args.get('frequency', default='h', type=str)
    station_id = request.args.get('station', default=None, type=str)
    limit = request.args.get('limit', default=None, type=int)
    system = get_system_info(sys_name)
    vehicle_type_id = request.args.get('vehicle',default=None,type=str)
    feed_type = request.args.get('feed',default='station',type=str)
    
    tz = system['tz']
    try:
        t1 = string_to_datetime(t1,tz)
        t2 = string_to_datetime(t2,tz)
    except:
        
        return return_api_error()
    
    
    res =  get_trips(t1,t2,sys_name,feed_type,station_id,vehicle_type_id,frequency)  
    return res


@app.route('/gbfs',methods=['GET'])
def get_live_gbfs():
    sys_name = request.args.get('system', default=None,type=str)
    feed = request.args.get('feed', default=None,type=str)
    table = pq.read_table(f'./tracker-data/{sys_name}/system.parquet')
    sys_url = table.to_pylist()[0]['url']
    feed_url = [x for x in requests.get(sys_url).json()['data']['en']['feeds'] if x['name']==feed][0]['url']   

    data = requests.get(feed_url).json()
    return jsonify(data)

    
if __name__ == '__main__':
    app.run(debug=True, host = '127.0.0.1', port = 8001)


    
