from flask import Flask, request, make_response, send_from_directory, jsonify
from flask.json.provider import DefaultJSONProvider
import json
import datetime as dt
from zoneinfo import ZoneInfo
import duckdb

from bikeraccoon._version import version

class BRJSONProvider(DefaultJSONProvider):
    """
    By default Flask jsonify outputs datetimes as GMT. 
    This overrides and outputs as TZ aware datetime strings.
    """
    def default(self, obj):
        if isinstance(obj, dt.datetime):
            return obj.isoformat()  # Customize JSON representation
        return super().default(obj)

def get_system_tz(sys_name):
    return duckdb.query(f'''select tz from './tracker-data/{sys_name}/system.parquet' ''').fetchall()[0][0]

def get_data_path(sys_name,feed_type,vehicle_type,freq):
    vehicle_type = 'all' if vehicle_type is None else vehicle_type

    if freq in ['h','t']:
        return f'./tracker-data/{sys_name}/trips.{feed_type}.hourly.*.parquet'
    elif freq in ['d','m','y']:
        return f'./tracker-data/{sys_name}/trips.{feed_type}.daily.*.parquet'
    
    
def api_response(f):
    def api_func(*args,**kwargs):
        start = dt.datetime.now()
        try:
            res = f(*args,**kwargs)
        except Exception as e:
            return return_api_error(e)
        t = dt.datetime.now() - start
        res =   {'data':res, 'query_time':str(t), 
                 'version':version}
        return jsonify(res)
    api_func.__name__ = f.__name__
    return api_func

@api_response
def get_trips(t1,t2,sys_name,feed_type,station_id,vehicle_type_id,frequency):
    start = dt.datetime.now()
    data_path = get_data_path(sys_name,feed_type,vehicle_type_id,frequency)
    

    if frequency == 't':
        select = f"FIRST(datetime),SUM(trips), SUM(returns)"
        groupby = ""
        where = f"datetime BETWEEN '{t1}' and '{t2}'"
        orderby = ""
    else:
        select = f"date_trunc('{frequency}',datetime),SUM(trips), SUM(returns)"
        groupby = f"date_trunc('{frequency}',datetime)"
        where = f"datetime BETWEEN '{t1}' and '{t2}'"
        orderby = f"ORDER BY date_trunc('{frequency}',datetime)"
        
    vehicle_select = "null"
    vehicle_groupby = ""
    vehicle_where = ""
    station_select = "null"
    station_groupby = ""
    station_where = ""
    if vehicle_type_id == "all":
        vehicle_select = "vehicle_type_id"
        vehicle_groupby = "vehicle_type_id"
    elif vehicle_type_id not in [None,"all"]:
        vehicle_select = "vehicle_type_id"
        vehicle_groupby = "vehicle_type_id"
        vehicle_where = f"vehicle_type_id = '{vehicle_type_id}'"

    if station_id == "all":
        station_select = "station_id"
        station_groupby = "station_id"
    elif station_id not in [None,"all"]:
        station_select = "station_id"
        station_groupby = "station_id"
        station_where = f"station_id = {station_id}"

    select = ",".join(x for x in [station_select,vehicle_select,select] if x != "")
    where = " AND ".join(x for x in [station_where,vehicle_where,where] if x != "")
    groupby = ",".join(x for x in [station_groupby,vehicle_groupby,groupby] if x != "")

    tz = get_system_tz(sys_name)
    
    query_text = f'''
           SET TIMEZONE='{tz}';
           SELECT {select}
           FROM read_parquet('{data_path}')
           WHERE {where}
           {"GROUP BY" if groupby != "" else ""} {groupby}
           {orderby}
           '''
    print(query_text)
    qry = duckdb.query(query_text)
    print(qry.fetchall()[0])
        #-- Convert to dict
    res = [{k:v for k,v in zip(['station_id','vehicle_type_id','datetime','trips','returns'],x)} for x in qry.fetchall()]
    

    return  res
       
    
    


def string_to_datetime(t,tz):
    y = int(t[:4])
    m = int(t[4:6])
    d = int(t[6:8])
    h = int(t[8:10])
    return dt.datetime(y,m,d,h,tzinfo=ZoneInfo(tz))



    
def return_api_error(text=""):

    content = f"Invalid API request :( \n{text}"
    return content, 400


def get_systems_info():
    qry = duckdb.query(f"select * from './tracker-data/*/system.parquet' ")
    return qry.fetchdf().to_dict('records')

def get_system_info(sys_name):
    tz = get_system_tz(sys_name)
    qry = duckdb.query(f"set timezone='{tz}'; select * from './tracker-data/{sys_name}/system.parquet' ")
    return qry.fetchdf().to_dict('records')[0]
