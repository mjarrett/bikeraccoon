# bikeraccoon

A python package that wraps the [bikeraccoon API](https://github.com/mjarrett/bikeraccoonAPI). `bikeraccoon` is built with Pandas and in most cases returns a Pandas.DataFrame.

## Usage

```
import bikeracccoon as br

# Get a list of available systems:
br.get_systems()

# Connect to API
api = br.LiveAPI('mobi_vancouver')
```

## LiveAPI methods

api.get_stations()
Returns a dataframe with information about each station in the bikeshare system.

api.get_system_trips(t1,t2=None,freq='h')
* t1: python datetime instance
* t2: python datetime instance
* freq: How to group results ('h','d','m','y')
Returns dataframe with columns "station trips" for trips associated with docking stations, and "free bike trips" for trips associated with free floating bikes.

api.get_station_trips(t1,t2=None,freq='h',station='all')
* t1: python datetime instance
* t2: python datetime instance
* freq: How to group results ('h','d','m','y')
* station: station_id of a station in the system. If station='all', returns data for all stations.
Returns long-style dataframe with rows for each timepoint and station

api.get_free_bike_trips(t1,t2=None,freq='h')
* t1: python datetime instance
* t2: python datetime instance
* freq: How to group results ('h','d','m','y')
Returns a dataframe with data for free-floating bike trips

The API calls use LRU caching to avoid repeatedly querying the API with the same query.

## Twitter bikesharebots

Several twitter bots use this package to generate stats and figures, such as [VanBikeShareBot](https://twitter.com/vanbikesharebot). For an example of this, see the file `sample_bikesharebot.py`. 



