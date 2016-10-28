import datetime
import numpy as np
import pandas as pd
import urllib2
import json
from pandas.io.json import json_normalize

import db_service
import config




def get_weather(lat,lon):
    response = urllib2.urlopen(config.WEATHER_URL+"lat="+lat+"&lon="+lon)
    data = json.loads(response.read())
    df_weather = json_normalize(data['LocationWeather']['forecast']['days'])
    df_weather.date = pd.to_datetime(df_weather.date).dt.date

    last_update = json_normalize(data['LocationWeather']['latestobservation']['latests'][0])['dateTime'].values[0]
    
    df_weather['lastUpdate']= pd.to_datetime(last_update)
    return df_weather
    

def get_weather_forecasts():
    sites_dict = db_service.get_sites_dict()
    df_weather = pd.DataFrame()
    
    for site_id in sites_dict:
        sname = sites_dict[site_id][0]
        lat = str(sites_dict[site_id][1])
        lon = str(sites_dict[site_id][2])
        
        df_weather_site = get_weather(lat,lon)
        df_weather_site['idbldsite']= site_id
        df_weather_site['sname']= sname
        df_weather = pd.concat([df_weather,df_weather_site],axis=0)
    df_weather = df_weather.rename(columns={'tx':'maxtemperature','tn':'mintemperature'})
    df_weather.to_csv('data/weather_forecasts.csv',index=0)

