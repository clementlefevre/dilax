import config
import db_service
import numpy as np
import pandas as pd



### From the count_weather_holidays data, generate a prediction and a target set.
### It is important to keep the index of the data with both X and Y to check the validty of predictions.

COLUMNS_CAL = ['day_'+str(i) for i in range(0,7)] + ['month_'+str(i) for i in range(1,13)]
COLUMNS_FOR_REG = ['mintemperature','maxtemperature','cloudamount', 'weathersituation']
COLUMNS_X = ['index','idbldsite', 'holiday', 'not_holiday']+COLUMNS_CAL+[col+"_reg" for col in COLUMNS_FOR_REG]
COLUMNS_Y = ['index','idbldsite', 'compensatedin']


def regularize_dataset(df):
    df = df.reset_index()
    df.set_index(['idbldsite','index'])
    groupy = df.groupby('idbldsite')
    
    data = df.copy()


    for col in COLUMNS_FOR_REG:
        data[col+"_reg"] = data.groupby('idbldsite')[col].transform(lambda x: (x - x.mean()) / x.std())

    data = data.fillna(0)
    return data


def create_predictors_and_target(dataset_reg,dataset_dict,forecasting=False):
    for name,group in dataset_reg.groupby('idbldsite'):
        dataset_dict[name]={'X' :group[COLUMNS_X],
                            'mean':group.mean(),'std':group.std()}
        if forecasting:
            dataset_dict[name]['date'] = group['date']
        else:
            dataset_dict[name]['y'] = group[COLUMNS_Y]
        
def regularize(title,field,idbldsite,dataset_past_dict):
    if pd.isnull(field):
        return 0
    else:  
        mean =  dataset_past_dict[idbldsite]['mean'][title]
        std =  dataset_past_dict[idbldsite]['std'][title]
        return (field-mean)/std
        
    
def regularize_forecast(df_forecast,dataset_past_dict):
    df_forecast = df_forecast.reset_index()
    for col in ['mintemperature','maxtemperature']:

        df_forecast[col+'_reg'] = df_forecast.apply(lambda x : regularize(col,x[col],x.idbldsite,dataset_past_dict),axis=1)

    for col in ['cloudamount_reg','weathersituation_reg']:
        df_forecast[col] = 0

   
    return df_forecast
    
def add_weather_forecasts(df):
    df_weather = pd.read_csv('data/weather_forecasts.csv',parse_dates=['date'])
    df = pd.merge(df,df_weather, on=['idbldsite','date'],how='left')

    return df

def create_forecasts_data(date_from,date_to):
    df_forecasts = pd.DataFrame()
    df_sites = db_service.get_sites()
    sites_id = df_sites.idbldsite.unique()

    for site_id in sites_id:
        
        df_forecasts_site =  pd.DataFrame(pd.date_range(date_from,date_to,freq='D'),columns=['date'])
        df_forecasts_site['idbldsite']=site_id

        df_forecasts_site['holiday'] = 0
        df_forecasts_site['not_holiday'] = 0
        
        for day in range(0,7):
                df_forecasts_site['day_'+str(day)]=np.where(df_forecasts_site.date.dt.dayofweek==day,1,0)

        for month in range(1,13):
            df_forecasts_site['month_'+str(month)]=np.where(df_forecasts_site.date.dt.month==month,1,0)

        df_forecasts = pd.concat([df_forecasts,df_forecasts_site],axis=0)
    df_forecasts = add_weather_forecasts(df_forecasts)

    return df_forecasts


def create_baseline(df):
    pass
    

