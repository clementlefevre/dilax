from sqlalchemy import create_engine
import datetime
import numpy as np
import pandas as pd
from  config import DB_USER,DB_NAME,DB_PORT,DB_PWD,DB_URL



def create_connection():
	return create_engine('postgresql://'+DB_USER+':'+DB_PWD+'@'+DB_URL+':'+DB_PORT+'/'+DB_NAME)

def get_sites():
	engine = create_connection()
	return  pd.read_sql_query('select * from "dwe_bld_site"',con=engine)

def get_address():
	engine = create_connection()
	return  pd.read_sql_query('select * from "dwe_bld_address"',con=engine)


def get_holidays():
	engine = create_connection()
	return  pd.read_sql_query('select * from "dwe_cal_holiday"',con=engine)


def get_weather_intraday():
	engine = create_connection()
	return pd.read_sql('select * from "dwe_ext_weather_meteogroup_intraday"',con=engine)

def get_counts():
	engine = create_connection()
	return pd.read_sql_query('select * from "dwe_cnt_site"',con=engine)


def round_coordinate(coordinate):
	return str(round(coordinate,2))

def get_sites_dict():
	sites = get_sites()
	sites = sites[['idbldsite','sname','latitude','longitude']]
	sites = sites.set_index('idbldsite')
	sites_dict =  sites[['sname','latitude','longitude']].T.apply(tuple).to_dict()
	return sites_dict


# This method create a dataset aggregate per hour of weather update.
def create_counts_sites_weather_holidays_intraday():

	engine = create_connection()
	df_weather_intraday = pd.read_sql('select * from "dwe_ext_weather_meteogroup_intraday"',con=engine)
	df_weather_day = pd.read_sql_query('select * from "dwe_ext_weather_meteogroup_day"',con=engine)
	df_premium = pd.read_sql_query('select * from "dwe_ext_weather_premium"',con=engine)
	df_sites = pd.read_sql_query('select * from "dwe_bld_site"',con=engine)
	df_counts = pd.read_sql_query('select * from "dwe_cnt_site"',con=engine)
	df_holidays = pd.read_sql_query('select * from "dwe_cal_holiday"',con=engine)


	# there are more than one entry per hour per idparent for the intraday data !!!
	groupy_weather_intraday = df_weather_intraday.groupby(['idparent','timestamp']).mean()
	groupy_weather_intraday = groupy_weather_intraday.reset_index()

	# merge weather intraday and day tables to retrieve use the coordinates for merging with sites
	df_weather = pd.merge(groupy_weather_intraday,df_weather_day,left_on='idparent',right_on='id',how='inner')

	df_weather['coord']=df_weather.latitude.apply(lambda x: str(round(x,2)))+";"+df_weather.longitude.apply(lambda x: str(round(x,2)))
	df_sites['coord']=df_sites.latitude.apply(lambda x: str(round(x,2)))+";"+df_sites.longitude.apply(lambda x: str(round(x,2)))

	df_sites_weather=pd.merge(df_weather,df_sites,on='coord',how='left')

	columns =['id_x','idparent','timestamp','utctimestamp','temperature','cloudamount_x',
	'weathersituation_x','latitude_x','longitude_x','day','maxtemperature','mintemperature',
	'weathersituation_y', 'cloudamount_y', 'lastupdated', 'idbldsite', 'sname', 'deactivated']

	df_sites_weather = df_sites_weather[columns]

	df_counts['timestamp_hour']=df_counts.timestamp.apply(lambda dt: datetime.datetime(dt.year, dt.month, dt.day, dt.hour))
	df_counts = df_counts.groupby(['idbldsite','timestamp_hour']).sum()
	df_counts = df_counts.reset_index()

	df_counts_sites_weather = pd.merge(df_counts[['idbldsite','compensatedin','timestamp_hour']],df_sites_weather,how='left',left_on=['idbldsite','timestamp_hour'],right_on=['idbldsite','timestamp'],suffixes=['count_','weather_'])
	df_counts_sites_weather['date']= df_counts_sites_weather.timestamp.dt.date

	df = pd.merge(df_counts_sites_weather,df_holidays[['idbldsite','day','name_en']],left_on=['idbldsite','date'],right_on=['idbldsite','day'],how='left')
	df['is_holiday']= ~df.name_en.isnull()*1

	columns =['idbldsite','compensatedin','timestamp_hour','temperature','cloudamount_x','weathersituation_x','maxtemperature','mintemperature','sname','is_holiday']

	df = df[columns]

	df['day_of_week']= df.timestamp_hour.dt.dayofweek
	df['month']= df.timestamp_hour.dt.month
	df['hour']= df.timestamp_hour.dt.hour

	# Filter on counts with weather data
	df = df[~df.temperature.isnull()]
	df.to_csv("data/counts_sites_weather_holidays_intraday.csv",index_col=True)

# This method create a dataset aggregate per day with weather info.
def create_counts_sites_weather_holidays_day():

	engine = create_connection()
	
	df_weather_day = pd.read_sql_query('select * from "dwe_ext_weather_meteogroup_day"',con=engine)
	
	df_sites = pd.read_sql_query('select * from "dwe_bld_site"',con=engine)
	df_counts = pd.read_sql_query('select * from "dwe_cnt_site"',con=engine)
	df_holidays = pd.read_sql_query('select * from "dwe_cal_holiday"',con=engine)


	df_weather_day['coord']=df_weather_day.latitude.apply(round_coordinate)+";"+df_weather_day.longitude.apply(round_coordinate)
	df_sites['coord']=df_sites.latitude.apply(round_coordinate)+";"+df_sites.longitude.apply(round_coordinate)

	df_sites_weather=pd.merge(df_weather_day[['maxtemperature','mintemperature','weathersituation',
		'cloudamount','day','coord']],df_sites[['idbldsite','coord','sname']],on='coord',how='left',suffixes=['_weather','_sites'])

	df_sites_weather.rename(columns={'day':'date'},inplace =True)

	

	df_counts['date']= pd.to_datetime(df_counts.timestamp.dt.date)
	df_counts = df_counts.groupby(['idbldsite','date']).sum()
	df_counts = df_counts.reset_index()

	df_counts_sites_weather = pd.merge(df_counts[['idbldsite','compensatedin','date']],df_sites_weather,how='left',left_on=['idbldsite','date'],right_on=['idbldsite','date'],suffixes=['counts_','weather_'])
	

	df = pd.merge(df_counts_sites_weather,df_holidays[['idbldsite','day','name_en']],left_on=['idbldsite','date'],right_on=['idbldsite','day'],how='left',suffixes=['_counts','_holidays'])
	df['is_holiday']= ~df.name_en.isnull()*1



	df['day_of_week']= df.date.dt.dayofweek
	df['day_of_month']= df.date.dt.day
	df['month']= df.date.dt.month
	df['year']= df.date.dt.year

	for day in range(0,7):
		df['day_'+str(day)]=np.where(df.day_of_week==day,1,0)

	for month in range(1,13):
		df['month_'+str(month)]=np.where(df.month==month,1,0)


	df['holiday']=np.where(df.is_holiday!=0,1,0)
	df['not_holiday']=np.where(df.is_holiday==0,1,0)

	



	# Filter on counts with weather data
	df = df[~df.maxtemperature.isnull()]
	df.to_csv("data/counts_sites_weather_holidays_day.csv",index_col=True)

	
