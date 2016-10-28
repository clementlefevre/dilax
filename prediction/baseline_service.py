import pandas as pd

def create_baseline(end_date,df_forecasts):
	df = pd.read_csv('data/counts_sites_weather_holidays_day.csv',parse_dates=['date'],index_col=0)
	df = df[df.date<end_date]
	df['day']=df.date.dt.dayofweek
	df['month']=df.date.dt.month
	groupy = df.groupby(['idbldsite','month','day'])[['compensatedin']].mean()
	df_baseline = groupy.reset_index()
	
	df_forecasts_baseline  = df_forecasts.copy()
	df_forecasts_baseline['day']=df_forecasts_baseline.date.dt.dayofweek
	df_forecasts_baseline['month']=df_forecasts_baseline.date.dt.month


	df_forecasts_baseline = pd.merge(df_forecasts_baseline,df_baseline[['idbldsite','month','day','compensatedin']],on=['idbldsite','month','day'])
	df_forecasts_baseline = df_forecasts_baseline.sort_values(by='date',ascending=True)
	return df_forecasts_baseline