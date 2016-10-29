# coding: utf-8




from sqlalchemy import create_engine
import datetime
import numpy as np
import pandas as pd
import urllib2
import urllib
import json

from pandas.io.json import json_normalize

from config import *
import db_service


def get_sites_with_address():
	df_sites = db_service.get_sites()
	df_address = db_service.get_address()
	df_sites = pd.merge(df_sites[['idbldsite','sname']],df_address[['idbldsite','city']],on='idbldsite')
	return df_sites


def holidays_API_ids():
	df_sites = get_sites_with_address()
	response = urllib2.urlopen(HOLIDAYS_API_ROOT_URL+'geo.php?pw='+HOLIDAYS_API_PWD+'&geo=all&child=1')
	data = json.loads(response.read())
	df_geo = json_normalize(data[0]['Geo-entries'])
	# Filter on "Gemeinde" to get the school holidays
	df_geo=df_geo[df_geo.title=="Gemeinde"]
	df_sites_holidays_id = pd.merge(df_sites,df_geo, left_on='city',right_on='location',how='left')
	#the API key is the field ID
	df_API_ID_list = df_sites_holidays_id.id.value_counts().index
	df_sites_holidays_id.to_csv("holidays/sites_id_holidays_id.csv",encoding='utf-8',sep=';')
	return df_API_ID_list

def create_year_range(year_start):
	year_end =  datetime.datetime.now().year
	year_range = range(year_start,year_end+1,1)
	return year_range


def create_holidays_table(list_API_ids,year_start=2012):
	year_range = create_year_range(year_start)
	df_public_holidays = pd.DataFrame()
	df_school_holidays = pd.DataFrame()

	for id in list_API_ids:
		for year in year_range:
		
			url = HOLIDAYS_API_ROOT_URL + 'holidays.php?pw='+ urllib.quote(HOLIDAYS_API_PWD)+'&geo='+id + '&jahr='+str(year)
			response = urllib2.urlopen(url)
			data = json.loads(response.read())
			public_holidays = pd.DataFrame(data['public_holidays'])
			public_holidays['holidays_API_site_id']= id
			school_holidays = pd.DataFrame(data['school_holidays'])
			school_holidays['holidays_API_site_id']= id
			df_public_holidays = pd.concat([df_public_holidays,public_holidays],axis=0)
			df_school_holidays = pd.concat([df_school_holidays,school_holidays],axis=0)

	df_public_holidays = df_public_holidays.drop_duplicates()
	df_school_holidays = df_school_holidays.drop_duplicates()
	df_public_holidays.to_csv("holidays/public_holidays.csv",encoding='utf-8',sep=';')
	df_school_holidays.to_csv("holidays/school_holidays.csv",encoding='utf-8',sep=';')
	print "Finished to retrieve holidays."


def get_holidays():
	holidays_API_id_list = holidays_API_ids()
	print holidays_API_id_list
	create_holidays_table(holidays_API_id_list)