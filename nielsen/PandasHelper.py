
import csv
import pandas as pd
import numpy as np
from os import listdir
from os.path import isfile, join
import matplotlib.pyplot as plt
import seaborn as sns

from config import DATA_PATH,MAIN_FILE

def get_files(path):
	return [f for f in listdir(path) if isfile(join(path, f))]


def string_to_np_array(x):
    
    list_str = x.replace("]["," ").replace("]","").replace("[","").replace(", "," ")
    lista =  (map(int,list_str.split()))
    return np.unique(np.asarray(lista))

def get_dataset_stats():
    chunks = pd.read_csv(DATA_PATH+"nielsen.csv",parse_dates=['first_timeframe'],chunksize=1000000)
    stats =  {}
    min_date = date.today()
    max_date = date(2000,1,1)
    rows =0
    for c in chunks:
        rows+=c.shape[0]
        c.first_timeframe = c.first_timeframe.dt.date
        min_c,max_c = c.first_timeframe.min(), c.first_timeframe.max()
        if min_c < min_date:
            min_date = min_c
        if max_c > max_date:
            max_date = max_c
        print "{:,}".format(rows),min_date,max_date
    stats = {'rows':rows,'min_date':min_date,'max_date':max_date}            
    return stats


def get_number_of_devices():
    df = pd.read_csv(DATA_PATH+"indexes/devices_ix.csv",index_col=0)
    return df.shape[0]

def get_number_of_sites():
    df = pd.read_csv(DATA_PATH+"indexes/sites_ix.csv",index_col=0)
    return df.shape[0]

def get_numbers_records_multi_sites_devices():
    rows = 0
    files = pdh.get_files(DATA_PATH+"data_multiples_sites_only_with_indexes/")
    for filo in files:
        df1 = pd.read_csv(DATA_PATH+"data_multiples_sites_only_with_indexes/"+filo,index_col=0) 
        rows+=df1.shape[0]
    return rows

def search_device_mac(device_id_list):
    df = pd.read_csv(DATA_PATH+"/indexes/devices_ix_multiple_sites.csv")
    result =  df[df.device_id.isin(device_id_list)]
    print result
    return result.device_mac.values.tolist()

def search_device_data(mac_address_list):  
	file_name = "data_search_results.csv"
	# chunks = pd.read_csv(DATA_PATH+"/nielsen.csv",chunksize = 1000000)
	chunks = pd.read_csv(DATA_PATH+"/nielsen.csv",chunksize=1000000)
	df = pd.DataFrame()
	print "start searching mac_address..."
	for i,c in enumerate(chunks):
	    df1 = c[c.device_id.isin(mac_address_list)]
	    print df1.shape,i
	    df = pd.concat([df,df1],axis=0)
	print "saving mac_address to"+file_name
	df.to_csv(DATA_PATH+file_name)

def display_spread():
	df = pd.read_csv(DATA_PATH+"/nielsen_indexes_sites_per_devices.csv", index_col=0)
	groupy = df.groupby('sites_count').count()
	lst = [df]
	del lst
	total = groupy.sites_id_array.sum(axis=0)
	plt.figure(figsize=(6,6))
	plt.bar(groupy.index,groupy.sites_id_array,1,align="center",color='#007f7f')
	plt.yscale('log')
	plt.xlabel('Number of sites visited', fontsize=12)
	plt.ylabel('Number of devices (log)', fontsize=12)
	plt.xticks(groupy.index[::3],fontsize=10)
	locs, labels = plt.xticks()
	plt.setp(labels, rotation=45)
	plt.title("Devices per visited sites in the whole year 2015", fontsize=14)
