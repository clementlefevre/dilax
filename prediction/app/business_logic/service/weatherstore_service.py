from datetime import timedelta
from sqlalchemy import create_engine
import pandas as pd
import logging

from ..model.config_manager import Config_manager

config_manager = Config_manager()
conf = config_manager.weatherstore

address = conf['drivername'] + "://" + conf['username'] + ":" \
    + conf['password'] + '@' + conf['host'] + ':' \
    + conf['port'] + '/' + conf['database']


def get_weatherstore_forecasts(datastore, df):
    db_user = datastore.db_params['db_user']
    idbldsite_list = df.idbldsite.unique().tolist()

    all_sites_id_list = get_weather_site_id(idbldsite_list, db_user)
    weather_site_id_list = [id[1] for id in all_sites_id_list]

    date_range = set_date_range(datastore)

    df_weatherstore = retrieve_forecasts(weather_site_id_list, date_range)

    df_weatherstore = add_idbdsite(df_weatherstore, all_sites_id_list)

    period = convert_period(datastore)

    df_weatherstore = df_weatherstore[
        (df_weatherstore.data_type == "forecast") & (df_weatherstore.period == period)]

    return df_weatherstore


def get_weather_site_id(idbldsite_list, db_user):
    weather_site_id_list = []
    engine = create_engine(address)
    for idbldsite in idbldsite_list:
        try:

            query = "SELECT id FROM sites WHERE idbldsite= {0} AND customer = '{1}'".format(
                str(idbldsite), db_user)
            id = pd.read_sql_query(query, con=engine)
            weather_site_id_list.append((idbldsite, id.id.values[0]))
        except IndexError:
            logging.error(
                "This idbldsite :{0} for customer : was not found in weather store ".format(idbldsite))
    engine.dispose()
    return weather_site_id_list


def retrieve_forecasts(weatherstore_site_id_list, date_range):
    refinedList = ",".join(str(site_id)
                           for site_id in weatherstore_site_id_list)

    query = ("SELECT * FROM weather_data WHERE site_id in ({0}) AND updated >= '{1}' AND updated < '{2}'".format(
        refinedList, *date_range))

    engine = create_engine(address)
    df_weatherstore = pd.read_sql_query(query,
                                        con=engine)
    engine.dispose()
    df_weatherstore = filter_on_latest_update(df_weatherstore)

    return df_weatherstore


def add_idbdsite(df_weatherstore, weatherstore_site_id_list):
    df_id_sites = pd.DataFrame(weatherstore_site_id_list, columns={
                               'idbldsite', 'site_id'})

    return pd.merge(df_weatherstore, df_id_sites, on='site_id')


def convert_period(datastore):
    if datastore.period == 'D':
        return 'day'
    elif datastore.period == "H":
        return "hour"
    else:
        raise (NotImplementedError)


def set_date_range(datastore):
    forecast_from = (datastore.predict_from - timedelta(days=1))
    forecast_to = datastore.predict_from

    return (forecast_from.strftime('%Y-%m-%d'), forecast_to.strftime('%Y-%m-%d'))


def filter_on_latest_update(df_weatherstore):

    df_weatherstore_latest = df_weatherstore.groupby(
        ['site_id', 'dateTime']).last()
    return df_weatherstore_latest.reset_index()
