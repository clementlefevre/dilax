from datetime import timedelta
from sqlalchemy import create_engine
import pandas as pd
import logging

from ..model.config_manager import Config_manager

config_manager = Config_manager()

address = config_manager.weatherstore['drivername']\
    + "://"\
    + config_manager.weatherstore['username']\
    + ":"\
    + config_manager.weatherstore['password']\
    + '@' + config_manager.weatherstore['host'] + ':' + config_manager.weatherstore[
    'port'] + '/' + config_manager.weatherstore['database']


def get_weatherstore_forecasts(datastore, df):
    site_id = df.idbldsite.unique()[0]
    db_user = datastore.db_params['db_user']
    weather_site_id = get_weather_site_id(site_id, db_user)

    if weather_site_id is not None:

        df_weatherstore = retrieve_forecasts(weather_site_id)
        df_weatherstore = df_weatherstore[df_weatherstore.updated.dt.date == (
            datastore.date_from - timedelta(days=1))]

        period = convert_period(datastore)

        df_weatherstore = df_weatherstore[
            (df_weatherstore.data_type == "forecast") & (df_weatherstore.period == period)]
        return df_weatherstore
    else:
        datastore.no_weatherstore_sites.append(site_id)
        logging.error("datastore.no_weatherstore_sites : {}".format(datastore.no_weatherstore_sites))
        return pd.DataFrame()


def get_weather_site_id(site_id, db_user):
    try:
        engine = create_engine(address)

        id = pd.read_sql_query("SELECT id FROM sites WHERE idbldsite=" + str(site_id) + " AND customer = \'" + db_user + "\'",
                               con=engine)
        engine.dispose()

        return id.id.values[0]
    except IndexError as e:
        logging.error(
            "This idbldsite :{0} was not found in weather store ".format(site_id))
        return None


def retrieve_forecasts(weather_site_id):
    query = "select * from weather_data where site_id=" + str(weather_site_id)

    engine = create_engine(address)
    df_weatherstore = pd.read_sql_query(query,
                                        con=engine)
    engine.dispose()

    return df_weatherstore


def convert_period(datastore):
    return 'day' if datastore.period == "D" else "hour"
