from datetime import timedelta
from sqlalchemy import create_engine
import pandas as pd

from ..model.config_manager import Config_manager

config_manager = Config_manager()

address = config_manager.weatherstore['drivername']\
    + "://"\
    + config_manager.weatherstore['username']\
    + ":"\
    + config_manager.weatherstore['password']\
    + '@' + config_manager.weatherstore['host'] + ':' + config_manager.weatherstore[
    'port'] + '/' + config_manager.weatherstore['database']


def get_weather_forecasts(datastore, df):
    site_id = df.idbldsite.unique()[0]
    db_user = datastore.db_params['db_user']
    weather_site_id = get_weather_site_id(site_id, db_user)

    df = retrieve_forecasts(weather_site_id)
    print datastore.date_from

    df = df[df.updated.dt.date == (
        datastore.date_from - timedelta(days=1))]


    period = convert_period(datastore)

    df = df[(df.data_type == "forecast") & (df.period == period)]
    return df


def get_weather_site_id(site_id, db_user):
    engine = create_engine(address)
    id = pd.read_sql_query("SELECT id FROM sites WHERE idbldsite=" + str(site_id) + " AND customer = \'" + db_user + "\'",
                           con=engine)
    engine.dispose()
    print "site_id", id.id.values[0]
    return id.id.values[0]


def retrieve_forecasts(weather_site_id):
    query = "select * from weather_data where site_id=" + str(weather_site_id)
    print query
    engine = create_engine(address)
    df_weatherstore = pd.read_sql_query(query,
                                        con=engine)
    engine.dispose()

    return df_weatherstore


def convert_period(datastore):
    return 'day' if datastore.period == "D" else "hour"
