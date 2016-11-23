"""This service handles the creation of a new Forecasts dataframe
to compute prediction on target values (visitors, turn-over)
"""
import pandas as pd
import logging
from datetime import datetime
from ..service.school_holidays_service import add_school_holidays
from ..service.public_holidays_service import add_public_holidays
from weather_API_service import add_weather_forecasts
from ..helper.data_helper import add_calendar_fields, regularize


def create_forecasts_data(datastore):
    """From a given datastore,
    create a DataFrame with all available predictors
    (holidays, weather, etc...)

    Args:
        datastore (TYPE): datastore

    Returns:
        TYPE: DataFrame
    """
    logging.info("{0} : start creating forecasts...".format(datastore))
    df_forecasts = pd.DataFrame()

    site_region = datastore.sites_infos[
        ['idbldsite', 'region_id']].values

    for site_id, region_id in [tuple(x) for x in site_region]:
        df_forecasts_site = pd.DataFrame(pd.date_range(
            datastore.date_from, datastore.date_to,
            freq=datastore.period), columns=['date'])

        df_forecasts_site['date_time'] = df_forecasts_site.date

        if datastore.period == 'H':

            df_forecasts_site['date'] = df_forecasts_site['date_time'].apply(lambda dt:
                                                                             datetime(dt.year,
                                                                                      dt.month,
                                                                                      dt.day))

        df_forecasts_site['idbldsite'] = site_id
        df_forecasts_site['region_id'] = region_id

        df_forecasts_site = add_school_holidays(df_forecasts_site)

        # !!!!  API working for the time being !!!
        try:
            df_forecasts_site = add_public_holidays(
                df_forecasts_site, datastore.public_holidays)
        except Exception as e:
            logging.error(e.message)
            logging.error(
                "{0} : no public holidays table, first use \"creation\" parameter in datastore.".format(datastore))

        df_forecasts_site = add_calendar_fields(df_forecasts_site)

        df_forecasts_site = add_weather_forecasts(
            datastore, df_forecasts_site)

        if datastore.period == 'H':
            df_forecasts_site['temperature'] = (
                df_forecasts_site.mintemperature + df_forecasts_site.maxtemperature) / 2

        df_forecasts = pd.concat([df_forecasts, df_forecasts_site], axis=0)

    df_forecasts = regularize(
        datastore, df_forecasts, is_forecast=True)

    return df_forecasts



