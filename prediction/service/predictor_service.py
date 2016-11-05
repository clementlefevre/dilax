import pandas as pd
from service.holidays_service import add_school_holidays
from weather_API_service import add_weather_forecasts
from helper.data_helper import add_calendar_fields, regularize


def create_forecasts_data(datastore):
    """Summary

    Args:
        predictor (TYPE): Description

    Returns:
        TYPE: Description
    """

    df_forecasts = pd.DataFrame()

    site_region = datastore.sites_infos[
        ['idbldsite', 'region_id']].values

    for site_id, region_id in [tuple(x) for x in site_region]:
        df_forecasts_site = pd.DataFrame(pd.date_range(
            datastore.date_from, datastore.date_to,
            freq=datastore.period), columns=['date'])

        df_forecasts_site['idbldsite'] = site_id
        df_forecasts_site['region_id'] = region_id

        df_forecasts_site = add_school_holidays(df_forecasts_site)

        # !!!! No API working for the time being !!!
        df_forecasts_site['is_public_holiday'] = 0

        df_forecasts_site = add_calendar_fields(df_forecasts_site)

        df_forecasts_site = add_weather_forecasts(
            datastore, df_forecasts_site)

        df_forecasts = pd.concat([df_forecasts, df_forecasts_site], axis=0)

    # wrong, it should regularize on the training_set values !!!
    df_forecasts = regularize(
        datastore, df_forecasts, is_forecast=True)

    return df_forecasts
