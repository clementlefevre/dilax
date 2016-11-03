

"""Summary

Attributes:
    COLUMNS_CAL (TYPE): Description
    COLUMNS_FOR_REG (TYPE): Description
    COLUMNS_X (TYPE): Description
    COLUMNS_Y (list): Description
"""


import pandas as pd
from service.holidays_service import add_school_holidays

# From the count_weather_holidays data, generate a predictor and a target set.
# It is important to keep the index of the data with both X and Y to check
# the validity of predictionsiiii


COLUMNS_CAL = ['day_' + str(i) for i in range(0, 7)] + \
    ['month_' + str(i) for i in range(1, 13)]
COLUMNS_FOR_REG = ['mintemperature', 'maxtemperature',
                   'cloudamount', 'weathersituation']
COLUMNS_X = ['index', 'idbldsite', 'holiday', 'not_holiday'] + \
    COLUMNS_CAL + [col + "_reg" for col in COLUMNS_FOR_REG]
COLUMNS_Y = ['index', 'idbldsite', 'compensatedin']


def create_predictors(predictor, retrocheck=False):
    """Summary

    Args:
        dataset_reg (TYPE): Description
        dataset_dict (TYPE): Description
        forecasting (bool, optional): Description

    Returns:
        TYPE: Description
    """
    for name, group in predictor.datastore.data.groupby('idbldsite'):
        predictor.predictor_dict[name] = {'X': group[COLUMNS_X],
                                          'mean': group.mean(), 'std': group.std()}
        if forecasting:
            predictor_dict[name]['date'] = group['date']
        else:
            predictor_dict[name]['y'] = group[COLUMNS_Y]


def regularize(title, field, idbldsite, dataset_past_dict):
    """Summary

    Args:
        title (TYPE): Description
        field (TYPE): Description
        idbldsite (TYPE): Description
        dataset_past_dict (TYPE): Description

    Returns:
        TYPE: Description
    """
    if pd.isnull(field):
        return 0
    else:
        mean = dataset_past_dict[idbldsite]['mean'][title]
        std = dataset_past_dict[idbldsite]['std'][title]
        return (field - mean) / std


def regularize_forecast(df_forecast, dataset_past_dict):
    """Summary

    Args:
        df_forecast (TYPE): Description
        dataset_past_dict (TYPE): Description

    Returns:
        TYPE: Description
    """
    df_forecast = df_forecast.reset_index()
    for col in ['mintemperature', 'maxtemperature']:

        df_forecast[col + '_reg'] = df_forecast.apply(lambda x: regularize(
            col, x[col], x.idbldsite, dataset_past_dict), axis=1)

    for col in ['cloudamount_reg', 'weathersituation_reg']:
        df_forecast[col] = 0

    return df_forecast


def add_weather_forecasts(df):
    """Summary

    Args:
        df (TYPE): Description

    Returns:
        TYPE: Description
    """
    df_weather = pd.read_csv(
        'data/weather_forecasts.csv', parse_dates=['date'])
    df = pd.merge(df, df_weather, on=['idbldsite', 'date'], how='left')

    return df


def create_forecasts_data(predictor):
    """Summary

    Args:
        date_from (TYPE): Description
        date_to (TYPE): Description

    Returns:
        TYPE: Description
    """
    df_forecasts = pd.DataFrame()
    df_sites_id = pd.DataFrame(
        predictor.datastore.data.idbldsite.unique(), columns=['idbldsite'])

    sites_regions = pd.merge(
        df_sites_id, predictor.datastore.data[['idbldsite', 'region_id']], on='idbldsite', how='left')
    sites_regions = sites_regions.drop_duplicates()

    for site_id, region_id in [tuple(x) for x in sites_regions.values]:
        print site_id, region_id

        df_forecasts_site = pd.DataFrame(pd.date_range(
            predictor.date_from, predictor.date_to, freq=predictor.period), columns=['date'])
        df_forecasts_site['idbldsite'] = site_id
        df_forecasts_site['region_id'] = region_id

        df_forecasts_site = add_school_holidays(df_forecasts_site)

    df_forecasts = pd.concat([df_forecasts, df_forecasts_site], axis=0)
    #df_forecasts = add_weather_forecasts(df_forecasts)

    predictor.forecasts = df_forecasts
