
import numpy as np
from model.config_manager import Config_manager


config_manager = Config_manager()


def get_nearest_coordinate(site_coordinate, weather_coordinates):
    """Allows the matching of site with other data based on coordinates.

    Args:
        site_coordinate (TYPE): Description
        weather_coordinates (TYPE): Description

    Returns:
        TYPE: Nearest point of a list of coordinates
    """
    return min(weather_coordinates,
               key=lambda x: abs(x - site_coordinate))


def add_calendar_fields(df):
    """Summary

    Args:
        datastore (TYPE): Description

    Returns:
        TYPE: Description
    """
    data = df

    data['day_of_week'] = data.date.dt.dayofweek
    data['day_of_month'] = data.date.dt.day
    data['day_of_year'] = data.date.dt.dayofyear
    data['month'] = data.date.dt.month
    data['year'] = data.date.dt.year

    for day in range(0, 7):
        data['day_' + str(day)] = np.where(data.day_of_week == day, 1, 0)

    for month in range(1, 13):
        data['month_' + str(month)] = np.where(data.month == month, 1, 0)

    data['public_holiday'] = np.where(data.is_public_holiday != 0, 1, 0)
    data['not_public_holiday'] = np.where(data.is_public_holiday == 0, 1, 0)
    data['school_holiday'] = np.where(data.is_school_holiday != 0, 1, 0)
    data['not_school_holiday'] = np.where(data.is_school_holiday == 0, 1, 0)
    data.drop(['is_public_holiday', 'is_school_holiday'], 1)

    return data


def get_sites_dict(df_sites):
    """This is useful to iterate over sites
    and retrieve the weather data from the API.

    Args:
        df_sites (TYPE): Description

    Returns:
        TYPE: Description
    """
    sites = df_sites
    sites = sites[['idbldsite', 'sname', 'latitude', 'longitude']]
    sites = sites.set_index('idbldsite')
    sites_dict = sites[['sname', 'latitude',
                        'longitude']].T.apply(tuple).to_dict()
    return sites_dict


def regularize(datastore, df, is_forecast=False):
    """Summary

    Args:
        datastore (TYPE): Description
        df (TYPE): Description
        is_forecast (bool, optional): Description

    Returns:
        TYPE: Description
    """
    df = df.reset_index()

    for col in df.columns.tolist():
        if config_manager.features[col]:
            if not is_forecast:

                df[col + "_reg"] = df.groupby('idbldsite')[col].apply(
                    lambda x: (x - x.mean()) / x.std())
                print col + "has been regularized for training set"

            else:
                training_set_groupy = datastore.training_data.\
                    groupby('idbldsite')[col]
                x_mean = training_set_groupy.mean()
                x_std = training_set_groupy.std()

                df[col + "_reg"] = df.apply(lambda x:
                                            (x[col] - x_mean.ix[x['idbldsite']]
                                             ) / x_std.ix[x['idbldsite']],
                                            axis=1)

    df = df.fillna(0)
    return df
