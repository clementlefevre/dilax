import numpy as np


def round_coordinate(coordinate):
    return str(round(coordinate, 2))


def round_coordinates(df):
    rounded_latitude = df.latitude.apply(
        round_coordinate)
    rounded_longitude = df.longitude.apply(round_coordinate)
    df['coord'] = rounded_latitude + ";" + rounded_longitude
    return df


def add_calendar_fields(datastore):
    data = datastore.data

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

    return data


def get_sites_dict(df_sites):
    sites = df_sites
    sites = sites[['idbldsite', 'sname', 'latitude', 'longitude']]
    sites = sites.set_index('idbldsite')
    sites_dict = sites[['sname', 'latitude',
                        'longitude']].T.apply(tuple).to_dict()
    return sites_dict
