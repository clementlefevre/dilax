def round_coordinate(coordinate):
    return str(round(coordinate, 2))


def round_coordinates(df):
    rounded_latitude = df.latitude.apply(
        round_coordinate)
    rounded_longitude = df.longitude.apply(round_coordinate)
    df['coord'] = rounded_latitude + ";" + rounded_longitude
    return df


def get_sites_dict(df_sites):
    sites = df_sites
    sites = sites[['idbldsite', 'sname', 'latitude', 'longitude']]
    sites = sites.set_index('idbldsite')
    sites_dict = sites[['sname', 'latitude',
                        'longitude']].T.apply(tuple).to_dict()
    return sites_dict
