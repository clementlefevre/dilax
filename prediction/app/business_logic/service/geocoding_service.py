"""Summary

Attributes:
    MAPS_URL (TYPE): Description
"""
import os
import urllib2
import json
import logging
import pandas as pd

from app.business_logic.helper.file_helper import get_file_path
from ..model.config_manager import Config_manager


config_manager = Config_manager()

fileDir = os.path.dirname(os.path.abspath(__file__))
file_path = get_file_path(
    '/data/sites_infos.csv', fileDir)


def add_region(df_sites):
    df_sites = df_sites[['idbldsite', 'sname',
                         'latitude', 'longitude', 'customer']]

    if os.path.exists(file_path):

        try:
            df_sites_regions = pd.read_csv(
                file_path, sep=';', encoding='utf-8')
        except Exception as e:
            logging.error("{0}:{1} is empty !".format(e.message, file_path))

        df_sites = pd.merge(df_sites, df_sites_regions[['idbldsite', 'customer', 'region', 'region_id']],
                            on=['idbldsite', 'customer'], how='left', indicator=True)

        df_sites_missing_region = df_sites[df_sites['_merge'] == 'left_only']
        df_sites_with_region = df_sites[df_sites['_merge'] == 'both']

        df_sites_missing_region = add_missing_regions(df_sites_missing_region)

        df_sites_with_region = df_sites_with_region.drop(['_merge'], 1)

        df_sites_infos = pd.concat(
            [df_sites_missing_region, df_sites_with_region], axis=0)
        save_sites_infos(df_sites_infos)

    else:
        df_sites_regions = pd.DataFrame()
        df_sites_infos = add_missing_regions(df_sites)

        save_sites_infos(df_sites_regions, df_sites_infos)

    return df_sites_infos


def save_sites_infos(df_sites_regions, df_sites_missing_region=None):
    if df_sites_missing_region is not None:
        df_sites_regions = pd.concat(
            [df_sites_regions, df_sites_missing_region], axis=0)
    print "df_sites_regions.head()"
    print df_sites_regions.head()
    df_sites_regions = df_sites_regions[
        ['customer', 'idbldsite', 'latitude', 'longitude', 'region', 'region_id', 'sname']]
    df_sites_regions.to_csv(file_path, sep=';', encoding='utf-8')


def add_missing_regions(df_sites_missing_region):
    if df_sites_missing_region.empty:
        return
    df_sites_missing_region['region'] = df_sites_missing_region.apply(
        lambda row: get_region_from_API(row['latitude'], row['longitude']), axis=1)

    df_sites_missing_region = add_missing_region_id(df_sites_missing_region)

    return df_sites_missing_region


def add_missing_region_id(df_sites_missing_region):
    df_regions = pd.read_csv(get_file_path(
        '/data/regions_countries.csv', fileDir), sep=';', encoding='utf-8')

    try:
        df_sites_missing_region = df_sites_missing_region.drop(
            ['region_id', '_merge'], 1)
    except ValueError as e:
        logging.warning("Could not find this column :{}".format(e.message))
    print "df_regions"

    df_sites_missing_region = pd.merge(df_sites_missing_region, df_regions[
                                       ['region', 'region_id']], on='region')
    return df_sites_missing_region


def get_region_from_API(latitude, longitude):
    """retrieve the local name of the region for a given coordinate.

    Args:
        latitude (int):
        longitude (int):

    Returns:
        str: local name of the region
    """

    MAPS_URL = config_manager.geocoding_API['maps_url']
    KEY = config_manager.geocoding_API['key']

    coord_str = str(latitude) + "," + str(longitude)
    try:
        response = urllib2.urlopen(
            MAPS_URL + coord_str + "&key=" + KEY)
        data = json.loads(response.read())
        address_components = data['results'][0]['address_components']

        region = [x for x in address_components
                  if 'administrative_area_level_1' in x[
                      'types']][0]['long_name'].encode('utf-8)')
        return region
    except (AttributeError, IndexError) as e:
        logging.error(data)
        logging.error("Could not retrieve the region name for :" + coord_str)
        logging.error(e.message)
