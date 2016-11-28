"""Summary

Attributes:
    MAPS_URL (TYPE): Description
"""
import os
import urllib2
import json
import pandas as pd
import logging
import inspect
from ..model.config_manager import Config_manager
from ..helper.data_helper import check_missing_data
from ..helper.file_helper import get_file_path


config_manager = Config_manager()

fileDir = os.path.dirname(os.path.abspath(__file__))


def get_region(latitude, longitude):
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
