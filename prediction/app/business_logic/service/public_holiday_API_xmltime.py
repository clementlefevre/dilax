"""This module retrieve the public holidays of a given place.
As of today 2016-11-06, the key is not valid.
Should be replaced by the new API Ferientag.ch (from 2017 onwards)


"""
import urllib2
import json


def get_public_holidays(country, year):
    """Summary

    Args:
        country (TYPE): Description
        year (TYPE): Description

    Returns:
        TYPE: Description
    """
    BASE_URL = "https://api.xmltime.com/holidays?"
    accesskey = "accesskey=rtdGNBuF1E"
    secretkey = "secretkey=K6WiKnRcziiz5H2Kcgw9"
    version = "version=2"
    types = "types=federal"
    country = "country=" + country
    year = "year=" + year
    lang = "lang=fr"

    url = BASE_URL + accesskey + "&" + secretkey + "&" + version + \
        "&" + types + "&" + country + "&" + year + "&" + lang
    print url
    request = urllib2.urlopen(url)
    data = json.loads(request.read())

    return data
