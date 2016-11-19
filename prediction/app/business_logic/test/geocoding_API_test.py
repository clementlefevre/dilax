from collections import namedtuple
from ..service.geocoding_API_service import get_region


def test_get_region():
    Site = namedtuple(
        'Site', ['idbldsite', 'site_name', 'latitude', 'longitude', 'region'])
    sites = [Site(idbldsite=12, site_name='Stuttgart',
                  latitude=48.776630499999996,
                  longitude=9.176330499999999,
                  region='Baden-W\xc3\xbcrttemberg'),
             Site(idbldsite=1, site_name='Hamburg City',
                  latitude=53.550842, longitude=9.995385, region='Hamburg'),
             Site(idbldsite=2, site_name='Hamburg AEZ', latitude=53.654227,
                  longitude=10.091736000000001, region='Hamburg'),
             Site(idbldsite=3, site_name='Muenchen Damen',
                  latitude=48.142641, longitude=11.576903, region='Bayern'),
             Site(idbldsite=4, site_name='Muenchen Herren',
                  latitude=48.142641, longitude=11.576903, region='Bayern'),
             Site(idbldsite=5, site_name='Duesseldorf Herren',
                  latitude=51.224403,
                  longitude=6.774597999999999, region='Nordrhein-Westfalen'),
             Site(idbldsite=6, site_name='Duesseldorf Damen',
                  latitude=51.224403,
                  longitude=6.774597999999999, region='Nordrhein-Westfalen'),
             Site(idbldsite=7, site_name='Wiesbaden', latitude=50.081434,
                  longitude=8.244644000000001, region='Hessen'),
             Site(idbldsite=8, site_name='Bikini Berlin',
                  latitude=52.50548199999999,
                  longitude=13.335294000000001, region='Berlin'),
             Site(idbldsite=9, site_name='Osnabrueck', latitude=52.276045,
                  longitude=8.043023, region='Niedersachsen'),
             Site(idbldsite=10, site_name='Nuernberg',
                  latitude=49.452290999999995,
                  longitude=11.074521, region='Bayern'),
             Site(idbldsite=11, site_name='Frankfurt MTZ', latitude=50.1109221,
                  longitude=8.6821267, region='Hessen')]
    for site in sites:

        assert site.region.decode(
            'utf-8') == get_region(site.latitude, site.longitude).decode('utf-8')
