
import requests
from urllib.parse import urljoin


class Gas:
    ALL_GHG = 385


class Sector:
    TOTAL_INCL_LUCF = 1718


class Source:
    CAIT = 148


class ClimateWatchData:

    def __init__(self, base_url='https://www.climatewatchdata.org/api/v1/'):
        self.base_url = base_url

    def emissions(self, gas, sector, source, location=None):
        url = urljoin(self.base_url, 'emissions')

        return requests.get(url, params={
            'gas': gas,
            'location': location,
            'sector': sector,
            'source': source,
        }).json()
