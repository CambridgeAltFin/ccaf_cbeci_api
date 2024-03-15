import datetime
from urllib.parse import urljoin

import requests as requests


class Digiconomist:
    def __init__(self, base_url='https://digiconomist.net/wp-json/mo/v1/'):
        self.base_url = base_url

    def bitcoin(self, date):
        format_date = date.strftime('%Y%m%d')
        return requests.get(urljoin(self.base_url, f'bitcoin/stats/{format_date}')).json()

    def ethereum(self, date):
        format_date = date.strftime('%Y%m%d')
        return requests.get(urljoin(self.base_url, f'ethereum/stats/{format_date}')).json()
