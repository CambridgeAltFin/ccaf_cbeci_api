from helpers import daterange

from datetime import datetime
from urllib.parse import urljoin

import requests as requests


class Migalabs:
    def __init__(self, base_url='https://migalabs.es/metabase/api/public/'):
        self.base_url = base_url

    def beacon_chain_client_distribution_over_time(self):
        response = requests.get(urljoin(self.base_url, 'card/ffdccfd0-60c2-424e-ab53-345e2f59b105/query')).json()

        return [
            {
                'Date': date[:10],
                'Prysm': prysm,
                'Lighthouse': lighthouse,
                'Teku': teku,
                'Nimbus': nimbus,
                'Lodestar': lodestar,
                'Grandine': grandine,
                'Others': others
            }
            for [i, date, parser, prysm, lighthouse, teku, nimbus, lodestar, grandine, others]
            in response['data']['rows']
        ]
