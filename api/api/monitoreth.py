from datetime import datetime
from urllib.parse import urljoin

import requests as requests

from config import config


class Monitoreth:
    def __init__(self, base_url='https://monitoreth.io/data-api/api/'):
        self.base_url = base_url

    def geographical_distribution(self):
        try:
            response = requests.get(
                urljoin(self.base_url, 'eth/v1/nodes/consensus/all/geographical_distribution/historic'),
                {
                    'start_time': '2022-01-01T00:00:00Z',
                    'end_time': f'{datetime.now().year + 1}-01-01T00:00:00Z',
                },
                headers={
                    'X-Api-Key': config['monitoreth']['api_key']
                }
            ).json()

            result = {}

            for item in response:
                date = item['timestamp'][:-10]
                for data in item['data']:
                    result[date + data['country_code']] = {
                        'date': item['timestamp'],
                        'country': data['country_code'],
                        'number_of_nodes': data['node_count'],
                    }
            return list(result.values())
        except:
            return []

    def client_diversity(self):
        try:
            response = requests.get(
                urljoin(self.base_url, 'eth/v1/nodes/consensus/all/client_diversity/historic'),
                {
                    'start_time': '2022-01-01T00:00:00Z',
                    'end_time': f'{datetime.now().year + 1}-01-01T00:00:00Z',
                },
                headers={
                    'X-Api-Key': config['monitoreth']['api_key']
                }
            ).json()

            result = {}

            for item in response:
                date = item['timestamp'][:-10]
                result[date] = {
                    'Date': item['timestamp']
                }
                for data in item['data']:
                    result[date]['others' if data['client_name'] == 'unknown' else data['client_name']] = data['node_count']
            return list(result.values())
        except:
            return []
