from helpers import daterange

from datetime import datetime
from urllib.parse import urljoin

import requests as requests


class Prometheus:
    def __init__(self, base_url='http://135.125.246.61:9090/api/v1/'):
        self.base_url = base_url

    def crawler_observed_client_distribution(
        self,
        start_date='2021-12-04',
        start_date_format='%Y-%m-%d',
        end_date=datetime.today().strftime('%Y-%m-%d'),
        end_date_format='%Y-%m-%d',
    ):
        prometheus_data = []
        start = datetime.strptime(start_date, start_date_format)
        end = datetime.strptime(end_date, end_date_format)
        for date in daterange(start, end):
            [year, month, day] = date.strftime('%Y-%m-%d').split('-')
            response = requests.get(urljoin(self.base_url, 'query'), {
                'query': 'crawler_observed_client_distribution',
                'time': int(datetime(year=int(year), month=int(month), day=int(day), hour=22, minute=30).timestamp())
            }).json()
            if len(response['data']['result']) == 0:
                continue
            item = {
                'Date': date.strftime('%Y-%m-%d')
            }
            for i in response['data']['result']:
                item[i['metric']['client']] = int(i['value'][1])
            prometheus_data.append(item)
        return prometheus_data
