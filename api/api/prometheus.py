from helpers import daterange

from datetime import datetime
from urllib.parse import urljoin

import requests as requests


class Prometheus:
    def __init__(self, base_url='http://135.125.246.61:9090/api/v1/'):
        self.base_url = base_url

    def crawler_observed_client_distribution(
        self,
        start_date='2021-12-04 22:30',
        start_date_format='%Y-%m-%d %H:%M',
        end_date=datetime.today().strftime('%Y-%m-%d'),
        end_date_format='%Y-%m-%d',
    ):
        prometheus_data = {}
        start = datetime.strptime(start_date, start_date_format)
        end = datetime.strptime(end_date, end_date_format)

        response = requests.get(urljoin(self.base_url, 'query_range'), {
            'query': 'crawler_observed_client_distribution',
            'start': int(start.timestamp()),
            'end': int(end.timestamp()),
            'step': 86400
        }).json()

        for item in response['data']['result']:
            for [timestamp, value] in item['values']:
                if timestamp not in prometheus_data:
                    prometheus_data[timestamp] = {
                        'Date': datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
                    }
                prometheus_data[timestamp][str(item['metric']['client']).lower()] = value

        return list(prometheus_data.values())

    def crawler_geographical_distribution(
        self,
        start_date='2021-12-04 22:30',
        start_date_format='%Y-%m-%d %H:%M',
        end_date=datetime.today().strftime('%Y-%m-%d'),
        end_date_format='%Y-%m-%d',
    ):
        prometheus_data = []
        start = datetime.strptime(start_date, start_date_format)
        end = datetime.strptime(end_date, end_date_format)

        response = requests.get(urljoin(self.base_url, 'query_range'), {
            'query': 'crawler_geographical_distribution',
            'start': int(start.timestamp()),
            'end': int(end.timestamp()),
            'step': 86400
        }).json()

        for item in response['data']['result']:
            for [timestamp, value] in item['values']:
                prometheus_data.append({
                    'country': item['metric']['country'],
                    'number_of_nodes': value,
                    'date': datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
                })

        return prometheus_data


class Prometheus2(Prometheus):
    def __init__(self, base_url='http://54.37.78.185:9090/api/v1/'):
        super().__init__(base_url=base_url)

    def crawler_observed_client_distribution(
        self,
        start_date='2021-12-04 22:30',
        start_date_format='%Y-%m-%d %H:%M',
        end_date=datetime.today().strftime('%Y-%m-%d') + ' 22:30',
        end_date_format='%Y-%m-%d %H:%M',
    ):
        prometheus_data = {}
        start = datetime.strptime(start_date, start_date_format).replace(tzinfo=timezone.utc)
        end = datetime.strptime(end_date, end_date_format).replace(tzinfo=timezone.utc)

        response = requests.get(urljoin(self.base_url, 'query_range'), {
            'query': 'crawler_observed_client_distribution',
            'start': int(start.timestamp()),
            'end': int(end.timestamp()),
            'step': 86400
        }).json()

        for item in response['data']['result']:
            for [timestamp, value] in item['values']:
                if timestamp not in prometheus_data and item['metric']['job'] == 'mainnet_crawler':
                    prometheus_data[timestamp] = {
                        'Date': datetime.fromtimestamp(timestamp, tz=pytz.utc).strftime('%Y-%m-%d %H:%M:%S')
                    }
                prometheus_data[timestamp][str(item['metric']['client']).lower()] = value

        return [x | {'others': x['unknown']} for x in list(prometheus_data.values())]

    def crawler_geographical_distribution(
        self,
        start_date='2021-12-04 22:30',
        start_date_format='%Y-%m-%d %H:%M',
        end_date=datetime.today().strftime('%Y-%m-%d') + ' 22:30',
        end_date_format='%Y-%m-%d %H:%M',
    ):
        prometheus_data = []
        start = datetime.strptime(start_date, start_date_format).replace(tzinfo=timezone.utc)
        end = datetime.strptime(end_date, end_date_format).replace(tzinfo=timezone.utc)

        response = requests.get(urljoin(self.base_url, 'query_range'), {
            'query': 'crawler_geographical_distribution',
            'start': int(start.timestamp()),
            'end': int(end.timestamp()),
            'step': 86400
        }).json()

        for item in response['data']['result']:
            if item['metric']['job'] != 'mainnet_crawler':
                continue
            for [timestamp, value] in item['values']:
                prometheus_data.append({
                    'country': item['metric']['country'],
                    'number_of_nodes': value,
                    'date': datetime.fromtimestamp(timestamp, tz=pytz.utc).strftime('%Y-%m-%d %H:%M:%S')
                })

        return prometheus_data
