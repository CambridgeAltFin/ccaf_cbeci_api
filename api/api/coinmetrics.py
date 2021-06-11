import requests
import logging
from collections import namedtuple
from datetime import datetime
from urllib.parse import urljoin

LOGGER = logging.getLogger()

def get_data(url, params=None, **kwargs):
    data = []
    response = requests.get(url, params=params, **kwargs).json()

    if 'data' in response:
        data.extend(response['data'])

    if 'next_page_token' in response:
        params['next_page_token'] = response['next_page_token']
        data.extend(get_data(url, params, **kwargs))

    return data



class CoinMetrics:

    def __init__(self, api_key, base_url='https://api.coinmetrics.io/v4/'):
        self.base_url = base_url
        self.api_key = api_key

    def timeseries(self):
        base_url = urljoin(self.base_url, 'timeseries/')
        Timeseries = namedtuple("timeseries", ["asset_metrics"])

        def asset_metrics(metrics, start_time=None, end_time=None, assets='btc', frequency='1d'):
            params = {
                'api_key': self.api_key,
                'assets': assets,
                'metrics': metrics,
                'frequency': frequency,
                'start_time': start_time.isoformat() if isinstance(start_time, datetime) else start_time,
                'end_time': end_time.isoformat() if isinstance(end_time, datetime) else end_time
            }

            return get_data(urljoin(base_url, 'asset-metrics'), params=params, timeout=3)

        return Timeseries(
            asset_metrics
        )

