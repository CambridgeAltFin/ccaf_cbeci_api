import click
import requests
import pandas as pd
from datetime import datetime


@click.command(name='cbsi:compare:datasource')
def handle():
    beaconnodes = requests.get(
        'https://migalabs.es/metabase/api/public/card/ffdccfd0-60c2-424e-ab53-345e2f59b105/query?parameters=%5B%5D'
    ).json()

    beaconnodes_df = pd.DataFrame.from_records([
        {
            'Date': date[:10],
            'prysm': prysm,
            'lighthouse': lighthouse,
            'teku': teku,
            'nimbus': nimbus,
            'lodestar': lodestar,
            'grandine': grandine,
            'others': others
        }
        for [i, date, parser, prysm, lighthouse, teku, nimbus, lodestar, grandine, others]
        in beaconnodes['data']['rows']
    ])

    csv_df = pd.read_csv('/home/ihor/Downloads/client_dist.csv')
    csv_df['Date'] = pd.to_datetime(csv_df['Dist Date']).dt.strftime('%Y-%m-%d')
    csv_df = csv_df[['Date', 'Prysm', 'Lighthouse', 'Teku', 'Nimbus', 'Lodestar', 'Grandine', 'Others']]

    prometheus = requests.get(
        'http://135.125.246.61:9090/api/v1/query_range?query=crawler_observed_client_distribution&start=1638378000&end=1669914000&step=126144&_=1669906861333'
    ).json()

    prometheus_formatted = {}

    for item in prometheus['data']['result']:
        metric = item['metric']['client']
        for [timestamp, value] in item['values']:
            if timestamp not in prometheus_formatted:
                prometheus_formatted[timestamp] = {
                    'Date': datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d')
                }
            prometheus_formatted[timestamp][metric] = value
    prometheus_df = pd.DataFrame.from_records([item for ts, item in list(prometheus_formatted.items())])

    csv_prometheus_df = csv_df.merge(prometheus_df, on='Date', how='left', suffixes=('_csv', '_prometheus'))
    beaconnodes_csv_prometheus_df = beaconnodes_df.merge(csv_prometheus_df, on='Date', how='left', suffixes=('_beaconnodes', ''))
