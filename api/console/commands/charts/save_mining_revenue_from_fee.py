import click
import psycopg2
import psycopg2.extras
import requests

from config import config


@click.command(name='charts:save:mining-revenue-from-fee')
def handle():
    response = requests.get(
        'https://charts.coinmetrics.io/pro-api/v4/timeseries/asset-metrics', {
            'assets': 'btc',
            'metrics': 'FeeRevPct',
            'page_size': 10000,
            'api_key': config['api.coinmetrics.io']['api_key'],
        }).json()
    chart_data = response['data']
    insert = []

    for item in chart_data:
        insert.append((
            'carbon_accounting_tool.miners_revenue',
            'Transaction fees',
            float(item['FeeRevPct']),
            item['time'][:10]
        ))
        insert.append((
            'carbon_accounting_tool.miners_revenue',
            'Mining rewards',
            100 - float(item['FeeRevPct']),
            item['time'][:10]
        ))

    with psycopg2.connect(**config['custom_data']) as connection:
        cursor = connection.cursor()
        psycopg2.extras.execute_values(
            cursor,
            'insert into charts (name, label, value, date) values %s on conflict (name, label, date) do nothing',
            insert
        )
