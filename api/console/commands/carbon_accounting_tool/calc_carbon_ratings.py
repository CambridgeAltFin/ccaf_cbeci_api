import click
import psycopg2
import psycopg2.extras
import requests
from datetime import datetime, timedelta

from config import config
from helpers import daterange


@click.command(name='carbon-accounting-tool:calc:carbon-ratings')
def handle():
    with psycopg2.connect(**config['custom_data']) as connection:
        cursor = connection.cursor()
        dates = list(daterange(datetime(2010, 12, 1), datetime.now() - timedelta(days=1)))

        entries = [{'date': date.strftime('%Y-%m-%d'), 'value': 1} for date in dates]

        tx_count = requests.post(
            'https://v2.api.carbon-ratings.com/currencies/btc/electricity-consumption/txCount/hybrid-based?key=' +
            config['api_carbon_ratings']['api_key'], json={'entries': entries}).json()
        holdings = requests.post(
            'https://v2.api.carbon-ratings.com/currencies/btc/electricity-consumption/holdings/hybrid-based?key=' +
            config['api_carbon_ratings']['api_key'], json={'entries': entries}).json()

        insert = []
        for date in dates:
            insert.append((
                next(x['outputValue'] for x in tx_count['entries'] if x['date'] == date.strftime('%Y-%m-%d')),
                next(x['outputValue'] for x in holdings['entries'] if x['date'] == date.strftime('%Y-%m-%d')),
                None,
                'btc',
                date.strftime('%Y-%m-%d'),
            ))

        eth2_emissions = requests.get(
            'https://v2.api.carbon-ratings.com/currencies/eth2/emissions/network?key=' +
            config['api_carbon_ratings']['api_key']
        ).json()

        for entry in eth2_emissions['entries']:
            insert.append((
                None,
                None,
                entry['emissions_365d'],
                'eth_pos',
                entry['date'],
            ))

        psycopg2.extras.execute_values(
            cursor,
            """
            insert into carbon_ratings ("kWh_per_tx", "kWh_per_holding", "emissions_365d", asset, date) 
            values %s
            on conflict(asset, date) do nothing
            """,
            insert
        )
