from datetime import date, timedelta
from config import config

import click
import psycopg2
import psycopg2.extras
import requests


@click.command(name='eth-pos:live-data')
def handle():
    today = date.today()
    yesterday = today - timedelta(days=2)
    digiconomist_response = requests.get(
        'https://digiconomist.net/wp-json/mo/v1/ethereum/stats/' + yesterday.strftime('%y%m%d')
    ).json()
    digiconomist = None
    if len(digiconomist_response) and digiconomist_response[0]:
        digiconomist = round(float(digiconomist_response[0]['24hr_kWh']) * 365.25 / 10 ** 6, 2)
    ccri_response = requests.get(
        f"https://v2.api.carbon-ratings.com/currencies/eth2/electricity-consumption/network?key={config['api_carbon_ratings']['api_key']}"
    ).json()
    ccri = None
    if ccri_response['entries']:
        ccri = round(ccri_response['entries'][-1]['consumption_365d'] / 10 ** 6, 2)

    with psycopg2.connect(**config['custom_data']) as conn:
        cursor = conn.cursor()
        if digiconomist is not None:
            cursor.execute("""
                insert into charts (name, label, value, date) 
                values ('eth_pos.live_data', 'Digiconomist', %s, %s) 
                on conflict (name, label, date) do nothing
            """, (digiconomist, yesterday))
        if ccri is not None:
            cursor.execute("""
                insert into charts (name, label, value, date) 
                values ('eth_pos.live_data', 'CCRI', %s, %s) 
                on conflict (name, label, date) do nothing
            """, (ccri, yesterday))

