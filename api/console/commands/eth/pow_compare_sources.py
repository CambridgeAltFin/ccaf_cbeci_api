from config import config

import click
import psycopg2
import psycopg2.extras
import requests


@click.command(name='eth-pow:compare:sources')
def handle():
    with psycopg2.connect(**config['custom_data']) as conn:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute(
            "select guess_consumption, date from consumptions where asset = 'eth' and price = 10 order by date"
        )
        cbnsi_data = [
            {'consumption': float(x['guess_consumption']), 'date': x['date'].strftime('%Y-%m-%d')}
            for x in cursor.fetchall()
        ]
        save_data(cursor, 'CBNSI', cbnsi_data)

        digiconomist_data = []
        for item in cbnsi_data:
            response = requests.get(
                'https://digiconomist.net/wp-json/mo/v1/ethereum/stats/' + str(item['date']).replace('-', '')
            ).json()

            if len(response):
                digiconomist_item = response[0]
                digiconomist_data.append({
                    'date': item['date'],
                    'consumption': int(digiconomist_item['24hr_kWh']) * 365.25 / 10 ** 9
                })
        save_data(cursor, 'Digiconomist', digiconomist_data)

        response = requests.get(
            'https://v2.api.carbon-ratings.com/currencies/eth/electricity-consumption/network?key=' +
            config['api_carbon_ratings']['api_key']
        ).json()
        ccri_data = [{'date': i['date'], 'consumption': i['consumption_365d']} for i in response['entries']]
        save_data(cursor, 'CCRI', ccri_data)

        km_data = []
        response = requests.get('https://kylemcdonald.github.io/ethereum-emissions/output/daily-gw.csv')
        for [date, _, guess, _] in [line.decode('utf-8').split(',') for line in response.iter_lines()][1:]:
            km_data.append({
                'date': date,
                'consumption': float(guess) * 24 * 365 / 1000
            })
        save_data(cursor, 'Kyle McDonald', km_data)


def save_data(cursor, label, data):
    psycopg2.extras.execute_values(
        cursor,
        "insert into charts (name, label, value, date) values %s on conflict (name, label, date) do nothing",
        [('eth.source_comparison', label, i['consumption'], i['date']) for i in data]
    )
