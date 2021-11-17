
from config import config
from components.energy_consumption import EnergyConsumptionServiceFactory

import click
import psycopg2
from datetime import datetime


@click.command(name='data:calc:energy-consumptions')
def handle():
    with psycopg2.connect(**config['custom_data']) as connection:
        cursor = connection.cursor()
        cursor.execute('TRUNCATE energy_consumptions')

        service = EnergyConsumptionServiceFactory.create()
        for cents in range(1, 21):
            insert = 'INSERT INTO energy_consumptions (timestamp,price,date,guess_consumption,max_consumption,min_consumption,guess_power,max_power,min_power) VALUES '
            values = []
            for timestamp, item in service.calc_data(cents / 100):
                row = to_dict(timestamp, item, cents)
                placeholder = ','.join(['%s' for _ in row.values()])
                values.append(cursor.mogrify('(' + placeholder + ')', tuple(row.values())).decode('utf-8'))
            insert += ','.join(values)
            cursor.execute(insert)


def to_dict(timestamp, row, cents):
    return {
        'timestamp': timestamp,
        'price': str(round(cents / 100, 2)),
        'date': datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d'),
        'guess_consumption': row['guess_consumption'],
        'max_consumption': row['max_consumption'],
        'min_consumption': row['min_consumption'],
        'guess_power': row['guess_power'],
        'max_power': row['max_power'],
        'min_power': row['min_power']
    }
