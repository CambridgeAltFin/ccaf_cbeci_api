
from services.energy_analytic import EnergyAnalytic
from config import config
from datetime import datetime

import click
import psycopg2


@click.command(name='data:calc:electricity-estimates')
def handle():
    with psycopg2.connect(**config['custom_data']) as connection:
        cursor = connection.cursor()
        cursor.execute('TRUNCATE electricity_consumption_estimates')

        service = EnergyAnalytic()
        for cents in range(1, 21):
            insert = 'INSERT INTO electricity_consumption_estimates (timestamp,cents,date,year,guess_consumption,max_consumption,min_consumption,guess_power,max_power,min_power) VALUES '
            values = []
            for timestamp, item in service.get_data(cents / 100):
                row = to_dict(timestamp, item, cents)
                placeholder = ','.join(['%s' for _ in row.values()])
                values.append(cursor.mogrify('(' + placeholder + ')', tuple(row.values())).decode('utf-8'))
            insert += ','.join(values)
            cursor.execute(insert)


def to_dict(timestamp, row, cents):
    return {
        'timestamp': timestamp,
        'cents': cents,
        'date': datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d'),
        'year': datetime.utcfromtimestamp(timestamp).strftime('%Y'),
        'guess_consumption': row['guess_consumption'],
        'max_consumption': row['max_consumption'],
        'min_consumption': row['min_consumption'],
        'guess_power': row['guess_power'],
        'max_power': row['max_power'],
        'min_power': row['min_power']
    }
