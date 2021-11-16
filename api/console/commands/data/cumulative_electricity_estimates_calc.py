
from services.energy_analytic import EnergyAnalytic
from config import config
from datetime import datetime

import click
import psycopg2


@click.command(name='data:calc:cumulative-electricity-estimates')
def handle():
    with psycopg2.connect(**config['custom_data']) as connection:
        cursor = connection.cursor()
        cursor.execute('TRUNCATE cumulative_electricity_consumption_estimates')

        service = EnergyAnalytic()
        for cents in range(1, 21):
            insert = 'INSERT INTO cumulative_electricity_consumption_estimates (timestamp,cents,date,monthly_consumption,cumulative_consumption) VALUES '
            values = []
            for timestamp, item in service.get_monthly_data(cents / 100):
                row = to_dict(timestamp, item, cents)
                placeholder = ','.join(['%s' for _ in row.values()])
                values.append(cursor.mogrify('(' + placeholder + ')', tuple(row.values())).decode('utf-8'))
            insert += ','.join(values)
            cursor.execute(insert)


def to_dict(date, row, cents):
    return {
        'timestamp': int(date.timestamp()),
        'cents': cents,
        'date': date.strftime('%Y-%m-%d'),
        'monthly_consumption': round(row['guess_consumption'], 2),
        'cumulative_consumption': round(row['cumulative_guess_consumption'], 2),
    }
