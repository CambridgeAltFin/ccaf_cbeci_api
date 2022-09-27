
from config import config
from components.energy_consumption import EnergyConsumptionServiceFactory

import click
import psycopg2


@click.command(name='data:calc:cumulative-electricity-estimates')
def handle():
    with psycopg2.connect(**config['custom_data']) as connection:
        cursor = connection.cursor()
        cursor.execute('TRUNCATE cumulative_energy_consumptions')

        service = EnergyConsumptionServiceFactory.create()
        for cents in range(1, 21):
            price = cents / 100
            insert = 'INSERT INTO cumulative_energy_consumptions (timestamp,price,date,monthly_consumption,cumulative_consumption) VALUES '
            values = []
            for timestamp, item in service.calc_monthly_data(price, current_month=True):
                row = to_dict(timestamp, item, price)
                placeholder = ','.join(['%s' for _ in row.values()])
                values.append(cursor.mogrify('(' + placeholder + ')', tuple(row.values())).decode('utf-8'))
            insert += ','.join(values)
            cursor.execute(insert)


def to_dict(date, row, price):
    return {
        'timestamp': int(date.timestamp()),
        'price': price,
        'date': date.strftime('%Y-%m-%d'),
        'monthly_consumption': row['guess_consumption'],
        'cumulative_consumption': row['cumulative_guess_consumption'],
    }
