
from config import config
from components.energy_consumption import EnergyConsumptionServiceFactory

import click
import psycopg2
import psycopg2.extras
from datetime import datetime


@click.command(name='data:calc:energy-consumptions')
def handle():
    with psycopg2.connect(**config['custom_data']) as connection:
        cursor = connection.cursor()
        cursor.execute("DELETE FROM consumptions WHERE asset = 'btc'")

        service = EnergyConsumptionServiceFactory.create()
        for cents in range(1, 21):
            psycopg2.extras.execute_values(
                cursor,
                'INSERT INTO consumptions ('
                '   asset,'
                '   price,'
                '   min_power,'
                '   guess_power,'
                '   max_power,'
                '   min_consumption,'
                '   guess_consumption,'
                '   max_consumption,'
                '   timestamp,'
                '   date,'
                '   version'
                ') VALUES %s',
                [to_tuple(timestamp, item, cents) for timestamp, item in service.calc_data(cents / 100)]
            )


def to_tuple(timestamp, row, cents):
    return (
        'btc',
        cents,
        row['min_power'],
        row['guess_power'],
        row['max_power'],
        row['min_consumption'],
        row['guess_consumption'],
        row['max_consumption'],
        timestamp,
        datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d'),
        '1.4.0'
    )
