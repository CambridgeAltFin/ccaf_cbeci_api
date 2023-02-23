from datetime import datetime, timedelta

from api.digiconomist import Digiconomist
from config import config, Connection
from components.gas_emission.gas_emission_factory import GreenhouseGasEmissionServiceFactory

import click
import psycopg2


@click.command(name='charts:calc:bitcoin-greenhouse-gas-emissions')
def handle():
    service = GreenhouseGasEmissionServiceFactory.create()

    with psycopg2.connect(**config[Connection.custom_data]) as connection:
        cursor = connection.cursor()

        cursor.execute('TRUNCATE greenhouse_gas_emissions')
        for cents in range(1, 21):
            price = round(cents / 100, 2)
            greenhouse_gas_emissions = service.calc_greenhouse_gas_emissions(price)
            insert = 'INSERT INTO greenhouse_gas_emissions (timestamp,date,price,value,name) VALUES '
            values = []
            for _, item in greenhouse_gas_emissions.iterrows():
                row = to_dict(item, cents)
                placeholder = ','.join(['%s' for _ in row.values()])
                values.append(cursor.mogrify('(' + placeholder + ')', tuple(row.values())).decode('utf-8'))
            insert += ','.join(values)
            cursor.execute(insert)

        cursor.execute('TRUNCATE cumulative_greenhouse_gas_emissions')
        for cents in range(1, 21):
            price = round(cents / 100, 2)
            greenhouse_gas_emissions = service.calc_total_greenhouse_gas_emissions(price)
            insert = 'INSERT INTO cumulative_greenhouse_gas_emissions (timestamp,date,price,value,cumulative_value) ' \
                     'VALUES '
            values = []
            for _, item in greenhouse_gas_emissions.iterrows():
                row = to_cum_dict(item, cents)
                placeholder = ','.join(['%s' for _ in row.values()])
                values.append(cursor.mogrify('(' + placeholder + ')', tuple(row.values())).decode('utf-8'))
            insert += ','.join(values)
            cursor.execute(insert)

        digiconomist = Digiconomist()
        yesterday = datetime.today() - timedelta(days=1)
        result = digiconomist.bitcoin(yesterday)
        if len(result) > 0:
            cursor.execute(
                'INSERT INTO digiconomist_btc ("24hr_kWh", "24hr_kgCO2", "Output_kWh", "Output_kgCO2", date, asset) '
                'VALUES (%s, %s, %s, %s, %s, %s)',
                (
                    result[0]['24hr_kWh'],
                    result[0]['24hr_kgCO2'],
                    result[0]['Output_kWh'],
                    result[0]['Output_kgCO2'],
                    yesterday,
                    'btc'
                )
            )


def to_dict(row, cents):
    return {
        'timestamp': int(row['timestamp']),
        'date': row['date'],
        'price': str(round(cents / 100, 2)),
        'value': round(row['value'], 6),
        'name': row['name']
    }


def to_cum_dict(row, cents):
    return {
        'timestamp': int(row['timestamp']),
        'date': row['date'],
        'price': str(round(cents / 100, 2)),
        'value': round(row['v'], 6),
        'cumulative_value': round(row['cumulative_v'], 6)
    }
