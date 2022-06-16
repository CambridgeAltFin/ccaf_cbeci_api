
import click
import psycopg2
import json

from config import config


@click.command(name='countries:save:code-3')
def handle():
    with psycopg2.connect(**config['custom_data']) as connection, open('../storage/countries.json') as countries:
        cursor = connection.cursor()
        for country in json.loads(countries.read()):
            cursor.execute('UPDATE countries SET code_3 = %s WHERE code = %s', (country['alpha-3'], country['alpha-2']))
