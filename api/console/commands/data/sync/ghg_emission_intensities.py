import click
import psycopg2
import requests
from psycopg2.extras import execute_values

from config import config


@click.command(name='data:sync:ghg-emission-intensities')
def handle():
    response = requests.get('https://www.eea.europa.eu/data-and-maps/daviz/co2-emission-intensity-9/download.exhibit')\
        .json()

    with psycopg2.connect(**config['custom_data']) as connection:
        cursor = connection.cursor()
        cursor.execute('SELECT id, country FROM countries')
        countries = cursor.fetchall()

        values = []

        for item in response['items']:
            geo_name = item['ugeo']
            if geo_name == 'Slovakia':
                geo_name = 'Slovak Republic'
            if geo_name == 'Czechia':
                geo_name = 'Czech Republic'

            country = next((country_id for country_id, country_name in countries if country_name == geo_name), None)

            value = item['index'] if item['index'] else None
            values.append((country, geo_name, item['date'], value))

        execute_values(
            cursor,
            'INSERT INTO ghg_emission_intensities (country_id, name, year, value) '
            'VALUES %s '
            'ON CONFLICT (name, year) DO NOTHING',
            values
        )
