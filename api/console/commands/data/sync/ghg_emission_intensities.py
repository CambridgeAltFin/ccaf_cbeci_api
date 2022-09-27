import click
import psycopg2
import requests
from psycopg2.extras import execute_values

from components.gas_emission.gas_emission_factory import GasEmissionRepositoryFactory

from config import config


def eae_api():
    response = requests.get('https://www.eea.europa.eu/data-and-maps/daviz/co2-emission-intensity-9/download.exhibit') \
        .json()

    for item in response['items']:
        geo_name = item['ugeo']
        if geo_name == 'Slovakia':
            geo_name = 'Slovak Republic'
        if geo_name == 'Czechia':
            geo_name = 'Czech Republic'
        yield {
            'name': geo_name,
            'code': None,
            'date': item['date'],
            'value': item['index'] if item['index'] else None
        }


def ember_climate_api():
    response = requests.get(
        'https://ember-data-api-scg3n.ondigitalocean.app/ember/country_overview_yearly.json?_shape=array'
    ).json()

    for item in response:
        yield {
            'name': item['country_or_region'],
            'code': item['country_code'],
            'date': item['year'],
            'value': item['emissions_intensity_gco2_per_kwh'] if item['emissions_intensity_gco2_per_kwh'] else None
        }


@click.command(name='data:sync:ghg-emission-intensities')
def handle():
    with psycopg2.connect(**config['custom_data']) as connection:
        cursor = connection.cursor()
        cursor.execute('SELECT id, country, code3 FROM countries')
        countries = cursor.fetchall()

        btc_emission_intensity = GasEmissionRepositoryFactory.create().get_actual_btc_emission_intensity()

        values = [(
            btc_emission_intensity['country_id'],
            btc_emission_intensity['name'],
            btc_emission_intensity['year'],
            btc_emission_intensity['value']
        )]

        for item in ember_climate_api():

            country = next(
                (country_id for country_id, name, code in countries if name == item['name'] or code == item['code']),
                None
            )

            values.append((country, item['name'], item['date'], item['value']))

        execute_values(
            cursor,
            'INSERT INTO ghg_emission_intensities (country_id, name, year, value) '
            'VALUES %s '
            'ON CONFLICT (name, year) DO NOTHING',
            values
        )
