
import click
import psycopg2

from config import config
from components.gas_emission.gas_emission_factory import EmissionIntensityServiceFactory
from components.gas_emission.gas_emission_factory import GasEmissionRepositoryFactory


@click.command(name='charts:calc:bitcoin-emission-intensity')
def handle():
    service = EmissionIntensityServiceFactory.create()
    service.create_predicted_points()

    with psycopg2.connect(**config['custom_data']) as connection:
        cursor = connection.cursor()
        intensity = GasEmissionRepositoryFactory.create().get_actual_btc_emission_intensity()
        cursor.execute(
            'UPDATE ghg_emission_intensities SET value = %s WHERE name = %s AND year = %s',
            (intensity['value'], intensity['name'], intensity['year'])
        )
