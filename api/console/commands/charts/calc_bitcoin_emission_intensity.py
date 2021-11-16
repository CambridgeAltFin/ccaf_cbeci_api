
import click
import datetime

from components.gas_emission.gas_emission_factory import EmissionIntensityServiceFactory


@click.command(name='charts:calc:bitcoin-emission-intensity')
def handle():
    service = EmissionIntensityServiceFactory.create()
    service.create_provisional_point(datetime.datetime.now())
