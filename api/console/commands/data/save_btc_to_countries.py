from components.energy_consumption.v1_3_1 import EnergyConsumptionServiceFactory
from components.countries import CountryFactory

import click


@click.command(name='save:btc-to-countries')
def handle():
    energy_consumption_service = EnergyConsumptionServiceFactory.create()
    consumption = round(energy_consumption_service.get_actual_data(.05)['guess_consumption'], 2)
    CountryFactory.create_service().update_btc_value(consumption)
