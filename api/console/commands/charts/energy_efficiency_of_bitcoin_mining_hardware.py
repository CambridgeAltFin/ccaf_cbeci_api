
import click
import psycopg2
from dateutil.relativedelta import relativedelta
from datetime import datetime

from config import config
from components.energy_consumption import EnergyConsumptionServiceFactory


weight_map = {0: 1, 1: 0.8, 2: 0.6, 3: 0.4, 4: 0.2, "False": 0}


@click.command(name='charts:save:energy-efficiency-of-bitcoin-mining-hardware')
@click.option('-v', '--version', default='1.4.0')
def handle(version):
    with psycopg2.connect(**config['custom_data']) as connection:
        cursor = connection.cursor()
        energy_consumption = EnergyConsumptionServiceFactory.create()
        equipments = energy_consumption.get_equipment_list(.05)

        for day in equipments:
            date = datetime.utcfromtimestamp(day['timestamp'])
            total_weight = 0
            for equipment in day['equipment_list']:
                release = datetime.utcfromtimestamp(equipment['unix_date_of_release'])
                difference = relativedelta(date, release)
                difference_in_months = difference.years * 12 + difference.months
                equipment['weight'] = weight_map[calculate_years(difference_in_months)]
                total_weight += equipment['weight']
            count = len(day['equipment_list'])
            factor = (count / total_weight) * (1 / count)
            day["adjustment"] = total_weight * factor
            for equipment in day['equipment_list']:
                equipment["adjustment"] = total_weight * factor
        print(equipments[0])


def calculate_years(difference):
    if difference < 0:
        return 'False'
    elif difference < 12:
        return 0
    elif 12 <= difference < 24:
        return 1
    elif 24 <= difference < 36:
        return 2
    elif 36 <= difference < 48:
        return 3
    elif 48 <= difference < 60:
        return 4
    else:
        return 'False'
