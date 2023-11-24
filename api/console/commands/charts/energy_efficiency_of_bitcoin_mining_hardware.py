import click
import psycopg2
import psycopg2.extras

from config import config
from components.energy_consumption import EnergyConsumptionServiceFactory


weight_map = {0: 1, 1: 0.8, 2: 0.6, 3: 0.4, 4: 0.2, "False": 0}


@click.command(name='charts:save:energy-efficiency-of-bitcoin-mining-hardware')
@click.option('-v', '--version', default='1.4.0')
def handle(version):
    with psycopg2.connect(**config['custom_data']) as connection:
        cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        service = EnergyConsumptionServiceFactory.create()
        equipment_list = service.get_equipment_list(.05)

        psycopg2.extras.execute_values(
            cursor,
            '''
            insert into miner_energy_efficients (lower_bound, estimated, upper_bound, date)
            values %s
            on conflict (date) do update set
             lower_bound = EXCLUDED.lower_bound,
             estimated = EXCLUDED.estimated,
             upper_bound = EXCLUDED.upper_bound
            ''',
            [(
                x['profitability_equipment_lower_bound'],
                x['profitability_equipment'],
                x['profitability_equipment_upper_bound'],
                x['date']
            ) for x in equipment_list]
        )
