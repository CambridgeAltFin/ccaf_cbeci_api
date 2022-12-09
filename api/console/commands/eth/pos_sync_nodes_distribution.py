from api.prometheus import Prometheus
from api.migalabs import Migalabs
from config import config

import click
import psycopg2
import psycopg2.extras


@click.command(name='eth-pos:sync:nodes-distribution')
def handle():
    data = Migalabs().beacon_chain_node_distribution()

    with psycopg2.connect(**config['custom_data']) as conn:
        cursor = conn.cursor()
        cursor.execute('select id, code from countries')
        countries = {code: i for (i, code) in cursor.fetchall()}

        psycopg2.extras.execute_values(
            cursor,
            'insert into eth_pos_nodes_distribution (country_id, number_of_nodes)  '
            'VALUES %s on conflict (country_id) do nothing',
            [(
                countries[i['country']],
                i.get('number_of_nodes', 0),
            ) for i in data]
        )