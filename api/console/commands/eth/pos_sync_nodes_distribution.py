from api.prometheus import Prometheus, Prometheus2
from api.migalabs import Migalabs
from config import config

import click
import psycopg2
import psycopg2.extras


@click.command(name='eth-pos:sync:nodes-distribution')
def handle():
    data = Migalabs().beacon_chain_node_distribution()
    prometheus_data = Prometheus().crawler_geographical_distribution()
    new_prometheus_data = Prometheus2().crawler_geographical_distribution()
    min_date = min(new_prometheus_data, key=lambda x: x['date'])
    new_prometheus_data.extend([x for x in prometheus_data if x['date'] < min_date['date']])

    with psycopg2.connect(**config['custom_data']) as conn:
        cursor = conn.cursor()
        cursor.execute('select id, code from countries')
        countries = {code: i for (i, code) in cursor.fetchall()}

        psycopg2.extras.execute_values(
            cursor,
            'insert into eth_pos_nodes_distribution (country_id, number_of_nodes, source, date)  '
            'VALUES %s on conflict (country_id, source, date) do nothing',
            [(
                countries[i['country']],
                i.get('number_of_nodes', 0),
                'migalabs',
                i.get('date', None),
            ) for i in data if i['country'] in countries]
        )

        psycopg2.extras.execute_values(
            cursor,
            'insert into eth_pos_nodes_distribution (country_id, number_of_nodes, source, date)  '
            'VALUES %s on conflict (country_id, source, date) do update set number_of_nodes = EXCLUDED.number_of_nodes',
            [(
                countries[i['country']],
                i.get('number_of_nodes', 0),
                'prometheus',
                i.get('date', None),
            ) for i in list(filter(lambda x: x['country'] in countries, new_prometheus_data))]
        )
