from api.prometheus import Prometheus
from api.migalabs import Migalabs
from config import config

import click
import psycopg2
import psycopg2.extras


@click.command(name='eth-pos:sync:nodes')
def handle():
    migalabs_data = Migalabs().beacon_chain_client_distribution_over_time()

    with psycopg2.connect(**config['custom_data']) as conn:
        cursor = conn.cursor()
        cursor.execute("select max(date) from eth_pos_nodes where source = 'prometheus'")
        [(date,)] = cursor.fetchall()
        prometheus_data = Prometheus().crawler_observed_client_distribution(
            start_date=date.strftime('%Y-%m-%d')) if date else Prometheus().crawler_observed_client_distribution()
        save_data(cursor, prometheus_data, 'prometheus')
        save_data(cursor, migalabs_data, 'migalabs')


def save_data(cursor, data, source):
    psycopg2.extras.execute_values(
        cursor,
        'insert into eth_pos_nodes (prysm, lighthouse, teku, nimbus, lodestar, grandine, others, source, date) '
        'VALUES %s on conflict (source, date) do nothing',
        [(
            i.get('Prysm', 0),
            i.get('Lighthouse', 0),
            i.get('Teku', 0),
            i.get('Nimbus', 0),
            i.get('Lodestar', 0),
            i.get('Grandine', 0),
            i.get('Others', 0),
            source,
            i['Date']
        ) for i in data]
    )