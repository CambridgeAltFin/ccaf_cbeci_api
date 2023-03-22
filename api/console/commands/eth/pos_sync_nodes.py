from api.prometheus import Prometheus, Prometheus2
from api.migalabs import Migalabs
from config import config

import click
import psycopg2
import psycopg2.extras


@click.command(name='eth-pos:sync:nodes')
def handle():
    migalabs_data = Migalabs().beacon_chain_client_distribution_over_time()
    prometheus_data = Prometheus().crawler_observed_client_distribution()
    new_prometheus_data = Prometheus2().crawler_observed_client_distribution()
    min_date = min(new_prometheus_data, key=lambda x: x['Date'])
    new_prometheus_data.extend([x for x in prometheus_data if x['Date'] < min_date['Date']])

    with psycopg2.connect(**config['custom_data']) as conn:
        cursor = conn.cursor()
        save_data_prometheus(cursor, prometheus_data, 'prometheus')
        save_data(cursor, migalabs_data, 'migalabs')

def save_data(cursor, data, source):
    psycopg2.extras.execute_values(
        cursor,
        """
        insert into eth_pos_nodes (prysm, lighthouse, teku, nimbus, lodestar, grandine, others, erigon, source, date) 
        values %s on conflict (source, date) do nothing
        """,
        [(
            i.get('prysm', 0),
            i.get('lighthouse', 0),
            i.get('teku', 0),
            i.get('nimbus', 0),
            i.get('lodestar', 0),
            i.get('grandine', 0),
            i.get('others', 0),
            i.get('erigon', 0),
            source,
            i['Date']
        ) for i in data]
    )

def save_data_prometheus(cursor, data, source):
    psycopg2.extras.execute_values(
        cursor,
        """
        insert into eth_pos_nodes (prysm, lighthouse, teku, nimbus, lodestar, grandine, others, erigon, source, date) 
        values %s on conflict (source, date) 
        do update set 
            prysm = EXCLUDED.prysm, 
            lighthouse = EXCLUDED.lighthouse, 
            teku = EXCLUDED.teku, 
            nimbus = EXCLUDED.nimbus, 
            lodestar = EXCLUDED.lodestar, 
            grandine = EXCLUDED.grandine, 
            others = EXCLUDED.others, 
            erigon = EXCLUDED.erigon
        """,
        [(
            i.get('prysm', 0),
            i.get('lighthouse', 0),
            i.get('teku', 0),
            i.get('nimbus', 0),
            i.get('lodestar', 0),
            i.get('grandine', 0),
            i.get('others', 0),
            i.get('erigon', 0),
            source,
            i['Date']
        ) for i in data]
    )
