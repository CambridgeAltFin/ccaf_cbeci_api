from api.prometheus import Prometheus, Prometheus2
from api.migalabs import Migalabs
from config import config

import click
import psycopg2
import psycopg2.extras
import pandas as pd


@click.command(name='eth-pos:sync:nodes')
def handle():
    # migalabs_data = Migalabs().beacon_chain_client_distribution_over_time()
    prometheus_data = Prometheus().crawler_observed_client_distribution()
    new_prometheus_data = Prometheus2().crawler_observed_client_distribution()

    ### add missing date for prometheus
    prometheus_data_ffill = pd.DataFrame(prometheus_data)
    start_date = prometheus_data_ffill['Date'][0]
    end_date = prometheus_data_ffill['Date'][prometheus_data_ffill.shape[0]-1]
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    new_dates = pd.DataFrame({'Date': date_range})
    new_dates['Date'] = new_dates['Date'].astype(str)
    prometheus_data_ffill = new_dates.merge(prometheus_data_ffill, how = 'left', on = 'Date')
    prometheus_data_ffill = prometheus_data_ffill.ffill()
    prometheus_data = prometheus_data_ffill.to_dict('records')

    with psycopg2.connect(**config['custom_data']) as conn:
        cursor = conn.cursor()
        save_data(cursor, prometheus_data, 'prometheus')
        save_data_prometheus(cursor, new_prometheus_data, 'prometheus')
        # save_data(cursor, migalabs_data, 'migalabs')

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
