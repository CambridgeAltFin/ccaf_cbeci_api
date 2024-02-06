from api.prometheus import Prometheus, Prometheus2
from api.monitoreth import Monitoreth
from config import config

import click
import psycopg2
import psycopg2.extras


@click.command(name='eth-pos:sync:nodes-distribution')
def handle():
    #prometheus_data = Prometheus().crawler_geographical_distribution()
    #new_prometheus_data = Prometheus2().crawler_geographical_distribution()
    monitoreth_data = Monitoreth().geographical_distribution()

    with psycopg2.connect(**config['custom_data']) as conn:
        cursor = conn.cursor()
        cursor.execute('select id, code from countries')
        countries = {code: i for (i, code) in cursor.fetchall()}

        def insert(data, source):
            psycopg2.extras.execute_values(
                cursor,
                'insert into eth_pos_nodes_distribution (country_id, number_of_nodes, source, date, datetime)  '
                'VALUES %s on conflict (country_id, source, date) do nothing',
                [(
                    countries[i['country']],
                    i.get('number_of_nodes', 0),
                    source,
                    i['date'][:10],
                    i.get('date', None),
                ) for i in data if i['country'] in countries]
            )

        def insert_and_update(data, source):
            psycopg2.extras.execute_values(
                cursor,
                'insert into eth_pos_nodes_distribution (country_id, number_of_nodes, source, date, datetime)  '
                'VALUES %s on conflict (country_id, source, date) do update set '
                'number_of_nodes = EXCLUDED.number_of_nodes, datetime = EXCLUDED.datetime',
                [(
                    countries[i['country']],
                    i.get('number_of_nodes', 0),
                    source,
                    i['date'][:10],
                    i.get('date', None),
                ) for i in data if i['country'] in countries]
            )

        #if len(prometheus_data):
        #    insert(prometheus_data, 'prometheus')
        #    insert(prometheus_data, 'monitoreth')

        #if len(new_prometheus_data):
        #    insert_and_update(new_prometheus_data, 'prometheus')
        #    insert(new_prometheus_data, 'monitoreth')

        if len(monitoreth_data):
            insert_and_update(monitoreth_data, 'monitoreth')
