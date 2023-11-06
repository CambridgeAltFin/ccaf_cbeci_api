from api.prometheus import Prometheus, Prometheus2
from api.monitoreth import Monitoreth
from config import config

import click
import psycopg2
import psycopg2.extras


@click.command(name='eth-pos:sync:nodes-distribution')
def handle():
    prometheus_data = Prometheus().crawler_geographical_distribution()
    new_prometheus_data = Prometheus2().crawler_geographical_distribution()
    monitoreth_data = Monitoreth().geographical_distribution()

    with psycopg2.connect(**config['custom_data']) as conn:
        cursor = conn.cursor()
        cursor.execute('select id, code from countries')
        countries = {code: i for (i, code) in cursor.fetchall()}

        def save(data, source):
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

        if len(prometheus_data):
            save(prometheus_data, 'prometheus')
            save(prometheus_data, 'monitoreth')

        if len(new_prometheus_data):
            save(new_prometheus_data, 'prometheus')
            save(new_prometheus_data, 'monitoreth')

        if len(monitoreth_data):
            save(monitoreth_data, 'monitoreth')
