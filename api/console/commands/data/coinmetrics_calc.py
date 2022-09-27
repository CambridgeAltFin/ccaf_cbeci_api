
from api.coinmetrics import CoinMetrics as ApiCoinMetrics
from console.cli import LOGGER
from config import config

import click
import psycopg2
import pandas as pd
from datetime import datetime
from dateutil import parser


@click.command(name='data:fetch:coinmetrics')
def handle():
    LOGGER.info('coinmetrics called')
    with psycopg2.connect(**config['blockchain_data']) as connection:
        cursor = connection.cursor()

        def save_hashrate(type, time, value, asset='btc'):
            table_name = 'hash_rate_by_types'
            date = parser.parse(time).date()

            # Template of the query to paste a row to a table
            insert_sql = f"INSERT INTO {table_name} (type, asset, value, date) " \
                         f"VALUES (%s, %s, %s, %s) ON CONFLICT ON CONSTRAINT {table_name}_date_ukey DO NOTHING;"
            try:
                cursor.execute("CREATE TABLE IF NOT EXISTS hash_rate_by_types ("
                               "id serial NOT NULL,"
                               "type text NOT NULL,"
                               "value real NOT NULL,"
                               "date date NOT NULL,"
                               "created_at timestamp without time zone NOT NULL DEFAULT NOW(),"
                               "asset text NOT NULL,"
                               "CONSTRAINT hash_rate_by_types_pkey PRIMARY KEY (id),"
                               "CONSTRAINT hash_rate_by_types_date_ukey UNIQUE (type, date)"
                               ");")
                cursor.execute(insert_sql, (type, asset, value, date))
            except Exception as error:
                LOGGER.exception(f"{table_name}: {str(error)}")
            finally:
                connection.commit()

    api_coinmetrics = ApiCoinMetrics(api_key=config['api.coinmetrics.io']['api_key'])
    start_time = datetime(year=2009, month=1, day=1)
    metrics = {
        's9': 'HashRate30dS9Pct',
        's7': 'HashRate30dS7Pct'
    }
    for t, metric in metrics.items():
        LOGGER.info(f"hash_rate_by_types (type: {t}): as of {datetime.utcnow().isoformat()}")
        data = api_coinmetrics.timeseries().asset_metrics(metrics=metric, start_time=start_time)
        df = pd.DataFrame(data).sort_values(by=['time'])

        for row in df.itertuples(name='Metric'):
            save_hashrate(type=t, asset=row.asset, time=row.time, value=float(getattr(row, metric)) / 100)
