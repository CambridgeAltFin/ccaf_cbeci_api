
import calendar
from console.cli import LOGGER
from config import config
from api.monitoreth import Monitoreth as ApiMonitoreth
from datetime import date

import os
import psycopg2
import click
import pandas as pd


@click.command(name='eth:fetch:pos_active_validators')
def handle_fetch():
    LOGGER.info('fetch:pos_active_validators called')
    with psycopg2.connect(**config['custom_data']) as connection:
        cursor = connection.cursor()

        table_name = 'pos_active_validators'

        apiMonitoreth = ApiMonitoreth()
        data = apiMonitoreth.active_validators()
        today = calendar.timegm(date.today().timetuple())
        print(f"data len: {len(data)}")

        try:
            cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ("
                            f"id serial NOT NULL,"
                            f"entity varchar(100) NOT NULL,"
                            f"value integer NOT NULL,"
                            f"timestamp bigint NOT NULL,"
                            f"CONSTRAINT {table_name}_pkey PRIMARY KEY (id)"
                            f");")
            
            cursor.execute(f"DELETE FROM {table_name} "
                            f"WHERE timestamp = %s", (today,))

            insert_sql = f"INSERT INTO {table_name} (entity, value, timestamp) " \
                f"VALUES (%s, %s, {today})"

            for item in data:
                cursor.execute(
                    insert_sql, (item['entity'], item['num_validators']))

        except Exception as error:
            LOGGER.exception(f"{table_name}: {str(error)}")
        finally:
            connection.commit()

@click.command(name='eth:import:pos_active_validators')
def handle_import():
    LOGGER.info('import:pos_active_validators called')
    with psycopg2.connect(**config['custom_data']) as connection:
        cursor = connection.cursor()

        table_name = 'pos_active_validators'
        data = pd.read_csv(os.getcwd() + '/../storage/eth_pos/active_validators.csv')

        try:
            cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ("
                            f"id serial NOT NULL,"
                            f"entity varchar(100) NOT NULL,"
                            f"value integer NOT NULL,"
                            f"timestamp bigint NOT NULL,"
                            f"CONSTRAINT {table_name}_pkey PRIMARY KEY (id)"
                            f");")
            
            delete_sql=f"DELETE FROM {table_name} " \
                        f"WHERE timestamp = %s"
            insert_sql=f"INSERT INTO {table_name} (entity, value, timestamp) " \
                        f"VALUES %s"
            
            insert_typles = tuple([
                (row['entity'], row['active_validators'], calendar.timegm(date.fromtimestamp(row['timestamp']).timetuple()))
                for index, row in data.iterrows()
            ])
            delete_dates = list(set([calendar.timegm(date.fromtimestamp(row['timestamp']).timetuple())
                for index, row in data.iterrows()]))
            delete_typles = tuple([
                (row,)
                for row in delete_dates
            ])

            psycopg2.extras.execute_batch(cursor, delete_sql, delete_typles)
            psycopg2.extras.execute_values(cursor, insert_sql, insert_typles)


        except Exception as error:
            LOGGER.exception(f"{table_name}: {str(error)}")
        finally:
            connection.commit()
