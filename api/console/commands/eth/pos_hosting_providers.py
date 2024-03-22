
import calendar
import datetime
from console.cli import LOGGER
from config import config
from api.monitoreth import Monitoreth as ApiMonitoreth
from datetime import date

import os
import psycopg2
import click
import pandas as pd
from config import pos_isp_associations

@click.command(name='eth:fetch:pos_hosting_providers')
def handle_fetch():
    LOGGER.info('fetch:pos_hosting_providers called')
    with psycopg2.connect(**config['custom_data']) as connection:
        cursor = connection.cursor()

        table_name = 'pos_hosting_providers'

        apiMonitoreth = ApiMonitoreth()
        data = apiMonitoreth.hosting_providers()

        today = calendar.timegm(date.today().timetuple())

        try:
            cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ("
                           f"id serial NOT NULL,"
                           f"isp varchar(255) NOT NULL,"
                           f"node_count integer NOT NULL,"
                           f"timestamp bigint NOT NULL,"
                           f"CONSTRAINT {table_name}_pkey PRIMARY KEY (id)"
                           f");")

            cursor.execute(f"DELETE FROM {table_name} "
                           f"WHERE timestamp = %s", (today,))

            insert_sql = f"INSERT INTO {table_name} (isp, node_count, timestamp) " \
                f"VALUES (%s, %s, {today})"

            for item in data:
                cursor.execute(
                    insert_sql, (item['isp'], item['total_nodes']))

        except Exception as error:
            LOGGER.exception(f"{table_name}: {str(error)}")
        finally:
            connection.commit()


@click.command(name='eth:import:pos_hosting_providers')
def handle_import():
    LOGGER.info('import:pos_hosting_providers called')
    with psycopg2.connect(**config['custom_data']) as connection:
        cursor = connection.cursor()

        table_name = 'pos_hosting_providers'
        data = pd.read_csv(
            os.getcwd() + '/../storage/eth_pos/armiarma_host_type_isp_distribution.csv')

        try:
            cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ("
                           f"id serial NOT NULL,"
                           f"isp varchar(255) NOT NULL,"
                           f"node_count integer NOT NULL,"
                           f"timestamp bigint NOT NULL,"
                           f"CONSTRAINT {table_name}_pkey PRIMARY KEY (id)"
                           f");")

            delete_sql = f"DELETE FROM {table_name} " \
                f"WHERE timestamp = %s"
            insert_sql = f"INSERT INTO {table_name} (isp, node_count, timestamp) " \
                f"VALUES %s"

            insert_data = []
            delete_data = []
            day_data = {}
            prev_date = None
            prev_date_formatted = None
            for index, row in data.iterrows():
                if (not row['isp'] or row['node_count'] == 0 ):
                    continue

                cur_date_formatted = calendar.timegm(datetime.datetime.strptime(
                        row['timestamp'], '%Y-%m-%d %H:%M:%S.%f').date().timetuple())
                
                if(prev_date != row['timestamp'] and cur_date_formatted == prev_date_formatted):
                    day_data = {}

                if (prev_date_formatted != cur_date_formatted and not (prev_date is None)):
                    for isp in day_data:
                        insert_data.append(
                            (isp, day_data[isp], prev_date_formatted))
                    delete_data.append((prev_date_formatted,))
                    
                    day_data = {}

                if row['isp'] in pos_isp_associations.keys():
                    row['isp'] = pos_isp_associations[row['isp']]

                if row['isp'] in day_data.keys():
                    day_data[row['isp']] += row['node_count']
                else:
                    day_data[row['isp']] = row['node_count']

                prev_date = row['timestamp']
                prev_date_formatted = cur_date_formatted

            prev_date_formatted = calendar.timegm(datetime.datetime.strptime(
                prev_date, '%Y-%m-%d %H:%M:%S.%f').date().timetuple())
            for isp in day_data:
                insert_data.append(
                    (isp, day_data[isp], prev_date_formatted))
            delete_data.append((prev_date_formatted,))

            psycopg2.extras.execute_batch(
                cursor, delete_sql, tuple(delete_data))
            psycopg2.extras.execute_values(
                cursor, insert_sql, tuple(insert_data))

        except Exception as error:
            LOGGER.exception(f"{table_name}: {str(error)}")
        finally:
            connection.commit()
