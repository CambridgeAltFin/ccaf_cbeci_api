
import calendar
from console.cli import LOGGER
from config import config
from api.monitoreth import Monitoreth as ApiMonitoreth
from datetime import date, timezone

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
                        
            chunkSize = 50
            for i in range(0, len(data), chunkSize): 
                chunk = data[i:i+chunkSize]
                sql = ""

                for i in range(len(chunk)):
                    sql += f"INSERT INTO {table_name} (entity, value, timestamp) " \
                        f"VALUES ('{chunk[i]['entity']}', {chunk[i]['num_validators']}, {today});\n"
                    
                cursor.execute(sql)

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
            
            chunkSize = 100
            sql = ""
            i = 0
            for index, row in data.iterrows():
                i = index
                if(i % chunkSize == 0 and i != 0): 
                    cursor.execute(sql)
                    sql = ""

                cur_date = calendar.timegm(date.fromtimestamp(row['timestamp']).timetuple())

                sql += f"DELETE FROM {table_name} " \
                            f"WHERE timestamp = {cur_date} AND entity = '{row['entity']}';\n" \
                            f"INSERT INTO {table_name} (entity, value, timestamp) " \
                            f"VALUES ('{row['entity']}', {row['active_validators']}, {cur_date});\n"

            if(i % chunkSize != 0):
                cursor.execute(sql)

        except Exception as error:
            LOGGER.exception(f"{table_name}: {str(error)}")
        finally:
            connection.commit()
