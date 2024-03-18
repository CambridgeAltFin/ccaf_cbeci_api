
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
                            f"share real NOT NULL,"
                            f"timestamp bigint NOT NULL,"
                            f"CONSTRAINT {table_name}_pkey PRIMARY KEY (id)"
                            f");")
            
            cursor.execute(f"DELETE FROM {table_name} "
                            f"WHERE timestamp = %s", (today,))
            
            total = sum(d['num_validators'] for d in data)
            
            chunkSize = 50
            for i in range(0, len(data), chunkSize): 
                chunk = data[i:i+chunkSize]
                sql = ""

                for i in range(len(chunk)):
                    share = round(chunk[i]['num_validators'] / total * 100, 6)
                    sql += f"INSERT INTO {table_name} (entity, value, share, timestamp) " \
                        f"VALUES ('{chunk[i]['entity']}', {chunk[i]['num_validators']}, {share}, {today});\n"
                    
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
            
            sql = ""
            dayRows = []
            dayTimestamp = 0
            dayTotal = 0
            for index, row in data.iterrows():
                if(dayTimestamp == 0):
                    dayTimestamp = row["timestamp"]
                if(row["timestamp"] != dayTimestamp):
                    cur_date = calendar.timegm(date.fromtimestamp(dayTimestamp).timetuple())

                    for j in range(len(dayRows)):
                        share = round(dayRows[j]['active_validators'] / dayTotal * 100, 6)
                        sql += f"DELETE FROM {table_name} " \
                            f"WHERE timestamp = {cur_date} AND entity = '{row['entity']}';\n" \
                            f"INSERT INTO {table_name} (entity, value, share, timestamp) " \
                            f"VALUES ('{row['entity']}', {row['active_validators']}, {share}, {cur_date});\n"
                        
                    cursor.execute(sql)
                    sql = ""
                    print(len(dayRows))
                    dayRows = []
                    dayTimestamp = row["timestamp"]
                    dayTotal = 0

                dayRows.append(row)
                dayTotal += row['active_validators']
            
            cur_date = calendar.timegm(date.fromtimestamp(dayTimestamp).timetuple())
            for j in range(len(dayRows)):
                share = round(dayRows[j]['active_validators'] / dayTotal * 100, 6)
                sql += f"DELETE FROM {table_name} " \
                    f"WHERE timestamp = {cur_date} AND entity = '{row['entity']}';\n" \
                    f"INSERT INTO {table_name} (entity, value, share, timestamp) " \
                    f"VALUES ('{row['entity']}', {row['active_validators']}, {share}, {cur_date});\n"
            if(sql != ""):
                cursor.execute(sql)

        except Exception as error:
            LOGGER.exception(f"{table_name}: {str(error)}")
        finally:
            connection.commit()
