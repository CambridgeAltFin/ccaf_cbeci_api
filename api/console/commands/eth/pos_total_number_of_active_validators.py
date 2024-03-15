
from console.cli import LOGGER
from config import config

import psycopg2
import click


@click.command(name='eth:fetch:total_number_of_active_validators')
def handle():
    LOGGER.info('coinmetrics called')
    with psycopg2.connect(**config['custom_data']) as connection:
        cursor = connection.cursor()

        table_name = 'total_number_of_active_validators'

        # Template of the query to paste a row to a table
        insert_sql = f"INSERT INTO {table_name} (value) " \
                        f"VALUES (%s)"
        try:
            cursor.execute("CREATE TABLE IF NOT EXISTS total_number_of_active_validators ("
                            "id serial NOT NULL,"
                            "value real NOT NULL,"
                            "created_at timestamp without time zone NOT NULL DEFAULT NOW(),"
                            "CONSTRAINT total_number_of_active_validators_pkey PRIMARY KEY (id)"
                            ");")
            cursor.execute(insert_sql, (3.14,))
        except Exception as error:
            LOGGER.exception(f"{table_name}: {str(error)}")
        finally:
            connection.commit()

