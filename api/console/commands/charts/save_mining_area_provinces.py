
import click
import csv
import psycopg2

from config import config


@click.command(name='charts:save:mining-area-provinces')
@click.option('-v', '--version', default='1.0.0')
@click.argument('filename')
def handle(filename, version):
    with open(filename) as file, psycopg2.connect(**config['custom_data']) as connection:
        reader = csv.reader(file)
        next(reader, None)

        cursor = connection.cursor()
        for row in reader:
            save(cursor, row, version)


def save(cursor, row, version):
    insert_sql = 'INSERT INTO atlas.public.mining_area_provinces (name, value, local_value, date, version) VALUES (%s, %s, %s, %s, %s)'
    cursor.execute(insert_sql, (row[1], 0, row[2], row[0], version))