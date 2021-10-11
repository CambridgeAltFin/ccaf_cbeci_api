import click
import csv
import psycopg2

from config import config


@click.command(name='save:mining-map-provinces')
@click.argument('filename')
def handle(filename):
    with open(filename) as file, psycopg2.connect(**config['custom_data']) as connection:
        reader = csv.reader(file)
        next(reader, None)

        cursor = connection.cursor()
        for row in reader:
            print(row)
            save(cursor, row)


def save(cursor, row):
    insert_sql = 'INSERT INTO mining_map_provinces (name, value, local_value, date) VALUES (%s, %s, %s, %s)'
    cursor.execute(insert_sql, (row[1], 0, row[4], row[0]))
