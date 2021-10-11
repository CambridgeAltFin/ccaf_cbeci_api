import click
import csv
import psycopg2

from config import config


@click.command(name='save:mining-map-countries')
@click.argument('filename')
def handle(filename):
    with open(filename) as file, psycopg2.connect(**config['custom_data']) as connection:
        reader = csv.reader(file)
        next(reader, None)

        cursor = connection.cursor()
        for row in reader:
            save(cursor, row)


def save(cursor, row):
    insert_sql = 'INSERT INTO mining_map_countries (name, value, date) VALUES (%s, %s, %s)'
    cursor.execute(insert_sql, (row[1], row[4], row[0]))
