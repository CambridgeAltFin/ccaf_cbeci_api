
import click
import csv
import psycopg2

from config import config


@click.command(name='charts:save:mining-area-provinces')
@click.option('-v', '--version', default='1.0.0')
@click.option('-c', '--country', default='CN')
@click.argument('filename')
def handle(filename, version, country):
    with open(filename) as file, psycopg2.connect(**config['custom_data']) as connection:
        reader = csv.reader(file)
        next(reader, None)

        cursor = connection.cursor()

        cursor.execute('SELECT max(id) FROM mining_area_provinces')
        key = cursor.fetchall()[0][0]

        cursor.execute('SELECT id FROM countries WHERE code = %s', (country,))
        country_id = cursor.fetchall()[0][0]

        for row in reader:
            key += 1
            save(cursor, key, row, version, country_id)


def save(cursor, key, row, version, country_id):
    insert_sql = 'INSERT INTO mining_area_provinces (id, name, value, local_value, date, version, country_id) ' \
                 'VALUES (%s, %s, %s, %s, %s, %s, %s)'
    cursor.execute(insert_sql, (key, row[2], 0, row[5], row[0], version, country_id))
