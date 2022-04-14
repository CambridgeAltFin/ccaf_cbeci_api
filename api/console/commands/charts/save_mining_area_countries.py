
import click
import csv
import psycopg2

from config import config


@click.command(name='charts:save:mining-area-countries')
@click.option('-v', '--version', default='1.0.0')
@click.argument('filename')
def handle(filename, version):
    with open(filename) as file, psycopg2.connect(**config['custom_data']) as connection:
        reader = csv.reader(file)
        next(reader, None)

        cursor = connection.cursor()

        cursor.execute('SELECT max(id) FROM mining_area_countries')
        key = cursor.fetchall()[0][0]

        for row in reader:
            country_id = None
            cursor.execute('SELECT country_id FROM mining_map_countries WHERE name = %s', (row[1],))
            data = cursor.fetchall()

            if len(data) > 0:
                country_id = data[0][0]
            key += 1
            save(cursor, key, row, version, country_id)


def save(cursor, key, row, version, country_id):
    insert_sql = 'INSERT INTO mining_area_countries (id, name, value, absolute_value, date, version, country_id) ' \
                 'VALUES (%s, %s, %s, %s, %s, %s, %s)'
    cursor.execute(insert_sql, (key, row[1], row[2], row[5], row[0], version, country_id))
