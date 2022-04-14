import click
import csv
import psycopg2

from config import config


@click.command(name='charts:save:mining-map-countries')
@click.argument('filename')
def handle(filename):
    with open(filename) as file, psycopg2.connect(**config['custom_data']) as connection:
        reader = csv.reader(file)
        next(reader, None)

        cursor = connection.cursor()

        cursor.execute('SELECT max(id) FROM mining_map_countries')
        key = cursor.fetchall()[0][0]

        for row in reader:
            country_id = None
            cursor.execute('SELECT date, country_id FROM mining_map_countries WHERE name = %s', (row[1],))
            data = cursor.fetchall()

            if len(data) > 0:
                country_id = data[0][1]
                if already_imported(data, row[0]):
                    continue

            key += 1
            save(cursor, key, row, country_id)


def save(cursor, key, row, country_id):
    insert_sql = 'INSERT INTO mining_map_countries (id, name, value, date, country_id) VALUES (%s, %s, %s, %s, %s)'
    cursor.execute(insert_sql, (key, row[1], row[4], row[0], country_id))


def already_imported(data, date):
    for i in data:
        if i[0].strftime('%Y-%m-%d') == date:
            return True
    return False
